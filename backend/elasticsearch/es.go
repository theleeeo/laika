package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"time"

	esv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/theleeeo/laika/core"
)

type Client struct {
	es *esv8.Client

	// Temporary solution to control refresh behavior during tests
	withRefresh bool

	federatedExecution FederatedExecution
}

// FederatedExecution selects how FederatedSearch executes against the cluster.
// It is an experiment toggle (ADR 0007 kept per-Type fan-out as a documented
// future execution swap): all modes return the same hit membership, but they
// differ in scoring statistics and pagination cost, and totals match only
// below ES's track_total_hits cap (default 10k) — the single query caps Total
// once across all indices where the fan-out sums per-leg capped totals, so
// the modes' totals diverge above the cap.
type FederatedExecution string

const (
	// FederatedSingleDFS is the default: one multi-index query with
	// dfs_query_then_fetch, so cross-type BM25 scores share global term
	// statistics (spec D13).
	FederatedSingleDFS FederatedExecution = "single-dfs"
	// FederatedSingle is the same single multi-index query with the ES default
	// query_then_fetch: scores use per-shard-local term statistics. With
	// single-shard indices this scores exactly like FederatedFanout, making it
	// the cheap way to evaluate fan-out ranking quality before paying for the
	// fan-out execution.
	FederatedSingle FederatedExecution = "single"
	// FederatedFanout issues one sub-search per Type via _msearch and merges
	// client-side; see federatedFanout for the trade-offs.
	FederatedFanout FederatedExecution = "fanout"
)

// ParseFederatedExecution maps a config string to a FederatedExecution; empty
// selects the default (FederatedSingleDFS).
func ParseFederatedExecution(s string) (FederatedExecution, error) {
	switch mode := FederatedExecution(s); mode {
	case "":
		return FederatedSingleDFS, nil
	case FederatedSingleDFS, FederatedSingle, FederatedFanout:
		return mode, nil
	default:
		return "", fmt.Errorf("unknown federated execution mode %q (want %q, %q or %q)",
			s, FederatedSingleDFS, FederatedSingle, FederatedFanout)
	}
}

// Option configures optional Client behavior.
type Option func(*Client)

// WithFederatedExecution selects the Federated Search execution mode. The
// default is FederatedSingleDFS.
func WithFederatedExecution(mode FederatedExecution) Option {
	return func(c *Client) { c.federatedExecution = mode }
}

func New(client *esv8.Client, withRefresh bool, opts ...Option) *Client {
	c := &Client{es: client, withRefresh: withRefresh, federatedExecution: FederatedSingleDFS}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Dial constructs a Client connected to the given Elasticsearch address(es).
// username/password may be empty for an unauthenticated cluster. It is a
// convenience for standalone commands (e.g. gen-mapping, diff-mapping) that only
// need a client to talk to a running cluster.
func Dial(addrs []string, username, password string) (*Client, error) {
	es, err := esv8.NewClient(esv8.Config{
		Addresses: addrs,
		Username:  username,
		Password:  password,
	})
	if err != nil {
		return nil, err
	}
	return New(es, false), nil
}

func (c *Client) Upsert(ctx context.Context, indexAlias, docID string, doc any, version int64) error {
	now := time.Now()
	defer func() {
		slog.Info("upserted doc", "docID", docID, "index", indexAlias, "duration", time.Since(now))
	}()

	if version <= 0 {
		return fmt.Errorf("invalid external version %d for %s/%s", version, indexAlias, docID)
	}

	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	refresh := "false"
	if c.withRefresh {
		refresh = "true"
	}

	res, err := c.es.Index(
		indexAlias,
		bytes.NewReader(body),
		c.es.Index.WithDocumentID(docID),
		c.es.Index.WithContext(ctx),
		c.es.Index.WithRefresh(refresh),
		c.es.Index.WithVersion(int(version)),
		c.es.Index.WithVersionType("external_gte"),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 409 {
		return fmt.Errorf("upsert %s/%s at version %d: %w", indexAlias, docID, version, core.ErrVersionConflict)
	}
	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("es error: %s %s", res.Status(), string(b))
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, indexAlias, docID string) error {
	refresh := "false"
	if c.withRefresh {
		refresh = "true"
	}

	res, err := c.es.Delete(
		indexAlias,
		docID,
		c.es.Delete.WithContext(ctx),
		c.es.Delete.WithRefresh(refresh),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil
	}
	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("es error: %s %s", res.Status(), string(b))
	}
	slog.Info("deleted doc", "docID", docID, "index", indexAlias)
	return nil
}

func (c *Client) BulkUpsert(ctx context.Context, items []core.BulkItem) ([]core.BulkFailure, error) {
	if len(items) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf)

	for _, it := range items {
		if it.Version <= 0 {
			return nil, fmt.Errorf("invalid external version %d for %s/%s", it.Version, it.Index, it.ID)
		}

		meta := map[string]any{"index": map[string]any{
			"_index":       it.Index,
			"_id":          it.ID,
			"version":      it.Version,
			"version_type": "external_gte",
		}}
		if err := json.MarshalEncode(enc, meta); err != nil {
			return nil, fmt.Errorf("marshal index meta: %w", err)
		}

		if err := json.MarshalEncode(enc, it.Doc); err != nil {
			return nil, fmt.Errorf("marshal doc: %w", err)
		}
	}

	// Callers bound the request size (the rebuild paths flush in chunks), so
	// this guards against a hung cluster, not against oversized payloads.
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	refresh := "false"
	if c.withRefresh {
		refresh = "true"
	}

	res, err := c.es.Bulk(
		bytes.NewReader(buf.Bytes()),
		c.es.Bulk.WithContext(ctx),
		c.es.Bulk.WithRefresh(refresh),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("es bulk error: %s %s", res.Status(), string(b))
	}

	failures, err := parseBulkResponse(res.Body)
	if err != nil {
		return nil, fmt.Errorf("parse bulk response: %w", err)
	}
	slog.Info("bulk upserted docs", "count", len(items), "rejected", len(failures))
	return failures, nil
}

// bulkResponse is the subset of the ES _bulk response needed to surface
// per-item outcomes: a 2xx request-level status still carries item-level
// rejections under "errors": true.
type bulkResponse struct {
	Errors bool `json:"errors"`
	Items  []map[string]struct {
		Index  string `json:"_index"`
		ID     string `json:"_id"`
		Status int    `json:"status"`
		Error  struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
	} `json:"items"`
}

// parseBulkResponse extracts the items ES rejected.
func parseBulkResponse(body io.Reader) ([]core.BulkFailure, error) {
	var resp bulkResponse
	if err := json.UnmarshalRead(body, &resp); err != nil {
		return nil, err
	}
	if !resp.Errors {
		return nil, nil
	}

	var failures []core.BulkFailure
	for _, item := range resp.Items {
		// Each item is keyed by its action; BulkUpsert only issues "index".
		for _, r := range item {
			if r.Status >= 200 && r.Status < 300 {
				continue
			}
			// An OCC loss: a concurrent build with a higher Build Sequence
			// already wrote fresher data, so this write is a benign no-op.
			if r.Status == 409 {
				continue
			}
			failures = append(failures, core.BulkFailure{
				Index:  r.Index,
				ID:     r.ID,
				Status: r.Status,
				Reason: fmt.Sprintf("%s: %s", r.Error.Type, r.Error.Reason),
			})
		}
	}
	return failures, nil
}
