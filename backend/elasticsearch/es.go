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
}

func New(client *esv8.Client, withRefresh bool) *Client {
	return &Client{es: client, withRefresh: withRefresh}
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

func (c *Client) BulkUpsert(ctx context.Context, items []core.BulkItem) error {
	if len(items) == 0 {
		return nil
	}

	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf)

	for _, it := range items {
		if it.Version <= 0 {
			return fmt.Errorf("invalid external version %d for %s/%s", it.Version, it.Index, it.ID)
		}

		meta := map[string]any{"index": map[string]any{
			"_index":       it.Index,
			"_id":          it.ID,
			"version":      it.Version,
			"version_type": "external_gte",
		}}
		if err := json.MarshalEncode(enc, meta); err != nil {
			return fmt.Errorf("marshal index meta: %w", err)
		}

		if err := json.MarshalEncode(enc, it.Doc); err != nil {
			return fmt.Errorf("marshal doc: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
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
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("es bulk error: %s %s", res.Status(), string(b))
	}
	slog.Info("bulk upserted docs", "count", len(items))
	return nil
}
