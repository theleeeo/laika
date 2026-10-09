package elasticsearch

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

// scanCursor is the backend's cursor of a scan, opaque to core: the point in
// time to search, the sort values of the previous page's last hit, which go
// back unchanged as search_after, and the first page's total.
type scanCursor struct {
	PITID string `json:"pit"`
	// SearchAfter is kept as raw JSON: decoding it into float64 would lose
	// longs above 2^53 (a numeric sort value, the implicit _shard_doc).
	SearchAfter jsontext.Value `json:"after,omitempty"`
	Total       int64          `json:"total"`
}

// encodeScanCursor renders c as base64url (unpadded) of its JSON.
func encodeScanCursor(c scanCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeScanCursor parses a cursor encodeScanCursor rendered. One that
// doesn't decode, or carries no point-in-time id or no sort values, is
// core.ErrInvalidPageToken.
func decodeScanCursor(token string) (scanCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return scanCursor{}, fmt.Errorf("%w: cursor is not base64url: %v", core.ErrInvalidPageToken, err)
	}
	var c scanCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return scanCursor{}, fmt.Errorf("%w: cursor does not decode: %v", core.ErrInvalidPageToken, err)
	}
	if c.PITID == "" {
		return scanCursor{}, fmt.Errorf("%w: cursor has no point-in-time id", core.ErrInvalidPageToken)
	}
	if !hasSortValues(c.SearchAfter) {
		return scanCursor{}, fmt.Errorf("%w: cursor has no sort values", core.ErrInvalidPageToken)
	}
	return c, nil
}

// hasSortValues reports whether raw is a non-empty JSON array, the shape of a
// hit's sort values and of search_after.
func hasSortValues(raw jsontext.Value) bool {
	var values []jsontext.Value
	return len(raw) > 0 && raw.Kind() == '[' && json.Unmarshal(raw, &values) == nil && len(values) > 0
}

// scanResponse is the part of a point-in-time search response a scan reads.
type scanResponse struct {
	PITID    string `json:"pit_id"`
	TimedOut bool   `json:"timed_out"`
	Shards   struct {
		Failed int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total struct {
			Value int64 `json:"value"`
		} `json:"total"`
		Hits []struct {
			Index string         `json:"_index"`
			ID    string         `json:"_id"`
			Score float64        `json:"_score"`
			Sort  jsontext.Value `json:"sort"`
		} `json:"hits"`
	} `json:"hits"`
}

// scan serves one page of a scan from a point in time on index, the concrete
// index core pinned, with vc that index's config. The first page (no
// req.PageToken) opens the point in time; a later page continues from its
// cursor with search_after. Every page sends the paged search's query and
// sort, a term on _index pinning hits to index, _source off, and
// allow_partial_search_results=false.
//
// Any 404 is core.ErrCursorExpired — the point in time or its index is gone —
// never the paged search's empty result, which would end a lost scan as an
// empty success. A hit from another index or a failed shard is
// core.ErrScanFault. A page shorter than req.PageSize is the last: its point
// in time is closed and no cursor returned. A scan ended by an error leaves
// its point in time to expire.
func (c *Client) scan(ctx context.Context, req core.SearchRequest, index string, vc *resource.VersionConfig) (core.SearchResponse, error) {
	start := time.Now()
	logger := core.LoggerFromContext(ctx)
	first := req.PageToken == ""

	var cur scanCursor
	if !first {
		var err error
		if cur, err = decodeScanCursor(req.PageToken); err != nil {
			return core.SearchResponse{}, err
		}
	}

	// Build the query before opening anything, so a filter the backend
	// refuses doesn't leave a point in time behind.
	boolQ, err := searchBoolQuery(req, vc)
	if err != nil {
		return core.SearchResponse{}, err
	}
	boolQ["filter"] = append(boolQ["filter"].([]any), map[string]any{"term": map[string]any{"_index": index}})

	keepAlive := fmt.Sprintf("%dms", c.scanKeepAlive.Milliseconds())
	if first {
		if cur.PITID, err = c.openPointInTime(ctx, index, keepAlive); err != nil {
			return core.SearchResponse{}, err
		}
	}

	body := map[string]any{
		"query":            map[string]any{"bool": boolQ},
		"size":             req.PageSize,
		"sort":             searchSort(req),
		"_source":          false,
		"track_total_hits": first,
		"pit":              map[string]any{"id": cur.PITID, "keep_alive": keepAlive},
	}
	if !first {
		body["search_after"] = cur.SearchAfter
	}
	b, err := json.Marshal(body)
	if err != nil {
		return core.SearchResponse{}, err
	}

	// Debug-only, as the paged search's: the body holds filter values.
	logger.Debug("es scan query", slog.String("index", index), slog.String("body", string(b)))

	searchCtx, cancel := context.WithTimeout(ctx, singleSearchTimeout)
	defer cancel()
	res, err := c.es.Search(
		c.es.Search.WithContext(searchCtx),
		c.es.Search.WithBody(bytes.NewReader(b)),
		c.es.Search.WithAllowPartialSearchResults(false),
	)
	if err != nil {
		return core.SearchResponse{}, err
	}
	defer res.Body.Close()
	if err := scanStatusError(res, "es scan search on "+index); err != nil {
		return core.SearchResponse{}, err
	}

	var decoded scanResponse
	if err := json.UnmarshalRead(res.Body, &decoded); err != nil {
		return core.SearchResponse{}, fmt.Errorf("decode es scan response: %w", err)
	}
	// A page cut short by a search timeout would read as the last one and
	// end the scan as a success with documents missing.
	if decoded.TimedOut {
		return core.SearchResponse{}, fmt.Errorf("%w: a scan page of %s timed out", core.ErrScanFault, index)
	}
	if decoded.Shards.Failed > 0 {
		return core.SearchResponse{}, fmt.Errorf("%w: %d shards of %s failed in a scan page", core.ErrScanFault, decoded.Shards.Failed, index)
	}
	for _, h := range decoded.Hits.Hits {
		if h.Index != index {
			return core.SearchResponse{}, fmt.Errorf("%w: scan of %s returned hit %q from index %q", core.ErrScanFault, index, h.ID, h.Index)
		}
	}

	if first {
		cur.Total = decoded.Hits.Total.Value
	}
	// Elasticsearch may return a new id for the point in time; the latest
	// one is the one to go on with.
	if decoded.PITID != "" {
		cur.PITID = decoded.PITID
	}

	hits := decoded.Hits.Hits
	out := core.SearchResponse{Total: cur.Total, Hits: make([]core.SearchHit, 0, len(hits))}
	for _, h := range hits {
		out.Hits = append(out.Hits, core.SearchHit{ID: h.ID, Score: h.Score})
	}

	if len(hits) == 0 || len(hits) < int(req.PageSize) {
		if err := c.closePointInTime(ctx, cur.PITID); err != nil {
			logger.Warn("close scan point in time",
				slog.String("index", index),
				slog.String("pit_id", cur.PITID),
				slog.String("error", err.Error()),
			)
		}
	} else {
		lastHit := hits[len(hits)-1]
		if !hasSortValues(lastHit.Sort) {
			return core.SearchResponse{}, fmt.Errorf("%w: scan of %s: last hit %q of a full page has no sort values", core.ErrScanFault, index, lastHit.ID)
		}
		cur.SearchAfter = lastHit.Sort
		out.NextPageToken = encodeScanCursor(cur)
	}

	logger.Debug("es scan result",
		slog.String("index", index),
		slog.Int64("total", out.Total),
		slog.Int("hit_count", len(out.Hits)),
		slog.Bool("last", out.NextPageToken == ""),
		slog.Duration("duration", time.Since(start)),
	)
	return out, nil
}

// scanStatusError turns an error response of a scan request into an error:
// any 404 is core.ErrCursorExpired, whatever its type.
func scanStatusError(res *esapi.Response, what string) error {
	if !res.IsError() {
		return nil
	}
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s: %s %s", core.ErrCursorExpired, what, res.Status(), raw)
	}
	return fmt.Errorf("%s error: %s %s", what, res.Status(), raw)
}

// openPointInTime opens a point in time on index and returns its id.
func (c *Client) openPointInTime(ctx context.Context, index, keepAlive string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, singleSearchTimeout)
	defer cancel()
	res, err := c.es.OpenPointInTime([]string{index}, keepAlive, c.es.OpenPointInTime.WithContext(ctx))
	if err != nil {
		return "", fmt.Errorf("open point in time on %s: %w", index, err)
	}
	defer res.Body.Close()
	if err := scanStatusError(res, "open point in time on "+index); err != nil {
		return "", err
	}
	var decoded struct {
		ID string `json:"id"`
	}
	if err := json.UnmarshalRead(res.Body, &decoded); err != nil {
		return "", fmt.Errorf("decode open point in time response: %w", err)
	}
	if decoded.ID == "" {
		return "", fmt.Errorf("open point in time on %s: response has no id", index)
	}
	return decoded.ID, nil
}

// closePointInTime closes the point in time id.
func (c *Client) closePointInTime(ctx context.Context, id string) error {
	b, err := json.Marshal(map[string]string{"id": id})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, singleSearchTimeout)
	defer cancel()
	res, err := c.es.ClosePointInTime(
		c.es.ClosePointInTime.WithContext(ctx),
		c.es.ClosePointInTime.WithBody(bytes.NewReader(b)),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		raw, _ := io.ReadAll(res.Body)
		return fmt.Errorf("close point in time: %s %s", res.Status(), raw)
	}
	return nil
}
