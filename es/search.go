package es

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"time"

	"github.com/theleeeo/indexer/gen/search/v1"
	"github.com/theleeeo/indexer/resource"

	"google.golang.org/protobuf/types/known/structpb"
)

func (c *Client) Search(ctx context.Context, req *search.SearchRequest, indexAlias string, vc *resource.VersionConfig) (*search.SearchResponse, error) {
	boolQ := map[string]any{
		"must":   []any{},
		"filter": []any{},
	}

	// Always tenant filter
	// boolQ["filter"] = append(boolQ["filter"].([]any), map[string]any{
	// 	"term": map[string]any{"tenant_id": req.TenantId},
	// })

	// Full-text query (optional)
	if req.Query != "" {
		boolQ["must"] = append(boolQ["must"].([]any), buildFullTextQuery(req.Query, vc))
	}

	// Structured filters
	for _, f := range req.Filters {
		if f == nil || f.Field == "" {
			continue
		}
		filterClause, err := buildFilterClause(f)
		if err != nil {
			return nil, err
		}
		boolQ["filter"] = append(boolQ["filter"].([]any), filterClause)
	}

	body := map[string]any{
		"query": map[string]any{"bool": boolQ},
		"from":  req.Page * req.PageSize,
		"size":  req.PageSize,
	}

	// Sort (optional). If none provided, ES default scoring applies.
	if len(req.Sort) > 0 {
		var sorts []any
		for _, srt := range req.Sort {
			if srt == nil || srt.Field == "" {
				continue
			}
			order := "asc"
			if srt.Desc {
				order = "desc"
			}
			sorts = append(sorts, map[string]any{
				srt.Field: map[string]any{"order": order},
			})
		}
		if len(sorts) > 0 {
			body["sort"] = sorts
		}
	}

	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	fmt.Printf("ES search request body: %s\n", b)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	res, err := c.es.Search(
		c.es.Search.WithContext(ctx),
		c.es.Search.WithIndex(indexAlias),
		c.es.Search.WithBody(bytes.NewReader(b)),
		// c.es.Search.WithSource("false"),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			// index not found, return empty result
			return &search.SearchResponse{Total: 0, Hits: []*search.SearchHit{}}, nil
		}

		raw, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("es search error: %s %s", res.Status(), string(raw))
	}

	var decoded map[string]any
	if err := json.UnmarshalRead(res.Body, &decoded); err != nil {
		return nil, err
	}

	hitsObj, _ := decoded["hits"].(map[string]any)

	// total: { "value": N, "relation": "eq" } (ES7+)
	var total int64
	if t, ok := hitsObj["total"].(map[string]any); ok {
		if v, ok := t["value"].(float64); ok {
			total = int64(v)
		}
	}

	out := &search.SearchResponse{Total: total}
	for _, h := range hitsObj["hits"].([]any) {
		m, _ := h.(map[string]any)
		id, _ := m["_id"].(string)
		score, _ := m["_score"].(float64)
		src, _ := m["_source"].(map[string]any)

		// TODO: If we json unmarshal into a real type and not a map[string]any this will be handled automatically by the structpb UnmarshalJSON
		st, err := structpb.NewStruct(src)
		if err != nil {
			// if struct conversion fails, skip rather than fail the whole query
			continue
		}
		out.Hits = append(out.Hits, &search.SearchHit{
			Id:     id,
			Score:  score,
			Source: st,
		})
	}

	return out, nil
}

// buildFullTextQuery constructs the ES query clause for a free-text search
// string against a version config. Top-level (object) relation fields are
// included in a single multi_match; nested relation fields each get their own
// nested query. All clauses are combined with bool.should so a document
// matches when any clause matches.
func buildFullTextQuery(query string, vc *resource.VersionConfig) any {
	var shouldClauses []any

	// Flat fields under the "fields" object.
	var flatFields []string
	for _, f := range vc.Fields {
		if f.Query.Search == nil || *f.Query.Search {
			flatFields = append(flatFields, "fields."+f.Name)
		}
	}
	if len(flatFields) > 0 {
		shouldClauses = append(shouldClauses, map[string]any{
			"multi_match": map[string]any{
				"query":  query,
				"fields": flatFields,
			},
		})
	}

	// Relation fields — nested relations need a nested query wrapper.
	for _, rel := range vc.Relations {
		var relFields []string
		for _, f := range rel.Fields {
			if f.Query.Search == nil || *f.Query.Search {
				relFields = append(relFields, rel.Resource+"."+f.Name)
			}
		}
		if len(relFields) == 0 {
			continue
		}
		mm := map[string]any{
			"multi_match": map[string]any{
				"query":  query,
				"fields": relFields,
			},
		}
		if rel.IsMany() {
			shouldClauses = append(shouldClauses, map[string]any{
				"nested": map[string]any{
					"path":  rel.Resource,
					"query": mm,
				},
			})
		} else {
			shouldClauses = append(shouldClauses, mm)
		}
	}

	if len(shouldClauses) == 1 {
		return shouldClauses[0]
	}
	return map[string]any{
		"bool": map[string]any{
			"should":               shouldClauses,
			"minimum_should_match": 1,
		},
	}
}

func buildFilterClause(f *search.Filter) (any, error) {
	var inner any

	switch f.Op {
	case search.FilterOp_FILTER_OP_EQ:
		if f.Value == "" {
			return nil, fmt.Errorf("EQ filter requires value for field %q", f.Field)
		}
		inner = map[string]any{"term": map[string]any{f.Field: f.Value}}

	case search.FilterOp_FILTER_OP_IN:
		if len(f.Values) == 0 {
			return nil, fmt.Errorf("IN filter requires values for field %q", f.Field)
		}
		inner = map[string]any{"terms": map[string]any{f.Field: f.Values}}

	default:
		return nil, fmt.Errorf("unsupported filter op for field %q", f.Field)
	}

	// Nested wrapping (optional)
	if f.NestedPath != "" {
		return map[string]any{
			"nested": map[string]any{
				"path":  f.NestedPath,
				"query": inner,
			},
		}, nil
	}

	return inner, nil
}
