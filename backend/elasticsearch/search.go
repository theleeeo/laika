package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"time"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

func (c *Client) Search(ctx context.Context, req core.SearchRequest, indexAlias string, vc *resource.VersionConfig) (core.SearchResponse, error) {
	boolQ := map[string]any{
		"must":   []any{},
		"filter": []any{},
	}

	if req.Query != "" {
		boolQ["must"] = append(boolQ["must"].([]any), buildFullTextQuery(req.Query, vc))
	}

	for _, f := range req.Filters {
		if f.Field == "" {
			continue
		}
		filterClause, err := buildFilterClause(f)
		if err != nil {
			return core.SearchResponse{}, err
		}
		boolQ["filter"] = append(boolQ["filter"].([]any), filterClause)
	}

	body := map[string]any{
		"query": map[string]any{"bool": boolQ},
		"from":  req.Page * req.PageSize,
		"size":  req.PageSize,
	}

	if len(req.Sort) > 0 {
		var sorts []any
		for _, srt := range req.Sort {
			if srt.Field == "" {
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
		return core.SearchResponse{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	res, err := c.es.Search(
		c.es.Search.WithContext(ctx),
		c.es.Search.WithIndex(indexAlias),
		c.es.Search.WithBody(bytes.NewReader(b)),
	)
	if err != nil {
		return core.SearchResponse{}, err
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return core.SearchResponse{}, nil
		}
		raw, _ := io.ReadAll(res.Body)
		return core.SearchResponse{}, fmt.Errorf("es search error: %s %s", res.Status(), string(raw))
	}

	var decoded map[string]any
	if err := json.UnmarshalRead(res.Body, &decoded); err != nil {
		return core.SearchResponse{}, err
	}

	hitsObj, _ := decoded["hits"].(map[string]any)

	var total int64
	if t, ok := hitsObj["total"].(map[string]any); ok {
		if v, ok := t["value"].(float64); ok {
			total = int64(v)
		}
	}

	out := core.SearchResponse{Total: total}
	for _, h := range hitsObj["hits"].([]any) {
		m, _ := h.(map[string]any)
		id, _ := m["_id"].(string)
		score, _ := m["_score"].(float64)
		src, _ := m["_source"].(map[string]any)
		out.Hits = append(out.Hits, core.SearchHit{
			ID:     id,
			Score:  score,
			Source: src,
		})
	}

	return out, nil
}

func buildFullTextQuery(query string, vc *resource.VersionConfig) any {
	var shouldClauses []any

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

func buildFilterClause(f core.Filter) (any, error) {
	var inner any

	switch f.Op {
	case core.FilterOpEq:
		if f.Value == "" {
			return nil, fmt.Errorf("EQ filter requires value for field %q", f.Field)
		}
		inner = map[string]any{"term": map[string]any{f.Field: f.Value}}

	case core.FilterOpIn:
		if len(f.Values) == 0 {
			return nil, fmt.Errorf("IN filter requires values for field %q", f.Field)
		}
		inner = map[string]any{"terms": map[string]any{f.Field: f.Values}}

	default:
		return nil, fmt.Errorf("unsupported filter op for field %q", f.Field)
	}

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
