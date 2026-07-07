package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

func (c *Client) Search(ctx context.Context, req core.SearchRequest, indexAlias string, vc *resource.VersionConfig) (core.SearchResponse, error) {
	start := time.Now()
	logger := core.LoggerFromContext(ctx)

	boolQ := map[string]any{
		"must":   []any{},
		"filter": []any{},
	}

	if req.Query != "" {
		boolQ["must"] = append(boolQ["must"].([]any), buildFullTextQuery(req.Query))
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

	// body contains the full ES query, including filter values (actor ids,
	// search terms). Debug-only; gate behind an explicit opt-in before shipping
	// these logs to a shared aggregator.
	logger.Debug("es query",
		slog.String("index", indexAlias),
		slog.String("body", string(b)),
	)

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

	logger.Debug("es result",
		slog.String("index", indexAlias),
		slog.Int64("total", total),
		slog.Int("hit_count", len(out.Hits)),
		slog.Duration("duration", time.Since(start)),
	)
	return out, nil
}

// federatedSearchType is the ES search_type for Federated Search. DFS gathers
// global term statistics across all queried indices before scoring so cross-type
// BM25 scores are comparable (spec D13). Kept as a single constant so the
// documented future experiment — dropping to plain query_then_fetch — is a
// one-line flip.
const federatedSearchType = "dfs_query_then_fetch"

// FederatedSearch runs a single multi-index query across the FilterGroups'
// aliases and returns results keyed by concrete index (spec D3, D12, D13).
func (c *Client) FederatedSearch(ctx context.Context, p core.FederatedSearchParams) (core.FederatedSearchResult, error) {
	indices := make([]string, 0, len(p.FilterGroups))
	for _, g := range p.FilterGroups {
		indices = append(indices, g.Alias)
	}

	filter := []any{}
	for _, f := range p.Filters {
		if f.Field == "" {
			continue
		}
		clause, err := buildFilterClause(f)
		if err != nil {
			return core.FederatedSearchResult{}, err
		}
		filter = append(filter, clause)
	}
	groups, err := buildIndexFilterGroups(p.FilterGroups)
	if err != nil {
		return core.FederatedSearchResult{}, err
	}
	filter = append(filter, groups)

	boolQ := map[string]any{"filter": filter}
	if p.Query != "" {
		boolQ["must"] = []any{buildFederatedTextQuery(p.Query, p.SecondaryScope)}
	}

	body := map[string]any{
		"query": map[string]any{"bool": boolQ},
		"from":  p.Page * p.PageSize,
		"size":  p.PageSize,
		// Per-resource counts (D12): one bucket per concrete index, folded back
		// to Types by core. size covers at most one index per requested Type.
		"aggs": map[string]any{
			"per_index": map[string]any{
				"terms": map[string]any{"field": "_index", "size": len(indices)},
			},
		},
	}
	if !p.IncludeSource {
		body["_source"] = false
	}

	b, err := json.Marshal(body)
	if err != nil {
		return core.FederatedSearchResult{}, err
	}

	core.LoggerFromContext(ctx).Debug("federated es query", slog.String("body", string(b)))

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	res, err := c.es.Search(
		c.es.Search.WithContext(ctx),
		c.es.Search.WithIndex(indices...),
		c.es.Search.WithBody(bytes.NewReader(b)),
		c.es.Search.WithSearchType(federatedSearchType),
	)
	if err != nil {
		return core.FederatedSearchResult{}, err
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return core.FederatedSearchResult{}, nil
		}
		raw, _ := io.ReadAll(res.Body)
		return core.FederatedSearchResult{}, fmt.Errorf("es federated search error: %s %s", res.Status(), string(raw))
	}

	var decoded map[string]any
	if err := json.UnmarshalRead(res.Body, &decoded); err != nil {
		return core.FederatedSearchResult{}, err
	}

	result := core.FederatedSearchResult{IndexCounts: map[string]int64{}}

	hitsObj, _ := decoded["hits"].(map[string]any)
	if t, ok := hitsObj["total"].(map[string]any); ok {
		if v, ok := t["value"].(float64); ok {
			result.Total = int64(v)
		}
	}
	for _, h := range hitsObj["hits"].([]any) {
		m, _ := h.(map[string]any)
		index, _ := m["_index"].(string)
		id, _ := m["_id"].(string)
		score, _ := m["_score"].(float64)
		src, _ := m["_source"].(map[string]any)
		result.Hits = append(result.Hits, core.FederatedRawHit{
			Index:  index,
			ID:     id,
			Score:  score,
			Source: src,
		})
	}

	if aggs, ok := decoded["aggregations"].(map[string]any); ok {
		if perIndex, ok := aggs["per_index"].(map[string]any); ok {
			if buckets, ok := perIndex["buckets"].([]any); ok {
				for _, bk := range buckets {
					b, _ := bk.(map[string]any)
					key, _ := b["key"].(string)
					dc, _ := b["doc_count"].(float64)
					result.IndexCounts[key] = int64(dc)
				}
			}
		}
	}

	return result, nil
}

// buildFederatedTextQuery is the scoring core of a federated query: a document
// matches on its own primary text (search) OR on secondary text (search_scoped),
// with a query-time boost so a primary match outranks a secondary one (spec D13,
// D15). Each tier pairs a whole-word multi_match — whose .full standard-analyzed
// subfield is boosted for exact-token precision — with an unboosted infix clause
// (see infixClause), so any substring of the indexed text matches while
// whole-word hits stay on top.
func buildFederatedTextQuery(query, scope string) any {
	primary := map[string]any{
		"multi_match": map[string]any{
			"query":  query,
			"fields": []any{"search.full^9", "search^3"},
		},
	}
	return map[string]any{
		"bool": map[string]any{
			"should":               []any{primary, infixClause("search", query), buildSecondaryClause(query, scope)},
			"minimum_should_match": 1,
		},
	}
}

// infixClause gives a searchable surface "*query*" semantics: the query is
// shredded with the same n-gram analysis the index side uses, and every gram
// must be present (minimum_should_match 100%). A document containing the query
// as a substring necessarily contains all its grams, so any prefix/infix/suffix
// fragment matches regardless of its length — the plain multi_match only
// matches fragments that are whole words (via .full) or no longer than
// max_gram. AND-of-grams admits rare false positives (all grams present but
// scattered); acceptable for search (spec D15).
func infixClause(field, query string) any {
	return map[string]any{
		"match": map[string]any{
			field: map[string]any{
				"query":                query,
				"analyzer":             ngramIndexAnalyzer,
				"minimum_should_match": "100%",
			},
		},
	}
}

// buildFullTextQuery is the scoring core of a single-resource Search. It mirrors
// the primary tier of Federated Search (see buildFederatedTextQuery) but stops
// there: a Document matches on its own high-signal text — the standardized
// primary `search` surface — and nothing else. A whole-word multi_match (with
// the standard-analyzed .full subfield boosted for exact-token precision) is
// paired with an unboosted infix clause (see infixClause), so any substring of
// the indexed text matches while whole-word hits stay on top — e.g. "7135252"
// finds "adrcd-7135252-34". The secondary (search_scoped) tier that Federated
// Search also consults is intentionally excluded.
func buildFullTextQuery(query string) any {
	primary := map[string]any{
		"multi_match": map[string]any{
			"query":  query,
			"fields": []any{"search.full^9", "search^3"},
		},
	}
	return map[string]any{
		"bool": map[string]any{
			"should":               []any{primary, infixClause("search", query)},
			"minimum_should_match": 1,
		},
	}
}

// buildSecondaryClause builds the nested query over a Federated Search's
// secondary tier (the search_scoped nested field). It matches the query text
// against each entry's text the same two ways as the primary tier: whole words
// via the multi_match (with the standard-analyzed .full subfield boosted for
// whole-token precision) or any substring via infixClause (spec D15).
//
// When scope is non-empty it weaves a term on the same entry's scope[] keyword
// array into the same nested bool.must (spec D11.2, D14). Because a nested query
// is satisfied per entry, placing the text match and the scope term together
// correlates them: an entry contributes its text only when its own scope
// contains the caller — a top-level scope filter could not express this. A term
// on a keyword array matches when the array contains the value. An empty scope
// yields an unscoped secondary match (standalone-app behaviour).
func buildSecondaryClause(query, scope string) any {
	must := []any{
		map[string]any{
			"bool": map[string]any{
				"should": []any{
					map[string]any{
						"multi_match": map[string]any{
							"query":  query,
							"fields": []any{"search_scoped.text.full^3", "search_scoped.text"},
						},
					},
					infixClause("search_scoped.text", query),
				},
				"minimum_should_match": 1,
			},
		},
	}
	if scope != "" {
		must = append(must, map[string]any{
			"term": map[string]any{"search_scoped.scope": scope},
		})
	}
	return map[string]any{
		"nested": map[string]any{
			"path":  "search_scoped",
			"query": map[string]any{"bool": map[string]any{"must": must}},
		},
	}
}

// buildIndexFilterGroups assembles the per-index filter groups of a Federated
// Search into a single bool query. Each group becomes a should-clause that pins
// documents to that Type's read alias (a term on the _index metadata field) and
// applies that Type's harvested visibility filters; minimum_should_match: 1
// requires a document to satisfy exactly one group, so a Type's filters never
// constrain another Type's documents. The result is meant to sit in the outer
// query's filter (non-scoring) context.
func buildIndexFilterGroups(groups []core.IndexFilterGroup) (any, error) {
	should := make([]any, 0, len(groups))
	for _, g := range groups {
		if g.MatchNothing {
			// A reference filter for this Type resolved to zero children, so no
			// document of this Type can match. match_none never satisfies
			// minimum_should_match, excluding the Type while other groups return.
			should = append(should, map[string]any{"match_none": map[string]any{}})
			continue
		}
		clauses := []any{
			map[string]any{"term": map[string]any{"_index": g.Alias}},
		}
		for _, f := range g.Filters {
			if f.Field == "" {
				continue
			}
			clause, err := buildFilterClause(f)
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, clause)
		}
		should = append(should, map[string]any{
			"bool": map[string]any{"filter": clauses},
		})
	}
	return map[string]any{
		"bool": map[string]any{
			"should":               should,
			"minimum_should_match": 1,
		},
	}, nil
}

// rangeKeys maps range ops to their ES range-clause keys.
var rangeKeys = map[core.FilterOp]string{
	core.FilterOpGt:  "gt",
	core.FilterOpGte: "gte",
	core.FilterOpLt:  "lt",
	core.FilterOpLte: "lte",
}

// buildFilterClause translates one filter into an ES bool-filter clause.
// Assembly order matters for negation ops: the positive clause is wrapped in
// nested first (when NestedPath is set) and must_not second, so a negation on
// a nested field is document-level — "no child matches" — rather than "some
// child differs" (spec: Semantics / Negation on relations).
func buildFilterClause(f core.Filter) (any, error) {
	clause, err := buildOpClause(f)
	if err != nil {
		return nil, err
	}
	if f.NestedPath != "" {
		clause = map[string]any{
			"nested": map[string]any{"path": f.NestedPath, "query": clause},
		}
	}
	if f.Op.IsNegation() {
		clause = map[string]any{
			"bool": map[string]any{"must_not": []any{clause}},
		}
	}
	return clause, nil
}

// buildOpClause builds the positive form of a filter's op — a negation op
// maps to its positive counterpart's query (neq→term, not_in→terms,
// not_exists→exists); buildFilterClause adds the must_not.
func buildOpClause(f core.Filter) (any, error) {
	switch f.Op {
	case core.FilterOpEq, core.FilterOpNeq:
		if f.Value == "" {
			return nil, opError(f, "requires value")
		}
		return map[string]any{"term": map[string]any{f.Field: f.Value}}, nil

	case core.FilterOpIn, core.FilterOpNotIn:
		if len(f.Values) == 0 {
			return nil, opError(f, "requires values")
		}
		return map[string]any{"terms": map[string]any{f.Field: f.Values}}, nil

	case core.FilterOpGt, core.FilterOpGte, core.FilterOpLt, core.FilterOpLte:
		if f.Value == "" {
			return nil, opError(f, "requires value")
		}
		return map[string]any{"range": map[string]any{
			f.Field: map[string]any{rangeKeys[f.Op]: f.Value},
		}}, nil

	case core.FilterOpPrefix:
		if f.Value == "" {
			return nil, opError(f, "requires value")
		}
		return map[string]any{"prefix": map[string]any{
			f.Field: map[string]any{"value": f.Value},
		}}, nil

	case core.FilterOpSuffix:
		if f.Value == "" {
			return nil, opError(f, "requires value")
		}
		return map[string]any{"wildcard": map[string]any{
			f.Field: map[string]any{"value": "*" + escapeWildcard(f.Value)},
		}}, nil

	case core.FilterOpContains:
		if f.Value == "" {
			return nil, opError(f, "requires value")
		}
		return map[string]any{"wildcard": map[string]any{
			f.Field: map[string]any{"value": "*" + escapeWildcard(f.Value) + "*"},
		}}, nil

	case core.FilterOpExists, core.FilterOpNotExists:
		return map[string]any{"exists": map[string]any{"field": f.Field}}, nil

	default:
		return nil, fmt.Errorf("unsupported filter op for field %q", f.Field)
	}
}

func opError(f core.Filter, msg string) error {
	return fmt.Errorf("%s filter %s for field %q", f.Op, msg, f.Field)
}

// escapeWildcard backslash-escapes ES wildcard metacharacters so a
// user-supplied value always matches literally inside a wildcard query.
func escapeWildcard(s string) string {
	return strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`).Replace(s)
}
