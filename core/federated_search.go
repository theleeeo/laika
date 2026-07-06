package core

import (
	"context"
	"fmt"
)

// FederatedSearchRequest is the core-level federated search request: one query
// matched across a caller-supplied, non-empty set of Resource Types, returning
// a single relevance-ranked cross-type list (spec D2).
type FederatedSearchRequest struct {
	Query         string
	Resources     []string
	Filters       []Filter // global filters on root fields.* paths common to every Type (D7)
	Page          int32
	PageSize      int32
	IncludeSource bool
}

// FederatedHit is a single cross-type hit, tagged with the Resource Type it
// belongs to. Keyed by (Resource, ID) — ID alone is not globally unique.
type FederatedHit struct {
	Resource string
	ID       string
	Score    float64
	Source   map[string]any
}

// ResourceCount is the number of matching Documents of one Resource Type,
// returned alongside the ranked hits so a UI can show per-Type tallies (D12).
type ResourceCount struct {
	Resource string
	Count    int64
}

// FederatedSearchResponse is the result of a federated search.
type FederatedSearchResponse struct {
	Total  int64
	Hits   []FederatedHit
	Counts []ResourceCount
}

// FederatedSearchParams is the backend-level description of a federated query.
// Core resolves Resource Types into index/alias terms so the backend stays
// type-agnostic: it searches the FilterGroups' aliases as a single multi-index
// query and reports results keyed by concrete index, which core maps back to
// Types.
type FederatedSearchParams struct {
	Query          string
	Filters        []Filter           // global root fields.* filters, applied to all Types
	FilterGroups   []IndexFilterGroup // per-index visibility groups (collect-mode, spec D11.1)
	SecondaryScope string             // caller scope woven into the secondary tier (spec D11.2)
	Page           int32
	PageSize       int32
	IncludeSource  bool
}

// FederatedRawHit is a hit as the backend returns it, keyed by the concrete
// index it came from (not the Resource Type — core resolves that).
type FederatedRawHit struct {
	Index  string
	ID     string
	Score  float64
	Source map[string]any
}

// FederatedSearchResult is the backend-level federated result. IndexCounts maps
// each concrete index to its matching-document count; core folds those into
// per-Resource-Type counts.
type FederatedSearchResult struct {
	Total       int64
	Hits        []FederatedRawHit
	IndexCounts map[string]int64
}

// FederatedSearch runs one query across a caller-supplied set of Resource Types
// and returns a single relevance-ranked cross-type list (spec D3, D12, D13).
//
// It is a single multi-index query: per-Type document visibility is enforced by
// collecting each Type's middleware filters in-process (collectIndexFilterGroups)
// and combining them as per-index filter groups, not by fanning out one query
// per Type. The backend applies dfs_query_then_fetch so cross-type scores share
// global term statistics and are genuinely comparable.
func (idx *Indexer) FederatedSearch(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
	if len(req.Resources) == 0 {
		return FederatedSearchResponse{}, &InvalidArgumentError{Msg: "at least one resource is required"}
	}

	// Map every concrete version index of each requested Type back to its Type,
	// so hits and counts resolve correctly even across a read-alias cutover.
	indexToResource := make(map[string]string)
	for _, name := range req.Resources {
		r := idx.resources.Get(name)
		if r == nil {
			return FederatedSearchResponse{}, fmt.Errorf("%q: %w", name, ErrUnknownResource)
		}
		// A global filter must be valid on every requested Type (spec:
		// Request validation): unknown-anywhere or op-mismatch-anywhere is a
		// loud InvalidArgument naming the offending Type.
		if vc := r.ReadVersionConfig(); vc != nil {
			if err := validateRequestFilters(vc, req.Filters); err != nil {
				return FederatedSearchResponse{}, &InvalidArgumentError{
					Msg: fmt.Sprintf("resource %q: %v", name, err)}
			}
		}
		for _, v := range r.SortedVersions() {
			indexToResource[IndexName(name, v)] = name
		}
	}

	page, pageSize := normalizePaging(req.Page, req.PageSize)

	// Collect each Type's visibility filters + the caller's secondary scope by
	// re-running its single-resource middleware chain in collect mode.
	groups, scope, err := idx.collectIndexFilterGroups(ctx, req.Resources, SearchRequest{Query: req.Query})
	if err != nil {
		return FederatedSearchResponse{}, err
	}

	result, err := idx.es.FederatedSearch(ctx, FederatedSearchParams{
		Query:          req.Query,
		Filters:        req.Filters,
		FilterGroups:   groups,
		SecondaryScope: scope,
		Page:           page,
		PageSize:       pageSize,
		IncludeSource:  req.IncludeSource,
	})
	if err != nil {
		return FederatedSearchResponse{}, err
	}

	resp := FederatedSearchResponse{Total: result.Total}
	for _, h := range result.Hits {
		resp.Hits = append(resp.Hits, FederatedHit{
			Resource: indexToResource[h.Index],
			ID:       h.ID,
			Score:    h.Score,
			Source:   h.Source,
		})
	}

	// Fold per-index counts into per-Type counts, emitting one entry per
	// requested Type (including zeros) in request order for a stable response.
	countByResource := make(map[string]int64, len(req.Resources))
	for index, c := range result.IndexCounts {
		if name, ok := indexToResource[index]; ok {
			countByResource[name] += c
		}
	}
	for _, name := range req.Resources {
		resp.Counts = append(resp.Counts, ResourceCount{Resource: name, Count: countByResource[name]})
	}

	return resp, nil
}

// IndexFilterGroup is one leg of a Federated Search's per-index filter groups:
// a Resource Type's read alias paired with the visibility Filters harvested
// from its single-resource middleware chain. The federated query builder scopes
// each group's Filters to its Alias and combines the groups in a
// bool.filter.should with minimum_should_match: 1, so a Document matches only
// when it belongs to a requested Type and satisfies that Type's filters.
type IndexFilterGroup struct {
	Resource string
	Alias    string
	Filters  []Filter
	// MatchNothing marks a group that can match no document — e.g. a reference
	// filter resolved to zero children. The query builder emits a clause that
	// never satisfies minimum_should_match, excluding this Type while other
	// Types in the federation still return.
	MatchNothing bool
}

// collectIndexFilterGroups enforces per-Type document visibility for a Federated
// Search without fanning out into one ES query per Type. For each requested
// Type it re-runs that Type's consumer search middleware chain in collect-only
// mode: the chain still enforces access (a middleware may fail closed by
// returning an error) but a terminal handler captures the Filters the chain
// appends instead of hitting the backend. The captured Filters, tagged with the
// Type's read alias, become an IndexFilterGroup.
//
// The internal query-shaping middlewares (deriveNestedPath, referenceResolve)
// are deliberately excluded from the collect chain: they resolve single-resource
// query mechanics, whereas the chain only needs to run the consumer's
// authorization and scoping. Reference-relation filters, however, cannot be
// expressed as a term on the multi-index query (the referenced field is not
// mapped on the parent), so after collecting each Type's filters this resolves
// them the same way referenceResolve does — a separate child search per
// reference, folded into a terms filter on the parent key. A reference that
// matches no children marks the group MatchNothing so that Type is excluded
// while the rest of the federation still returns.
//
// Core stays policy-free: it cannot classify a middleware error as "denied"
// versus "failed", so any error propagates and fails the whole Federated Search
// closed. A consumer that instead wants a denied Type merely excluded (its
// documents contributing nothing while other Types still return) fails closed by
// appending a match-nothing filter rather than returning an error, yielding an
// empty group for that Type.
//
// The same collect pass also harvests the caller's secondary scope value (spec
// D11.2, D14): a consumer middleware sets req.SecondaryScope from the caller
// identity, which is the caller's tenant regardless of Type, so it is a single
// value across all groups. The last non-empty value seen is returned; empty
// means unscoped secondary. The Federated Search query builder weaves it into
// the nested search_scoped clause.
func (idx *Indexer) collectIndexFilterGroups(ctx context.Context, resources []string, base SearchRequest) ([]IndexFilterGroup, string, error) {
	groups := make([]IndexFilterGroup, 0, len(resources))
	var scope string
	for _, name := range resources {
		r := idx.resources.Get(name)
		if r == nil {
			return nil, "", fmt.Errorf("%q: %w", name, ErrUnknownResource)
		}

		req := base
		req.Resource = name
		req.Filters = nil       // start clean; the chain appends this Type's own filters
		req.SecondaryScope = "" // start clean; the chain sets the caller's scope

		filters, entryScope, err := idx.collectFilters(ctx, req)
		if err != nil {
			return nil, "", fmt.Errorf("collect filters for %q: %w", name, err)
		}
		if entryScope != "" {
			scope = entryScope
		}

		// The collect chain excludes referenceResolve, so a filter naming a
		// reference relation's field is still unresolved here. Resolve it now —
		// issuing the same separate child search the single-resource path does —
		// so the group carries a terms filter on the parent key (a mapped field)
		// rather than a term on the reference field (never mapped on the parent).
		resolved, matchedNothing, err := idx.resolveReferenceFilters(ctx, name, filters)
		if err != nil {
			return nil, "", fmt.Errorf("resolve reference filters for %q: %w", name, err)
		}

		group := IndexFilterGroup{Resource: name, Alias: AliasName(r.Resource)}
		if matchedNothing {
			group.MatchNothing = true
		} else {
			group.Filters = resolved
		}
		groups = append(groups, group)
	}
	return groups, scope, nil
}

// collectFilters runs the consumer middleware chain around a collect-only
// terminal that captures the request's Filters and SecondaryScope instead of
// searching. Any error raised by the chain (enforcement failing closed) is
// returned to the caller.
func (idx *Indexer) collectFilters(ctx context.Context, req SearchRequest) ([]Filter, string, error) {
	var (
		collected []Filter
		scope     string
	)
	terminal := func(_ context.Context, r SearchRequest) (SearchResponse, error) {
		collected = r.Filters
		scope = r.SecondaryScope
		return SearchResponse{}, nil
	}
	handler := chainSearch(terminal, idx.searchMiddlewares)
	if _, err := handler(ctx, req); err != nil {
		return nil, "", err
	}
	return collected, scope, nil
}
