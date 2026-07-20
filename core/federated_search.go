package core

import (
	"context"
	"fmt"
	"strings"
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

	// ResourceFilters are per-Type visibility filters, keyed by Resource Type
	// name — the channel a trusted federated middleware scopes each Type
	// through. They are core-API-only (never exposed over proto) and exempt
	// from strict request-time filter validation: the same trust model as
	// middleware-appended filters on the single-resource path. Reference-
	// relation paths are resolved via child searches (see
	// buildIndexFilterGroups); a Type with no entry gets an unfiltered group
	// (Type membership only). Denial is the middleware's concern: fail the
	// whole search with an error, or drop the Type from Resources to exclude
	// just it.
	ResourceFilters map[string][]Filter

	// SecondaryScope is the caller's single tenant scope value for the
	// secondary tier (spec D11.2, D14), set by a trusted federated middleware.
	// The query builder weaves it into the nested search_secondary clause. Empty
	// means unscoped secondary (standalone-app behaviour).
	SecondaryScope string
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

// FederatedSearch runs one query across a caller-supplied set of Resource
// Types through the registered federated middleware chain (see
// Config.FederatedSearchMiddlewares). The base handler validates, builds
// per-index filter groups, and queries the backend; see federatedSearchBase.
func (idx *Indexer) FederatedSearch(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
	return idx.federatedSearchChain(ctx, req)
}

// federatedSearchBase runs one query across a caller-supplied set of Resource Types
// and returns a single relevance-ranked cross-type list (spec D3, D12, D13).
//
// It is a single multi-index query: per-Type document visibility is enforced by
// the per-Type filters a federated middleware supplies on the request
// (ResourceFilters), combined as per-index filter groups (buildIndexFilterGroups),
// not by fanning out one query per Type. The backend applies dfs_query_then_fetch
// so cross-type scores share global term statistics and are genuinely comparable.
func (idx *Indexer) federatedSearchBase(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
	if len(req.Resources) == 0 {
		return FederatedSearchResponse{}, &InvalidArgumentError{Msg: "at least one resource is required"}
	}

	// Global filters are root-only (fields.*, spec D7): they are applied raw
	// to the multi-index query, never routed through deriveNestedPath or
	// referenceResolve, so a relation-field path would silently match nothing
	// (or, negated, everything) instead of what capabilities advertise.
	for _, f := range req.Filters {
		if f.Field == "" {
			continue
		}
		if !strings.HasPrefix(f.Field, "fields.") {
			return FederatedSearchResponse{}, &InvalidArgumentError{Msg: fmt.Sprintf(
				"federated filter field %q: global filters must target root fields (fields.*)", f.Field)}
		}
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

	// Build each Type's visibility group from the per-Type filters a federated
	// middleware supplied on the request (ResourceFilters).
	groups, err := idx.buildIndexFilterGroups(ctx, req.Resources, req.ResourceFilters)
	if err != nil {
		return FederatedSearchResponse{}, err
	}

	result, err := idx.es.FederatedSearch(ctx, FederatedSearchParams{
		Query:          req.Query,
		Filters:        req.Filters,
		FilterGroups:   groups,
		SecondaryScope: req.SecondaryScope,
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
// a Resource Type's read alias paired with the visibility Filters a federated
// middleware supplied for it (FederatedSearchRequest.ResourceFilters, reference
// paths resolved). The federated query builder scopes
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

// buildIndexFilterGroups builds a Federated Search's per-index visibility
// groups from the per-Type filters supplied on the request. For each requested
// Type it resolves reference-relation filter paths the same way the
// single-resource path does — a separate child search per reference, folded
// into a terms filter on the parent key (the referenced field is never mapped
// on the parent, so it cannot be a term on the multi-index query) — and tags
// the result with the Type's read alias. A reference that matches no children
// marks the group MatchNothing so that Type is excluded while other Types in
// the federation still return. A Type with no perType entry gets an unfiltered
// group (Type membership only).
//
// Nested-path derivation (deriveNestedPath) is deliberately not applied here,
// matching the former collect mode: per-Type filters targeting
// denormalized-many nested fields are unsupported on the federated path.
func (idx *Indexer) buildIndexFilterGroups(ctx context.Context, resources []string, perType map[string][]Filter) ([]IndexFilterGroup, error) {
	groups := make([]IndexFilterGroup, 0, len(resources))
	for _, name := range resources {
		r := idx.resources.Get(name)
		if r == nil {
			return nil, fmt.Errorf("%q: %w", name, ErrUnknownResource)
		}

		resolved, matchedNothing, err := idx.resolveReferenceFilters(ctx, name, perType[name])
		if err != nil {
			return nil, fmt.Errorf("resolve reference filters for %q: %w", name, err)
		}

		group := IndexFilterGroup{Resource: name, Alias: AliasName(r.Resource)}
		if matchedNothing {
			group.MatchNothing = true
		} else {
			group.Filters = resolved
		}
		groups = append(groups, group)
	}
	return groups, nil
}
