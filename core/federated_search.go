package core

import (
	"context"
	"fmt"
)

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
// are deliberately excluded: they resolve single-resource query mechanics
// (referenceResolve even issues its own ES searches), whereas collection only
// needs the consumer's authorization and scoping. Federated query building
// applies its own mapping-aware shaping to the collected Filters.
//
// Core stays policy-free: it cannot classify a middleware error as "denied"
// versus "failed", so any error propagates and fails the whole Federated Search
// closed. A consumer that instead wants a denied Type merely excluded (its
// documents contributing nothing while other Types still return) fails closed by
// appending a match-nothing filter rather than returning an error, yielding an
// empty group for that Type.
func (idx *Indexer) collectIndexFilterGroups(ctx context.Context, resources []string, base SearchRequest) ([]IndexFilterGroup, error) {
	groups := make([]IndexFilterGroup, 0, len(resources))
	for _, name := range resources {
		r := idx.resources.Get(name)
		if r == nil {
			return nil, fmt.Errorf("%q: %w", name, ErrUnknownResource)
		}

		req := base
		req.Resource = name
		req.Filters = nil // start clean; the chain appends this Type's own filters

		filters, err := idx.collectFilters(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("collect filters for %q: %w", name, err)
		}

		groups = append(groups, IndexFilterGroup{
			Resource: name,
			Alias:    AliasName(r.Resource),
			Filters:  filters,
		})
	}
	return groups, nil
}

// collectFilters runs the consumer middleware chain around a collect-only
// terminal that captures the request's Filters instead of searching. Any error
// raised by the chain (enforcement failing closed) is returned to the caller.
func (idx *Indexer) collectFilters(ctx context.Context, req SearchRequest) ([]Filter, error) {
	var collected []Filter
	terminal := func(_ context.Context, r SearchRequest) (SearchResponse, error) {
		collected = r.Filters
		return SearchResponse{}, nil
	}
	handler := chainSearch(terminal, idx.searchMiddlewares)
	if _, err := handler(ctx, req); err != nil {
		return nil, err
	}
	return collected, nil
}
