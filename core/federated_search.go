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

		groups = append(groups, IndexFilterGroup{
			Resource: name,
			Alias:    AliasName(r.Resource),
			Filters:  filters,
		})
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
