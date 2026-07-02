package core

import "context"

// deriveNestedPath is a search middleware that fills in Filter.NestedPath for
// filters naming a many-cardinality denormalized relation field, so callers
// never have to know the Elasticsearch mapping shape.
//
// A many-cardinality denormalized relation is mapped as an ES "nested" object
// (see backend/elasticsearch/mapping.go), so a term/terms filter on one of its
// fields must be wrapped in a nested query keyed by the relation name. A
// one-cardinality relation is a plain "object" and needs no path; a reference
// relation stores no fields on the parent and is rerouted by referenceResolve;
// a root field ("fields.x") is never nested.
//
// It runs just before referenceResolve: reference filters are still present
// here but are skipped, and referenceResolve later strips them (clearing any
// NestedPath) — so ordering the two the other way round would be wrong.
//
// An explicitly-set NestedPath is left untouched, so a caller that knows the
// shape can always override the derivation.
func (idx *Indexer) deriveNestedPath(next SearchHandler) SearchHandler {
	return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
		cfg := idx.resources.Get(req.Resource)
		if cfg == nil {
			return next(ctx, req)
		}
		vc := cfg.ReadVersionConfig()
		if vc == nil {
			return next(ctx, req)
		}

		for i := range req.Filters {
			f := &req.Filters[i]
			if f.NestedPath != "" {
				continue // caller set it explicitly; don't override
			}
			relName, _, ok := splitRelationField(f.Field)
			if !ok {
				continue // root field ("fields.x") or unqualified — never nested
			}
			rel := vc.GetRelation(relName)
			if rel == nil || rel.IsReference() {
				continue // unknown relation, or reference (handled by referenceResolve)
			}
			if rel.IsMany() {
				f.NestedPath = relName
			}
		}

		return next(ctx, req)
	}
}
