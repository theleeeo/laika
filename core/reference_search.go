package core

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/theleeeo/laika/core/resource"
)

// referenceTarget is a child search the reference resolver derives from a
// parent request: search the child Resource with Filters, collect each hit's
// ForeignField value, then fold those values into a terms filter on the
// parent's ParentKeyField (wrapped in ParentKeyNestedPath when non-empty).
type referenceTarget struct {
	Resource            string
	ForeignField        string
	ParentKeyField      string
	ParentKeyNestedPath string
	Filters             []Filter
}

// referenceResolve is the innermost search middleware (runs just above
// searchBase, after every user middleware). It is the only place reference
// relations are handled, so nothing reference-related happens before the user
// middlewares run.
//
// It routes each request filter by its field path, opaquely to the relation's
// strategy:
//
//   - a filter naming a reference relation's field ("b.name", "b.tenant_id") is
//     extracted onto that child's search (rewritten to "fields.<field>");
//   - every other filter — root fields ("fields.tenant_id") and denormalized
//     relation fields ("b.name" when "b" is denormalize) — stays on the primary.
//
// Each child search is then executed and folded into a terms filter on the
// parent key before the primary search runs.
//
// It reads idx.resources at call time so that SetPlans updates are visible
// without recomposing the search chain.
func (idx *Indexer) referenceResolve(next SearchHandler) SearchHandler {
	return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
		resolved, matchedNothing, err := idx.resolveReferenceFilters(ctx, req.Resource, req.Filters, req.Scope)
		if err != nil {
			return SearchResponse{}, err
		}
		if matchedNothing {
			// No child matched a reference filter, so no parent can match.
			return SearchResponse{}, nil
		}
		req.Filters = resolved
		return next(ctx, req)
	}
}

// resolveReferenceFilters turns reference-relation filters for resource into
// filters the target index can answer directly. It partitions filters by field
// path — a filter naming a reference relation's field is extracted onto that
// child's search, everything else (root fields, denormalized relation fields)
// stays on the primary — then executes each child search and folds its matching
// join keys into a terms filter on the parent key. The returned slice is the
// primary filters plus those folded terms filters.
//
// matchedNothing reports that some reference relation matched no children, which
// means no parent document can satisfy that filter. Callers decide what that
// means: single-resource Search returns an empty result, while Federated Search
// excludes just that Type (its group matches nothing) so other Types still
// return. In that case the returned filter slice is unspecified.
//
// scope is the caller's tenant value, threaded onto each child SearchRequest
// unchanged from the parent's request Scope: the child search is answering
// the same caller, so a multi-tenant referenced resource (one declaring a
// scoped nested block) must be scoped identically to the primary search,
// rather than defaulting to empty and fail-closing to zero hits (see
// buildScopedNestedClause in backend/elasticsearch/search.go). A single-tenant
// child ignores Scope, so passing it through is inert there.
//
// A resource with no read version config resolves to its filters unchanged.
// idx.resources is read at call time so SetPlans updates need no chain rebuild.
func (idx *Indexer) resolveReferenceFilters(ctx context.Context, resourceName string, filters []Filter, scope string) (resolved []Filter, matchedNothing bool, err error) {
	logger := LoggerFromContext(ctx)
	cfg := idx.resources.Get(resourceName)
	if cfg == nil {
		return filters, false, nil
	}
	vc := cfg.ReadVersionConfig()
	if vc == nil {
		return filters, false, nil
	}

	logger.Debug("reference resolve: start", slog.Int("filter_count", len(filters)))

	// Partition filters by field path: reference-relation filters become
	// child targets; everything else stays on the primary.
	var primary []Filter
	targets := map[string]*referenceTarget{}
	for _, f := range filters {
		relName, field, isPath := splitRelationField(f.Field)
		var rel *resource.RelationConfig
		if isPath {
			rel = vc.GetRelation(relName)
		}
		isReference := rel != nil && rel.IsReference()

		if isReference {
			tgt := targets[relName]
			if tgt == nil {
				tgt = newReferenceTarget(*rel, vc)
				targets[relName] = tgt
			}
			childFilter := f
			childFilter.Field = "fields." + field
			childFilter.NestedPath = ""
			tgt.Filters = append(tgt.Filters, childFilter)
			logger.Debug("reference resolve: filter routed",
				slog.String("field", f.Field),
				slog.String("rel_name", relName),
				slog.Bool("is_relation", rel != nil),
				slog.Bool("is_reference", true),
				slog.String("dest", "child:"+relName),
			)
			continue
		}

		logger.Debug("reference resolve: filter routed",
			slog.String("field", f.Field),
			slog.String("rel_name", relName),
			slog.Bool("is_relation", rel != nil),
			slog.Bool("is_reference", false),
			slog.String("dest", "primary"),
		)
		primary = append(primary, f)
	}
	resolved = primary

	// Resolve each child target into a terms filter on the parent key.
	// Iterate config order (not map order) so behavior is deterministic.
	for _, rel := range vc.Relations {
		tgt, ok := targets[rel.Resource]
		if !ok {
			continue
		}

		childCfg := idx.resources.Get(tgt.Resource)
		if childCfg == nil {
			return nil, false, ErrUnknownResource
		}

		logger.Debug("reference resolve: child search",
			slog.String("child_resource", tgt.Resource),
			slog.String("foreign_field", tgt.ForeignField),
			slog.String("parent_key_field", tgt.ParentKeyField),
			slog.String("parent_key_nested_path", tgt.ParentKeyNestedPath),
			slog.Any("child_filters", summarizeFilters(tgt.Filters)),
		)

		// The child search requests PageSize: maxReferenceTerms. The logic below
		// assumes the backend returns all matching hits (len(Hits) == Total) up to
		// that ceiling. A backend that caps its page size below maxReferenceTerms
		// would under-populate the terms filter silently.
		childResp, err := idx.es.Search(ctx, SearchRequest{
			Resource: tgt.Resource,
			Filters:  tgt.Filters,
			PageSize: maxReferenceTerms,
			Scope:    scope,
		}, AliasName(tgt.Resource), childCfg.ReadVersionConfig())
		if err != nil {
			return nil, false, err
		}

		logger.Debug("reference resolve: child result",
			slog.String("child_resource", tgt.Resource),
			slog.Int64("total", childResp.Total),
			slog.Int("hit_count", len(childResp.Hits)),
		)

		if childResp.Total > maxReferenceTerms {
			logger.Warn("reference resolve: child exceeds term ceiling",
				slog.String("child_resource", tgt.Resource),
				slog.Int64("total", childResp.Total),
				slog.Int("ceiling", maxReferenceTerms),
			)
			return nil, false, &InvalidArgumentError{Msg: fmt.Sprintf(
				"reference relation to %q matched %d children, exceeding the %d-term ceiling; this child is not low-count enough for strategy: reference",
				tgt.Resource, childResp.Total, maxReferenceTerms)}
		}

		values := make([]string, 0, len(childResp.Hits))
		for _, h := range childResp.Hits {
			if v := foreignValue(h, tgt.ForeignField); v != "" {
				values = append(values, v)
			}
		}
		if len(values) < len(childResp.Hits) {
			logger.Warn("reference resolve: some child hits yielded no join key",
				slog.String("child_resource", tgt.Resource),
				slog.String("foreign_field", tgt.ForeignField),
				slog.Int("hit_count", len(childResp.Hits)),
				slog.Int("value_count", len(values)),
			)
		}
		if len(values) == 0 {
			// No child matches -> no parent can match this reference.
			logger.Debug("reference resolve: no child matches, short-circuiting",
				slog.String("child_resource", tgt.Resource),
			)
			return nil, true, nil
		}

		resolved = append(resolved, Filter{
			Field:      tgt.ParentKeyField,
			Op:         FilterOpIn,
			Values:     values,
			NestedPath: tgt.ParentKeyNestedPath,
		})
		logger.Debug("reference resolve: folded terms filter",
			slog.String("parent_key_field", tgt.ParentKeyField),
			slog.String("nested_path", tgt.ParentKeyNestedPath),
			slog.Int("value_count", len(values)),
		)
	}

	return resolved, false, nil
}

// splitRelationField splits "rel.field" into ("rel", "field", true). A field
// already under "fields." (a root field) or with no dot returns ok=false.
// Note: a head segment of "fields" is treated as a root-field path, so a
// relation literally named "fields" would be unreachable via this split.
func splitRelationField(path string) (string, string, bool) {
	i := strings.IndexByte(path, '.')
	if i <= 0 {
		return "", "", false
	}
	head := path[:i]
	if head == "fields" {
		return "", "", false
	}
	return head, path[i+1:], true
}

// newReferenceTarget derives the child-search identity and the parent key path
// from a reference relation. The parent key lives at fields.<local> when read
// from the root, or <from>.<local> (nested when the sibling is a many relation)
// when sourced from a denormalized sibling.
func newReferenceTarget(rel resource.RelationConfig, vc *resource.VersionConfig) *referenceTarget {
	tgt := &referenceTarget{
		Resource:     rel.Resource,
		ForeignField: rel.Join.Foreign,
	}
	if rel.Join.From == "" {
		tgt.ParentKeyField = "fields." + rel.Join.Local
		return tgt
	}
	tgt.ParentKeyField = rel.Join.From + "." + rel.Join.Local
	if sib := vc.GetRelation(rel.Join.From); sib != nil && sib.IsMany() {
		tgt.ParentKeyNestedPath = rel.Join.From
	}
	return tgt
}

// maxReferenceTerms bounds how many child IDs a reference target may fold into
// a terms filter. Reference is chosen for low-count children, so this ceiling is
// generous; exceeding it is a misconfiguration and fails loudly rather than
// silently truncating. Stays well under Elasticsearch's 65536 terms limit.
const maxReferenceTerms = 10000

// foreignValue extracts the child's join value for a hit. The common case is
// foreign == "id" (the document id); otherwise it reads fields.<foreign> from
// the source.
func foreignValue(h SearchHit, foreign string) string {
	if foreign == "id" {
		return h.ID
	}
	if h.Source == nil {
		return ""
	}
	fields, _ := h.Source["fields"].(map[string]any)
	if fields == nil {
		return ""
	}
	v, _ := fields[foreign].(string)
	return v
}
