package dsl

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/app/source"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// BuildPlansFromConfig constructs aggregation plans for each resource type
// and version in the config. This is the default plan builder used by the standalone binary.
// Library users can build their own plans and pass them to NewBuilder directly.
func BuildPlansFromConfig(provider source.Provider, resources resource.Configs) map[string][]projection.Plan {
	// Reverse map for parent discovery, derived once from the join declarations
	// across all resources. Each root plan populates BuildDoc.Parents from it.
	reverse := buildReverseMap(resources)

	plans := make(map[string][]projection.Plan, len(resources))
	for _, rCfg := range resources {
		versionPlans := make([]projection.Plan, len(rCfg.Versions))
		for i, vc := range rCfg.Versions {
			versionPlans[i] = buildPlanForVersion(provider, rCfg.Resource, &vc, reverse[rCfg.Resource])
		}
		plans[rCfg.Resource] = versionPlans
	}
	return plans
}

// buildPlanForVersion creates a RootPlan for the resource version and chains SubPlans
// for each relation in topological order. parentRefs drives reverse-relation
// discovery: the root plan derives BuildDoc.Parents from the root's own data.
func buildPlanForVersion(provider source.Provider, resourceName string, vc *resource.VersionConfig, parentRefs []parentRef) projection.Plan {
	// Root plan: fetches the root resources and initialises their BuildDocs.
	// When ResourceIDs is empty, the plan lists all resources of the type with
	// pagination via provider.ListResources. When set, it answers every asked
	// id exactly once, in asked order: with its document, with a nil Doc when
	// the provider has no data for it, or with Err when its read fails (ADR
	// 0014). After fetching, it derives the Parents to bootstrap from each
	// root's own data (see ADR 0006). An item with a nil Doc, whether a nil or
	// an Err, has no resolved data, so it derives no Parents, and every later
	// stage passes it through untouched.
	plan := aggregation.Root(func(ctx context.Context, params aggregation.FetchParameters[projection.BuildRequest]) (aggregation.FetchResult[projection.BuildDoc], error) {
		var result aggregation.FetchResult[projection.BuildDoc]
		var err error
		if len(params.Request.ResourceIDs) == 0 {
			result, err = fetchAllResources(ctx, provider, resourceName, vc.Fields, params)
		} else {
			result, err = fetchResourcesByID(ctx, provider, resourceName, vc.Fields, params)
		}
		if err != nil {
			return result, err
		}

		for i := range result.Items {
			result.Items[i].Parents = deriveParents(parentRefs, result.Items[i].Resolved[resourceName])
		}
		return result, nil
	})

	// Extend the plan with a relation stage for each relation, in topological
	// order. Resolve that order first: if the config is invalid the plan will
	// never execute successfully, but we defer the error to execution time
	// rather than panicking at startup so that validation can catch it first —
	// the root plan alone (no relations) becomes the chain.
	// TODO: Error here? Or at least log it so it's not silent?
	if ordered, err := resolveOrder(vc.Relations); err == nil {
		for _, rel := range ordered {
			if rel.IsReference() {
				// reference relations are resolved at search time; never fetched
				// or denormalized into the document.
				continue
			}
			plan = plan.Sub(newRelationFetcher(provider, rel), relationBuilder(rel))
		}
	}

	// Terminal stage: populate the standardized search surfaces from the
	// per-field tier selectors, once every relation is denormalized into Doc.
	return projection.Plan{
		Version: vc.Version,
		Executer: plan.Map(func(d projection.BuildDoc) projection.BuildDoc {
			return populateStandardizedSearchFields(vc, d)
		}),
	}
}

// relationBuilder returns the build stage for a single relation: it folds the
// resources the fetcher returned into the parent document. A nil fetch result
// means the relation resolved to nothing, and the document is left untouched,
// as is a parent with a nil Doc (a nil or an Err item).
func relationBuilder(rel resource.RelationConfig) func(projection.BuildDoc, *fetchedRelation) projection.BuildDoc {
	// TODO: There are a bunch of things here that can go wrong, like missing id, incorrect types, etc. Handle that better.
	return func(parentDoc projection.BuildDoc, fr *fetchedRelation) projection.BuildDoc {
		if parentDoc.Doc == nil || fr == nil {
			return parentDoc
		}

		for _, r := range fr.Related {
			if r.ID == "" {
				continue
			}
			parentDoc.Relations = append(parentDoc.Relations, model.Resource{Type: fr.ResourceType, Id: r.ID})
		}

		// Update the resolved map so downstream relations can reference this data.
		resolvedData := make([]map[string]any, len(fr.Related))
		for i, r := range fr.Related {
			resolvedData[i] = r.Data
		}
		parentDoc.Resolved[rel.Resource] = resolvedData

		subResources := make([]map[string]any, 0, len(fr.Related))
		for _, r := range fr.Related {
			filtered := filterFields(r.Data, rel.Fields)
			if r.ID != "" {
				filtered["id"] = r.ID
			}
			subResources = append(subResources, filtered)
		}

		// Shape the indexed document per the relation's cardinality. A "many"
		// relation is an array of objects (ES nested); a "one" relation is a
		// single object (ES object). For "one" we take the first related row
		// and omit the key entirely when there are no related rows, so the
		// indexed shape matches the generated mapping.
		if rel.IsMany() {
			parentDoc.Doc[rel.Resource] = subResources
		} else if len(subResources) > 0 {
			slog.Warn("relation cardinality is one but multiple related rows were returned; using the first row", "resource", parentDoc.Root.Type, "relation", rel.Resource, "count", len(subResources))
			parentDoc.Doc[rel.Resource] = subResources[0]
		}
		return parentDoc
	}
}

func filterFields(data map[string]any, fields []resource.FieldConfig) map[string]any {
	result := make(map[string]any, len(fields))
	for _, f := range fields {
		if v, ok := data[f.Name]; ok {
			result[f.Name] = v
		}
	}
	return result
}

// resolveOrder topologically sorts relations so that dependencies (relations
// whose local field comes from a sibling relation via Join.From) are resolved
// before their dependants.
func resolveOrder(relations []resource.RelationConfig) ([]resource.RelationConfig, error) {
	byResource := make(map[string]resource.RelationConfig, len(relations))
	for _, r := range relations {
		byResource[r.Resource] = r
	}

	var ordered []resource.RelationConfig
	visited := make(map[string]bool)
	inStack := make(map[string]bool)

	var visit func(rel resource.RelationConfig) error
	visit = func(rel resource.RelationConfig) error {
		if inStack[rel.Resource] {
			return fmt.Errorf("cycle detected involving %q", rel.Resource)
		}
		if visited[rel.Resource] {
			return nil
		}
		inStack[rel.Resource] = true

		if rel.Join.From != "" {
			dep, ok := byResource[rel.Join.From]
			if !ok {
				return fmt.Errorf("join from %q not found among relations", rel.Join.From)
			}
			if err := visit(dep); err != nil {
				return err
			}
		}

		inStack[rel.Resource] = false
		visited[rel.Resource] = true
		ordered = append(ordered, rel)
		return nil
	}

	for _, rel := range relations {
		if err := visit(rel); err != nil {
			return nil, err
		}
	}

	return ordered, nil
}

// fetchResourcesByID answers every asked id, in asked order, with one
// FetchResource each, until the provider has a by-ids read (L5.4). An id the
// provider has no data for is answered with an explicit nil Doc, its
// version's nil (ADR 0013). A read that fails answers its id alone with
// BuildDoc.Err, carrying nothing but Root, and the other ids are still read
// (ADR 0014). A read that fails because ctx is done fails the execution
// instead: the build was cancelled, not the id.
func fetchResourcesByID(
	ctx context.Context,
	provider source.Provider,
	resourceName string,
	fields []resource.FieldConfig,
	params aggregation.FetchParameters[projection.BuildRequest],
) (aggregation.FetchResult[projection.BuildDoc], error) {
	items := make([]projection.BuildDoc, 0, len(params.Request.ResourceIDs))
	for _, id := range params.Request.ResourceIDs {
		root := model.Resource{Type: params.Request.ResourceType, Id: id}

		data, err := provider.FetchResource(ctx, source.FetchResourceParams{
			ResourceType: params.Request.ResourceType,
			ResourceID:   id,
			Metadata:     params.Request.Metadata,
		})
		if err != nil {
			err = fmt.Errorf("fetch resource %s/%s: %w", params.Request.ResourceType, id, err)
			if ctx.Err() != nil {
				return aggregation.FetchResult[projection.BuildDoc]{}, err
			}
			items = append(items, projection.BuildDoc{Root: root, Err: err})
			continue
		}

		if data.Data == nil {
			items = append(items, projection.BuildDoc{
				Root:     root,
				Metadata: params.Request.Metadata,
			})
			continue
		}

		items = append(items, projection.BuildDoc{
			Doc: map[string]any{
				"fields": filterFields(data.Data, fields),
			},
			Resolved: map[string][]map[string]any{
				resourceName: {data.Data},
			},
			Root:     root,
			Metadata: params.Request.Metadata,
		})
	}
	return aggregation.FetchResult[projection.BuildDoc]{Items: items}, nil
}

// defaultListPageSize is an all-of-type walk's listing page size when its
// request names none (projection.BuildRequest.PageSize 0).
const defaultListPageSize = 100

// fetchAllResources lists resources of a type with pagination and returns a
// page of BuildDocs. The NextPageToken from the provider is passed through so
// that the RootPlan's pagination loop keeps calling until exhausted.
func fetchAllResources(
	ctx context.Context,
	provider source.Provider,
	resourceName string,
	fields []resource.FieldConfig,
	params aggregation.FetchParameters[projection.BuildRequest],
) (aggregation.FetchResult[projection.BuildDoc], error) {
	// The first fetch honors a caller-supplied start token (a resumed rebuild
	// walk); after that the provider's own next-page tokens drive the loop.
	pageToken := params.Request.PageToken
	if params.NextPageToken != nil {
		pageToken = params.NextPageToken.(string)
	}

	// A paced walk sets the request's page size so its rate budget counts
	// the pages it asked for; otherwise the plan lists 100 at a time.
	pageSize := int32(defaultListPageSize)
	if params.Request.PageSize > 0 {
		pageSize = int32(params.Request.PageSize)
	}

	resp, err := provider.ListResources(ctx, source.ListResourcesParams{
		ResourceType: params.Request.ResourceType,
		PageToken:    pageToken,
		PageSize:     pageSize,
		Metadata:     params.Request.Metadata,
	})
	if err != nil {
		return aggregation.FetchResult[projection.BuildDoc]{}, fmt.Errorf("list resources %s: %w", params.Request.ResourceType, err)
	}

	items := make([]projection.BuildDoc, 0, len(resp.Resources))
	for _, r := range resp.Resources {
		if r.Data == nil {
			continue
		}
		filtered := filterFields(r.Data, fields)
		items = append(items, projection.BuildDoc{
			Doc: map[string]any{
				"fields": filtered,
			},
			Resolved: map[string][]map[string]any{
				resourceName: {r.Data},
			},
			Root:     model.Resource{Type: params.Request.ResourceType, Id: r.ID},
			Metadata: params.Request.Metadata,
		})
	}

	var npt any
	if resp.NextPageToken != "" {
		npt = resp.NextPageToken
	}

	return aggregation.FetchResult[projection.BuildDoc]{
		Items:         items,
		NextPageToken: npt,
	}, nil
}
