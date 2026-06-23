package dsl

import (
	"context"
	"fmt"

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
	plans := make(map[string][]projection.Plan, len(resources))
	for _, rCfg := range resources {
		versionPlans := make([]projection.Plan, len(rCfg.Versions))
		for i, vc := range rCfg.Versions {
			versionPlans[i] = buildPlanForVersion(provider, rCfg.Resource, &vc)
		}
		plans[rCfg.Resource] = versionPlans
	}
	return plans
}

// buildPlanForVersion creates a RootPlan for the resource version and chains SubPlans
// for each relation in topological order.
func buildPlanForVersion(provider source.Provider, resourceName string, vc *resource.VersionConfig) projection.Plan {
	// Root plan: fetches the root resource and initialises the BuildDoc.
	// When ResourceID is empty, the plan lists all resources of the type with
	// pagination via provider.ListResources. When set, it fetches a single
	// resource as before.
	rootPlan := aggregation.NewRootPlan(func(params aggregation.FetchParameters[projection.BuildRequest]) (aggregation.FetchResult[projection.BuildDoc], error) {
		if params.Request.ResourceID == "" {
			return fetchAllResources(provider, resourceName, vc.Fields, params)
		}
		return fetchSingleResource(provider, resourceName, vc.Fields, params)
	})

	// Resolve the topological order of relations.
	ordered, err := resolveOrder(resourceName, vc.Relations)
	if err != nil {
		// If the config is invalid the plan will never execute successfully,
		// but we defer the error to execution time rather than panicking at
		// startup so that validation can catch it first.
		// TODO: Error here? Or at least log it so it's not silent?
		return projection.Plan{
			Version:  vc.Version,
			Executer: rootPlan,
		}
	}

	// Chain a SubPlan for each relation.
	var current aggregation.Executer[projection.BuildRequest, projection.BuildDoc] = rootPlan
	for _, rel := range ordered {
		current = buildRelationSubPlan(provider, current, rel)
	}

	return projection.Plan{
		Version:  vc.Version,
		Executer: current,
	}
}

// buildRelationSubPlan creates a SubPlan for a single relation.
func buildRelationSubPlan(
	provider source.Provider,
	parent aggregation.Executer[projection.BuildRequest, projection.BuildDoc],
	rel resource.RelationConfig,
) *aggregation.SubPlan[projection.BuildRequest, projection.BuildDoc, projection.BuildDoc] {
	fetcher := &relationFetcher{
		provider: provider,
		rel:      rel,
	}

	// TODO: There are a bunch of things here that can go wrong, like missing id, incorrect types, etc. Handle that better.
	builder := func(parentDoc projection.BuildDoc, fetchResult any) projection.BuildDoc {
		if parentDoc.Doc == nil {
			return parentDoc
		}

		fr, ok := fetchResult.(*fetchedRelation)
		if !ok || fr == nil {
			return parentDoc
		}

		for _, r := range fr.Related {
			if r.ID == "" {
				continue
			}
			parentDoc.Relations = append(parentDoc.Relations, model.VersionedResource{
				Resource: model.Resource{Type: fr.ResourceType, Id: r.ID},
				Version:  r.Version,
			})
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

		parentDoc.Doc[rel.Resource] = subResources
		return parentDoc
	}

	return aggregation.NewSubPlan(parent, fetcher, builder)
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
// whose key source is another relation) are resolved before their dependants.
func resolveOrder(rootType string, relations []resource.RelationConfig) ([]resource.RelationConfig, error) {
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

		if rel.Key.Source != rootType {
			dep, ok := byResource[rel.Key.Source]
			if !ok {
				return fmt.Errorf("key source %q not found among relations", rel.Key.Source)
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

// fetchSingleResource fetches one resource by ID and wraps it in a BuildDoc.
func fetchSingleResource(
	provider source.Provider,
	resourceName string,
	fields []resource.FieldConfig,
	params aggregation.FetchParameters[projection.BuildRequest],
) (aggregation.FetchResult[projection.BuildDoc], error) {
	data, err := provider.FetchResource(context.Background(), source.FetchResourceParams{
		ResourceType: params.Request.ResourceType,
		ResourceID:   params.Request.ResourceID,
		Metadata:     params.Request.Metadata,
	})
	if err != nil {
		return aggregation.FetchResult[projection.BuildDoc]{}, fmt.Errorf("fetch resource %s/%s: %w", params.Request.ResourceType, params.Request.ResourceID, err)
	}

	root := model.Resource{Type: params.Request.ResourceType, Id: params.Request.ResourceID}

	if data.Data == nil {
		return aggregation.FetchResult[projection.BuildDoc]{Items: []projection.BuildDoc{{
			Doc:      nil,
			Resolved: nil,
			Root:     root,
			Metadata: params.Request.Metadata,
		}}}, nil
	}

	filtered := filterFields(data.Data, fields)

	return aggregation.FetchResult[projection.BuildDoc]{
		Items: []projection.BuildDoc{{
			Doc: map[string]any{
				"fields": filtered,
			},
			Resolved: map[string][]map[string]any{
				resourceName: {data.Data},
			},
			Root:     root,
			Metadata: params.Request.Metadata,
		}},
	}, nil
}

// fetchAllResources lists resources of a type with pagination and returns a
// page of BuildDocs. The NextPageToken from the provider is passed through so
// that the RootPlan's pagination loop keeps calling until exhausted.
func fetchAllResources(
	provider source.Provider,
	resourceName string,
	fields []resource.FieldConfig,
	params aggregation.FetchParameters[projection.BuildRequest],
) (aggregation.FetchResult[projection.BuildDoc], error) {
	var pageToken string
	if params.NextPageToken != nil {
		pageToken = params.NextPageToken.(string)
	}

	resp, err := provider.ListResources(context.Background(), source.ListResourcesParams{
		ResourceType: params.Request.ResourceType,
		PageToken:    pageToken,
		PageSize:     100,
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
