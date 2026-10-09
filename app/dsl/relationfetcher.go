package dsl

import (
	"context"
	"fmt"

	"github.com/theleeeo/laika/app/source"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/projection"
)

// relationFetcher implements aggregation.SubFetcher[BuildDoc, *fetchedRelation].
type relationFetcher struct {
	provider source.Provider
	rel      resource.RelationConfig
}

func newRelationFetcher(provider source.Provider, rel resource.RelationConfig) *relationFetcher {
	return &relationFetcher{provider: provider, rel: rel}
}

// Fetch reads the relation's resources for one parent document. A parent with
// a nil Doc (the root fetch answered a nil or an Err for it) has nothing to
// relate and is not read for.
func (f *relationFetcher) Fetch(ctx context.Context, parent projection.BuildDoc) (*fetchedRelation, error) {
	if parent.Doc == nil {
		return nil, nil
	}

	sourceData, ok := parent.Resolved[f.rel.LocalSource(parent.Root.Type)]
	if !ok || len(sourceData) == 0 {
		return &fetchedRelation{}, nil
	}

	// Read the value from the local field, but identify the related resources
	// to the provider by the foreign field they are matched on.
	var key source.ResourceKey
	if val, ok := sourceData[0][f.rel.Join.Local]; ok {
		if valStr, ok := val.(string); ok {
			key = source.ResourceKey{Field: f.rel.Join.Foreign, Value: valStr}
		}
	}
	if key.Value == "" {
		return &fetchedRelation{}, nil
	}

	relatedResp, err := f.provider.FetchRelated(ctx, source.FetchRelatedParams{
		RootResource: source.RootResource{
			Type: parent.Root.Type,
			Id:   parent.Root.Id,
		},
		ResourceType: f.rel.Resource,
		Key:          key,
		Metadata:     parent.Metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch related %s for %s/%s: %w", f.rel.Resource, parent.Root.Type, parent.Root.Id, err)
	}

	return &fetchedRelation{
		ResourceType: f.rel.Resource,
		Related:      relatedResp.Related,
	}, nil
}

// fetchedRelation holds the raw data returned by a relation fetch.
type fetchedRelation struct {
	ResourceType string
	Related      []source.RelatedResource
}
