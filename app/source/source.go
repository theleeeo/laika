package source

import (
	"context"
)

type Provider interface {
	FetchResource(ctx context.Context, params FetchResourceParams) (FetchResourceResult, error)
	// FetchRelated fetches resources of resourceType associated with key.
	// sourceResource and sourceField identify where the key was extracted from.
	FetchRelated(ctx context.Context, params FetchRelatedParams) (FetchRelatedResult, error)
	// ListResources returns a paginated list of all resources of a given type.
	ListResources(ctx context.Context, params ListResourcesParams) (ListResourcesResult, error)
}

type FetchResourceParams struct {
	ResourceType string
	ResourceID   string
	Metadata     map[string]string
}

type FetchResourceResult struct {
	Data map[string]any
}

// ResourceKey holds a single extracted field name and its value.
type ResourceKey struct {
	Field string
	Value string
}

type FetchRelatedParams struct {
	// The resource for which to fetch related resources.
	RootResource RootResource
	// The resource type to fetch.
	ResourceType string
	// Key is the extracted field-value pair used to identify the related resource.
	Key ResourceKey
	// Metadata contains arbitrary caller-provided context propagated from
	// the indexing API.
	Metadata map[string]string
}

type RootResource struct {
	Type string
	Id   string
}

type FetchRelatedResult struct {
	// The related resources that were found for the given key.
	Related []RelatedResource
}

// RelatedResource is a single related resource returned by FetchRelated.
type RelatedResource struct {
	ID      string
	Data    map[string]any
	Version int64 // 0 means provider does not supply version for this resource.
}

type ListResourcesParams struct {
	ResourceType string
	PageToken    string
	PageSize     int32
	Metadata     map[string]string
}

type ListResourcesResult struct {
	Resources     []ListedResource
	NextPageToken string
}

type ListedResource struct {
	ID   string
	Data map[string]any
}
