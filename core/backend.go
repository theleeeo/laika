package core

import (
	"context"
	"fmt"

	"github.com/theleeeo/laika/core/resource"
)

// SearchBackend is the interface that wraps the document-level operations
// needed by the Indexer. es.Client implements this interface.
type SearchBackend interface {
	Upsert(ctx context.Context, index, docID string, doc any, version int64) error
	BulkUpsert(ctx context.Context, items []BulkItem) error
	Delete(ctx context.Context, index, docID string) error
	Search(ctx context.Context, req SearchRequest, indexAlias string, vc *resource.VersionConfig) (SearchResponse, error)
	FederatedSearch(ctx context.Context, params FederatedSearchParams) (FederatedSearchResult, error)
}

// BulkItem is a single document to write in a bulk upsert.
type BulkItem struct {
	Index   string
	ID      string
	Doc     any
	Version int64
}

// IndexName returns the versioned index name for a resource type and version.
// Example: IndexName("a", 2) → "a_search_v2"
func IndexName(resource string, version int) string {
	return fmt.Sprintf("%s_search_v%d", resource, version)
}

// AliasName returns the read alias name for a resource type.
// Example: AliasName("a") → "a_search"
func AliasName(resource string) string {
	return resource + "_search"
}

// IndexNames returns the versioned index names for all given versions.
func IndexNames(resource string, versions []int) []string {
	names := make([]string, len(versions))
	for i, v := range versions {
		names[i] = IndexName(resource, v)
	}
	return names
}
