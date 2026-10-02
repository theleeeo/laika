package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/theleeeo/laika/core/resource"
)

// ErrVersionConflict is returned by a document write that lost the Build
// Sequence OCC race: the index already holds the document at a strictly
// higher external version, written by a concurrent build. Losing this race
// is benign — the winner carried fresher data — so callers treat it as a
// no-op, and the seq-guarded finish (ClearStale or FinishOwned) keeps
// recovery correct when the superseding build served a different change.
var ErrVersionConflict = errors.New("version conflict")

// SearchBackend is the interface that wraps the document-level operations
// needed by the Indexer. es.Client implements this interface.
type SearchBackend interface {
	Upsert(ctx context.Context, index, docID string, doc any, version int64) error
	// BulkUpsert writes the items and reports per-item rejections. The error
	// covers request-level failures (transport, timeout, non-2xx response),
	// where nothing can be assumed written. Version conflicts are not
	// reported: they are OCC losses to a concurrent build (see
	// ErrVersionConflict) and count as success.
	BulkUpsert(ctx context.Context, items []BulkItem) ([]BulkFailure, error)
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

// BulkFailure is one document a bulk write rejected for a reason other than
// a version conflict — a mapping rejection, a missing index, a malformed
// document. The write did not land and will not be retried by the backend.
type BulkFailure struct {
	Index  string
	ID     string
	Status int
	Reason string
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
