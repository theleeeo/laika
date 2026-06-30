package core

import (
	"context"
	"errors"
)

// Search executes a search query against the indexed documents for the given
// resource, running it through the registered search middleware chain.
func (idx *Indexer) Search(ctx context.Context, req SearchRequest) (SearchResponse, error) {
	return idx.searchChain(ctx, req)
}

// searchBase is the innermost search handler: it validates the resource,
// normalizes paging, and calls the backend. It is the base of the middleware
// chain composed in New.
func (idx *Indexer) searchBase(ctx context.Context, req SearchRequest) (SearchResponse, error) {
	if req.Resource == "" {
		return SearchResponse{}, errors.New("resource is required")
	}

	r := idx.resources.Get(req.Resource)
	if r == nil {
		return SearchResponse{}, ErrUnknownResource
	}

	if req.PageSize <= 0 {
		req.PageSize = 25
	}
	if req.PageSize > 100 {
		req.PageSize = 100
	}
	if req.Page < 0 {
		req.Page = 0
	}

	return idx.es.Search(ctx, req, AliasName(r.Resource), r.ReadVersionConfig())
}
