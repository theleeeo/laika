package core

import (
	"context"
	"errors"
	"log/slog"
)

// Search executes a search query against the indexed documents for the given
// resource, running it through the registered search middleware chain.
func (idx *Indexer) Search(ctx context.Context, req SearchRequest) (SearchResponse, error) {
	logger := LoggerFromContext(ctx).With(
		slog.String("search_id", newSearchID()),
		slog.String("resource", req.Resource),
	)
	ctx = WithLogger(ctx, logger)
	logger.Debug("search request received",
		slog.String("query", req.Query),
		slog.Int("filter_count", len(req.Filters)),
		slog.Any("filters", summarizeFilters(req.Filters)),
	)
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

	req.Page, req.PageSize = normalizePaging(req.Page, req.PageSize)

	LoggerFromContext(ctx).Debug("search base: issuing primary query",
		slog.String("alias", AliasName(r.Resource)),
		slog.Int("page", int(req.Page)),
		slog.Int("page_size", int(req.PageSize)),
		slog.Int("filter_count", len(req.Filters)),
		slog.Any("filters", summarizeFilters(req.Filters)),
	)

	return idx.es.Search(ctx, req, AliasName(r.Resource), r.ReadVersionConfig())
}

// normalizePaging clamps paging to the shared defaults: page size defaults to
// 25, caps at 100, and page is never negative. Both search paths use it.
func normalizePaging(page, pageSize int32) (int32, int32) {
	if pageSize <= 0 {
		pageSize = 25
	}
	if pageSize > 100 {
		pageSize = 100
	}
	if page < 0 {
		page = 0
	}
	return page, pageSize
}
