package core

import (
	"context"
	"errors"
	"log/slog"

	"github.com/theleeeo/laika/core/resource"
)

// Search executes a search query against the indexed documents for the given
// resource, running it through the registered search middleware chain.
//
// A scan (req.Scan) goes through the same chain on every page. Its first page
// pins the one index the read alias serves and that index's schema version;
// a later page continues from req.PageToken, the previous page's
// NextPageToken, under the token's pinning, once the token is younger than
// Config.ScanMaxAge, its version still configured with that index, and the
// request, as the registered middlewares leave it, the one that issued it.
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
	// Strict validation of caller-supplied filters (spec: Request validation).
	// Runs before the middleware chain so middleware-appended filters are
	// exempt. An unknown resource falls through: searchBase reports it as
	// ErrUnknownResource, keeping that error's precedence — except for a
	// scan, which reports it before anything else.
	//
	// The context's scan state is replaced here: a scan sets its own, any
	// other search clears it, so a search a middleware starts inside a scan
	// never inherits the scan's pinning.
	cfg := idx.resources.Get(req.Resource)
	var vc *resource.VersionConfig
	if req.Scan {
		if cfg == nil {
			return SearchResponse{}, ErrUnknownResource
		}
		st, err := idx.beginScan(ctx, cfg, req)
		if err != nil {
			return SearchResponse{}, err
		}
		ctx = withScanState(ctx, st)
		vc = st.vc
	} else {
		if req.PageToken != "" {
			return SearchResponse{}, &InvalidArgumentError{Msg: "a page token continues a scan: scan must be set"}
		}
		ctx = withScanState(ctx, nil)
		if cfg != nil {
			vc = cfg.ReadVersionConfig()
		}
	}
	if vc != nil {
		if err := validateRequestFilters(vc, req.Filters); err != nil {
			return SearchResponse{}, err
		}
	}
	return idx.searchChain(ctx, req)
}

// searchBase is the innermost search handler: it validates the resource,
// normalizes paging, and calls the backend — for a scan, on the index and
// config it pinned, without the paged defaults. It is the base of the
// middleware chain composed in New.
func (idx *Indexer) searchBase(ctx context.Context, req SearchRequest) (SearchResponse, error) {
	if req.Resource == "" {
		return SearchResponse{}, errors.New("resource is required")
	}

	r := idx.resources.Get(req.Resource)
	if r == nil {
		return SearchResponse{}, ErrUnknownResource
	}

	if req.Scan {
		// A scan searches the index it pinned with the pinned version's
		// config; scanPage has normalized its page size.
		st, err := scanStateFor(ctx, req)
		if err != nil {
			return SearchResponse{}, err
		}
		if st.index == "" {
			// No read alias yet: nothing to scan, as a paged search's 404.
			return SearchResponse{}, nil
		}
		LoggerFromContext(ctx).Debug("search base: issuing scan page",
			slog.String("index", st.index),
			slog.Int("version", st.version),
			slog.Bool("first_page", req.PageToken == ""),
			slog.Int("page_size", int(req.PageSize)),
			slog.Int("filter_count", len(req.Filters)),
		)
		return idx.es.Search(ctx, req, st.index, st.vc)
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
