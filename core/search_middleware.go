package core

import "context"

// SearchHandler executes a search. The innermost handler is the Indexer's own
// validate + normalize + backend call.
type SearchHandler func(ctx context.Context, req SearchRequest) (SearchResponse, error)

// SearchMiddleware wraps a SearchHandler, returning a new one. Middlewares run
// outermost-first in registration order.
type SearchMiddleware func(next SearchHandler) SearchHandler

// chainSearch composes middlewares around a base handler. The slice runs
// outermost-first: for []{A, B} the resulting call order is A → B → base.
func chainSearch(base SearchHandler, mws []SearchMiddleware) SearchHandler {
	h := base
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
