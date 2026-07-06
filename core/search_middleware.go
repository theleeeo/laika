package core

import "context"

// SearchHandler executes a search. The innermost handler is the Indexer's own
// validate + normalize + backend call.
type SearchHandler func(ctx context.Context, req SearchRequest) (SearchResponse, error)

// SearchMiddleware wraps a SearchHandler, returning a new one. Middlewares run
// outermost-first in registration order.
type SearchMiddleware func(next SearchHandler) SearchHandler

// FederatedSearchHandler executes a federated search. The innermost handler is
// the Indexer's own validate + group-build + backend call.
type FederatedSearchHandler func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error)

// FederatedSearchMiddleware wraps a FederatedSearchHandler, returning a new
// one. Middlewares run outermost-first in registration order. The federated
// chain is fully independent of the single-resource chain: neither ever runs
// on the other path.
type FederatedSearchMiddleware func(next FederatedSearchHandler) FederatedSearchHandler

// chain composes middlewares around a base handler. The slice runs
// outermost-first: for []{A, B} the resulting call order is A → B → base.
func chain[H any, M ~func(H) H](base H, mws []M) H {
	h := base
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
