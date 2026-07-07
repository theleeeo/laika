package aggregation

import (
	"context"
)

type FetchParameters[Req any] struct {
	Request       Req
	NextPageToken any
}

type FetchResult[P any] struct {
	Items         []P
	NextPageToken any
}

// send delivers res on ch, honoring cancellation: when ctx is cancelled and no
// receiver is ready, it gives up instead of blocking forever on an abandoned
// channel. A receiver that is already parked on the channel is always served,
// so terminal errors reach consumers that keep draining. Reports whether the
// result was delivered.
func send[P any](ctx context.Context, ch chan<- ExecutionResult[P], res ExecutionResult[P]) bool {
	select {
	case ch <- res:
		return true
	default:
	}
	select {
	case ch <- res:
		return true
	case <-ctx.Done():
		return false
	}
}

func NewRootPlan[Req any, P any](fetcher func(ctx context.Context, params FetchParameters[Req]) (FetchResult[P], error)) *RootPlan[Req, P] {
	return &RootPlan[Req, P]{fetcher: fetcher}
}

type RootPlan[Req any, P any] struct {
	fetcher func(ctx context.Context, params FetchParameters[Req]) (FetchResult[P], error)
}

func (p *RootPlan[Req, P]) Execute(ctx context.Context, params Req) <-chan ExecutionResult[P] {
	var npt any
	ch := make(chan ExecutionResult[P])
	go func() {
		defer close(ch)
		for {
			if err := ctx.Err(); err != nil {
				send(ctx, ch, ExecutionResult[P]{Err: err})
				return
			}

			result, err := p.fetcher(ctx, FetchParameters[Req]{Request: params, NextPageToken: npt})
			if err != nil {
				send(ctx, ch, ExecutionResult[P]{Err: err})
				return
			}

			if !send(ctx, ch, ExecutionResult[P]{Items: result.Items}) {
				return
			}

			if result.NextPageToken == nil {
				return
			}
			npt = result.NextPageToken
		}
	}()
	return ch
}

type SubFetcher[Parent any] interface {
	Fetch(ctx context.Context, parent Parent) (any, error)
}

func NewSubPlan[Req any, Parent any, Result any](
	root Executer[Req, Parent],
	fetcher SubFetcher[Parent],
	builder func(Parent, any) Result,
) *SubPlan[Req, Parent, Result] {
	return &SubPlan[Req, Parent, Result]{Parent: root, Fetcher: fetcher, Builder: builder}
}

type SubPlan[Req any, Parent any, Result any] struct {
	Parent  Executer[Req, Parent]
	Fetcher SubFetcher[Parent]
	Builder func(Parent, any) Result
}

func (p *SubPlan[Req, P, R]) Execute(ctx context.Context, rootParams Req) <-chan ExecutionResult[R] {
	ch := make(chan ExecutionResult[R])
	go func() {
		defer close(ch)

		parentCh := p.Parent.Execute(ctx, rootParams)

		for parentItems := range parentCh {
			if parentItems.Err != nil {
				send(ctx, ch, ExecutionResult[R]{Err: parentItems.Err})
				return
			}

			rowResult := make([]R, len(parentItems.Items))
			for i, parentItem := range parentItems.Items {
				if err := ctx.Err(); err != nil {
					send(ctx, ch, ExecutionResult[R]{Err: err})
					return
				}

				fetchResult, err := p.Fetcher.Fetch(ctx, parentItem)
				if err != nil {
					send(ctx, ch, ExecutionResult[R]{Err: err})
					return
				}

				rowResult[i] = p.Builder(parentItem, fetchResult)
			}

			if !send(ctx, ch, ExecutionResult[R]{Items: rowResult}) {
				return
			}
		}
	}()
	return ch
}

// NewMapPlan wraps a parent plan with a pure transform applied to every item.
// It fetches nothing; it exists for terminal shaping stages that derive fields
// once all upstream relations are resolved (e.g. the standardized search
// surfaces). An error result from the parent is forwarded untouched.
func NewMapPlan[Req any, P any](parent Executer[Req, P], mapFn func(P) P) *MapPlan[Req, P] {
	return &MapPlan[Req, P]{parent: parent, mapFn: mapFn}
}

type MapPlan[Req any, P any] struct {
	parent Executer[Req, P]
	mapFn  func(P) P
}

func (p *MapPlan[Req, P]) Execute(ctx context.Context, params Req) <-chan ExecutionResult[P] {
	ch := make(chan ExecutionResult[P])
	go func() {
		defer close(ch)
		for res := range p.parent.Execute(ctx, params) {
			if res.Err != nil {
				send(ctx, ch, res)
				return
			}
			for i := range res.Items {
				res.Items[i] = p.mapFn(res.Items[i])
			}
			if !send(ctx, ch, res) {
				return
			}
		}
	}()
	return ch
}

type ExecutionResult[P any] struct {
	Items []P
	Err   error
}

type Executer[Req, P any] interface {
	Execute(ctx context.Context, params Req) <-chan ExecutionResult[P]
}
