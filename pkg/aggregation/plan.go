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

// TODO: Cancellation + cleanup + drain
func NewRootPlan[Req any, P any](fetcher func(FetchParameters[Req]) (FetchResult[P], error)) *RootPlan[Req, P] {
	return &RootPlan[Req, P]{fetcher: fetcher}
}

type RootPlan[Req any, P any] struct {
	fetcher func(FetchParameters[Req]) (FetchResult[P], error)
}

func (p *RootPlan[Req, P]) Execute(ctx context.Context, params Req) <-chan ExecutionResult[P] {
	var npt any
	ch := make(chan ExecutionResult[P])
	go func() {
		defer close(ch)
		for {
			result, err := p.fetcher(FetchParameters[Req]{Request: params, NextPageToken: npt})
			if err != nil {
				ch <- ExecutionResult[P]{Err: err}
				return
			}

			ch <- ExecutionResult[P]{Items: result.Items}

			if result.NextPageToken == nil {
				return
			}
			npt = result.NextPageToken
		}
	}()
	return ch
}

type SubFetcher[Parent any] interface {
	Fetch(Parent) (any, error)
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
				ch <- ExecutionResult[R]{Err: parentItems.Err}
				return
			}

			rowResult := make([]R, len(parentItems.Items))
			for i, parentItem := range parentItems.Items {
				fetchResult, err := p.Fetcher.Fetch(parentItem)
				if err != nil {
					ch <- ExecutionResult[R]{Err: err}
					return
				}

				rowResult[i] = p.Builder(parentItem, fetchResult)
			}

			ch <- ExecutionResult[R]{Items: rowResult}
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
