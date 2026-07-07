package aggregation

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type ctxKey struct{}

func Test_RootPlan_PropagatesContextToFetcher(t *testing.T) {
	var got any
	fetcher := func(ctx context.Context, params FetchParameters[string]) (FetchResult[string], error) {
		got = ctx.Value(ctxKey{})
		return FetchResult[string]{Items: []string{"a"}}, nil
	}

	ctx := context.WithValue(context.Background(), ctxKey{}, "marker")
	for range NewRootPlan(fetcher).Execute(ctx, "request") {
	}

	require.Equal(t, "marker", got)
}

func Test_RootPlan_CancelledBeforeExecute_FetchesNothing(t *testing.T) {
	var calls int
	fetcher := func(ctx context.Context, params FetchParameters[string]) (FetchResult[string], error) {
		calls++
		return FetchResult[string]{Items: []string{"a"}, NextPageToken: "more"}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var results []ExecutionResult[string]
	for res := range NewRootPlan(fetcher).Execute(ctx, "request") {
		results = append(results, res)
	}

	require.Equal(t, 0, calls)
	require.Len(t, results, 1)
	require.ErrorIs(t, results[0].Err, context.Canceled)
}

func Test_RootPlan_CancelledBetweenPages_SurfacesFetcherError(t *testing.T) {
	// A fetcher that honors ctx, as any real I/O-backed fetcher does: the
	// second page is only fetched after the consumer cancels, so it fails
	// with the ctx error and the plan surfaces it instead of paginating on.
	var calls int
	fetcher := func(ctx context.Context, params FetchParameters[string]) (FetchResult[string], error) {
		calls++
		if params.NextPageToken == nil {
			return FetchResult[string]{Items: []string{"a"}, NextPageToken: "more"}, nil
		}
		<-ctx.Done()
		return FetchResult[string]{}, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var results []ExecutionResult[string]
	for res := range NewRootPlan(fetcher).Execute(ctx, "request") {
		results = append(results, res)
		cancel()
	}

	require.Equal(t, 2, calls)
	require.Len(t, results, 2)
	require.Equal(t, []string{"a"}, results[0].Items)
	require.ErrorIs(t, results[1].Err, context.Canceled)
}

func Test_RootPlan_AbandonedConsumer_UnblocksOnCancel(t *testing.T) {
	fetcher := func(ctx context.Context, params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: []string{"page"}, NextPageToken: "more"}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := NewRootPlan(fetcher).Execute(ctx, "request")

	// Take one page, then abandon the channel and cancel. The producer must
	// give up its blocked send and close the channel rather than leak.
	<-ch
	cancel()

	require.Eventually(t, func() bool {
		select {
		case _, ok := <-ch:
			return !ok
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond, "producer did not close the channel after cancel")
}

func Test_SubPlan_PropagatesContextToFetcher(t *testing.T) {
	rootFetcher := func(_ context.Context, params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: []string{"a"}}, nil
	}

	var got any
	subFetcher := &ctxRecordingFetcher{onFetch: func(ctx context.Context) { got = ctx.Value(ctxKey{}) }}
	builder := func(parent string, fetchResult any) string { return parent }

	ctx := context.WithValue(context.Background(), ctxKey{}, "marker")
	for range NewSubPlan(NewRootPlan(rootFetcher), subFetcher, builder).Execute(ctx, "request") {
	}

	require.Equal(t, "marker", got)
}

func Test_SubPlan_CancelledMidPage_StopsFetchingItems(t *testing.T) {
	rootFetcher := func(_ context.Context, params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: []string{"a", "b", "c"}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The fetch for "a" cancels the ctx; the plan must not fetch "b" or "c".
	var calls int
	subFetcher := &ctxRecordingFetcher{onFetch: func(context.Context) {
		calls++
		cancel()
	}}
	builder := func(parent string, fetchResult any) string { return parent }

	var results []ExecutionResult[string]
	for res := range NewSubPlan(NewRootPlan(rootFetcher), subFetcher, builder).Execute(ctx, "request") {
		results = append(results, res)
	}

	require.Equal(t, 1, calls)
	require.Len(t, results, 1)
	require.ErrorIs(t, results[0].Err, context.Canceled)
}

func Test_MapPlan_CancelledBeforeExecute_ForwardsError(t *testing.T) {
	fetcher := func(_ context.Context, params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: []string{"a"}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var results []ExecutionResult[string]
	for res := range NewMapPlan(NewRootPlan(fetcher), func(s string) string { return s }).Execute(ctx, "request") {
		results = append(results, res)
	}

	require.Len(t, results, 1)
	require.ErrorIs(t, results[0].Err, context.Canceled)
}

type ctxRecordingFetcher struct {
	onFetch func(ctx context.Context)
}

func (f *ctxRecordingFetcher) Fetch(ctx context.Context, parent string) (any, error) {
	f.onFetch(ctx)
	return "sub", nil
}
