package aggregation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type MockFetcher[Parent any, Result any] struct {
	FetchFunc func(Parent) (any, error)
}

func (m *MockFetcher[Parent, Result]) Fetch(parent Parent) (any, error) {
	return m.FetchFunc(parent)
}

func Test_RootPlan(t *testing.T) {
	fetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		if params.NextPageToken == nil {
			return FetchResult[string]{Items: []string{"a", "b"}, NextPageToken: "token"}, nil
		}
		return FetchResult[string]{Items: []string{"c"}, NextPageToken: nil}, nil
	}

	rootPlan := NewRootPlan(fetcher)

	ch := rootPlan.Execute(context.Background(), "request")

	var results []ExecutionResult[string]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 2, len(results))
	require.Equal(t, []string{"a", "b"}, results[0].Items)
	require.Equal(t, []string{"c"}, results[1].Items)
}

func Test_SubPlan_Execute(t *testing.T) {
	rootFetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		if params.NextPageToken == nil {
			return FetchResult[string]{Items: []string{"a", "b"}, NextPageToken: "token"}, nil
		}
		return FetchResult[string]{Items: []string{"c"}, NextPageToken: nil}, nil
	}

	subFetcher := &MockFetcher[string, string]{
		FetchFunc: func(parent string) (any, error) {
			return parent + "_sub", nil
		},
	}

	builder := func(parent string, fetchResult any) string {
		return parent + "_" + fetchResult.(string)
	}

	rootPlan := NewRootPlan(rootFetcher)
	subPlan := NewSubPlan(rootPlan, subFetcher, builder)

	ch := subPlan.Execute(context.Background(), "request")

	var results []ExecutionResult[string]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 2, len(results))
	require.Equal(t, []string{"a_a_sub", "b_b_sub"}, results[0].Items)
	require.Equal(t, []string{"c_c_sub"}, results[1].Items)
}

func Test_RootPlan_Execute_WithError(t *testing.T) {
	fetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: nil, NextPageToken: nil}, context.Canceled
	}

	rootPlan := NewRootPlan(fetcher)

	ch := rootPlan.Execute(context.Background(), "request")

	var results []ExecutionResult[string]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 1, len(results))
	require.ErrorIs(t, results[0].Err, context.Canceled)
}

func Test_SubPlan_WithError(t *testing.T) {
	rootFetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: []string{"a", "b"}, NextPageToken: nil}, nil
	}

	subFetcher := &MockFetcher[string, string]{
		FetchFunc: func(parent string) (any, error) {
			if parent == "a" {
				return nil, context.Canceled
			}
			return parent + "_sub", nil
		},
	}

	builder := func(parent string, fetchResult any) string {
		return parent + "_" + fetchResult.(string)
	}

	rootPlan := NewRootPlan(rootFetcher)
	subPlan := NewSubPlan(rootPlan, subFetcher, builder)

	ch := subPlan.Execute(context.Background(), "request")

	var results []ExecutionResult[string]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 1, len(results))
	require.ErrorIs(t, results[0].Err, context.Canceled)
}

func Test_SubPlan_WithParentError(t *testing.T) {
	rootFetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: nil, NextPageToken: nil}, context.Canceled
	}

	subFetcher := &MockFetcher[string, string]{
		FetchFunc: func(parent string) (any, error) {
			return parent + "_sub", nil
		},
	}

	builder := func(parent string, fetchResult any) string {
		return parent + "_" + fetchResult.(string)
	}

	rootPlan := NewRootPlan(rootFetcher)
	subPlan := NewSubPlan(rootPlan, subFetcher, builder)

	ch := subPlan.Execute(context.Background(), "request")

	var results []ExecutionResult[string]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 1, len(results))
	require.ErrorIs(t, results[0].Err, context.Canceled)
}

func Test_SubPlan_MultipleSubItems(t *testing.T) {
	rootFetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: []string{"a", "b"}, NextPageToken: nil}, nil
	}

	subFetcher := &MockFetcher[string, string]{
		FetchFunc: func(parent string) (any, error) {
			return []string{parent + "_sub1", parent + "_sub2"}, nil
		},
	}

	type SubResult struct {
		Parent string
		Subs   []string
	}

	builder := func(parent string, fetchResult any) SubResult {
		return SubResult{
			Parent: parent,
			Subs:   fetchResult.([]string),
		}
	}

	rootPlan := NewRootPlan(rootFetcher)
	subPlan := NewSubPlan(rootPlan, subFetcher, builder)

	ch := subPlan.Execute(context.Background(), "request")

	var results []ExecutionResult[SubResult]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 1, len(results))
	require.Equal(t, "a", results[0].Items[0].Parent)
	require.Equal(t, []string{"a_sub1", "a_sub2"}, results[0].Items[0].Subs)
	require.Equal(t, "b", results[0].Items[1].Parent)
	require.Equal(t, []string{"b_sub1", "b_sub2"}, results[0].Items[1].Subs)
}

func Test_MultipleSubPlans(t *testing.T) {
	rootFetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: []string{"a", "b"}, NextPageToken: nil}, nil
	}

	type SubResult1 struct {
		Parent string
		Sub1   string
	}

	type SubResult2 struct {
		Parent string
		Sub1   string
		Sub2   string
	}

	subFetcher1 := &MockFetcher[string, SubResult1]{
		FetchFunc: func(parent string) (any, error) {
			return SubResult1{Parent: parent, Sub1: parent + "_sub1"}, nil
		},
	}

	subFetcher2 := &MockFetcher[SubResult1, SubResult2]{
		FetchFunc: func(parent SubResult1) (any, error) {
			return SubResult2{Parent: parent.Parent, Sub1: parent.Sub1, Sub2: parent.Sub1 + "_sub2"}, nil
		},
	}

	builder1 := func(parent string, fetchResult any) SubResult1 {
		return fetchResult.(SubResult1)
	}

	builder2 := func(parent SubResult1, fetchResult any) SubResult2 {
		return fetchResult.(SubResult2)
	}

	rootPlan := NewRootPlan(rootFetcher)
	subPlan1 := NewSubPlan(rootPlan, subFetcher1, builder1)
	subPlan2 := NewSubPlan(subPlan1, subFetcher2, builder2)

	ch := subPlan2.Execute(context.Background(), "request")

	var results []ExecutionResult[SubResult2]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 1, len(results))
	require.Equal(t, "a", results[0].Items[0].Parent)
	require.Equal(t, "a_sub1", results[0].Items[0].Sub1)
	require.Equal(t, "a_sub1_sub2", results[0].Items[0].Sub2)
	require.Equal(t, "b", results[0].Items[1].Parent)
	require.Equal(t, "b_sub1", results[0].Items[1].Sub1)
	require.Equal(t, "b_sub1_sub2", results[0].Items[1].Sub2)
}

func Test_SubPlan_EmptyParentResults(t *testing.T) {
	rootFetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: []string{}, NextPageToken: nil}, nil
	}

	subFetcher := &MockFetcher[string, string]{
		FetchFunc: func(parent string) (any, error) {
			return parent + "_sub", nil
		},
	}

	builder := func(parent string, fetchResult any) string {
		return parent + "_" + fetchResult.(string)
	}

	rootPlan := NewRootPlan(rootFetcher)
	subPlan := NewSubPlan(rootPlan, subFetcher, builder)

	ch := subPlan.Execute(context.Background(), "request")

	var results []ExecutionResult[string]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 1, len(results))
	require.Equal(t, 0, len(results[0].Items))
}

func Test_SubPlan_WithMapResult(t *testing.T) {
	rootFetcher := func(params FetchParameters[string]) (FetchResult[map[string]string], error) {
		return FetchResult[map[string]string]{Items: []map[string]string{{"parent": "a"}, {"parent": "b"}}, NextPageToken: nil}, nil
	}

	subFetcher := &MockFetcher[map[string]string, map[string]string]{
		FetchFunc: func(parent map[string]string) (any, error) {
			return map[string]string{"sub": parent["parent"] + "_sub"}, nil
		},
	}

	builder := func(parent map[string]string, fetchResult any) map[string]string {
		res := fetchResult.(map[string]string)
		res["parent"] = parent["parent"]
		return res
	}

	rootPlan := NewRootPlan(rootFetcher)
	subPlan := NewSubPlan(rootPlan, subFetcher, builder)

	ch := subPlan.Execute(context.Background(), "request")

	var results []ExecutionResult[map[string]string]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 1, len(results))
	require.Equal(t, "a", results[0].Items[0]["parent"])
	require.Equal(t, "a_sub", results[0].Items[0]["sub"])
	require.Equal(t, "b", results[0].Items[1]["parent"])
	require.Equal(t, "b_sub", results[0].Items[1]["sub"])
}
