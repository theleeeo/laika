package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// staticExecuter emits a single fixed page of BuildDocs.
type staticExecuter struct {
	docs []projection.BuildDoc
	// byID, when it has an entry for the requested resource, serves that
	// instead of docs — e.g. to give one resource Parents without making
	// every cascaded build cascade again.
	byID map[string][]projection.BuildDoc
	// onExecute, when set, runs as Execute is called — before any document
	// is fetched — so a test can order fetches against store calls.
	onExecute func()
}

func (e *staticExecuter) Execute(ctx context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	if e.onExecute != nil {
		e.onExecute()
	}
	docs := e.docs
	if d, ok := e.byID[req.ResourceID]; ok {
		docs = d
	}
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
	ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: docs}
	close(ch)
	return ch
}

// cancellingStore cancels the build's ctx during the first RemoveResource
// call, simulating a job timeout landing mid-rebuild.
type cancellingStore struct {
	cancel      context.CancelFunc
	removeCalls int
}

func (s *cancellingStore) RemoveResource(ctx context.Context, _ model.Resource) error {
	s.removeCalls++
	s.cancel()
	return context.Canceled
}

func (s *cancellingStore) AddChildResources(context.Context, model.Resource, []model.Resource) error {
	return nil
}
func (s *cancellingStore) AddRelations(context.Context, []Relation) error { return nil }
func (s *cancellingStore) ReplaceEdges(context.Context, model.Resource, int64, []EdgeSet, []int) error {
	return nil
}
func (s *cancellingStore) GetChildResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}
func (s *cancellingStore) GetParentResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}
func (s *cancellingStore) MarkStale(context.Context, []model.Resource, map[string]string, time.Duration) ([]Owned, error) {
	return nil, nil
}
func (s *cancellingStore) BeginBuild(context.Context, model.Resource, int64) (BuildBegun, error) {
	return BuildBegun{BuildIdx: 1}, nil
}
func (s *cancellingStore) NextChangeSeq(context.Context) (int64, error) { return 0, nil }
func (s *cancellingStore) AnyChangedSince(context.Context, []ChangeCheck) (bool, error) {
	return false, nil
}
func (s *cancellingStore) ClearStale(context.Context, model.Resource, int64) error { return nil }
func (s *cancellingStore) DeleteResourceIfSeq(context.Context, model.Resource, int64, int64) (FollowUp, error) {
	return FollowUp{}, nil
}
func (s *cancellingStore) ListStale(context.Context, time.Time, int, time.Duration) ([]StaleResource, error) {
	return nil, nil
}
func (s *cancellingStore) RenewOwners(_ context.Context, owned []Owned) ([]Owned, error) {
	return owned, nil
}
func (s *cancellingStore) ReleaseOwners(context.Context, []Owned) error { return nil }
func (s *cancellingStore) FinishOwned(context.Context, model.Resource, int64, int64) (FollowUp, error) {
	return FollowUp{}, nil
}

// A cancelled ctx must abort the all-of-type rebuild loop with the ctx error
// instead of warn-and-continuing through every remaining document (and then
// reporting success).
func TestRebuildAll_CancelledContext_AbortsDocLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &cancellingStore{cancel: cancel}
	docs := make([]projection.BuildDoc, 3)
	for i, id := range []string{"1", "2", "3"} {
		docs[i] = projection.BuildDoc{
			Root: model.Resource{Type: "product", Id: id},
			Doc:  map[string]any{"fields": map[string]any{"title": "t"}},
		}
	}

	idx := mustNew(Config{
		Resources: testResources(),
		Plans: map[string][]projection.Plan{
			"product": {{Version: 1, Executer: &staticExecuter{docs: docs}}},
		},
		ES:    &fakeBackend{},
		Store: store,
	})

	err := idx.rebuild(ctx, RebuildArgs{ResourceType: "product"}, rebuildResume{})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if store.removeCalls != 1 {
		t.Fatalf("expected rebuild to stop after the cancelling call, RemoveResource was called %d times", store.removeCalls)
	}
}

func (s *cancellingStore) RegisterChanges(context.Context, []Registration, time.Duration) (Registered, error) {
	panic("RegisterChanges: not implemented")
}
