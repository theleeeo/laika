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
}

func (e *staticExecuter) Execute(ctx context.Context, _ projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
	ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: e.docs}
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
func (s *cancellingStore) AnyResourceVersionDrifted(context.Context, []model.VersionedResource) (bool, error) {
	return false, nil
}
func (s *cancellingStore) DeleteResource(context.Context, model.Resource) error { return nil }
func (s *cancellingStore) GetChildResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}
func (s *cancellingStore) GetParentResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}
func (s *cancellingStore) NextRebuildCounter(context.Context, model.Resource) (int64, error) {
	return 1, nil
}
func (s *cancellingStore) UpsertResource(context.Context, model.Resource, int64) error { return nil }

func (s *cancellingStore) MarkStale(context.Context, []model.Resource) error { return nil }
func (s *cancellingStore) MarkDeleted(context.Context, model.Resource) (int64, error) {
	return 0, nil
}
func (s *cancellingStore) BeginBuild(context.Context, model.Resource) (int64, int64, error) {
	return 1, 0, nil
}
func (s *cancellingStore) ClearStale(context.Context, model.Resource, int64) error { return nil }
func (s *cancellingStore) DeleteResourceIfSeq(context.Context, model.Resource, int64) error {
	return nil
}
func (s *cancellingStore) ListStale(context.Context, time.Time, int) ([]StaleResource, error) {
	return nil, nil
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

	idx := New(Config{
		Resources: testResources(),
		Plans: map[string][]projection.Plan{
			"product": {{Version: 1, Executer: &staticExecuter{docs: docs}}},
		},
		ES:    &fakeBackend{},
		Store: store,
	})

	err := idx.rebuild(ctx, RebuildArgs{ResourceType: "product"})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if store.removeCalls != 1 {
		t.Fatalf("expected rebuild to stop after the cancelling call, RemoveResource was called %d times", store.removeCalls)
	}
}
