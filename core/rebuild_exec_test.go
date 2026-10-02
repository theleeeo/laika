package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// twoVersionResources returns a "product" type with Schema Versions 1 and 2 —
// the ADR 0004 migration shape (v2 adds a field alongside the serving v1).
func twoVersionResources() resource.Configs {
	cfgs := resource.Configs{
		{
			Resource: "product",
			Versions: []resource.VersionConfig{
				{Version: 1, Fields: []resource.FieldConfig{{Name: "title", Type: "text"}}},
				{Version: 2, Fields: []resource.FieldConfig{
					{Name: "title", Type: "text"},
					{Name: "price", Type: "long"},
				}},
			},
		},
	}
	for _, c := range cfgs {
		c.ApplyDefaults()
	}
	return cfgs
}

func productDoc(id string, rels ...model.VersionedResource) projection.BuildDoc {
	return projection.BuildDoc{
		Root:      model.Resource{Type: "product", Id: id},
		Doc:       map[string]any{"fields": map[string]any{"title": "t-" + id}},
		Relations: rels,
	}
}

// pagingExecuter emits the given pages in order, mimicking an all-of-type
// walk's paginated ListResources stream.
type pagingExecuter struct {
	pages []aggregation.ExecutionResult[projection.BuildDoc]
}

func (e *pagingExecuter) Execute(context.Context, projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], len(e.pages))
	for _, p := range e.pages {
		ch <- p
	}
	close(ch)
	return ch
}

// captureBackend records every write and can reject specific document IDs in
// bulk responses or answer single upserts with a version conflict.
type captureBackend struct {
	mu             sync.Mutex
	bulkCalls      [][]BulkItem
	deletes        []string        // "index/id"
	upserts        []string        // "index/id"
	rejectIDs      map[string]bool // BulkUpsert reports these IDs as rejected
	upsertConflict map[string]bool // Upsert returns ErrVersionConflict for "index/id"
	// bulkErr fails the whole BulkUpsert request (no per-item failures) —
	// the case where nothing can be assumed written.
	bulkErr error
}

func (b *captureBackend) Upsert(_ context.Context, index, docID string, _ any, _ int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := index + "/" + docID
	b.upserts = append(b.upserts, key)
	if b.upsertConflict[key] {
		return fmt.Errorf("superseded: %w", ErrVersionConflict)
	}
	return nil
}

func (b *captureBackend) BulkUpsert(_ context.Context, items []BulkItem) ([]BulkFailure, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bulkCalls = append(b.bulkCalls, append([]BulkItem(nil), items...))
	if b.bulkErr != nil {
		return nil, b.bulkErr
	}
	var failures []BulkFailure
	for _, it := range items {
		if b.rejectIDs[it.ID] {
			failures = append(failures, BulkFailure{Index: it.Index, ID: it.ID, Status: 400, Reason: "test rejection"})
		}
	}
	return failures, nil
}

func (b *captureBackend) Delete(_ context.Context, index, docID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deletes = append(b.deletes, index+"/"+docID)
	return nil
}

func (b *captureBackend) Search(context.Context, SearchRequest, string, *resource.VersionConfig) (SearchResponse, error) {
	return SearchResponse{}, nil
}

func (b *captureBackend) FederatedSearch(context.Context, FederatedSearchParams) (FederatedSearchResult, error) {
	return FederatedSearchResult{}, nil
}

func (b *captureBackend) allBulkItems() []BulkItem {
	b.mu.Lock()
	defer b.mu.Unlock()
	var all []BulkItem
	for _, call := range b.bulkCalls {
		all = append(all, call...)
	}
	return all
}

func (b *captureBackend) deletesSnapshot() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.deletes...)
}

// rebuildRecordingStore records per-resource lifecycle calls.
type rebuildRecordingStore struct {
	mu       sync.Mutex
	calls    []string
	buildIdx int64
	// driftBudget bounds how many AnyResourceVersionDrifted calls report
	// drift for driftChildren, so a drift-triggered re-build settles instead
	// of looping forever.
	driftBudget   atomic.Int32
	driftChildren map[string]bool
}

func (s *rebuildRecordingStore) record(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, fmt.Sprintf(format, args...))
}

func (s *rebuildRecordingStore) has(prefix string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

func (s *rebuildRecordingStore) MarkStale(_ context.Context, rs []model.Resource, _ map[string]string) error {
	for _, r := range rs {
		s.record("MarkStale:%s/%s", r.Type, r.Id)
	}
	return nil
}

func (s *rebuildRecordingStore) BeginBuild(_ context.Context, r model.Resource) (BuildBegun, error) {
	s.record("BeginBuild:%s/%s", r.Type, r.Id)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buildIdx++
	return BuildBegun{BuildIdx: s.buildIdx, StaleSeq: 42}, nil
}

func (s *rebuildRecordingStore) NextChangeSeq(context.Context) (int64, error) { return 0, nil }

func (s *rebuildRecordingStore) AnyChangedSince(context.Context, []ChangeCheck) (bool, error) {
	return false, nil
}

func (s *rebuildRecordingStore) ClearStale(_ context.Context, r model.Resource, seq int64) error {
	s.record("ClearStale:%s/%s:%d", r.Type, r.Id, seq)
	return nil
}

func (s *rebuildRecordingStore) DeleteResourceIfSeq(_ context.Context, r model.Resource, seq int64) error {
	s.record("DeleteResourceIfSeq:%s/%s:%d", r.Type, r.Id, seq)
	return nil
}

func (s *rebuildRecordingStore) ListStale(context.Context, time.Time, int) ([]StaleResource, error) {
	return nil, nil
}

func (s *rebuildRecordingStore) AddChildResources(_ context.Context, parent model.Resource, _ []model.Resource) error {
	s.record("AddChildResources:%s/%s", parent.Type, parent.Id)
	return nil
}

func (s *rebuildRecordingStore) AddRelations(context.Context, []Relation) error { return nil }

func (s *rebuildRecordingStore) AnyResourceVersionDrifted(_ context.Context, observed []model.VersionedResource) (bool, error) {
	for _, r := range observed {
		if s.driftChildren[r.Id] && s.driftBudget.Add(-1) >= 0 {
			return true, nil
		}
	}
	return false, nil
}

func (s *rebuildRecordingStore) GetChildResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}

func (s *rebuildRecordingStore) GetParentResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}

func (s *rebuildRecordingStore) RemoveResource(_ context.Context, r model.Resource) error {
	s.record("RemoveResource:%s/%s", r.Type, r.Id)
	return nil
}

func newRebuildIndexer(st Store, es SearchBackend, plans map[string][]projection.Plan, chunkSize int) *Indexer {
	return mustNew(Config{
		Resources:        twoVersionResources(),
		Plans:            plans,
		ES:               es,
		Store:            st,
		RebuildChunkSize: chunkSize,
	})
}

func TestRebuild_TargetedVersion_WritesOnlySelectedIndex_AndMergesEdges(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	child := model.VersionedResource{Resource: model.Resource{Type: "product", Id: "c1"}, Version: 1}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1", child)}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1", child)}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{
		{ResourceType: "product", Versions: []int{2}, ResourceIDs: []string{"1"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	items := es.allBulkItems()
	if len(items) == 0 {
		t.Fatal("expected a write to product_search_v2")
	}
	for _, it := range items {
		if it.Index != "product_search_v2" {
			t.Fatalf("a targeted rebuild must only write the selected version's index, wrote %s", it.Index)
		}
	}
	if st.has("RemoveResource:product/1") {
		t.Fatal("a targeted rebuild must merge edges, not wipe them: the non-targeted versions' plans did not run, so wiping would drop the edges only they discover")
	}
	if !st.has("AddChildResources:product/1") {
		t.Fatal("edges discovered by the executed plan must still be persisted")
	}
	if !st.has("ClearStale:product/1") {
		t.Fatal("a fully flushed resource must clear its stale mark")
	}
}

func TestRebuild_AllVersions_WipesEdges_AndWritesEveryIndex(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{
		{ResourceType: "product", ResourceIDs: []string{"1"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	indices := map[string]bool{}
	for _, it := range es.allBulkItems() {
		indices[it.Index] = true
	}
	if !indices["product_search_v1"] || !indices["product_search_v2"] {
		t.Fatalf("a full rebuild must write every schema version's index, wrote %v", indices)
	}
	if !st.has("RemoveResource:product/1") {
		t.Fatal("a full rebuild must wipe-and-replace edges (ADR 0002)")
	}
	if !st.has("ClearStale:product/1") {
		t.Fatal("a fully flushed resource must clear its stale mark")
	}
}

func TestRebuild_VersionWithoutPlanFails(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	// Config declares v2 but the embedder registered no plan for it.
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{
		{ResourceType: "product", Versions: []int{2}, ResourceIDs: []string{"1"}},
	})
	if err == nil || !strings.Contains(err.Error(), "version 2") {
		t.Fatalf("rebuilding a version that has no plan must fail loudly, got %v", err)
	}
}

func TestRebuildAll_FlushesInBoundedChunks(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	docs := make([]projection.BuildDoc, 10)
	for i := range docs {
		docs[i] = productDoc(fmt.Sprintf("%d", i+1))
	}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: docs}},
	}}
	idx := newRebuildIndexer(st, es, plans, 4)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}})
	if err != nil {
		t.Fatal(err)
	}

	if len(es.bulkCalls) < 3 {
		t.Fatalf("10 docs at chunk size 4 must flush in at least 3 bulk requests, got %d", len(es.bulkCalls))
	}
	total := 0
	for _, call := range es.bulkCalls {
		if len(call) > 4 {
			t.Fatalf("a bulk request must not exceed the chunk size: %d items", len(call))
		}
		total += len(call)
	}
	if total != 10 {
		t.Fatalf("every document must be written exactly once, wrote %d", total)
	}
}

func TestRebuildAll_RejectedDocIsMarkedStale_NotCleared(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{rejectIDs: map[string]bool{"3": true}}
	docs := make([]projection.BuildDoc, 5)
	for i := range docs {
		docs[i] = productDoc(fmt.Sprintf("%d", i+1))
	}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: docs}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}})
	if err == nil {
		t.Fatal("a rebuild with rejected documents must not report success")
	}

	if !st.has("MarkStale:product/3") {
		t.Fatal("a rejected resource must be durably marked stale so the sweep recovers it")
	}
	if st.has("ClearStale:product/3") {
		t.Fatal("a rejected resource must not clear its stale mark")
	}
	for _, id := range []string{"1", "2", "4", "5"} {
		if !st.has("ClearStale:product/" + id) {
			t.Fatalf("resource %s flushed fine and must be cleared", id)
		}
	}
}

func TestRebuildAll_NilDocDeletesFromAllVersions(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	gone := projection.BuildDoc{Root: model.Resource{Type: "product", Id: "2"}}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1"), gone, productDoc("3")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}})
	if err != nil {
		t.Fatal(err)
	}

	deletes := map[string]bool{}
	for _, d := range es.deletesSnapshot() {
		deletes[d] = true
	}
	if !deletes["product_search_v1/2"] || !deletes["product_search_v2/2"] {
		t.Fatalf("a resource gone at source must be deleted from every schema version's index, got %v", deletes)
	}
	for _, it := range es.allBulkItems() {
		if it.ID == "2" {
			t.Fatal("a nil document must never be bulk-written")
		}
	}
	if st.has("ClearStale:product/2") {
		t.Fatal("the delete path owns the deleted resource; the rebuild must not clear it")
	}
	for _, id := range []string{"1", "3"} {
		if !st.has("ClearStale:product/" + id) {
			t.Fatalf("resource %s must still complete normally", id)
		}
	}
}

func TestRebuildAll_ChildDrift_RemarksResourceStale(t *testing.T) {
	st := &rebuildRecordingStore{driftChildren: map[string]bool{"cX": true}}
	st.driftBudget.Store(2)
	es := &captureBackend{}
	child := model.VersionedResource{Resource: model.Resource{Type: "product", Id: "cX"}, Version: 5}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1", child), productDoc("2")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !st.has("MarkStale:product/1") {
		t.Fatal("a child that drifted during the rebuild's edge-less window must re-mark the parent (ADR 0002 drift check)")
	}
	if st.has("MarkStale:product/2") {
		t.Fatal("a resource without drifted children must not be re-marked")
	}
}

// failingExecuter fails the plan execution with the given error.
type failingExecuter struct{ err error }

func (e *failingExecuter) Execute(context.Context, projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
	ch <- aggregation.ExecutionResult[projection.BuildDoc]{Err: e.err}
	close(ch)
	return ch
}

func TestRebuildByIDs_PlanErrorLeavesResourceStale(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
		{Version: 2, Executer: &failingExecuter{err: errors.New("provider exploded")}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{
		{ResourceType: "product", ResourceIDs: []string{"1"}},
	})
	if err == nil {
		t.Fatal("a rebuild that failed a resource must not report success")
	}

	if !st.has("MarkStale:product/1") {
		t.Fatal("a resource whose plan failed must be durably marked stale so the sweep recovers it")
	}
	if st.has("ClearStale:product/1") {
		t.Fatal("a failed resource must not clear its stale mark")
	}
	for _, it := range es.allBulkItems() {
		if it.ID == "1" {
			t.Fatal("no partial version set may be written for a resource whose plan failed")
		}
	}
}

func TestRebuildAll_PageError_AbortsAndMarksBegunResourcesStale(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &pagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
			{Items: []projection.BuildDoc{productDoc("1"), productDoc("2")}},
			{Err: errors.New("provider page exploded")},
		}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}})
	if err == nil || !strings.Contains(err.Error(), "provider page exploded") {
		t.Fatalf("a page error must abort the walk, got %v", err)
	}

	for _, id := range []string{"1", "2"} {
		if !st.has("MarkStale:product/" + id) {
			t.Fatalf("resource %s had its edges wiped but was never completed — it must be marked stale so the sweep repairs it", id)
		}
		if st.has("ClearStale:product/" + id) {
			t.Fatalf("resource %s must not be cleared on abort", id)
		}
	}
}

func TestBuild_UpsertConflict_IsBenignAndBuildCompletes(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{upsertConflict: map[string]bool{"product_search_v1/1": true}}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.Build(context.Background(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}})
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, u := range es.upserts {
		if u == "product_search_v2/1" {
			found = true
		}
	}
	if !found {
		t.Fatal("an OCC loss on one version's index must not stop the build from writing the remaining versions")
	}
	if !st.has("ClearStale:product/1:42") {
		t.Fatal("an OCC loss is benign — the seq-guarded clear must still run (a newer change keeps the mark via the guard)")
	}
}

// cursorPagingExecuter emits fixed token-stamped pages and records every
// BuildRequest it receives — the shape of a resumable ListResources walk.
type cursorPagingExecuter struct {
	mu    sync.Mutex
	reqs  []projection.BuildRequest
	pages []aggregation.ExecutionResult[projection.BuildDoc]
}

func (e *cursorPagingExecuter) Execute(_ context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	e.mu.Lock()
	e.reqs = append(e.reqs, req)
	e.mu.Unlock()
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], len(e.pages))
	for _, p := range e.pages {
		ch <- p
	}
	close(ch)
	return ch
}

func (e *cursorPagingExecuter) requests() []projection.BuildRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]projection.BuildRequest(nil), e.reqs...)
}

func TestRebuildAll_CheckpointsFollowFlushedPageBoundaries(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &cursorPagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
			{Items: []projection.BuildDoc{productDoc("1"), productDoc("2")}, NextPageToken: "p2"},
			{Items: []projection.BuildDoc{productDoc("3"), productDoc("4")}, NextPageToken: "p3"},
			{Items: []projection.BuildDoc{productDoc("5")}},
		}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 2) // chunk size 2: flush mid-walk

	var cps []RebuildCursor
	err := idx.RebuildNowResumable(context.Background(),
		ResourceSelector{ResourceType: "product", Versions: []int{1}},
		nil,
		func(c RebuildCursor) { cps = append(cps, c) })
	if err != nil {
		t.Fatal(err)
	}

	// Flush 1 fires on page 1's last document, before page 1's token is
	// recorded — no completed boundary yet, so no checkpoint.
	// Flush 2 fires on page 2's last document, by which time page 1 has
	// completed: checkpoint {1,"p2"}. Page 2's own token is not recorded until
	// its page loop ends, after that flush.
	// finish() flushes doc 5: checkpoint {1,"p3"} (page 2's boundary).
	// A checkpoint must never claim a position whose pages are not yet flushed.
	want := []RebuildCursor{{PlanVersion: 1, PageToken: "p2"}, {PlanVersion: 1, PageToken: "p3"}}
	if len(cps) != len(want) || cps[0] != want[0] || cps[1] != want[1] {
		t.Fatalf("checkpoints must trail flushed page boundaries, got %v want %v", cps, want)
	}
}

func TestRebuildAll_MultiPlanWalkNeverCheckpoints(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	var cps []RebuildCursor
	err := idx.RebuildNowResumable(context.Background(),
		ResourceSelector{ResourceType: "product"},
		nil,
		func(c RebuildCursor) { cps = append(cps, c) })
	if err != nil {
		t.Fatal(err)
	}

	// A resource first seen by v1's plan stays unsettled until v2's document
	// lands, so no mid-walk position is safe to resume from: an attempt
	// resuming past it whose remaining listing omits it would leave it with
	// wiped edges, a stale document and no stale mark.
	if len(cps) != 0 {
		t.Fatalf("a multi-plan walk has no settled mid-walk position and must never checkpoint, got %v", cps)
	}
	if !st.has("ClearStale:product/1") {
		t.Fatal("the walk must still complete normally")
	}
	if !st.has("RemoveResource:product/1") {
		t.Fatal("a full rebuild must still wipe-and-replace edges (ADR 0002)")
	}
}

func TestRebuild_TargetedVersionResume_SeedsToken(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	v1 := &cursorPagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
		{Items: []projection.BuildDoc{productDoc("1")}},
	}}
	v2 := &cursorPagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
		{Items: []projection.BuildDoc{productDoc("1")}},
	}}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: v1},
		{Version: 2, Executer: v2},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	// Selecting one version leaves the walk with exactly one active plan —
	// the shape a cursor can address.
	err := idx.RebuildNowResumable(context.Background(),
		ResourceSelector{ResourceType: "product", Versions: []int{2}},
		&RebuildCursor{PlanVersion: 2, PageToken: "p7"},
		nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(v1.requests()) != 0 {
		t.Fatal("a version-targeted walk must not run the versions it did not select")
	}
	reqs := v2.requests()
	if len(reqs) != 1 || reqs[0].PageToken != "p7" {
		t.Fatalf("the walk's only active plan must start at the cursor's page token, got %+v", reqs)
	}
	if st.has("RemoveResource:product/1") {
		t.Fatal("a version-targeted rebuild merges edges, not wipes: the non-targeted versions' plans did not run, so wiping would drop the edges only they discover")
	}
	if !st.has("ClearStale:product/1") {
		t.Fatal("a resource whose every selected plan flushed must settle")
	}
	items := es.allBulkItems()
	if len(items) == 0 {
		t.Fatal("expected a write to product_search_v2")
	}
	for _, it := range items {
		if it.Index != "product_search_v2" {
			t.Fatalf("a version-targeted walk must only write the selected version's index, wrote %s", it.Index)
		}
	}
}

func TestRebuildAll_MultiPlanCursorIgnored(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	v1 := &cursorPagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
		{Items: []projection.BuildDoc{productDoc("1")}},
	}}
	v2 := &cursorPagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
		{Items: []projection.BuildDoc{productDoc("1")}},
	}}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: v1},
		{Version: 2, Executer: v2},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNowResumable(context.Background(),
		ResourceSelector{ResourceType: "product"},
		&RebuildCursor{PlanVersion: 2, PageToken: "p7"},
		nil)
	if err != nil {
		t.Fatal(err)
	}

	// A multi-plan walk never checkpoints, so it can never be handed a cursor
	// it produced: the only safe reading of one is to discard it.
	for name, ex := range map[string]*cursorPagingExecuter{"v1": v1, "v2": v2} {
		reqs := ex.requests()
		if len(reqs) != 1 || reqs[0].PageToken != "" {
			t.Fatalf("a multi-plan walk must ignore the cursor and walk %s from the start, got %+v", name, reqs)
		}
	}
	if !st.has("RemoveResource:product/1") {
		t.Fatal("a walk that ignored its cursor runs from scratch — full wipe-and-replace applies")
	}
}

func TestRebuildAll_ResumeWithUnknownVersionRestartsFromScratch(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	v1 := &cursorPagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
		{Items: []projection.BuildDoc{productDoc("1")}},
	}}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: v1},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNowResumable(context.Background(),
		ResourceSelector{ResourceType: "product"},
		&RebuildCursor{PlanVersion: 9, PageToken: "pX"}, // config changed between attempts
		nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(v1.requests()) != 1 || v1.requests()[0].PageToken != "" {
		t.Fatalf("a cursor naming a version this walk does not run must restart it from scratch, got %+v", v1.requests())
	}
	if !st.has("RemoveResource:product/1") {
		t.Fatal("a from-scratch walk is not resumed — full wipe-and-replace applies")
	}
}

func TestRebuildAll_FailedFlushDoesNotCheckpoint(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{bulkErr: errors.New("elasticsearch unreachable")}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &cursorPagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
			{Items: []projection.BuildDoc{productDoc("1"), productDoc("2")}, NextPageToken: "p2"},
			{Items: []projection.BuildDoc{productDoc("3"), productDoc("4")}, NextPageToken: "p3"},
		}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 2)

	var cps []RebuildCursor
	err := idx.RebuildNowResumable(context.Background(),
		ResourceSelector{ResourceType: "product"},
		nil,
		func(c RebuildCursor) { cps = append(cps, c) })
	if err == nil || !strings.Contains(err.Error(), "elasticsearch unreachable") {
		t.Fatalf("a request-level write failure must abort the walk, got %v", err)
	}

	if len(cps) != 0 {
		t.Fatalf("a failed flush wrote nothing; checkpointing past it would strand the resources it dropped, got %v", cps)
	}
	for _, id := range []string{"1", "2"} {
		if !st.has("MarkStale:product/" + id) {
			t.Fatalf("resource %s had its edges wiped but never landed — salvage must mark it stale", id)
		}
		if st.has("ClearStale:product/" + id) {
			t.Fatalf("resource %s must not be cleared after a failed flush", id)
		}
	}
}

func TestRebuildAll_NonStringTokenNeverCheckpointsMidPlan(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &cursorPagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
			{Items: []projection.BuildDoc{productDoc("1")}, NextPageToken: 42},
			{Items: []projection.BuildDoc{productDoc("2")}},
		}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	var cps []RebuildCursor
	err := idx.RebuildNowResumable(context.Background(),
		ResourceSelector{ResourceType: "product"},
		nil,
		func(c RebuildCursor) { cps = append(cps, c) })
	if err != nil {
		t.Fatal(err)
	}

	if len(cps) != 0 {
		t.Fatalf("a non-string token cannot be resumed from, so it must never be checkpointed, got %v", cps)
	}
	if !st.has("ClearStale:product/1") || !st.has("ClearStale:product/2") {
		t.Fatal("the walk itself must still complete")
	}
}

// cancelStoppingExecuter models what the real aggregation pipeline does under
// cancellation: it stops producing and closes its channel *without* a terminal
// error, because a cancelled producer abandons its send when no receiver is
// parked — and the walk is not parked while it flushes to Elasticsearch.
type cancelStoppingExecuter struct {
	pages []aggregation.ExecutionResult[projection.BuildDoc]
}

func (e *cancelStoppingExecuter) Execute(ctx context.Context, _ projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc])
	go func() {
		defer close(ch)
		for _, p := range e.pages {
			// Checked before the page is offered, not only inside the select:
			// a select whose receive and whose ctx.Done() are both ready picks
			// between them uniformly, so a cancelled walk could still be handed
			// the next page and the test would only fail against the bug about
			// half the time. Refusing to offer anything once cancelled makes
			// the unwalked remainder — and therefore the red — deterministic.
			if ctx.Err() != nil {
				return
			}
			select {
			case ch <- p:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

func TestRebuildAll_CancelledWalkNeverReportsSuccess(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &cancelStoppingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
			{Items: []projection.BuildDoc{productDoc("1"), productDoc("2")}, NextPageToken: "p2"},
			{Items: []projection.BuildDoc{productDoc("3"), productDoc("4")}, NextPageToken: "p3"},
			{Items: []projection.BuildDoc{productDoc("5")}},
		}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cps []RebuildCursor
	err := idx.RebuildNowResumable(ctx,
		ResourceSelector{ResourceType: "product", Versions: []int{1}},
		nil,
		func(c RebuildCursor) {
			cps = append(cps, c)
			cancel()
		})

	// The walk was cut short at its first checkpoint, leaving page 3 unwalked.
	// Reporting success would let the caller — the RunRebuild activity — record
	// a half-done backfill as a finished one.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled walk left its listing unfinished and must say so, got %v", err)
	}
	if len(cps) == 0 {
		t.Fatal("the walk must still checkpoint the boundary it did reach")
	}
	if st.has("BeginBuild:product/5") {
		t.Fatal("the walk stopped before page 3; product 5 must never have been begun")
	}
}

func (s *rebuildRecordingStore) RegisterChanges(context.Context, []Registration) (Registered, error) {
	panic("RegisterChanges: not implemented")
}
