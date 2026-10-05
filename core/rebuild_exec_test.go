package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

func productDoc(id string, rels ...model.Resource) projection.BuildDoc {
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

// captureBackend records every write and can reject specific documents in
// bulk responses or answer single upserts with a version conflict.
type captureBackend struct {
	mu        sync.Mutex
	bulkCalls [][]BulkItem
	deletes   []string // "index/id"
	// deleteVersions is each delete's version, parallel to deletes.
	deleteVersions []int64
	upserts        []string        // "index/id"
	rejectIDs      map[string]bool // BulkUpsert reports every document of these IDs as rejected
	rejectDocs     map[string]bool // BulkUpsert reports these "index/id" documents as rejected
	// extraFailures are appended to every BulkUpsert response as they are —
	// e.g. a failure whose Index names no document of the chunk.
	extraFailures  []BulkFailure
	upsertConflict map[string]bool // Upsert returns ErrVersionConflict for "index/id"
	// bulkErr fails the whole BulkUpsert request (no per-item failures) —
	// the case where nothing can be assumed written.
	bulkErr error
	// onBulk, when set, runs inside every BulkUpsert after the write is
	// recorded — e.g. to cancel the walk between the write and its drift
	// check.
	onBulk func()
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
	if b.onBulk != nil {
		b.onBulk()
	}
	if b.bulkErr != nil {
		return nil, b.bulkErr
	}
	var failures []BulkFailure
	for _, it := range items {
		if b.rejectIDs[it.ID] || b.rejectDocs[it.Index+"/"+it.ID] {
			failures = append(failures, BulkFailure{Index: it.Index, ID: it.ID, Status: 400, Reason: "test rejection"})
		}
	}
	return append(failures, b.extraFailures...), nil
}

func (b *captureBackend) Delete(_ context.Context, index, docID string, version int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deletes = append(b.deletes, index+"/"+docID)
	b.deleteVersions = append(b.deleteVersions, version)
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

// deletesAt is every delete as "index/id@version", in order.
func (b *captureBackend) deletesAt() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.deletes))
	for i, d := range b.deletes {
		out[i] = fmt.Sprintf("%s@%d", d, b.deleteVersions[i])
	}
	return out
}

// rebuildRecordingStore records per-resource lifecycle calls.
type rebuildRecordingStore struct {
	mu       sync.Mutex
	calls    []string
	buildIdx int64
	// changeSeqs counts NextChangeSeq calls; checks records every
	// AnyChangedSince batch.
	changeSeqs int64
	checks     [][]ChangeCheck
	// driftBudget bounds how many AnyChangedSince calls report drift for
	// driftChildren, so a drift-triggered re-build settles instead of
	// looping forever.
	driftBudget   atomic.Int32
	driftChildren map[string]bool
	// errBudget bounds how many AnyChangedSince calls carrying an errChildren
	// resource fail, so a re-build after a failed query settles.
	errBudget   atomic.Int32
	errChildren map[string]bool
	// changeSeqErr fails every NextChangeSeq from the changeSeqErrFrom-th
	// call on (1-based); 0 fails every call.
	changeSeqErr     error
	changeSeqErrFrom int64
	// markErrs fails the first n MarkStale calls naming "<type>/<id>"; each
	// failed attempt records "MarkStaleFailed:<type>/<id>" instead of
	// "MarkStale:<type>/<id>".
	markErrs map[string]int
	// ctxAware makes MarkStale and AnyChangedSince fail on a done context, as
	// the real store does. ClearStale stays context-blind.
	ctxAware bool
	// tokens hands out owner tokens: a MarkStale with a lease claims every
	// resource it marks under a fresh one. This store keeps no ownership
	// state — it claims even a row a live owner holds, and its FinishOwned
	// never hands on a follow-up — so ownership behaviour is tested on
	// recordingStore, not here.
	tokens int64
	// replaced records every ReplaceEdges call, in order; replaceErrs fails
	// the ReplaceEdges of each resource whose id it names.
	replaced    []edgeReplace
	replaceErrs map[string]error
	// beginErrs fails the BeginBuild of each resource whose id it names; the
	// call is still recorded.
	beginErrs map[string]error
	// deleteErrs fails the DeleteResourceIfSeq of each resource whose id it
	// names; the call is still recorded.
	deleteErrs map[string]error
}

func (s *rebuildRecordingStore) replacedSnapshot() []edgeReplace {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]edgeReplace(nil), s.replaced...)
}

// replacedFor returns the recorded ReplaceEdges calls for r, in order.
func (s *rebuildRecordingStore) replacedFor(r model.Resource) []edgeReplace {
	var out []edgeReplace
	for _, c := range s.replacedSnapshot() {
		if c.resource == r {
			out = append(out, c)
		}
	}
	return out
}

func (s *rebuildRecordingStore) checksSnapshot() [][]ChangeCheck {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]ChangeCheck(nil), s.checks...)
}

func (s *rebuildRecordingStore) callsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
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

func (s *rebuildRecordingStore) MarkStale(ctx context.Context, rs []model.Resource, _ map[string]string, lease time.Duration) ([]Owned, error) {
	var err error
	if s.ctxAware && ctx.Err() != nil {
		err = ctx.Err()
	}
	s.mu.Lock()
	for _, r := range rs {
		if key := r.Type + "/" + r.Id; s.markErrs[key] > 0 {
			s.markErrs[key]--
			err = errors.New("mark stale failed")
		}
	}
	s.mu.Unlock()
	for _, r := range rs {
		if err != nil {
			s.record("MarkStaleFailed:%s/%s", r.Type, r.Id)
		} else {
			s.record("MarkStale:%s/%s", r.Type, r.Id)
		}
	}
	if err != nil || lease <= 0 {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	owned := make([]Owned, len(rs))
	for i, r := range rs {
		s.tokens++
		owned[i] = Owned{Resource: r, Token: s.tokens}
	}
	return owned, nil
}

func (s *rebuildRecordingStore) BeginBuild(_ context.Context, r model.Resource, _ int64) (BuildBegun, error) {
	s.record("BeginBuild:%s/%s", r.Type, r.Id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.beginErrs[r.Id]; err != nil {
		return BuildBegun{}, err
	}
	s.buildIdx++
	// Start = 100 + BuildIdx: distinct per build, so a test can tell whose
	// start a check carries.
	return BuildBegun{BuildIdx: s.buildIdx, StaleSeq: 42, Start: 100 + s.buildIdx}, nil
}

func (s *rebuildRecordingStore) NextChangeSeq(context.Context) (int64, error) {
	s.record("NextChangeSeq")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changeSeqs++
	if s.changeSeqErr != nil && s.changeSeqs >= s.changeSeqErrFrom {
		return 0, s.changeSeqErr
	}
	return 1000 + s.changeSeqs, nil
}

func (s *rebuildRecordingStore) AnyChangedSince(ctx context.Context, checks []ChangeCheck) (bool, error) {
	s.record("AnyChangedSince:%d", len(checks))
	s.mu.Lock()
	s.checks = append(s.checks, append([]ChangeCheck(nil), checks...))
	s.mu.Unlock()
	if s.ctxAware && ctx.Err() != nil {
		return false, ctx.Err()
	}
	for _, c := range checks {
		if s.errChildren[c.Resource.Id] && s.errBudget.Add(-1) >= 0 {
			return false, errors.New("drift query failed")
		}
	}
	for _, c := range checks {
		if s.driftChildren[c.Resource.Id] && s.driftBudget.Add(-1) >= 0 {
			return true, nil
		}
	}
	return false, nil
}

func (s *rebuildRecordingStore) ClearStale(_ context.Context, r model.Resource, seq int64) error {
	s.record("ClearStale:%s/%s:%d", r.Type, r.Id, seq)
	return nil
}

func (s *rebuildRecordingStore) DeleteResourceIfSeq(_ context.Context, r model.Resource, seq, _ int64) (FollowUp, error) {
	s.record("DeleteResourceIfSeq:%s/%s:%d", r.Type, r.Id, seq)
	s.mu.Lock()
	defer s.mu.Unlock()
	return FollowUp{}, s.deleteErrs[r.Id]
}

func (s *rebuildRecordingStore) ListStale(context.Context, time.Time, int, time.Duration) ([]StaleResource, error) {
	return nil, nil
}

func (s *rebuildRecordingStore) RenewOwners(_ context.Context, owned []Owned) ([]Owned, error) {
	return owned, nil
}
func (s *rebuildRecordingStore) ReleaseOwners(context.Context, []Owned) error { return nil }
func (s *rebuildRecordingStore) FinishOwned(_ context.Context, r model.Resource, _, _ int64) (FollowUp, error) {
	s.record("FinishOwned:%s/%s", r.Type, r.Id)
	return FollowUp{}, nil
}

func (s *rebuildRecordingStore) ReplaceEdges(ctx context.Context, r model.Resource, buildSeq int64, sets []EdgeSet, declared []int, _ map[string]string) (map[string]string, error) {
	return nil, s.replaceEdges(ctx, r, buildSeq, sets, declared)
}

func (s *rebuildRecordingStore) replaceEdges(_ context.Context, r model.Resource, buildSeq int64, sets []EdgeSet, declared []int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, fmt.Sprintf("ReplaceEdges:%s/%s:%d", r.Type, r.Id, buildSeq))
	s.replaced = append(s.replaced, edgeReplace{resource: r, buildSeq: buildSeq, sets: slices.Clone(sets), declared: slices.Clone(declared)})
	return s.replaceErrs[r.Id]
}

func (s *rebuildRecordingStore) GetChildResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}

func (s *rebuildRecordingStore) GetParentResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}

// BeginDelete bumps the Build Sequence and is never superseded: this store
// keeps no rows, and its tests don't drive notified deletes.
func (s *rebuildRecordingStore) BeginDelete(_ context.Context, r model.Resource, token int64) (DeleteBegun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, fmt.Sprintf("BeginDelete:%s/%s:%d", r.Type, r.Id, token))
	s.buildIdx++
	return DeleteBegun{BuildIdx: s.buildIdx}, nil
}

func (s *rebuildRecordingStore) RemoveResource(_ context.Context, r model.Resource, buildSeq int64) error {
	s.record("RemoveResource:%s/%s:%d", r.Type, r.Id, buildSeq)
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

// assertReplaces checks r's ReplaceEdges calls: exactly one per want entry,
// in order, each at buildSeq, carrying exactly that entry's sets (in any
// order) and declaring nothing — a rebuild passes nil.
func assertReplaces(t *testing.T, st *rebuildRecordingStore, r model.Resource, buildSeq int64, want ...[]EdgeSet) {
	t.Helper()
	got := st.replacedFor(r)
	if len(got) != len(want) {
		t.Fatalf("%v: want %d ReplaceEdges call(s), got %d: %v", r, len(want), len(got), st.callsSnapshot())
	}
	for i, c := range got {
		if c.buildSeq != buildSeq {
			t.Fatalf("%v: ReplaceEdges call %d at Build Sequence %d, want the resource's %d", r, i, c.buildSeq, buildSeq)
		}
		if !equalSets(c.sets, want[i]) {
			t.Fatalf("%v: ReplaceEdges call %d carries sets %+v, want %+v", r, i, sortedSets(c.sets), sortedSets(want[i]))
		}
		if c.declared != nil {
			t.Fatalf("%v: a rebuild must declare nothing, ReplaceEdges call %d declared %v", r, i, c.declared)
		}
	}
}

// assertNoWipe checks the rebuild never removed r's edges wholesale.
func assertNoWipe(t *testing.T, st *rebuildRecordingStore, r model.Resource) {
	t.Helper()
	if st.has("RemoveResource:" + r.Type + "/" + r.Id) {
		t.Fatalf("a rebuild must not remove the edges of a resource it builds: %v", st.callsSnapshot())
	}
}

// versionSet is version's edge set with the given product children.
func versionSet(version int, children ...string) EdgeSet {
	s := EdgeSet{SchemaVersion: version}
	for _, c := range children {
		s.Children = append(s.Children, product(c))
	}
	return s
}

// A targeted rebuild replaces only its selected versions' edge sets: the
// non-targeted versions' plans did not run, and their sets stay untouched.
func TestRebuild_TargetedVersion_WritesOnlySelectedIndex_AndReplacesOnlyItsEdgeSet(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c2")}}},
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
	assertNoWipe(t, st, product("1"))
	assertReplaces(t, st, product("1"), 1, []EdgeSet{versionSet(2, "c2")})
	if !st.has("ClearStale:product/1") {
		t.Fatal("a fully flushed resource must clear its stale mark")
	}
}

// A full rebuild replaces every version's edge set in one call at the
// resource's Build Sequence — an empty set for a version whose plan found no
// children — and wipes nothing.
func TestRebuild_AllVersions_ReplacesEveryVersionsEdgeSet_AndWritesEveryIndex(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1")}}},
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
	assertNoWipe(t, st, product("1"))
	assertReplaces(t, st, product("1"), 1, []EdgeSet{versionSet(1, "c1"), versionSet(2)})
	if !st.has("ClearStale:product/1") {
		t.Fatal("a fully flushed resource must clear its stale mark")
	}
}

// A multi-plan walk replaces each version's set as that version's documents
// land: with one document per flush, product/1's v1 set in the first flush
// and its v2 set in the second, both at its one Build Sequence.
func TestRebuildAll_MultiPlan_ReplacesEachVersionsSetAsItsDocumentLands(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c2")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 1)

	if err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product"}}); err != nil {
		t.Fatal(err)
	}

	if n := len(es.bulkCalls); n != 2 {
		t.Fatalf("chunk size 1 must flush each version's document separately, got %d flushes", n)
	}
	assertNoWipe(t, st, product("1"))
	assertReplaces(t, st, product("1"), 1, []EdgeSet{versionSet(1, "c1")}, []EdgeSet{versionSet(2, "c2")})
	if !st.has("ClearStale:product/1") {
		t.Fatal("a resource whose every document landed must clear its stale mark")
	}
}

// Ruling R2: a rejected document fails its resource, but the edge set of
// every document that landed in the same flush is still replaced.
func TestRebuildByIDs_RejectedSiblingDocument_StillReplacesTheLandedSet_AndFailsTheResource(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{rejectDocs: map[string]bool{"product_search_v2/1": true}}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c2")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product", ResourceIDs: []string{"1"}}})
	if err == nil {
		t.Fatal("a rebuild with a rejected document must not report success")
	}

	assertNoWipe(t, st, product("1"))
	assertReplaces(t, st, product("1"), 1, []EdgeSet{versionSet(1, "c1")})
	if !st.has("MarkStale:product/1") {
		t.Fatal("a resource with a rejected document must be durably marked stale")
	}
	if st.has("ClearStale:product/1") {
		t.Fatal("a resource with a rejected document must not clear its stale mark")
	}
}

// A bulk failure whose Index/ID names no document of the chunk cannot be
// tied to one document, so it rejects every document of its id: none of the
// resource's edge sets is replaced and the resource fails. Another resource
// of the same flush completes.
func TestRebuildByIDs_FailureNamingNoChunkDocument_RejectsEveryDocumentOfItsID(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{extraFailures: []BulkFailure{{Index: "not_in_the_chunk", ID: "1", Status: 400, Reason: "test rejection"}}}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: childDocs(map[string][]string{"1": {"c1a"}, "2": {"c2a"}})},
		{Version: 2, Executer: childDocs(map[string][]string{"1": {"c1b"}, "2": {"c2b"}})},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product", ResourceIDs: []string{"1", "2"}}})
	if err == nil || !strings.Contains(err.Error(), "failed 1 resource(s)") {
		t.Fatalf("a resource whose documents were all rejected must fail the rebuild, got %v", err)
	}

	if got := st.replacedFor(product("1")); len(got) != 0 {
		t.Fatalf("no document of product/1 can be taken as landed, so none of its sets may be replaced: %+v", got)
	}
	calls := st.callsSnapshot()
	if countPrefix(calls, "MarkStale:product/1") == 0 {
		t.Fatalf("a resource whose documents were rejected must be durably marked stale: %v", calls)
	}
	if countPrefix(calls, "ClearStale:product/1:") != 0 {
		t.Fatalf("a resource whose documents were rejected must not clear its stale mark: %v", calls)
	}
	assertReplaces(t, st, product("2"), 2, []EdgeSet{versionSet(1, "c2a"), versionSet(2, "c2b")})
	if countPrefix(calls, "ClearStale:product/2:") != 1 {
		t.Fatalf("product/2's documents landed and it must complete: %v", calls)
	}
}

// The same resource and version twice in one chunk — a listing that repeats a
// resource — sends one edge set for that version, the later document's: the
// bulk write applies its items in order, so the later document is the one
// Elasticsearch holds.
func TestRebuildAll_SameVersionTwiceInOneChunk_SendsTheLaterDocumentsSet(t *testing.T) {
	st := &rebuildRecordingStore{}
	err := rebuildAllProducts(t, st, &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "early"), productDocWith("1", "late")}})
	if err != nil {
		t.Fatal(err)
	}

	assertReplaces(t, st, product("1"), 1, []EdgeSet{versionSet(1, "late")})
}

// A failed edge replace fails its resource — marked stale, not cleared — and
// the rebuild reports it; another resource of the same flush completes.
func TestRebuildByIDs_FailedEdgeReplace_FailsTheResource(t *testing.T) {
	st := &rebuildRecordingStore{replaceErrs: map[string]error{"1": errors.New("db down")}}
	exec := childDocs(map[string][]string{"1": {"c1"}, "2": {"c2"}})
	idx := newRebuildIndexer(st, &captureBackend{}, map[string][]projection.Plan{"product": {{Version: 1, Executer: exec}}}, 0)

	err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product", ResourceIDs: []string{"1", "2"}}})
	if err == nil || !strings.Contains(err.Error(), "failed 1 resource(s)") {
		t.Fatalf("a resource whose edges were not stored must fail the rebuild, got %v", err)
	}

	calls := st.callsSnapshot()
	if countPrefix(calls, "MarkStale:product/1") == 0 {
		t.Fatalf("a resource whose edges were not stored must be durably marked stale: %v", calls)
	}
	if countPrefix(calls, "ClearStale:product/1:") != 0 {
		t.Fatalf("a resource whose edges were not stored must not clear its stale mark: %v", calls)
	}
	if countPrefix(calls, "ClearStale:product/2:") != 1 {
		t.Fatalf("product/2's edges were stored and it must complete: %v", calls)
	}
}

// Each resource of a flush gets its own single ReplaceEdges call carrying its
// own sets.
func TestRebuildByIDs_TwoResourcesInOneFlush_EachReplaceTheirOwnSets(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: childDocs(map[string][]string{"1": {"c1a"}, "2": {"c2a"}})},
		{Version: 2, Executer: childDocs(map[string][]string{"1": nil, "2": {"c2b", "c2c"}})},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	if err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product", ResourceIDs: []string{"1", "2"}}}); err != nil {
		t.Fatal(err)
	}

	if n := len(es.bulkCalls); n != 1 {
		t.Fatalf("both resources' documents must land in one flush, got %d flushes", n)
	}
	assertReplaces(t, st, product("1"), 1, []EdgeSet{versionSet(1, "c1a"), versionSet(2)})
	assertReplaces(t, st, product("2"), 2, []EdgeSet{versionSet(1, "c2a"), versionSet(2, "c2b", "c2c")})
	if n := len(st.replacedSnapshot()); n != 2 {
		t.Fatalf("one ReplaceEdges call per resource, got %d: %v", n, st.callsSnapshot())
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

// A plan walk deletes a resource listed without data at a Build Sequence of
// its own: one first seen as nil begins a build for it, one an earlier plan's
// document already began in this walk reuses that build's sequence, and one
// whose BeginBuild fails is deleted nowhere and marked stale instead.
func TestRebuildAll_NilDoc_DeletesAtTheResourcesSequence(t *testing.T) {
	t.Run("first seen as nil: begins a build for the delete", func(t *testing.T) {
		st := &rebuildRecordingStore{buildIdx: 41}
		es := &captureBackend{}
		plans := map[string][]projection.Plan{"product": {
			{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1"), nilDoc("2"), productDoc("3")}}},
		}}
		idx := newRebuildIndexer(st, es, plans, 0)

		if err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}}); err != nil {
			t.Fatal(err)
		}

		// 1 begins at 42, 2 at 43, 3 at 44.
		calls := st.callsSnapshot()
		begin, remove := slices.Index(calls, "BeginBuild:product/2"), slices.Index(calls, "RemoveResource:product/2:43")
		if begin == -1 || st.count("BeginBuild:product/2") != 1 {
			t.Fatalf("the nil resource must begin exactly one build for its delete: %v", calls)
		}
		if remove != -1 && remove < begin {
			t.Fatalf("the delete must follow its BeginBuild: %v", calls)
		}
		assertDeletedAt(t, st, es, "2", 43)
		if st.has("ClearStale:product/2") {
			t.Fatalf("the delete path owns the deleted resource; the rebuild must not clear it: %v", calls)
		}
	})

	t.Run("begun by an earlier plan's document: reuses its sequence", func(t *testing.T) {
		st := &rebuildRecordingStore{buildIdx: 41}
		es := &captureBackend{}
		plans := map[string][]projection.Plan{"product": {
			{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1"), productDoc("2")}}},
			{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1"), nilDoc("2")}}},
		}}
		idx := newRebuildIndexer(st, es, plans, 0)

		if err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}}); err != nil {
			t.Fatal(err)
		}

		// v1 begins 1 at 42 and 2 at 43; v2's nil for 2 begins nothing.
		if n := st.count("BeginBuild:product/2"); n != 1 {
			t.Fatalf("a resource begun earlier in the walk must not begin again for its delete, got %d: %v", n, st.callsSnapshot())
		}
		assertDeletedAt(t, st, es, "2", 43)
	})

	t.Run("BeginBuild fails: deletes nothing, marks stale", func(t *testing.T) {
		st := &rebuildRecordingStore{beginErrs: map[string]error{"2": errors.New("db down")}}
		es := &captureBackend{}
		plans := map[string][]projection.Plan{"product": {
			{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1"), nilDoc("2"), productDoc("3")}}},
		}}
		idx := newRebuildIndexer(st, es, plans, 0)

		if err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}}); err == nil {
			t.Fatal("a rebuild that could not settle a resource must report failure")
		}

		if !st.has("BeginBuild:product/2") {
			t.Fatalf("setup: the nil resource must try to begin a build: %v", st.callsSnapshot())
		}
		for _, d := range es.deletesSnapshot() {
			if strings.HasSuffix(d, "/2") {
				t.Fatalf("a delete without a Build Sequence must not run: %v", es.deletesSnapshot())
			}
		}
		if st.has("RemoveResource:product/2") {
			t.Fatalf("a delete without a Build Sequence must not remove edges: %v", st.callsSnapshot())
		}
		if !st.has("MarkStale:product/2") {
			t.Fatalf("the failed resource must be durably marked stale for the sweep: %v", st.callsSnapshot())
		}
		for _, id := range []string{"1", "3"} {
			if !st.has("ClearStale:product/" + id) {
				t.Fatalf("resource %s must still complete normally", id)
			}
		}
	})
}

// A plan walk fetches a page before it begins the resources on it, so a
// resource listed without data can be recreated, built and settled before
// the walk's delete runs at a higher Build Sequence and removes it (ruling
// R8). The delete is therefore followed by the drift check of its root
// against the walk start, as a root the walk settles checks itself: a change
// accepted after the walk start — or a failed check — re-marks the root and
// re-builds it; no change re-marks nothing.
func TestRebuildAll_NilDoc_DeleteIsFollowedByTheRootsDriftCheck(t *testing.T) {
	// The walk lists 1 with data and 2 without; a live build of 2 finds it
	// recreated.
	walk := func() *staticExecuter {
		return &staticExecuter{
			docs: []projection.BuildDoc{productDocWith("1"), nilDoc("2")},
			byID: map[string][]projection.BuildDoc{"2": {productDocWith("2")}},
		}
	}
	// nilChecks is every drift check of root 2 alone from the walk start.
	nilChecks := func(st *rebuildRecordingStore) int {
		n := 0
		for _, batch := range walkChecks(st) {
			if fmt.Sprint(batch) == fmt.Sprint([]ChangeCheck{{product("2"), 1001}}) {
				n++
			}
		}
		return n
	}

	for name, setup := range map[string]func(st *rebuildRecordingStore){
		"changed after the walk start": func(st *rebuildRecordingStore) {
			st.driftChildren = map[string]bool{"2": true}
			st.driftBudget.Store(1)
		},
		"drift check fails": func(st *rebuildRecordingStore) {
			st.errChildren = map[string]bool{"2": true}
			st.errBudget.Store(1)
		},
	} {
		t.Run(name+": re-marks, then re-builds", func(t *testing.T) {
			st := &rebuildRecordingStore{buildIdx: 41} // 1 begins at 42, 2 at 43
			setup(st)
			if err := rebuildAllProducts(t, st, walk()); err != nil {
				t.Fatal(err)
			}

			calls := st.callsSnapshot()
			if n := nilChecks(st); n != 1 {
				t.Fatalf("the deleted root must be checked once from the walk start, got %d: %v", n, st.checksSnapshot())
			}
			remove := callIndexes(calls, "RemoveResource:product/2:43")
			marks := callIndexes(calls, "MarkStale:product/2")
			begins := callIndexes(calls, "BeginBuild:product/2")
			if len(remove) != 1 || len(marks) != 1 || marks[0] < remove[0] {
				t.Fatalf("the delete must be followed by one re-mark of its root: %v", calls)
			}
			if len(begins) != 2 || begins[1] < marks[0] {
				t.Fatalf("the re-mark must claim and re-build the root: %v", calls)
			}
			if len(callIndexes(calls, "MarkStale:product/1")) != 0 {
				t.Fatalf("an unchanged root must not be re-marked: %v", calls)
			}
		})
	}

	t.Run("re-mark fails: fails the root", func(t *testing.T) {
		st := &rebuildRecordingStore{driftChildren: map[string]bool{"2": true}, markErrs: map[string]int{"product/2": 1}}
		st.driftBudget.Store(1)
		if err := rebuildAllProducts(t, st, walk()); err == nil {
			t.Fatal("a rebuild whose deleted root could not be re-marked must report failure")
		}

		calls := st.callsSnapshot()
		failed, marked := callIndexes(calls, "MarkStaleFailed:product/2"), callIndexes(calls, "MarkStale:product/2")
		if len(failed) != 1 || len(marked) != 1 || marked[0] < failed[0] {
			t.Fatalf("a failed re-mark must fail the root, which marks it stale for the sweep: %v", calls)
		}
		if len(callIndexes(calls, "BeginBuild:product/2")) != 1 {
			t.Fatalf("a failed re-mark claims nothing and re-builds nothing: %v", calls)
		}
	})

	t.Run("no change after the walk start: no re-mark", func(t *testing.T) {
		st := &rebuildRecordingStore{}
		if err := rebuildAllProducts(t, st, walk()); err != nil {
			t.Fatal(err)
		}

		calls := st.callsSnapshot()
		if n := nilChecks(st); n != 1 {
			t.Fatalf("the deleted root must be checked once from the walk start, got %d: %v", n, st.checksSnapshot())
		}
		if countPrefix(calls, "MarkStale:") != 0 || len(callIndexes(calls, "BeginBuild:product/2")) != 1 {
			t.Fatalf("no change after the walk start re-marks and re-builds nothing: %v", calls)
		}
	})
}

func TestRebuildAll_ChildDrift_RemarksResourceStale(t *testing.T) {
	st := &rebuildRecordingStore{driftChildren: map[string]bool{"cX": true}}
	st.driftBudget.Store(2)
	es := &captureBackend{}
	child := model.Resource{Type: "product", Id: "cX"}
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
		t.Fatal("a child that changed after the rebuild's start must re-mark the parent (ADR 0002 drift check)")
	}
	if st.has("MarkStale:product/2") {
		t.Fatal("a resource without drifted children must not be re-marked")
	}
}

// childDocs serves each root its own children, so the roots of one chunk
// carry distinct drift checks.
func childDocs(children map[string][]string) *staticExecuter {
	byID := make(map[string][]projection.BuildDoc, len(children))
	for id, cs := range children {
		byID[id] = []projection.BuildDoc{productDocWith(id, cs...)}
	}
	return &staticExecuter{byID: byID}
}

// rebuildIDs runs a single-plan rebuildByIDs of ids in one chunk and waits for
// the re-builds it schedules.
func rebuildIDs(t *testing.T, st *rebuildRecordingStore, exec *staticExecuter, ids ...string) {
	t.Helper()
	idx := newRebuildIndexer(st, &captureBackend{}, map[string][]projection.Plan{"product": {{Version: 1, Executer: exec}}}, 0)
	if err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product", ResourceIDs: ids}}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func sortedChecks(cs []ChangeCheck) string {
	cs = append([]ChangeCheck(nil), cs...)
	slices.SortFunc(cs, func(a, b ChangeCheck) int { return strings.Compare(a.Resource.Id, b.Resource.Id) })
	return fmt.Sprint(cs)
}

// checksBelow returns the recorded AnyChangedSince batches whose checks all
// carry a start of at most max — with rebuildRecordingStore's starts, the
// flusher's queries for the first max-100 roots, told apart from a later
// re-build's whatever the interleaving.
func checksBelow(st *rebuildRecordingStore, max int64) [][]ChangeCheck {
	var out [][]ChangeCheck
	for _, batch := range st.checksSnapshot() {
		if !slices.ContainsFunc(batch, func(c ChangeCheck) bool { return c.Start > max }) {
			out = append(out, batch)
		}
	}
	return out
}

func callIndexes(calls []string, call string) []int {
	var at []int
	for i, c := range calls {
		if c == call {
			at = append(at, i)
		}
	}
	return at
}

func countPrefix(calls []string, prefix string) int {
	n := 0
	for _, c := range calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

// One batched AnyChangedSince serves a no-drift chunk; every root's children
// are measured from that root's own BeginBuild start, and no root checks
// itself — rebuildByIDs begins a root before fetching it.
func TestRebuildByIDs_DriftCheck_ChecksEachRootsChildrenFromItsOwnStart(t *testing.T) {
	st := &rebuildRecordingStore{}
	rebuildIDs(t, st, childDocs(map[string][]string{"1": {"c1a", "c1b"}, "2": {"c2"}}), "1", "2")

	checks := st.checksSnapshot()
	if len(checks) != 1 {
		t.Fatalf("a no-drift chunk makes one batched drift check, got %d: %v", len(checks), st.callsSnapshot())
	}
	want := []ChangeCheck{{product("c1a"), 101}, {product("c1b"), 101}, {product("c2"), 102}}
	if got := sortedChecks(checks[0]); got != fmt.Sprint(want) {
		t.Fatalf("checks %s, want %v", got, want)
	}
	for _, c := range checks[0] {
		if c.Resource.Id == "1" || c.Resource.Id == "2" {
			t.Fatalf("a root begun before its fetch must not check itself: %v", checks[0])
		}
	}
}

func TestRebuildByIDs_NoRelations_MakesNoDriftCheck(t *testing.T) {
	st := &rebuildRecordingStore{}
	rebuildIDs(t, st, childDocs(map[string][]string{"1": nil, "2": nil}), "1", "2")

	if calls := st.callsSnapshot(); countPrefix(calls, "AnyChangedSince") != 0 {
		t.Fatalf("a chunk without children has nothing to check: %v", calls)
	}
}

// Only roots with children enter the drift check; with one such root the
// batched hit is that root's own, so it is not narrowed.
func TestRebuildByIDs_DriftCheck_OnlyRootsWithChildrenAreChecked(t *testing.T) {
	st := &rebuildRecordingStore{driftChildren: map[string]bool{"c2": true}}
	st.driftBudget.Store(1)
	rebuildIDs(t, st, childDocs(map[string][]string{"1": nil, "2": {"c2"}}), "1", "2")

	calls := st.callsSnapshot()
	marks := callIndexes(calls, "MarkStale:product/2")
	if len(marks) != 1 {
		t.Fatalf("the changed root must be re-marked once: %v", calls)
	}
	if got := checksBelow(st, 102); len(got) != 1 || sortedChecks(got[0]) != fmt.Sprint([]ChangeCheck{{product("c2"), 102}}) {
		t.Fatalf("one root with children: one batched check, no narrowing, got %v: %v", got, calls)
	}
	if len(callIndexes(calls, "MarkStale:product/1")) != 0 {
		t.Fatalf("a root without children must not be re-marked: %v", calls)
	}
}

// A hit narrows with one query per root, and only the root whose child
// changed is re-scheduled, mark first.
func TestRebuildByIDs_DriftHit_NarrowsAndReschedulesOnlyTheChangedRoot(t *testing.T) {
	// Budget 2: the batched hit and root 1's narrow hit; root 1's re-build
	// then sees no drift and settles.
	st := &rebuildRecordingStore{driftChildren: map[string]bool{"c1": true}}
	st.driftBudget.Store(2)
	rebuildIDs(t, st, childDocs(map[string][]string{"1": {"c1"}, "2": {"c2"}}), "1", "2")

	calls := st.callsSnapshot()
	marks := callIndexes(calls, "MarkStale:product/1")
	if len(marks) != 1 {
		t.Fatalf("the changed root must be re-marked once: %v", calls)
	}
	if len(callIndexes(calls, "MarkStale:product/2")) != 0 {
		t.Fatalf("a root whose children did not change must not be re-marked: %v", calls)
	}
	checks := checksBelow(st, 102)
	if len(checks) != 3 || len(checks[0]) != 2 {
		t.Fatalf("a hit must narrow: the batched query, then one per root, got %v: %v", checks, calls)
	}
	narrowed := []string{sortedChecks(checks[1]), sortedChecks(checks[2])}
	slices.Sort(narrowed)
	want := []string{
		fmt.Sprint([]ChangeCheck{{product("c1"), 101}}),
		fmt.Sprint([]ChangeCheck{{product("c2"), 102}}),
	}
	if !slices.Equal(narrowed, want) {
		t.Fatalf("each narrowed query carries one root's checks with its start, got %v want %v", narrowed, want)
	}
	begins := callIndexes(calls, "BeginBuild:product/1")
	if len(begins) != 2 || begins[1] < marks[0] {
		t.Fatalf("the re-mark must precede the root's re-build: %v", calls)
	}
	if len(callIndexes(calls, "BeginBuild:product/2")) != 1 {
		t.Fatalf("the unchanged root must not be re-built: %v", calls)
	}
}

// A failed drift query re-schedules: a redundant build is safe, a missed one
// is not.
func TestRebuildByIDs_DriftQueryError_ReschedulesTheRoot(t *testing.T) {
	st := &rebuildRecordingStore{errChildren: map[string]bool{"c1": true}}
	st.errBudget.Store(1)
	rebuildIDs(t, st, childDocs(map[string][]string{"1": {"c1"}}), "1")

	calls := st.callsSnapshot()
	marks := callIndexes(calls, "MarkStale:product/1")
	begins := callIndexes(calls, "BeginBuild:product/1")
	if len(marks) != 1 || len(begins) != 2 || begins[1] < marks[0] {
		t.Fatalf("a failed drift query must re-mark and re-build the root: %v", calls)
	}
}

// The narrowing query's error re-schedules its own root only.
func TestRebuildByIDs_NarrowDriftQueryError_ReschedulesThatRoot(t *testing.T) {
	// Budget 2: the batched query and root 1's narrow query fail.
	st := &rebuildRecordingStore{errChildren: map[string]bool{"c1": true}}
	st.errBudget.Store(2)
	rebuildIDs(t, st, childDocs(map[string][]string{"1": {"c1"}, "2": {"c2"}}), "1", "2")

	calls := st.callsSnapshot()
	marks := callIndexes(calls, "MarkStale:product/1")
	if checks := checksBelow(st, 102); len(marks) != 1 || len(checks) != 3 {
		t.Fatalf("a failed batched query narrows, and root 1's failed narrow re-marks it, got %v: %v", checks, calls)
	}
	if len(callIndexes(calls, "MarkStale:product/2")) != 0 {
		t.Fatalf("root 2's narrow query succeeded without drift; it must not be re-marked: %v", calls)
	}
}

// walkChecks returns the recorded AnyChangedSince batches whose checks all
// carry a walk start — with rebuildRecordingStore's starts (walks 1000+n,
// builds 100+BuildIdx), the plan walk flusher's queries, told apart from a
// drift re-schedule's live re-build whatever the interleaving.
func walkChecks(st *rebuildRecordingStore) [][]ChangeCheck {
	var out [][]ChangeCheck
	for _, batch := range st.checksSnapshot() {
		if !slices.ContainsFunc(batch, func(c ChangeCheck) bool { return c.Start <= 1000 }) {
			out = append(out, batch)
		}
	}
	return out
}

// rebuildAllProducts runs a plan walk of every product over the given plans'
// executers (Versions 1, 2, …) in one chunk and waits for the re-builds it
// schedules.
func rebuildAllProducts(t *testing.T, st *rebuildRecordingStore, execs ...*staticExecuter) error {
	t.Helper()
	plans := make([]projection.Plan, len(execs))
	for i, e := range execs {
		plans[i] = projection.Plan{Version: i + 1, Executer: e}
	}
	idx := newRebuildIndexer(st, &captureBackend{}, map[string][]projection.Plan{"product": plans}, 0)
	err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product"}})
	if werr := idx.WaitForIdle(t.Context()); werr != nil {
		t.Fatal(werr)
	}
	return err
}

// recordingExecute emits docs and records "Execute:v<version>" in the store's
// call log when the walk starts the plan.
func recordingExecute(st *rebuildRecordingStore, version int, docs ...projection.BuildDoc) *staticExecuter {
	return &staticExecuter{docs: docs, onExecute: func() { st.record("Execute:v%d", version) }}
}

// Each plan walk takes its own start, before the executer is asked for the
// first page: the aggregation pipeline may fetch as soon as Execute is called.
func TestRebuildAll_TakesOneWalkStartPerPlan_BeforeItsFirstFetch(t *testing.T) {
	st := &rebuildRecordingStore{}
	err := rebuildAllProducts(t, st,
		recordingExecute(st, 1, productDocWith("1")),
		recordingExecute(st, 2, productDocWith("1")))
	if err != nil {
		t.Fatal(err)
	}

	calls := st.callsSnapshot()
	starts := callIndexes(calls, "NextChangeSeq")
	exec1, exec2 := callIndexes(calls, "Execute:v1"), callIndexes(calls, "Execute:v2")
	if len(starts) != 2 || len(exec1) != 1 || len(exec2) != 1 {
		t.Fatalf("two plan walks take two walk starts, got %d: %v", len(starts), calls)
	}
	if starts[0] >= exec1[0] || exec1[0] >= starts[1] || starts[1] >= exec2[0] {
		t.Fatalf("each walk start must precede that plan's first fetch: %v", calls)
	}
}

// A walk's roots check themselves against the walk start — a root without
// relations too — because the walk fetched a root's page before its
// BeginBuild. Their children are checked against it as well.
func TestRebuildAll_RootsCheckThemselvesFromTheWalkStart(t *testing.T) {
	st := &rebuildRecordingStore{}
	err := rebuildAllProducts(t, st, &staticExecuter{docs: []projection.BuildDoc{
		productDocWith("1", "c1"), productDocWith("2"),
	}})
	if err != nil {
		t.Fatal(err)
	}

	checks := st.checksSnapshot()
	if len(checks) != 1 {
		t.Fatalf("a no-drift chunk makes one batched drift check, got %d: %v", len(checks), st.callsSnapshot())
	}
	want := []ChangeCheck{{product("1"), 1001}, {product("2"), 1001}, {product("c1"), 1001}}
	if got := sortedChecks(checks[0]); got != fmt.Sprint(want) {
		t.Fatalf("checks %s, want %v (the walk start, not BeginBuild's)", got, want)
	}
}

// A root changed after the walk start is re-scheduled like a changed child:
// marked first, then re-built. Here the changed root has no relations at all.
func TestRebuildAll_ChangedRoot_IsRescheduled(t *testing.T) {
	// Budget 2: the batched hit and root 2's narrow hit. Root 2's live
	// re-build is served root 2's own document: it checks children only, has
	// none, and so makes no drift query.
	st := &rebuildRecordingStore{driftChildren: map[string]bool{"2": true}}
	st.driftBudget.Store(2)
	exec := childDocs(map[string][]string{"1": {"c1"}, "2": nil})
	exec.docs = []projection.BuildDoc{productDocWith("1", "c1"), productDocWith("2")}
	err := rebuildAllProducts(t, st, exec)
	if err != nil {
		t.Fatal(err)
	}

	calls := st.callsSnapshot()
	marks := callIndexes(calls, "MarkStale:product/2")
	begins := callIndexes(calls, "BeginBuild:product/2")
	if len(marks) != 1 || len(begins) != 2 || begins[1] < marks[0] {
		t.Fatalf("a changed root must be re-marked, then re-built: %v", calls)
	}
	if len(callIndexes(calls, "MarkStale:product/1")) != 0 {
		t.Fatalf("an unchanged root must not be re-marked: %v", calls)
	}
	if checks := walkChecks(st); len(checks) != 3 {
		t.Fatalf("a hit must narrow: the batched query, then one per root, got %v: %v", checks, calls)
	}
	if all, walk := st.checksSnapshot(), walkChecks(st); len(all) != len(walk) {
		t.Fatalf("root 2's live re-build has no children and must make no drift query: %v", all)
	}
}

// A resource is begun on its first sighting only, so a later plan's walk
// start never replaces the start of the walk that first fetched it.
func TestRebuildAll_ResourceSeenByTwoPlans_KeepsItsFirstWalkStart(t *testing.T) {
	st := &rebuildRecordingStore{}
	err := rebuildAllProducts(t, st,
		&staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1")}},
		&staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1")}})
	if err != nil {
		t.Fatal(err)
	}

	checks := walkChecks(st)
	if len(checks) == 0 {
		t.Fatalf("root 1 must be checked: %v", st.checksSnapshot())
	}
	roots := 0
	for _, batch := range checks {
		for _, c := range batch {
			if c.Start != 1001 {
				t.Fatalf("every check of root 1 carries the first walk's start 1001: %v", checks)
			}
			if c.Resource.Id == "1" {
				roots++
			}
		}
	}
	if roots != 1 {
		t.Fatalf("a root checks itself once per chunk, not once per document: %v", checks)
	}
}

// One chunk can settle roots begun by different walks; each is checked
// against the start of the walk that first fetched it.
func TestRebuildAll_ChunkSpanningTwoWalks_ChecksEachRootFromItsOwnWalkStart(t *testing.T) {
	st := &rebuildRecordingStore{}
	err := rebuildAllProducts(t, st,
		&staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1")}},
		&staticExecuter{docs: []projection.BuildDoc{productDocWith("2", "c2"), productDocWith("1")}})
	if err != nil {
		t.Fatal(err)
	}

	checks := st.checksSnapshot()
	if len(checks) != 1 {
		t.Fatalf("both walks' roots settle in one chunk, one batched check, got %d: %v", len(checks), st.callsSnapshot())
	}
	want := []ChangeCheck{{product("1"), 1001}, {product("2"), 1002}, {product("c1"), 1001}, {product("c2"), 1002}}
	if got := sortedChecks(checks[0]); got != fmt.Sprint(want) {
		t.Fatalf("checks %s, want %v", got, want)
	}
	calls := st.callsSnapshot()
	for _, id := range []string{"1", "2"} {
		if len(callIndexes(calls, "ClearStale:product/"+id+":42")) != 1 {
			t.Fatalf("root %s received every document it expects and must complete: %v", id, calls)
		}
	}
}

// Without a walk start nothing can be checked, so the walk does not start.
func TestRebuildAll_WalkStartError_AbortsBeforeAnyFetch(t *testing.T) {
	st := &rebuildRecordingStore{changeSeqErr: errors.New("sequence unavailable")}
	err := rebuildAllProducts(t, st, recordingExecute(st, 1, productDocWith("1")))
	if err == nil || !strings.Contains(err.Error(), "sequence unavailable") {
		t.Fatalf("a failed walk start must fail the rebuild, got %v", err)
	}
	if calls := st.callsSnapshot(); countPrefix(calls, "Execute:") != 0 || countPrefix(calls, "BeginBuild:") != 0 {
		t.Fatalf("no plan may execute without a walk start: %v", calls)
	}
}

// A later plan's failed walk start aborts the walk before that plan executes,
// and salvage marks the roots an earlier plan began — still awaiting the
// failed plan's documents — stale instead of clearing them.
func TestRebuildAll_LaterWalkStartError_SalvagesBegunRoots(t *testing.T) {
	st := &rebuildRecordingStore{changeSeqErr: errors.New("sequence unavailable"), changeSeqErrFrom: 2}
	err := rebuildAllProducts(t, st,
		recordingExecute(st, 1, productDocWith("1")),
		recordingExecute(st, 2, productDocWith("1")))
	if err == nil || !strings.Contains(err.Error(), "sequence unavailable") {
		t.Fatalf("a failed walk start must fail the rebuild, got %v", err)
	}
	calls := st.callsSnapshot()
	if countPrefix(calls, "Execute:v1") != 1 || countPrefix(calls, "BeginBuild:product/1") != 1 {
		t.Fatalf("plan 1 must execute and begin root 1: %v", calls)
	}
	if countPrefix(calls, "Execute:v2") != 0 {
		t.Fatalf("plan 2 may not execute without a walk start: %v", calls)
	}
	if countPrefix(calls, "MarkStale:product/1") == 0 {
		t.Fatalf("root 1 was begun but never settled; salvage must mark it stale: %v", calls)
	}
	if countPrefix(calls, "ClearStale:product/1") != 0 {
		t.Fatalf("an unsettled root must not be cleared: %v", calls)
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
			t.Fatalf("resource %s was begun but never completed — it must be marked stale so the sweep repairs it", id)
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
	// v1's document and edge set refreshed, v2's not, and no stale mark.
	if len(cps) != 0 {
		t.Fatalf("a multi-plan walk has no settled mid-walk position and must never checkpoint, got %v", cps)
	}
	if !st.has("ClearStale:product/1") {
		t.Fatal("the walk must still complete normally")
	}
	// Both documents land in the one flush, so one call carries both sets.
	assertNoWipe(t, st, product("1"))
	assertReplaces(t, st, product("1"), 1, []EdgeSet{versionSet(1), versionSet(2)})
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
	// Only the selected version's set is replaced: the non-targeted
	// version's plan did not run, and its set stays untouched.
	assertNoWipe(t, st, product("1"))
	assertReplaces(t, st, product("1"), 1, []EdgeSet{versionSet(2)})
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
	// From scratch, both plans run, and both versions' sets are replaced.
	assertNoWipe(t, st, product("1"))
	assertReplaces(t, st, product("1"), 1, []EdgeSet{versionSet(1), versionSet(2)})
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
	// The walk's only plan ran from the listing's head and replaced its
	// version's set.
	assertNoWipe(t, st, product("1"))
	assertReplaces(t, st, product("1"), 1, []EdgeSet{versionSet(1)})
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
			t.Fatalf("resource %s was begun but never landed — salvage must mark it stale", id)
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

// assertDriftRemarkFailureFailsRoot checks ruling R6 for a root whose drift
// re-schedule could not mark it: it is not cleared, the rebuild reports it
// failed, and its mark is retried — and lands — after the failed attempt.
func assertDriftRemarkFailureFailsRoot(t *testing.T, st *rebuildRecordingStore, err error, id string) {
	t.Helper()
	calls := st.callsSnapshot()
	if err == nil || !strings.Contains(err.Error(), "failed 1 resource(s)") {
		t.Fatalf("a root whose drift re-mark failed must fail the rebuild, got %v: %v", err, calls)
	}
	if n := countPrefix(calls, "ClearStale:product/"+id+":"); n != 0 {
		t.Fatalf("a root whose drift re-mark failed must not be cleared — nothing else would recover it: %v", calls)
	}
	failed, marks := callIndexes(calls, "MarkStaleFailed:product/"+id), callIndexes(calls, "MarkStale:product/"+id)
	if len(failed) == 0 || len(marks) == 0 || marks[len(marks)-1] < failed[0] {
		t.Fatalf("the failed re-mark must be retried and land: %v", calls)
	}
}

// A walk root whose drift hit cannot be re-marked fails instead of clearing;
// a root of the same chunk without drift still completes.
func TestRebuildAll_RootDriftRemarkFails_FailsTheRoot(t *testing.T) {
	// Budget 2: the batched hit and root 1's narrow hit.
	st := &rebuildRecordingStore{driftChildren: map[string]bool{"1": true}, markErrs: map[string]int{"product/1": 1}}
	st.driftBudget.Store(2)
	err := rebuildAllProducts(t, st, &staticExecuter{docs: []projection.BuildDoc{productDocWith("1"), productDocWith("2")}})

	assertDriftRemarkFailureFailsRoot(t, st, err, "1")
	if calls := st.callsSnapshot(); len(callIndexes(calls, "ClearStale:product/2:42")) != 1 {
		t.Fatalf("root 2 had no drift and must complete: %v", calls)
	}
}

// The same for a child-drift hit on a targeted rebuild.
func TestRebuildByIDs_ChildDriftRemarkFails_FailsTheRoot(t *testing.T) {
	// Budget 2: the batched hit and root 1's narrow hit.
	st := &rebuildRecordingStore{driftChildren: map[string]bool{"c1": true}, markErrs: map[string]int{"product/1": 1}}
	st.driftBudget.Store(2)
	exec := childDocs(map[string][]string{"1": {"c1"}, "2": {"c2"}})
	idx := newRebuildIndexer(st, &captureBackend{}, map[string][]projection.Plan{"product": {{Version: 1, Executer: exec}}}, 0)
	err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product", ResourceIDs: []string{"1", "2"}}})
	if werr := idx.WaitForIdle(t.Context()); werr != nil {
		t.Fatal(werr)
	}

	assertDriftRemarkFailureFailsRoot(t, st, err, "1")
	if calls := st.callsSnapshot(); len(callIndexes(calls, "ClearStale:product/2:42")) != 1 {
		t.Fatalf("root 2 had no drift and must complete: %v", calls)
	}
}

// A walk cancelled between its write and the drift check: the drift query
// errors and the re-mark fails on the cancelled context, so the root fails
// and its mark is retried on a context detached from cancellation. The only
// flush is finish's — after the walk's own ctx checks — and finish succeeds,
// so salvage never runs: the retried mark is the flusher's.
func TestRebuild_CancelledBeforeDriftCheck_FailsTheRoot(t *testing.T) {
	cases := map[string]struct {
		exec *staticExecuter
		sel  ResourceSelector
	}{
		"plan walk": {
			exec: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1")}},
			sel:  ResourceSelector{ResourceType: "product"},
		},
		"by IDs": {
			exec: childDocs(map[string][]string{"1": {"c1"}}),
			sel:  ResourceSelector{ResourceType: "product", ResourceIDs: []string{"1"}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st := &rebuildRecordingStore{ctxAware: true}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			es := &captureBackend{onBulk: cancel}
			idx := newRebuildIndexer(st, es, map[string][]projection.Plan{"product": {{Version: 1, Executer: tc.exec}}}, 0)

			err := idx.RebuildNow(ctx, []ResourceSelector{tc.sel})
			if werr := idx.WaitForIdle(t.Context()); werr != nil {
				t.Fatal(werr)
			}

			if calls := st.callsSnapshot(); len(es.bulkCalls) != 1 || countPrefix(calls, "AnyChangedSince:") != 1 {
				t.Fatalf("one flush, then its (failing) drift query: %d flushes: %v", len(es.bulkCalls), calls)
			}
			assertDriftRemarkFailureFailsRoot(t, st, err, "1")
		})
	}
}

func (s *rebuildRecordingStore) RegisterChanges(context.Context, []Registration, time.Duration) (Registered, error) {
	panic("RegisterChanges: not implemented")
}

// A flush whose edge replace fails because the walk's context ended — here
// the last flush of a checkpointing single-plan walk, where no salvage
// follows — still marks the resource stale, on a context detached from the
// walk's: the checkpoint the flush reports steps over it, so the mark is its
// only recovery.
func TestRebuildAll_EdgeReplaceFailsOnCancellation_MarksTheResourceStaleDetached(t *testing.T) {
	st := &rebuildRecordingStore{ctxAware: true, replaceErrs: map[string]error{"1": context.Canceled}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	es := &captureBackend{onBulk: cancel}
	plans := map[string][]projection.Plan{"product": {{Version: 1, Executer: &cursorPagingExecuter{
		pages: []aggregation.ExecutionResult[projection.BuildDoc]{{Items: []projection.BuildDoc{productDoc("1")}, NextPageToken: "p2"}},
	}}}}
	idx := newRebuildIndexer(st, es, plans, 0)

	var cps []RebuildCursor
	err := idx.RebuildNowResumable(ctx, ResourceSelector{ResourceType: "product"}, nil,
		func(c RebuildCursor) { cps = append(cps, c) })
	if err == nil {
		t.Fatal("a walk whose resource was not settled must not report success")
	}

	calls := st.callsSnapshot()
	if countPrefix(calls, "MarkStale:product/1") == 0 {
		t.Fatalf("a resource whose edge replace failed on cancellation must still be durably marked stale: %v (checkpoints %v)", calls, cps)
	}
	if countPrefix(calls, "ClearStale:product/1") != 0 {
		t.Fatalf("a resource whose edges were not stored must not clear its mark: %v", calls)
	}
}

// A flush whose document is rejected while the walk's context ends — here
// the last flush of a checkpointing single-plan walk, where no salvage
// follows — marks the resource stale on a context detached from the walk's
// before the flush reports its checkpoint: the checkpoint steps over the
// resource, so the mark is its only recovery.
func TestRebuildAll_RejectedDocumentOnCancellation_MarksTheResourceStaleBeforeTheCheckpoint(t *testing.T) {
	st := &rebuildRecordingStore{ctxAware: true}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	es := &captureBackend{rejectIDs: map[string]bool{"1": true}, onBulk: cancel}
	plans := map[string][]projection.Plan{"product": {{Version: 1, Executer: &cursorPagingExecuter{
		pages: []aggregation.ExecutionResult[projection.BuildDoc]{{Items: []projection.BuildDoc{productDoc("1")}, NextPageToken: "p2"}},
	}}}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNowResumable(ctx, ResourceSelector{ResourceType: "product"}, nil,
		func(c RebuildCursor) { st.record("Checkpoint:%d/%v", c.PlanVersion, c.PageToken) })
	if err == nil {
		t.Fatal("a walk whose resource was not settled must not report success")
	}

	calls := st.callsSnapshot()
	marks, cps := callIndexes(calls, "MarkStale:product/1"), callIndexes(calls, "Checkpoint:1/p2")
	if len(marks) == 0 {
		t.Fatalf("a resource with a rejected document must be durably marked stale even on a cancelled walk: %v", calls)
	}
	if len(cps) != 0 && cps[0] < marks[0] {
		t.Fatalf("the checkpoint must not step over the resource before it is durably marked: %v", calls)
	}
	if countPrefix(calls, "ClearStale:product/1") != 0 {
		t.Fatalf("a resource with a rejected document must not clear its mark: %v", calls)
	}
}
