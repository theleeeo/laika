package core

// Build runs its ids in batches (ADR 0014): it cuts them into chunks of the
// type's batch size, begins each chunk in one BeginBuilds, and asks each plan
// about each chunk once per metadata group. Each id keeps its own Build
// Sequence, write, edge set and finish. A batch whose plan call fails fails
// every id in it, released with backoff and logged once at Error; a plan's
// error for one id fails that id alone. A batch's Parents are marked in one
// call and its drift check is one query that narrows on a hit.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// requests is every request e received, in order.
func (e *recordingExecuter) requests() []projection.BuildRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.reqs)
}

// askedOnce fails the test unless ex's requests asked about each of ids
// exactly once.
func askedOnce(t *testing.T, name string, ex *recordingExecuter, ids []string) {
	t.Helper()
	asked := make(map[string]int, len(ids))
	for _, r := range ex.requests() {
		for _, id := range r.ResourceIDs {
			asked[id]++
		}
	}
	for _, id := range ids {
		if asked[id] != 1 {
			t.Fatalf("%s asked about %s %d times, want once", name, id, asked[id])
		}
	}
	if len(asked) != len(ids) {
		t.Fatalf("%s asked about %d ids, want %d", name, len(asked), len(ids))
	}
}

// occupyPool parks the pool's only worker until the returned release is
// called, so the builds a Build submits wait in the queue.
func occupyPool(t *testing.T, idx *Indexer) (release func()) {
	t.Helper()
	block, started := make(chan struct{}), make(chan struct{})
	if !idx.pool.trySubmit(func(context.Context) { close(started); <-block }) {
		t.Fatal("failed to occupy the pool")
	}
	within(t, started, "the blocking task")
	var once sync.Once
	return func() { once.Do(func() { close(block) }) }
}

func TestBuild_CutsItsIDsIntoChunksOfTheTypesBatchSize(t *testing.T) {
	st := &recordingStore{}
	v1, v2 := &recordingExecuter{}, &recordingExecuter{}
	idx := mustNew(Config{
		Resources:       twoVersionResources(),
		Plans:           map[string][]projection.Plan{"product": {{Version: 1, Executer: v1}, {Version: 2, Executer: v2}}},
		ES:              &captureBackend{},
		Store:           st,
		BuildBatchSize:  100,
		BuildBatchSizes: map[string]int{"product": 50},
	})
	ids := productIDs(250)

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: ids}); err != nil {
		t.Fatal(err)
	}

	if n := st.count("BeginBuilds:"); n != 5 {
		t.Fatalf("want five chunks of 50, each begun in one BeginBuilds, got %d BeginBuilds", n)
	}
	if n := st.count("BeginBuild:"); n != 0 {
		t.Fatalf("no id may be begun alone, got %d BeginBuild", n)
	}
	for name, ex := range map[string]*recordingExecuter{"v1": v1, "v2": v2} {
		reqs := ex.requests()
		if len(reqs) != 5 {
			t.Fatalf("%s's plan must be asked once per chunk, got %d requests", name, len(reqs))
		}
		for i, r := range reqs {
			if !slices.Equal(r.ResourceIDs, ids[i*50:(i+1)*50]) {
				t.Fatalf("%s's request %d asks about %v, want chunk %d in order", name, i, r.ResourceIDs, i)
			}
		}
	}
}

func TestBuild_OwnedIDsOfTwoMetadataValues_AskEachPlanOncePerChunkAndGroup_EachIDOnItsOwnSequence(t *testing.T) {
	st := &recordingStore{}
	ids := productIDs(250)
	actor := func(i int) map[string]string {
		if i%2 == 0 {
			return map[string]string{"actor": "a"}
		}
		return map[string]string{"actor": "b"}
	}
	for i, id := range ids {
		st.seedMetadata(product(id), actor(i))
	}
	tokens := claim(t, st, ids...)
	v1, v2 := &recordingExecuter{}, &recordingExecuter{}
	be := &captureBackend{}
	idx := mustNew(Config{
		Resources: twoVersionResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: v1}, {Version: 2, Executer: v2}}},
		ES:        be,
		Store:     st,
	})

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: ids, OwnerTokens: tokens}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	// The default batch size is 100: chunks of 100, 100 and 50, each begun
	// in one statement with every id's own token.
	if n := st.count("BeginBuilds:"); n != 3 {
		t.Fatalf("want one BeginBuilds per chunk of 100, got %d: %v", n, st.callsSnapshot()[:min(5, len(st.callsSnapshot()))])
	}
	for _, c := range st.callsSnapshot() {
		if !strings.HasPrefix(c, "BeginBuilds:") {
			continue
		}
		for _, b := range strings.Split(strings.TrimPrefix(c, "BeginBuilds:"), ",") {
			id, tok, _ := strings.Cut(strings.TrimPrefix(b, "product/"), ":")
			if tok != fmt.Sprint(tokens[id]) {
				t.Fatalf("%s must be begun with its owner token %d, got %s", id, tokens[id], b)
			}
		}
	}

	// Each plan is asked once per chunk and metadata group, every request
	// holding only ids whose rows hold its metadata.
	rowIdx := make(map[string]int, len(ids))
	for i, id := range ids {
		rowIdx[id] = i
	}
	for name, ex := range map[string]*recordingExecuter{"v1": v1, "v2": v2} {
		reqs := ex.requests()
		if len(reqs) != 6 {
			t.Fatalf("%s's plan must be asked once per chunk and metadata group (3×2), got %d requests", name, len(reqs))
		}
		for _, r := range reqs {
			for _, id := range r.ResourceIDs {
				if !maps.Equal(r.Metadata, actor(rowIdx[id])) {
					t.Fatalf("%s: %s fetched with %v, its row holds %v", name, id, r.Metadata, actor(rowIdx[id]))
				}
			}
		}
		askedOnce(t, name, ex, ids)
	}

	// Every id's write, edge set and finish carry its own Build Sequence and
	// its own stale_seq and token.
	items := make(map[string][]BulkItem)
	for _, it := range be.allBulkItems() {
		items[it.ID] = append(items[it.ID], it)
	}
	edges := make(map[string][]edgeReplace)
	for _, r := range st.replacedSnapshot() {
		edges[r.resource.Id] = append(edges[r.resource.Id], r)
	}
	seqs := make(map[int64]string, len(ids))
	for _, id := range ids {
		begun := st.begunOf(product(id))
		if len(begun) != 1 {
			t.Fatalf("%s begun %d times, want once", id, len(begun))
		}
		bb := begun[0]
		if other, dup := seqs[bb.BuildIdx]; dup {
			t.Fatalf("%s and %s share Build Sequence %d", id, other, bb.BuildIdx)
		}
		seqs[bb.BuildIdx] = id
		if len(items[id]) != 2 {
			t.Fatalf("%s: want one write per version, got %+v", id, items[id])
		}
		for _, it := range items[id] {
			if it.Version != bb.BuildIdx {
				t.Fatalf("%s written at %d, its Build Sequence is %d", id, it.Version, bb.BuildIdx)
			}
		}
		if len(edges[id]) != 1 || edges[id][0].buildSeq != bb.BuildIdx {
			t.Fatalf("%s: want one edge write at its Build Sequence %d, got %+v", id, bb.BuildIdx, edges[id])
		}
		if bb.StaleSeq != tokens[id] {
			t.Fatalf("%s begun with stale_seq %d, its claim was %d", id, bb.StaleSeq, tokens[id])
		}
		if st.indexOf(fmt.Sprintf("FinishOwned:product/%s:%d:%d", id, bb.StaleSeq, tokens[id])) == -1 {
			t.Fatalf("%s must finish with its own stale_seq and token", id)
		}
		if r, _ := st.row(product(id)); r.stale || r.owner != 0 {
			t.Fatalf("%s must be finished: %+v", id, r)
		}
	}
}

// One id's error fails it alone: it is released with a backoff, the others
// are written and finished.
func TestBuild_OneIDsError_FailsOnlyThatID(t *testing.T) {
	st := &recordingStore{}
	tokens := claim(t, st, "1", "2", "3")
	be := &captureBackend{}
	ex := &staticExecuter{
		docs: []projection.BuildDoc{productDoc("x")},
		byID: map[string][]projection.BuildDoc{"2": {errDoc("2", errors.New("source failed for 2"))}},
	}
	idx := mustNew(Config{
		Resources: testResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}}},
		ES:        be,
		Store:     st,
	})

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1", "2", "3"}, OwnerTokens: tokens}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if n := st.count("BeginBuilds:"); n != 1 {
		t.Fatalf("one chunk, one BeginBuilds, got %d", n)
	}
	for _, id := range []string{"1", "3"} {
		if st.indexOf(fmt.Sprintf("FinishOwned:product/%s:", id)) == -1 {
			t.Fatalf("%s must finish: %v", id, st.callsSnapshot())
		}
	}
	if st.indexOf("FinishOwned:product/2:") != -1 || st.indexOf("ReplaceEdges:product/2:") != -1 {
		t.Fatalf("the failed id must neither store edges nor finish: %v", st.callsSnapshot())
	}
	if st.indexOf(fmt.Sprintf("ReleaseFailed:product/2:%d", tokens["2"])) == -1 || st.count("ReleaseFailed:") != 1 {
		t.Fatalf("only the failed id is released with a backoff: %v", st.callsSnapshot())
	}
	if r, _ := st.row(product("2")); r.attempts != 1 || !r.stale {
		t.Fatalf("the failed id keeps its mark and is backed off: %+v", r)
	}
	for _, it := range be.allBulkItems() {
		if it.ID == "2" {
			t.Fatalf("the failed id must not be written: %+v", it)
		}
	}
}

// A failing plan call fails every id of its batch: each is released with a
// backoff, one Error line names the batch, and no line about one of its rows
// is logged above Debug.
func TestBuild_FailingPlanCall_FailsTheBatch_LogsOneErrorLine(t *testing.T) {
	logs := captureDefaultLogs(t)
	st := &recordingStore{}
	ids := productIDs(12)
	md := map[string]string{"actor": "a"}
	for _, id := range ids {
		st.seedMetadata(product(id), md)
	}
	tokens := claim(t, st, ids...)
	be := &captureBackend{}
	ex := &recordingExecuter{failIDs: map[string]bool{"5": true}}
	idx := mustNew(Config{
		Resources: testResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}}},
		ES:        be,
		Store:     st,
	})

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: ids, OwnerTokens: tokens}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if n := len(ex.requests()); n != 1 {
		t.Fatalf("the batch's plan is asked once, got %d requests", n)
	}
	if n := st.count("ReleaseFailed:"); n != len(ids) {
		t.Fatalf("every id of the batch is released with a backoff, got %d: %v", n, st.callsSnapshot())
	}
	for _, id := range ids {
		if r, _ := st.row(product(id)); r.attempts != 1 || !r.stale || r.owner != 0 {
			t.Fatalf("%s keeps its mark, released and backed off: %+v", id, r)
		}
	}
	if st.count("FinishOwned") != 0 || st.count("ReplaceEdges") != 0 || len(be.allBulkItems()) != 0 {
		t.Fatalf("a failed batch writes and finishes nothing: %v", st.callsSnapshot())
	}

	var errLines []map[string]any
	for _, rec := range logs.records(t) {
		if rec["level"] == "ERROR" {
			errLines = append(errLines, rec)
		}
		if _, perRow := rec["id"]; perRow && rec["level"] != "DEBUG" {
			t.Fatalf("no line about one row of a failed batch may be logged above Debug: %v", rec)
		}
	}
	if len(errLines) != 1 {
		t.Fatalf("want exactly one Error line, got %d: %v", len(errLines), errLines)
	}
	line := errLines[0]
	if line["type"] != "product" || line["count"] != float64(len(ids)) {
		t.Fatalf("the Error line must name the type and the count: %v", line)
	}
	if got := fmt.Sprint(line["metadata"]); got != fmt.Sprint(map[string]any{"actor": "a"}) {
		t.Fatalf("the Error line must name the batch's metadata, got %v", line["metadata"])
	}
	var first []string
	for _, v := range line["ids"].([]any) {
		first = append(first, v.(string))
	}
	if !slices.Equal(first, ids[:10]) {
		t.Fatalf("the Error line must name the first ten ids, got %v", first)
	}
	if e, _ := line["error"].(string); !strings.Contains(e, "plan failed") {
		t.Fatalf("the Error line must carry the error, got %v", line["error"])
	}
}

// A batch's Parents are marked in one call, its drift check is one query
// that narrows to the drifted root on a hit, and the drifted roots are
// re-marked in one call — all before any id finishes.
func TestBuild_BatchMarksItsParentsOnce_AndItsDriftCheckNarrowsOnAHit(t *testing.T) {
	st := &recordingStore{}
	tokens := claim(t, st, "1", "2", "3")
	ex := &recordingExecuter{
		relations: map[string][]model.Resource{"1": {product("c1")}, "2": {product("c2")}, "3": {product("c3")}},
		parents:   map[string][]model.Resource{"1": {product("p1")}, "2": {product("p2")}, "3": {product("p1"), product("p3")}},
	}
	idx := mustNew(Config{
		Resources: testResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}}},
		ES:        &captureBackend{},
		Store:     st,
		PoolSize:  1,
		QueueSize: 16,
	})
	release := occupyPool(t, idx)
	defer release()
	// c2 changes after the batch began and before its drift check.
	var once sync.Once
	st.onCheck = func([]ChangeCheck) {
		once.Do(func() {
			if _, err := st.RegisterChanges(context.Background(), []Registration{{Resource: product("c2")}}, time.Minute); err != nil {
				t.Error(err)
			}
		})
	}
	before := len(st.callsSnapshot())
	marksBefore := len(st.marksSnapshot())

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1", "2", "3"}, OwnerTokens: tokens}); err != nil {
		t.Fatal(err)
	}
	calls := st.callsSnapshot()[before:]
	marks := st.marksSnapshot()[marksBefore:]
	checks := st.checksSnapshot()
	release()
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if len(marks) != 2 {
		t.Fatalf("want one mark of the Parents and one of the drifted roots, got %v", marks)
	}
	parents := slices.SortedFunc(slices.Values(marks[0]), func(a, b model.Resource) int { return strings.Compare(a.Id, b.Id) })
	if fmt.Sprint(parents) != fmt.Sprint([]model.Resource{product("p1"), product("p2"), product("p3")}) {
		t.Fatalf("the Parents of every id are marked once in one call, got %v", marks[0])
	}
	if fmt.Sprint(marks[1]) != fmt.Sprint([]model.Resource{product("2")}) {
		t.Fatalf("only the drifted root is re-marked, got %v", marks[1])
	}

	firstFinish, firstCheck := -1, -1
	var markAt []int
	for i, c := range calls {
		switch {
		case strings.HasPrefix(c, "FinishOwned:") && firstFinish == -1:
			firstFinish = i
		case strings.HasPrefix(c, "AnyChangedSince:") && firstCheck == -1:
			firstCheck = i
		case strings.HasPrefix(c, "MarkStale:"):
			markAt = append(markAt, i)
		}
	}
	if firstFinish == -1 || markAt[0] > firstCheck || markAt[1] > firstFinish {
		t.Fatalf("the Parents are marked before the drift check, the drifted roots before any id finishes: %v", calls)
	}

	if len(checks) != 4 || len(checks[0]) != 3 {
		t.Fatalf("want one batched check of every child, then one per root on its hit, got %v", checks)
	}
	for _, c := range checks[1:] {
		if len(c) != 1 {
			t.Fatalf("a narrowing check holds one root's children, got %v", c)
		}
	}
	if fus := st.followUpsOf(product("2")); len(fus) != 1 {
		t.Fatalf("the drifted root's finish hands on a follow-up, got %v", fus)
	}
	for _, id := range []string{"1", "3"} {
		if fus := st.followUpsOf(product(id)); len(fus) != 0 {
			t.Fatalf("%s didn't drift and hands on nothing, got %v", id, fus)
		}
	}
}

// The build follow-ups a Build's finishes hand on are submitted together, as
// one owned build: its BeginBuilds begins them all.
func TestBuild_FollowUpsAreSubmittedAsOneOwnedBuild(t *testing.T) {
	st := &recordingStore{}
	tokens := claim(t, st, "1", "2", "3")
	ex := &recordingExecuter{}
	idx := mustNew(Config{
		Resources: testResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}}},
		ES:        &captureBackend{},
		Store:     st,
		PoolSize:  1,
		QueueSize: 16,
	})
	// Changes to 1 and 3 are registered while the build runs, so their
	// finishes re-claim them for follow-ups.
	var once sync.Once
	st.onReplace = func(edgeReplace) {
		once.Do(func() {
			if _, err := st.RegisterChanges(context.Background(), []Registration{{Resource: product("1")}, {Resource: product("3")}}, time.Minute); err != nil {
				t.Error(err)
			}
		})
	}

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1", "2", "3"}, OwnerTokens: tokens}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	fu1, fu3 := st.followUpsOf(product("1")), st.followUpsOf(product("3"))
	if len(fu1) != 1 || len(fu3) != 1 {
		t.Fatalf("1 and 3 must each hand on one follow-up, got %v %v", fu1, fu3)
	}
	var begins []string
	for _, c := range st.callsSnapshot() {
		if strings.HasPrefix(c, "BeginBuilds:") {
			begins = append(begins, c)
		}
	}
	want := fmt.Sprintf("BeginBuilds:product/1:%d,product/3:%d", fu1[0].Token, fu3[0].Token)
	if len(begins) != 2 || begins[1] != want {
		t.Fatalf("the follow-ups must run as one owned build (%s), got %v", want, begins)
	}
}

// A rejected document fails its id alone; a bulk request that fails as a
// whole fails every id it wrote for. A failed id stores no edges and doesn't
// finish.
func TestBuild_BulkRejection_FailsOnlyTheRejectedID_ARequestErrorEveryWrittenID(t *testing.T) {
	for name, tc := range map[string]struct {
		be     *captureBackend
		failed []string
	}{
		"rejected item": {be: &captureBackend{rejectIDs: map[string]bool{"2": true}}, failed: []string{"2"}},
		"request error": {be: &captureBackend{bulkErr: errors.New("es down")}, failed: []string{"1", "2", "3"}},
	} {
		t.Run(name, func(t *testing.T) {
			st := &recordingStore{}
			tokens := claim(t, st, "1", "2", "3")
			idx := mustNew(Config{
				Resources: testResources(),
				Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: &recordingExecuter{}}}},
				ES:        tc.be,
				Store:     st,
			})

			if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1", "2", "3"}, OwnerTokens: tokens}); err != nil {
				t.Fatal(err)
			}

			if n := len(tc.be.bulkCalls); n != 1 {
				t.Fatalf("one bulk write per batch, got %d", n)
			}
			for _, id := range []string{"1", "2", "3"} {
				failed := slices.Contains(tc.failed, id)
				released := st.indexOf(fmt.Sprintf("ReleaseFailed:product/%s:%d", id, tokens[id])) != -1
				finished := st.indexOf(fmt.Sprintf("FinishOwned:product/%s:", id)) != -1
				edges := st.indexOf(fmt.Sprintf("ReplaceEdges:product/%s:", id)) != -1
				if released != failed || finished == failed || edges == failed {
					t.Fatalf("%s (failed %v): released %v, edges %v, finished %v: %v", id, failed, released, edges, finished, st.callsSnapshot())
				}
			}
		})
	}
}

// A Build may mix owned and unowned ids: an owned id fetches with its row's
// metadata, an unowned one with the caller's, and ids whose metadata is
// equal by content — nil equal to empty — are asked about together.
func TestBuild_MixedOwnership_GroupsByEachIDsMetadataByContent(t *testing.T) {
	st := &recordingStore{}
	st.seedMetadata(product("1"), map[string]string{"actor": "a"})
	st.seedMetadata(product("2"), map[string]string{})
	tokens := claim(t, st, "1", "2")
	ex := &recordingExecuter{}
	idx := mustNew(Config{
		Resources: testResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}}},
		ES:        &captureBackend{},
		Store:     st,
	})

	// 3 owns nothing and fetches with the caller's metadata, equal to 1's
	// row's; 2's row holds none.
	if err := idx.Build(t.Context(), BuildArgs{
		ResourceType: "product", ResourceIds: []string{"1", "2", "3"}, OwnerTokens: tokens,
		Metadata: map[string]string{"actor": "a"},
	}); err != nil {
		t.Fatal(err)
	}

	if n := st.count("BeginBuilds:"); n != 1 {
		t.Fatalf("one batch, one BeginBuilds, got %d", n)
	}
	want := fmt.Sprintf("BeginBuilds:product/1:%d,product/2:%d,product/3:0", tokens["1"], tokens["2"])
	if st.indexOf(want) == -1 {
		t.Fatalf("each id begins with its own token, 0 for the unowned one (%s): %v", want, st.callsSnapshot())
	}
	reqs := ex.requests()
	if len(reqs) != 2 {
		t.Fatalf("want one request per metadata group, got %d: %+v", len(reqs), reqs)
	}
	got := map[string]map[string]string{}
	for _, r := range reqs {
		got[strings.Join(r.ResourceIDs, ",")] = r.Metadata
	}
	if md, ok := got["1,3"]; !ok || !maps.Equal(md, map[string]string{"actor": "a"}) {
		t.Fatalf("1 and 3 fetch together with actor a, got %v", got)
	}
	if md, ok := got["2"]; !ok || len(md) != 0 {
		t.Fatalf("2 fetches alone with none, got %v", got)
	}
	if st.indexOf("ClearStale:product/3:") == -1 || st.indexOf("FinishOwned:product/1:") == -1 || st.indexOf("FinishOwned:product/2:") == -1 {
		t.Fatalf("an unowned id clears, an owned one finishes owned: %v", st.callsSnapshot())
	}
}

// A batch whose BeginBuilds fails fails every id in it: one Error line, with
// no metadata since the batch never grouped, and each owned id released with
// a backoff.
func TestBuild_FailingBeginBuilds_FailsTheBatch_LogsOneErrorLine(t *testing.T) {
	logs := captureDefaultLogs(t)
	st := &recordingStore{}
	tokens := claim(t, st, "1", "2", "3")
	st.beginErr = errors.New("deadlock detected")
	ex := &recordingExecuter{}
	idx := mustNew(Config{
		Resources: testResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}}},
		ES:        &captureBackend{},
		Store:     st,
	})

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1", "2", "3"}, OwnerTokens: tokens}); err != nil {
		t.Fatal(err)
	}

	if len(ex.requests()) != 0 || st.count("ReleaseFailed:") != 3 {
		t.Fatalf("nothing is fetched, every id is released with a backoff: %v", st.callsSnapshot())
	}
	var errLines int
	for _, rec := range logs.records(t) {
		if rec["level"] == "ERROR" {
			errLines++
			if e, _ := rec["error"].(string); rec["count"] != float64(3) || !strings.Contains(e, "deadlock detected") {
				t.Fatalf("the Error line names the count and the error: %v", rec)
			}
		}
		if _, perRow := rec["id"]; perRow && rec["level"] != "DEBUG" {
			t.Fatalf("no per-row line above Debug: %v", rec)
		}
	}
	if errLines != 1 {
		t.Fatalf("want one Error line, got %d", errLines)
	}
}

// R5: a batch whose plan call fails once ctx has ended is not a failure of
// the batch: no Error line, its owned ids are released without a backoff,
// and Build returns ctx's error.
func TestBuild_PlanCallFailingAfterCtxEnded_ReleasesWithoutBackoff(t *testing.T) {
	logs := captureDefaultLogs(t)
	st := &recordingStore{}
	tokens := claim(t, st, "1", "2")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ex := &recordingExecuter{failIDs: map[string]bool{"1": true}}
	idx := mustNew(Config{
		Resources: testResources(),
		Plans: map[string][]projection.Plan{"product": {{Version: 1, Executer: executerFunc(func(c context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
			cancel()
			return ex.Execute(c, req)
		})}}},
		ES:    &captureBackend{},
		Store: st,
	})

	if err := idx.Build(ctx, BuildArgs{ResourceType: "product", ResourceIds: []string{"1", "2"}, OwnerTokens: tokens}); !errors.Is(err, context.Canceled) {
		t.Fatalf("want ctx's error, got %v", err)
	}
	if st.count("ReleaseFailed") != 0 || st.count("ReleaseOwners:") != 2 {
		t.Fatalf("both ids are released without a backoff: %v", st.callsSnapshot())
	}
	for _, rec := range logs.records(t) {
		if rec["level"] == "ERROR" {
			t.Fatalf("a cancellation logs no Error line: %v", rec)
		}
	}
}
