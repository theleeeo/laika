package core

// A plan is asked about a list of ids and answers each of them exactly once
// (ADR 0014): with a document, with an explicit nil — its version's answer
// that it has no document (ADR 0013) — or with an error of that id's own.
// An asked id the plan leaves unanswered fails; it is never read as a nil,
// so nothing is deleted for it. A plan that answers an id twice, or one it
// was not asked about, is broken, and the call fails for every asked id.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// answerExecuter answers every request with items, in one result, and
// records the requests.
type answerExecuter struct {
	items []projection.BuildDoc
	reqs  []projection.BuildRequest
}

func (e *answerExecuter) Execute(_ context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	e.reqs = append(e.reqs, req)
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
	ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: e.items}
	close(ch)
	return ch
}

// executerFunc is a plan's Executer as a func.
type executerFunc func(context.Context, projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc]

func (f executerFunc) Execute(ctx context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	return f(ctx, req)
}

// errDoc is a plan's error for id alone.
func errDoc(id string, err error) projection.BuildDoc {
	return projection.BuildDoc{Root: product(id), Err: err}
}

func askProducts(ids ...string) projection.BuildRequest {
	return projection.BuildRequest{ResourceType: "product", ResourceIDs: ids}
}

// versionsOf is the Schema Versions of docs, in order.
func versionsOf(docs []versionedDoc) []int {
	out := make([]int, 0, len(docs))
	for _, vd := range docs {
		out = append(out, vd.version)
	}
	return out
}

func TestExecuteAllPlans_AnswersEachAskedIDWithItsOwnOutcome(t *testing.T) {
	errC := errors.New("c failed at source")
	errD := errors.New("d failed at source")
	// Version 1 answers a with a document, b with an explicit nil and c
	// with an error of its own; version 2 has a document for each but d,
	// which version 1 built and version 2 fails.
	v1 := &answerExecuter{items: []projection.BuildDoc{productDoc("a"), nilDoc("b"), errDoc("c", errC), productDoc("d")}}
	v2 := &answerExecuter{items: []projection.BuildDoc{productDoc("c"), errDoc("d", errD), productDoc("b"), productDoc("a")}}
	plans := []projection.Plan{{Version: 1, Executer: v1}, {Version: 2, Executer: v2}}

	got, err := executeAllPlans(t.Context(), plans, askProducts("a", "b", "c", "d"))
	if err != nil {
		t.Fatalf("a per-id error must not fail the call: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("want one outcome per asked id, got %+v", got)
	}

	a := got["a"]
	if a.err != nil || !slices.Equal(versionsOf(a.docs), []int{1, 2}) || len(a.missing) != 0 {
		t.Fatalf("a: want documents of versions 1 and 2, got %+v", a)
	}
	for _, vd := range a.docs {
		if vd.doc.Root.Id != "a" {
			t.Fatalf("a's outcome must hold a's documents, got %+v", vd.doc)
		}
	}
	b := got["b"]
	if b.err != nil || !slices.Equal(versionsOf(b.docs), []int{2}) || !slices.Equal(b.missing, []int{1}) {
		t.Fatalf("b: want version 2's document and version 1's nil, got %+v", b)
	}
	c := got["c"]
	if !errors.Is(c.err, errC) || len(c.docs) != 0 || len(c.missing) != 0 {
		t.Fatalf("c: want its own error and nothing else, got %+v", c)
	}
	if !strings.Contains(c.err.Error(), "version 1") {
		t.Fatalf("c: the error must name the version that failed it, got %v", c.err)
	}
	// A later plan's error fails the id, dropping what an earlier plan built.
	d := got["d"]
	if !errors.Is(d.err, errD) || len(d.docs) != 0 || len(d.missing) != 0 {
		t.Fatalf("d: want version 2's error and no documents or nils, got %+v", d)
	}
}

// An asked id with no answer fails; it is not that version's nil, and the
// ids the plan did answer are unaffected.
func TestExecuteAllPlans_AnAskedIDLeftUnansweredFailsAndIsNoNil(t *testing.T) {
	v1 := &answerExecuter{items: []projection.BuildDoc{productDoc("a")}}
	v2 := &answerExecuter{items: []projection.BuildDoc{productDoc("a"), productDoc("b")}}
	plans := []projection.Plan{{Version: 1, Executer: v1}, {Version: 2, Executer: v2}}

	got, err := executeAllPlans(t.Context(), plans, askProducts("a", "b"))
	if err != nil {
		t.Fatalf("an unanswered id must not fail the call: %v", err)
	}
	if a := got["a"]; a.err != nil || !slices.Equal(versionsOf(a.docs), []int{1, 2}) {
		t.Fatalf("a: want both versions' documents, got %+v", a)
	}
	b := got["b"]
	if b.err == nil || !strings.Contains(b.err.Error(), "unanswered") {
		t.Fatalf("b: want an error naming it unanswered, got %+v", b)
	}
	if len(b.docs) != 0 || len(b.missing) != 0 {
		t.Fatalf("b: an unanswered id carries no documents and no nils, got %+v", b)
	}
}

// A plan that answers an id twice, or an id it was not asked about, fails
// the call: no outcome for any asked id.
func TestExecuteAllPlans_ABrokenAnswerFailsTheCall(t *testing.T) {
	for name, items := range map[string][]projection.BuildDoc{
		"answered twice":         {productDoc("a"), productDoc("b"), nilDoc("a")},
		"answered twice, as err": {productDoc("a"), errDoc("a", errors.New("x")), productDoc("b")},
		"not asked about":        {productDoc("a"), productDoc("b"), productDoc("z")},
	} {
		t.Run(name, func(t *testing.T) {
			plans := []projection.Plan{{Version: 1, Executer: &answerExecuter{items: items}}}

			got, err := executeAllPlans(t.Context(), plans, askProducts("a", "b"))
			if err == nil {
				t.Fatalf("want the call to fail, got %+v", got)
			}
			if got != nil {
				t.Fatalf("a failed call has no outcomes, got %+v", got)
			}
		})
	}
}

// Every plan answers before anything is decided: a later plan's broken
// answer fails the call even after an earlier plan answered well.
func TestExecuteAllPlans_ALaterPlansBrokenAnswerFailsTheCall(t *testing.T) {
	v1 := &answerExecuter{items: []projection.BuildDoc{productDoc("a")}}
	v2 := &answerExecuter{items: []projection.BuildDoc{productDoc("a"), productDoc("a")}}
	plans := []projection.Plan{{Version: 1, Executer: v1}, {Version: 2, Executer: v2}}

	if got, err := executeAllPlans(t.Context(), plans, askProducts("a")); err == nil {
		t.Fatalf("want the call to fail, got %+v", got)
	}
	if len(v2.reqs) != 1 || !slices.Equal(v2.reqs[0].ResourceIDs, []string{"a"}) {
		t.Fatalf("want version 2 asked about a, got %+v", v2.reqs)
	}
}

// A build of one id whose plans answer nothing, or answer it with its own
// error, fails, as a plan error does, and deletes nothing: no document
// leaves the backend, and the row stays, marked, for a later build.
func TestBuild_AnIDThePlansLeaveUnanswered_FailsAndDeletesNothing(t *testing.T) {
	for name, answer := range map[string][]projection.BuildDoc{
		"unanswered":    {},
		"its own error": {errDoc("X", errors.New("X failed at source"))},
	} {
		t.Run(name, func(t *testing.T) { testBuildFailsAndDeletesNothing(t, answer) })
	}
}

func testBuildFailsAndDeletesNothing(t *testing.T, answer []projection.BuildDoc) {
	noDeletes := func(t *testing.T, st *recordingStore, be *captureBackend) {
		t.Helper()
		if d := be.deletesAt(); len(d) != 0 {
			t.Fatalf("a failed id must delete no document, got %v", d)
		}
		for _, prefix := range []string{"RemoveResource", "DeleteResourceIfSeq", "ReplaceEdges"} {
			if n := st.count(prefix); n != 0 {
				t.Fatalf("a failed id must reach no %s: %v", prefix, st.callsSnapshot())
			}
		}
		if len(be.upserts) != 0 || len(be.bulkCalls) != 0 {
			t.Fatalf("a failed id must write nothing: upserts %v, bulk %v", be.upserts, be.bulkCalls)
		}
	}
	silent := func() *staticExecuter {
		return &staticExecuter{byID: map[string][]projection.BuildDoc{"X": answer}}
	}

	t.Run("unowned", func(t *testing.T) {
		st := &recordingStore{}
		idx, be := newDeletePathIndexer(st, silent())

		if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"X"}}); err != nil {
			t.Fatalf("an unowned build logs a failed id and returns nil, got %v", err)
		}

		noDeletes(t, st, be)
		if st.count("ClearStale") != 0 {
			t.Fatalf("a failed build must keep the mark: %v", st.callsSnapshot())
		}
	})

	t.Run("owned", func(t *testing.T) {
		st := &recordingStore{}
		idx, be := newDeletePathIndexer(st, silent())

		mustRegister(t, idx, notify("X", ChangeUpdated, nil))
		waitIdle(t, idx)

		noDeletes(t, st, be)
		if st.count("ReleaseFailed") != 1 || st.count("FinishOwned") != 0 {
			t.Fatalf("a failed owned build is released with backoff, not finished: %v", st.callsSnapshot())
		}
		if r, ok := st.row(product("X")); !ok || !r.stale {
			t.Fatalf("X's row must stay, marked, got %+v (present %v)", r, ok)
		}
	})

	t.Run("rebuild by ids", func(t *testing.T) {
		st := &recordingStore{}
		idx, be := newDeletePathIndexer(st, silent())

		err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product", ResourceIDs: []string{"X"}}})
		if err == nil {
			t.Fatal("a rebuild whose id failed must fail")
		}

		noDeletes(t, st, be)
		if _, ok := st.row(product("X")); !ok {
			t.Fatalf("X's row must stay for the sweep: %v", st.callsSnapshot())
		}
	})
}

// An owned build whose ctx ends while its plans run, and whose plans then
// close without answering its id — as a plan cut short by a cancellation or
// shutdown does — leaves the id unanswered, which is no nil: nothing is
// deleted for it, no row removed, no edge set replaced. The failure is the
// cancellation's, not the build's, so the id is released without a backoff
// through ReleaseOwners, and its row stays marked for the sweep.
func TestBuild_ACancelledOwnedBuildWhosePlansCloseUnanswered_DeletesNothingAndIsNotBackedOff(t *testing.T) {
	st := &recordingStore{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var executed int
	closesUnanswered := executerFunc(func(context.Context, projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
		executed++
		cancel()
		ch := make(chan aggregation.ExecutionResult[projection.BuildDoc])
		close(ch)
		return ch
	})
	idx, be := newDeletePathIndexer(st, closesUnanswered)

	owned, err := st.MarkStale(t.Context(), []model.Resource{product("X")}, time.Minute)
	if err != nil || len(owned) != 1 {
		t.Fatalf("claiming: %v %v", owned, err)
	}
	token := owned[0].Token

	_ = idx.Build(ctx, BuildArgs{ResourceType: "product", ResourceIds: []string{"X"}, OwnerTokens: map[string]int64{"X": token}})
	waitIdle(t, idx)

	if executed == 0 {
		t.Fatalf("X's plans must have run: %v", st.callsSnapshot())
	}
	if d := be.deletesAt(); len(d) != 0 {
		t.Fatalf("an unanswered id must delete no document, got %v", d)
	}
	for _, prefix := range []string{"DeleteResourceIfSeq", "RemoveResource", "ReplaceEdges", "FinishOwned"} {
		if n := st.count(prefix); n != 0 {
			t.Fatalf("an unanswered id must reach no %s: %v", prefix, st.callsSnapshot())
		}
	}
	if n := st.count("ReleaseFailed"); n != 0 {
		t.Fatalf("a build the cancellation cut short must not back X off: %v", st.callsSnapshot())
	}
	if st.indexOf(fmt.Sprintf("ReleaseOwners:product/X:%d", token)) == -1 || st.owner(product("X")) != 0 {
		t.Fatalf("X's ownership must be released on a live ctx, owner now %d: %v", st.owner(product("X")), st.callsSnapshot())
	}
	if r, ok := st.row(product("X")); !ok || !r.stale {
		t.Fatalf("X's row must stay, marked, got %+v (present %v)", r, ok)
	}
}

// A call that fails on a broken answer stops the plan: its context ends, so
// a plan with more to send isn't left blocked on its channel.
func TestExecutePlan_ABrokenAnswerStopsThePlan(t *testing.T) {
	stopped := make(chan struct{})
	plan := projection.Plan{Version: 1, Executer: executerFunc(
		func(ctx context.Context, _ projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
			ch := make(chan aggregation.ExecutionResult[projection.BuildDoc])
			go func() {
				defer close(ch)
				ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: []projection.BuildDoc{productDoc("z")}}
				select {
				case ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: []projection.BuildDoc{productDoc("a")}}:
				case <-ctx.Done():
					close(stopped)
				}
			}()
			return ch
		})}

	if _, err := executePlan(t.Context(), plan, askProducts("a")); err == nil {
		t.Fatal("want an answer of an unasked id to fail the call")
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the plan's context must end when the call fails")
	}
}

// A by-ids rebuild fails an id its plan answers with an error of its own
// where the plan's error fails it: marked at once, before the next id
// begins — not left unsettled for finish to mark — counted once, and none
// of the other plan's document for it written. The next id completes.
func TestRebuildByIDs_AnIDsOwnError_FailsItAsAPlanError(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	err := rebuildSelected(t, st, es, 0, twoVersionResources(),
		ResourceSelector{ResourceType: "product", ResourceIDs: []string{"X", "1"}},
		&staticExecuter{docs: []projection.BuildDoc{productDoc("1")}, byID: map[string][]projection.BuildDoc{"X": {errDoc("X", errors.New("X failed at source"))}}},
		&staticExecuter{docs: []projection.BuildDoc{productDoc("1"), productDoc("X")}})

	assertFailedOnce(t, st, err, "X")
	calls := st.callsSnapshot()
	if n := countPrefix(calls, "BeginBuild:product/X"); n != 1 {
		t.Fatalf("X must be begun once, got %d: %v", n, calls)
	}
	marked, next := callIndexes(calls, "MarkStale:product/X"), callIndexes(calls, "BeginBuild:product/1")
	if len(marked) != 1 || len(next) != 1 || marked[0] > next[0] {
		t.Fatalf("X must be failed, and marked, before 1 begins: %v", calls)
	}
	for _, chunk := range es.bulkCalls {
		for _, it := range chunk {
			if it.ID == "X" {
				t.Fatalf("no document of the failed X may be written, got %+v", it)
			}
		}
	}
}
