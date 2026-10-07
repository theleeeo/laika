package core

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/projection"
)

// The reverse sweep (ADR 0012) lists a type's rows page by page, probes each
// page's ids once per actor, and sends every id no probe returned — a
// suspect — through scheduleBuild: marked first, then built as an owned build
// that deletes it when every plan finds it gone.

// probeCall is one recorded Probe call.
type probeCall struct {
	ids      []string
	metadata map[string]string
}

// fakeProbe records every call and answers it with answer; a nil answer
// returns every id it was asked about.
type fakeProbe struct {
	answer func(ctx context.Context, ids []string, md map[string]string) ([]string, error)

	mu    sync.Mutex
	calls []probeCall
}

func (p *fakeProbe) probe(ctx context.Context, ids []string, md map[string]string) ([]string, error) {
	p.mu.Lock()
	p.calls = append(p.calls, probeCall{ids: slices.Clone(ids), metadata: maps.Clone(md)})
	p.mu.Unlock()
	if p.answer == nil {
		return ids, nil
	}
	return p.answer(ctx, ids, md)
}

func (p *fakeProbe) callsSnapshot() []probeCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.calls)
}

// probedIDs is every id the probe was asked about, in call order.
func (p *fakeProbe) probedIDs() []string {
	var out []string
	for _, c := range p.callsSnapshot() {
		out = append(out, c.ids...)
	}
	return out
}

// sweepExecuter builds a document for every requested id but those in gone,
// for which it returns nil, and records every request.
type sweepExecuter struct {
	gone map[string]bool

	mu   sync.Mutex
	reqs []projection.BuildRequest
}

func (e *sweepExecuter) Execute(_ context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	e.mu.Lock()
	e.reqs = append(e.reqs, req)
	e.mu.Unlock()
	doc := productDoc(req.ResourceID)
	if e.gone[req.ResourceID] {
		doc = nilDoc(req.ResourceID)
	}
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
	ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: []projection.BuildDoc{doc}}
	close(ch)
	return ch
}

// requestsFor is every request for id, in arrival order.
func (e *sweepExecuter) requestsFor(id string) []projection.BuildRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []projection.BuildRequest
	for _, r := range e.reqs {
		if r.ResourceID == id {
			out = append(out, r)
		}
	}
	return out
}

// newSweepIndexer serves "product" over two Schema Versions, both built by
// ex; only version 2's plan has a Probe, so the sweep must pick it. The type
// is swept with sweep, whose PageInterval defaults to a nanosecond here, and
// the pressure back-off is a millisecond.
func newSweepIndexer(st *recordingStore, ex *sweepExecuter, p *fakeProbe, sweep ReverseSweepConfig) (*Indexer, *captureBackend) {
	if sweep.PageInterval == 0 {
		sweep.PageInterval = time.Nanosecond
	}
	be := &captureBackend{}
	idx := mustNew(Config{
		Resources: twoVersionResources(),
		Plans: map[string][]projection.Plan{"product": {
			{Version: 1, Executer: ex},
			{Version: 2, Executer: ex, Probe: p.probe},
		}},
		ES:            be,
		Store:         st,
		PoolSize:      2,
		QueueSize:     16,
		ReverseSweeps: map[string]ReverseSweepConfig{"product": sweep},
	})
	idx.poolBackoff = time.Millisecond
	return idx, be
}

func actor(name string) map[string]string { return map[string]string{"actor": name} }

// seedRows gives each id a row of product, unmarked, with md.
func seedRows(st *recordingStore, md map[string]string, ids ...string) {
	for _, id := range ids {
		st.seedMetadata(product(id), md)
	}
}

// A page whose rows carry two actors' metadata, and rows with none, is probed
// once per actor with that actor's ids and metadata, and once for the rows
// without metadata, with none. Every id the probes return is present at
// source: nothing is marked or built.
func TestReverseSweep_ProbesEachActorsIDsInOneCall(t *testing.T) {
	st := &recordingStore{}
	seedRows(st, actor("A"), "a1", "a2", "a3")
	seedRows(st, actor("B"), "b1", "b2")
	seedRows(st, nil, "n1", "n2")
	p := &fakeProbe{}
	ex := &sweepExecuter{}
	idx, _ := newSweepIndexer(st, ex, p, ReverseSweepConfig{})

	got, err := idx.ReverseSweepNow(t.Context(), "product")
	if err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	if want := (ReverseSweepResult{Listed: 7}); got != want {
		t.Fatalf("result: got %+v, want %+v", got, want)
	}
	calls := p.callsSnapshot()
	byActor := make(map[string][]string, len(calls))
	for _, c := range calls {
		key := c.metadata["actor"]
		if c.metadata != nil && key == "" {
			t.Fatalf("unexpected probe metadata %v", c.metadata)
		}
		if _, dup := byActor[key]; dup {
			t.Fatalf("actor %q probed twice in one page: %+v", key, calls)
		}
		byActor[key] = c.ids
	}
	want := map[string][]string{"A": {"a1", "a2", "a3"}, "B": {"b1", "b2"}, "": {"n1", "n2"}}
	if len(calls) != 3 || !maps.EqualFunc(byActor, want, slices.Equal) {
		t.Fatalf("want one probe per actor and one without metadata, %v; got %+v", want, calls)
	}
	if st.count("MarkStale") != 0 || st.count("BeginBuild") != 0 || len(ex.reqs) != 0 {
		t.Fatalf("ids the probe returned must be neither marked nor built: %v", st.callsSnapshot())
	}
}

// A suspect is marked — the page's suspects in one MarkStale, before any
// build begins — and built as an owned build with the metadata its row holds
// at BeginBuild: here a registration changed it after the page was listed. A
// suspect that is gone at source, every plan returning nil, has its
// documents deleted from every version at its build's Build Sequence and its
// row removed. An id the probe returns but wasn't asked about is ignored, and
// a present id is neither marked nor built.
func TestReverseSweep_MarksSuspectsThenBuildsThemOwned(t *testing.T) {
	st := &recordingStore{buildIdx: 41}
	seedRows(st, actor("A"), "present", "suspect")
	seedRows(st, actor("B"), "gone")
	p := &fakeProbe{answer: func(_ context.Context, ids []string, md map[string]string) ([]string, error) {
		if md["actor"] == "A" {
			// A registration changes the suspect's metadata after the page
			// was listed; its build must run with the new metadata.
			st.seedMetadata(product("suspect"), actor("A2"))
			return []string{"present", "unasked"}, nil
		}
		return nil, nil
	}}
	ex := &sweepExecuter{gone: map[string]bool{"gone": true}}
	idx, be := newSweepIndexer(st, ex, p, ReverseSweepConfig{})

	got, err := idx.ReverseSweepNow(t.Context(), "product")
	if err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	if want := (ReverseSweepResult{Listed: 3, Suspects: 2}); got != want {
		t.Fatalf("result: got %+v, want %+v", got, want)
	}
	calls := st.callsSnapshot()
	if marks := callsWithPrefix(st, "MarkStale"); !slices.Equal(marks, []string{"MarkStale:2"}) {
		t.Fatalf("the page's two suspects must be marked in one MarkStale: %v", calls)
	}
	if mark, begin := st.indexOf("MarkStale"), st.indexOf("BeginBuild"); begin == -1 || mark > begin {
		t.Fatalf("suspects must be marked before any build begins: %v", calls)
	}

	// The present id and the unasked one.
	if len(callsWithPrefix(st, "BeginBuild:product/present:")) != 0 || len(ex.requestsFor("present")) != 0 {
		t.Fatalf("an id the probe returned must not be built: %v", calls)
	}
	if r, _ := st.row(product("present")); r.stale || r.staleSeq != 0 {
		t.Fatalf("an id the probe returned must not be marked, got %+v", r)
	}
	if _, ok := st.row(product("unasked")); ok {
		t.Fatalf("an id the probe returned unasked must be ignored: %v", calls)
	}

	// The suspect that exists: an owned build with its row's metadata.
	begins := callsWithPrefix(st, "BeginBuild:product/suspect:")
	if len(begins) != 1 || strings.HasSuffix(begins[0], ":0") {
		t.Fatalf("the suspect must be built once, owned: %v", calls)
	}
	reqs := ex.requestsFor("suspect")
	if len(reqs) != 2 {
		t.Fatalf("the suspect's build must run both versions' plans, got %d requests", len(reqs))
	}
	for _, r := range reqs {
		if !maps.Equal(r.Metadata, actor("A2")) {
			t.Fatalf("the suspect's build must fetch with the metadata its row holds at BeginBuild, got %v", r.Metadata)
		}
	}
	if r, ok := st.row(product("suspect")); !ok || r.stale || r.owner != 0 {
		t.Fatalf("the suspect's build must finish clean, got %+v (exists %v)", r, ok)
	}

	// The suspect that is gone: documents deleted, row removed.
	removes := callsWithPrefix(st, "RemoveResource:product/gone:")
	if len(removes) != 1 {
		t.Fatalf("the gone suspect's edges must be removed once: %v", calls)
	}
	var seq int64
	if _, err := fmt.Sscanf(removes[0], "RemoveResource:product/gone:%d", &seq); err != nil {
		t.Fatal(err)
	}
	if got := deletesOf(be, "gone"); !slices.Equal(got, bothVersionsAt("gone", seq)) {
		t.Fatalf("the gone suspect's documents must be deleted from every version at its build's sequence: got %v", got)
	}
	if r, ok := st.row(product("gone")); ok {
		t.Fatalf("the gone suspect's row must be removed, got %+v", r)
	}
	if len(deletesOf(be, "suspect")) != 0 || len(deletesOf(be, "present")) != 0 {
		t.Fatalf("only the gone suspect may be deleted: %v", be.deletesAt())
	}
}

// A probe may use the ids it is given as it likes: one that filters them in
// place, as present := ids[:0] does, still has every id it dropped found as a
// suspect, marked and built.
func TestReverseSweep_ProbeFilteringItsIDsInPlace_FindsEverySuspect(t *testing.T) {
	st := &recordingStore{}
	seedRows(st, nil, "a", "b", "c")
	p := &fakeProbe{answer: func(_ context.Context, ids []string, _ map[string]string) ([]string, error) {
		present := ids[:0]
		for _, id := range ids {
			if id != "b" {
				present = append(present, id)
			}
		}
		return present, nil
	}}
	ex := &sweepExecuter{}
	idx, _ := newSweepIndexer(st, ex, p, ReverseSweepConfig{})

	got, err := idx.ReverseSweepNow(t.Context(), "product")
	if err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	if want := (ReverseSweepResult{Listed: 3, Suspects: 1}); got != want {
		t.Fatalf("result: got %+v, want %+v", got, want)
	}
	if marks := callsWithPrefix(st, "MarkStale"); !slices.Equal(marks, []string{"MarkStale:1"}) {
		t.Fatalf("b, the id the probe dropped, must be marked: %v", st.callsSnapshot())
	}
	if len(callsWithPrefix(st, "BeginBuild:product/b:")) != 1 {
		t.Fatalf("b must be built once: %v", st.callsSnapshot())
	}
	for _, id := range []string{"a", "c"} {
		if len(callsWithPrefix(st, "BeginBuild:product/"+id+":")) != 0 {
			t.Fatalf("%s, which the probe returned, must not be built: %v", id, st.callsSnapshot())
		}
	}
}

// A group whose probe fails is logged, counted and skipped — its ids neither
// marked nor built — and the rest of the page is still probed; the page's
// cursor still passes it.
func TestReverseSweep_FailedProbeIsSkippedAndCounted(t *testing.T) {
	logs := captureDefaultLogs(t)
	st := &recordingStore{}
	seedRows(st, actor("A"), "a1", "a2")
	seedRows(st, actor("B"), "b1")
	p := &fakeProbe{answer: func(_ context.Context, ids []string, md map[string]string) ([]string, error) {
		if md["actor"] == "A" {
			return nil, errors.New("source down")
		}
		return nil, nil
	}}
	ex := &sweepExecuter{}
	idx, _ := newSweepIndexer(st, ex, p, ReverseSweepConfig{})

	var cursors []string
	got, err := idx.ReverseSweepResumable(t.Context(), "product", "", func(after string) { cursors = append(cursors, after) })
	if err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	if want := (ReverseSweepResult{Listed: 3, Suspects: 1, FailedProbes: 1, Unprobed: 2}); got != want {
		t.Fatalf("result: got %+v, want %+v", got, want)
	}
	if len(p.callsSnapshot()) != 2 {
		t.Fatalf("both groups must be probed: %+v", p.callsSnapshot())
	}
	for _, id := range []string{"a1", "a2"} {
		if r, _ := st.row(product(id)); r.stale || len(ex.requestsFor(id)) != 0 {
			t.Fatalf("%s's probe failed: it must be neither marked nor built, got %+v: %v", id, r, st.callsSnapshot())
		}
	}
	if !slices.Equal(callsWithPrefix(st, "MarkStale"), []string{"MarkStale:1"}) || len(ex.requestsFor("b1")) == 0 {
		t.Fatalf("the other group's suspect must be marked and built: %v", st.callsSnapshot())
	}
	if !slices.Equal(cursors, []string{"b1"}) {
		t.Fatalf("a failed probe must not hold the cursor back: checkpoints %v", cursors)
	}
	logs.mu.Lock()
	out := logs.buf.String()
	logs.mu.Unlock()
	if !strings.Contains(out, `"level":"WARN"`) || !strings.Contains(out, "source down") || !strings.Contains(out, `"ids":2`) {
		t.Fatalf("a failed probe must be warned about with its id count and error, got logs:\n%s", out)
	}
}

// A probe that fails because the run's ctx ended ends the run with ctx's
// error, counting nothing and checkpointing nothing.
func TestReverseSweep_ProbeFailingOnCancellationEndsTheRun(t *testing.T) {
	st := &recordingStore{}
	seedRows(st, actor("A"), "a1")
	ctx, cancel := context.WithCancel(t.Context())
	p := &fakeProbe{answer: func(ctx context.Context, _ []string, _ map[string]string) ([]string, error) {
		cancel()
		return nil, ctx.Err()
	}}
	idx, _ := newSweepIndexer(st, &sweepExecuter{}, p, ReverseSweepConfig{})

	checkpointed := false
	got, err := idx.ReverseSweepResumable(ctx, "product", "", func(string) { checkpointed = true })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if want := (ReverseSweepResult{Listed: 1}); got != want {
		t.Fatalf("result: got %+v, want %+v", got, want)
	}
	if checkpointed || st.count("MarkStale") != 0 {
		t.Fatalf("a cancelled run must neither checkpoint nor mark: %v", st.callsSnapshot())
	}
}

// A failed mark ends the run with its error and the result so far; the page
// is not checkpointed, so a resumed run probes it again.
func TestReverseSweep_FailedMarkEndsTheRunWithoutCheckpoint(t *testing.T) {
	st := &recordingStore{markErr: errors.New("pg down")}
	seedRows(st, nil, "a")
	p := &fakeProbe{answer: func(context.Context, []string, map[string]string) ([]string, error) { return nil, nil }}
	idx, _ := newSweepIndexer(st, &sweepExecuter{}, p, ReverseSweepConfig{})

	checkpointed := false
	got, err := idx.ReverseSweepResumable(t.Context(), "product", "", func(string) { checkpointed = true })
	if err == nil || !strings.Contains(err.Error(), "pg down") {
		t.Fatalf("want the mark's error, got %v", err)
	}
	if want := (ReverseSweepResult{Listed: 1}); got != want {
		t.Fatalf("result: got %+v, want %+v", got, want)
	}
	if checkpointed || st.count("BeginBuild") != 0 {
		t.Fatalf("an unmarked page must be neither checkpointed nor built: %v", st.callsSnapshot())
	}
}

// A type none of whose plans has a Probe is skipped with a warning: nothing
// is listed, probed or marked, and the run succeeds.
func TestReverseSweep_TypeWithoutProbe_IsSkippedWithAWarning(t *testing.T) {
	logs := captureDefaultLogs(t)
	st := &recordingStore{}
	seedRows(st, nil, "a")
	ex := &sweepExecuter{}
	idx := mustNew(Config{
		Resources:     twoVersionResources(),
		Plans:         map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}, {Version: 2, Executer: ex}}},
		ES:            &captureBackend{},
		Store:         st,
		ReverseSweeps: map[string]ReverseSweepConfig{"product": {}},
	})

	got, err := idx.ReverseSweepNow(t.Context(), "product")
	if err != nil {
		t.Fatal(err)
	}
	if got != (ReverseSweepResult{}) {
		t.Fatalf("a skipped type's result must be zero, got %+v", got)
	}
	if calls := st.callsSnapshot(); len(calls) != 0 {
		t.Fatalf("a type without a Probe must make no Store call: %v", calls)
	}
	logs.mu.Lock()
	out := logs.buf.String()
	logs.mu.Unlock()
	if !strings.Contains(out, `"level":"WARN"`) || !strings.Contains(out, `"type":"product"`) || !strings.Contains(out, "Probe") {
		t.Fatalf("want a warning naming the type, got logs:\n%s", out)
	}
}

// A type without a ReverseSweeps entry is not swept, and one Resources
// doesn't configure is unknown: both are errors, and nothing is listed.
func TestReverseSweep_UnconfiguredOrUnknownType_IsAnError(t *testing.T) {
	st := &recordingStore{}
	p := &fakeProbe{}
	idx := mustNew(Config{
		Resources: twoVersionResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: &sweepExecuter{}, Probe: p.probe}}},
		ES:        &captureBackend{},
		Store:     st,
	})

	if _, err := idx.ReverseSweepNow(t.Context(), "product"); err == nil || errors.Is(err, ErrUnknownResource) {
		t.Fatalf("a type without a ReverseSweeps entry: want a not-configured error, got %v", err)
	}
	if _, err := idx.ReverseSweepNow(t.Context(), "ghost"); !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("an unknown type: want ErrUnknownResource, got %v", err)
	}
	if calls := st.callsSnapshot(); len(calls) != 0 || len(p.callsSnapshot()) != 0 {
		t.Fatalf("nothing may be listed or probed: %v", calls)
	}
}

// Pages are keyset pages of PageSize ids; each page's suspects are marked
// before its checkpoint, which is the page's last id. A run killed after a
// checkpoint and resumed from it probes every id, none twice.
func TestReverseSweep_KilledMidTypeResumesFromItsCursor(t *testing.T) {
	st := &recordingStore{}
	seedRows(st, nil, "a", "b", "c", "d", "e")
	p := &fakeProbe{answer: func(_ context.Context, ids []string, _ map[string]string) ([]string, error) {
		// b and e are suspects.
		return slices.DeleteFunc(slices.Clone(ids), func(id string) bool { return id == "b" || id == "e" }), nil
	}}
	idx, _ := newSweepIndexer(st, &sweepExecuter{}, p, ReverseSweepConfig{PageSize: 2})

	type checkpoint struct {
		after string
		marks int
	}
	var cps []checkpoint
	record := func(after string) { cps = append(cps, checkpoint{after, st.count("MarkStale")}) }

	// The first run is killed right after its first checkpoint.
	ctx, cancel := context.WithCancel(t.Context())
	first, err := idx.ReverseSweepResumable(ctx, "product", "", func(after string) {
		record(after)
		cancel()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("the killed run: want context.Canceled, got %v", err)
	}
	if want := (ReverseSweepResult{Listed: 2, Suspects: 1}); first != want {
		t.Fatalf("the killed run's result: got %+v, want %+v", first, want)
	}
	if !slices.Equal(cps, []checkpoint{{"b", 1}}) {
		t.Fatalf("the first page must be checkpointed at its last id once its suspect is marked: %+v", cps)
	}

	second, err := idx.ReverseSweepResumable(t.Context(), "product", cps[len(cps)-1].after, record)
	if err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	if want := (ReverseSweepResult{Listed: 3, Suspects: 1}); second != want {
		t.Fatalf("the resumed run's result: got %+v, want %+v", second, want)
	}
	if want := []checkpoint{{"b", 1}, {"d", 1}, {"e", 2}}; !slices.Equal(cps, want) {
		t.Fatalf("checkpoints: got %+v, want %+v", cps, want)
	}
	if got, want := p.probedIDs(), []string{"a", "b", "c", "d", "e"}; !slices.Equal(got, want) {
		t.Fatalf("every id must be probed once across the runs: got %v, want %v", got, want)
	}
	if got, want := callsWithPrefix(st, "ListResources"), []string{
		"ListResources:product::2", "ListResources:product:b:2", "ListResources:product:d:2",
	}; !slices.Equal(got, want) {
		t.Fatalf("listing: got %v, want %v", got, want)
	}
	for _, id := range []string{"b", "e"} {
		if len(callsWithPrefix(st, "BeginBuild:product/"+id+":")) != 1 {
			t.Fatalf("suspect %s must be built once: %v", id, st.callsSnapshot())
		}
	}
}

// signalLogs installs a default logger, enabled at every level, that signals
// on the returned channel (dropping signals while one is pending) for each
// record whose message is msg, until the test ends.
func signalLogs(t *testing.T, msg string) <-chan struct{} {
	t.Helper()
	ch := make(chan struct{}, 1)
	prev, prevOut, prevFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(signalHandler{msg: msg, ch: ch}))
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	return ch
}

type signalHandler struct {
	msg string
	ch  chan struct{}
}

func (h signalHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h signalHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == h.msg {
		select {
		case h.ch <- struct{}{}:
		default:
		}
	}
	return nil
}
func (h signalHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h signalHandler) WithGroup(string) slog.Handler      { return h }

// pressurePool parks the pool's one worker and fills its queue of two to its
// high water of two, and returns the release that drains it; a cleanup
// releases it too, so a failing test leaves no worker parked.
func pressurePool(t *testing.T, idx *Indexer) (release func()) {
	block, started := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(block) }) }
	t.Cleanup(release)
	if !idx.pool.trySubmit(func(context.Context) { close(started); <-block }) {
		t.Error("parking the worker was refused")
		return release
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Error("the worker never picked up the parking task")
		return release
	}
	for range 2 {
		if !idx.pool.trySubmit(func(context.Context) {}) {
			t.Error("filling the queue was refused")
		}
	}
	if !idx.pool.pressured() {
		t.Error("the filled pool must report pressure")
	}
	return release
}

// newPressureIndexer sweeps "product" in pages of two over a pool of one
// worker whose queue is pressured at two queued tasks.
func newPressureIndexer(st *recordingStore, p *fakeProbe) *Indexer {
	idx := mustNew(Config{
		Resources:      twoVersionResources(),
		Plans:          map[string][]projection.Plan{"product": {{Version: 1, Executer: &sweepExecuter{}, Probe: p.probe}}},
		ES:             &captureBackend{},
		Store:          st,
		PoolSize:       1,
		QueueSize:      2,
		QueueHighWater: 2,
		ReverseSweeps:  map[string]ReverseSweepConfig{"product": {PageSize: 2, PageInterval: time.Nanosecond}},
	})
	idx.poolBackoff = time.Millisecond
	return idx
}

// While the pool reports pressure, the sweep lists no new page: it backs off
// — several times — and lists nothing more until the queue drains, whether
// the pressure was there before its first page or arose during a page.
func TestReverseSweep_BacksOffWhileThePoolIsPressured(t *testing.T) {
	for name, tc := range map[string]struct {
		// midRun pressures the pool from the first page's probe; otherwise
		// it is pressured before the sweep starts.
		midRun bool
		// listedWhilePressured is the listing the sweep made before it
		// backed off.
		listedWhilePressured []string
	}{
		"before the first page": {listedWhilePressured: nil},
		"after a page":          {midRun: true, listedWhilePressured: []string{"ListResources:product::2"}},
	} {
		t.Run(name, func(t *testing.T) {
			backoff := signalLogs(t, poolBackoffMsg)
			st := &recordingStore{}
			seedRows(st, nil, "a", "b", "c", "d")
			p := &fakeProbe{}
			idx := newPressureIndexer(st, p)

			var release func()
			if tc.midRun {
				var fill sync.Once
				p.answer = func(_ context.Context, ids []string, _ map[string]string) ([]string, error) {
					fill.Do(func() { release = pressurePool(t, idx) })
					return ids, nil
				}
			} else {
				release = pressurePool(t, idx)
			}

			type outcome struct {
				res ReverseSweepResult
				err error
			}
			done := make(chan outcome, 1)
			go func() {
				res, err := idx.ReverseSweepNow(t.Context(), "product")
				done <- outcome{res, err}
			}()

			for range 3 {
				select {
				case <-backoff:
				case o := <-done:
					t.Fatalf("the sweep finished while the pool was pressured: %+v", o)
				}
			}
			if got := callsWithPrefix(st, "ListResources"); !slices.Equal(got, tc.listedWhilePressured) {
				t.Fatalf("no page may be listed while the pool is pressured: got %v, want %v", got, tc.listedWhilePressured)
			}

			release()
			o := <-done
			if o.err != nil {
				t.Fatal(o.err)
			}
			waitIdle(t, idx)
			if want := (ReverseSweepResult{Listed: 4}); o.res != want {
				t.Fatalf("result: got %+v, want %+v", o.res, want)
			}
			if got, want := p.probedIDs(), []string{"a", "b", "c", "d"}; !slices.Equal(got, want) {
				t.Fatalf("once the queue drained the sweep must go on: probed %v, want %v", got, want)
			}
		})
	}
}

// A suspect whose row has a live owner is marked — its stale_seq moves, for
// the owner's finish to see and follow up — and counted, but the mark claims
// nothing, so the sweep builds nothing of it.
func TestReverseSweep_SuspectWithALiveOwner_IsMarkedButNotBuilt(t *testing.T) {
	const liveOwner = 99
	owned := product("owned")
	st := &recordingStore{}
	seedRows(st, nil, "owned")
	st.mu.Lock()
	st.rows[owned].owner = liveOwner
	st.mu.Unlock()
	p := &fakeProbe{answer: func(context.Context, []string, map[string]string) ([]string, error) { return nil, nil }}
	ex := &sweepExecuter{}
	idx, _ := newSweepIndexer(st, ex, p, ReverseSweepConfig{})

	got, err := idx.ReverseSweepNow(t.Context(), "product")
	if err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	if want := (ReverseSweepResult{Listed: 1, Suspects: 1}); got != want {
		t.Fatalf("result: got %+v, want %+v", got, want)
	}
	r, _ := st.row(owned)
	if !r.stale || r.staleSeq == 0 || r.owner != liveOwner {
		t.Fatalf("the owned suspect must be marked under a new stale_seq and keep its owner, got %+v", r)
	}
	if len(callsWithPrefix(st, "BeginBuild:product/owned:")) != 0 || len(ex.requestsFor("owned")) != 0 {
		t.Fatalf("the sweep must not build a suspect its owner holds: %v", st.callsSnapshot())
	}
}

// groupByMetadata puts rows with equal metadata — nil and empty alike, maps
// built separately alike — in one group, in the order each group first
// appears; the group without metadata is probed with nil.
func TestGroupByMetadata(t *testing.T) {
	row := func(id string, md map[string]string) ListedResource {
		return ListedResource{Resource: product(id), Metadata: md}
	}
	for name, tc := range map[string]struct {
		page []ListedResource
		want []probeGroup
	}{
		"nil and empty share the nil group": {
			page: []ListedResource{row("a", nil), row("b", map[string]string{}), row("c", nil)},
			want: []probeGroup{{metadata: nil, ids: []string{"a", "b", "c"}}},
		},
		"empty first is still probed with nil": {
			page: []ListedResource{row("a", map[string]string{}), row("b", nil)},
			want: []probeGroup{{metadata: nil, ids: []string{"a", "b"}}},
		},
		"equal maps built separately share a group": {
			page: []ListedResource{
				row("a", map[string]string{"actor": "A", "org": "1"}),
				row("b", map[string]string{"actor": "B"}),
				row("c", map[string]string{"org": "1", "actor": "A"}),
				row("d", nil),
			},
			want: []probeGroup{
				{metadata: map[string]string{"actor": "A", "org": "1"}, ids: []string{"a", "c"}},
				{metadata: map[string]string{"actor": "B"}, ids: []string{"b"}},
				{metadata: nil, ids: []string{"d"}},
			},
		},
		"a separator inside a value is not a key boundary": {
			page: []ListedResource{
				row("a", map[string]string{"k": `v";"x"="y`}),
				row("b", map[string]string{"k": "v", "x": "y"}),
			},
			want: []probeGroup{
				{metadata: map[string]string{"k": `v";"x"="y`}, ids: []string{"a"}},
				{metadata: map[string]string{"k": "v", "x": "y"}, ids: []string{"b"}},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := groupByMetadata(tc.page)
			if !slices.EqualFunc(got, tc.want, func(a, b probeGroup) bool {
				return (a.metadata == nil) == (b.metadata == nil) && maps.Equal(a.metadata, b.metadata) && slices.Equal(a.ids, b.ids)
			}) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

// New rejects a ReverseSweeps entry for a type Resources doesn't configure
// and negative values, and applies the defaults to zero values.
func TestNew_ReverseSweeps(t *testing.T) {
	newWith := func(sweeps map[string]ReverseSweepConfig) (*Indexer, error) {
		return New(Config{Resources: testResources(), ES: &fakeBackend{}, Store: &recordingStore{}, ReverseSweeps: sweeps})
	}

	if _, err := newWith(map[string]ReverseSweepConfig{"ghost": {}}); !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("an unknown type: want ErrUnknownResource, got %v", err)
	}
	for name, c := range map[string]ReverseSweepConfig{
		"interval":      {Interval: -time.Second},
		"page size":     {PageSize: -1},
		"page interval": {PageInterval: -time.Second},
	} {
		if _, err := newWith(map[string]ReverseSweepConfig{"product": c}); err == nil {
			t.Fatalf("a negative %s must be rejected", name)
		}
	}

	idx, err := newWith(map[string]ReverseSweepConfig{"product": {}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := idx.reverseSweeps["product"], (ReverseSweepConfig{Interval: 24 * time.Hour, PageSize: 200, PageInterval: time.Second}); got != want {
		t.Fatalf("defaults: got %+v, want %+v", got, want)
	}
	if idx.poolBackoff != time.Second {
		t.Fatalf("the pressure back-off defaults to 1s, got %v", idx.poolBackoff)
	}

	set := ReverseSweepConfig{Interval: time.Hour, PageSize: 5, PageInterval: time.Millisecond}
	idx, err = newWith(map[string]ReverseSweepConfig{"product": set})
	if err != nil {
		t.Fatal(err)
	}
	if got := idx.reverseSweeps["product"]; got != set {
		t.Fatalf("set values must be kept: got %+v, want %+v", got, set)
	}

	idx, err = newWith(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.reverseSweeps) != 0 {
		t.Fatalf("no entry sweeps nothing, got %+v", idx.reverseSweeps)
	}
}
