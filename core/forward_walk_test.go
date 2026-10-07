package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/projection"
)

// The forward walk walks a type from its source once per metadata map,
// each walk a RebuildWalk paced by the type's ForwardWalkConfig, so a
// resource created or changed without a notification is indexed.

// walkExecuter serves "product" walks. An all-of-type walk gets its pages
// over an unbuffered channel, each but the last with a next-page token, so a
// page is taken only when the walk asks for it; a by-id request gets one
// document. A walk whose metadata names failActor as its actor fails at its
// first page.
type walkExecuter struct {
	pages [][]projection.BuildDoc
	// beforePage, when set, runs before page i of a walk is offered.
	beforePage func(i int)
	failActor  string

	mu    sync.Mutex
	reqs  []projection.BuildRequest
	taken []int // pages the walk took, in order
}

func (e *walkExecuter) Execute(ctx context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	e.mu.Lock()
	e.reqs = append(e.reqs, req)
	e.mu.Unlock()
	if req.ResourceID != "" {
		ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
		ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: []projection.BuildDoc{productDoc(req.ResourceID)}}
		close(ch)
		return ch
	}
	if e.failActor != "" && req.Metadata["actor"] == e.failActor {
		ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
		ch <- aggregation.ExecutionResult[projection.BuildDoc]{Err: errors.New("listing failed")}
		close(ch)
		return ch
	}
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc])
	go func() {
		defer close(ch)
		for i, items := range e.pages {
			if e.beforePage != nil {
				e.beforePage(i)
			}
			var next any
			if i < len(e.pages)-1 {
				next = fmt.Sprintf("p%d", i+1)
			}
			select {
			case ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: items, NextPageToken: next}:
			case <-ctx.Done():
				return
			}
			e.mu.Lock()
			e.taken = append(e.taken, i)
			e.mu.Unlock()
		}
	}()
	return ch
}

func (e *walkExecuter) requests() []projection.BuildRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.reqs)
}

func (e *walkExecuter) takenPages() []int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.taken)
}

// threePages is a walk of six products in pages of two.
func threePages() [][]projection.BuildDoc {
	return [][]projection.BuildDoc{
		{productDoc("1"), productDoc("2")},
		{productDoc("3"), productDoc("4")},
		{productDoc("5"), productDoc("6")},
	}
}

// pageWaits records the page-interval waits a walk asks for, without waiting.
type pageWaits struct {
	mu sync.Mutex
	ds []time.Duration
}

func (w *pageWaits) wait(ctx context.Context, d time.Duration) error {
	w.mu.Lock()
	w.ds = append(w.ds, d)
	w.mu.Unlock()
	return ctx.Err()
}

func (w *pageWaits) snapshot() []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.ds)
}

// newWalkIndexer serves "product" with one plan, ex, over a pool of one worker
// whose queue is pressured at two queued tasks, with the forward walks walks.
// The pressure back-off is a millisecond, and page-interval waits are
// recorded in the returned pageWaits instead of waited.
func newWalkIndexer(ex *walkExecuter, walks map[string]ForwardWalkConfig) (*Indexer, *rebuildRecordingStore, *pageWaits) {
	st := &rebuildRecordingStore{}
	idx := mustNew(Config{
		Resources:      testResources(),
		Plans:          map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}}},
		ES:             &captureBackend{},
		Store:          st,
		PoolSize:       1,
		QueueSize:      2,
		QueueHighWater: 2,
		ForwardWalks:   walks,
	})
	idx.poolBackoff = time.Millisecond
	w := &pageWaits{}
	idx.waitPageInterval = w.wait
	return idx, st, w
}

// New rejects a ForwardWalks entry for a type Resources doesn't configure,
// negative values and a page size above an int32's, and applies the defaults to zero values.
func TestNew_ForwardWalks(t *testing.T) {
	newWith := func(walks map[string]ForwardWalkConfig) (*Indexer, error) {
		return New(Config{Resources: testResources(), ES: &fakeBackend{}, Store: &recordingStore{}, ForwardWalks: walks})
	}

	if _, err := newWith(map[string]ForwardWalkConfig{"ghost": {}}); !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("an unknown type: want ErrUnknownResource, got %v", err)
	}
	for name, c := range map[string]ForwardWalkConfig{
		"negative interval":      {Interval: -time.Second},
		"negative page size":     {PageSize: -1},
		"negative page interval": {PageInterval: -time.Second},
		"page size above int32":  {PageSize: math.MaxInt32 + 1},
	} {
		if _, err := newWith(map[string]ForwardWalkConfig{"product": c}); err == nil {
			t.Fatalf("a %s must be rejected", name)
		}
	}

	idx, err := newWith(map[string]ForwardWalkConfig{"product": {}})
	if err != nil {
		t.Fatal(err)
	}
	got := idx.forwardWalks["product"]
	if got.Interval != 24*time.Hour || got.PageSize != 100 || got.PageInterval != time.Second || got.Metadata != nil {
		t.Fatalf("defaults: got %+v, want Interval 24h, PageSize 100, PageInterval 1s", got)
	}

	md := func(context.Context) ([]map[string]string, error) { return nil, nil }
	idx, err = newWith(map[string]ForwardWalkConfig{"product": {Interval: time.Hour, PageSize: 5, PageInterval: time.Millisecond, Metadata: md}})
	if err != nil {
		t.Fatal(err)
	}
	got = idx.forwardWalks["product"]
	if got.Interval != time.Hour || got.PageSize != 5 || got.PageInterval != time.Millisecond || got.Metadata == nil {
		t.Fatalf("set values must be kept: got %+v", got)
	}

	idx, err = newWith(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.forwardWalks) != 0 {
		t.Fatalf("no entry walks nothing, got %+v", idx.forwardWalks)
	}
}

// A type without a ForwardWalks entry, or one Resources doesn't configure,
// can't be forward-walked: ForwardWalkNow fails and walks nothing.
func TestForwardWalkNow_UnconfiguredOrUnknownType_IsAnError(t *testing.T) {
	ex := &walkExecuter{pages: threePages()}
	idx, _, _ := newWalkIndexer(ex, nil)

	if _, err := idx.ForwardWalkNow(t.Context(), "product"); err == nil {
		t.Fatal("a type without a ForwardWalks entry must be an error")
	}
	if _, err := idx.ForwardWalkNow(t.Context(), "ghost"); !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("an unknown type: want ErrUnknownResource, got %v", err)
	}
	if reqs := ex.requests(); len(reqs) != 0 {
		t.Fatalf("nothing may be walked, got %+v", reqs)
	}
}

// A run walks the type once per metadata map, in the order the func returned
// them, each walk fetching with its own map and asking its plans for the
// config's page size. Every listed resource is built.
func TestForwardWalkNow_WalksOncePerMetadataMap(t *testing.T) {
	ex := &walkExecuter{pages: threePages()}
	idx, st, _ := newWalkIndexer(ex, map[string]ForwardWalkConfig{"product": {
		PageSize: 2,
		Metadata: func(context.Context) ([]map[string]string, error) {
			return []map[string]string{actor("A"), actor("B")}, nil
		},
	}})

	res, err := idx.ForwardWalkNow(t.Context(), "product")
	if err != nil {
		t.Fatal(err)
	}
	if want := (ForwardWalkResult{Walks: 2}); res != want {
		t.Fatalf("result: got %+v, want %+v", res, want)
	}
	reqs := ex.requests()
	if len(reqs) != 2 {
		t.Fatalf("want one walk per map, got %+v", reqs)
	}
	for i, want := range []map[string]string{actor("A"), actor("B")} {
		if !maps.Equal(reqs[i].Metadata, want) || reqs[i].ResourceID != "" || reqs[i].PageSize != 2 {
			t.Fatalf("walk %d: got %+v, want an all-of-type walk with metadata %v and page size 2", i, reqs[i], want)
		}
	}
	for _, id := range []string{"1", "2", "3", "4", "5", "6"} {
		if st.count("BeginBuild:product/"+id) != 2 {
			t.Fatalf("each walk must build %s: %v", id, st.callsSnapshot())
		}
	}
}

// A nil Metadata func walks once with no metadata; one returning no maps walks
// nothing that run, and one that fails fails the run, walking nothing.
func TestForwardWalkNow_MetadataFuncShapes(t *testing.T) {
	for name, tc := range map[string]struct {
		md      func(context.Context) ([]map[string]string, error)
		want    ForwardWalkResult
		walks   int
		wantErr bool
	}{
		"nil func": {md: nil, want: ForwardWalkResult{Walks: 1}, walks: 1},
		"no maps": {
			md:   func(context.Context) ([]map[string]string, error) { return nil, nil },
			want: ForwardWalkResult{Walks: 0},
		},
		"func error": {
			md:      func(context.Context) ([]map[string]string, error) { return nil, errors.New("directory down") },
			wantErr: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			ex := &walkExecuter{pages: threePages()}
			idx, _, _ := newWalkIndexer(ex, map[string]ForwardWalkConfig{"product": {Metadata: tc.md}})

			res, err := idx.ForwardWalkNow(t.Context(), "product")
			if (err != nil) != tc.wantErr {
				t.Fatalf("error: got %v, want error %v", err, tc.wantErr)
			}
			if res != tc.want {
				t.Fatalf("result: got %+v, want %+v", res, tc.want)
			}
			reqs := ex.requests()
			if len(reqs) != tc.walks {
				t.Fatalf("want %d walk(s), got %+v", tc.walks, reqs)
			}
			if tc.walks == 1 && reqs[0].Metadata != nil {
				t.Fatalf("a nil func walks with no metadata, got %v", reqs[0].Metadata)
			}
		})
	}
}

// A walk that fails doesn't stop the run: the remaining maps are walked, and
// the run's error carries the failed walk's.
func TestForwardWalkNow_FailedWalkDoesNotStopTheRest(t *testing.T) {
	ex := &walkExecuter{pages: threePages(), failActor: "A"}
	idx, st, _ := newWalkIndexer(ex, map[string]ForwardWalkConfig{"product": {
		Metadata: func(context.Context) ([]map[string]string, error) {
			return []map[string]string{actor("A"), actor("B")}, nil
		},
	}})

	res, err := idx.ForwardWalkNow(t.Context(), "product")
	if err == nil || !strings.Contains(err.Error(), "listing failed") {
		t.Fatalf("the run must fail with the failed walk's error, got %v", err)
	}
	if res.Walks != 2 {
		t.Fatalf("result: got %+v, want 2 walks", res)
	}
	reqs := ex.requests()
	if len(reqs) != 2 || !maps.Equal(reqs[1].Metadata, actor("B")) {
		t.Fatalf("the walk after a failed one must run, got %+v", reqs)
	}
	if st.count("BeginBuild:product/6") != 1 {
		t.Fatalf("B's walk must build its resources: %v", st.callsSnapshot())
	}
}

// A walk whose failures are all marked stale — the rebuild's
// RebuildMarkedFailuresError, however wrapped — is a done walk: its count
// adds to FailedResources and is logged, and the run succeeds. Any other
// error is joined into the run's, after every walk ran.
func TestRunForwardWalks_ClassifiesMarkedFailures(t *testing.T) {
	logged := signalLogs(t, forwardWalkMarkedFailuresMsg)
	sels := []ResourceSelector{
		{ResourceType: "product", Metadata: actor("A")},
		{ResourceType: "product", Metadata: actor("B")},
		{ResourceType: "product", Metadata: actor("C")},
	}
	boom := errors.New("boom")
	outcomes := map[string]error{
		"A": fmt.Errorf("rebuild product: %w", &RebuildMarkedFailuresError{ResourceType: "product", Count: 3}),
		"B": nil,
		"C": &RebuildMarkedFailuresError{ResourceType: "product", Count: 2},
	}
	var walked []string
	walk := func(sel ResourceSelector) error {
		walked = append(walked, sel.Metadata["actor"])
		return outcomes[sel.Metadata["actor"]]
	}

	res, err := runForwardWalks("product", sels, slog.Default(), walk, markedFailuresInProcess)
	if err != nil {
		t.Fatalf("a run whose walks only failed marked resources succeeds, got %v", err)
	}
	if want := (ForwardWalkResult{Walks: 3, FailedResources: 5}); res != want {
		t.Fatalf("result: got %+v, want %+v", res, want)
	}
	select {
	case <-logged:
	default:
		t.Fatal("a walk's marked failures must be logged")
	}

	outcomes["B"] = boom
	walked = nil
	res, err = runForwardWalks("product", sels, slog.Default(), walk, markedFailuresInProcess)
	if !errors.Is(err, boom) {
		t.Fatalf("another error fails the run, got %v", err)
	}
	if !slices.Equal(walked, []string{"A", "B", "C"}) {
		t.Fatalf("every walk must run after a failed one, walked %v", walked)
	}
	if want := (ForwardWalkResult{Walks: 3, FailedResources: 5}); res != want {
		t.Fatalf("result: got %+v, want %+v", res, want)
	}
}

// A paced all-of-type walk passes its page size to its plans and, after each
// page but the last, waits out the rest of its page interval, measured from
// the page's start.
func TestRebuild_PacedWalk_PassesPageSizeAndWaitsOutEachPageInterval(t *testing.T) {
	ex := &walkExecuter{pages: threePages()}
	idx, _, waits := newWalkIndexer(ex, nil)

	start := time.Now()
	err := idx.RebuildNowResumable(t.Context(), ResourceSelector{
		ResourceType: "product",
		Pacing:       &WalkPacing{PageSize: 2, PageInterval: time.Hour},
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)

	if reqs := ex.requests(); len(reqs) != 1 || reqs[0].PageSize != 2 {
		t.Fatalf("the walk must ask its plan for pages of 2, got %+v", reqs)
	}
	ds := waits.snapshot()
	if len(ds) != 2 {
		t.Fatalf("want a wait after each page but the last (2), got %v", ds)
	}
	for i, d := range ds {
		if d <= time.Hour-elapsed-time.Second || d > time.Hour {
			t.Fatalf("wait %d: got %v, want the rest of the page's hour", i, d)
		}
	}
}

// While the build pool is pressured a paced walk takes no page: it backs off
// — several times — until the pressure lifts, whether the pressure was there
// before its first page or arose during a page.
func TestRebuild_PacedWalk_BacksOffWhileThePoolIsPressured(t *testing.T) {
	for name, tc := range map[string]struct {
		midRun bool
		// takenWhilePressured is the pages the walk took before it backed off.
		takenWhilePressured []int
		requested           int
	}{
		"before the first page": {takenWhilePressured: nil, requested: 0},
		"after a page":          {midRun: true, takenWhilePressured: []int{0}, requested: 1},
	} {
		t.Run(name, func(t *testing.T) {
			backoff := signalLogs(t, poolBackoffMsg)
			ex := &walkExecuter{pages: threePages()}
			idx, st, _ := newWalkIndexer(ex, nil)

			var release func()
			if tc.midRun {
				// Pressured once the walk has started its plan, before its
				// first page is offered: the walk takes that page, then
				// backs off before the next.
				ex.beforePage = func(i int) {
					if i == 0 {
						release = pressurePool(t, idx)
					}
				}
			} else {
				release = pressurePool(t, idx)
			}

			done := make(chan error, 1)
			go func() {
				done <- idx.RebuildNowResumable(t.Context(), ResourceSelector{
					ResourceType: "product",
					Pacing:       &WalkPacing{PageSize: 2, PageInterval: time.Nanosecond},
				}, nil, nil)
			}()

			for range 3 {
				select {
				case <-backoff:
				case err := <-done:
					t.Fatalf("the walk finished while the pool was pressured: %v", err)
				}
			}
			if got := ex.takenPages(); !slices.Equal(got, tc.takenWhilePressured) {
				t.Fatalf("no page may be taken while the pool is pressured: got %v, want %v", got, tc.takenWhilePressured)
			}
			if got := len(ex.requests()); got != tc.requested {
				t.Fatalf("walk requests while pressured: got %d, want %d", got, tc.requested)
			}

			release()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			waitIdle(t, idx)
			if got := ex.takenPages(); !slices.Equal(got, []int{0, 1, 2}) {
				t.Fatalf("once the pressure lifted the walk must go on: took %v", got)
			}
			if st.count("BeginBuild:product/6") != 1 {
				t.Fatalf("the walk must build every page's resources: %v", st.callsSnapshot())
			}
		})
	}
}

// An unpaced walk — an explicit rebuild, its Pacing nil — leaves the page
// size to its plans and neither waits between pages nor backs off under
// pressure; a targeted rebuild ignores pacing.
func TestRebuild_UnpacedWalkAndTargetedRebuildArentPaced(t *testing.T) {
	backoff := signalLogs(t, poolBackoffMsg)
	ex := &walkExecuter{pages: threePages()}
	idx, st, waits := newWalkIndexer(ex, nil)
	pressurePool(t, idx)

	if err := idx.RebuildNowResumable(t.Context(), ResourceSelector{ResourceType: "product"}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := idx.RebuildNowResumable(t.Context(), ResourceSelector{
		ResourceType: "product",
		ResourceIDs:  []string{"9"},
		Pacing:       &WalkPacing{PageSize: 2, PageInterval: time.Hour},
	}, nil, nil); err != nil {
		t.Fatal(err)
	}

	reqs := ex.requests()
	if len(reqs) != 2 || reqs[0].PageSize != 0 || reqs[1].PageSize != 0 {
		t.Fatalf("an unpaced walk and a targeted rebuild leave the page size to the plan, got %+v", reqs)
	}
	if ds := waits.snapshot(); len(ds) != 0 {
		t.Fatalf("no page-interval wait may be made, got %v", ds)
	}
	select {
	case <-backoff:
		t.Fatal("no back-off may be made")
	default:
	}
	if st.count("BeginBuild:product/6") != 1 || st.count("BeginBuild:product/9") != 1 {
		t.Fatalf("both rebuilds must build: %v", st.callsSnapshot())
	}
}

// A selector's pacing values may not be negative, nor its page size above
// an int32's, the provider contract's page size.
func TestValidateSelectors_RejectsInvalidPacing(t *testing.T) {
	ex := &walkExecuter{pages: threePages()}
	idx, _, _ := newWalkIndexer(ex, nil)
	for name, p := range map[string]*WalkPacing{
		"negative page size":     {PageSize: -1},
		"negative page interval": {PageInterval: -time.Second},
		"page size above int32":  {PageSize: math.MaxInt32 + 1},
	} {
		err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product", Pacing: p}})
		var invalid *InvalidArgumentError
		if !errors.As(err, &invalid) {
			t.Fatalf("a %s: want an InvalidArgumentError, got %v", name, err)
		}
	}
	if reqs := ex.requests(); len(reqs) != 0 {
		t.Fatalf("nothing may be walked, got %+v", reqs)
	}
}
