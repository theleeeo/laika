package core

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/theleeeo/laika/model"
)

// An owned Build renews the leases of its owned ids every third of the
// OwnerLease while it runs (ADR 0014). The renewal's ticker is replaced by a
// manualTicker, so every renewal round is a tick the test sends while the
// build is parked in its plan, never a timed wait: the test waits for each
// round to have returned from the store (renewRounds.tick), a tick is taken
// only once the round before it has finished, and Build returns only once
// its renewal has stopped.

// manualTicker stands in for the Indexer's renewal ticker (renewTicker): the
// test sends each tick on an unbuffered channel.
type manualTicker struct {
	mu        sync.Mutex
	intervals []time.Duration
	ch        chan time.Time
	stopped   chan struct{}
	stopOnce  sync.Once
}

// installManualTicker makes every renewal of idx tick on one manualTicker.
func installManualTicker(idx *Indexer) *manualTicker {
	m := &manualTicker{ch: make(chan time.Time), stopped: make(chan struct{})}
	idx.renewTicker = func(d time.Duration) (<-chan time.Time, func()) {
		m.mu.Lock()
		m.intervals = append(m.intervals, d)
		m.mu.Unlock()
		return m.ch, func() { m.stopOnce.Do(func() { close(m.stopped) }) }
	}
	return m
}

// tick sends one tick; it fails the test if no renewal takes it promptly.
func (m *manualTicker) tick(t *testing.T) {
	t.Helper()
	select {
	case m.ch <- time.Now():
	case <-time.After(2 * time.Second):
		t.Fatal("no renewal took the tick")
	}
}

// untaken reports that no renewal takes a tick within a short wait.
func (m *manualTicker) untaken() bool {
	select {
	case m.ch <- time.Now():
		return false
	case <-time.After(50 * time.Millisecond):
		return true
	}
}

func (m *manualTicker) started() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.intervals)
}

func (m *manualTicker) isStopped() bool {
	select {
	case <-m.stopped:
		return true
	default:
		return false
	}
}

// renewRounds records the ids, with their tokens, of every RenewOwners call
// on st, one entry per call, as "id:token" sorted. done receives once per
// call, when it returns.
type renewRounds struct {
	mu     sync.Mutex
	rounds [][]string
	done   chan struct{}
}

func recordRenewRounds(st *recordingStore) *renewRounds {
	r := &renewRounds{done: make(chan struct{}, 64)}
	st.afterRenew = func() { r.done <- struct{}{} }
	prev := st.onRenew
	st.onRenew = func(owned []Owned) {
		if prev != nil {
			prev(owned)
		}
		round := make([]string, len(owned))
		for i, o := range owned {
			round[i] = fmt.Sprintf("%s:%d", o.Id, o.Token)
		}
		slices.Sort(round)
		r.mu.Lock()
		r.rounds = append(r.rounds, round)
		r.mu.Unlock()
	}
	return r
}

// tick sends one tick and waits for the renewal round it starts to have
// called the store and returned from it. The next tick is taken only once
// the round has finished.
func (r *renewRounds) tick(t *testing.T, m *manualTicker) {
	t.Helper()
	m.tick(t)
	within(t, r.done, "the renewal round")
}

func (r *renewRounds) snapshot() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.rounds)
}

// claim marks and claims ids on st, as a registration does, and returns
// their owner tokens.
func claim(t *testing.T, st *recordingStore, ids ...string) map[string]int64 {
	t.Helper()
	rs := make([]model.Resource, len(ids))
	for i, id := range ids {
		rs[i] = product(id)
	}
	owned, err := st.MarkStale(t.Context(), rs, time.Minute)
	if err != nil || len(owned) != len(ids) {
		t.Fatalf("claiming %v: %v %v", ids, owned, err)
	}
	tokens := make(map[string]int64, len(owned))
	for _, o := range owned {
		tokens[o.Id] = o.Token
	}
	return tokens
}

// goBuild runs an owned Build of ids in the background; the returned channel
// receives its error.
func goBuild(ctx context.Context, idx *Indexer, ids []string, tokens map[string]int64) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- idx.Build(ctx, BuildArgs{ResourceType: "product", ResourceIds: ids, OwnerTokens: tokens})
	}()
	return done
}

func idTokens(tokens map[string]int64, ids ...string) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = fmt.Sprintf("%s:%d", id, tokens[id])
	}
	slices.Sort(out)
	return out
}

// An owned build whose plan blocks for twice the OwnerLease — six ticks of a
// third of it — renews its lease six times, each with its one owned id, and
// finishes as before. The renewal stops when Build returns: a tick sent after
// it is taken by nothing.
func TestBuild_Owned_RenewsItsLeasesEveryThirdOfTheLeaseWhileItBuilds(t *testing.T) {
	st := &recordingStore{}
	idx, ex := newRecordingIndexer(st, 2, 4)
	idx.ownerLease = 30 * time.Second
	ticker := installManualTicker(idx)
	rounds := recordRenewRounds(st)
	ex.arrived = make(chan string, 4)
	ex.proceed = make(chan struct{})
	release := closer(t, ex.proceed)
	tokens := claim(t, st, "1")

	done := goBuild(t.Context(), idx, []string{"1"}, tokens)
	within(t, ex.arrived, "the build, parked in its plan")
	if got := ticker.started(); !slices.Equal(got, []time.Duration{10 * time.Second}) {
		t.Fatalf("an owned build must renew every third of the OwnerLease (10s), its ticker was started with %v", got)
	}
	for range 6 {
		rounds.tick(t, ticker)
	}
	release()
	if err := within(t, done, "the build"); err != nil {
		t.Fatal(err)
	}

	got := rounds.snapshot()
	if len(got) != 6 {
		t.Fatalf("a build blocked for twice the lease must renew six times, renewed %d: %v", len(got), got)
	}
	for _, r := range got {
		if !slices.Equal(r, idTokens(tokens, "1")) {
			t.Fatalf("every renewal names the owned id under its token: %v", got)
		}
	}
	if st.indexOf(fmt.Sprintf("FinishOwned:product/1:%d:%d", tokens["1"], tokens["1"])) == -1 {
		t.Fatalf("the build finishes with FinishOwned: %v", st.callsSnapshot())
	}
	if r, _ := st.row(product("1")); r.stale || r.owner != 0 {
		t.Fatalf("the finish clears the mark and the ownership: %+v", r)
	}
	if !ticker.isStopped() || !ticker.untaken() {
		t.Fatal("the renewal must stop before Build returns")
	}
	if n := len(rounds.snapshot()); n != 6 {
		t.Fatalf("no renewal may run after Build returns, got %d rounds", n)
	}
}

// Renewals that fail are logged at Warn with the type, the count and the
// error and retried at the next tick with the same set: nothing is released,
// no work is skipped, and both ids — the parked one and the one waiting for
// its turn — build and finish. Once the Store answers again, the renewals
// hold.
func TestBuild_Owned_KeepsRenewingThroughFailedRenewals(t *testing.T) {
	logs := captureDefaultLogs(t)
	st := &recordingStore{renewErr: fmt.Errorf("db down")}
	idx, ex := newRecordingIndexer(st, 2, 4)
	ticker := installManualTicker(idx)
	rounds := recordRenewRounds(st)
	ex.arrived = make(chan string, 4)
	ex.proceed = make(chan struct{})
	ex.parkIDs = map[string]bool{"1": true}
	release := closer(t, ex.proceed)
	tokens := claim(t, st, "1", "2")

	done := goBuild(t.Context(), idx, []string{"1", "2"}, tokens)
	within(t, ex.arrived, "id 1's build, parked in its plan")
	for range 3 {
		rounds.tick(t, ticker)
	}
	st.mu.Lock()
	st.renewErr = nil
	st.mu.Unlock()
	for range 3 {
		rounds.tick(t, ticker)
	}
	release()
	if err := within(t, done, "the build"); err != nil {
		t.Fatal(err)
	}

	got := rounds.snapshot()
	if len(got) != 6 {
		t.Fatalf("every tick renews, failed or not: %d rounds %v", len(got), got)
	}
	for _, r := range got {
		if !slices.Equal(r, idTokens(tokens, "1", "2")) {
			t.Fatalf("a failed renewal is retried with the same set, both ids: %v", got)
		}
	}
	for _, id := range []string{"1", "2"} {
		if n := st.count("RenewOwnersFailed:product/" + id + ":"); n != 3 {
			t.Fatalf("id %s: three failed renewals, got %d: %v", id, n, st.callsSnapshot())
		}
		if n := st.count(fmt.Sprintf("RenewOwners:product/%s:", id)); n != 3 {
			t.Fatalf("id %s: three held renewals once the Store answers, got %d: %v", id, n, st.callsSnapshot())
		}
		if st.indexOf(fmt.Sprintf("FinishOwned:product/%s:%d:%d", id, tokens[id], tokens[id])) == -1 {
			t.Fatalf("id %s still builds and finishes: %v", id, st.callsSnapshot())
		}
		if r, _ := st.row(product(id)); r.stale || r.owner != 0 {
			t.Fatalf("id %s: the finish clears the mark and the ownership: %+v", id, r)
		}
	}
	for _, p := range []string{"ReleaseOwners", "ReleaseFailed"} {
		if st.count(p) != 0 {
			t.Fatalf("a failed renewal releases nothing: %v", st.callsSnapshot())
		}
	}

	var warns int
	for _, rec := range logs.records(t) {
		if rec["msg"] != renewDuringBuildFailedMsg {
			continue
		}
		warns++
		if rec["level"] != "WARN" || rec["type"] != "product" || rec["count"] != float64(2) || rec["error"] != "db down" {
			t.Fatalf("a failed renewal is logged at Warn with its type, count and error: %v", rec)
		}
	}
	if warns != 3 {
		t.Fatalf("each failed renewal is logged once, got %d", warns)
	}
}

// The renewal set is the call's owned ids not yet finished: an id that has
// finished is no longer renewed, an id still waiting for its turn is, and an
// id the call doesn't own never is.
func TestBuild_Owned_RenewsItsUnfinishedOwnedIDsOnly(t *testing.T) {
	st := &recordingStore{}
	idx, ex := newRecordingIndexer(st, 2, 4)
	ticker := installManualTicker(idx)
	rounds := recordRenewRounds(st)
	ex.arrived = make(chan string, 4)
	ex.proceed = make(chan struct{})
	ex.parkIDs = map[string]bool{"2": true}
	release := closer(t, ex.proceed)
	tokens := claim(t, st, "1", "2", "3")
	if _, err := st.MarkStale(t.Context(), []model.Resource{product("u")}, 0); err != nil {
		t.Fatal(err)
	}

	done := goBuild(t.Context(), idx, []string{"1", "2", "3", "u"}, tokens)
	within(t, ex.arrived, "id 2's build, parked in its plan")
	rounds.tick(t, ticker)
	rounds.tick(t, ticker)
	release()
	if err := within(t, done, "the build"); err != nil {
		t.Fatal(err)
	}

	got := rounds.snapshot()
	want := idTokens(tokens, "2", "3")
	if len(got) != 2 || !slices.Equal(got[0], want) || !slices.Equal(got[1], want) {
		t.Fatalf("with id 1 finished, id 2 building and id 3 waiting, each renewal names 2 and 3 only (%v): %v", want, got)
	}
}

// An id a renewal reports lost — its lease lapsed and another owner claimed
// the row — is logged once at Info and leaves the set; its build goes on and
// finishes, matching nothing: the new owner keeps the row and its mark, and
// no follow-up is handed on. The other id is still renewed.
func TestBuild_Owned_AnIDARenewalReportsLostIsNotRenewedAgainAndBuildsOn(t *testing.T) {
	logs := captureDefaultLogs(t)
	st := &recordingStore{}
	idx, ex := newRecordingIndexer(st, 2, 4)
	ticker := installManualTicker(idx)
	newToken := stealOnRenew(st, product("1"), false)
	rounds := recordRenewRounds(st)
	ex.arrived = make(chan string, 4)
	ex.proceed = make(chan struct{})
	ex.parkIDs = map[string]bool{"1": true}
	release := closer(t, ex.proceed)
	tokens := claim(t, st, "1", "2")

	done := goBuild(t.Context(), idx, []string{"1", "2"}, tokens)
	within(t, ex.arrived, "id 1's build, parked in its plan")
	for range 3 {
		rounds.tick(t, ticker)
	}
	release()
	if err := within(t, done, "the build"); err != nil {
		t.Fatal(err)
	}

	got := rounds.snapshot()
	if len(got) != 3 || !slices.Equal(got[0], idTokens(tokens, "1", "2")) ||
		!slices.Equal(got[1], idTokens(tokens, "2")) || !slices.Equal(got[2], idTokens(tokens, "2")) {
		t.Fatalf("the first renewal names both ids and reports 1 lost; the later ones name 2 only: %v", got)
	}
	if n := len(ex.requestsFor("1")); n != 1 {
		t.Fatalf("id 1's build goes on after its loss: %d requests", n)
	}
	if st.indexOf(fmt.Sprintf("FinishOwned:product/1:%d:%d", tokens["1"], tokens["1"])) == -1 {
		t.Fatalf("id 1 finishes under its old token: %v", st.callsSnapshot())
	}
	if fus := st.followUpsOf(product("1")); len(fus) != 0 {
		t.Fatalf("the lost id's finish matches nothing and hands on no follow-up: %v", fus)
	}
	if r, _ := st.row(product("1")); !r.stale || r.owner != newToken() {
		t.Fatalf("the new owner (token %d) keeps id 1's row and its mark: %+v", newToken(), r)
	}
	if st.count("ReleaseOwners") != 0 || st.count("ReleaseFailed") != 0 {
		t.Fatalf("a lost id is not released: %v", st.callsSnapshot())
	}

	var lost []map[string]any
	for _, rec := range logs.records(t) {
		if rec["msg"] == renewDuringBuildLostMsg {
			lost = append(lost, rec)
		}
	}
	if len(lost) != 1 || lost[0]["level"] != "INFO" || lost[0]["type"] != "product" || lost[0]["id"] != "1" ||
		lost[0]["token"] != float64(tokens["1"]) {
		t.Fatalf("the lost id is logged once at Info with its type, id and token: %v", lost)
	}
}

// A Build that owns nothing starts no renewal and never calls RenewOwners.
func TestBuild_OwningNothing_NeverRenews(t *testing.T) {
	st := &recordingStore{}
	idx, ex := newRecordingIndexer(st, 2, 4)
	ticker := installManualTicker(idx)
	ex.arrived = make(chan string, 4)
	ex.proceed = make(chan struct{})
	release := closer(t, ex.proceed)

	done := goBuild(t.Context(), idx, []string{"1"}, nil)
	within(t, ex.arrived, "the build, parked in its plan")
	if !ticker.untaken() {
		t.Fatal("a build that owns nothing must not renew")
	}
	release()
	if err := within(t, done, "the build"); err != nil {
		t.Fatal(err)
	}
	if got := ticker.started(); len(got) != 0 {
		t.Fatalf("a build that owns nothing starts no renewal: %v", got)
	}
	if st.count("RenewOwners") != 0 {
		t.Fatalf("a build that owns nothing never calls RenewOwners: %v", st.callsSnapshot())
	}
}

// The renewal stops when ctx ends, while the build is still parked, and no
// renewal runs after Build returns.
func TestBuild_Owned_RenewalStopsWhenCtxEnds(t *testing.T) {
	st := &recordingStore{}
	idx, ex := newRecordingIndexer(st, 2, 4)
	ticker := installManualTicker(idx)
	rounds := recordRenewRounds(st)
	ex.arrived = make(chan string, 4)
	ex.proceed = make(chan struct{})
	release := closer(t, ex.proceed)
	tokens := claim(t, st, "1", "2")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := goBuild(ctx, idx, []string{"1", "2"}, tokens)
	within(t, ex.arrived, "id 1's build, parked in its plan")
	rounds.tick(t, ticker)
	cancel()
	within(t, ticker.stopped, "the renewal's stop once ctx ended")
	release()
	_ = within(t, done, "the build")

	if n := len(rounds.snapshot()); n != 1 {
		t.Fatalf("one renewal before ctx ended, none after: %d", n)
	}
	if !ticker.untaken() {
		t.Fatal("no renewal may run after Build returns")
	}
}
