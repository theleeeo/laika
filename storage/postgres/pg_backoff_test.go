package postgres

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
)

// ---- The sweep's backoff (L2.8) ----

// backoff is a row's sweep_attempts and sweep_after; nil pointers are NULL.
type backoff struct {
	attempts *int
	after    *time.Time
}

func (b backoff) none() bool { return b.attempts == nil && b.after == nil }

func (b backoff) is(c backoff) bool {
	if (b.attempts == nil) != (c.attempts == nil) || (b.after == nil) != (c.after == nil) {
		return false
	}
	if b.attempts != nil && *b.attempts != *c.attempts {
		return false
	}
	return b.after == nil || b.after.Equal(*c.after)
}

func (b backoff) String() string {
	a, s := "NULL", "NULL"
	if b.attempts != nil {
		a = fmt.Sprint(*b.attempts)
	}
	if b.after != nil {
		s = b.after.Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("sweep_attempts %s sweep_after %s", a, s)
}

// backoffOf reads a row's backoff columns.
func backoffOf(t *testing.T, pool *pgxpool.Pool, res model.Resource) backoff {
	t.Helper()
	var b backoff
	if err := pool.QueryRow(context.Background(),
		`SELECT sweep_attempts, sweep_after FROM resources WHERE type=$1 AND id=$2`, res.Type, res.Id).Scan(&b.attempts, &b.after); err != nil {
		t.Fatalf("read backoff %s/%s: %v", res.Type, res.Id, err)
	}
	return b
}

// backOff sets a row's backoff directly, independent of the Store: attempts
// failures, its turn in (a Postgres interval, negative for the past) from
// now. It returns what it set.
func backOff(t *testing.T, pool *pgxpool.Pool, res model.Resource, attempts int, in string) backoff {
	t.Helper()
	var b backoff
	if err := pool.QueryRow(context.Background(),
		`UPDATE resources SET sweep_attempts = $3, sweep_after = now() + $4::interval WHERE type=$1 AND id=$2
		 RETURNING sweep_attempts, sweep_after`,
		res.Type, res.Id, attempts, in).Scan(&b.attempts, &b.after); err != nil {
		t.Fatalf("back off %s/%s: %v", res.Type, res.Id, err)
	}
	return b
}

// requireBackoff fails unless the row's backoff columns are want.
func requireBackoff(t *testing.T, pool *pgxpool.Pool, res model.Resource, want backoff, why string) {
	t.Helper()
	if got := backoffOf(t, pool, res); !got.is(want) {
		t.Fatalf("%s/%s: %s: %v, want %v", res.Type, res.Id, why, got, want)
	}
}

// requireNoBackoff fails unless both backoff columns are NULL.
func requireNoBackoff(t *testing.T, pool *pgxpool.Pool, res model.Resource, why string) {
	t.Helper()
	requireBackoff(t, pool, res, backoff{}, why)
}

// dbNow reads the database's clock, which now() inside a statement run
// between two dbNow calls falls between.
func dbNow(t *testing.T, pool *pgxpool.Pool) time.Time {
	t.Helper()
	var now time.Time
	if err := pool.QueryRow(context.Background(), `SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatalf("read clock: %v", err)
	}
	return now
}

// releaseFailed runs ReleaseFailed and fails the test on error, returning
// the database clock just before and just after the call.
func releaseFailed(t *testing.T, st *Store, pool *pgxpool.Pool, owned []core.Owned, b core.SweepBackoff) (got []core.BackedOff, before, after time.Time) {
	t.Helper()
	before = dbNow(t, pool)
	got, err := st.ReleaseFailed(context.Background(), owned, b)
	if err != nil {
		t.Fatalf("ReleaseFailed: %v", err)
	}
	return got, before, dbNow(t, pool)
}

// requireTurnIn fails unless the row was backed off delay from a now()
// between before and after: its sweep_after, and the BackedOff's After.
func requireTurnIn(t *testing.T, got core.BackedOff, before, after time.Time, delay time.Duration) {
	t.Helper()
	if lo, hi := before.Add(delay), after.Add(delay); got.After.Before(lo) || got.After.After(hi) {
		t.Fatalf("%s/%s at attempt %d: sweep_after %v, want %v from now(), between %v and %v",
			got.Type, got.Id, got.Attempts, got.After, delay, lo, hi)
	}
}

func TestListStale_SkipsABackedOffRowUntilItsTurnAndOrdersByIt(t *testing.T) {
	st, pool := isolatedStore(t)
	r := func(id string) model.Resource { return model.Resource{Type: "bo-ls", Id: id} }
	due, waiting, fresh, recent := r("due"), r("waiting"), r("fresh"), r("recent")
	// due was marked first, but failed: its turn came a minute ago.
	seedStale(t, pool, due, 1, "30 minutes", false)
	dueBackoff := backOff(t, pool, due, 3, "-1 minute")
	// waiting failed too, and its turn is an hour away.
	seedStale(t, pool, waiting, 2, "20 minutes", false)
	backOff(t, pool, waiting, 1, "1 hour")
	// Never failed: their turn is their mark.
	seedStale(t, pool, fresh, 3, "5 minutes", false)
	seedStale(t, pool, recent, 4, "2 minutes", false)

	// The candidates are taken by their turn: limit 1 takes fresh, whose
	// mark is newer than due's.
	if got := listStale(t, st, 1); !reflect.DeepEqual(staleIDs(got), []string{"fresh"}) {
		t.Fatalf("limit 1: got %v, want [fresh], the earliest turn", staleIDs(got))
	}
	// And returned by it; waiting's turn has not come.
	if got := listStale(t, st, 10); !reflect.DeepEqual(staleIDs(got), []string{"recent", "due"}) {
		t.Fatalf("got %v, want [recent due]: by turn, waiting skipped until its sweep_after", staleIDs(got))
	}
	requireBackoff(t, pool, due, dueBackoff, "a claim leaves the backoff")
	requireOwner(t, pool, waiting, owner{}, "a row whose turn has not come is not claimed")

	// Once its turn passes, waiting is listed.
	backOff(t, pool, waiting, 1, "-1 second")
	if got := listStale(t, st, 10); !reflect.DeepEqual(staleIDs(got), []string{"waiting"}) {
		t.Fatalf("after its turn: got %v, want [waiting]", staleIDs(got))
	}
}

func TestReleaseFailed_ReleasesAndBacksOffDoublingUpToTheCap(t *testing.T) {
	st := NewStore(testPool)
	res := model.Resource{Type: "bo-rf", Id: "1"}
	register(t, st, core.Registration{Resource: res, Version: 1, Metadata: meta("m")})
	_, _, seq, since, _ := row(t, res)
	b := core.SweepBackoff{Base: time.Minute, Max: 10 * time.Minute}

	for n, delay := range []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 10 * time.Minute, 10 * time.Minute} {
		attempts := n + 1
		tok := own(t, testPool, res)
		got, before, after := releaseFailed(t, st, testPool, []core.Owned{{Resource: res, Token: tok}}, b)
		if len(got) != 1 || got[0].Resource != res || got[0].Attempts != attempts {
			t.Fatalf("attempt %d: got %+v, want %s/%s backed off at attempt %d", attempts, got, res.Type, res.Id, attempts)
		}
		requireTurnIn(t, got[0], before, after, delay)
		requireBackoff(t, testPool, res, backoff{attempts: &attempts, after: &got[0].After}, "the row stores what ReleaseFailed returned")
		requireOwner(t, testPool, res, owner{}, "a failure releases the ownership")
		if _, _, s, sn, _ := row(t, res); s != seq || sn == nil || !sn.Equal(*since) {
			t.Fatalf("attempt %d: the mark must stay: seq %d (was %d) since %v (was %v)", attempts, s, seq, sn, since)
		}
	}
}

func TestReleaseFailed_ReleasesOnlyMatchingTokensAndReturnsThoseRows(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "bo-rm", Id: id} }
	match, mismatch, unowned, zero := r("match"), r("mismatch"), r("unowned"), r("zero")
	markUnclaimed(t, st, match, mismatch, unowned, zero)
	tokens := map[model.Resource]int64{}
	for _, res := range []model.Resource{match, mismatch, zero} {
		tokens[res] = own(t, testPool, res)
	}
	owners := map[model.Resource]owner{}
	for _, res := range []model.Resource{mismatch, zero} {
		owners[res] = ownerOf(t, testPool, res)
	}

	got, before, after := releaseFailed(t, st, testPool, []core.Owned{
		{Resource: mismatch, Token: tokens[mismatch] + 1},
		{Resource: match, Token: tokens[match]},
		{Resource: unowned, Token: 1},
		{Resource: r("absent"), Token: 1},
		{Resource: zero, Token: 0},
	}, core.SweepBackoff{Base: time.Minute, Max: time.Hour})
	if len(got) != 1 || got[0].Resource != match || got[0].Attempts != 1 {
		t.Fatalf("got %+v, want only match, at attempt 1", got)
	}
	requireTurnIn(t, got[0], before, after, time.Minute)
	requireOwner(t, testPool, match, owner{}, "a matching token is released")
	for _, res := range []model.Resource{mismatch, zero} {
		requireOwner(t, testPool, res, owners[res], "an ownership not held is not released")
		requireNoBackoff(t, testPool, res, "an ownership not held is not backed off")
	}
	requireNoBackoff(t, testPool, unowned, "an unowned row is not backed off")

	if got, err := st.ReleaseFailed(ctx, nil, core.SweepBackoff{Base: time.Minute, Max: time.Hour}); err != nil || len(got) != 0 {
		t.Fatalf("empty input must be a no-op: %v %v", got, err)
	}
}

func TestReleaseFailed_AHugeAttemptCountBacksOffByTheCap(t *testing.T) {
	st := NewStore(testPool)
	b := core.SweepBackoff{Base: 5 * time.Minute, Max: 24 * time.Hour}
	for _, prior := range []int{1000, math.MaxInt32 - 1} {
		res := model.Resource{Type: "bo-rh", Id: fmt.Sprint(prior)}
		markUnclaimed(t, st, res)
		backOff(t, testPool, res, prior, "-1 second")
		tok := own(t, testPool, res)

		got, before, after := releaseFailed(t, st, testPool, []core.Owned{{Resource: res, Token: tok}}, b)
		if len(got) != 1 || got[0].Attempts != prior+1 {
			t.Fatalf("after %d failures: got %+v, want attempt %d", prior, got, prior+1)
		}
		requireTurnIn(t, got[0], before, after, b.Max)
	}
}

func TestFinishOwned_ResetsTheBackoffInBothBranches(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	cleared, moved, lost := model.Resource{Type: "bo-fo", Id: "cleared"}, model.Resource{Type: "bo-fo", Id: "moved"}, model.Resource{Type: "bo-fo", Id: "lost"}
	markUnclaimed(t, st, cleared, moved, lost)
	tokens := map[model.Resource]int64{}
	for _, res := range []model.Resource{cleared, moved, lost} {
		tokens[res] = own(t, testPool, res)
		backOff(t, testPool, res, 2, "-1 minute")
	}
	markUnclaimed(t, st, moved, lost) // their seqs move
	lostBackoff := backOff(t, testPool, lost, 2, "-1 minute")

	if f, err := st.FinishOwned(ctx, cleared, tokens[cleared], tokens[cleared]); err != nil || f.Token != 0 {
		t.Fatalf("clear: %+v %v", f, err)
	}
	requireNoBackoff(t, testPool, cleared, "a finish that clears the mark resets the backoff")

	if f, err := st.FinishOwned(ctx, moved, tokens[moved], tokens[moved]); err != nil || f.Token == 0 {
		t.Fatalf("re-claim: %+v %v", f, err)
	}
	requireNoBackoff(t, testPool, moved, "a finish that re-claims for a follow-up resets the backoff")

	// A finish whose ownership was lost changes nothing, as the contract says.
	if f, err := st.FinishOwned(ctx, lost, tokens[lost], tokens[lost]+1); err != nil || f.Token != 0 {
		t.Fatalf("lost: %+v %v", f, err)
	}
	requireBackoff(t, testPool, lost, lostBackoff, "a finish that matches nothing changes nothing")
}

func TestClearStale_ResetsTheBackoffWithOrWithoutTheMark(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	cleared, moved := model.Resource{Type: "bo-cs", Id: "cleared"}, model.Resource{Type: "bo-cs", Id: "moved"}
	markUnclaimed(t, st, cleared, moved)
	marked := map[model.Resource]int64{cleared: own(t, testPool, cleared), moved: own(t, testPool, moved)}
	markUnclaimed(t, st, moved) // seq moves
	movedOwner := ownerOf(t, testPool, moved)
	_, _, movedSeq, movedSince, _ := row(t, moved)
	for _, res := range []model.Resource{cleared, moved} {
		backOff(t, testPool, res, 4, "1 hour")
		if err := st.ClearStale(ctx, res, marked[res]); err != nil {
			t.Fatal(err)
		}
		requireNoBackoff(t, testPool, res, "a build that succeeded resets the backoff")
	}
	if _, _, _, since, _ := row(t, cleared); since != nil {
		t.Fatalf("matching seq must clear the mark: since %v", since)
	}
	requireOwner(t, testPool, cleared, owner{}, "matching seq must clear the ownership with the mark")
	if _, _, seq, since, _ := row(t, moved); seq != movedSeq || since == nil || !since.Equal(*movedSince) {
		t.Fatalf("moved seq must keep the mark: seq %d (was %d) since %v (was %v)", seq, movedSeq, since, movedSince)
	}
	requireOwner(t, testPool, moved, movedOwner, "moved seq must keep the ownership")
}

func TestDeleteResourceIfSeq_ResetsTheBackoffWhenTheSeqMoved(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "bo-dr", Id: id} }

	// The re-claim branch: the owner's follow-up starts with no backoff.
	mine := r("mine")
	seq := register(t, st, core.Registration{Resource: mine, Deleted: true}).Items[0].StaleSeq
	tok := own(t, testPool, mine)
	backOff(t, testPool, mine, 2, "-1 minute")
	re := register(t, st, core.Registration{Resource: mine, Version: 1}).Items[0].StaleSeq // a recreate under the live lease
	backOff(t, testPool, mine, 2, "-1 minute")                                             // the registration reset it
	f, err := st.DeleteResourceIfSeq(ctx, mine, seq, tok)
	if err != nil {
		t.Fatal(err)
	}
	requireFollowUp(t, f, core.FollowUp{Token: re})
	requireFreshOwner(t, testPool, mine, re)
	requireNoBackoff(t, testPool, mine, "a delete whose seq moved resets the backoff as it re-claims")

	// A delete that owns nothing, or whose ownership was lost, re-claims
	// nothing, and still succeeded.
	for _, c := range []struct {
		id    string
		token func(tok int64) int64
	}{{"lost", func(tok int64) int64 { return tok + 1 }}, {"unowned", func(int64) int64 { return 0 }}} {
		res := r(c.id)
		seq := register(t, st, core.Registration{Resource: res, Deleted: true}).Items[0].StaleSeq
		tok := own(t, testPool, res)
		markUnclaimed(t, st, res) // seq moves
		want := ownerOf(t, testPool, res)
		_, _, moved, since, _ := row(t, res)
		backOff(t, testPool, res, 2, "-1 minute")

		f, err := st.DeleteResourceIfSeq(ctx, res, seq, c.token(tok))
		if err != nil {
			t.Fatal(err)
		}
		requireFollowUp(t, f, core.FollowUp{})
		requireOwner(t, testPool, res, want, c.id+": the ownership is left as it is")
		if _, _, s, sn, deleted := row(t, res); !deleted || s != moved || sn == nil || !sn.Equal(*since) {
			t.Fatalf("%s: the row must stay as it was: deleted %v seq %d (want %d) since %v (want %v)", c.id, deleted, s, moved, sn, since)
		}
		requireNoBackoff(t, testPool, res, c.id+": a delete whose seq moved resets the backoff")
	}
}

func TestRegisterChanges_ResetsTheBackoffOfItsOwnItemOnly(t *testing.T) {
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "bo-rc", Id: id} }
	child, parent, stale, gone := r("child"), r("parent"), r("stale"), r("gone")
	relate(t, [2]model.Resource{parent, child})
	register(t, st,
		core.Registration{Resource: child, Version: 1},
		core.Registration{Resource: parent, Version: 1},
		core.Registration{Resource: stale, Version: 5},
		core.Registration{Resource: gone, Version: 1})
	backoffs := map[model.Resource]backoff{}
	for _, res := range []model.Resource{child, parent, stale, gone} {
		expireOwner(t, testPool, res)
		backoffs[res] = backOff(t, testPool, res, 3, "1 hour")
	}

	got := register(t, st,
		core.Registration{Resource: child, Version: 2},
		core.Registration{Resource: stale, Version: 4}, // older than the row's: writes nothing
		core.Registration{Resource: gone, Deleted: true})
	if !got.Items[0].Accepted || got.Items[1].Accepted || !got.Items[2].Accepted || len(got.Parents) != 1 || got.Parents[0].Resource != parent {
		t.Fatalf("registration: %+v, want child and gone accepted, stale rejected, parent marked", got)
	}
	requireNoBackoff(t, testPool, child, "a registration of the resource itself resets its backoff")
	requireNoBackoff(t, testPool, gone, "a delete registration of the resource itself resets its backoff")
	requireBackoff(t, testPool, parent, backoffs[parent], "a Parent mark leaves the backoff")
	requireBackoff(t, testPool, stale, backoffs[stale], "a stale version writes nothing")
}

func TestMarkStale_LeavesTheBackoff(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	unclaimed, claimed := model.Resource{Type: "bo-ms", Id: "unclaimed"}, model.Resource{Type: "bo-ms", Id: "claimed"}
	markUnclaimed(t, st, unclaimed, claimed)
	want := map[model.Resource]backoff{
		unclaimed: backOff(t, testPool, unclaimed, 2, "1 hour"),
		claimed:   backOff(t, testPool, claimed, 2, "1 hour"),
	}

	markUnclaimed(t, st, unclaimed)
	owned, err := st.MarkStale(ctx, []model.Resource{claimed}, time.Minute)
	if err != nil || len(owned) != 1 {
		t.Fatalf("MarkStale with a lease must claim the unowned row: %+v %v", owned, err)
	}
	for res, b := range want {
		requireBackoff(t, testPool, res, b, "MarkStale leaves the backoff")
	}
}
