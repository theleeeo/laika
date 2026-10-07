package tests

import (
	"errors"
	"time"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
)

// staleSince reads the resource's stale mark; nil means clean.
func (t *TestSuite) staleSince(resourceType, id string) *time.Time {
	var ts *time.Time
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT stale_since FROM resources WHERE type=$1 AND id=$2`, resourceType, id).Scan(&ts)
	t.Require().NoError(err)
	return ts
}

// resourceRowCount reports how many rows exist for (resourceType, id) in the
// resources table.
func (t *TestSuite) resourceRowCount(resourceType, id string) int {
	var n int
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT count(*) FROM resources WHERE type=$1 AND id=$2`, resourceType, id).Scan(&n)
	t.Require().NoError(err)
	return n
}

// docExists reports whether a document for (resourceType, id) is present in ES.
// It reads via a direct GET against the resource's read alias, so it does not
// rely on full-text query analysis (see task brief). ES writes in the suite use
// refresh=true, so a completed build is immediately visible here.
func (t *TestSuite) docExists(resourceType, id string) bool {
	res, err := t.esClient.Get(core.AliasName(resourceType), id)
	t.Require().NoError(err)
	defer res.Body.Close()
	if res.StatusCode == 404 {
		return false
	}
	t.Require().Falsef(res.IsError(), "unexpected ES GET status for %s/%s: %s", resourceType, id, res.Status())
	return true
}

// Inline happy path: a successful inline build clears the stale mark and lands
// the document.
func (t *TestSuite) Test_InlineBuild_ClearsStaleMark() {
	t.setResourceConfig(DefaultResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "value1"})

	err := t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	t.Require().Nil(t.staleSince("a", "1"), "successful inline build must clear the mark")
	t.Require().True(t.docExists("a", "1"), "successful inline build must land the document")
}

// Failure path: the build fails, the stale mark survives and the row is backed
// off, and once its backoff has passed (sweep_after backdated past the suite
// indexer's default) a sweep — the provider having recovered — rebuilds it and
// clears the mark and the backoff.
func (t *TestSuite) Test_FailedBuild_StaysStale_SweepRecovers() {
	t.setResourceConfig(DefaultResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "value1"})
	t.fakeProvider.SetError("a", "1", errors.New("provider down"))

	err := t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	t.Require().NotNil(t.staleSince("a", "1"), "failed build must leave the stale mark")
	t.Require().False(t.docExists("a", "1"), "failed build must not have landed a document")

	attempts, after := t.sweepBackoff("a", "1")
	t.Require().NotNil(attempts, "the failed inline build must back the row off")
	t.Require().Equal(1, *attempts)
	t.Require().NotNil(after)

	// Provider recovers; the sweep must rebuild and clear the mark once the
	// row's backoff has passed.
	t.fakeProvider.SetError("a", "1", nil)
	_, err = t.pool.Exec(t.T().Context(),
		`UPDATE resources SET sweep_after = now() - interval '1 millisecond' WHERE type='a' AND id='1'`)
	t.Require().NoError(err)

	n, err := t.idx.SweepStale(t.T().Context(), 0, 100)
	t.Require().NoError(err)
	t.Require().GreaterOrEqual(n, 1)
	t.worker.Drain(t.T().Context())

	t.Require().Nil(t.staleSince("a", "1"), "sweep must rebuild and clear the mark")
	t.Require().True(t.docExists("a", "1"), "sweep must land the document")
	attempts, after = t.sweepBackoff("a", "1")
	t.Require().Nil(attempts, "the successful sweep build resets the backoff")
	t.Require().Nil(after)
}

// Delete happy path: an inline delete removes the document and hard-deletes the
// resource row once the pool drains.
func (t *TestSuite) Test_DeleteNotification_RemovesDocAndRow() {
	t.setResourceConfig(DefaultResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "value1"})
	err := t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())
	t.Require().True(t.docExists("a", "1"))

	t.fakeProvider.DeleteResource("a", "1")
	err = t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeDeleted,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	t.Require().Equal(0, t.resourceRowCount("a", "1"), "finished tombstone must hard-delete the row")
	t.Require().False(t.docExists("a", "1"), "delete must remove the document")
}

// Sweep finishes a tombstone whose inline delete never ran (e.g. the process
// crashed after the mark claimed it). The sweep skips a row with a live owner,
// so the case lets the claim's lease expire; the sweep then resolves the
// tombstone synchronously: hard-deletes the row and removes the document.
func (t *TestSuite) Test_SweepStale_FinishesTombstone() {
	t.setResourceConfig(DefaultResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "value1"})
	err := t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())
	t.Require().True(t.docExists("a", "1"))

	// Simulate a crashed inline delete: tombstone the row directly, no pool
	// work. The registration claims the row for the delete that never runs.
	_, err = t.st.RegisterChanges(t.T().Context(), []core.Registration{
		{Resource: model.Resource{Type: "a", Id: "1"}, Deleted: true},
	}, time.Minute)
	t.Require().NoError(err)
	t.Require().Equal(1, t.resourceRowCount("a", "1"), "tombstone must leave the row present until swept")

	// The crashed owner's lease expires.
	_, err = t.pool.Exec(t.T().Context(),
		`UPDATE resources SET owner_since = now() - interval '1 hour' WHERE type='a' AND id='1'`)
	t.Require().NoError(err)

	n, err := t.idx.SweepStale(t.T().Context(), 0, 100)
	t.Require().NoError(err)
	t.Require().GreaterOrEqual(n, 1)

	t.Require().Equal(0, t.resourceRowCount("a", "1"), "sweep must finish the tombstone and hard-delete the row")
	t.Require().False(t.docExists("a", "1"), "sweep must remove the document")
}

// sweepBackoff reads the resource's backoff columns; nil means never failed.
func (t *TestSuite) sweepBackoff(resourceType, id string) (attempts *int, after *time.Time) {
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT sweep_attempts, sweep_after FROM resources WHERE type=$1 AND id=$2`, resourceType, id).Scan(&attempts, &after)
	t.Require().NoError(err)
	return attempts, after
}

// pgNow reads Postgres's clock, the one sweep_after is stamped with.
func (t *TestSuite) pgNow() time.Time {
	var now time.Time
	t.Require().NoError(t.pool.QueryRow(t.T().Context(), `SELECT now()`).Scan(&now))
	return now
}

// Test_SweepStale_BacksOffAFailingResource: a/1, whose fetch always fails,
// was stale before a/2, and the sweep serves one resource a pass. The first
// pass takes a/1 and its failed build backs it off: a pass skips a/1 while
// its sweep_after is ahead, and once it has passed a/1 queues behind a/2's
// mark, so the next pass serves a/2 instead of a/1 again. Each further failure of a/1 backs it off by
// SweepBackoff doubled per attempt, up to SweepBackoffMax. Its backoff is
// read against Postgres's clock: the failure's now() falls between the
// clock read before the pass and the one after it, so sweep_after minus the
// first is at least the backoff and minus the second at most.
func (t *TestSuite) Test_SweepStale_BacksOffAFailingResource() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()
	const base, maxBackoff = 10 * time.Millisecond, 40 * time.Millisecond
	x := t.newIndexer(DefaultResourceConfig, core.Config{SweepBackoff: base, SweepBackoffMax: maxBackoff})

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "failing"})
	t.fakeProvider.SetError("a", "1", errors.New("provider down"))
	t.fakeProvider.SetResource("a", "2", map[string]any{"id": "2", "field1": "healthy"})

	// Both are marked by a claim whose owner crashed; a/1 is the older mark.
	_, err := t.st.MarkStale(ctx, []model.Resource{{Type: "a", Id: "1"}, {Type: "a", Id: "2"}}, time.Minute)
	t.Require().NoError(err)
	_, err = t.pool.Exec(ctx, `UPDATE resources SET owner_since = now() - interval '1 hour',
		stale_since = CASE id WHEN '1' THEN now() - interval '2 minutes' ELSE now() - interval '1 minute' END
		WHERE type='a' AND id IN ('1','2')`)
	t.Require().NoError(err)

	// failPass runs one pass, which must be a failed build of a/1, and checks
	// the backoff it left.
	failPass := func(attempt int, backoff time.Duration) time.Time {
		before := t.pgNow()
		n, err := x.SweepStale(ctx, 0, 1)
		t.Require().NoError(err)
		t.Require().Equalf(1, n, "pass of attempt %d serves one resource", attempt)
		after := t.pgNow()

		attempts, sweepAfter := t.sweepBackoff("a", "1")
		t.Require().NotNilf(attempts, "attempt %d must count", attempt)
		t.Require().Equalf(attempt, *attempts, "a/1's failures in a row")
		t.Require().NotNil(sweepAfter)
		t.Require().GreaterOrEqualf(sweepAfter.Sub(before), backoff, "attempt %d backs a/1 off by at least %s", attempt, backoff)
		t.Require().LessOrEqualf(sweepAfter.Sub(after), backoff, "attempt %d backs a/1 off by at most %s", attempt, backoff)
		t.Require().NotNil(t.staleSince("a", "1"), "a failed build leaves a/1 stale")
		t.Require().False(t.docExists("a", "1"))
		return *sweepAfter
	}

	// Pass 1: a/1, the older mark, fails.
	after1 := failPass(1, base)
	t.Require().Equal(1, t.fakeProvider.FetchCount("a", "1"))
	t.Require().NotNil(t.staleSince("a", "2"), "pass 1 served a/1 only")
	t.Require().False(t.docExists("a", "2"))

	// A backed-off row is skipped while its sweep_after is ahead: with a/1's
	// an hour ahead, a pass whose threshold (90s) only a/1's mark (2 minutes
	// old) passes serves nothing.
	_, err = t.pool.Exec(ctx, `UPDATE resources SET sweep_after = now() + interval '1 hour' WHERE type='a' AND id='1'`)
	t.Require().NoError(err)
	n, err := x.SweepStale(ctx, 90*time.Second, 100)
	t.Require().NoError(err)
	t.Require().Zero(n, "a/1 is skipped while its sweep_after is ahead")
	t.Require().Equal(1, t.fakeProvider.FetchCount("a", "1"))

	// Pass 2: a/1's backoff has passed (its sweep_after backdated), so both
	// are eligible; a/1's turn is now its sweep_after, behind a/2's mark, so
	// the pass serves a/2 though a/1 was stale first.
	_, err = t.pool.Exec(ctx, `UPDATE resources SET sweep_after = now() - interval '1 millisecond' WHERE type='a' AND id='1'`)
	t.Require().NoError(err)
	n, err = x.SweepStale(ctx, 0, 1)
	t.Require().NoError(err)
	t.Require().Equal(1, n)
	t.Require().Equal(1, t.fakeProvider.FetchCount("a", "1"), "pass 2 must not retry a/1")
	t.Require().Nil(t.staleSince("a", "2"), "pass 2 serves a/2 though a/1 was stale first")
	t.Require().True(t.docExists("a", "2"))
	attempts, _ := t.sweepBackoff("a", "2")
	t.Require().Nil(attempts, "a/2 never failed")

	// Further passes: a/1 alone is left. Each is let past its backoff (its
	// sweep_after backdated, rather than waited out), fails again, and is
	// backed off for twice as long as the one before, up to the cap; its
	// sweep_after moves later each time.
	prev := after1
	for _, step := range []struct {
		attempt int
		backoff time.Duration
	}{{2, 2 * base}, {3, 4 * base}, {4, maxBackoff}, {5, maxBackoff}} {
		_, err := t.pool.Exec(ctx, `UPDATE resources SET sweep_after = now() - interval '1 millisecond' WHERE type='a' AND id='1'`)
		t.Require().NoError(err)
		next := failPass(step.attempt, step.backoff)
		t.Require().Truef(next.After(prev), "attempt %d's sweep_after is later than the one before", step.attempt)
		prev = next
	}
	t.Require().Equal(5, t.fakeProvider.FetchCount("a", "1"))
}
