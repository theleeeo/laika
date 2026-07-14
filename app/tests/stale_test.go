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

// Failure path: the build fails, the stale mark survives, and a later sweep
// (once the provider recovers) rebuilds and clears the mark.
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

	// Provider recovers; the sweep must rebuild and clear the mark.
	t.fakeProvider.SetError("a", "1", nil)

	n, err := t.idx.SweepStale(t.T().Context(), 0, 100)
	t.Require().NoError(err)
	t.Require().GreaterOrEqual(n, 1)
	t.worker.Drain(t.T().Context())

	t.Require().Nil(t.staleSince("a", "1"), "sweep must rebuild and clear the mark")
	t.Require().True(t.docExists("a", "1"), "sweep must land the document")
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

// Sweep finishes a tombstone whose inline delete never ran (e.g. the pool shed
// the work or the process crashed). The sweep resolves the tombstone
// synchronously: hard-deletes the row and removes the document.
func (t *TestSuite) Test_SweepStale_FinishesTombstone() {
	t.setResourceConfig(DefaultResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "value1"})
	err := t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())
	t.Require().True(t.docExists("a", "1"))

	// Simulate a shed inline delete: tombstone the row directly, no pool work.
	_, err = t.st.MarkDeleted(t.T().Context(), model.Resource{Type: "a", Id: "1"})
	t.Require().NoError(err)
	t.Require().Equal(1, t.resourceRowCount("a", "1"), "tombstone must leave the row present until swept")

	n, err := t.idx.SweepStale(t.T().Context(), 0, 100)
	t.Require().NoError(err)
	t.Require().GreaterOrEqual(n, 1)

	t.Require().Equal(0, t.resourceRowCount("a", "1"), "sweep must finish the tombstone and hard-delete the row")
	t.Require().False(t.docExists("a", "1"), "sweep must remove the document")
}
