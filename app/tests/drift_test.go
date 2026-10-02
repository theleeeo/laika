package tests

import (
	"context"
	"time"

	"github.com/theleeeo/laika/app/source"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
)

// The drift check (ADR 0002) on the Change Sequence: every accepted
// registration stamps resources.change_seq from the sequence, and a build
// re-schedules its root when a fetched child (or, for a plan walk's root, the
// root itself) carries a change_seq above the build's start. These cases pin
// the interleavings the check exists for; every race among them is forced
// through provider gates.
//
// Child data never carries the parent's join key (c_id): ADR 0006 would make
// the child's own build schedule the parent, and the extra build would break
// the build counts. No b resources are created: the FakeProvider keys
// relations by "type|keyValue" only, so b/x's and c/x's relations to a share
// the key "a|x". Field values are separator-free for the n-gram reason given on
// Test_ConcurrentRequests_RelatedParent_ConcurrentChildUpdatesConverge.

// changeSeq reads the resource's Change Sequence stamp. The sequence survives
// TRUNCATE, so its values are only ever compared with each other.
func (t *TestSuite) changeSeq(resourceType, id string) int64 {
	var seq int64
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT change_seq FROM resources WHERE type=$1 AND id=$2`, resourceType, id).Scan(&seq)
	t.Require().NoError(err)
	return seq
}

// awaitGate waits for a provider gate to be reached, failing the test after a
// bounded wait instead of hanging.
func (t *TestSuite) awaitGate(reached <-chan struct{}, what string) {
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.FailNow("timed out waiting for " + what)
	}
}

// awaitWalk waits for a RebuildNow running in a goroutine and requires it to
// succeed.
func (t *TestSuite) awaitWalk(errCh <-chan error) {
	select {
	case err := <-errCh:
		t.Require().NoError(err)
	case <-time.After(30 * time.Second):
		t.FailNow("timed out waiting for the rebuild walk to return")
	}
}

// onlyHit returns the single document of the resource type.
func (t *TestSuite) onlyHit(resourceType string) core.SearchHit {
	resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: resourceType})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)
	return resp.Hits[0]
}

// relationF1 maps each denormalized relation of a hit to its f1 value, by id.
func relationF1(hit core.SearchHit, relation string) map[string]string {
	out := map[string]string{}
	for _, r := range sourceList(hit.Source, relation) {
		out[fieldStr(r, "id")] = fieldStr(r, "f1")
	}
	return out
}

// relatedGate is the notification/walk metadata that holds a build in its
// FetchRelated of a — after the call has snapshotted the relation data.
func relatedGate(token string) map[string]string {
	return map[string]string{"test_related_gate": token, "test_related_gate_resource": "a"}
}

// Test_DriftCheck_ChildVersionAboveObserved_BuildsParentOnce: a/1 is
// registered at version 10^15 and built. c/1 is then registered and built
// once, ungated; its FetchRelated(a) serves a/1 with observed version 1, far
// below the stored one. a/1's change_seq predates c/1's start, so the build
// re-schedules nothing. The old check (stored Notification version above the
// observed FetchRelated version) re-scheduled c/1 on every build forever;
// WaitForIdle is bounded so that loop would fail instead of hang.
func (t *TestSuite) Test_DriftCheck_ChildVersionAboveObserved_BuildsParentOnce() {
	t.setResourceConfig(RelatedResourceConfig)
	const bigVersion = int64(1_000_000_000_000_000)

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "av1"})
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: bigVersion,
	}))
	t.worker.Drain(t.T().Context())
	t.Require().Equal(bigVersion, t.resourceVersion("a", "1"))

	t.fakeProvider.SetResource("c", "1", map[string]any{"id": "1", "f1": "cv1"})
	t.fakeProvider.SetRelatedVersioned("a", []string{"1"}, []source.RelatedResource{
		{ID: "1", Data: map[string]any{"id": "1", "f1": "av1"}, Version: 1},
	})
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "c", ResourceID: "1", Kind: core.ChangeCreated, Version: 1,
	}))

	ctx, cancel := context.WithTimeout(t.T().Context(), 30*time.Second)
	defer cancel()
	t.Require().NoError(t.idx.WaitForIdle(ctx), "c/1 must settle, not re-schedule itself forever")

	t.Require().Equal(int64(1), t.resourceRebuildCounter("c", "1"), "one build, no drift re-schedule")
	t.Require().Nil(t.staleSince("c", "1"))
	t.Require().Equal(map[string]string{"1": "av1"}, relationF1(t.onlyHit("c"), "a"))
}

// Test_DriftCheck_ChildRegisteredDuringParentFirstBuild_ReschedulesOnce: c/1's
// first build takes its start at BeginBuild and is held in FetchRelated(a)
// after snapshotting a/1 at av1. c/1 has no edges yet, so a/1's registration
// (av2, accepted, change_seq above c/1's start) cannot fan out to it. Released,
// the build writes av1, finds a/1 above its start and re-schedules once; the
// second build fetches av2 and finds nothing newer. The relation is served
// unversioned (observed version 0), which the old version check skipped — no
// re-schedule, av1 would have stayed.
func (t *TestSuite) Test_DriftCheck_ChildRegisteredDuringParentFirstBuild_ReschedulesOnce() {
	t.setResourceConfig(RelatedResourceConfig)

	t.fakeProvider.SetResource("c", "1", map[string]any{"id": "1", "f1": "cv1"})
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "av1"})
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{{"id": "1", "f1": "av1"}})

	gate := t.fakeProvider.SetFetchGate("c1-related-a")
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "c", ResourceID: "1", Kind: core.ChangeCreated, Version: 1,
		Metadata: relatedGate("c1-related-a"),
	}))
	t.awaitGate(gate, "c/1's build to reach its FetchRelated(a) gate")
	t.Require().Equal(int64(1), t.resourceRebuildCounter("c", "1"), "c/1's build has begun")

	parents, err := t.st.GetParentResources(t.T().Context(), model.Resource{Type: "a", Id: "1"})
	t.Require().NoError(err)
	t.Require().Empty(parents, "no edge to c/1, so a/1's registration cannot fan out")

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "av2"})
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{{"id": "1", "f1": "av2"}})
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1,
	}))

	t.fakeProvider.ReleaseFetchGate("c1-related-a")
	t.worker.Drain(t.T().Context())

	t.Require().Equal(int64(2), t.resourceRebuildCounter("c", "1"), "first build plus exactly one drift re-schedule")
	t.Require().Nil(t.staleSince("c", "1"))
	t.Require().Equal(map[string]string{"1": "av2"}, relationF1(t.onlyHit("c"), "a"))
}

// Test_DriftCheck_StaleRejectedChildDuringParentBuild_DoesNotReschedule: a/2
// is registered at version 5 and built. c/2's first build is held in
// FetchRelated(a) after snapshotting a/2 at av5; while it is held, a/2's
// version-3 registration is rejected as stale, so a/2's change_seq does not
// move. Released, c/2's build finds no child above its start and is not
// re-scheduled: a rejected registration is not a change.
func (t *TestSuite) Test_DriftCheck_StaleRejectedChildDuringParentBuild_DoesNotReschedule() {
	t.setResourceConfig(RelatedResourceConfig)

	t.fakeProvider.SetResource("a", "2", map[string]any{"id": "2", "f1": "av5"})
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "2", Kind: core.ChangeCreated, Version: 5,
	}))
	t.worker.Drain(t.T().Context())
	seqBefore := t.changeSeq("a", "2")
	aBuildsBefore := t.resourceRebuildCounter("a", "2")

	t.fakeProvider.SetResource("c", "2", map[string]any{"id": "2", "f1": "cv1"})
	t.fakeProvider.SetRelated("a", []string{"2"}, []map[string]any{{"id": "2", "f1": "av5"}})

	gate := t.fakeProvider.SetFetchGate("c2-related-a")
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "c", ResourceID: "2", Kind: core.ChangeCreated, Version: 1,
		Metadata: relatedGate("c2-related-a"),
	}))
	t.awaitGate(gate, "c/2's build to reach its FetchRelated(a) gate")

	parents, err := t.st.GetParentResources(t.T().Context(), model.Resource{Type: "a", Id: "2"})
	t.Require().NoError(err)
	t.Require().Empty(parents)

	err = t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "2", Kind: core.ChangeUpdated, Version: 3,
	})
	t.Require().ErrorIs(err, core.ErrStaleVersion)
	t.Require().Equal(seqBefore, t.changeSeq("a", "2"), "a stale rejection must not stamp change_seq")
	t.Require().Equal(int64(5), t.resourceVersion("a", "2"))

	t.fakeProvider.ReleaseFetchGate("c2-related-a")
	t.worker.Drain(t.T().Context())

	t.Require().Equal(int64(1), t.resourceRebuildCounter("c", "2"), "no drift re-schedule")
	t.Require().Equal(aBuildsBefore, t.resourceRebuildCounter("a", "2"), "a stale registration builds nothing")
	t.Require().Nil(t.staleSince("c", "2"))
	t.Require().Equal(map[string]string{"2": "av5"}, relationF1(t.onlyHit("c"), "a"))
}

// Test_DriftCheck_RebuildWalk_ChildNewToRootAfterPageFetch_ReschedulesRoot: a
// walk over c takes its walk start, lists c/1's page and is held in
// FetchRelated(a) after snapshotting [a/1 av1, a/9 a9v1] — before c/1's
// BeginBuild in the walk. a/9 has no row and no edge, so its registration
// (a9v2, change_seq above the walk start) fans out to nothing. Released, the
// walk begins c/1, writes a9v1 and its flusher finds a/9 above the walk start:
// c/1 is re-scheduled once and that build fetches a9v2. The old version check
// skipped a/9's unversioned observation (version 0).
func (t *TestSuite) Test_DriftCheck_RebuildWalk_ChildNewToRootAfterPageFetch_ReschedulesRoot() {
	t.setResourceConfig(RelatedResourceConfig)

	t.fakeProvider.SetResource("c", "1", map[string]any{"id": "1", "f1": "cv1"})
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "av1"})
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{{"id": "1", "f1": "av1"}})
	for _, n := range []core.Notification{
		{ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
		{ResourceType: "c", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
	} {
		t.Require().NoError(t.idx.RegisterChange(t.T().Context(), n))
	}
	t.worker.Drain(t.T().Context())
	b0 := t.resourceRebuildCounter("c", "1")

	// a/9 joins c/1's relations at the source only: no row, no edge.
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "av1"},
		{"id": "9", "f1": "a9v1"},
	})

	gate := t.fakeProvider.SetFetchGate("walk-related-a")
	errCh := make(chan error, 1)
	go func() {
		errCh <- t.idx.RebuildNow(t.T().Context(), []core.ResourceSelector{
			{ResourceType: "c", Metadata: relatedGate("walk-related-a")},
		})
	}()
	t.awaitGate(gate, "the walk's FetchRelated(a) gate")
	t.Require().Equal(b0, t.resourceRebuildCounter("c", "1"), "the walk has not begun c/1 yet")

	parents, err := t.st.GetParentResources(t.T().Context(), model.Resource{Type: "a", Id: "9"})
	t.Require().NoError(err)
	t.Require().Empty(parents, "no edge to a/9, so its registration cannot fan out")

	t.fakeProvider.SetResource("a", "9", map[string]any{"id": "9", "f1": "a9v2"})
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "av1"},
		{"id": "9", "f1": "a9v2"},
	})
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "9", Kind: core.ChangeCreated, Version: 1,
	}))

	t.fakeProvider.ReleaseFetchGate("walk-related-a")
	t.awaitWalk(errCh)
	t.worker.Drain(t.T().Context())

	t.Require().Equal(b0+2, t.resourceRebuildCounter("c", "1"), "the walk's build plus exactly one drift re-schedule")
	t.Require().Nil(t.staleSince("c", "1"))
	t.Require().Equal(map[string]string{"1": "av1", "9": "a9v2"}, relationF1(t.onlyHit("c"), "a"))
}

// Test_DriftCheck_RebuildWalk_RootChangedAfterPageFetch_ReschedulesRoot: a
// walk over c takes its walk start, lists c/1's page (cv1) and is held in
// FetchRelated(a). c/1 then changes to cv2; the registration is accepted
// (change_seq above the walk start) and its inline build begins (B0+1) and is
// held at its FetchResource. Released, the walk begins c/1 (B0+2), writes the
// page's cv1, and the root check finds c/1 above the walk start: it marks c/1
// and an ungated build (B0+3) fetches cv2. Only after the walk returns is the
// inline build released; its B0+1 write loses ES OCC. Without the root check
// the walk's cv1 at B0+2 would win and its ClearStale would clear the
// registration's mark.
func (t *TestSuite) Test_DriftCheck_RebuildWalk_RootChangedAfterPageFetch_ReschedulesRoot() {
	t.setResourceConfig(RelatedResourceConfig)

	t.fakeProvider.SetResource("c", "1", map[string]any{"id": "1", "f1": "cv1"})
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "av1"})
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{{"id": "1", "f1": "av1"}})
	for _, n := range []core.Notification{
		{ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
		{ResourceType: "c", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
	} {
		t.Require().NoError(t.idx.RegisterChange(t.T().Context(), n))
	}
	t.worker.Drain(t.T().Context())
	b0 := t.resourceRebuildCounter("c", "1")

	walkGate := t.fakeProvider.SetFetchGate("walk-related-a")
	errCh := make(chan error, 1)
	go func() {
		errCh <- t.idx.RebuildNow(t.T().Context(), []core.ResourceSelector{
			{ResourceType: "c", Metadata: relatedGate("walk-related-a")},
		})
	}()
	t.awaitGate(walkGate, "the walk's FetchRelated(a) gate")
	t.Require().Equal(b0, t.resourceRebuildCounter("c", "1"), "the walk has not begun c/1 yet")

	t.fakeProvider.SetResource("c", "1", map[string]any{"id": "1", "f1": "cv2"})
	inlineGate := t.fakeProvider.SetFetchGate("c1-inline")
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "c", ResourceID: "1", Kind: core.ChangeUpdated, Version: 2,
		Metadata: map[string]string{"test_fetch_gate": "c1-inline", "test_fetch_gate_resource": "c"},
	}))
	t.awaitGate(inlineGate, "c/1's inline build to reach its FetchResource gate")
	t.Require().Equal(b0+1, t.resourceRebuildCounter("c", "1"), "the inline build has begun")

	t.fakeProvider.ReleaseFetchGate("walk-related-a")
	t.awaitWalk(errCh)

	t.fakeProvider.ReleaseFetchGate("c1-inline")
	t.worker.Drain(t.T().Context())

	t.Require().Equal(b0+3, t.resourceRebuildCounter("c", "1"), "inline, walk, and one root re-schedule")
	t.Require().Nil(t.staleSince("c", "1"))
	fields, _ := t.onlyHit("c").Source["fields"].(map[string]any)
	t.Require().Equal("cv2", fields["f1"])

	for query, want := range map[string]int{"cv1": 0, "cv2": 1} {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "c", Query: query})
		t.Require().NoError(err)
		t.Require().Lenf(resp.Hits, want, "query %q", query)
	}
}
