package tests

import (
	"context"
	"encoding/json"
	"time"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
)

// A resource's stored metadata is its own: only its own registrations write
// it, and while it has none its own plans' report (stored by the build's edge
// write); a mark another resource's change sets leaves it alone, and a build
// that owns the resource fetches with what the row holds when the build
// begins.
// The FakeProvider's test_override_field1 makes the metadata a fetch ran with
// visible in the indexed document's field1.

// rowMetadata reads the resource's stored metadata; nil when it has none.
func (t *TestSuite) rowMetadata(resourceType, id string) map[string]string {
	var raw []byte
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT metadata FROM resources WHERE type=$1 AND id=$2`, resourceType, id).Scan(&raw)
	t.Require().NoError(err)
	if len(raw) == 0 {
		return nil
	}
	var md map[string]string
	t.Require().NoError(json.Unmarshal(raw, &md))
	return md
}

// Test_Metadata_ChildMarkDuringOwnerBuild_FollowUpBuildsParentsOwn: a/1 (R)
// with child b/x (C) is built once, so the edge a/1 -> b/x exists. The
// interleaving, forced through the FakeProvider's fetch gate and a second
// indexer:
//
//  1. a/1 is registered with md0 (v0, carrying the gate): the registration
//     claims a/1, and its owned build is held in FetchResource(a), after
//     BeginBuild.
//  2. a/1 is registered with md1 (v1): it commits unclaimed, as the held
//     build owns a/1.
//  3. b/x is registered with its own metadata (vC) on instance B, which
//     builds b/x and settles before the release. The registration marks
//     Parent a/1 through the edge, b/x's own build derives a/1 as a Parent
//     and marks it again (ADR 0006), and b/x's change_seq, above the held build's start, makes
//     that build's drift check re-mark a/1.
//  4. Released, the held build writes v0 and its finish hands on one
//     follow-up, which owns a/1 and fetches with what the row holds: md1.
//
// Before, b/x's Parent mark stored vC on a/1's row (last mark wins) and the
// follow-up fetched a/1 as vC: another resource's metadata.
func (t *TestSuite) Test_Metadata_ChildMarkDuringOwnerBuild_FollowUpBuildsParentsOwn() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "src"})
	bx := map[string]any{"id": "x", "a_id": "1", "field1": "bx"}
	t.fakeProvider.SetResource("b", "x", bx)
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{bx})
	for _, n := range []core.Notification{
		{ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
		{ResourceType: "b", ResourceID: "x", Kind: core.ChangeCreated, Version: 1},
	} {
		t.Require().NoError(t.idx.RegisterChange(ctx, n))
	}
	t.worker.Drain(ctx)

	parents, err := t.st.GetParentResources(ctx, model.Resource{Type: "b", Id: "x"})
	t.Require().NoError(err)
	t.Require().Contains(parents, model.Resource{Type: "a", Id: "1"}, "the edge a/1 -> b/x must exist")

	instB := t.newIndexer(DefaultResourceConfig, core.Config{})
	buildsBefore := t.resourceRebuildCounter("a", "1")

	gate := t.fakeProvider.SetFetchGate("r-held")
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeUpdated, Version: 2,
		Metadata: map[string]string{
			"test_fetch_gate":          "r-held",
			"test_fetch_gate_resource": "a",
			"test_override_field1":     "v0",
		},
	}))
	t.awaitGate(gate, "a/1's owned build to reach its FetchResource(a) gate")
	t.Require().Equal(buildsBefore+1, t.resourceRebuildCounter("a", "1"), "a/1's owned build has begun")
	ownerSeq, _ := t.ownerColumns("a", "1")
	t.Require().NotNil(ownerSeq, "the held build owns a/1")

	md1 := map[string]string{"test_override_field1": "v1"}
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeUpdated, Version: 3, Metadata: md1,
	}))
	afterMd1, _ := t.ownerColumns("a", "1")
	t.Require().Equal(ownerSeq, afterMd1, "md1's registration commits unclaimed: the held build still owns a/1")
	t.Require().Equal(md1, t.rowMetadata("a", "1"), "a/1's own registration stores md1")
	afterR := t.staleSeq("a", "1")

	t.Require().NoError(instB.RegisterChange(ctx, core.Notification{
		ResourceType: "b", ResourceID: "x", Kind: core.ChangeUpdated, Version: 2,
		Metadata: map[string]string{"test_override_field1": "vC"},
	}))
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	t.Require().NoError(instB.WaitForIdle(waitCtx))
	t.Require().Greater(t.staleSeq("a", "1"), afterR, "b/x's change marked a/1")
	afterC, _ := t.ownerColumns("a", "1")
	t.Require().Equal(ownerSeq, afterC, "b/x's change did not claim a/1: the held build still owns it")
	t.Require().Equal(md1, t.rowMetadata("a", "1"), "b/x's marks leave a/1's metadata alone")

	t.fakeProvider.ReleaseFetchGate("r-held")
	t.worker.Drain(ctx)
	t.Require().NoError(instB.WaitForIdle(waitCtx))

	t.Require().Equal(buildsBefore+2, t.resourceRebuildCounter("a", "1"), "the held build plus exactly one follow-up")
	t.Require().Nil(t.staleSince("a", "1"))
	t.Require().Equal(md1, t.rowMetadata("a", "1"))
	fields, _ := t.onlyHit("a").Source["fields"].(map[string]any)
	t.Require().Equal("v1", fields["field1"], "the follow-up fetched a/1 with its own md1")
}
