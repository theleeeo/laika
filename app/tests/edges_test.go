package tests

import (
	"encoding/json"
	"slices"
	"time"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/model"
)

// Edges follow the Build Sequence per Schema Version (ADR 0002): each
// version's stored edges come from the build whose document that version's
// index holds — the highest Build Sequence among the builds that ran its plan
// — and no build path leaves a resource without edges while it builds. So a
// change to a child only the newer build found still fans out to the Parent.
// These cases pin the interleavings that break that; every race among them is
// forced through provider gates, never by timing. A second concurrent build
// of one resource is a direct idx.Build, which owns nothing (ADR 0008), so
// ownership does not serialize it.
//
// Children have no source data at all (no SetResource): ADR 0006 would make a
// child's own build schedule the Parent from the child's data, and that path
// would reach the Parent without the edges under test. A change registered to
// such a child builds nothing but its own delete.

// EdgesVersionedConfig is a two-version resource "p" whose v1 relates to "b"
// and whose v2 relates to "c" (both joined on p's id through p_id), so each
// Schema Version discovers a different edge set. ReadVersion stays at 1.
var EdgesVersionedConfig = resource.Configs{
	{
		Resource: "p",
		Versions: []resource.VersionConfig{
			{
				Version: 1,
				Fields:  []resource.FieldConfig{{Name: "f1", Query: primaryTier}},
				Relations: []resource.RelationConfig{{
					Resource: "b",
					Join:     resource.JoinConfig{Local: "id", Foreign: "p_id"},
					Fields:   []resource.FieldConfig{{Name: "f1", Query: primaryTier}},
				}},
			},
			{
				Version: 2,
				Fields:  []resource.FieldConfig{{Name: "f1", Query: primaryTier}},
				Relations: []resource.RelationConfig{{
					Resource: "c",
					Join:     resource.JoinConfig{Local: "id", Foreign: "p_id"},
					Fields:   []resource.FieldConfig{{Name: "f1", Query: primaryTier}},
				}},
			},
		},
		ReadVersion: 1,
	},
	{
		Resource: "b",
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields:  []resource.FieldConfig{{Name: "f1", Query: primaryTier}},
		}},
	},
	{
		Resource: "c",
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields:  []resource.FieldConfig{{Name: "f1", Query: primaryTier}},
		}},
	},
}

// edgeKeys renders resources as sorted "type/id" strings, never nil, so edge
// sets compare with Equal regardless of the order they were read in.
func edgeKeys(rs []model.Resource) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Type+"/"+r.Id)
	}
	slices.Sort(out)
	return out
}

// childEdges reads the resource's stored edges, every Schema Version's, as
// sorted "type/id" strings.
func (t *TestSuite) childEdges(resourceType, id string) []string {
	children, err := t.st.GetChildResources(t.T().Context(), model.Resource{Type: resourceType, Id: id})
	t.Require().NoError(err)
	return edgeKeys(children)
}

// versionEdges reads the resource's stored edges of one Schema Version as
// sorted "type/id" strings.
func (t *TestSuite) versionEdges(resourceType, id string, version int) []string {
	rows, err := t.pool.Query(t.T().Context(),
		`SELECT related_resource, related_resource_id FROM relations
		 WHERE resource=$1 AND resource_id=$2 AND schema_version=$3`,
		resourceType, id, version)
	t.Require().NoError(err)
	defer rows.Close()

	var children []model.Resource
	for rows.Next() {
		var r model.Resource
		t.Require().NoError(rows.Scan(&r.Type, &r.Id))
		children = append(children, r)
	}
	t.Require().NoError(rows.Err())
	return edgeKeys(children)
}

// docRelationIDs reads a document from a concrete version index and returns
// the children denormalized under relation as sorted "relation/id" strings.
// The document must exist.
func (t *TestSuite) docRelationIDs(index, id, relation string) []string {
	res, err := t.esClient.Get(index, id)
	t.Require().NoError(err)
	defer res.Body.Close()
	t.Require().False(res.IsError(), "get %s/%s: %s", index, id, res.Status())

	var body struct {
		Source map[string]any `json:"_source"`
	}
	t.Require().NoError(json.NewDecoder(res.Body).Decode(&body))

	var children []model.Resource
	for _, r := range sourceList(body.Source, relation) {
		children = append(children, model.Resource{Type: relation, Id: fieldStr(r, "id")})
	}
	return edgeKeys(children)
}

// awaitBuild waits for a direct idx.Build running in a goroutine and requires
// it to succeed.
func (t *TestSuite) awaitBuild(errCh <-chan error, what string) {
	select {
	case err := <-errCh:
		t.Require().NoError(err, what)
	case <-time.After(30 * time.Second):
		t.FailNow("timed out waiting for " + what + " to return")
	}
}

// Test_Edges_OlderBuildLoses_EdgesFollowNewerBuild: two direct builds of P =
// a/1 race. B1 begins (Build Sequence s1), fetches a/1 and is held in its
// FetchRelated(b) after snapshotting {b/d}. The source moves a/1's children to
// {b/c}. B2 begins (s2 > s1), fetches a/1 and {b/c}, writes its document at
// s2 and its edges {b/c}, and returns. Released, B1 writes its document at s1
// — losing ES OCC to B2's — and today wipes P's edges and writes {b/d}: the
// document is B2's but the edges are B1's. A change registered to b/c (no
// source data, so ADR 0006 cannot reach P) must then still mark P.
func (t *TestSuite) Test_Edges_OlderBuildLoses_EdgesFollowNewerBuild() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "av1"})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{{"id": "d", "field1": "bd"}})
	b0 := t.resourceRebuildCounter("a", "1")

	gate := t.fakeProvider.SetFetchGate("b1-related-b")
	defer t.fakeProvider.ReleaseFetchGate("b1-related-b")
	b1Err := make(chan error, 1)
	go func() {
		b1Err <- t.idx.Build(ctx, core.BuildArgs{
			ResourceType: "a",
			ResourceIds:  []string{"1"},
			Metadata:     map[string]string{"test_related_gate": "b1-related-b", "test_related_gate_resource": "b"},
		})
	}()
	t.awaitGate(gate, "B1 to reach its FetchRelated(b) gate")
	t.Require().Equal(b0+1, t.resourceRebuildCounter("a", "1"), "B1 has begun")

	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{{"id": "c", "field1": "bc"}})
	t.Require().NoError(t.idx.Build(ctx, core.BuildArgs{ResourceType: "a", ResourceIds: []string{"1"}}))
	t.Require().Equal(b0+2, t.resourceRebuildCounter("a", "1"), "B2 began after B1")

	t.fakeProvider.ReleaseFetchGate("b1-related-b")
	t.awaitBuild(b1Err, "B1")

	t.Require().Equal([]string{"b/c"}, t.docRelationIDs(core.IndexName("a", 1), "1", "b"),
		"B2's document wins: B1's write at the lower Build Sequence loses ES OCC")
	t.Equal([]string{"b/c"}, t.childEdges("a", "1"),
		"P's edges must be those of B2, whose document the index holds")

	before := t.staleSeq("a", "1")
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
		ResourceType: "b", ResourceID: "c", Kind: core.ChangeUpdated,
	}))
	t.Greater(t.staleSeq("a", "1"), before, "a change to b/c, found only by B2, must fan out to P")

	t.worker.Drain(ctx)
}

// Test_Edges_LiveBuildVsTargetedRebuild_PerVersionEdgesFollowDocument: a live
// build L of p (runs both versions' plans, v1 before v2) races a v2-targeted
// explicit-id rebuild R that begins after it (sR > sL). L is held in its v2
// plan's FetchRelated(c) after snapshotting {c/old} — its v1 plan has already
// fetched {b/x}. The source then moves p's c-children to {c/new}. Afterwards,
// each version's stored edges must equal the children of the document that
// version's index holds: v1 is written only by L, v2 is won by R.
func (t *TestSuite) Test_Edges_LiveBuildVsTargetedRebuild_PerVersionEdgesFollowDocument() {
	for _, c := range EdgesVersionedConfig {
		c.ApplyDefaults()
	}
	t.Require().NoError(EdgesVersionedConfig.Validate())
	t.setResourceConfig(EdgesVersionedConfig)

	liveGate := func(token string) map[string]string {
		return map[string]string{"test_related_gate": token, "test_related_gate_resource": "c"}
	}

	// seed gives p/<id> its data, v1 child b/x<id> and v2 child c/old<id>.
	seed := func(id string) {
		t.fakeProvider.SetResource("p", id, map[string]any{"id": id, "f1": "pv" + id})
		t.fakeProvider.SetRelated("b", []string{id}, []map[string]any{{"id": "x" + id, "f1": "bx"}})
		t.fakeProvider.SetRelated("c", []string{id}, []map[string]any{{"id": "old" + id, "f1": "cold"}})
	}

	// startLive starts L as a direct Build of p/<id> held at token's
	// FetchRelated(c) gate, and returns its result channel once it is held.
	startLive := func(id, token string) <-chan error {
		gate := t.fakeProvider.SetFetchGate(token)
		b0 := t.resourceRebuildCounter("p", id)
		errCh := make(chan error, 1)
		go func() {
			errCh <- t.idx.Build(t.T().Context(), core.BuildArgs{
				ResourceType: "p", ResourceIds: []string{id}, Metadata: liveGate(token),
			})
		}()
		t.awaitGate(gate, "L to reach its v2 FetchRelated(c) gate")
		t.Require().Equal(b0+1, t.resourceRebuildCounter("p", id), "L has begun")
		return errCh
	}

	// assertEdgesFollowDocuments checks the forced outcome (v1 holds L's b
	// child, v2 holds R's c/new) and then that each version's stored edges are
	// that version's document's children.
	assertEdgesFollowDocuments := func(id string) {
		v1Doc := t.docRelationIDs(core.IndexName("p", 1), id, "b")
		v2Doc := t.docRelationIDs(core.IndexName("p", 2), id, "c")
		t.Require().Equal([]string{"b/x" + id}, v1Doc, "v1 holds L's document, its only writer")
		t.Require().Equal([]string{"c/new" + id}, v2Doc, "v2 holds R's document, the higher Build Sequence")

		t.Equal(v1Doc, t.versionEdges("p", id, 1), "v1's edges must be its document's children")
		t.Equal(v2Doc, t.versionEdges("p", id, 2), "v2's edges must be its document's children")
	}

	// (a) R finishes first: L begins (sL), snapshots {c/old}, held. R begins
	// (sR > sL) ungated, snapshots {c/new}, writes its v2 document and edges
	// and returns. Released, L writes v1 (wins, sole writer) and v2 (loses
	// ES OCC to R), then writes its edges.
	t.Run("rebuild finishes first", func() {
		const id = "1"
		seed(id)
		defer t.fakeProvider.ReleaseFetchGate("live-a")
		lErr := startLive(id, "live-a")

		t.fakeProvider.SetRelated("c", []string{id}, []map[string]any{{"id": "new" + id, "f1": "cnew"}})
		b0 := t.resourceRebuildCounter("p", id)
		t.Require().NoError(t.idx.RebuildNow(t.T().Context(), []core.ResourceSelector{
			{ResourceType: "p", Versions: []int{2}, ResourceIDs: []string{id}},
		}))
		t.Require().Equal(b0+1, t.resourceRebuildCounter("p", id), "R began after L")

		t.fakeProvider.ReleaseFetchGate("live-a")
		t.awaitBuild(lErr, "L")
		t.worker.Drain(t.T().Context())

		assertEdgesFollowDocuments(id)
	})

	// (b) L finishes first: L begins (sL), snapshots {c/old}, held. R begins
	// (sR > sL), snapshots {c/new} and is held in its FetchRelated(c).
	// Released, L writes v1 and v2 (each the first write at its index) and
	// its edges, and returns. Released, R writes v2 at sR, winning over L's,
	// and its edges.
	t.Run("live build finishes first", func() {
		const id = "2"
		seed(id)
		defer t.fakeProvider.ReleaseFetchGate("live-b")
		defer t.fakeProvider.ReleaseFetchGate("rebuild-b")
		lErr := startLive(id, "live-b")

		t.fakeProvider.SetRelated("c", []string{id}, []map[string]any{{"id": "new" + id, "f1": "cnew"}})
		b0 := t.resourceRebuildCounter("p", id)
		rGate := t.fakeProvider.SetFetchGate("rebuild-b")
		rErr := make(chan error, 1)
		go func() {
			rErr <- t.idx.RebuildNow(t.T().Context(), []core.ResourceSelector{
				{ResourceType: "p", Versions: []int{2}, ResourceIDs: []string{id}, Metadata: liveGate("rebuild-b")},
			})
		}()
		t.awaitGate(rGate, "R to reach its FetchRelated(c) gate")
		t.Require().Equal(b0+1, t.resourceRebuildCounter("p", id), "R began after L")

		t.fakeProvider.ReleaseFetchGate("live-b")
		t.awaitBuild(lErr, "L")

		t.fakeProvider.ReleaseFetchGate("rebuild-b")
		t.awaitWalk(rErr)
		t.worker.Drain(t.T().Context())

		assertEdgesFollowDocuments(id)
	})
}

// Test_Edges_ExplicitIDFullRebuild_KeepsEdgesWhileFetching: P = a/1 is built
// with child b/c, so its edges are {b/c}. An explicit-id full rebuild of a/1
// (no Versions) is held at its FetchResource of a/1 — today it has already
// wiped P's edges before BeginBuild. While it is held, a change registered to
// b/c (no source data, so ADR 0006 cannot reach P) must mark P through the
// edge P already had.
func (t *TestSuite) Test_Edges_ExplicitIDFullRebuild_KeepsEdgesWhileFetching() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "av1"})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{{"id": "c", "field1": "bc"}})
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated,
	}))
	t.worker.Drain(ctx)
	t.Require().Equal([]string{"b/c"}, t.childEdges("a", "1"))

	gate := t.fakeProvider.SetFetchGate("rebuild-a")
	defer t.fakeProvider.ReleaseFetchGate("rebuild-a")
	errCh := make(chan error, 1)
	go func() {
		errCh <- t.idx.RebuildNow(ctx, []core.ResourceSelector{{
			ResourceType: "a",
			ResourceIDs:  []string{"1"},
			Metadata:     map[string]string{"test_fetch_gate": "rebuild-a", "test_fetch_gate_resource": "a"},
		}})
	}()
	t.awaitGate(gate, "the rebuild to reach its FetchResource(a) gate")

	before := t.staleSeq("a", "1")
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
		ResourceType: "b", ResourceID: "c", Kind: core.ChangeUpdated,
	}))
	t.Greater(t.staleSeq("a", "1"), before, "a change to P's existing child must fan out while P rebuilds")

	t.fakeProvider.ReleaseFetchGate("rebuild-a")
	t.awaitWalk(errCh)
	t.worker.Drain(ctx)
}
