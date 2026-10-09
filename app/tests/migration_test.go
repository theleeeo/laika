package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/app/dsl"
	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// MigrationResourceConfig is the ADR 0004 shape mid-migration: resource "m"
// with the serving v1 and a freshly added v2 that projects an extra field.
// ReadVersion stays at 1 — the read alias serves v1 while v2 backfills.
var MigrationResourceConfig = resource.Configs{
	{
		Resource: "m",
		Versions: []resource.VersionConfig{
			{Version: 1, Fields: []resource.FieldConfig{
				{Name: "field1", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}},
			}},
			{Version: 2, Fields: []resource.FieldConfig{
				{Name: "field1", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}},
				{Name: "field2"},
			}},
		},
		ReadVersion: 1,
	},
}

// docFields fetches a document's projected fields directly from a concrete
// version index, bypassing the read alias.
func (t *TestSuite) docFields(index, id string) (map[string]any, bool) {
	res, err := t.esClient.Get(index, id)
	t.Require().NoError(err)
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil, false
	}
	t.Require().False(res.IsError(), "get %s/%s: %s", index, id, res.Status())

	var body struct {
		Source map[string]any `json:"_source"`
	}
	t.Require().NoError(json.NewDecoder(res.Body).Decode(&body))
	fields, _ := body.Source["fields"].(map[string]any)
	return fields, true
}

// Test_Migration_MultiVersionLifecycle walks the ADR 0004 rolling-migration
// lifecycle against real infrastructure: live builds fan out to every Schema
// Version, a targeted rebuild backfills only the new version's index, a full
// rebuild resets every version, the read alias converges onto the config's
// readVersion in both directions (ADR 0009), and a delete clears every
// version.
func (t *TestSuite) Test_Migration_MultiVersionLifecycle() {
	for _, c := range MigrationResourceConfig {
		c.ApplyDefaults()
	}
	t.Require().NoError(MigrationResourceConfig.Validate())
	t.setResourceConfig(MigrationResourceConfig)

	const v1Index, v2Index = "m_search_v1", "m_search_v2"
	ids := []string{"1", "2", "3"}

	// --- Live ingest writes every active Schema Version (ADR 0004). ---
	for _, id := range ids {
		t.fakeProvider.SetResource("m", id, map[string]any{
			"id": id, "field1": "orig" + id, "field2": "extra" + id,
		})
		t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "m", ResourceID: id, Kind: core.ChangeCreated,
		}))
	}
	t.worker.Drain(t.T().Context())

	for _, id := range ids {
		f1, ok := t.docFields(v1Index, id)
		t.Require().True(ok, "v1 must hold %s after a live build", id)
		t.Require().Equal("orig"+id, f1["field1"])
		t.Require().NotContains(f1, "field2", "v1's projection must not leak v2 fields")

		f2, ok := t.docFields(v2Index, id)
		t.Require().True(ok, "v2 must hold %s after a live build — every build writes every active Schema Version", id)
		t.Require().Equal("extra"+id, f2["field2"])
	}

	// --- A targeted rebuild backfills only the selected version's index. ---
	for _, id := range ids {
		t.fakeProvider.SetResource("m", id, map[string]any{
			"id": id, "field1": "new" + id, "field2": "fresh" + id,
		})
	}
	t.Require().NoError(t.idx.RebuildNow(t.T().Context(), []core.ResourceSelector{
		{ResourceType: "m", Versions: []int{2}},
	}))
	t.worker.Drain(t.T().Context())

	for _, id := range ids {
		f2, ok := t.docFields(v2Index, id)
		t.Require().True(ok)
		t.Require().Equal("fresh"+id, f2["field2"], "targeted rebuild must refresh v2")

		f1, ok := t.docFields(v1Index, id)
		t.Require().True(ok)
		t.Require().Equal("orig"+id, f1["field1"], "targeted rebuild must not rewrite the serving v1 index")
	}

	// --- A full rebuild resets every version. ---
	t.Require().NoError(t.idx.RebuildNow(t.T().Context(), []core.ResourceSelector{
		{ResourceType: "m"},
	}))
	t.worker.Drain(t.T().Context())

	for _, id := range ids {
		f1, ok := t.docFields(v1Index, id)
		t.Require().True(ok)
		t.Require().Equal("new"+id, f1["field1"], "full rebuild must reset v1 too")
	}

	// --- The read alias still serves ReadVersion 1. ---
	resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "m", Query: "new1"})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)
	t.Require().Equal("1", resp.Hits[0].ID)

	// --- Cutover is a readVersion change: the alias converges onto the config
	// (ADR 0009), forward, idempotently, and back again on a rollback. ---
	ctx := t.T().Context()
	esBackend := elasticsearch.New(t.esClient, true)
	aliasTarget := func() string {
		target, err := esBackend.GetAlias(ctx, core.AliasName("m"))
		t.Require().NoError(err)
		return target
	}

	cut := *MigrationResourceConfig[0]
	cut.ReadVersion = 2
	cutCfg := resource.Configs{&cut}

	t.Require().NoError(core.ConvergeReadAliases(ctx, esBackend, cutCfg))
	t.Require().Equal(v2Index, aliasTarget(), "forward convergence must move the alias to the new readVersion")

	t.Require().NoError(core.ConvergeReadAliases(ctx, esBackend, cutCfg))
	t.Require().Equal(v2Index, aliasTarget(), "re-running convergence must be a no-op")

	t.Require().NoError(core.ConvergeReadAliases(ctx, esBackend, MigrationResourceConfig))
	t.Require().Equal(v1Index, aliasTarget(), "a readVersion rollback must converge the alias back")

	// A missing alias is recreated from config — the bootstrap-completing case.
	delRes, err := t.esClient.Indices.DeleteAlias([]string{v1Index}, []string{core.AliasName("m")})
	t.Require().NoError(err)
	delRes.Body.Close()
	t.Require().NoError(core.ConvergeReadAliases(ctx, esBackend, MigrationResourceConfig))
	t.Require().Equal(v1Index, aliasTarget(), "convergence must recreate a missing alias")

	// A readVersion whose index was never bootstrapped must fail loudly and
	// leave the alias where it was.
	broken := *MigrationResourceConfig[0]
	broken.ReadVersion = 3
	t.Require().Error(core.ConvergeReadAliases(ctx, esBackend, resource.Configs{&broken}))
	t.Require().Equal(v1Index, aliasTarget(), "a failed convergence must not move the alias")

	// --- A delete clears every version's document. ---
	t.fakeProvider.DeleteResource("m", "1")
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "m", ResourceID: "1", Kind: core.ChangeDeleted,
	}))
	t.worker.Drain(t.T().Context())

	if _, ok := t.docFields(v1Index, "1"); ok {
		t.T().Fatal("delete must remove the v1 document")
	}
	if _, ok := t.docFields(v2Index, "1"); ok {
		t.T().Fatal("delete must remove the v2 document")
	}
	t.Require().False(t.resourceTracked("m", "1"), "the tombstone must be hard-deleted after ES cleanup")
}

// Test_Migration_CutoverReadinessGates walks the pre-cutover readiness check
// through the migration it is meant to gate: a proposed readVersion bump is
// refused while the target index is missing, refused while the backfill has
// not covered the type, refused while a stale backlog lingers, and admitted
// once all three hold — then reports in-sync after the cutover.
func (t *TestSuite) Test_Migration_CutoverReadinessGates() {
	for _, c := range MigrationResourceConfig {
		c.ApplyDefaults()
	}
	t.Require().NoError(MigrationResourceConfig.Validate())

	ctx := t.T().Context()
	esBackend := elasticsearch.New(t.esClient, true)

	// Pre-migration world: only v1 is configured, bootstrapped, and serving.
	v1Only := resource.Configs{{
		Resource:    "m",
		Versions:    []resource.VersionConfig{MigrationResourceConfig[0].Versions[0]},
		ReadVersion: 1,
	}}
	for _, c := range v1Only {
		c.ApplyDefaults()
	}
	t.Require().NoError(v1Only.Validate())
	t.setResourceConfig(v1Only)

	for _, id := range []string{"1", "2", "3"} {
		t.fakeProvider.SetResource("m", id, map[string]any{
			"id": id, "field1": "orig" + id, "field2": "extra" + id,
		})
		t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
			ResourceType: "m", ResourceID: id, Kind: core.ChangeCreated,
		}))
	}
	t.worker.Drain(ctx)

	// The proposed change: v2 added and readVersion bumped to 2.
	proposed := *MigrationResourceConfig[0]
	proposed.ReadVersion = 2
	proposedCfg := resource.Configs{&proposed}
	t.Require().NoError(proposedCfg.Validate())

	check := func() core.ResourceReadiness {
		results := core.CheckCutoverReadiness(ctx, esBackend, t.st, proposedCfg, core.ReadinessOptions{})
		t.Require().Len(results, 1)
		return results[0]
	}
	gate := func(r core.ResourceReadiness, name string) core.ReadinessCheck {
		for _, c := range r.Checks {
			if c.Name == name {
				return c
			}
		}
		t.T().Fatalf("readiness has no %q check: %+v", name, r.Checks)
		return core.ReadinessCheck{}
	}

	// v2 was never bootstrapped: the target-index gate refuses the cutover.
	r := check()
	t.Require().False(r.Ready)
	t.Require().Equal(core.AliasForward, r.Move)
	t.Require().False(gate(r, core.CheckTargetIndex).OK)

	// gen-mapping equivalent: create the v2 index and start fanning writes to
	// it (readVersion still 1, so the alias stays on v1).
	t.setResourceConfig(MigrationResourceConfig)

	// v2 exists but no pre-migration resource has its edge set: coverage
	// refuses until the backfill has run, and v2 holds none of the documents.
	r = check()
	t.Require().False(r.Ready)
	coverage := gate(r, core.CheckCoverage)
	t.Require().False(coverage.OK)
	t.Require().Contains(coverage.Detail, `3 of 3 "m" resources`)
	docGap := gate(r, core.CheckDocGap)
	t.Require().False(docGap.OK)
	t.Require().Contains(docGap.Detail, "m_search_v1 holds 3, m_search_v2 holds 0", "the operator must see the doc counts")

	// Backfill v2 (ADR 0004 step 2).
	t.Require().NoError(t.idx.RebuildNow(ctx, []core.ResourceSelector{
		{ResourceType: "m", Versions: []int{2}},
	}))
	t.worker.Drain(ctx)

	// Coverage and the gap hold now, but an aged stale mark still blocks the cutover.
	_, err := t.st.MarkStale(ctx, []model.Resource{{Type: "m", Id: "2"}}, 0)
	t.Require().NoError(err)
	_, err = t.pool.Exec(ctx,
		`UPDATE resources SET stale_since = now() - interval '1 hour' WHERE type='m' AND id='2'`)
	t.Require().NoError(err)

	r = check()
	t.Require().False(r.Ready)
	t.Require().False(gate(r, core.CheckStaleBacklog).OK)

	_, err = t.pool.Exec(ctx,
		`UPDATE resources SET stale_since = NULL WHERE type='m' AND id='2'`)
	t.Require().NoError(err)

	// All gates hold: the readVersion bump may deploy.
	r = check()
	t.Require().True(r.Ready, "%+v", r.Checks)

	// Deploying the bumped config converges the alias (ADR 0009); the same
	// check then doubles as the post-cutover soak verdict.
	t.Require().NoError(core.ConvergeReadAliases(ctx, esBackend, proposedCfg))
	r = check()
	t.Require().True(r.Ready, "%+v", r.Checks)
	t.Require().Equal(core.AliasInSync, r.Move)
}

// Test_Migration_CutoverAcceptsAVersionsGap: v2's plan returns nil for 2 and
// 4, so v2 legitimately holds fewer documents than v1 (ADR 0013). Before the
// backfill, coverage refuses the cutover even with the gap accepted. The
// backfill marks 2 and 4 for their nils, and the sweep's build, which runs
// every plan, settles them with an empty v2 set (seams S40). Coverage then
// passes, the gap of -2 fails until the operator accepts it for "m", and the
// accepted check is ready.
func (t *TestSuite) Test_Migration_CutoverAcceptsAVersionsGap() {
	for _, c := range MigrationResourceConfig {
		c.ApplyDefaults()
	}
	t.Require().NoError(MigrationResourceConfig.Validate())

	ctx := t.T().Context()
	esBackend := elasticsearch.New(t.esClient, true)
	ids := []string{"1", "2", "3", "4", "5"}
	excluded := map[string]bool{"2": true, "4": true}

	// Pre-migration world: only v1 is configured and serving.
	v1Only := resource.Configs{{
		Resource:    "m",
		Versions:    []resource.VersionConfig{MigrationResourceConfig[0].Versions[0]},
		ReadVersion: 1,
	}}
	for _, c := range v1Only {
		c.ApplyDefaults()
	}
	t.Require().NoError(v1Only.Validate())
	t.setResourceConfig(v1Only)
	for _, id := range ids {
		t.fakeProvider.SetResource("m", id, map[string]any{
			"id": id, "field1": "orig" + id, "field2": "extra" + id,
		})
		t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
			ResourceType: "m", ResourceID: id, Kind: core.ChangeCreated,
		}))
	}
	t.worker.Drain(ctx)

	// v2 added, its plan excluding 2 and 4 by id.
	t.setResourceConfig(MigrationResourceConfig)
	plans := dsl.BuildPlansFromConfig(t.fakeProvider, MigrationResourceConfig)
	for i, p := range plans["m"] {
		if p.Version == 2 {
			plans["m"][i].Executer = nilForIDs{inner: p.Executer, ids: excluded}
		}
	}
	x := t.newIndexer(MigrationResourceConfig, core.Config{Plans: plans})

	proposed := *MigrationResourceConfig[0]
	proposed.ReadVersion = 2
	proposedCfg := resource.Configs{&proposed}
	t.Require().NoError(proposedCfg.Validate())
	check := func(opts core.ReadinessOptions) core.ResourceReadiness {
		results := core.CheckCutoverReadiness(ctx, esBackend, t.st, proposedCfg, opts)
		t.Require().Len(results, 1)
		return results[0]
	}
	gate := func(r core.ResourceReadiness, name string) core.ReadinessCheck {
		for _, c := range r.Checks {
			if c.Name == name {
				return c
			}
		}
		t.T().Fatalf("readiness has no %q check: %+v", name, r.Checks)
		return core.ReadinessCheck{}
	}
	accepted := core.ReadinessOptions{AcceptGap: map[string]bool{"m": true}}

	// Before the backfill no resource has v2's answer: accepting the gap
	// doesn't pass coverage.
	r := check(accepted)
	t.Require().False(r.Ready)
	t.Require().False(gate(r, core.CheckCoverage).OK, "an accepted gap must not pass coverage")
	t.Require().Contains(gate(r, core.CheckCoverage).Detail, `5 of 5 "m" resources`)

	// Backfill v2, then the sweep builds what the backfill marked for its nils.
	t.Require().NoError(x.RebuildNow(ctx, []core.ResourceSelector{
		{ResourceType: "m", Versions: []int{2}},
	}))
	t.Require().NoError(x.WaitForIdle(ctx))
	for id := range excluded {
		t.Require().NotNil(t.staleSince("m", id), "the backfill must mark %s for its nil", id)
	}
	swept, err := x.SweepStale(ctx, 0, 100)
	t.Require().NoError(err)
	t.Require().Equal(len(excluded), swept)
	for _, id := range ids {
		t.Require().Nil(t.staleSince("m", id), "the sweep must settle %s", id)
		_, ok := t.docFields(core.IndexName("m", 2), id)
		t.Require().Equal(!excluded[id], ok, "v2 holds %s only if its plan returns it", id)
	}

	// Covered, but v2 holds 3 documents to v1's 5: the gap fails until accepted.
	r = check(core.ReadinessOptions{})
	t.Require().False(r.Ready)
	coverage := gate(r, core.CheckCoverage)
	t.Require().True(coverage.OK, coverage.Detail)
	docGap := gate(r, core.CheckDocGap)
	t.Require().False(docGap.OK)
	t.Require().Contains(docGap.Detail, `m_search_v1 holds 5, m_search_v2 holds 3, of 5 "m" resources (gap -2)`)
	t.Require().Contains(docGap.Detail, "-accept-gap m")

	r = check(accepted)
	t.Require().True(r.Ready, "%+v", r.Checks)
	t.Require().Contains(gate(r, core.CheckDocGap).Detail, "(accepted)")
}

// Test_Migration_ResumedRebuildWalk aborts a version-targeted v2 backfill at
// its first mid-walk checkpoint and resumes from that cursor: pages before
// the cursor keep the aborted attempt's settled output and are not re-walked,
// the rest rebuilds from current source data, v1 is never touched, and no
// stale backlog is left behind (ADR 0011).
func (t *TestSuite) Test_Migration_ResumedRebuildWalk() {
	for _, c := range MigrationResourceConfig {
		c.ApplyDefaults()
	}
	t.Require().NoError(MigrationResourceConfig.Validate())
	t.setResourceConfig(MigrationResourceConfig)

	const v1Index, v2Index = "m_search_v1", "m_search_v2"
	ids := []string{"1", "2", "3", "4", "5"}

	for _, id := range ids {
		t.fakeProvider.SetResource("m", id, map[string]any{
			"id": id, "field1": "orig" + id, "field2": "x" + id,
		})
		t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "m", ResourceID: id, Kind: core.ChangeCreated,
		}))
	}
	t.worker.Drain(t.T().Context())

	// A paginated walk with a small chunk, so a checkpoint fires mid-walk:
	// pages of 2 (ids sorted: [1,2] [3,4] [5]), flush every 2 documents.
	// Checkpoints trail flushed page boundaries, so the first checkpoint is
	// {2, "3"} — emitted by the flush that lands on page 2's last document.
	t.fakeProvider.SetPageSize(2)
	smallChunkIdx := t.newIndexer(MigrationResourceConfig, core.Config{RebuildChunkSize: 2})

	// The walk must rebuild from this state, so the aborted attempt's output
	// is distinguishable from the ingest above.
	for _, id := range ids {
		t.fakeProvider.SetResource("m", id, map[string]any{
			"id": id, "field1": "walk" + id, "field2": "x" + id,
		})
	}

	ctx, cancel := context.WithCancel(t.T().Context())
	defer cancel()
	var cur *core.RebuildCursor
	err := smallChunkIdx.RebuildNowResumable(ctx,
		core.ResourceSelector{ResourceType: "m", Versions: []int{2}}, nil,
		func(c core.RebuildCursor) {
			if cur == nil {
				cc := c
				cur = &cc
				cancel()
			}
		})
	t.Require().Error(err, "an aborted walk must not report success")
	t.Require().NotNil(cur, "a mid-walk flush at a page boundary must checkpoint")
	t.Require().Equal(2, cur.PlanVersion)
	t.Require().Equal("3", cur.PageToken, "the checkpoint trails the last fully flushed page")

	// The source moves on while the walk is down; the resumed walk picks up
	// current data for the pages it re-walks.
	for _, id := range ids {
		t.fakeProvider.SetResource("m", id, map[string]any{
			"id": id, "field1": "resumed" + id, "field2": "x" + id,
		})
	}
	t.fakeProvider.ResetCallCounts()

	t.Require().NoError(smallChunkIdx.RebuildNowResumable(t.T().Context(),
		core.ResourceSelector{ResourceType: "m", Versions: []int{2}}, cur, nil))

	_, listCalls := t.fakeProvider.CallCounts()
	t.Require().Equal(2, listCalls,
		"the resumed walk lists only the pages from the cursor on ([3,4] and [5]); from-scratch would be 3")

	for _, id := range ids {
		f1, ok := t.docFields(v1Index, id)
		t.Require().True(ok, "v1 must still hold %s", id)
		t.Require().Equal("orig"+id, f1["field1"],
			"a targeted v2 walk must never touch v1")
	}
	for _, id := range []string{"1", "2"} {
		f2, ok := t.docFields(v2Index, id)
		t.Require().True(ok)
		t.Require().Equal("walk"+id, f2["field1"],
			"pages before the cursor keep the aborted attempt's settled output")
	}
	for _, id := range []string{"3", "4", "5"} {
		f2, ok := t.docFields(v2Index, id)
		t.Require().True(ok)
		t.Require().Equal("resumed"+id, f2["field1"],
			"pages from the cursor on rebuild from current source data")
	}

	// Everything the walks began must have settled (seq-guarded ClearStale).
	// A non-zero sweep means a resource was left stale.
	swept, err := t.idx.SweepStale(t.T().Context(), 0, 100)
	t.Require().NoError(err)
	t.Require().Zero(swept, "a completed resumed walk must leave no stale backlog")
}

// leftOutOfListing wraps a plan's Executer for a Schema Version that excludes
// some resources by leaving them out of its listing: a walk of the type
// (ResourceID empty) drops every item rooted at one of ids, and a fetch of one
// of them by id answers nil, as nilForIDs does.
type leftOutOfListing struct {
	inner aggregation.Executer[projection.BuildRequest, projection.BuildDoc]
	ids   map[string]bool
}

func (e leftOutOfListing) Execute(ctx context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	if req.ResourceID != "" {
		return nilForIDs(e).Execute(ctx, req)
	}
	out := make(chan aggregation.ExecutionResult[projection.BuildDoc])
	go func() {
		defer close(out)
		for r := range e.inner.Execute(ctx, req) {
			kept := r.Items[:0]
			for _, item := range r.Items {
				if !e.ids[item.Root.Id] {
					kept = append(kept, item)
				}
			}
			r.Items = kept
			out <- r
		}
	}()
	return out
}

// Test_Migration_BackfillProbesWhatItsListingLeftOut: v2's plan excludes 2
// and 4 by leaving them out of its listing, and its Probe leaves them out
// too. A Versions:[2] backfill walks only 1, 3 and 5, then asks the Probe
// about the rows it didn't reach — exactly 2 and 4 — and records them as
// excluded by v2 without a build: an empty v2 edge set, no v2 document, no
// stale mark (laika ADR 0013's Q24 note). Right after it, with no stale
// sweep between, coverage passes, the doc-gap is the two left-out ids, and
// the cutover is ready once the gap is accepted. The pass touches only v2:
// v1's documents of 2 and 4 stay.
func (t *TestSuite) Test_Migration_BackfillProbesWhatItsListingLeftOut() {
	for _, c := range MigrationResourceConfig {
		c.ApplyDefaults()
	}
	t.Require().NoError(MigrationResourceConfig.Validate())

	ctx := t.T().Context()
	esBackend := elasticsearch.New(t.esClient, true)
	v1Index, v2Index := core.IndexName("m", 1), core.IndexName("m", 2)
	ids := []string{"1", "2", "3", "4", "5"}
	leftOut := map[string]bool{"2": true, "4": true}

	// Pre-migration world: only v1 is configured and serving.
	v1Only := resource.Configs{{
		Resource:    "m",
		Versions:    []resource.VersionConfig{MigrationResourceConfig[0].Versions[0]},
		ReadVersion: 1,
	}}
	for _, c := range v1Only {
		c.ApplyDefaults()
	}
	t.Require().NoError(v1Only.Validate())
	t.setResourceConfig(v1Only)
	for _, id := range ids {
		t.fakeProvider.SetResource("m", id, map[string]any{
			"id": id, "field1": "orig" + id, "field2": "extra" + id,
		})
		t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
			ResourceType: "m", ResourceID: id, Kind: core.ChangeCreated,
		}))
	}
	t.worker.Drain(ctx)

	// v2 added, its plan leaving 2 and 4 out of its listing, its by-id fetch
	// and its Probe, which records the ids it is asked about.
	t.setResourceConfig(MigrationResourceConfig)
	var (
		probeMu sync.Mutex
		asked   []string
	)
	plans := dsl.BuildPlansFromConfig(t.fakeProvider, MigrationResourceConfig)
	for i, p := range plans["m"] {
		if p.Version == 2 {
			plans["m"][i].Executer = leftOutOfListing{inner: p.Executer, ids: leftOut}
			plans["m"][i].Probe = func(_ context.Context, probed []string, _ map[string]string) ([]string, error) {
				probeMu.Lock()
				defer probeMu.Unlock()
				asked = append(asked, probed...)
				var present []string
				for _, id := range probed {
					if !leftOut[id] {
						present = append(present, id)
					}
				}
				return present, nil
			}
		}
	}
	x := t.newIndexer(MigrationResourceConfig, core.Config{Plans: plans})

	proposed := *MigrationResourceConfig[0]
	proposed.ReadVersion = 2
	proposedCfg := resource.Configs{&proposed}
	t.Require().NoError(proposedCfg.Validate())
	check := func(opts core.ReadinessOptions) core.ResourceReadiness {
		results := core.CheckCutoverReadiness(ctx, esBackend, t.st, proposedCfg, opts)
		t.Require().Len(results, 1)
		return results[0]
	}
	gate := func(r core.ResourceReadiness, name string) core.ReadinessCheck {
		for _, c := range r.Checks {
			if c.Name == name {
				return c
			}
		}
		t.T().Fatalf("readiness has no %q check: %+v", name, r.Checks)
		return core.ReadinessCheck{}
	}

	// Backfill v2. No stale sweep runs after it.
	t.Require().NoError(x.RebuildNow(ctx, []core.ResourceSelector{
		{ResourceType: "m", Versions: []int{2}},
	}))
	t.Require().NoError(x.WaitForIdle(ctx))

	// Covered with no sweep, and v2 holds 3 documents to v1's 5: the gap is
	// the two left-out ids, and fails until accepted.
	r := check(core.ReadinessOptions{})
	t.Require().False(r.Ready)
	coverage := gate(r, core.CheckCoverage)
	t.Require().True(coverage.OK, coverage.Detail)
	docGap := gate(r, core.CheckDocGap)
	t.Require().False(docGap.OK)
	t.Require().Contains(docGap.Detail, fmt.Sprintf(`%s holds 5, %s holds 3, of 5 "m" resources (gap %+d)`,
		v1Index, v2Index, -len(leftOut)))

	r = check(core.ReadinessOptions{AcceptGap: map[string]bool{"m": true}})
	t.Require().True(r.Ready, "%+v", r.Checks)
	t.Require().Contains(gate(r, core.CheckDocGap).Detail, "(accepted)")

	// What the readiness rests on: the backfill asked v2's Probe about
	// exactly the rows its listing left out, and recorded v2's answer for
	// each of them without marking it or touching v1.
	probeMu.Lock()
	gotAsked := slices.Sorted(slices.Values(asked))
	probeMu.Unlock()
	t.Require().Equal([]string{"2", "4"}, gotAsked,
		"the backfill must ask v2's Probe about exactly the rows its listing left out")
	for _, id := range ids {
		_, ok := t.docFields(v2Index, id)
		t.Require().Equal(!leftOut[id], ok, "v2 holds %s only if its listing returns it", id)
		_, ok = t.docFields(v1Index, id)
		t.Require().True(ok, "the v2 backfill must not touch v1's document of %s", id)
		t.Require().Nil(t.staleSince("m", id), "%s must not be marked: v2 decided it without a build", id)
		t.Require().True(t.hasEdgeSet("m", id, 2), "%s must have a v2 edge set after the backfill", id)
		t.Require().True(t.hasEdgeSet("m", id, 1), "the v2 backfill must not touch v1's edge set of %s", id)
	}
	for id := range leftOut {
		t.Require().Empty(t.versionEdges("m", id, 2), "v2 excludes %s: its v2 edge set must be empty", id)
	}
}

// hasEdgeSet reports whether the resource has a stored edge set of the
// Schema Version, empty or not.
func (t *TestSuite) hasEdgeSet(resourceType, id string, version int) bool {
	var ok bool
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT EXISTS (SELECT 1 FROM edge_sets WHERE type=$1 AND id=$2 AND schema_version=$3)`,
		resourceType, id, version).Scan(&ok)
	t.Require().NoError(err)
	return ok
}
