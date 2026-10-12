package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/app/dsl"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// planRequests records the ids of every by-ids request each wrapped plan is
// asked, by "type/v<version>", in call order. An all-of-type walk's request
// (no ids) is not recorded.
type planRequests struct {
	mu   sync.Mutex
	reqs map[string][][]string
}

// wrap makes every plan of plans record its requests here.
func (p *planRequests) wrap(plans map[string][]projection.Plan) {
	for typ, vps := range plans {
		for i := range vps {
			vps[i].Executer = recordingExecuter{
				inner: vps[i].Executer,
				key:   fmt.Sprintf("%s/v%d", typ, vps[i].Version),
				log:   p,
			}
		}
	}
}

func (p *planRequests) record(key string, ids []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reqs == nil {
		p.reqs = map[string][][]string{}
	}
	p.reqs[key] = append(p.reqs[key], slices.Clone(ids))
}

// take returns the requests of resourceType's version plan recorded so far
// and forgets every plan's.
func (p *planRequests) take(resourceType string, version int) [][]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.reqs[fmt.Sprintf("%s/v%d", resourceType, version)]
	p.reqs = nil
	return out
}

type recordingExecuter struct {
	inner aggregation.Executer[projection.BuildRequest, projection.BuildDoc]
	key   string
	log   *planRequests
}

func (e recordingExecuter) Execute(ctx context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	if len(req.ResourceIDs) > 0 {
		e.log.record(e.key, req.ResourceIDs)
	}
	return e.inner.Execute(ctx, req)
}

// withParents wraps a plan's Executer so the item rooted at id reports
// parents as its Parents (ADR 0006): a child whose own data names many
// Parents of one type, which its build marks in one scheduleBuild and so
// submits as one owned Build.
type withParents struct {
	inner   aggregation.Executer[projection.BuildRequest, projection.BuildDoc]
	id      string
	parents []model.Resource
}

func (e withParents) Execute(ctx context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	out := make(chan aggregation.ExecutionResult[projection.BuildDoc])
	go func() {
		defer close(out)
		for r := range e.inner.Execute(ctx, req) {
			for i, item := range r.Items {
				if item.Root.Id == e.id && item.Doc != nil {
					r.Items[i].Parents = slices.Clone(e.parents)
				}
			}
			out <- r
		}
	}()
	return out
}

// batchSnapshot is what a scenario's builds wrote for each id: its document's
// whole source and its stored edges.
type batchSnapshot struct {
	docs  map[string]map[string]any
	edges map[string][]string
}

// batchRun is one run of the batch scenario: what its by-ids rebuild and its
// cascade wrote, and the by-ids requests a's plan was asked in each.
type batchRun struct {
	rebuild, cascade         batchSnapshot
	rebuildReqs, cascadeReqs [][]string
}

// docSource reads a document's whole _source from a concrete version index.
// The document must exist.
func (t *TestSuite) docSource(index, id string) map[string]any {
	res, err := t.esClient.Get(index, id)
	t.Require().NoError(err)
	defer res.Body.Close()
	t.Require().False(res.IsError(), "get %s/%s: %s", index, id, res.Status())
	var body struct {
		Source map[string]any `json:"_source"`
	}
	t.Require().NoError(json.NewDecoder(res.Body).Decode(&body))
	return body.Source
}

func (t *TestSuite) snapshotA(ids []string) batchSnapshot {
	s := batchSnapshot{docs: map[string]map[string]any{}, edges: map[string][]string{}}
	for _, id := range ids {
		s.docs[id] = t.docSource(core.IndexName("a", 1), id)
		s.edges[id] = t.versionEdges("a", id, 1)
	}
	return s
}

// runBatchScenario runs the scenario on empty stores with an indexer whose
// build batch size is size. A by-ids rebuild builds the a's of ids, each
// with its b child. Then the source changes every a and child, and b/hub's
// creation is registered: hub's build reports every a of ids as its Parent,
// so the cascade marks them all and builds them in one owned Build.
func (t *TestSuite) runBatchScenario(size int, ids []string) batchRun {
	ctx := t.T().Context()
	t.wipeStores()
	t.setResourceConfig(DefaultResourceConfig)

	parents := make([]model.Resource, len(ids))
	for i, id := range ids {
		parents[i] = model.Resource{Type: "a", Id: id}
		t.fakeProvider.SetResource("a", id, map[string]any{"id": id, "field1": "a-" + id, "field2": "before"})
		t.fakeProvider.SetRelated("b", []string{id}, []map[string]any{
			{"id": "b" + id, "a_id": id, "field1": "child-of-" + id, "field2": "before"},
		})
	}
	t.fakeProvider.SetResource("b", "hub", map[string]any{"id": "hub", "field1": "hub"})

	plans := dsl.BuildPlansFromConfig(t.fakeProvider, DefaultResourceConfig)
	for i := range plans["b"] {
		plans["b"][i].Executer = withParents{inner: plans["b"][i].Executer, id: "hub", parents: parents}
	}
	var reqs planRequests
	reqs.wrap(plans)
	x := t.newIndexer(DefaultResourceConfig, core.Config{Plans: plans, BuildBatchSize: size})

	var run batchRun
	t.Require().NoError(x.RebuildNow(ctx, []core.ResourceSelector{{ResourceType: "a", ResourceIDs: ids}}))
	// Nothing the rebuild scheduled may still run into the cascade's requests.
	t.Require().NoError(x.WaitForIdle(ctx))
	run.rebuild = t.snapshotA(ids)
	run.rebuildReqs = reqs.take("a", 1)

	for _, id := range ids {
		t.fakeProvider.SetResource("a", id, map[string]any{"id": id, "field1": "a-" + id, "field2": "after"})
		t.fakeProvider.SetRelated("b", []string{id}, []map[string]any{
			{"id": "b" + id, "a_id": id, "field1": "child-of-" + id, "field2": "after"},
		})
	}
	t.Require().NoError(x.RegisterChange(ctx, core.Notification{ResourceType: "b", ResourceID: "hub", Kind: core.ChangeCreated}))
	t.Require().NoError(x.WaitForIdle(ctx))
	run.cascade = t.snapshotA(ids)
	run.cascadeReqs = reqs.take("a", 1)

	for _, id := range ids {
		fields, _ := run.cascade.docs[id]["fields"].(map[string]any)
		t.Require().Equal("after", fields["field2"], "size %d: the cascade must have rebuilt a/%s from the changed source", size, id)
		t.Require().Nil(t.staleSince("a", id), "size %d: the cascade must settle a/%s", size, id)
	}
	return run
}

// requireOncePerBatch checks reqs asks each of ids once, in requests of at
// most size ids, as few as the batches of size make.
func (t *TestSuite) requireOncePerBatch(reqs [][]string, ids []string, size int, what string) {
	want := (len(ids) + size - 1) / size
	t.Require().Len(reqs, want, "%s with batch size %d: a's plan must be asked once per batch of %d ids, got requests %v", what, size, size, reqs)
	var asked []string
	for _, r := range reqs {
		t.Require().LessOrEqual(len(r), size, "%s with batch size %d: request %v holds more ids than a batch", what, size, r)
		asked = append(asked, r...)
	}
	slices.Sort(asked)
	t.Require().Equal(slices.Sorted(slices.Values(ids)), asked, "%s with batch size %d: the requests must ask each id once", what, size)
}

// Test_BuildBatches_MatchOneAtATimeBuilds (L5.6): a by-ids rebuild of seven
// a's, and a cascade that marks the seven from one child's build, write in
// batches of three the documents and edges they write one at a time (batches
// of one), and a's plan is asked once per batch: three requests that together
// ask each id once, where one-at-a-time builds ask it seven times.
func (t *TestSuite) Test_BuildBatches_MatchOneAtATimeBuilds() {
	ids := []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7"}

	oneAtATime := t.runBatchScenario(1, ids)
	batched := t.runBatchScenario(3, ids)

	t.Require().Equal(oneAtATime.rebuild.docs, batched.rebuild.docs, "the batched by-ids rebuild must write one-at-a-time builds' documents")
	t.Require().Equal(oneAtATime.rebuild.edges, batched.rebuild.edges, "the batched by-ids rebuild must store one-at-a-time builds' edges")
	t.Require().Equal(oneAtATime.cascade.docs, batched.cascade.docs, "the batched cascade must write one-at-a-time builds' documents")
	t.Require().Equal(oneAtATime.cascade.edges, batched.cascade.edges, "the batched cascade must store one-at-a-time builds' edges")
	for _, id := range ids {
		t.Require().Equal([]string{"b/b" + id}, batched.cascade.edges[id], "a/%s's edge set must be its child", id)
	}

	t.requireOncePerBatch(oneAtATime.rebuildReqs, ids, 1, "the by-ids rebuild")
	t.requireOncePerBatch(oneAtATime.cascadeReqs, ids, 1, "the cascade")
	t.requireOncePerBatch(batched.rebuildReqs, ids, 3, "the by-ids rebuild")
	t.requireOncePerBatch(batched.cascadeReqs, ids, 3, "the cascade")
}
