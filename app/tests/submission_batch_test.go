package tests

import (
	"fmt"

	"github.com/theleeeo/laika/app/dsl"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/model"
)

// SubmissionBatchConfig is a two-version resource "q" and a one-version
// resource "r", neither with relations, so a build of either reaches no other
// resource: no Parent is marked and no child edge is stored, and every
// request a plan is asked comes from the submission under test.
var SubmissionBatchConfig = resource.Configs{
	{
		Resource: "q",
		Versions: []resource.VersionConfig{
			{Version: 1, Fields: []resource.FieldConfig{{Name: "f1", Query: primaryTier}}},
			{Version: 2, Fields: []resource.FieldConfig{
				{Name: "f1", Query: primaryTier},
				{Name: "f2", Query: primaryTier},
			}},
		},
		ReadVersion: 1,
	},
	{
		Resource: "r",
		Versions: []resource.VersionConfig{
			{Version: 1, Fields: []resource.FieldConfig{{Name: "f1", Query: primaryTier}}},
		},
	},
}

// submissionBatchIDs are the ids of each type the cases build: seven q's,
// three batches of q's size 3 (3, 3, 1), and five r's, three batches of r's
// size 2 (2, 2, 1).
var submissionBatchIDs = map[string][]string{
	"q": {"q1", "q2", "q3", "q4", "q5", "q6", "q7"},
	"r": {"r1", "r2", "r3", "r4", "r5"},
}

// submissionBatchSizes are the batch sizes of the cases' indexer: q takes the
// indexer's BuildBatchSize, r its own BuildBatchSizes entry.
var submissionBatchSizes = map[string]int{"q": 3, "r": 2}

// submissionBatchIndexer sets SubmissionBatchConfig up with each id's source
// data and returns an indexer of q's and r's batch sizes whose plans record
// their requests in reqs.
func (t *TestSuite) submissionBatchIndexer(reqs *planRequests) *core.Indexer {
	t.setResourceConfig(SubmissionBatchConfig)
	for typ, ids := range submissionBatchIDs {
		for _, id := range ids {
			t.fakeProvider.SetResource(typ, id, map[string]any{"id": id, "f1": typ + "-" + id, "f2": "second-" + id})
		}
	}
	plans := dsl.BuildPlansFromConfig(t.fakeProvider, SubmissionBatchConfig)
	reqs.wrap(plans)
	return t.newIndexer(SubmissionBatchConfig, core.Config{
		Plans:           plans,
		BuildBatchSize:  submissionBatchSizes["q"],
		BuildBatchSizes: map[string]int{"r": submissionBatchSizes["r"]},
	})
}

// requireSubmissionBatchesBuilt checks what built every id of
// submissionBatchIDs: each Schema Version's plan was asked once per batch of
// its type's size, each version's document landed with its source data, and
// each row is clean and unowned.
func (t *TestSuite) requireSubmissionBatchesBuilt(reqs map[string][][]string, what string) {
	plans := map[string][]int{"q": {1, 2}, "r": {1}}
	for _, typ := range []string{"q", "r"} {
		versions := plans[typ]
		ids := submissionBatchIDs[typ]
		for _, v := range versions {
			t.requireOncePerBatch(reqs[fmt.Sprintf("%s/v%d", typ, v)], ids, submissionBatchSizes[typ], fmt.Sprintf("%s, %s's v%d plan", what, typ, v))
			for _, id := range ids {
				fields, _ := t.docSource(core.IndexName(typ, v), id)["fields"].(map[string]any)
				t.Require().Equal(typ+"-"+id, fields["f1"], "%s must land %s/%s's v%d document", what, typ, id, v)
			}
		}
		for _, id := range ids {
			t.Require().Nil(t.staleSince(typ, id), "%s must clear %s/%s's mark", what, typ, id)
			seq, since := t.ownerColumns(typ, id)
			t.Require().Nil(seq, "%s must drop %s/%s's ownership", what, typ, id)
			t.Require().Nil(since)
		}
	}
	t.Require().Len(reqs, 3, "%s: only q's two plans and r's are asked, got %v", what, reqs)
}

// Test_SubmissionBatches_RegistrationBuildsInBatches (L5.7): one registration
// of a page of changes, seven q's and five r's interleaved, builds them in
// per-type batches: each of q's two plans is asked three times (batches of 3)
// and r's plan three times (batches of 2), the requests together asking each
// id once, and every document lands. Building one task per id asks each plan
// once per id. The batches run on the pool concurrently, so the check holds
// whatever order their requests arrive in.
func (t *TestSuite) Test_SubmissionBatches_RegistrationBuildsInBatches() {
	ctx := t.T().Context()
	var reqs planRequests
	x := t.submissionBatchIndexer(&reqs)

	var ns []core.Notification
	for i := range max(len(submissionBatchIDs["q"]), len(submissionBatchIDs["r"])) {
		for _, typ := range []string{"q", "r"} {
			if i < len(submissionBatchIDs[typ]) {
				ns = append(ns, core.Notification{ResourceType: typ, ResourceID: submissionBatchIDs[typ][i], Kind: core.ChangeCreated})
			}
		}
	}
	statuses, err := x.RegisterChanges(ctx, ns)
	t.Require().NoError(err)
	for i, s := range statuses {
		t.Require().Equal(core.RegisterAccepted, s, "notification %d (%s/%s) must be accepted", i, ns[i].ResourceType, ns[i].ResourceID)
	}
	t.Require().NoError(x.WaitForIdle(ctx))

	t.requireSubmissionBatchesBuilt(reqs.takeAll(), "the registration")
}

// Test_SubmissionBatches_SweepBuildsBacklogInBatches (L5.7): a sweep pass over
// a stale backlog of seven q's and five r's that no live owner holds (marked
// with lease 0, as a crashed instance's or a shed build's mark is left) builds
// it in per-type batches: each of q's two plans is asked three times (batches
// of 3) and r's plan three times (batches of 2), the requests together asking
// each id once, every document lands and every mark is cleared. A sweep that
// builds one entry per call asks each plan once per id.
func (t *TestSuite) Test_SubmissionBatches_SweepBuildsBacklogInBatches() {
	ctx := t.T().Context()
	var reqs planRequests
	x := t.submissionBatchIndexer(&reqs)

	var backlog []model.Resource
	for typ, ids := range submissionBatchIDs {
		for _, id := range ids {
			backlog = append(backlog, model.Resource{Type: typ, Id: id})
		}
	}
	owned, err := t.st.MarkStale(ctx, backlog, 0)
	t.Require().NoError(err)
	t.Require().Empty(owned, "a lease-0 mark claims nothing")
	// The marks are old: stale_since is stamped on Postgres's clock and the
	// threshold-0 cutoff is the host's, so a fresh mark could hide behind a
	// container clock running ahead.
	_, err = t.pool.Exec(ctx, `UPDATE resources SET stale_since = now() - interval '1 hour'`)
	t.Require().NoError(err)
	t.Require().Empty(reqs.takeAll(), "marking builds nothing")

	n, err := x.SweepStale(ctx, 0, 100)
	t.Require().NoError(err)
	t.Require().Equal(len(backlog), n, "the sweep must list the whole backlog")
	t.Require().NoError(x.WaitForIdle(ctx))

	t.requireSubmissionBatchesBuilt(reqs.takeAll(), "the sweep")
}
