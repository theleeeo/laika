package core

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/theleeeo/laika/model"
)

// scheduleBuild durably marks roots stale, then opportunistically builds the
// ones the mark claimed inline on the pool. The mark always lands before the
// build attempt, so a shed submission, failed build, or crash is recovered by
// the stale sweep. See ADR 0008.
//
// A root that already has a live owner is marked but not submitted: its
// owner's finish sees the moved stale_seq and runs the follow-up. That
// includes the building resource itself when its drift check re-marks it.
//
// It is the cascade path, whose submissions shed on a full queue and never
// wait: build Parents and drift re-builds run inside pool tasks, where a task
// waiting on its own pool could deadlock it, and the rebuild flusher runs in a
// rebuild walk, which must not stall behind producer backpressure.
func (idx *Indexer) scheduleBuild(ctx context.Context, roots []model.Resource, metadata map[string]string) error {
	if len(roots) == 0 {
		return nil
	}
	owned, err := idx.st.MarkStale(ctx, roots, idx.ownerLease)
	if err != nil {
		return fmt.Errorf("marking %d resources stale: %w", len(roots), err)
	}
	idx.submitOwnedBuilds(ctx, owned, metadata)
	return nil
}

// submitOwnedBuilds is scheduleBuild's opportunistic half: one inline build
// per resource type of the claimed roots, each id carrying its owner token. A
// shed build releases the ownership of its batch.
func (idx *Indexer) submitOwnedBuilds(ctx context.Context, owned []Owned, metadata map[string]string) {
	for resourceType, batch := range groupOwnedByType(owned) {
		args := BuildArgs{
			ResourceType: resourceType,
			ResourceIds:  make([]string, len(batch)),
			Metadata:     metadata,
			OwnerTokens:  make(map[string]int64, len(batch)),
		}
		for i, o := range batch {
			args.ResourceIds[i] = o.Id
			args.OwnerTokens[o.Id] = o.Token
		}
		if !idx.submitOwned(ctx, false, batch, idx.buildTask(args)) {
			slog.Info(notSubmittedMsg(false, "resources left stale for sweep"),
				slog.String("type", resourceType),
				slog.Int("count", len(batch)),
			)
		}
	}
}

// submitBuild submits one inline build of res, already marked stale, with
// its own metadata, if the mark claimed it (token != 0). An unclaimed res has
// a live owner, whose follow-up carries the change, so nothing is submitted.
func (idx *Indexer) submitBuild(ctx context.Context, res model.Resource, metadata map[string]string, token int64, wait bool) {
	if token == 0 {
		return
	}
	args := BuildArgs{
		ResourceType: res.Type,
		ResourceIds:  []string{res.Id},
		Metadata:     metadata,
		OwnerTokens:  map[string]int64{res.Id: token},
	}
	if !idx.submitOwned(ctx, wait, []Owned{{Resource: res, Token: token}}, idx.buildTask(args)) {
		slog.Info(notSubmittedMsg(wait, "resource left stale for sweep"),
			slog.String("type", res.Type), slog.String("id", res.Id))
	}
}

// submitDelete submits the inline delete of the tombstoned res, guarded by
// staleSeq, if the mark claimed it (token != 0), as submitBuild does.
func (idx *Indexer) submitDelete(ctx context.Context, res model.Resource, staleSeq, token int64, wait bool) {
	if token == 0 {
		return
	}
	if !idx.submitOwned(ctx, wait, []Owned{{Resource: res, Token: token}}, func(taskCtx context.Context, _ []Owned) {
		idx.deleteOne(taskCtx, res, staleSeq, token)
	}) {
		slog.Info(notSubmittedMsg(wait, "tombstone left for sweep"),
			slog.String("type", res.Type), slog.String("id", res.Id))
	}
}

// submitFollowUp submits the follow-up an owned build or delete finished
// with, if one is due: a build with the row's metadata, or a delete when the
// row is a tombstone. It runs inside the finishing task (or the synchronous
// sweep), so it never waits; a shed follow-up releases the re-claimed
// ownership and leaves the mark to the sweep.
func (idx *Indexer) submitFollowUp(ctx context.Context, res model.Resource, fu FollowUp) {
	if fu.Token == 0 {
		return
	}
	if fu.Deleted {
		idx.submitDelete(ctx, res, fu.Token, fu.Token, false)
		return
	}
	idx.submitBuild(ctx, res, nil, fu.Token, false)
}

// buildTask is the pool task of an owned inline build. It builds only the ids
// whose ownership the task still holds at dequeue (see submitOwned).
func (idx *Indexer) buildTask(args BuildArgs) func(context.Context, []Owned) {
	return func(taskCtx context.Context, held []Owned) {
		args := heldOnly(args, held)
		if len(args.ResourceIds) == 0 {
			return
		}
		if err := idx.Build(taskCtx, args); err != nil {
			slog.Warn("inline build failed; resources remain stale for sweep",
				slog.String("type", args.ResourceType),
				slog.String("error", err.Error()),
			)
		}
	}
}

// heldOnly narrows an owned build's args to the ids of held, the ownerships
// it still holds; the others are dropped from ResourceIds and OwnerTokens.
func heldOnly(args BuildArgs, held []Owned) BuildArgs {
	keep := make(map[Owned]bool, len(held))
	for _, o := range held {
		keep[o] = true
	}
	out := args
	out.ResourceIds = make([]string, 0, len(held))
	out.OwnerTokens = make(map[string]int64, len(held))
	for _, id := range args.ResourceIds {
		token := args.OwnerTokens[id]
		if keep[Owned{Resource: model.Resource{Type: args.ResourceType, Id: id}, Token: token}] {
			out.ResourceIds = append(out.ResourceIds, id)
			out.OwnerTokens[id] = token
		}
	}
	return out
}

// submitOwned hands task, which works on the claimed owned, to the pool (see
// submit). When a worker dequeues it, the task renews their leases, so the
// lease runs from the dequeue rather than from the claim, and runs only for
// the ownerships still held, which it is given: one lost while queued is
// covered by its new owner or a clean row. If none is held, or the renewal
// fails, the task does nothing and its marks stay (see renewOwners). A
// submission the pool refuses releases them, so the next change claims or
// the sweep rebuilds.
func (idx *Indexer) submitOwned(ctx context.Context, wait bool, owned []Owned, task func(context.Context, []Owned)) bool {
	if idx.submit(ctx, wait, func(taskCtx context.Context) {
		if held := idx.renewOwners(taskCtx, owned); len(held) > 0 {
			task(taskCtx, held)
		}
	}) {
		return true
	}
	idx.releaseOwners(ctx, owned)
	return false
}

// submit hands task to the pool: waiting for a slot when wait is set, else
// shedding on a full queue. Either way a false return leaves the already
// marked work to the sweep.
func (idx *Indexer) submit(ctx context.Context, wait bool, task func(context.Context)) bool {
	if wait {
		return idx.pool.submitWait(ctx, task)
	}
	return idx.pool.trySubmit(task)
}

// ownerReleaseTimeout bounds a release made on a context detached from
// cancellation. It is short: a release only spares the next change the wait
// for the lease to expire, and the lease is the backstop.
const ownerReleaseTimeout = 5 * time.Second

// renewOwners renews the leases of owned and returns the ownerships still
// held; its caller works only on those. An ownership it lost — after the
// lease lapsed, another owner claimed the row under a newer mark, or a clear
// or hard delete dropped it — is logged at Info and not worked on: the new
// owner or a clean row covers it. A failed renewal is logged and holds
// nothing, so work whose ownership is unknown is never done: its ownership
// is released, as a failed owned build's is, and its mark stays for the
// next change or the sweep.
func (idx *Indexer) renewOwners(ctx context.Context, owned []Owned) []Owned {
	if len(owned) == 0 {
		return nil
	}
	held, err := idx.st.RenewOwners(ctx, owned)
	if err != nil {
		slog.Warn("renewing build ownership failed; its work is skipped and left stale",
			slog.Int("count", len(owned)), slog.String("error", err.Error()))
		idx.releaseOwners(ctx, owned)
		return nil
	}
	if len(held) < len(owned) {
		kept := make(map[Owned]bool, len(held))
		for _, o := range held {
			kept[o] = true
		}
		for _, o := range owned {
			if !kept[o] {
				slog.Info("build ownership lost before its work ran; skipped, its new owner or a clean row covers it",
					slog.String("type", o.Type), slog.String("id", o.Id), slog.Int64("token", o.Token))
			}
		}
	}
	return held
}

// releaseOwners drops the ownership of owned, best effort, on a context
// detached from ctx's cancellation: the release is often due precisely
// because ctx ended. A failure is logged; the lease expires on its own.
func (idx *Indexer) releaseOwners(ctx context.Context, owned []Owned) {
	if len(owned) == 0 {
		return
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ownerReleaseTimeout)
	defer cancel()
	if err := idx.st.ReleaseOwners(rctx, owned); err != nil {
		slog.Warn("releasing build ownership failed; it expires with its lease",
			slog.Int("count", len(owned)), slog.String("error", err.Error()))
	}
}

// notSubmittedMsg is the Info log for a submission the pool did not accept.
func notSubmittedMsg(wait bool, outcome string) string {
	if wait {
		return "wait for a build slot ended by cancellation or shutdown; " + outcome
	}
	return "build pool saturated; " + outcome
}

// groupOwnedByType groups claimed resources by type; an entry without a
// token was not claimed and is dropped.
func groupOwnedByType(owned []Owned) map[string][]Owned {
	byType := make(map[string][]Owned, len(owned))
	for _, o := range owned {
		if o.Token == 0 {
			continue
		}
		byType[o.Type] = append(byType[o.Type], o)
	}
	return byType
}
