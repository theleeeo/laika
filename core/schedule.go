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
// The mark leaves each root's metadata alone, and the build it submits
// carries none: an owned build fetches with the root's own metadata as its
// BeginBuild returns it.
//
// It is the cascade path, whose submissions shed on a full queue and never
// wait: build Parents and drift re-builds run inside pool tasks, where a task
// waiting on its own pool could deadlock it, and the rebuild flusher runs in a
// rebuild walk, which must not stall behind producer backpressure.
func (idx *Indexer) scheduleBuild(ctx context.Context, roots []model.Resource) error {
	if len(roots) == 0 {
		return nil
	}
	owned, err := idx.st.MarkStale(ctx, roots, idx.ownerLease)
	if err != nil {
		return fmt.Errorf("marking %d resources stale: %w", len(roots), err)
	}
	idx.submitOwnedBuilds(ctx, owned)
	return nil
}

// submitOwnedBuilds is scheduleBuild's opportunistic half: one inline build
// per resource type of the claimed roots, each id carrying its owner token. A
// shed build releases the ownership of its batch.
func (idx *Indexer) submitOwnedBuilds(ctx context.Context, owned []Owned) {
	for resourceType, batch := range groupOwnedByType(owned) {
		args := BuildArgs{
			ResourceType: resourceType,
			ResourceIds:  make([]string, len(batch)),
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

// submitBuild submits one owned inline build of res, already marked stale,
// if the mark claimed it (token != 0); the build fetches with res's metadata
// as its BeginBuild returns it. An unclaimed res has a live owner, whose
// follow-up carries the change, so nothing is submitted.
func (idx *Indexer) submitBuild(ctx context.Context, res model.Resource, token int64, wait bool) {
	if token == 0 {
		return
	}
	args := BuildArgs{
		ResourceType: res.Type,
		ResourceIds:  []string{res.Id},
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
// with, if one is due: a build, which fetches with the row's metadata as its
// BeginBuild returns it, or a delete when the row is a tombstone. It runs
// inside the finishing task (or the synchronous sweep), so it never waits; a
// shed follow-up releases the re-claimed ownership and leaves the mark to the
// sweep.
func (idx *Indexer) submitFollowUp(ctx context.Context, res model.Resource, fu FollowUp) {
	if fu.Token == 0 {
		return
	}
	if fu.Deleted {
		idx.submitDelete(ctx, res, fu.Token, fu.Token, false)
		return
	}
	idx.submitBuild(ctx, res, fu.Token, false)
}

// buildTask is the pool task of an owned inline build. It builds only the ids
// whose ownership the task still holds at dequeue (see submitOwned).
func (idx *Indexer) buildTask(args BuildArgs) func(context.Context, []Owned) {
	return func(taskCtx context.Context, held []Owned) {
		args := heldOnly(args, held)
		if len(args.ResourceIds) == 0 {
			return
		}
		// An owned build logs its own failures (Build), so its error is not
		// logged again.
		_ = idx.Build(taskCtx, args)
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
// is released without a backoff — the work did not fail, it did not run —
// and its mark stays for the next change or the sweep.
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

// failedAttemptsErrorLevel is the attempt count from which a failed owned
// build or delete is logged at Error: a resource that failed this many times
// in a row is not healing on its own.
const failedAttemptsErrorLevel = 5

// releaseFailed finishes the owned work on owned — an owned build or delete,
// op says which — that failed with cause: it drops the ownership and backs
// each row off with Store.ReleaseFailed under the Indexer's sweep backoff,
// so the sweep leaves the row until the backoff has passed, and logs each
// row it backed off with its attempt count, at Error from the fifth attempt
// in a row. A row with a change of its own registered since the claim is
// released without a backoff (ReleaseFailed returns it with no attempts) and
// logged at Warn as such; a row whose ownership was lost meanwhile is
// neither released nor backed off, and is logged without one. The mark
// stays either way. A failure reached once ctx has ended — a cancellation
// or shutdown cut the work short — is not the work's, so it releases
// through releaseOwners without a backoff. The release runs on a context detached from ctx's
// cancellation, as releaseOwners' does; a failed one is logged per entry,
// and each ownership expires with its lease.
func (idx *Indexer) releaseFailed(ctx context.Context, op string, owned []Owned, cause error) {
	if len(owned) == 0 {
		return
	}
	if ctx.Err() != nil {
		for _, o := range owned {
			slog.Info("owned "+op+" ended by cancellation or shutdown; released without a backoff, its mark stays",
				slog.String("type", o.Type), slog.String("id", o.Id), slog.String("error", cause.Error()))
		}
		idx.releaseOwners(ctx, owned)
		return
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ownerReleaseTimeout)
	defer cancel()
	backedOff, err := idx.st.ReleaseFailed(rctx, owned, idx.sweepBackoff)
	if err != nil {
		for _, o := range owned {
			slog.Warn("owned "+op+" failed, and releasing its ownership failed; it expires with its lease and the sweep retries it",
				slog.String("type", o.Type), slog.String("id", o.Id),
				slog.String("error", cause.Error()), slog.String("release_error", err.Error()))
		}
		return
	}
	logged := make(map[model.Resource]bool, len(backedOff))
	for _, b := range backedOff {
		logged[b.Resource] = true
		if b.Attempts == 0 {
			slog.Warn("owned "+op+" failed, but a change to the resource was registered meanwhile; released without a backoff, left for the sweep",
				slog.String("type", b.Type), slog.String("id", b.Id), slog.String("error", cause.Error()))
			continue
		}
		level := slog.LevelWarn
		if b.Attempts >= failedAttemptsErrorLevel {
			level = slog.LevelError
		}
		slog.Log(rctx, level, "owned "+op+" failed; the stale sweep retries it after a backoff",
			slog.String("type", b.Type), slog.String("id", b.Id), slog.Int("attempts", b.Attempts),
			slog.Time("retry_after", b.After), slog.String("error", cause.Error()))
	}
	for _, o := range owned {
		if !logged[o.Resource] {
			slog.Warn("owned "+op+" failed after its ownership was lost; its new owner or a clean row covers it",
				slog.String("type", o.Type), slog.String("id", o.Id), slog.String("error", cause.Error()))
		}
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
