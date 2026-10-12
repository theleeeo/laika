package core

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/theleeeo/laika/model"
)

// RegisterChange registers a single change notification: a batch of one
// (see RegisterChanges). A stale version returns ErrStaleVersion.
func (idx *Indexer) RegisterChange(ctx context.Context, n Notification, opts ...RegisterOption) error {
	statuses, err := idx.RegisterChanges(ctx, []Notification{n}, opts...)
	if err != nil {
		return err
	}
	if statuses[0] == RegisterStale {
		return fmt.Errorf("%s/%s version %d: %w", n.ResourceType, n.ResourceID, n.Version, ErrStaleVersion)
	}
	return nil
}

// RegisterStatus is one notification's outcome in RegisterChanges.
type RegisterStatus int

const (
	// RegisterAccepted: the change is durably recorded and its builds are
	// scheduled.
	RegisterAccepted RegisterStatus = iota
	// RegisterStale: a non-zero version not greater than the stored one;
	// nothing was written for it.
	RegisterStale
)

// RegisterChanges registers a batch of notifications atomically: every
// accepted item's version or tombstone, stale mark and metadata, and the
// marks of their Parents, which leave the Parents' metadata, commit in one
// statement or not at all. A non-nil
// error means nothing was committed and the whole batch may be retried.
// After the commit, what the statement claimed is submitted to the inline
// build pool: each accepted delete as a task of its own, and the builds of
// the accepted items and the Parents per type, the items first, in tasks of
// at most the type's batch size (WaitForSlot applies to each task); an item
// or Parent with a live owner is only marked, and its owner's follow-up
// carries the change. The statuses are index-aligned with ns.
//
// The batch is validated before any statement: an unknown resource or two
// notifications naming the same resource fail the call.
//
// Once the statement has committed, a second fanout marks the Parents the
// statement's fanout missed (markMissedParents): a Parent whose edge to an
// item committed while the statement ran. It runs whether or not ctx has
// ended since the commit; its failure is logged, not returned.
//
// Every mark lands in the statement or the second fanout, before any item
// or Parent submission, so a WaitForSlot wait or a shed leaves nothing
// unmarked (ADR 0008); a wait ended by ctx or shutdown still returns the
// statuses and a nil error.
func (idx *Indexer) RegisterChanges(ctx context.Context, ns []Notification, opts ...RegisterOption) ([]RegisterStatus, error) {
	items, err := idx.registrations(ns)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return []RegisterStatus{}, nil
	}

	reg, err := idx.st.RegisterChanges(ctx, items, idx.ownerLease)
	if err != nil {
		return nil, fmt.Errorf("registering %d changes: %w", len(items), err)
	}
	if len(reg.Items) != len(items) {
		return nil, fmt.Errorf("registering %d changes: store returned %d results", len(items), len(reg.Items))
	}

	statuses := make([]RegisterStatus, len(items))
	var stale int
	for i, it := range reg.Items {
		if !it.Accepted {
			statuses[i] = RegisterStale
			stale++
		}
	}
	slog.Info("registered changes",
		"count", len(items),
		"stale", stale,
		"affected_parents", len(reg.Parents),
	)

	idx.markMissedParents(ctx, items, reg)

	wait := newRegisterOptions(opts).waitForSlot
	var builds []Owned
	for i, it := range reg.Items {
		if !it.Accepted {
			continue
		}
		if items[i].Deleted {
			idx.submitDelete(ctx, items[i].Resource, it.StaleSeq, it.Token, wait)
			continue
		}
		builds = append(builds, Owned{Resource: items[i].Resource, Token: it.Token})
	}
	// The Parents never repeat an item (Registered), so no id is given two
	// tokens in one batch.
	for _, p := range reg.Parents {
		builds = append(builds, Owned{Resource: p.Resource, Token: p.Token})
	}
	idx.submitBuildBatches(ctx, wait, builds)
	return statuses, nil
}

// markMissedParents is a registration's second fanout, run once its
// statement has committed: it reads every Parent the stored edges of the
// accepted items name (Store.ParentsOf), drops the Parents the statement
// marked (reg.Parents) and the accepted items, whose own rows carry the
// change's mark, and marks the rest through scheduleBuild, which submits a
// build of each it claims, shedding on a full queue. A rejected item is not
// dropped, as the statement's fanout marks one too.
//
// It closes the race of a Parent's edge write against the statement (ADR
// 0002). The statement's fanout reads the edges from its snapshot, and a
// build's drift check can't see a registration that hasn't committed. A
// build of a Parent that started below the change and stored an edge to the
// item either runs its drift check after the commit, which sees the change
// and re-marks the Parent, or ran it before, when its document was written
// and its edge committed: this read sees that edge, and its mark brings a
// build that begins after it and fetches after the change. That rests on
// when the drift check and the edge commit happen, not on any Build Sequence,
// so a rebuild walk, which takes its Build Sequence after its fetch, is
// covered too. It filters nothing by mark for the same reason: a Parent
// marked since the change may be served by a build that fetched before it.
// Outside the race it marks nothing, or a Parent whose edge committed just
// after the commit, a duplicate of that build's own drift re-mark.
//
// It runs on a context detached from ctx's cancellation and bounded by
// secondFanoutTimeout, as releaseOwners does: the batch has committed, so a
// caller that gives up right after the commit doesn't skip it. A failed read
// or mark is logged at Warn with the ids it concerns — the changed resources,
// or the Parents — at most loggedIDsCap of them, and leaves the window as it
// was: the batch is committed, so the registration still succeeds.
func (idx *Indexer) markMissedParents(ctx context.Context, items []Registration, reg Registered) {
	var children []model.Resource
	marked := make(map[model.Resource]bool, len(items)+len(reg.Parents))
	for i, it := range reg.Items {
		if !it.Accepted {
			continue
		}
		children = append(children, items[i].Resource)
		marked[items[i].Resource] = true
	}
	if len(children) == 0 {
		return
	}
	for _, p := range reg.Parents {
		marked[p.Resource] = true
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), secondFanoutTimeout)
	defer cancel()
	parents, err := idx.st.ParentsOf(ctx, children)
	if err != nil {
		ids, omitted := loggedIDs(children)
		slog.Warn("reading the Parents a registration's fanout missed failed; a Parent whose edge to one of these changed resources raced the registration is left unmarked",
			slog.Int("count", len(children)), slog.Any("ids", ids), slog.Int("ids_omitted", omitted),
			slog.String("error", err.Error()))
		return
	}
	parents = slices.DeleteFunc(parents, func(p model.Resource) bool { return marked[p] })
	if len(parents) == 0 {
		return
	}
	slog.Info("marking Parents a registration's fanout missed", slog.Int("count", len(parents)))
	if err := idx.scheduleBuild(ctx, parents); err != nil {
		ids, omitted := loggedIDs(parents)
		slog.Warn("marking the Parents a registration's fanout missed failed; they are left unmarked",
			slog.Int("count", len(parents)), slog.Any("ids", ids), slog.Int("ids_omitted", omitted),
			slog.String("error", err.Error()))
	}
}

// loggedIDsCap bounds the ids a second fanout's warning names: a batch, and
// the Parents its read returns, can be large, and one log line shouldn't be.
const loggedIDsCap = 100

// loggedIDs is rs as "type/id" strings for a warning, so an operator can
// rebuild them: the first loggedIDsCap of them, and how many were left out.
func loggedIDs(rs []model.Resource) (ids []string, omitted int) {
	n := min(len(rs), loggedIDsCap)
	ids = make([]string, n)
	for i, r := range rs[:n] {
		ids[i] = r.Type + "/" + r.Id
	}
	return ids, len(rs) - n
}

// registrations validates a batch and translates it for the store: every
// resource known, none named twice.
func (idx *Indexer) registrations(ns []Notification) ([]Registration, error) {
	items := make([]Registration, len(ns))
	seen := make(map[model.Resource]struct{}, len(ns))
	for i, n := range ns {
		if err := idx.verifyResourceConfig(n); err != nil {
			return nil, err
		}
		res := model.Resource{Type: n.ResourceType, Id: n.ResourceID}
		if _, dup := seen[res]; dup {
			return nil, &InvalidArgumentError{Msg: fmt.Sprintf("resource %s/%s appears more than once in the batch", res.Type, res.Id)}
		}
		seen[res] = struct{}{}
		items[i] = Registration{
			Resource: res,
			Deleted:  n.Kind == ChangeDeleted,
			Version:  n.Version,
			Metadata: n.Metadata,
		}
	}
	return items, nil
}
