package core

import (
	"context"
	"fmt"
	"log/slog"

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
// marks of their Parents, commit in one statement or not at all. A non-nil
// error means nothing was committed and the whole batch may be retried.
// After the commit, deletes and builds are submitted per id (WaitForSlot
// applies to them). The statuses are index-aligned with ns.
//
// The batch is validated before any statement: an unknown resource or two
// notifications naming the same resource fail the call.
//
// Every mark lands in the statement, before any submission, so a
// WaitForSlot wait or a shed leaves nothing unmarked (ADR 0008); a wait
// ended by ctx or shutdown still returns the statuses and a nil error.
func (idx *Indexer) RegisterChanges(ctx context.Context, ns []Notification, opts ...RegisterOption) ([]RegisterStatus, error) {
	items, err := idx.registrations(ns)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return []RegisterStatus{}, nil
	}

	reg, err := idx.st.RegisterChanges(ctx, items)
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

	wait := newRegisterOptions(opts).waitForSlot
	for i, it := range reg.Items {
		if !it.Accepted {
			continue
		}
		res, seq := items[i].Resource, it.StaleSeq
		if !items[i].Deleted {
			idx.submitBuild(ctx, res, items[i].Metadata, wait)
			continue
		}
		if !idx.submit(ctx, wait, func(taskCtx context.Context) {
			idx.deleteOne(taskCtx, res, seq)
		}) {
			slog.Info(notSubmittedMsg(wait, "tombstone left for sweep"),
				slog.String("type", res.Type), slog.String("id", res.Id))
		}
	}
	for _, p := range reg.Parents {
		idx.submitBuild(ctx, p.Resource, p.Metadata, wait)
	}
	return statuses, nil
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
