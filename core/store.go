package core

import (
	"context"
	"time"

	"github.com/theleeeo/laika/model"
)

type Store interface {
	GetChildResources(ctx context.Context, parentResource model.Resource) ([]model.Resource, error)
	GetParentResources(ctx context.Context, childResource model.Resource) ([]model.Resource, error)
	RemoveResource(ctx context.Context, resource model.Resource) error
	// ReplaceEdges stores the edges one build of resource discovered, per
	// Schema Version, at the build's Build Sequence buildSeq, in one
	// transaction. Each set replaces its version's stored edge set only when
	// buildSeq is not below the sequence that set is stamped with, and then
	// stamps it with buildSeq; a set stamped higher is left unchanged. So each
	// version's edges follow the Build Sequence like its document does
	// (ADR 0002): they come from the build Elasticsearch keeps. A set with no
	// Children replaces the stored one with no edges; a child may repeat in
	// Children (two relation fields can name it) and is stored once. The order of sets does
	// not matter.
	//
	// Versions not in sets are untouched, unless declared is non-nil: then
	// the stored sets of versions outside declared that are stamped at or
	// below buildSeq are dropped with their edges. The live build, which runs
	// every configured plan, passes the config's versions; a rebuild passes
	// nil.
	ReplaceEdges(ctx context.Context, resource model.Resource, buildSeq int64, sets []EdgeSet, declared []int) error

	// RegisterChanges records a batch of changes in one atomic statement:
	// each accepted item's version (or tombstone), stale mark and metadata,
	// and the stale marks of the accepted items' Parents, commit together or
	// not at all. See Registration for what is accepted. The items must name
	// distinct resources; the caller validates that.
	//
	// Every row it marks — accepted items and Parents — is claimed in the
	// same update when it has no owner or its owner's lease (lease, measured
	// from owner_since) has expired; the outcome reports each claim's token.
	RegisterChanges(ctx context.Context, items []Registration, lease time.Duration) (Registered, error)

	// MarkStale durably records build intent for the given resources, along
	// with the notification metadata the eventual build must run with. The
	// metadata is stored per resource (last mark wins) so a sweep-recovered
	// build carries the same context an inline build would have.
	//
	// A lease above zero also claims each marked row that has no live owner,
	// in the same update, and returns the claimed rows; only those may be
	// submitted. A lease of zero marks without claiming and returns nil: the
	// mark hands the work to the sweep and nothing is submitted.
	MarkStale(ctx context.Context, resources []model.Resource, metadata map[string]string, lease time.Duration) ([]Owned, error)
	// BeginBuild bumps the Build Sequence, captures the current stale_seq and
	// takes the build's start from the Change Sequence. Callers invoke it
	// before the build's fetches. A non-zero token that is the row's owner
	// token renews its lease (owner_since = now()); 0 is a build that owns
	// nothing.
	BeginBuild(ctx context.Context, resource model.Resource, token int64) (BuildBegun, error)
	// RenewOwners renews the lease of every given ownership whose token is
	// still the row's owner token, leaves the others alone, and returns the
	// renewed ones: the ownerships still held. One it didn't renew is lost —
	// another owner claimed the row under a newer mark, or a token-less clear
	// (ClearStale) or a hard delete dropped it — and its holder does nothing
	// for it. A pool task calls it when it is dequeued, the sweep before
	// serving each entry.
	RenewOwners(ctx context.Context, owned []Owned) ([]Owned, error)
	// ReleaseOwners drops every given ownership whose token is still the
	// row's owner token, leaving the stale mark: a failed or shed owned
	// build, so the next change claims or the sweep rebuilds.
	ReleaseOwners(ctx context.Context, owned []Owned) error
	// NextChangeSeq takes a value of the Change Sequence: the start of a
	// Rebuild plan walk, taken before the walk fetches its first page.
	NextChangeSeq(ctx context.Context) (int64, error)
	// AnyChangedSince is the ADR 0002 drift check: it reports whether any
	// checked resource had a change accepted by RegisterChanges after the
	// check's Start, i.e. its stored change_seq exceeds Start. A resource
	// without a row has never changed.
	AnyChangedSince(ctx context.Context, checks []ChangeCheck) (bool, error)
	// ClearStale finishes a build that owns nothing (a rebuild walk, a
	// direct Build): it clears the stale mark, and any ownership with it,
	// only if staleSeq still matches.
	ClearStale(ctx context.Context, resource model.Resource, staleSeq int64) error
	// FinishOwned finishes an owned build in one statement. If stale_seq
	// still equals staleSeq it clears the mark and the ownership. If it moved
	// and token is still the owner token, it re-claims the row for a
	// follow-up and returns it. Otherwise it changes nothing and returns no
	// follow-up.
	FinishOwned(ctx context.Context, resource model.Resource, staleSeq, token int64) (FollowUp, error)
	// DeleteResourceIfSeq finishes an owned delete: it hard-deletes the
	// tombstoned row, and its ownership with it, when stale_seq still equals
	// staleSeq. When it moved and token is still the owner token, it
	// re-claims the row and returns the follow-up, as FinishOwned does.
	DeleteResourceIfSeq(ctx context.Context, resource model.Resource, staleSeq, token int64) (FollowUp, error)
	// ListStale returns up to limit resources whose stale mark predates
	// before and that have no live owner under lease, and claims every row
	// it returns in the same statement.
	ListStale(ctx context.Context, before time.Time, limit int, lease time.Duration) ([]StaleResource, error)
}

// EdgeSet is the edges one Schema Version's plan discovered for a resource:
// its Children.
type EdgeSet struct {
	SchemaVersion int
	Children      []model.Resource
}

// BuildBegun is what BeginBuild returns for one resource.
type BuildBegun struct {
	// BuildIdx is the bumped Build Sequence, sent as the ES external_gte
	// version.
	BuildIdx int64
	// StaleSeq is the stale_seq the build's finish — FinishOwned, or
	// ClearStale for a build that owns nothing — is guarded by.
	StaleSeq int64
	// Start is a value of the Change Sequence taken before the build's
	// fetches; the drift check compares against it.
	Start int64
}

// ChangeCheck is one resource of a drift check, with the start of the build
// that fetched it.
type ChangeCheck struct {
	Resource model.Resource
	Start    int64
}

// Owned is a resource whose ownership a statement claimed, with its owner
// token: the row's owner_seq, which the claim set to the row's stale_seq.
type Owned struct {
	model.Resource
	Token int64
}

// FollowUp is what finishing an owned build or delete hands on.
type FollowUp struct {
	// Token is the owner token re-claimed for the follow-up, the row's
	// stale_seq at the re-claim; 0 means no follow-up is due. A follow-up
	// delete is guarded by it (DeleteResourceIfSeq's staleSeq).
	Token int64
	// Metadata is the row's metadata: that of its last mark, in commit order.
	Metadata map[string]string
	// Deleted reports a tombstone: the follow-up is a delete, not a build.
	Deleted bool
}

// StaleResource is one entry of the stale backlog.
type StaleResource struct {
	model.Resource
	StaleSeq int64
	// Token is the owner token ListStale claimed the row under.
	Token   int64
	Deleted bool
	// Metadata is the notification metadata stored by the most recent
	// MarkStale, replayed into the build that serves the mark.
	Metadata map[string]string
}

// Registration is one item of a RegisterChanges batch.
type Registration struct {
	Resource model.Resource
	// Deleted tombstones the row instead of upserting it. A delete is always
	// accepted; Version is ignored and the stored version resets to 0.
	Deleted bool
	// Version 0 means the upstream does not track versions: the item is
	// always accepted. A non-zero Version is accepted only when strictly
	// greater than the stored one; otherwise the item is stale and nothing
	// is written for it — neither its row nor its Parents' marks.
	Version int64
	// Metadata is stored on the item's own row with its mark.
	Metadata map[string]string
}

// Registered is the committed outcome of RegisterChanges.
type Registered struct {
	// Items is index-aligned with the input.
	Items []RegisteredItem
	// Parents are the rows the statement marked stale as Parents of accepted
	// items, excluding resources that are themselves accepted items of the
	// batch (their own row already carries the mark). Each appears once.
	Parents []MarkedParent
}

// RegisteredItem is one item's outcome.
type RegisteredItem struct {
	Accepted bool
	// StaleSeq is the row's stale_seq after the mark, for accepted items; a
	// delete is submitted with it (DeleteResourceIfSeq).
	StaleSeq int64
	// Token is the owner token the mark claimed, equal to StaleSeq; 0 when
	// the row has a live owner, whose follow-up carries the change, so
	// nothing is submitted for it.
	Token int64
}

// MarkedParent is a Parent RegisterChanges marked stale, with the metadata
// it stored on the row: that of the last accepted child in batch order.
type MarkedParent struct {
	model.Resource
	Metadata map[string]string
	// Token is the owner token the mark claimed; 0 when the row has a live
	// owner, so nothing is submitted for it.
	Token int64
}
