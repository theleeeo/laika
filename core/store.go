package core

import (
	"context"
	"errors"
	"time"

	"github.com/theleeeo/laika/model"
)

// ErrRegistrationAborted is returned by Store.RegisterChanges when a
// concurrent statement made the Store abort the registration, as Postgres
// aborts one of two statements that deadlock: nothing of it was committed and
// it can be retried whole.
var ErrRegistrationAborted = errors.New("registration aborted")

type Store interface {
	GetChildResources(ctx context.Context, parentResource model.Resource) ([]model.Resource, error)
	GetParentResources(ctx context.Context, childResource model.Resource) ([]model.Resource, error)
	// RemoveResource removes a deleted resource's edges at the deleting
	// path's Build Sequence buildSeq, in one transaction: each stored edge
	// set stamped at or below buildSeq goes with its relations; a set stamped
	// above it was written by a newer build and is left, as ReplaceEdges
	// leaves a set stamped above the build that offers it.
	RemoveResource(ctx context.Context, resource model.Resource, buildSeq int64) error
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
	//
	// reported is the resource's own metadata as the build's plans report it
	// (projection.BuildDoc.ResourceMetadata). When it is non-empty, the same
	// transaction stores it as the resource row's metadata if the row has
	// none (NULL or empty), whatever the Build Sequence guard decided for the
	// sets; metadata the row holds — a registration's, or an earlier report —
	// is never overwritten. A nil or empty reported writes nothing. A row
	// that is gone is not recreated.
	ReplaceEdges(ctx context.Context, resource model.Resource, buildSeq int64, sets []EdgeSet, declared []int, reported map[string]string) error

	// RegisterChanges records a batch of changes in one atomic statement:
	// each accepted item's version (or tombstone), stale mark and metadata,
	// and the stale marks of the accepted items' Parents, commit together or
	// not at all. See Registration for what is accepted. The items must name
	// distinct resources; the caller validates that. A registration aborted
	// over a concurrent statement returns an error wrapping
	// ErrRegistrationAborted.
	//
	// Every row it marks — accepted items and Parents — is claimed in the
	// same update when it has no owner or its owner's lease (lease, measured
	// from owner_since) has expired; the outcome reports each claim's token.
	RegisterChanges(ctx context.Context, items []Registration, lease time.Duration) (Registered, error)

	// MarkStale durably records build intent for the given resources. It
	// leaves each row's metadata alone: a resource's metadata is written only
	// by its own registrations and, while it has none, its plans' report
	// (ReplaceEdges), so a mark one resource's change makes on another never
	// gives it the first one's metadata. The build that serves the mark runs
	// with what the row holds when it begins (BuildBegun.Metadata).
	//
	// A lease above zero also claims each marked row that has no live owner,
	// in the same update, and returns the claimed rows; only those may be
	// submitted. A lease of zero marks without claiming and returns nil: the
	// mark hands the work to the sweep and nothing is submitted.
	MarkStale(ctx context.Context, resources []model.Resource, lease time.Duration) ([]Owned, error)
	// BeginBuild bumps the Build Sequence, captures the current stale_seq and
	// takes the build's start from the Change Sequence, and reads the row's
	// metadata. Callers invoke it before the build's fetches. A non-zero
	// token that is the row's owner token renews its lease (owner_since =
	// now()); 0 is a build that owns nothing.
	BeginBuild(ctx context.Context, resource model.Resource, token int64) (BuildBegun, error)
	// BeginDelete is BeginBuild for a notified delete of a tombstone, run
	// before it deletes anything: it bumps the row's Build Sequence, which
	// the delete's Elasticsearch deletes and edge removal carry, and a
	// non-zero token that is the row's owner token renews its lease. It
	// reports the delete superseded only when the row is no longer a
	// tombstone (a recreate) or is gone (a finished delete). A mark that
	// moved stale_seq and kept the row deleted — a Parent mark, a MarkStale,
	// a newer delete — does not supersede it: the delete runs at the bump,
	// and its finish (DeleteResourceIfSeq) sees the moved mark and hands on
	// the follow-up. A superseded delete deletes nothing and finishes as
	// DeleteResourceIfSeq does when stale_seq moved.
	BeginDelete(ctx context.Context, resource model.Resource, token int64) (DeleteBegun, error)
	// RenewOwners renews the lease of every given ownership whose token is
	// still the row's owner token, leaves the others alone, and returns the
	// renewed ones: the ownerships still held. One it didn't renew is lost —
	// another owner claimed the row under a newer mark, or a token-less clear
	// (ClearStale) or a hard delete dropped it — and its holder does nothing
	// for it. A pool task calls it when it is dequeued, the sweep before
	// serving each entry.
	RenewOwners(ctx context.Context, owned []Owned) ([]Owned, error)
	// ReleaseOwners drops every given ownership whose token is still the
	// row's owner token, leaving the stale mark, with no backoff: owned work
	// that did not fail — a shed submission, a failed renewal, the ids a
	// cancellation left unfinished — so the next change claims or the sweep
	// rebuilds. Failed owned work releases through ReleaseFailed.
	ReleaseOwners(ctx context.Context, owned []Owned) error
	// ReleaseFailed finishes an owned build or delete that returned an
	// error: it drops every given ownership whose token is still the row's
	// owner token, as ReleaseOwners does, and backs each of those rows off
	// in the same statement. The row's sweep_attempts becomes n, one more
	// than before, and its sweep_after now() + min(backoff.Base × 2^(n−1),
	// backoff.Max); the exponent stops growing once the delay reaches Max,
	// so no attempt count overflows. The stale mark stays. A row for which a
	// registration of the resource itself (RegisterChanges' own item, not a
	// Parent mark or MarkStale) was accepted since the claim is released but
	// not backed off: that registration reset its backoff, and its change is
	// tried at once. It returns every row it released: a backed-off one with
	// its new attempt count, one registered since the claim with Attempts 0
	// and a zero After. A row whose ownership was lost is neither released
	// nor backed off, and not returned. ListStale skips a row until its
	// sweep_after, and a successful build or delete, or a registration of
	// the resource itself, resets both columns (ADR 0008).
	ReleaseFailed(ctx context.Context, owned []Owned, backoff SweepBackoff) ([]BackedOff, error)
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
	// DeleteResourceIfSeq finishes a delete — a notified one, or a build
	// path's whose plans all returned nil: it hard-deletes the row, tombstone
	// or not, and its ownership with it, when stale_seq still equals
	// staleSeq. When it moved and token is still the owner token, it
	// re-claims the row and returns the follow-up, as FinishOwned does; a
	// token of 0, a delete that owns nothing, re-claims nothing.
	DeleteResourceIfSeq(ctx context.Context, resource model.Resource, staleSeq, token int64) (FollowUp, error)
	// ListStale returns up to limit resources whose stale mark predates
	// before, that have no live owner under lease and whose backoff
	// (ReleaseFailed's sweep_after) has passed, ordered by sweep_after, or
	// by the mark's time for a row that has none, and claims every row it
	// returns in the same statement.
	ListStale(ctx context.Context, before time.Time, limit int, lease time.Duration) ([]StaleResource, error)
	// ListResources returns up to limit live (not tombstoned) rows of
	// resourceType whose id sorts after after, in id order, each with its
	// metadata: a keyset page of the type's rows. An empty after starts at
	// the first id. It claims and locks nothing. The reverse sweep enumerates
	// a type's indexed resources with it (ADR 0012).
	ListResources(ctx context.Context, resourceType, after string, limit int) ([]ListedResource, error)
}

// ListedResource is one row ListResources returns.
type ListedResource struct {
	model.Resource
	// Metadata is the row's metadata; nil when it has none (NULL or empty).
	Metadata map[string]string
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
	// ClearStale for a build that owns nothing, or DeleteResourceIfSeq when
	// its plans all returned nil — is guarded by.
	StaleSeq int64
	// Start is a value of the Change Sequence taken before the build's
	// fetches; the drift check compares against it.
	Start int64
	// Metadata is the row's metadata when the build began: its last
	// registration's, or, on a row no registration gave any, its plans'
	// report (see ReplaceEdges); nil when the row has none (NULL or empty).
	// An owned build fetches with it, so a build that waited in the pool
	// queue or the sweep runs with metadata registered meanwhile.
	Metadata map[string]string
}

// DeleteBegun is what BeginDelete returns for one tombstone.
type DeleteBegun struct {
	// BuildIdx is the bumped Build Sequence, the version of the delete's
	// Elasticsearch deletes and the bound of its edge removal; 0 when
	// Superseded.
	BuildIdx int64
	// Superseded reports that the delete must not run: a recreate or a
	// finished delete got to the row first. A newer mark that kept the row
	// deleted does not supersede it.
	Superseded bool
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

// SweepBackoff is how long the StaleSweep leaves a resource whose owned
// build or delete failed: Base after its first failure in a row, doubling
// with each further one, never more than Max (ADR 0008).
type SweepBackoff struct {
	Base time.Duration
	Max  time.Duration
}

// BackedOff is a row ReleaseFailed backed off.
type BackedOff struct {
	model.Resource
	// Attempts is the row's failures in a row, this one included: its
	// sweep_attempts after the release. 0 means the row was released without
	// a backoff: a change of its own was registered since the claim.
	Attempts int
	// After is when the sweep may serve the row again: its sweep_after. Zero
	// when Attempts is 0.
	After time.Time
}

// FollowUp is what finishing an owned build or delete hands on.
type FollowUp struct {
	// Token is the owner token re-claimed for the follow-up, the row's
	// stale_seq at the re-claim; 0 means no follow-up is due. A follow-up
	// delete is guarded by it (DeleteResourceIfSeq's staleSeq).
	Token int64
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
	// Metadata is stored on the item's own row with its mark, replacing
	// what the row holds. It is the only mark that writes metadata: the
	// item's Parents are marked with theirs left alone.
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

// MarkedParent is a Parent RegisterChanges marked stale. The mark leaves
// the Parent's metadata alone; its build runs with the Parent's own.
type MarkedParent struct {
	model.Resource
	// Token is the owner token the mark claimed; 0 when the row has a live
	// owner, so nothing is submitted for it.
	Token int64
}
