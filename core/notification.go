package core

// ChangeKind describes the type of change that occurred.
type ChangeKind int

const (
	ChangeCreated ChangeKind = iota
	ChangeUpdated
	ChangeDeleted
)

func (k ChangeKind) String() string {
	switch k {
	case ChangeCreated:
		return "created"
	case ChangeUpdated:
		return "updated"
	case ChangeDeleted:
		return "deleted"
	default:
		return "unknown"
	}
}

// Notification represents a typed change notification from a source service.
// It replaces the old generic ChangeEvent (Create/Update/Delete/AddRelation/RemoveRelation/SetRelations).
//
// The notification says "resource X changed" — the indexer then determines which
// root search documents are affected and rebuilds them from authoritative source data.
type Notification struct {
	// ResourceType is the type of the resource that changed (e.g. "a", "b", "c").
	ResourceType string

	// ResourceID is the unique identifier of the resource that changed.
	ResourceID string

	// Kind describes what happened: created, updated, or deleted.
	Kind ChangeKind

	// Metadata contains arbitrary caller-provided context propagated through
	// rebuild processing and provider fetches.
	Metadata map[string]string

	// Version is the resource's version at the source, monotonic per
	// resource. It serves stale rejection only and is compared only with the
	// version stored for the same resource: a non-zero Version not strictly
	// greater than the stored one is rejected as stale. Zero means the source
	// does not track versions — the notification is always accepted. Ignored
	// for deletes. Nothing else compares it; the drift check runs on Laika's
	// own Change Sequence.
	Version int64
}
