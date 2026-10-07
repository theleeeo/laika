package core

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// recordingStore records the order of Store calls and keeps the stale-mark
// and ownership state of core/store.go in memory: one row per resource with
// its stale_seq, mark, owner token, change_seq, metadata and tombstone,
// created by a mark or a BeginBuild. A non-zero owner
// is a live lease — leases never expire here — and every mark takes the next
// value of one global counter as its stale_seq; a claim makes the row's new
// stale_seq its owner token. Each item RegisterChanges accepts takes the next
// value of the Change Sequence as its change_seq, and BeginBuild's start is
// the Change Sequence's value at the begin, so AnyChangedSince reports a
// change accepted after a build began. A row's metadata follows the
// contract: only an accepted registration writes it, for its own row, and
// ReplaceEdges stores a report on a row that has none; marks leave it.
type recordingStore struct {
	mu    sync.Mutex
	calls []string
	// drift is one-shot: report drift on the first AnyChangedSince, whatever
	// the change_seqs. driftErr fails every AnyChangedSince.
	drift    atomic.Bool
	driftErr error
	// start is added to every BeginBuild's BuildBegun.Start; checks records
	// every AnyChangedSince batch. changes is the Change Sequence's last
	// value handed out.
	start   int64
	checks  [][]ChangeCheck
	changes int64
	// onCheck, when set, runs at the start of every AnyChangedSince, outside
	// the lock and before it records the call: a test can register a change
	// after a build stored its edges and before its drift check and re-mark.
	onCheck func([]ChangeCheck)
	// marked, when set, receives (non-blocking) after every MarkStale and
	// RegisterChanges: the point past which a WaitForSlot registration may wait.
	marked chan struct{}

	// RegisterChanges serves these: parents are marked as Parents of every
	// batch (only their Resource is read); parentsOf maps a child to the
	// Parents each accepted registration of it marks; stale resources are
	// rejected; registerErr fails the call. registrations records every batch
	// it received.
	parents       []MarkedParent
	parentsOf     map[model.Resource][]model.Resource
	stale         map[model.Resource]bool
	registerErr   error
	registrations [][]Registration

	// beginErr fails every BeginBuild, beginDeleteErr every BeginDelete,
	// removeErr every RemoveResource, finishErr every FinishOwned and
	// deleteErr every DeleteResourceIfSeq.
	beginErr       error
	beginDeleteErr error
	removeErr      error
	finishErr      error
	deleteErr      error
	// onBeginDelete, when set, runs at the start of every BeginDelete,
	// outside the lock and before it records the call: a test can register a
	// change, or hand the row to another owner, between a delete's renewal
	// and its Build Sequence bump.
	onBeginDelete func(model.Resource)
	// onRemove, when set, runs inside every RemoveResource, outside the lock
	// and before it returns: a test can register a change while a delete is
	// in flight.
	onRemove func(model.Resource)
	// onRenew, when set, runs at the start of every RenewOwners, outside the
	// lock, with the ownerships it was given: a test can hand a row to
	// another owner just before its holder renews it. renewErr fails every
	// RenewOwners as a done ctx does: it records RenewOwnersFailed per entry
	// and returns the error.
	onRenew  func([]Owned)
	renewErr error
	// replaced records every ReplaceEdges call, in order; replaceErr fails
	// every ReplaceEdges, which then stores no metadata. onReplace, when set,
	// runs inside every ReplaceEdges, outside the lock and before it stores
	// anything: a test can see what else had happened by the time a build
	// stored its edges.
	replaced   []edgeReplace
	replaceErr error
	onReplace  func(edgeReplace)
	// markErr fails every MarkStale after recording it.
	markErr error
	// releaseFailedErr fails every ReleaseFailed as a done ctx does;
	// failedBackoffs records the backoff of every ReleaseFailed that ran.
	releaseFailedErr error
	failedBackoffs   []SweepBackoff

	rows     map[model.Resource]*memRow
	seq      int64 // the last stale_seq handed out
	buildIdx int64
	// followUps records every non-zero FollowUp FinishOwned and
	// DeleteResourceIfSeq returned, per resource, in order.
	followUps map[model.Resource][]FollowUp
}

// edgeReplace is one recorded Store.ReplaceEdges call.
type edgeReplace struct {
	resource model.Resource
	buildSeq int64
	sets     []EdgeSet
	declared []int
	// reported is the metadata the build passed as its plans' report.
	reported map[string]string
}

// memRow is one resource row of recordingStore.
type memRow struct {
	staleSeq int64
	stale    bool
	// owner is the owner token; 0 = no owner.
	owner int64
	// changeSeq is the Change Sequence value of the last accepted
	// registration of the row; 0 for none.
	changeSeq int64
	metadata  map[string]string
	deleted   bool
	// attempts and after are the row's sweep_attempts and sweep_after, as
	// ReleaseFailed leaves them; 0 and the zero time for never failed.
	attempts int
	after    time.Time
}

func (s *recordingStore) signalMarked() {
	if s.marked == nil {
		return
	}
	select {
	case s.marked <- struct{}{}:
	default:
	}
}

func (s *recordingStore) count(prefix string) int {
	n := 0
	for _, c := range s.callsSnapshot() {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

func (s *recordingStore) record(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordLocked(format, args...)
}

func (s *recordingStore) recordLocked(format string, args ...any) {
	s.calls = append(s.calls, fmt.Sprintf(format, args...))
}

func (s *recordingStore) callsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *recordingStore) checksSnapshot() [][]ChangeCheck {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]ChangeCheck(nil), s.checks...)
}

func (s *recordingStore) indexOf(prefix string) int {
	for i, c := range s.callsSnapshot() {
		if len(c) >= len(prefix) && c[:len(prefix)] == prefix {
			return i
		}
	}
	return -1
}

// row returns a copy of res's row and whether it exists.
func (s *recordingStore) row(res model.Resource) (memRow, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[res]
	if !ok {
		return memRow{}, false
	}
	out := *r
	out.metadata = maps.Clone(r.metadata)
	return out, true
}

// owner is res's owner token, 0 for none.
func (s *recordingStore) owner(res model.Resource) int64 {
	r, _ := s.row(res)
	return r.owner
}

// followUpsOf is every non-zero FollowUp returned for res, in order.
func (s *recordingStore) followUpsOf(res model.Resource) []FollowUp {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]FollowUp(nil), s.followUps[res]...)
}

// seedMetadata gives res's row metadata md, creating the row unmarked if
// absent, as an earlier registration whose build finished would leave it.
func (s *recordingStore) seedMetadata(res model.Resource, md map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rows == nil {
		s.rows = make(map[model.Resource]*memRow)
	}
	r, ok := s.rows[res]
	if !ok {
		r = &memRow{}
		s.rows[res] = r
	}
	r.metadata = maps.Clone(md)
}

// markLocked marks res stale under a new stale_seq, leaving its metadata;
// deleted, when non-nil, sets the tombstone. It returns the row.
func (s *recordingStore) markLocked(res model.Resource, deleted *bool) *memRow {
	if s.rows == nil {
		s.rows = make(map[model.Resource]*memRow)
	}
	r, ok := s.rows[res]
	if !ok {
		r = &memRow{}
		s.rows[res] = r
	}
	s.seq++
	r.staleSeq = s.seq
	r.stale = true
	if deleted != nil {
		r.deleted = *deleted
	}
	return r
}

// claimLocked claims an unowned row, returning the token; 0 when owned.
func claimLocked(r *memRow) int64 {
	if r.owner != 0 {
		return 0
	}
	r.owner = r.staleSeq
	return r.owner
}

// followUpLocked is the shared moved-seq branch of FinishOwned and
// DeleteResourceIfSeq: re-claim for token, if it still owns the row.
func (s *recordingStore) followUpLocked(res model.Resource, r *memRow, token int64) FollowUp {
	if token == 0 || r.owner != token {
		return FollowUp{}
	}
	r.owner = r.staleSeq
	fu := FollowUp{Token: r.owner, Deleted: r.deleted}
	if s.followUps == nil {
		s.followUps = make(map[model.Resource][]FollowUp)
	}
	s.followUps[res] = append(s.followUps[res], fu)
	return fu
}

// MarkStale fails on a done ctx, as a real store's query would, and with
// markErr, marking nothing. It leaves the rows' metadata.
func (s *recordingStore) MarkStale(ctx context.Context, rs []model.Resource, lease time.Duration) ([]Owned, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.recordLocked("MarkStale:%d", len(rs))
	if s.markErr != nil {
		s.mu.Unlock()
		return nil, s.markErr
	}
	var owned []Owned
	for _, res := range rs {
		r := s.markLocked(res, nil)
		if lease > 0 {
			if tok := claimLocked(r); tok != 0 {
				owned = append(owned, Owned{Resource: res, Token: tok})
			}
		}
	}
	s.mu.Unlock()
	s.signalMarked()
	return owned, nil
}

func (s *recordingStore) BeginBuild(_ context.Context, r model.Resource, token int64) (BuildBegun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordLocked("BeginBuild:%s/%s:%d", r.Type, r.Id, token)
	if s.beginErr != nil {
		return BuildBegun{}, s.beginErr
	}
	s.buildIdx++
	// Like the real store, BeginBuild inserts a row, unmarked, for an id
	// without one.
	if s.rows == nil {
		s.rows = make(map[model.Resource]*memRow)
	}
	row, ok := s.rows[r]
	if !ok {
		row = &memRow{}
		s.rows[r] = row
	}
	var md map[string]string
	if len(row.metadata) > 0 {
		md = maps.Clone(row.metadata)
	}
	return BuildBegun{BuildIdx: s.buildIdx, StaleSeq: row.staleSeq, Start: s.start + s.changes, Metadata: md}, nil
}
func (s *recordingStore) NextChangeSeq(context.Context) (int64, error) {
	s.record("NextChangeSeq")
	return 0, nil
}

// AnyChangedSince reports drift once when drift is set, and otherwise when a
// checked row's change_seq exceeds its check's Start.
func (s *recordingStore) AnyChangedSince(_ context.Context, checks []ChangeCheck) (bool, error) {
	if s.onCheck != nil {
		s.onCheck(checks)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordLocked("AnyChangedSince:%d", len(checks))
	s.checks = append(s.checks, append([]ChangeCheck(nil), checks...))
	changed := s.drift.Swap(false)
	if s.driftErr != nil {
		return false, s.driftErr
	}
	for _, c := range checks {
		if row, ok := s.rows[c.Resource]; ok && row.changeSeq > c.Start {
			changed = true
		}
	}
	return changed, nil
}
func (s *recordingStore) ClearStale(_ context.Context, r model.Resource, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordLocked("ClearStale:%s/%s:%d", r.Type, r.Id, seq)
	if row, ok := s.rows[r]; ok && row.staleSeq == seq {
		row.stale = false
		row.owner = 0
	}
	return nil
}

// FinishOwned fails on a done ctx, as a real store's query would, and then
// records FinishOwnedFailed instead.
func (s *recordingStore) FinishOwned(ctx context.Context, r model.Resource, staleSeq, token int64) (FollowUp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		s.recordLocked("FinishOwnedFailed:%s/%s:%d:%d", r.Type, r.Id, staleSeq, token)
		return FollowUp{}, err
	}
	s.recordLocked("FinishOwned:%s/%s:%d:%d", r.Type, r.Id, staleSeq, token)
	if s.finishErr != nil {
		return FollowUp{}, s.finishErr
	}
	row, ok := s.rows[r]
	if !ok {
		return FollowUp{}, nil
	}
	if row.staleSeq == staleSeq {
		row.stale = false
		row.owner = 0
		return FollowUp{}, nil
	}
	return s.followUpLocked(r, row, token), nil
}
func (s *recordingStore) DeleteResourceIfSeq(_ context.Context, r model.Resource, staleSeq, token int64) (FollowUp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordLocked("DeleteResourceIfSeq:%s/%s:%d:%d", r.Type, r.Id, staleSeq, token)
	if s.deleteErr != nil {
		return FollowUp{}, s.deleteErr
	}
	row, ok := s.rows[r]
	if !ok {
		return FollowUp{}, nil
	}
	if row.staleSeq == staleSeq {
		delete(s.rows, r)
		return FollowUp{}, nil
	}
	return s.followUpLocked(r, row, token), nil
}
func (s *recordingStore) ListStale(context.Context, time.Time, int, time.Duration) ([]StaleResource, error) {
	return nil, nil
}

// ListResources follows the contract over the in-memory rows: up to limit
// live rows of resourceType whose id sorts after after, in id order, each
// with a copy of its metadata (nil when it has none); tombstones are skipped.
// It records the call.
func (s *recordingStore) ListResources(_ context.Context, resourceType, after string, limit int) ([]ListedResource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordLocked("ListResources:%s:%s:%d", resourceType, after, limit)
	var out []ListedResource
	for res, r := range s.rows {
		if res.Type != resourceType || res.Id <= after || r.deleted {
			continue
		}
		var md map[string]string
		if len(r.metadata) > 0 {
			md = maps.Clone(r.metadata)
		}
		out = append(out, ListedResource{Resource: res, Metadata: md})
	}
	slices.SortFunc(out, func(a, b ListedResource) int { return strings.Compare(a.Id, b.Id) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// RenewOwners and ReleaseOwners fail on a done ctx, as a real store's query
// would, and then record RenewOwnersFailed / ReleaseOwnersFailed per entry
// instead — so a release made on a detached ctx is observable. RenewOwners
// returns the given ownerships whose token is still the row's owner token;
// renewErr fails it as a done ctx does.
func (s *recordingStore) RenewOwners(ctx context.Context, owned []Owned) ([]Owned, error) {
	if s.onRenew != nil {
		s.onRenew(owned)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := ctx.Err()
	if err == nil {
		err = s.renewErr
	}
	var held []Owned
	for _, o := range owned {
		if err != nil {
			s.recordLocked("RenewOwnersFailed:%s/%s:%d", o.Type, o.Id, o.Token)
			continue
		}
		s.recordLocked("RenewOwners:%s/%s:%d", o.Type, o.Id, o.Token)
		if row, ok := s.rows[o.Resource]; ok && o.Token != 0 && row.owner == o.Token {
			held = append(held, o)
		}
	}
	if err != nil {
		return nil, err
	}
	return held, nil
}
func (s *recordingStore) ReleaseOwners(ctx context.Context, owned []Owned) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := ctx.Err()
	for _, o := range owned {
		if err != nil {
			s.recordLocked("ReleaseOwnersFailed:%s/%s:%d", o.Type, o.Id, o.Token)
			continue
		}
		s.recordLocked("ReleaseOwners:%s/%s:%d", o.Type, o.Id, o.Token)
		if row, ok := s.rows[o.Resource]; ok && row.owner == o.Token {
			row.owner = 0
		}
	}
	return err
}

// ReleaseFailed fails on a done ctx, as a real store's query would, and
// with releaseFailedErr, and then records ReleaseFailedFailed per entry,
// releasing nothing. Otherwise it records the call per entry and the backoff
// it was given (failedBackoffs), and drops each ownership whose token is
// still the row's owner token, as ReleaseOwners does, counting the row's
// attempts up and setting its after as the contract says. It does not model
// ListStale's skip or the resets.
func (s *recordingStore) ReleaseFailed(ctx context.Context, owned []Owned, backoff SweepBackoff) ([]BackedOff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := ctx.Err()
	if err == nil {
		err = s.releaseFailedErr
	}
	if err != nil {
		for _, o := range owned {
			s.recordLocked("ReleaseFailedFailed:%s/%s:%d", o.Type, o.Id, o.Token)
		}
		return nil, err
	}
	s.failedBackoffs = append(s.failedBackoffs, backoff)
	var out []BackedOff
	for _, o := range owned {
		s.recordLocked("ReleaseFailed:%s/%s:%d", o.Type, o.Id, o.Token)
		if row, ok := s.rows[o.Resource]; ok && row.owner == o.Token {
			row.owner = 0
			row.attempts++
			row.after = time.Now().Add(backoffDelay(backoff, row.attempts))
			out = append(out, BackedOff{Resource: o.Resource, Attempts: row.attempts, After: row.after})
		}
	}
	return out, nil
}

// backoffDelay is min(b.Base × 2^(n−1), b.Max): the doubling stops once it
// would reach the cap, so no attempt count overflows it.
func backoffDelay(b SweepBackoff, n int) time.Duration {
	delay := b.Base
	for i := 1; i < n && delay < b.Max; i++ {
		if delay > b.Max/2 {
			return b.Max
		}
		delay *= 2
	}
	return min(delay, b.Max)
}

// failedBackoffsSnapshot is every backoff ReleaseFailed was given, in order.
func (s *recordingStore) failedBackoffsSnapshot() []SweepBackoff {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SweepBackoff(nil), s.failedBackoffs...)
}

// seedAttempts gives res's row n failed attempts in a row, as earlier failed
// releases would leave it; the row must exist.
func (s *recordingStore) seedAttempts(res model.Resource, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[res].attempts = n
}

// ReplaceEdges records the call, runs onReplace, and then fails with
// replaceErr, writing nothing. Otherwise it follows the contract's metadata
// rule on the in-memory row: a non-empty reported is stored only on a row
// that exists and has no metadata. It never creates a row; edges are not
// modelled.
func (s *recordingStore) ReplaceEdges(_ context.Context, r model.Resource, buildSeq int64, sets []EdgeSet, declared []int, reported map[string]string) error {
	call := edgeReplace{resource: r, buildSeq: buildSeq, sets: sets, declared: declared, reported: maps.Clone(reported)}
	s.mu.Lock()
	s.recordLocked("ReplaceEdges:%s/%s:%d", r.Type, r.Id, buildSeq)
	s.replaced = append(s.replaced, call)
	s.mu.Unlock()
	if s.onReplace != nil {
		s.onReplace(call)
	}
	if s.replaceErr != nil {
		return s.replaceErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if row, ok := s.rows[r]; ok && len(row.metadata) == 0 && len(reported) > 0 {
		row.metadata = maps.Clone(reported)
	}
	return nil
}

func (s *recordingStore) replacedSnapshot() []edgeReplace {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]edgeReplace(nil), s.replaced...)
}
func (s *recordingStore) GetChildResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}
func (s *recordingStore) GetParentResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}

// BeginDelete follows the contract: the delete is superseded when the row is
// gone or no longer a tombstone, whatever its stale_seq; otherwise it bumps
// the Build Sequence. Its lease renewal is a no-op here, where leases never
// expire.
func (s *recordingStore) BeginDelete(_ context.Context, r model.Resource, token int64) (DeleteBegun, error) {
	if s.onBeginDelete != nil {
		s.onBeginDelete(r)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordLocked("BeginDelete:%s/%s:%d", r.Type, r.Id, token)
	if s.beginDeleteErr != nil {
		return DeleteBegun{}, s.beginDeleteErr
	}
	row, ok := s.rows[r]
	if !ok || !row.deleted {
		return DeleteBegun{Superseded: true}, nil
	}
	s.buildIdx++
	return DeleteBegun{BuildIdx: s.buildIdx}, nil
}

func (s *recordingStore) RemoveResource(_ context.Context, r model.Resource, buildSeq int64) error {
	s.record("RemoveResource:%s/%s:%d", r.Type, r.Id, buildSeq)
	if s.onRemove != nil {
		s.onRemove(r)
	}
	return s.removeErr
}

// RegisterChanges marks and claims as the real statement does: each accepted
// item's row, which takes the item's metadata and the next change_seq, then
// the Parents of the batch — the canned parents, and every accepted item's
// parentsOf — excluding the batch's accepted items, each once, with their
// metadata left alone.
func (s *recordingStore) RegisterChanges(_ context.Context, items []Registration, _ time.Duration) (Registered, error) {
	s.mu.Lock()
	s.recordLocked("RegisterChanges:%d", len(items))
	s.registrations = append(s.registrations, items)
	if s.registerErr != nil {
		s.mu.Unlock()
		return Registered{}, s.registerErr
	}
	out := Registered{Items: make([]RegisteredItem, len(items))}
	accepted := make(map[model.Resource]bool, len(items))
	var parentOrder []model.Resource
	seenParent := make(map[model.Resource]bool)
	addParent := func(p model.Resource) {
		if !seenParent[p] {
			seenParent[p] = true
			parentOrder = append(parentOrder, p)
		}
	}
	for _, p := range s.parents {
		addParent(p.Resource)
	}
	for i, it := range items {
		if s.stale[it.Resource] {
			continue
		}
		accepted[it.Resource] = true
		r := s.markLocked(it.Resource, &it.Deleted)
		r.metadata = maps.Clone(it.Metadata)
		s.changes++
		r.changeSeq = s.changes
		out.Items[i] = RegisteredItem{Accepted: true, StaleSeq: r.staleSeq, Token: claimLocked(r)}
		for _, p := range s.parentsOf[it.Resource] {
			addParent(p)
		}
	}
	for _, p := range parentOrder {
		if accepted[p] {
			continue
		}
		r := s.markLocked(p, nil)
		out.Parents = append(out.Parents, MarkedParent{Resource: p, Token: claimLocked(r)})
	}
	s.mu.Unlock()
	s.signalMarked()
	return out, nil
}

func newHotPathIndexer(st Store, poolSize, queueSize int) *Indexer {
	doc := projection.BuildDoc{
		Root: model.Resource{Type: "product", Id: "1"},
		Doc:  map[string]any{"fields": map[string]any{"title": "t"}},
	}
	return mustNew(Config{
		Resources: testResources(),
		Plans: map[string][]projection.Plan{
			"product": {{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{doc}}}},
		},
		ES:        &fakeBackend{},
		Store:     st,
		PoolSize:  poolSize,
		QueueSize: queueSize,
	})
}

func TestRegisterChange_MarksStaleBeforeBuilding_ThenClears(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, 4)

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	// The registration's mark is stale_seq 1 and claims the row under token 1;
	// the owned build finishes with FinishOwned, which clears mark and owner.
	mark, begin, clear := st.indexOf("RegisterChanges"), st.indexOf("BeginBuild:product/1:1"), st.indexOf("FinishOwned:product/1:1:1")
	if mark == -1 || begin == -1 || clear == -1 {
		t.Fatalf("missing calls: %v", st.callsSnapshot())
	}
	if mark > begin {
		t.Fatalf("the registration's mark must land before the build starts: %v", st.callsSnapshot())
	}
	if clear < begin {
		t.Fatalf("FinishOwned must follow the build: %v", st.callsSnapshot())
	}
	if r, _ := st.row(product("1")); r.stale || r.owner != 0 {
		t.Fatalf("the finished build must clear the mark and the ownership: %+v", r)
	}
}

func TestRegisterChange_PoolSaturated_ShedsButReturnsSuccess(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 1, 1)

	// Occupy the only worker, then fill the queue's single slot.
	block := make(chan struct{})
	started := make(chan struct{})
	if !idx.pool.trySubmit(func(context.Context) { close(started); <-block }) {
		t.Fatal("failed to occupy pool")
	}
	<-started
	if !idx.pool.trySubmit(func(context.Context) {}) {
		t.Fatal("failed to fill queue")
	}

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if err != nil {
		t.Fatalf("shed must not surface as an RPC error: %v", err)
	}
	if st.indexOf("RegisterChanges") == -1 {
		t.Fatal("the stale mark must land even when the build is shed")
	}
	if st.indexOf("BeginBuild") != -1 {
		t.Fatal("no build may start on a saturated pool")
	}
	close(block)
	_ = idx.WaitForIdle(t.Context())
}

func TestRegisterChange_Delete_TombstonesAndRunsInlineDelete(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, 4)

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeDeleted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if st.indexOf("RegisterChanges") == -1 {
		t.Fatalf("delete must tombstone first: %v", st.callsSnapshot())
	}
	// The tombstone is stale_seq 1, claimed under token 1.
	if st.indexOf("DeleteResourceIfSeq:product/1:1:1") == -1 {
		t.Fatalf("inline delete must finish the tombstone with the captured seq and its token: %v", st.callsSnapshot())
	}
}

func TestBuildOne_Drift_RemarksStale_SoGuardedClearIsNoop(t *testing.T) {
	st := &recordingStore{}
	st.drift.Store(true) // first drift check reports drift
	idx := newHotPathIndexer(st, 2, 4)

	// Plan must emit relations for the drift check to run.
	doc := projection.BuildDoc{
		Root: model.Resource{Type: "product", Id: "1"},
		Doc:  map[string]any{"fields": map[string]any{"title": "t"}},
		Relations: []model.Resource{
			{Type: "product", Id: "child"},
		},
	}
	idx.plans = map[string][]projection.Plan{
		"product": {{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{doc}}}},
	}

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Drift must re-mark (MarkStale for the drift re-schedule, after the
	// notification's RegisterChanges) and trigger a second build.
	calls := st.callsSnapshot()
	marks, begins := st.count("MarkStale"), st.count("BeginBuild")
	if marks < 1 || begins < 2 {
		t.Fatalf("drift must re-mark and re-build (marks=%d begins=%d): %v", marks, begins, calls)
	}
}

func productDocWith(id string, children ...string) projection.BuildDoc {
	rels := make([]model.Resource, len(children))
	for i, c := range children {
		rels[i] = product(c)
	}
	return projection.BuildDoc{
		Root:      product(id),
		Doc:       map[string]any{"fields": map[string]any{"title": "t"}},
		Relations: rels,
	}
}

// The drift check is one AnyChangedSince over every child the plans fetched,
// across every plan, each measured from the build's start; the root is not
// checked.
func TestBuild_DriftCheck_ChecksEveryFetchedChildFromTheBuildStart(t *testing.T) {
	st := &recordingStore{start: 77}
	idx := newHotPathIndexer(st, 2, 4)
	idx.plans = map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1", "c2")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c3")}}},
	}}

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	checks := st.checksSnapshot()
	if len(checks) != 1 {
		t.Fatalf("one drift check per build, got %d: %v", len(checks), st.callsSnapshot())
	}
	want := []ChangeCheck{{product("c1"), 77}, {product("c2"), 77}, {product("c3"), 77}}
	if fmt.Sprint(checks[0]) != fmt.Sprint(want) {
		t.Fatalf("checks %v, want %v", checks[0], want)
	}
}

// A hit re-schedules the root mark-first: the drift's MarkStale lands before
// the root's second BeginBuild, and the second build runs from its own start.
func TestBuild_DriftHit_ReschedulesTheRootMarkFirst(t *testing.T) {
	st := &rebuildRecordingStore{driftChildren: map[string]bool{"c1": true}}
	st.driftBudget.Store(1)
	idx := newHotPathIndexer(st, 2, 4)
	idx.plans = map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1")}}},
	}}

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	calls := st.callsSnapshot()
	var check, mark, begins []int
	for i, c := range calls {
		switch c {
		case "AnyChangedSince:1":
			check = append(check, i)
		case "MarkStale:product/1":
			mark = append(mark, i)
		case "BeginBuild:product/1":
			begins = append(begins, i)
		}
	}
	if len(check) != 2 || len(mark) != 1 || len(begins) != 2 {
		t.Fatalf("a hit must re-mark once and re-build (checks=%d marks=%d begins=%d): %v",
			len(check), len(mark), len(begins), calls)
	}
	if check[0] >= mark[0] || mark[0] >= begins[1] {
		t.Fatalf("the re-mark must follow the hit and precede the second build: %v", calls)
	}
	checks := st.checksSnapshot()
	if checks[0][0].Start != 101 || checks[1][0].Start != 102 {
		t.Fatalf("each build checks from its own start, got %v", checks)
	}
}

func TestBuild_DriftMiss_SchedulesNothing(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, 4)
	idx.plans = map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1")}}},
	}}

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if st.count("AnyChangedSince") != 1 || st.count("MarkStale") != 0 || st.count("BeginBuild") != 1 {
		t.Fatalf("a miss is checked once and schedules nothing: %v", st.callsSnapshot())
	}
}

func TestBuild_NoRelations_MakesNoDriftCheck(t *testing.T) {
	st := &recordingStore{}
	st.drift.Store(true)
	idx := newHotPathIndexer(st, 2, 4)

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if st.count("AnyChangedSince") != 0 || st.count("MarkStale") != 0 {
		t.Fatalf("a build without children has nothing to check: %v", st.callsSnapshot())
	}
}

// A failed drift query fails the build: the stale mark survives for the sweep
// and nothing is re-scheduled.
func TestBuild_DriftQueryError_LeavesTheMarkAndSchedulesNothing(t *testing.T) {
	st := &recordingStore{driftErr: errors.New("db down")}
	idx := newHotPathIndexer(st, 2, 4)
	idx.plans = map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1")}}},
	}}

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	calls := st.callsSnapshot()
	if st.count("AnyChangedSince") != 1 {
		t.Fatalf("the drift query must run: %v", calls)
	}
	if st.count("ClearStale") != 0 {
		t.Fatalf("a failed drift check must not clear the mark: %v", calls)
	}
	if st.count("MarkStale") != 0 || st.count("BeginBuild") != 1 {
		t.Fatalf("a failed drift check re-schedules nothing: %v", calls)
	}
}

// recordingExecuter serves a doc for every requested id and records each
// request, so a test sees which ids were built with which metadata.
type recordingExecuter struct {
	// relations gives the doc of each id it names those Relations (the
	// root's children, drift-checked by its build); parents gives it those
	// derived Parents (ADR 0006).
	relations map[string][]model.Resource
	parents   map[string][]model.Resource
	mu        sync.Mutex
	reqs      []projection.BuildRequest
	// arrived and proceed, when set, park every Execute — or, with parkIDs
	// set, only those of the ids it names: it sends its id on arrived and
	// returns once it receives from proceed (one send releases one parked
	// Execute, a close releases all).
	arrived chan string
	proceed chan struct{}
	parkIDs map[string]bool
	// failIDs fails the Execute of the ids it names.
	failIDs map[string]bool
}

func (e *recordingExecuter) Execute(_ context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	e.mu.Lock()
	e.reqs = append(e.reqs, req)
	e.mu.Unlock()
	if e.arrived != nil && (e.parkIDs == nil || e.parkIDs[req.ResourceID]) {
		e.arrived <- req.ResourceID
		<-e.proceed
	}
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
	if e.failIDs[req.ResourceID] {
		ch <- aggregation.ExecutionResult[projection.BuildDoc]{Err: errors.New("plan failed")}
		close(ch)
		return ch
	}
	ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: []projection.BuildDoc{{
		Root:      model.Resource{Type: req.ResourceType, Id: req.ResourceID},
		Doc:       map[string]any{"fields": map[string]any{"title": "t"}},
		Relations: e.relations[req.ResourceID],
		Parents:   e.parents[req.ResourceID],
	}}}
	close(ch)
	return ch
}

// requestsFor is every request for id, in arrival order.
func (e *recordingExecuter) requestsFor(id string) []projection.BuildRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []projection.BuildRequest
	for _, r := range e.reqs {
		if r.ResourceID == id {
			out = append(out, r)
		}
	}
	return out
}

// metadataByID is the metadata each built id ran with.
func (e *recordingExecuter) metadataByID() map[string]map[string]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]map[string]string, len(e.reqs))
	for _, r := range e.reqs {
		out[r.ResourceID] = r.Metadata
	}
	return out
}

func newRecordingIndexer(st Store, poolSize, queueSize int) (*Indexer, *recordingExecuter) {
	ex := &recordingExecuter{}
	return mustNew(Config{
		Resources: testResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}}},
		ES:        &fakeBackend{},
		Store:     st,
		PoolSize:  poolSize,
		QueueSize: queueSize,
	}), ex
}

func product(id string) model.Resource { return model.Resource{Type: "product", Id: id} }

func TestRegisterChanges_InvalidBatch_FailsBeforeAnyStoreCall(t *testing.T) {
	cases := map[string]struct {
		ns   []Notification
		want func(error) bool
	}{
		"unknown resource": {
			ns: []Notification{
				{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated},
				{ResourceType: "nope", ResourceID: "1", Kind: ChangeUpdated},
			},
			want: func(err error) bool { return errors.Is(err, ErrUnknownResource) },
		},
		"duplicate resource": {
			ns: []Notification{
				{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1},
				{ResourceType: "product", ResourceID: "2", Kind: ChangeUpdated},
				{ResourceType: "product", ResourceID: "1", Kind: ChangeDeleted},
			},
			want: func(err error) bool {
				_, ok := errors.AsType[*InvalidArgumentError](err)
				return ok
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st := &recordingStore{}
			idx := newHotPathIndexer(st, 2, 4)

			statuses, err := idx.RegisterChanges(t.Context(), tc.ns)
			if !tc.want(err) {
				t.Fatalf("unexpected error %v", err)
			}
			if statuses != nil {
				t.Fatalf("a failed call returns no statuses, got %v", statuses)
			}
			if calls := st.callsSnapshot(); len(calls) != 0 {
				t.Fatalf("an invalid batch must not reach the store: %v", calls)
			}
		})
	}
}

func TestRegisterChanges_StoreError_ReturnsErrorAndSubmitsNothing(t *testing.T) {
	st := &recordingStore{registerErr: errors.New("boom")}
	idx := newHotPathIndexer(st, 2, 4)

	_, err := idx.RegisterChanges(t.Context(), []Notification{
		{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated},
		{ResourceType: "product", ResourceID: "2", Kind: ChangeDeleted},
	})
	if err == nil {
		t.Fatal("a failed statement must fail the call")
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if st.indexOf("BeginBuild") != -1 || st.indexOf("DeleteResourceIfSeq") != -1 {
		t.Fatalf("nothing may be submitted when nothing committed: %v", st.callsSnapshot())
	}
}

func TestRegisterChanges_SubmitsEachAcceptedItemAndParentAfterTheStatement(t *testing.T) {
	st := &recordingStore{
		stale:   map[model.Resource]bool{product("2"): true},
		parents: []MarkedParent{{Resource: product("p")}},
	}
	// The Parent holds metadata of its own, which its mark leaves.
	st.seedMetadata(product("p"), map[string]string{"m": "p"})
	idx, ex := newRecordingIndexer(st, 4, 8)

	ns := []Notification{
		{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 5, Metadata: map[string]string{"m": "1"}},
		{ResourceType: "product", ResourceID: "2", Kind: ChangeUpdated, Version: 3, Metadata: map[string]string{"m": "2"}},
		{ResourceType: "product", ResourceID: "3", Kind: ChangeDeleted, Metadata: map[string]string{"m": "3"}},
		{ResourceType: "product", ResourceID: "4", Kind: ChangeCreated, Metadata: map[string]string{"m": "4"}},
	}
	statuses, err := idx.RegisterChanges(t.Context(), ns)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	want := []RegisterStatus{RegisterAccepted, RegisterStale, RegisterAccepted, RegisterAccepted}
	if fmt.Sprint(statuses) != fmt.Sprint(want) {
		t.Fatalf("statuses %v, want %v", statuses, want)
	}

	if len(st.registrations) != 1 {
		t.Fatalf("one statement per batch, got %d", len(st.registrations))
	}
	got := st.registrations[0]
	wantItems := []Registration{
		{Resource: product("1"), Version: 5, Metadata: ns[0].Metadata},
		{Resource: product("2"), Version: 3, Metadata: ns[1].Metadata},
		{Resource: product("3"), Deleted: true, Metadata: ns[2].Metadata},
		{Resource: product("4"), Metadata: ns[3].Metadata},
	}
	if fmt.Sprint(got) != fmt.Sprint(wantItems) {
		t.Fatalf("registrations %v, want %v", got, wantItems)
	}

	// Each accepted non-delete item and each Parent is built as itself with
	// its own metadata — an item's the registration stored, the Parent's
	// what its row held — and the stale item and the delete are not built.
	wantBuilt := map[string]map[string]string{
		"1": {"m": "1"}, "4": {"m": "4"}, "p": {"m": "p"},
	}
	if built := ex.metadataByID(); fmt.Sprint(built) != fmt.Sprint(wantBuilt) {
		t.Fatalf("built %v, want %v", built, wantBuilt)
	}
	if r, _ := st.row(product("p")); !maps.Equal(r.metadata, map[string]string{"m": "p"}) {
		t.Fatalf("the Parent's mark must leave its metadata, it holds %v", r.metadata)
	}
	// The delete runs with the StaleSeq and Token the store returned for it:
	// the second mark of the batch (item 2 is stale and marks nothing).
	if st.indexOf("DeleteResourceIfSeq:product/3:2:2") == -1 {
		t.Fatalf("the delete must be submitted with its own stale seq: %v", st.callsSnapshot())
	}

	ex.mu.Lock()
	nBuilds := len(ex.reqs)
	ex.mu.Unlock()
	if nBuilds != len(wantBuilt) {
		t.Fatalf("each root is built once, got %d builds", nBuilds)
	}

	// The statement is the registration's only mark and comes before every
	// submit.
	if calls := st.callsSnapshot(); calls[0] != "RegisterChanges:4" {
		t.Fatalf("the statement must come first: %v", calls)
	}
	if st.count("MarkStale") != 0 {
		t.Fatalf("registration marks nothing outside the statement: %v", st.callsSnapshot())
	}
}

// Builds are submitted per id: two items with the same metadata are two pool
// tasks and build in parallel, not one BuildArgs walked in sequence.
func TestRegisterChanges_SubmitsOneBuildPerID(t *testing.T) {
	st := &recordingStore{}
	idx, ex := newRecordingIndexer(st, 2, 4)
	ex.arrived = make(chan string, 2)
	ex.proceed = make(chan struct{})

	if _, err := idx.RegisterChanges(t.Context(), []Notification{
		{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated},
		{ResourceType: "product", ResourceID: "2", Kind: ChangeUpdated},
	}); err != nil {
		t.Fatal(err)
	}
	within(t, ex.arrived, "the first build")
	within(t, ex.arrived, "the second build, while the first is still running")
	close(ex.proceed)
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterChange_StaleItem_ReturnsErrStaleVersionAndSchedulesNothing(t *testing.T) {
	st := &recordingStore{stale: map[model.Resource]bool{product("1"): true}}
	idx := newHotPathIndexer(st, 2, 4)

	err := idx.RegisterChange(t.Context(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("want ErrStaleVersion, got %v", err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if st.indexOf("BeginBuild") != -1 {
		t.Fatalf("a stale item schedules nothing: %v", st.callsSnapshot())
	}
}

func TestRegisterChanges_EmptyBatch_DoesNotCallTheStore(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, 4)

	statuses, err := idx.RegisterChanges(t.Context(), nil)
	if err != nil || len(statuses) != 0 {
		t.Fatalf("got %v, %v", statuses, err)
	}
	if calls := st.callsSnapshot(); len(calls) != 0 {
		t.Fatalf("an empty batch has nothing to commit: %v", calls)
	}
}
