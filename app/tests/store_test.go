package tests

import (
	"context"
	"sync"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/storage/postgres"
)

// suiteStore is the Store every Indexer the suite builds runs on: the suite's
// *postgres.Store, counting each resource's begun builds — the BeginBuild
// calls that succeeded. A row's build_idx is a Build Sequence value, not a
// count of its builds, so this count is what a test asserts on when it means
// "how many builds of this resource began". The count spans the whole test
// and every Indexer the suite built, and survives a row's delete and
// recreate; reset clears it between tests.
type suiteStore struct {
	*postgres.Store

	mu     sync.Mutex
	builds map[model.Resource]int64
}

var _ core.Store = (*suiteStore)(nil)

func newSuiteStore(st *postgres.Store) *suiteStore {
	return &suiteStore{Store: st, builds: map[model.Resource]int64{}}
}

// BeginBuild delegates to the Postgres store and, on success, counts a begun
// build of res.
func (s *suiteStore) BeginBuild(ctx context.Context, res model.Resource, token int64) (core.BuildBegun, error) {
	begun, err := s.Store.BeginBuild(ctx, res, token)
	if err != nil {
		return begun, err
	}
	s.mu.Lock()
	s.builds[res]++
	s.mu.Unlock()
	return begun, nil
}

// beganBuilds returns how many builds of res have begun since the last reset.
func (s *suiteStore) beganBuilds(res model.Resource) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.builds[res]
}

// reset clears the begun-build counts.
func (s *suiteStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.builds = map[model.Resource]int64{}
}

// gatingStore is the suite's counting store with a gate on BeginDelete: the
// first BeginDelete of the armed resource closes reached on entry and then
// blocks until release is closed (or its ctx ends), before delegating. It
// also counts RemoveResource calls per resource, so a test can tell that a
// delete removed no edges at all. It embeds *suiteStore, not the bare
// *postgres.Store, so the builds of an Indexer running on it still count
// towards resourceRebuildCounter.
type gatingStore struct {
	*suiteStore

	mu      sync.Mutex
	res     model.Resource
	armed   bool
	reached chan struct{}
	release chan struct{}
	removes map[model.Resource]int
}

var _ core.Store = (*gatingStore)(nil)

func newGatingStore(s *suiteStore) *gatingStore {
	return &gatingStore{suiteStore: s, removes: map[model.Resource]int{}}
}

// armBeginDelete arms the gate for res and returns its reached channel.
func (g *gatingStore) armBeginDelete(res model.Resource) <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.res, g.armed = res, true
	g.reached = make(chan struct{})
	g.release = make(chan struct{})
	return g.reached
}

// releaseBeginDelete lets the held BeginDelete through and disarms the gate.
// It is safe to call more than once, and when nothing was held.
func (g *gatingStore) releaseBeginDelete() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.release != nil {
		close(g.release)
	}
	g.armed, g.reached, g.release = false, nil, nil
}

// BeginDelete implements [core.Store].
func (g *gatingStore) BeginDelete(ctx context.Context, res model.Resource, staleSeq, token int64) (core.DeleteBegun, error) {
	g.mu.Lock()
	var reached, release chan struct{}
	if g.armed && g.res == res {
		reached, release = g.reached, g.release
		g.armed = false // only the first BeginDelete of res is held
	}
	g.mu.Unlock()

	if reached != nil {
		close(reached)
		select {
		case <-release:
		case <-ctx.Done():
			return core.DeleteBegun{}, ctx.Err()
		}
	}
	return g.suiteStore.BeginDelete(ctx, res, staleSeq, token)
}

// RemoveResource implements [core.Store], counting the call.
func (g *gatingStore) RemoveResource(ctx context.Context, res model.Resource, buildSeq int64) error {
	g.mu.Lock()
	g.removes[res]++
	g.mu.Unlock()
	return g.suiteStore.RemoveResource(ctx, res, buildSeq)
}

// removeCalls returns how many RemoveResource calls of res ran on g.
func (g *gatingStore) removeCalls(res model.Resource) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.removes[res]
}
