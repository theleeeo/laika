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
