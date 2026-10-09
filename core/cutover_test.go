package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeCutoverES is an in-memory CutoverBackend.
type fakeCutoverES struct {
	aliases    map[string]string // alias -> current index
	aliasErr   map[string]error  // alias -> injected GetAlias error
	indices    map[string]bool   // index -> exists
	existsErr  map[string]error  // index -> injected IndexExists error
	counts     map[string]int64  // index -> doc count
	countErr   map[string]error  // index -> injected CountDocs error
	countCalls []string          // indices CountDocs was asked about
}

func newFakeCutoverES() *fakeCutoverES {
	return &fakeCutoverES{
		aliases:   map[string]string{},
		aliasErr:  map[string]error{},
		indices:   map[string]bool{},
		existsErr: map[string]error{},
		counts:    map[string]int64{},
		countErr:  map[string]error{},
	}
}

func (f *fakeCutoverES) GetAlias(_ context.Context, aliasName string) (string, error) {
	if err := f.aliasErr[aliasName]; err != nil {
		return "", err
	}
	return f.aliases[aliasName], nil
}

func (f *fakeCutoverES) IndexExists(_ context.Context, indexName string) (bool, error) {
	if err := f.existsErr[indexName]; err != nil {
		return false, err
	}
	return f.indices[indexName], nil
}

func (f *fakeCutoverES) CountDocs(_ context.Context, indexName string) (int64, error) {
	f.countCalls = append(f.countCalls, indexName)
	if err := f.countErr[indexName]; err != nil {
		return 0, err
	}
	return f.counts[indexName], nil
}

// fakeStaleCounter is an in-memory StaleCounter recording what it was asked.
type fakeStaleCounter struct {
	counts    map[string]int // resource type -> stale count
	oldest    map[string]time.Time
	err       error
	gotTypes  []string
	gotBefore time.Time

	rows             map[string]int // resource type -> rows that aren't tombstones
	missing          map[string]int // resource type -> unmarked rows lacking the version's set
	coverageErr      error
	gotCoverageTypes []string
	gotVersions      []int
}

func newFakeStaleCounter() *fakeStaleCounter {
	return &fakeStaleCounter{
		counts:  map[string]int{},
		oldest:  map[string]time.Time{},
		rows:    map[string]int{},
		missing: map[string]int{},
	}
}

func (f *fakeStaleCounter) CountMissingEdgeSets(_ context.Context, resourceType string, schemaVersion int) (int, int, error) {
	f.gotCoverageTypes = append(f.gotCoverageTypes, resourceType)
	f.gotVersions = append(f.gotVersions, schemaVersion)
	if f.coverageErr != nil {
		return 0, 0, f.coverageErr
	}
	return f.rows[resourceType], f.missing[resourceType], nil
}

func (f *fakeStaleCounter) CountStale(_ context.Context, resourceType string, before time.Time) (int, time.Time, error) {
	f.gotTypes = append(f.gotTypes, resourceType)
	f.gotBefore = before
	if f.err != nil {
		return 0, time.Time{}, f.err
	}
	return f.counts[resourceType], f.oldest[resourceType], nil
}

// readyForwardES sets up resource "m" one gate short of a v1->v2 cutover:
// alias on v1, both indices exist, equal doc counts.
func readyForwardES() *fakeCutoverES {
	es := newFakeCutoverES()
	es.aliases["m_search"] = "m_search_v1"
	es.indices["m_search_v1"] = true
	es.indices["m_search_v2"] = true
	es.counts["m_search_v1"] = 1200
	es.counts["m_search_v2"] = 1200
	return es
}

// readyBackwardES sets up resource "m" one gate short of a v3->v2 rollback:
// alias on v3, both indices exist, equal doc counts.
func readyBackwardES() *fakeCutoverES {
	es := newFakeCutoverES()
	es.aliases["m_search"] = "m_search_v3"
	es.indices["m_search_v3"] = true
	es.indices["m_search_v2"] = true
	es.counts["m_search_v3"] = 1200
	es.counts["m_search_v2"] = 1200
	return es
}

func findCheck(t *testing.T, r ResourceReadiness, name string) ReadinessCheck {
	t.Helper()
	for _, c := range r.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("readiness for %q has no check %q (got %+v)", r.Resource, name, r.Checks)
	return ReadinessCheck{}
}

func requireNotApplicable(t *testing.T, r ResourceReadiness, name string) {
	t.Helper()
	c := findCheck(t, r, name)
	require.True(t, c.OK, c.Detail)
	require.Contains(t, c.Detail, "not applicable")
}

func TestCheckCutoverReadiness_ForwardAllGatesPass(t *testing.T) {
	es := readyForwardES()
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.Equal(t, "m", r.Resource)
	require.Equal(t, "m_search_v1", r.CurrentIndex)
	require.Equal(t, "m_search_v2", r.TargetIndex)
	require.Equal(t, AliasForward, r.Move)
	require.True(t, r.Ready)
	for _, c := range r.Checks {
		require.True(t, c.OK, "check %s: %s", c.Name, c.Detail)
	}
}

func TestCheckCutoverReadiness_TargetIndexMissingShortCircuits(t *testing.T) {
	es := readyForwardES()
	delete(es.indices, "m_search_v2")
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.False(t, r.Ready)
	require.False(t, findCheck(t, r, CheckTargetIndex).OK)
	require.Contains(t, findCheck(t, r, CheckTargetIndex).Detail, "gen-mapping")
	require.Empty(t, es.countCalls, "no point counting docs of a missing index")
	require.Empty(t, st.gotTypes, "gates after a missing target are noise")
}

func TestCheckCutoverReadiness_Coverage(t *testing.T) {
	cases := []struct {
		name    string
		missing int
		wantOK  bool
	}{
		{"a row lacks the set", 2, false},
		{"none lacks it", 0, true},
	}
	moves := []struct {
		name     string
		es       func() *fakeCutoverES
		wantMove AliasMove
	}{
		{"forward", readyForwardES, AliasForward},
		{"backward", readyBackwardES, AliasBackward},
	}

	for _, mv := range moves {
		for _, tc := range cases {
			t.Run(mv.name+"/"+tc.name, func(t *testing.T) {
				st := newFakeStaleCounter()
				st.rows["m"] = 1200
				st.missing["m"] = tc.missing

				got := CheckCutoverReadiness(context.Background(), mv.es(), st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
				require.Len(t, got, 1)
				require.Equal(t, mv.wantMove, got[0].Move)

				coverage := findCheck(t, got[0], CheckCoverage)
				require.Equal(t, tc.wantOK, coverage.OK, coverage.Detail)
				require.Equal(t, tc.wantOK, got[0].Ready)
				require.Equal(t, []string{"m"}, st.gotCoverageTypes)
				require.Equal(t, []int{2}, st.gotVersions, "coverage is of the target's Schema Version")
				require.Contains(t, coverage.Detail, "1200")
				require.Contains(t, coverage.Detail, "v2 edge set")
				if !tc.wantOK {
					require.Contains(t, coverage.Detail, "2 of 1200")
				}
			})
		}
	}
}

func TestCheckCutoverReadiness_DocGap(t *testing.T) {
	cases := []struct {
		name    string
		target  int64
		wantGap string
	}{
		{"target holds fewer", 1140, "gap -60"},
		{"target holds more", 1203, "gap +3"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			es := readyForwardES()
			es.counts["m_search_v2"] = tc.target
			st := newFakeStaleCounter()
			st.rows["m"] = 1200

			got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
			require.Len(t, got, 1)
			gap := findCheck(t, got[0], CheckDocGap)
			require.False(t, gap.OK, gap.Detail)
			require.False(t, got[0].Ready)
			// The operator must see both counts, the gap and the rows to judge it.
			require.Contains(t, gap.Detail, "m_search_v1 holds 1200")
			require.Contains(t, gap.Detail, fmt.Sprintf("m_search_v2 holds %d", tc.target))
			require.Contains(t, gap.Detail, `of 1200 "m" resources`)
			require.Contains(t, gap.Detail, tc.wantGap)
			require.Contains(t, gap.Detail, "-accept-gap m")
			require.NotContains(t, gap.Detail, "(accepted)")

			got = CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}),
				ReadinessOptions{AcceptGap: map[string]bool{"m": true}})
			require.Len(t, got, 1)
			gap = findCheck(t, got[0], CheckDocGap)
			require.True(t, gap.OK, gap.Detail)
			require.True(t, got[0].Ready)
			require.Contains(t, gap.Detail, tc.wantGap)
			require.Contains(t, gap.Detail, "(accepted)")
		})
	}
}

func TestCheckCutoverReadiness_AcceptGapNamesItsTypeOnly(t *testing.T) {
	es := readyForwardES()
	es.aliases["a_search"] = "a_search_v1"
	es.indices["a_search_v2"] = true
	es.counts["a_search_v1"] = 100
	es.counts["a_search_v2"] = 90
	es.aliases["b_search"] = "b_search_v1"
	es.indices["b_search_v2"] = true
	es.counts["b_search_v1"] = 100
	es.counts["b_search_v2"] = 90
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"a": 2, "b": 2}),
		ReadinessOptions{AcceptGap: map[string]bool{"a": true}})
	require.Len(t, got, 2)

	byResource := map[string]ResourceReadiness{}
	for _, r := range got {
		byResource[r.Resource] = r
	}
	require.True(t, findCheck(t, byResource["a"], CheckDocGap).OK)
	require.True(t, byResource["a"].Ready)
	require.False(t, findCheck(t, byResource["b"], CheckDocGap).OK, "accepting a's gap must not pass b's")
	require.False(t, byResource["b"].Ready)
}

func TestCheckCutoverReadiness_AcceptGapPassesNoOtherGate(t *testing.T) {
	accept := ReadinessOptions{AcceptGap: map[string]bool{"m": true}}

	t.Run("coverage", func(t *testing.T) {
		es := readyForwardES()
		es.counts["m_search_v2"] = 1140
		st := newFakeStaleCounter()
		st.rows["m"] = 1200
		st.missing["m"] = 1

		got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), accept)
		require.Len(t, got, 1)
		require.True(t, findCheck(t, got[0], CheckDocGap).OK)
		require.False(t, findCheck(t, got[0], CheckCoverage).OK, "an accepted gap must not pass coverage")
		require.False(t, got[0].Ready)
	})

	t.Run("stale-backlog", func(t *testing.T) {
		es := readyForwardES()
		es.counts["m_search_v2"] = 1140
		st := newFakeStaleCounter()
		st.counts["m"] = 1
		st.oldest["m"] = time.Now().Add(-42 * time.Minute)

		got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), accept)
		require.Len(t, got, 1)
		require.True(t, findCheck(t, got[0], CheckDocGap).OK)
		require.False(t, findCheck(t, got[0], CheckStaleBacklog).OK, "an accepted gap must not pass an aged mark")
		require.False(t, got[0].Ready)
	})
}

func TestCheckCutoverReadiness_CoverageCountErrorFailsOnlyCoverage(t *testing.T) {
	es := readyForwardES()
	st := newFakeStaleCounter()
	st.coverageErr = errors.New("pg exploded")

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.False(t, r.Ready)
	coverage := findCheck(t, r, CheckCoverage)
	require.False(t, coverage.OK)
	require.Contains(t, coverage.Detail, "pg exploded")
	for _, c := range r.Checks {
		if c.Name != CheckCoverage {
			require.True(t, c.OK, "check %s: %s", c.Name, c.Detail)
		}
	}
	gap := findCheck(t, r, CheckDocGap)
	require.Contains(t, gap.Detail, "m_search_v1 holds 1200, m_search_v2 holds 1200 (gap +0)")
	require.NotContains(t, gap.Detail, "resources", "without a coverage count the gap names no rows")
}

func TestCheckCutoverReadiness_StaleBacklogFailsGate(t *testing.T) {
	es := readyForwardES()
	st := newFakeStaleCounter()
	st.counts["m"] = 3
	st.oldest["m"] = time.Now().Add(-42 * time.Minute)

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.False(t, r.Ready)
	stale := findCheck(t, r, CheckStaleBacklog)
	require.False(t, stale.OK)
	require.Contains(t, stale.Detail, "3")
}

func TestCheckCutoverReadiness_StaleCutoffUsesMaxStaleAge(t *testing.T) {
	es := readyForwardES()
	st := newFakeStaleCounter()

	CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}),
		ReadinessOptions{MaxStaleAge: time.Hour})

	require.Equal(t, []string{"m"}, st.gotTypes)
	require.WithinDuration(t, time.Now().Add(-time.Hour), st.gotBefore, 10*time.Second)
}

func TestCheckCutoverReadiness_MaxStaleAgeDefaults(t *testing.T) {
	es := readyForwardES()
	st := newFakeStaleCounter()

	CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})

	require.WithinDuration(t, time.Now().Add(-DefaultMaxStaleAge), st.gotBefore, 10*time.Second)
}

func TestCheckCutoverReadiness_BackwardRunsSameGates(t *testing.T) {
	// A rollback is a cutover in reverse (ADR 0009); it deserves the same gates.
	es := newFakeCutoverES()
	es.aliases["m_search"] = "m_search_v3"
	es.indices["m_search_v2"] = true
	es.counts["m_search_v3"] = 500
	es.counts["m_search_v2"] = 500
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)
	require.Equal(t, AliasBackward, got[0].Move)
	require.True(t, got[0].Ready)
	require.ElementsMatch(t, []string{"m_search_v3", "m_search_v2"}, es.countCalls)
}

func TestCheckCutoverReadiness_CreateSkipsCoverageAndGap(t *testing.T) {
	// No alias yet: there is no current read index to compare against, so
	// coverage and the gap cannot gate — but the target must exist and the
	// backlog be clean.
	es := newFakeCutoverES()
	es.indices["m_search_v2"] = true
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.Equal(t, AliasCreate, r.Move)
	require.True(t, r.Ready)
	requireNotApplicable(t, r, CheckCoverage)
	requireNotApplicable(t, r, CheckDocGap)
	require.Empty(t, es.countCalls, "nothing to compare when the alias does not exist")
	require.Empty(t, st.gotCoverageTypes)
	require.Equal(t, []string{"m"}, st.gotTypes, "the stale gate still applies")
}

func TestCheckCutoverReadiness_InSyncSkipsCoverageAndGap(t *testing.T) {
	// Already cut over: the tool doubles as a post-cutover soak check, but
	// comparing an index's count with itself proves nothing.
	es := newFakeCutoverES()
	es.aliases["m_search"] = "m_search_v2"
	es.indices["m_search_v2"] = true
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.Equal(t, AliasInSync, r.Move)
	require.True(t, r.Ready)
	requireNotApplicable(t, r, CheckCoverage)
	requireNotApplicable(t, r, CheckDocGap)
	require.Empty(t, es.countCalls)
	require.Empty(t, st.gotCoverageTypes)
}

func TestCheckCutoverReadiness_ForeignAliasIsNotGateable(t *testing.T) {
	es := newFakeCutoverES()
	es.aliases["m_search"] = "hand_built_index"
	es.indices["m_search_v2"] = true
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.False(t, r.Ready)
	require.Equal(t, AliasForeign, r.Move)
	alias := findCheck(t, r, CheckAliasState)
	require.False(t, alias.OK)
	require.Contains(t, alias.Detail, "hand_built_index")
	require.Empty(t, es.countCalls)
	require.Empty(t, st.gotTypes)
}

func TestCheckCutoverReadiness_FetchErrorsFailTheirGate(t *testing.T) {
	sentinel := errors.New("es exploded")

	cases := []struct {
		name      string
		breakES   func(*fakeCutoverES)
		breakST   func(*fakeStaleCounter)
		failCheck string
	}{
		{"alias fetch", func(f *fakeCutoverES) { f.aliasErr["m_search"] = sentinel }, nil, CheckAliasState},
		{"index existence", func(f *fakeCutoverES) { f.existsErr["m_search_v2"] = sentinel }, nil, CheckTargetIndex},
		{"doc count", func(f *fakeCutoverES) { f.countErr["m_search_v2"] = sentinel }, nil, CheckDocGap},
		{"coverage count", nil, func(f *fakeStaleCounter) { f.coverageErr = sentinel }, CheckCoverage},
		{"stale count", nil, func(f *fakeStaleCounter) { f.err = sentinel }, CheckStaleBacklog},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			es := readyForwardES()
			st := newFakeStaleCounter()
			if tc.breakES != nil {
				tc.breakES(es)
			}
			if tc.breakST != nil {
				tc.breakST(st)
			}

			got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
			require.Len(t, got, 1)
			require.False(t, got[0].Ready)
			failed := findCheck(t, got[0], tc.failCheck)
			require.False(t, failed.OK)
			require.Contains(t, failed.Detail, "es exploded")
		})
	}
}

func TestCheckCutoverReadiness_OneResourceFailingDoesNotStopOthers(t *testing.T) {
	es := readyForwardES()
	es.aliasErr["a_search"] = errors.New("es exploded")
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"a": 2, "m": 2}), ReadinessOptions{})
	require.Len(t, got, 2)

	byResource := map[string]ResourceReadiness{}
	for _, r := range got {
		byResource[r.Resource] = r
	}
	require.False(t, byResource["a"].Ready)
	require.True(t, byResource["m"].Ready, "the healthy resource must still be assessed")
}
