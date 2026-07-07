package tests

import "github.com/theleeeo/laika/core"

// Test_SingleSearch_PrimaryInfix proves single-resource Search matches a query
// against the standardized primary `search` surface the same way Federated
// Search's primary tier does — including substring (infix) matches such as
// "7135252" finding "adrcd-7135252-34" — while ignoring the secondary
// `search_scoped` tier. Documents are seeded directly so the test controls the
// standardized surfaces without driving the full Build pipeline (mirrors the
// federated-search tests).
func (t *TestSuite) Test_SingleSearch_PrimaryInfix() {
	t.setResourceConfig(DefaultResourceConfig)

	// a1: the query term lives as an interior substring of the primary surface.
	t.indexRaw(core.IndexName("a", 1), "a1", map[string]any{"search": "adrcd-7135252-34"})
	// a2: its only text lives in the secondary tier — single-resource Search
	// must never surface it, since it consults the primary surface only.
	t.indexRaw(core.IndexName("a", 1), "a2", map[string]any{
		"search_scoped": []map[string]any{{"text": "sekret-999", "scope": []string{}}},
	})

	t.Run("infix substring match on primary surface", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a", Query: "7135252"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("a1", resp.Hits[0].ID)
	})

	t.Run("whole-token match on primary surface", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a", Query: "adrcd-7135252-34"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("a1", resp.Hits[0].ID)
	})

	t.Run("no match for an absent term", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a", Query: "0000000"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 0)
	})

	t.Run("secondary-tier text is not matched", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a", Query: "sekret-999"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 0)
	})
}
