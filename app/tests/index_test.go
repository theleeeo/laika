package tests

import (
	"errors"
	"sync"
	"time"

	"github.com/theleeeo/indexer/core"
	"github.com/theleeeo/indexer/app/source"
	
)

// sourceList extracts a []any from a hit's Source map.
func sourceList(src map[string]any, key string) []any {
	v, _ := src[key].([]any)
	return v
}

// fieldStr extracts a string field from an element returned by sourceList.
func fieldStr(item any, key string) string {
	m, _ := item.(map[string]any)
	s, _ := m[key].(string)
	return s
}


// resourceTracked reports whether (resourceType, resourceID) is present in the
// resources table.
func (t *TestSuite) resourceTracked(resourceType, resourceID string) bool {
	var count int
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT COUNT(*) FROM resources WHERE type=$1 AND id=$2`,
		resourceType, resourceID,
	).Scan(&count)
	t.Require().NoError(err)
	return count > 0
}

func (t *TestSuite) Test_Resource_CRUD_OneIndex() {
	t.setResourceConfig(DefaultResourceConfig)

	// Populate source with two "a" resources.
	t.fakeProvider.SetResource("a", "1", map[string]any{
		"id":     "1",
		"field1": "value1",
	})
	t.fakeProvider.SetResource("a", "2", map[string]any{
		"id":     "2",
		"field1": "value2",
	})

	t.Run("create resources", func() {
		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeCreated,
		})
		t.Require().NoError(err)
		t.Require().True(t.resourceTracked("a", "1"))

		t.worker.Drain(t.T().Context())

		err = t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "2",
			Kind:         core.ChangeCreated,
		})
		t.Require().NoError(err)
		t.Require().True(t.resourceTracked("a", "2"))

		t.worker.Drain(t.T().Context())
	})

	t.Run("no query or filters", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 2)
		t.Require().Equal("1", resp.Hits[0].ID)
		t.Require().Equal("2", resp.Hits[1].ID)
	})

	t.Run("with query, string value", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "a",
			Query:    "value1",
		})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].ID)
	})

	t.Run("with query, no matches", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "a",
			Query:    "false",
		})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 0)
	})

	t.Run("update existing resource", func() {
		// Update the source data.
		t.fakeProvider.SetResource("a", "1", map[string]any{
			"id":     "1",
			"field1": "updated_value",
		})

		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeUpdated,
		})
		t.Require().NoError(err)
		t.Require().True(t.resourceTracked("a", "1"))

		t.worker.Drain(t.T().Context())

		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "a",
			Query:    "updated_value",
		})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].ID)
	})

	t.Run("delete resource", func() {
		t.fakeProvider.DeleteResource("a", "1")

		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeDeleted,
		})
		t.Require().NoError(err)
		t.Require().False(t.resourceTracked("a", "1"))
		t.worker.Drain(t.T().Context())

		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("2", resp.Hits[0].ID)
	})

	t.Run("delete non-existing resource", func() {
		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "non_existing_id",
			Kind:         core.ChangeDeleted,
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())
	})
}

func (t *TestSuite) Test_Resource_CRUD_MultipleIndices() {
	t.setResourceConfig(DefaultResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1"})
	t.fakeProvider.SetResource("b", "2", map[string]any{"id": "2"})

	t.Run("create resources in different indices", func() {
		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated,
		})
		t.Require().NoError(err)

		err = t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "b", ResourceID: "2", Kind: core.ChangeCreated,
		})
		t.Require().NoError(err)

		t.worker.Drain(t.T().Context())
	})

	t.Run("search in index a", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].ID)
	})

	t.Run("search in index b", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "b"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("2", resp.Hits[0].ID)
	})

	t.Run("search in non existing index", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "c"})
		t.Require().EqualError(err, "unknown resource")
		t.Require().Empty(resp.Hits)
	})

	t.Run("delete resources", func() {
		t.fakeProvider.DeleteResource("a", "1")
		t.fakeProvider.DeleteResource("b", "2")

		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a", ResourceID: "1", Kind: core.ChangeDeleted,
		})
		t.Require().NoError(err)

		err = t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "b", ResourceID: "2", Kind: core.ChangeDeleted,
		})
		t.Require().NoError(err)

		t.worker.Drain(t.T().Context())
	})

	t.Run("verify deletions", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 0)

		resp, err = t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "b"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 0)
	})
}

func (t *TestSuite) Test_Create_WithRelation() {
	t.setResourceConfig(DefaultResourceConfig)

	// Source has resource "a/1" with a related "b/1".
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1"})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "1", "field1": "b_val"},
	})

	t.Run("create root resource that has a related resource", func() {
		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated,
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())

		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].ID)

		// The "b" field should be a list with one entry.
		relations := sourceList(resp.Hits[0].Source, "b")
		t.Require().Len(relations, 1)
		t.Require().Equal("1", fieldStr(relations[0], "id"))
	})
}

// Test that when a related child resource is created after the parent,
// and we rebuild the parent, the parent document has the relation populated.
func (t *TestSuite) Test_Create_ParentRelation_Already_Exists() {
	t.setResourceConfig(DefaultResourceConfig)

	// Initially a/1 exists with a relation to b/1 already known by the source.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1"})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "1", "field1": "b_val"},
	})

	// Create and build a/1.
	err := t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated,
	})
	t.Require().NoError(err)

	// Now create b/1 — since a/1 already has a relation to b/1 in PG after
	// its rebuild, AffectedRoots("b","1") should find a/1 and re-rebuild it.
	t.fakeProvider.SetResource("b", "1", map[string]any{"id": "1"})

	err = t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "b", ResourceID: "1", Kind: core.ChangeCreated,
	})
	t.Require().NoError(err)

	t.worker.Drain(t.T().Context())

	t.Run("verify relation on parent", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].ID)
		relations := sourceList(resp.Hits[0].Source, "b")
		t.Require().Len(relations, 1)
		t.Require().Equal("1", fieldStr(relations[0], "id"))
	})
}

// Test that when a root "c" has relations to "a" and "b", and the source
// returns the full graph, it gets indexed correctly.
func (t *TestSuite) Test_RelatedRelations_FullGraph() {
	t.setResourceConfig(RelatedResourceConfig)

	// b/1 exists as a root resource.
	t.fakeProvider.SetResource("b", "1", map[string]any{"id": "1", "f1": "bval"})

	// a/1 exists as a root resource and has relation to b/1.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "aval"})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "bval"},
	})

	// c/1 exists and has relations to both a/1 and b/1.
	t.fakeProvider.SetResource("c", "1", map[string]any{"id": "1", "f1": "cval"})
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "aval"},
	})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "bval"},
	})

	// Create all resources.
	for _, n := range []core.Notification{
		{ResourceType: "b", ResourceID: "1", Kind: core.ChangeCreated},
		{ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated},
		{ResourceType: "c", ResourceID: "1", Kind: core.ChangeCreated},
	} {
		err := t.idx.RegisterChange(t.T().Context(), n)
		t.Require().NoError(err)
	}
	t.worker.Drain(t.T().Context())

	t.Run("verify c has both a and b relations", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "c"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].ID)

		aRels := sourceList(resp.Hits[0].Source, "a")
		t.Require().Len(aRels, 1)
		t.Require().Equal("1", fieldStr(aRels[0], "id"))

		bRels := sourceList(resp.Hits[0].Source, "b")
		t.Require().Len(bRels, 1)
		t.Require().Equal("1", fieldStr(bRels[0], "id"))
	})
}

// Test that updating a child resource triggers a rebuild of parent root documents.
func (t *TestSuite) Test_ChildUpdate_Rebuilds_Parent() {
	t.setResourceConfig(RelatedResourceConfig)

	// Set up the graph: c/1 → a/1, c/1 → b/1.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "aval"})
	t.fakeProvider.SetResource("b", "1", map[string]any{"id": "1", "f1": "bval"})
	t.fakeProvider.SetResource("c", "1", map[string]any{"id": "1", "f1": "cval"})
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "aval"},
	})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "bval"},
	})

	// Build c/1 to establish the relation graph in PG.
	err := t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "c", ResourceID: "1", Kind: core.ChangeCreated,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())
	t.Require().Equal(int64(1), t.resourceRebuildCounter("c", "1"))

	// Now update a/1 at the source.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "aval_updated"})
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "aval_updated"},
	})

	err = t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeUpdated,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())
	t.Require().Equal(int64(1), t.resourceRebuildCounter("a", "1"))
	t.Require().Equal(int64(2), t.resourceRebuildCounter("c", "1"))

	t.Run("verify c sees updated a data", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "c"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)

		aRels := sourceList(resp.Hits[0].Source, "a")
		t.Require().Len(aRels, 1)
		t.Require().Equal("aval_updated", fieldStr(aRels[0], "f1"))
	})
}

// Test_Rebuild_SpecificIDs triggers a full rebuild for specific resource IDs
// and verifies the documents are rebuilt in ES.
func (t *TestSuite) Test_Rebuild_SpecificIDs() {
	t.setResourceConfig(DefaultResourceConfig)

	// Create two resources via the normal change notification path.
	t.fakeProvider.SetResource("a", "1", map[string]any{
		"id": "1", "field1": "original1",
	})
	t.fakeProvider.SetResource("a", "2", map[string]any{
		"id": "2", "field1": "original2",
	})

	for _, id := range []string{"1", "2"} {
		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a", ResourceID: id, Kind: core.ChangeCreated,
		})
		t.Require().NoError(err)
	}
	t.worker.Drain(t.T().Context())

	// Update the source data (without notifying the indexer).
	t.fakeProvider.SetResource("a", "1", map[string]any{
		"id": "1", "field1": "updated1",
	})

	// Rebuild only resource "1".
	err := t.idx.Rebuild(t.T().Context(), []core.ResourceSelector{
		{ResourceType: "a", ResourceIDs: []string{"1"}},
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	// Resource "1" should have updated data; "2" should be unchanged.
	resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "a", Query: "updated1",
	})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)
	t.Require().Equal("1", resp.Hits[0].ID)

	resp, err = t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "a", Query: "original2",
	})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)
	t.Require().Equal("2", resp.Hits[0].ID)
}

// Test_Rebuild_All triggers a full rebuild of all resources of a type
// using the "rebuild all" path (empty resource IDs).
func (t *TestSuite) Test_Rebuild_All() {
	t.setResourceConfig(DefaultResourceConfig)

	// Create resources the normal way.
	t.fakeProvider.SetResource("a", "1", map[string]any{
		"id": "1", "field1": "v1",
	})
	t.fakeProvider.SetResource("a", "2", map[string]any{
		"id": "2", "field1": "v2",
	})

	for _, id := range []string{"1", "2"} {
		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a", ResourceID: id, Kind: core.ChangeCreated,
		})
		t.Require().NoError(err)
	}
	t.worker.Drain(t.T().Context())

	// Update both at the source without notifying.
	t.fakeProvider.SetResource("a", "1", map[string]any{
		"id": "1", "field1": "rebuilt1",
	})
	t.fakeProvider.SetResource("a", "2", map[string]any{
		"id": "2", "field1": "rebuilt2",
	})

	// Rebuild all — empty ResourceIDs triggers the plan's ListResources path.
	err := t.idx.Rebuild(t.T().Context(), []core.ResourceSelector{
		{ResourceType: "a"},
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	// Both resources should reflect the updated source data.
	resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a"})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 2)

	// Verify by searching for the new values.
	resp, err = t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "a", Query: "rebuilt1",
	})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)
	t.Require().Equal("1", resp.Hits[0].ID)

	resp, err = t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "a", Query: "rebuilt2",
	})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)
	t.Require().Equal("2", resp.Hits[0].ID)
}

// Test_Rebuild_UnknownResource verifies that rebuilding an unknown resource type
// returns an error.
func (t *TestSuite) Test_Rebuild_UnknownResource() {
	t.setResourceConfig(DefaultResourceConfig)

	err := t.idx.Rebuild(t.T().Context(), []core.ResourceSelector{
		{ResourceType: "nonexistent"},
	})
	t.Require().Error(err)
	t.Require().ErrorIs(err, core.ErrUnknownResource)
}

// Test_Rebuild_WithRelations verifies that a full rebuild correctly populates
// related resources and persists the relation graph.
func (t *TestSuite) Test_Rebuild_WithRelations() {
	t.setResourceConfig(DefaultResourceConfig)

	// Set up source: a/1 has a relation to b/1.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "aval"})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "b1", "field1": "bval"},
	})
	t.fakeProvider.SetResource("b", "b1", map[string]any{"id": "b1", "field1": "bval"})

	// Rebuild a/1 via the rebuild API (specific ID).
	err := t.idx.Rebuild(t.T().Context(), []core.ResourceSelector{
		{ResourceType: "a", ResourceIDs: []string{"1"}},
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	// Verify the relation is populated in the search result.
	resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a"})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)

	bRels := sourceList(resp.Hits[0].Source, "b")
	t.Require().Len(bRels, 1)
	t.Require().Equal("b1", fieldStr(bRels[0], "id"))
	t.Require().Equal("bval", fieldStr(bRels[0], "field1"))
}

// Test_Rebuild_All_WithRelations verifies that a "rebuild all" via the plan's
// ListResources path correctly populates related data.
func (t *TestSuite) Test_Rebuild_All_WithRelations() {
	t.setResourceConfig(DefaultResourceConfig)

	// Set up source.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "aval"})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "b1", "field1": "bval"},
	})
	t.fakeProvider.SetResource("b", "b1", map[string]any{"id": "b1", "field1": "bval"})

	// Rebuild all a resources.
	err := t.idx.Rebuild(t.T().Context(), []core.ResourceSelector{
		{ResourceType: "a"},
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	// Verify.
	resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a"})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)

	bRels := sourceList(resp.Hits[0].Source, "b")
	t.Require().Len(bRels, 1)
	t.Require().Equal("b1", fieldStr(bRels[0], "id"))
}

// Test_Rebuild_EmptySelectors verifies that passing no selectors returns an error.
func (t *TestSuite) Test_Rebuild_EmptySelectors() {
	t.setResourceConfig(DefaultResourceConfig)

	err := t.idx.Rebuild(t.T().Context(), nil)
	t.Require().Error(err)
}

// resourceVersion returns the version stored in the resources table for the
// given resource, or 0 if not found.
func (t *TestSuite) resourceVersion(resourceType, resourceID string) int64 {
	var version int64
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT version FROM resources WHERE type=$1 AND id=$2`,
		resourceType, resourceID,
	).Scan(&version)
	if err != nil {
		return 0
	}
	return version
}

func (t *TestSuite) resourceRebuildCounter(resourceType, resourceID string) int64 {
	var counter int64
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT build_idx FROM resources WHERE type=$1 AND id=$2`,
		resourceType, resourceID,
	).Scan(&counter)
	if err != nil {
		return 0
	}
	return counter
}

func (t *TestSuite) Test_VersionControl() {
	t.setResourceConfig(DefaultResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{
		"id":     "1",
		"field1": "v1_data",
	})

	t.Run("create with version=1 succeeds", func() {
		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeCreated,
			Version:      1,
		})
		t.Require().NoError(err)
		t.Require().True(t.resourceTracked("a", "1"))
		t.Require().Equal(int64(1), t.resourceVersion("a", "1"))
		t.worker.Drain(t.T().Context())
		t.Require().Equal(int64(1), t.resourceRebuildCounter("a", "1"))
	})

	t.Run("update with version=2 succeeds", func() {
		t.fakeProvider.SetResource("a", "1", map[string]any{
			"id":     "1",
			"field1": "v2_data",
		})

		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeUpdated,
			Version:      2,
		})
		t.Require().NoError(err)
		t.Require().Equal(int64(2), t.resourceVersion("a", "1"))
		t.worker.Drain(t.T().Context())
		t.Require().Equal(int64(2), t.resourceRebuildCounter("a", "1"))

		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "a", Query: "v2_data",
		})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
	})

	t.Run("update with stale version=1 returns error", func() {
		t.fakeProvider.SetResource("a", "1", map[string]any{
			"id":     "1",
			"field1": "stale_data",
		})

		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeUpdated,
			Version:      1,
		})
		t.Require().ErrorIs(err, core.ErrStaleVersion)
		// Version in DB should remain 2.
		t.Require().Equal(int64(2), t.resourceVersion("a", "1"))
	})

	t.Run("update with same version=2 returns error", func() {
		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeUpdated,
			Version:      2,
		})
		t.Require().ErrorIs(err, core.ErrStaleVersion)
		t.Require().Equal(int64(2), t.resourceRebuildCounter("a", "1"))
	})

	t.Run("update with version=0 skips check and succeeds", func() {
		t.fakeProvider.SetResource("a", "1", map[string]any{
			"id":     "1",
			"field1": "no_version_data",
		})

		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeUpdated,
			Version:      0,
		})
		t.Require().NoError(err)
		// Version in DB should remain 2 (version=0 does DO NOTHING on conflict).
		t.Require().Equal(int64(2), t.resourceVersion("a", "1"))
		t.worker.Drain(t.T().Context())
		t.Require().Equal(int64(3), t.resourceRebuildCounter("a", "1"))

		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "a", Query: "no_version_data",
		})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
	})

	t.Run("delete ignores version and always proceeds", func() {
		t.fakeProvider.DeleteResource("a", "1")

		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeDeleted,
			Version:      1, // stale version, but should not matter for deletes
		})
		t.Require().NoError(err)
		t.Require().False(t.resourceTracked("a", "1"))
		t.worker.Drain(t.T().Context())
	})
}

func (t *TestSuite) Test_ConcurrentRequests_SameResource_LatestVersionWins() {
	t.setResourceConfig(DefaultResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{
		"id":     "1",
		"field1": "v1",
	})

	err := t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a",
		ResourceID:   "1",
		Kind:         core.ChangeCreated,
		Version:      1,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start

		t.fakeProvider.SetResource("a", "1", map[string]any{
			"id":     "1",
			"field1": "v2_stale",
		})

		errs <- t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeUpdated,
			Version:      2,
		})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start

		t.fakeProvider.SetResource("a", "1", map[string]any{
			"id":     "1",
			"field1": "v3_latest",
		})

		errs <- t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeUpdated,
			Version:      3,
		})
	}()

	close(start)
	wg.Wait()
	close(errs)

	for registerErr := range errs {
		if registerErr == nil {
			continue
		}
		t.Require().True(errors.Is(registerErr, core.ErrStaleVersion), "unexpected register error: %v", registerErr)
	}

	// Keep source at latest value before workers fetch the document.
	t.fakeProvider.SetResource("a", "1", map[string]any{
		"id":     "1",
		"field1": "v3_latest",
	})

	t.worker.Drain(t.T().Context())

	t.Require().Equal(int64(3), t.resourceVersion("a", "1"))

	resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "a",
		Query:    "v3_latest",
	})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)
	t.Require().Equal("1", resp.Hits[0].ID)

	staleResp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "a",
		Query:    "v2_stale",
	})
	t.Require().NoError(err)
	t.Require().Len(staleResp.Hits, 0)
}

func (t *TestSuite) Test_ConcurrentRequests_MultipleVersions_ConvergeToLatest() {
	t.setResourceConfig(DefaultResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{
		"id":     "1",
		"field1": "v1",
	})

	err := t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a",
		ResourceID:   "1",
		Kind:         core.ChangeCreated,
		Version:      1,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	const maxVersion = 10
	start := make(chan struct{})
	errs := make(chan error, maxVersion-1)
	var wg sync.WaitGroup

	for v := int64(2); v <= maxVersion; v++ {
		version := v
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			t.fakeProvider.SetResource("a", "1", map[string]any{
				"id":     "1",
				"field1": "v_concurrent",
			})

			errs <- t.idx.RegisterChange(t.T().Context(), core.Notification{
				ResourceType: "a",
				ResourceID:   "1",
				Kind:         core.ChangeUpdated,
				Version:      version,
			})
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for registerErr := range errs {
		if registerErr == nil {
			continue
		}
		t.Require().True(errors.Is(registerErr, core.ErrStaleVersion), "unexpected register error: %v", registerErr)
	}

	t.fakeProvider.SetResource("a", "1", map[string]any{
		"id":     "1",
		"field1": "v10_latest",
	})

	err = t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a",
		ResourceID:   "1",
		Kind:         core.ChangeUpdated,
		Version:      maxVersion + 1,
	})
	t.Require().NoError(err)

	t.worker.Drain(t.T().Context())

	t.Require().Equal(int64(maxVersion+1), t.resourceVersion("a", "1"))

	latestResp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "a",
		Query:    "v10_latest",
	})
	t.Require().NoError(err)
	t.Require().Len(latestResp.Hits, 1)
	t.Require().Equal("1", latestResp.Hits[0].ID)

	staleResp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "a",
		Query:    "v_concurrent",
	})
	t.Require().NoError(err)
	t.Require().Len(staleResp.Hits, 0)
}

func (t *TestSuite) Test_ConcurrentRequests_SameResource_BlockedOlderCannotOverwriteNewer() {
	t.setResourceConfig(DefaultResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "v1_base"})

	err := t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a",
		ResourceID:   "1",
		Kind:         core.ChangeCreated,
		Version:      1,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	gateReached := t.fakeProvider.SetFetchGate("old-a-build")

	oldErrCh := make(chan error, 1)
	go func() {
		oldErrCh <- t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeUpdated,
			Version:      2,
			Metadata: map[string]string{
				"test_fetch_gate":          "old-a-build",
				"test_fetch_gate_resource": "a",
				"test_override_field1":     "v2_old",
			},
		})
	}()

	select {
	case <-gateReached:
	case <-time.After(10 * time.Second):
		t.FailNow("timed out waiting for blocked old a build to reach gate")
	}

	err = t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a",
		ResourceID:   "1",
		Kind:         core.ChangeUpdated,
		Version:      3,
		Metadata: map[string]string{
			"test_override_field1": "v3_new",
		},
	})
	t.Require().NoError(err)

	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, searchErr := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "a",
			Query:    "v3_new",
		})
		t.Require().NoError(searchErr)
		if len(resp.Hits) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.FailNow("timed out waiting for newer a build to be indexed")
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.fakeProvider.ReleaseFetchGate("old-a-build")

	oldErr := <-oldErrCh
	t.Require().NoError(oldErr)

	t.worker.Drain(t.T().Context())

	t.Require().Equal(int64(3), t.resourceVersion("a", "1"))

	resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "a",
		Query:    "v3_new",
	})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)

	staleResp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "a",
		Query:    "v2_old",
	})
	t.Require().NoError(err)
	t.Require().Len(staleResp.Hits, 0)
}

func (t *TestSuite) Test_ConcurrentRequests_RelatedParent_ConcurrentChildUpdatesConverge() {
	t.setResourceConfig(RelatedResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "a_base"})
	t.fakeProvider.SetResource("b", "1", map[string]any{"id": "1", "f1": "b_base"})
	t.fakeProvider.SetResource("c", "1", map[string]any{"id": "1", "f1": "c_base"})
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{{"id": "1", "f1": "a_base"}})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{{"id": "1", "f1": "b_base"}})

	for _, n := range []core.Notification{
		{ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
		{ResourceType: "b", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
		{ResourceType: "c", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
	} {
		err := t.idx.RegisterChange(t.T().Context(), n)
		t.Require().NoError(err)
	}
	t.worker.Drain(t.T().Context())

	start := make(chan struct{})
	errCh := make(chan error, 2)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		errCh <- t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         core.ChangeUpdated,
			Version:      2,
			Metadata: map[string]string{
				"test_override_f1": "c_from_a",
			},
		})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		errCh <- t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "b",
			ResourceID:   "1",
			Kind:         core.ChangeUpdated,
			Version:      2,
			Metadata: map[string]string{
				"test_override_f1": "c_from_b",
			},
		})
	}()

	close(start)
	wg.Wait()
	close(errCh)

	for registerErr := range errCh {
		t.Require().NoError(registerErr)
	}

	err := t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a",
		ResourceID:   "1",
		Kind:         core.ChangeUpdated,
		Version:      3,
		Metadata: map[string]string{
			"test_override_f1": "c_latest",
		},
	})
	t.Require().NoError(err)

	t.worker.Drain(t.T().Context())

	latestResp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "c",
		Query:    "c_latest",
	})
	t.Require().NoError(err)
	t.Require().Len(latestResp.Hits, 1)

	staleRespA, err := t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "c",
		Query:    "c_from_a",
	})
	t.Require().NoError(err)
	t.Require().Len(staleRespA.Hits, 0)

	staleRespB, err := t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "c",
		Query:    "c_from_b",
	})
	t.Require().NoError(err)
	t.Require().Len(staleRespB.Hits, 0)
}

// Test_RaceCondition_ChildUpdatedDuringParentBuild reproduces the race where a
// child resource is updated while a parent rebuild is in flight, after the
// parent's old relation edge has been removed and before the new one is
// persisted. RegisterChange for the child sees no parent edge to fan out to,
// so without drift detection the parent would index permanently-stale child
// data. The drift check in buildOne re-enqueues the parent on detection.
func (t *TestSuite) Test_RaceCondition_ChildUpdatedDuringParentBuild() {
	t.setResourceConfig(RelatedResourceConfig)

	// Initial graph: c/1 -> a/1, c/1 -> b/1.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "a_v1"})
	t.fakeProvider.SetResource("b", "1", map[string]any{"id": "1", "f1": "b_v1"})
	t.fakeProvider.SetResource("c", "1", map[string]any{"id": "1", "f1": "c_v1"})
	// Use versioned relations so drift detection can compare observed vs stored.
	t.fakeProvider.SetRelatedVersioned("a", []string{"1"}, []source.RelatedResource{{ID: "1", Data: map[string]any{"id": "1", "f1": "a_v1"}, Version: 1}})
	t.fakeProvider.SetRelatedVersioned("b", []string{"1"}, []source.RelatedResource{{ID: "1", Data: map[string]any{"id": "1", "f1": "b_v1"}, Version: 1}})

	for _, n := range []core.Notification{
		{ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
		{ResourceType: "b", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
		{ResourceType: "c", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
	} {
		t.Require().NoError(t.idx.RegisterChange(t.T().Context(), n))
	}
	t.worker.Drain(t.T().Context())

	// Pause c's build inside FetchRelated for the "a" child.
	gateReached := t.fakeProvider.SetFetchGate("c-related-a")

	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "c",
		ResourceID:   "1",
		Kind:         core.ChangeUpdated,
		Version:      2,
		Metadata: map[string]string{
			"test_related_gate":          "c-related-a",
			"test_related_gate_resource": "a",
		},
	}))

	select {
	case <-gateReached:
	case <-time.After(10 * time.Second):
		t.FailNow("timed out waiting for c build to reach related-fetch gate")
	}

	// While c is paused holding the snapshot of a at version 1, advance a to
	// version 2 in the source and notify. The c->a edge was already removed at
	// the start of c's build, so this notification finds no parents and cannot
	// fan out to c.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "a_v2"})
	t.fakeProvider.SetRelatedVersioned("a", []string{"1"}, []source.RelatedResource{{ID: "1", Data: map[string]any{"id": "1", "f1": "a_v2"}, Version: 2}})

	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a",
		ResourceID:   "1",
		Kind:         core.ChangeUpdated,
		Version:      2,
	}))

	// Release: c finishes writing the stale doc, then drift detection compares
	// observed version (1) vs stored version (2) and re-enqueues c.
	t.fakeProvider.ReleaseFetchGate("c-related-a")
	t.worker.Drain(t.T().Context())

	// c must converge to the latest a relation data.
	resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "c"})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)
	t.Require().Equal("1", resp.Hits[0].ID)

	aRels := sourceList(resp.Hits[0].Source, "a")
	t.Require().Len(aRels, 1)
	t.Require().Equal("1", fieldStr(aRels[0], "id"))
	t.Require().Equal("a_v2", fieldStr(aRels[0], "f1"))

	// Search for new A field works
	resp, err = t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "c",
		Query:    "a_v2",
	})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)
	t.Require().Equal("1", resp.Hits[0].ID)

	// Search for old A field does not work
	staleResp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "c",
		Query:    "a_v1",
	})
	t.Require().NoError(err)
	t.Require().Len(staleResp.Hits, 0)

	// c was rebuilt at least twice: once originally, once after concurrent
	// update was detected via drift.
	t.Require().GreaterOrEqual(t.resourceRebuildCounter("c", "1"), int64(3))
}
