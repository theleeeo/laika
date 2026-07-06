package tests

import (
	"errors"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

// TypedResourceConfig exercises the typed filter matrix: keyword, long, date
// and boolean fields on a single resource.
var TypedResourceConfig = resource.Configs{
	{
		Resource: "t",
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields: []resource.FieldConfig{
				{Name: "name"}, // keyword
				{Name: "size", Type: "long"},
				{Name: "created_at", Type: "date"},
				{Name: "active", Type: "boolean"},
			},
		}},
	},
}

func (t *TestSuite) Test_FilterOps_TypedFields() {
	for _, c := range TypedResourceConfig {
		c.ApplyDefaults()
	}
	must(t.T(), TypedResourceConfig.Validate())
	t.setResourceConfig(TypedResourceConfig)

	t.fakeProvider.SetResource("t", "1", map[string]any{
		"id": "1", "name": "vx-alpha", "size": 5,
		"created_at": "2026-01-10T00:00:00Z", "active": true,
	})
	t.fakeProvider.SetResource("t", "2", map[string]any{
		"id": "2", "name": "vx-beta", "size": 50,
		"created_at": "2026-03-10T00:00:00Z", "active": false,
	})
	t.fakeProvider.SetResource("t", "3", map[string]any{
		"id": "3", "name": "gamma", "size": 500,
		"created_at": "2026-06-10T00:00:00Z",
		// no "active" key: exercises exists / not_exists
	})

	for _, id := range []string{"1", "2", "3"} {
		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "t", ResourceID: id, Kind: core.ChangeCreated,
		})
		t.Require().NoError(err)
	}
	t.worker.Drain(t.T().Context())

	// search runs the filters sorted by size ascending, so expected ID slices
	// are deterministic: doc 1 (5) < doc 2 (50) < doc 3 (500).
	search := func(filters ...core.Filter) []string {
		t.T().Helper()
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "t",
			Filters:  filters,
			Sort:     []core.SortOption{{Field: "fields.size"}},
		})
		t.Require().NoError(err)
		ids := make([]string, 0, len(resp.Hits))
		for _, h := range resp.Hits {
			ids = append(ids, h.ID)
		}
		return ids
	}

	t.Run("numeric range as two stacked AND filters", func() {
		t.Require().Equal([]string{"2"}, search(
			core.Filter{Field: "fields.size", Op: core.FilterOpGte, Value: "10"},
			core.Filter{Field: "fields.size", Op: core.FilterOpLt, Value: "100"},
		))
	})

	t.Run("date range", func() {
		t.Require().Equal([]string{"2", "3"}, search(
			core.Filter{Field: "fields.created_at", Op: core.FilterOpGt, Value: "2026-02-01T00:00:00Z"},
		))
	})

	t.Run("prefix", func() {
		t.Require().Equal([]string{"1", "2"}, search(
			core.Filter{Field: "fields.name", Op: core.FilterOpPrefix, Value: "vx-"},
		))
	})

	t.Run("suffix", func() {
		t.Require().Equal([]string{"1"}, search(
			core.Filter{Field: "fields.name", Op: core.FilterOpSuffix, Value: "alpha"},
		))
	})

	t.Run("contains", func() {
		t.Require().Equal([]string{"2"}, search(
			core.Filter{Field: "fields.name", Op: core.FilterOpContains, Value: "bet"},
		))
	})

	t.Run("neq", func() {
		t.Require().Equal([]string{"2", "3"}, search(
			core.Filter{Field: "fields.name", Op: core.FilterOpNeq, Value: "vx-alpha"},
		))
	})

	t.Run("not_in", func() {
		t.Require().Equal([]string{"3"}, search(
			core.Filter{Field: "fields.name", Op: core.FilterOpNotIn, Values: []string{"vx-alpha", "vx-beta"}},
		))
	})

	t.Run("boolean eq", func() {
		t.Require().Equal([]string{"1"}, search(
			core.Filter{Field: "fields.active", Op: core.FilterOpEq, Value: "true"},
		))
	})

	t.Run("exists and not_exists", func() {
		t.Require().Equal([]string{"1", "2"}, search(
			core.Filter{Field: "fields.active", Op: core.FilterOpExists},
		))
		t.Require().Equal([]string{"3"}, search(
			core.Filter{Field: "fields.active", Op: core.FilterOpNotExists},
		))
	})

	t.Run("op not allowed for type is rejected", func() {
		_, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "t",
			Filters:  []core.Filter{{Field: "fields.size", Op: core.FilterOpPrefix, Value: "5"}},
		})
		var inv *core.InvalidArgumentError
		t.Require().True(errors.As(err, &inv), "expected InvalidArgumentError, got %v", err)
	})

	t.Run("unknown field is rejected", func() {
		_, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "t",
			Filters:  []core.Filter{{Field: "fields.nope", Op: core.FilterOpEq, Value: "x"}},
		})
		var inv *core.InvalidArgumentError
		t.Require().True(errors.As(err, &inv), "expected InvalidArgumentError, got %v", err)
	})
}

func (t *TestSuite) Test_FilterOps_NestedNegation() {
	t.setResourceConfig(DefaultResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "p1"})
	t.fakeProvider.SetResource("a", "2", map[string]any{"id": "2", "field1": "p2"})
	t.fakeProvider.SetResource("a", "3", map[string]any{"id": "3", "field1": "p3"})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "b1", "field1": "active"},
		{"id": "b2", "field1": "inactive"},
	})
	t.fakeProvider.SetRelated("b", []string{"2"}, []map[string]any{
		{"id": "b3", "field1": "inactive"},
	})
	// Parent 3 has no children at all.

	for _, id := range []string{"1", "2", "3"} {
		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "a", ResourceID: id, Kind: core.ChangeCreated,
		})
		t.Require().NoError(err)
	}
	t.worker.Drain(t.T().Context())

	search := func(filters ...core.Filter) []string {
		t.T().Helper()
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "a",
			Filters:  filters,
			Sort:     []core.SortOption{{Field: "fields.field1"}},
		})
		t.Require().NoError(err)
		ids := make([]string, 0, len(resp.Hits))
		for _, h := range resp.Hits {
			ids = append(ids, h.ID)
		}
		return ids
	}

	t.Run("eq on nested relation matches parents with some matching child", func() {
		t.Require().Equal([]string{"1"}, search(
			core.Filter{Field: "b.field1", Op: core.FilterOpEq, Value: "active"},
		))
	})

	// Document-level negation: "no child matches", so a parent with one
	// active and one inactive child is excluded, and a childless parent
	// is included.
	t.Run("neq on nested relation means no child matches", func() {
		t.Require().Equal([]string{"2", "3"}, search(
			core.Filter{Field: "b.field1", Op: core.FilterOpNeq, Value: "active"},
		))
	})

	t.Run("not_in on nested relation means no child matches any value", func() {
		t.Require().Equal([]string{"3"}, search(
			core.Filter{Field: "b.field1", Op: core.FilterOpNotIn, Values: []string{"active", "inactive"}},
		))
	})
}
