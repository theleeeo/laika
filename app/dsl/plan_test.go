package dsl

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/app/source"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// mockProvider is a minimal source.Provider for testing plan execution.
type mockProvider struct {
	resources map[string]map[string]any   // "type|id" -> data
	related   map[string][]map[string]any // "type|keyval" -> []data
	listed    map[string][]source.ListedResource
	// last* metadata snapshots for verifying request propagation.
	lastFetchResourceMetadata map[string]string
	lastFetchRelatedMetadata  map[string]string
	lastListMetadata          map[string]string
	// listPageSizes records the PageSize of every ListResources call.
	listPageSizes []int32
	// lastFetchRelatedKey records the key passed to the most recent
	// FetchRelated call, for asserting the foreign field name is sent.
	lastFetchRelatedKey source.ResourceKey
	// fetchedRelated records which resource types FetchRelated was called for.
	fetchedRelated map[string]bool
	// pageSize controls how many items are returned per ListResources page.
	pageSize int
	// lastCtxValues records, per provider method, the test marker value found
	// on the incoming ctx — for asserting the plan's ctx reaches the provider.
	lastCtxValues map[string]any
	// fetchedResourceIDs records the ResourceID of every FetchResource call,
	// in call order.
	fetchedResourceIDs []string
	// fetchResourceErr fails FetchResource for the ids it names.
	fetchResourceErr map[string]error
	// fetchedRelatedRoots records the root id of every FetchRelated call.
	fetchedRelatedRoots []string
	// onFetchResource, when set, runs at the start of every FetchResource
	// call with its id.
	onFetchResource func(id string)
}

type ctxMarkerKey struct{}

func (m *mockProvider) recordCtx(method string, ctx context.Context) {
	if m.lastCtxValues == nil {
		m.lastCtxValues = make(map[string]any)
	}
	m.lastCtxValues[method] = ctx.Value(ctxMarkerKey{})
}

func newMockProvider() *mockProvider {
	return &mockProvider{
		resources:      make(map[string]map[string]any),
		related:        make(map[string][]map[string]any),
		listed:         make(map[string][]source.ListedResource),
		fetchedRelated: make(map[string]bool),
		pageSize:       100,
	}
}

func copyMetadata(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (m *mockProvider) FetchResource(ctx context.Context, params source.FetchResourceParams) (source.FetchResourceResult, error) {
	m.recordCtx("FetchResource", ctx)
	m.lastFetchResourceMetadata = copyMetadata(params.Metadata)
	m.fetchedResourceIDs = append(m.fetchedResourceIDs, params.ResourceID)
	if m.onFetchResource != nil {
		m.onFetchResource(params.ResourceID)
	}
	if err := m.fetchResourceErr[params.ResourceID]; err != nil {
		return source.FetchResourceResult{}, err
	}
	data, ok := m.resources[params.ResourceType+"|"+params.ResourceID]
	if !ok {
		return source.FetchResourceResult{}, nil
	}
	return source.FetchResourceResult{Data: data}, nil
}

func (m *mockProvider) FetchRelated(ctx context.Context, params source.FetchRelatedParams) (source.FetchRelatedResult, error) {
	m.recordCtx("FetchRelated", ctx)
	m.lastFetchRelatedMetadata = copyMetadata(params.Metadata)
	m.lastFetchRelatedKey = params.Key
	m.fetchedRelated[params.ResourceType] = true
	m.fetchedRelatedRoots = append(m.fetchedRelatedRoots, params.RootResource.Id)
	key := params.ResourceType + "|" + params.Key.Value
	data, ok := m.related[key]
	if !ok {
		return source.FetchRelatedResult{}, nil
	}
	rr := make([]source.RelatedResource, len(data))
	for i, d := range data {
		id, _ := d["id"].(string)
		rr[i] = source.RelatedResource{ID: id, Data: d}
	}
	return source.FetchRelatedResult{Related: rr}, nil
}

func (m *mockProvider) ListResources(ctx context.Context, params source.ListResourcesParams) (source.ListResourcesResult, error) {
	m.recordCtx("ListResources", ctx)
	m.lastListMetadata = copyMetadata(params.Metadata)
	m.listPageSizes = append(m.listPageSizes, params.PageSize)
	all, ok := m.listed[params.ResourceType]
	if !ok {
		return source.ListResourcesResult{}, nil
	}

	pageSize := m.pageSize
	if params.PageSize > 0 && int(params.PageSize) < pageSize {
		pageSize = int(params.PageSize)
	}

	start := 0
	if params.PageToken != "" {
		for i, r := range all {
			if r.ID == params.PageToken {
				start = i
				break
			}
		}
	}

	end := start + pageSize
	if end > len(all) {
		end = len(all)
	}

	var npt string
	if end < len(all) {
		npt = all[end].ID
	}

	return source.ListResourcesResult{
		Resources:     all[start:end],
		NextPageToken: npt,
	}, nil
}

func TestRelationFetch_PassesForeignFieldToProvider(t *testing.T) {
	prov := newMockProvider()
	prov.resources["order|1"] = map[string]any{"id": "1", "number": "ORD-1"}
	prov.related["customer|1"] = []map[string]any{{"id": "c1", "name": "Alice"}}

	// order.id == customer.order_id : the local field read from order is "id",
	// the foreign field the provider filters customers by is "order_id".
	vc := &resource.VersionConfig{
		Fields: []resource.FieldConfig{{Name: "number"}},
		Relations: []resource.RelationConfig{{
			Resource: "customer",
			Join:     resource.JoinConfig{Local: "id", Foreign: "order_id"},
			Fields:   []resource.FieldConfig{{Name: "name"}},
		}},
	}

	plan := buildPlanForVersion(prov, "order", vc, nil)
	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "order",
		ResourceIDs:  []string{"1"},
	})
	for r := range ch {
		require.NoError(t, r.Err)
	}

	// The provider must receive the foreign field name, not the local one.
	require.Equal(t, "order_id", prov.lastFetchRelatedKey.Field)
	require.Equal(t, "1", prov.lastFetchRelatedKey.Value)
}

func TestRelationFetch_ChainedJoinFromSibling(t *testing.T) {
	prov := newMockProvider()
	prov.resources["order|1"] = map[string]any{"id": "1", "number": "ORD-1"}
	// customer is fetched by order.id; it carries the address foreign key.
	prov.related["customer|1"] = []map[string]any{{"id": "c1", "name": "Alice", "address_id": "addr9"}}
	// address is fetched by the address_id read from the customer sibling.
	prov.related["address|addr9"] = []map[string]any{{"id": "addr9", "city": "NYC"}}

	vc := &resource.VersionConfig{
		Fields: []resource.FieldConfig{{Name: "number"}},
		Relations: []resource.RelationConfig{
			{
				Resource: "customer",
				Join:     resource.JoinConfig{Local: "id", Foreign: "order_id"},
				Fields:   []resource.FieldConfig{{Name: "name"}, {Name: "address_id"}},
			},
			{
				// Local field address_id comes from the customer sibling, not the root.
				Resource: "address",
				Join:     resource.JoinConfig{Local: "address_id", Foreign: "id", From: "customer"},
				Fields:   []resource.FieldConfig{{Name: "city"}},
			},
		},
	}

	plan := buildPlanForVersion(prov, "order", vc, nil)
	var docs []projection.BuildDoc
	for r := range plan.Execute(context.Background(), projection.BuildRequest{ResourceType: "order", ResourceIDs: []string{"1"}}) {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 1)
	addr, ok := docs[0].Doc["address"].([]map[string]any)
	require.True(t, ok, "address relation should be resolved into the doc")
	require.Len(t, addr, 1)
	require.Equal(t, "NYC", addr[0]["city"])
	// The address fetch was keyed by the foreign field on address ("id").
	require.Equal(t, "id", prov.lastFetchRelatedKey.Field)
	require.Equal(t, "addr9", prov.lastFetchRelatedKey.Value)
}

func TestBuildPlanForVersion_PropagatesMetadata(t *testing.T) {
	prov := newMockProvider()
	prov.resources["order|1"] = map[string]any{"id": "1", "number": "ORD-1"}
	prov.related["customer|1"] = []map[string]any{{"id": "c1", "name": "Alice"}}

	vc := &resource.VersionConfig{
		Fields: []resource.FieldConfig{{Name: "number"}},
		Relations: []resource.RelationConfig{{
			Resource: "customer",
			Join:     resource.JoinConfig{Local: "id", Foreign: "order_id"},
			Fields:   []resource.FieldConfig{{Name: "name"}},
		}},
	}

	plan := buildPlanForVersion(prov, "order", vc, nil)
	metadata := map[string]string{"tenant-id": "t1", "trace-id": "abc"}

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "order",
		ResourceIDs:  []string{"1"},
		Metadata:     metadata,
	})

	for r := range ch {
		require.NoError(t, r.Err)
	}

	require.Equal(t, metadata, prov.lastFetchResourceMetadata)
	require.Equal(t, prov.lastFetchResourceMetadata, prov.lastFetchRelatedMetadata)

	// List-all path should forward metadata as well.
	prov.listed["order"] = []source.ListedResource{{ID: "1", Data: map[string]any{"id": "1", "number": "ORD-1"}}}
	ch = plan.Execute(context.Background(), projection.BuildRequest{ResourceType: "order", Metadata: metadata})
	for r := range ch {
		require.NoError(t, r.Err)
	}
	require.Equal(t, metadata, prov.lastListMetadata)
}

func TestBuildPlanForVersion_FetchSingle(t *testing.T) {
	prov := newMockProvider()
	prov.resources["product|1"] = map[string]any{"id": "1", "title": "Widget"}

	fields := []resource.FieldConfig{{Name: "title"}}
	vc := &resource.VersionConfig{Fields: fields}
	plan := buildPlanForVersion(prov, "product", vc, nil)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product",
		ResourceIDs:  []string{"1"},
	})

	var docs []projection.BuildDoc
	for r := range ch {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 1)
	require.Equal(t, "product", docs[0].Root.Type)
	require.Equal(t, "1", docs[0].Root.Id)
	require.Equal(t, "Widget", docs[0].Doc["fields"].(map[string]any)["title"])
}

func TestBuildPlanForVersion_FetchSingle_NotFound(t *testing.T) {
	prov := newMockProvider()

	fields := []resource.FieldConfig{{Name: "title"}}
	vc := &resource.VersionConfig{Fields: fields}
	plan := buildPlanForVersion(prov, "product", vc, nil)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product",
		ResourceIDs:  []string{"999"},
	})

	var docs []projection.BuildDoc
	for r := range ch {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 1)
	require.Nil(t, docs[0].Doc, "doc should be nil for missing resource")
}

// collectPlan executes the plan for the request and returns every item,
// failing the test if the execution reports an error.
func collectPlan(t *testing.T, plan projection.Plan, req projection.BuildRequest) []projection.BuildDoc {
	t.Helper()
	var docs []projection.BuildDoc
	for r := range plan.Execute(context.Background(), req) {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}
	return docs
}

func TestBuildPlanForVersion_FetchByIDs_OneFetchPerID(t *testing.T) {
	prov := newMockProvider()
	prov.resources["product|1"] = map[string]any{"id": "1", "title": "Widget"}
	prov.resources["product|2"] = map[string]any{"id": "2", "title": "Gadget"}

	vc := &resource.VersionConfig{Fields: []resource.FieldConfig{{Name: "title"}}}
	plan := buildPlanForVersion(prov, "product", vc, nil)

	docs := collectPlan(t, plan, projection.BuildRequest{ResourceType: "product", ResourceIDs: []string{"2", "1"}})

	require.Equal(t, []string{"2", "1"}, prov.fetchedResourceIDs, "one FetchResource per asked id, in asked order")
	require.Len(t, docs, 2)
	require.Equal(t, "2", docs[0].Root.Id)
	require.Equal(t, "Gadget", docs[0].Doc["fields"].(map[string]any)["title"])
	require.Equal(t, "1", docs[1].Root.Id)
	require.Equal(t, "Widget", docs[1].Doc["fields"].(map[string]any)["title"])
	for _, d := range docs {
		require.Equal(t, "product", d.Root.Type)
		require.NoError(t, d.Err)
	}
}

func TestBuildPlanForVersion_FetchByIDs_NoDataAnswersNil(t *testing.T) {
	prov := newMockProvider()
	prov.resources["product|1"] = map[string]any{"id": "1", "title": "Widget"}

	vc := &resource.VersionConfig{Fields: []resource.FieldConfig{{Name: "title"}}}
	plan := buildPlanForVersion(prov, "product", vc, nil)

	docs := collectPlan(t, plan, projection.BuildRequest{ResourceType: "product", ResourceIDs: []string{"1", "999"}})

	require.Len(t, docs, 2)
	require.Equal(t, "1", docs[0].Root.Id)
	require.NotNil(t, docs[0].Doc)
	require.Equal(t, model.Resource{Type: "product", Id: "999"}, docs[1].Root)
	require.Nil(t, docs[1].Doc, "an id the provider has no data for is answered with an explicit nil")
	require.NoError(t, docs[1].Err)
}

func TestBuildPlanForVersion_FetchByIDs_ErrorFailsThatIDAlone(t *testing.T) {
	prov := newMockProvider()
	prov.resources["order|1"] = map[string]any{"id": "1", "number": "ORD-1", "shop_id": "s1"}
	prov.resources["order|2"] = map[string]any{"id": "2", "number": "ORD-2", "shop_id": "s2"}
	prov.related["customer|1"] = []map[string]any{{"id": "c1", "name": "Alice"}}
	prov.related["customer|2"] = []map[string]any{{"id": "c2", "name": "Bob"}}
	failure := errors.New("source unavailable")
	prov.fetchResourceErr = map[string]error{"2": failure}

	// The relation and the parent ref exercise every later stage: an Err
	// item must pass through them untouched.
	parentRefs := []parentRef{{parentType: "shop", foreignField: "shop_id"}}
	plan := buildPlanForVersion(prov, "order", customerRelationConfig(), parentRefs)

	docs := collectPlan(t, plan, projection.BuildRequest{
		ResourceType: "order",
		ResourceIDs:  []string{"1", "2"},
		Metadata:     map[string]string{"tenant": "t1"},
	})

	require.Len(t, docs, 2)

	require.Equal(t, "1", docs[0].Root.Id)
	require.NoError(t, docs[0].Err)
	require.Equal(t, "ORD-1", docs[0].Doc["fields"].(map[string]any)["number"])
	require.Equal(t, []model.Resource{{Type: "shop", Id: "s1"}}, docs[0].Parents)
	require.Equal(t, []model.Resource{{Type: "customer", Id: "c1"}}, docs[0].Relations)

	require.ErrorIs(t, docs[1].Err, failure)
	require.Equal(t, projection.BuildDoc{Root: model.Resource{Type: "order", Id: "2"}, Err: docs[1].Err}, docs[1],
		"a failed id carries its Root and Err and nothing else")
	require.Equal(t, []string{"1"}, prov.fetchedRelatedRoots, "no related read for the failed id")
}

func TestBuildPlanForVersion_FetchByIDs_CancelledFailsExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prov := newMockProvider()
	prov.resources["product|1"] = map[string]any{"id": "1", "title": "Widget"}
	prov.fetchResourceErr = map[string]error{"2": context.Canceled}
	prov.onFetchResource = func(id string) {
		if id == "2" {
			cancel()
		}
	}

	vc := &resource.VersionConfig{Fields: []resource.FieldConfig{{Name: "title"}}}
	plan := buildPlanForVersion(prov, "product", vc, nil)

	var execErr error
	var docs []projection.BuildDoc
	for r := range plan.Execute(ctx, projection.BuildRequest{ResourceType: "product", ResourceIDs: []string{"1", "2", "3"}}) {
		if r.Err != nil {
			execErr = r.Err
		}
		docs = append(docs, r.Items...)
	}

	require.ErrorIs(t, execErr, context.Canceled, "a cancelled read fails the execution, not the id")
	require.Empty(t, docs)
	require.Equal(t, []string{"1", "2"}, prov.fetchedResourceIDs, "no read after the cancellation")
}

func TestBuildPlanForVersion_FetchAll_SinglePage(t *testing.T) {
	prov := newMockProvider()
	prov.listed["product"] = []source.ListedResource{
		{ID: "1", Data: map[string]any{"id": "1", "title": "Widget"}},
		{ID: "2", Data: map[string]any{"id": "2", "title": "Gadget"}},
	}

	fields := []resource.FieldConfig{{Name: "title"}}
	vc := &resource.VersionConfig{Fields: fields}
	plan := buildPlanForVersion(prov, "product", vc, nil)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product",
	})

	var docs []projection.BuildDoc
	for r := range ch {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 2)
	require.Equal(t, "1", docs[0].Root.Id)
	require.Equal(t, "Widget", docs[0].Doc["fields"].(map[string]any)["title"])
	require.Equal(t, "2", docs[1].Root.Id)
	require.Equal(t, "Gadget", docs[1].Doc["fields"].(map[string]any)["title"])
}

func TestBuildPlanForVersion_FetchAll_MultiplePages(t *testing.T) {
	prov := newMockProvider()
	prov.pageSize = 2
	prov.listed["product"] = []source.ListedResource{
		{ID: "1", Data: map[string]any{"id": "1", "title": "A"}},
		{ID: "2", Data: map[string]any{"id": "2", "title": "B"}},
		{ID: "3", Data: map[string]any{"id": "3", "title": "C"}},
	}

	fields := []resource.FieldConfig{{Name: "title"}}
	vc := &resource.VersionConfig{Fields: fields}
	plan := buildPlanForVersion(prov, "product", vc, nil)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product",
	})

	var docs []projection.BuildDoc
	var pages int
	for r := range ch {
		require.NoError(t, r.Err)
		pages++
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 3)
	require.Equal(t, 2, pages, "should have 2 pages")
	require.Equal(t, "1", docs[0].Root.Id)
	require.Equal(t, "2", docs[1].Root.Id)
	require.Equal(t, "3", docs[2].Root.Id)
}

func TestBuildPlanForVersion_FetchAll_StartsAtRequestPageToken(t *testing.T) {
	prov := newMockProvider()
	prov.pageSize = 2
	prov.listed["product"] = []source.ListedResource{
		{ID: "1", Data: map[string]any{"id": "1", "title": "A"}},
		{ID: "2", Data: map[string]any{"id": "2", "title": "B"}},
		{ID: "3", Data: map[string]any{"id": "3", "title": "C"}},
		{ID: "4", Data: map[string]any{"id": "4", "title": "D"}},
	}

	fields := []resource.FieldConfig{{Name: "title"}}
	vc := &resource.VersionConfig{Fields: fields}
	plan := buildPlanForVersion(prov, "product", vc, nil)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product",
		PageToken:    "3", // resume mid-walk: page tokens in mockProvider are resource IDs
	})

	var ids []string
	for r := range ch {
		require.NoError(t, r.Err)
		for _, d := range r.Items {
			ids = append(ids, d.Root.Id)
		}
	}

	require.Equal(t, []string{"3", "4"}, ids,
		"a walk started from a page token must skip everything before it")
}

func TestBuildPlanForVersion_FetchAll_PageSize(t *testing.T) {
	listed := []source.ListedResource{
		{ID: "1", Data: map[string]any{"id": "1", "title": "A"}},
		{ID: "2", Data: map[string]any{"id": "2", "title": "B"}},
		{ID: "3", Data: map[string]any{"id": "3", "title": "C"}},
	}
	vc := &resource.VersionConfig{Fields: []resource.FieldConfig{{Name: "title"}}}

	for _, tc := range []struct {
		name     string
		pageSize int
		want     []int32
	}{
		{name: "request page size", pageSize: 2, want: []int32{2, 2}},
		{name: "zero keeps the plan's 100", pageSize: 0, want: []int32{100}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prov := newMockProvider()
			prov.listed["product"] = listed
			plan := buildPlanForVersion(prov, "product", vc, nil)

			var ids []string
			for r := range plan.Execute(context.Background(), projection.BuildRequest{
				ResourceType: "product",
				PageSize:     tc.pageSize,
			}) {
				require.NoError(t, r.Err)
				for _, d := range r.Items {
					ids = append(ids, d.Root.Id)
				}
			}

			require.Equal(t, []string{"1", "2", "3"}, ids)
			require.Equal(t, tc.want, prov.listPageSizes,
				"each listing call asks for the request's page size, or 100 when it has none")
		})
	}
}

func TestBuildPlanForVersion_FetchAll_Empty(t *testing.T) {
	prov := newMockProvider()
	// No resources listed for this type.

	fields := []resource.FieldConfig{{Name: "title"}}
	vc := &resource.VersionConfig{Fields: fields}
	plan := buildPlanForVersion(prov, "product", vc, nil)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product",
	})

	var docs []projection.BuildDoc
	for r := range ch {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 0)
}

func TestBuildPlanForVersion_FetchAll_WithRelation(t *testing.T) {
	prov := newMockProvider()
	prov.listed["order"] = []source.ListedResource{
		{ID: "1", Data: map[string]any{"id": "1", "number": "ORD-1"}},
	}
	prov.related["customer|1"] = []map[string]any{
		{"id": "c1", "name": "Alice"},
	}

	fields := []resource.FieldConfig{{Name: "number"}}
	vc := &resource.VersionConfig{
		Fields: fields,
		Relations: []resource.RelationConfig{
			{
				Resource: "customer",
				Join:     resource.JoinConfig{Local: "id", Foreign: "order_id"},
				Fields:   []resource.FieldConfig{{Name: "name"}},
			},
		},
	}

	plan := buildPlanForVersion(prov, "order", vc, nil)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "order",
	})

	var docs []projection.BuildDoc
	for r := range ch {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 1)
	require.Equal(t, "1", docs[0].Root.Id)
	require.Equal(t, "ORD-1", docs[0].Doc["fields"].(map[string]any)["number"])

	// Should have the customer relation populated.
	customers, ok := docs[0].Doc["customer"].([]map[string]any)
	require.True(t, ok, "customer field should be present")
	require.Len(t, customers, 1)
	require.Equal(t, "Alice", customers[0]["name"])
	require.Equal(t, "c1", customers[0]["id"])

	// Should have tracked the relation.
	require.Len(t, docs[0].Relations, 1)
	require.Equal(t, "customer", docs[0].Relations[0].Type)
	require.Equal(t, "c1", docs[0].Relations[0].Id)
}

// runPlan executes the plan for a single resource ID and drains the channel,
// failing the test if any error is emitted.
func runPlan(t *testing.T, plan projection.Plan, resourceType, resourceID string) {
	t.Helper()
	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: resourceType,
		ResourceIDs:  []string{resourceID},
	})
	for r := range ch {
		require.NoError(t, r.Err)
	}
}

func TestReferenceRelationNotFetched(t *testing.T) {
	prov := newMockProvider()
	prov.resources["c|c1"] = map[string]any{"id": "c1", "b_id": "b1"}

	resources := resource.Configs{
		{Resource: "c", Versions: []resource.VersionConfig{{Version: 1,
			Fields: []resource.FieldConfig{{Name: "b_id"}},
			Relations: []resource.RelationConfig{
				{Resource: "b", Strategy: resource.StrategyReference, Join: resource.JoinConfig{Local: "b_id", Foreign: "id"}, Fields: []resource.FieldConfig{{Name: "name"}}},
			}}}},
		{Resource: "b", Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "name"}}}}},
	}
	plans := BuildPlansFromConfig(prov, resources)
	runPlan(t, plans["c"][0], "c", "c1")

	if prov.fetchedRelated["b"] {
		t.Fatal("reference relation 'b' must not be fetched via FetchRelated")
	}
}

// execSinglePlan builds a single-version plan, executes it for one resource id,
// drains the result channel asserting no error, and returns the single BuildDoc.
func execSinglePlan(t *testing.T, prov *mockProvider, vc *resource.VersionConfig, resourceType, id string) projection.BuildDoc {
	t.Helper()
	plan := buildPlanForVersion(prov, resourceType, vc, nil)
	var docs []projection.BuildDoc
	for r := range plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: resourceType,
		ResourceIDs:  []string{id},
	}) {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}
	require.Len(t, docs, 1)
	return docs[0]
}

// cardinality:one with a single related row → indexed as one object, not an array.
func TestRelationAssembly_SingularCardinality_SingleObject(t *testing.T) {
	prov := newMockProvider()
	prov.resources["order|1"] = map[string]any{"id": "1", "number": "ORD-1", "customer_id": "c1"}
	prov.related["customer|c1"] = []map[string]any{{"id": "c1", "name": "Alice"}}

	vc := &resource.VersionConfig{
		Fields: []resource.FieldConfig{{Name: "number"}},
		Relations: []resource.RelationConfig{{
			Resource:    "customer",
			Cardinality: "one",
			Join:        resource.JoinConfig{Local: "customer_id", Foreign: "id"},
			Fields:      []resource.FieldConfig{{Name: "name"}},
		}},
	}

	doc := execSinglePlan(t, prov, vc, "order", "1")

	customer, ok := doc.Doc["customer"].(map[string]any)
	require.True(t, ok, "cardinality:one relation must be a single object, got %T", doc.Doc["customer"])
	require.Equal(t, "Alice", customer["name"])
	require.Equal(t, "c1", customer["id"])

	// Resolved keeps the full array form for chained joins.
	require.Len(t, doc.Resolved["customer"], 1)
	// Graph edge still tracked.
	require.Len(t, doc.Relations, 1)
	require.Equal(t, "c1", doc.Relations[0].Id)
	require.Equal(t, "customer", doc.Relations[0].Type)
}

// cardinality:one with no related rows → the key is omitted entirely.
func TestRelationAssembly_SingularCardinality_NoRows_OmitsKey(t *testing.T) {
	prov := newMockProvider()
	prov.resources["order|1"] = map[string]any{"id": "1", "number": "ORD-1", "customer_id": "c1"}
	// No prov.related entry for customer|c1 → zero related rows.

	vc := &resource.VersionConfig{
		Fields: []resource.FieldConfig{{Name: "number"}},
		Relations: []resource.RelationConfig{{
			Resource:    "customer",
			Cardinality: "one",
			Join:        resource.JoinConfig{Local: "customer_id", Foreign: "id"},
			Fields:      []resource.FieldConfig{{Name: "name"}},
		}},
	}

	doc := execSinglePlan(t, prov, vc, "order", "1")

	_, present := doc.Doc["customer"]
	require.False(t, present, "cardinality:one key must be omitted when there are no related rows")
	require.Empty(t, doc.Resolved["customer"])
}

// cardinality:one with multiple related rows → first row wins; Resolved keeps all.
func TestRelationAssembly_SingularCardinality_MultipleRows_FirstWins(t *testing.T) {
	prov := newMockProvider()
	prov.resources["order|1"] = map[string]any{"id": "1", "number": "ORD-1", "customer_id": "c1"}
	prov.related["customer|c1"] = []map[string]any{
		{"id": "c1", "name": "Alice"},
		{"id": "c2", "name": "Bob"},
	}

	vc := &resource.VersionConfig{
		Fields: []resource.FieldConfig{{Name: "number"}},
		Relations: []resource.RelationConfig{{
			Resource:    "customer",
			Cardinality: "one",
			Join:        resource.JoinConfig{Local: "customer_id", Foreign: "id"},
			Fields:      []resource.FieldConfig{{Name: "name"}},
		}},
	}

	doc := execSinglePlan(t, prov, vc, "order", "1")

	customer, ok := doc.Doc["customer"].(map[string]any)
	require.True(t, ok, "cardinality:one relation must be a single object, got %T", doc.Doc["customer"])
	require.Equal(t, "Alice", customer["name"])
	require.Len(t, doc.Resolved["customer"], 2, "Resolved must keep every related row")
}

// cardinality:many (default) → indexed as an array (regression guard).
func TestRelationAssembly_ManyCardinality_Array(t *testing.T) {
	prov := newMockProvider()
	prov.resources["order|1"] = map[string]any{"id": "1", "number": "ORD-1"}
	prov.related["customer|1"] = []map[string]any{
		{"id": "c1", "name": "Alice"},
		{"id": "c2", "name": "Bob"},
	}

	vc := &resource.VersionConfig{
		Fields: []resource.FieldConfig{{Name: "number"}},
		Relations: []resource.RelationConfig{{
			Resource:    "customer",
			Cardinality: "many",
			Join:        resource.JoinConfig{Local: "id", Foreign: "order_id"},
			Fields:      []resource.FieldConfig{{Name: "name"}},
		}},
	}

	doc := execSinglePlan(t, prov, vc, "order", "1")

	customers, ok := doc.Doc["customer"].([]map[string]any)
	require.True(t, ok, "cardinality:many relation must be an array, got %T", doc.Doc["customer"])
	require.Len(t, customers, 2)
}

func TestBuildPlansFromConfig_VersionedPlans(t *testing.T) {
	prov := newMockProvider()
	prov.resources["product|1"] = map[string]any{"id": "1", "title": "Widget", "price": "9.99"}

	cfgs := resource.Configs{
		{
			Resource: "product",
			Versions: []resource.VersionConfig{
				{Version: 1, Fields: []resource.FieldConfig{{Name: "title"}}},
				{Version: 2, Fields: []resource.FieldConfig{{Name: "title"}, {Name: "price"}}},
			},
			ReadVersion: 1,
		},
	}

	plans := BuildPlansFromConfig(prov, cfgs)
	require.Len(t, plans, 1)
	require.Len(t, plans["product"], 2)

	// Version 1: only title.
	ch1 := plans["product"][0].Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product", ResourceIDs: []string{"1"},
	})
	var docs1 []projection.BuildDoc
	for r := range ch1 {
		require.NoError(t, r.Err)
		docs1 = append(docs1, r.Items...)
	}
	require.Len(t, docs1, 1)
	fmt.Printf("Version 1 doc: %+v\n", docs1[0].Doc)
	fields1 := docs1[0].Doc["fields"].(map[string]any)
	fmt.Printf("Version 1 fields: %+v\n", fields1)
	require.Equal(t, "Widget", fields1["title"])
	require.NotContains(t, fields1, "price")

	// Version 2: title + price.
	ch2 := plans["product"][1].Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product", ResourceIDs: []string{"1"},
	})
	var docs2 []projection.BuildDoc
	for r := range ch2 {
		require.NoError(t, r.Err)
		docs2 = append(docs2, r.Items...)
	}
	require.Len(t, docs2, 1)
	fields2 := docs2[0].Doc["fields"].(map[string]any)
	require.Equal(t, "Widget", fields2["title"])
	require.Equal(t, "9.99", fields2["price"])
}
