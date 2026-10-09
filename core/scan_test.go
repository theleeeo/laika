package core

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/theleeeo/laika/core/resource"
)

// scanCall is one SearchBackend.Search call the scan fake received.
type scanCall struct {
	req   SearchRequest
	index string
	vc    *resource.VersionConfig
}

// scanBackend is a SearchBackend fake for scans: a stateful alias, a record of
// every Search call, canned child hits per alias for reference searches, and
// a primary responder that hands out a fresh cursor per scan page.
type scanBackend struct {
	mu sync.Mutex

	targets    map[string][]string // alias -> targets; a missing alias has none
	aliasErr   error
	aliasCalls int

	childHits map[string][]SearchHit // alias -> hits of a reference child search

	// respond answers a primary search; nil answers a scan page with one hit
	// and the cursor "cur-<n>", n counting primary calls.
	respond func(c scanCall) (SearchResponse, error)

	calls   []scanCall
	primary []scanCall
}

func (b *scanBackend) Upsert(context.Context, string, string, any, int64) error { return nil }
func (b *scanBackend) BulkUpsert(context.Context, []BulkItem) ([]BulkFailure, error) {
	return nil, nil
}
func (b *scanBackend) Delete(context.Context, string, string, int64) error { return nil }
func (b *scanBackend) FederatedSearch(context.Context, FederatedSearchParams) (FederatedSearchResult, error) {
	return FederatedSearchResult{}, nil
}

func (b *scanBackend) GetAliasTargets(_ context.Context, alias string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.aliasCalls++
	if b.aliasErr != nil {
		return nil, b.aliasErr
	}
	return b.targets[alias], nil
}

func (b *scanBackend) Search(_ context.Context, req SearchRequest, index string, vc *resource.VersionConfig) (SearchResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := scanCall{req: req, index: index, vc: vc}
	b.calls = append(b.calls, c)
	if hits, ok := b.childHits[index]; ok {
		return SearchResponse{Total: int64(len(hits)), Hits: hits}, nil
	}
	b.primary = append(b.primary, c)
	if b.respond != nil {
		return b.respond(c)
	}
	n := len(b.primary)
	resp := SearchResponse{Total: 10, Hits: []SearchHit{{ID: fmt.Sprintf("p%d", n)}}}
	if req.Scan {
		resp.NextPageToken = fmt.Sprintf("cur-%d", n)
	}
	return resp, nil
}

func (b *scanBackend) setAlias(alias string, targets ...string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.targets == nil {
		b.targets = map[string][]string{}
	}
	b.targets[alias] = targets
}

func (b *scanBackend) setChildHits(alias string, hits ...SearchHit) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.childHits == nil {
		b.childHits = map[string][]SearchHit{}
	}
	b.childHits[alias] = hits
}

func (b *scanBackend) lastPrimary(t *testing.T) scanCall {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.primary) == 0 {
		t.Fatal("the backend received no primary search")
	}
	return b.primary[len(b.primary)-1]
}

// scanProductV1 has a field "old" that scanProductV2 drops for "new"; both
// have a reference relation to "brand".
func scanProductV1() resource.VersionConfig {
	return resource.VersionConfig{
		Version: 1,
		Fields: []resource.FieldConfig{
			{Name: "title", Type: "text", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}},
			{Name: "brand_id"},
			{Name: "old"},
		},
		Relations: []resource.RelationConfig{{
			Resource: "brand", Strategy: resource.StrategyReference,
			Join:   resource.JoinConfig{Local: "brand_id", Foreign: "id"},
			Fields: []resource.FieldConfig{{Name: "name"}},
		}},
	}
}

func scanProductV2() resource.VersionConfig {
	vc := scanProductV1()
	vc.Version = 2
	vc.Fields = []resource.FieldConfig{
		{Name: "title", Type: "text", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}},
		{Name: "brand_id"},
		{Name: "new"},
	}
	return vc
}

// scanResources configures "product" with the given versions, read at
// readVersion, and its reference child "brand".
func scanResources(readVersion int, versions ...resource.VersionConfig) resource.Configs {
	return resource.Configs{
		{Resource: "product", ReadVersion: readVersion, Versions: versions},
		{Resource: "brand", ReadVersion: 1, Versions: []resource.VersionConfig{{
			Version: 1, Fields: []resource.FieldConfig{{Name: "name"}},
		}}},
	}
}

// scanClock is a settable clock for Indexer.now.
type scanClock struct{ t time.Time }

func (c *scanClock) now() time.Time          { return c.t }
func (c *scanClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// newScanIndexer builds an Indexer over scanResources(1, v1) whose read alias
// serves product_search_v1, with a settable clock.
func newScanIndexer(t *testing.T, be *scanBackend, mws ...SearchMiddleware) (*Indexer, *scanClock) {
	t.Helper()
	be.setAlias(AliasName("product"), IndexName("product", 1))
	idx, err := New(Config{
		Resources:         scanResources(1, scanProductV1()),
		ES:                be,
		SearchMiddlewares: mws,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	clock := &scanClock{t: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
	idx.now = clock.now
	return idx, clock
}

func scanReq() SearchRequest {
	return SearchRequest{Resource: "product", Scan: true}
}

// nextPage returns req continued with the token of resp.
func nextPage(t *testing.T, req SearchRequest, resp SearchResponse) SearchRequest {
	t.Helper()
	if resp.NextPageToken == "" {
		t.Fatal("expected a NextPageToken")
	}
	req.PageToken = resp.NextPageToken
	return req
}

func mustDecodeToken(t *testing.T, s string) scanToken {
	t.Helper()
	tok, err := decodeScanToken(s)
	if err != nil {
		t.Fatalf("decode token %q: %v", s, err)
	}
	return tok
}

func assertInvalidArgument(t *testing.T, err error) {
	t.Helper()
	var ia *InvalidArgumentError
	if !errors.As(err, &ia) {
		t.Fatalf("expected *InvalidArgumentError, got %v", err)
	}
}

// ---- Token ----

func TestScan_GarbledTokenIsInvalidPageToken(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be)

	for _, tok := range []string{"%%%not-base64%%%", "bm90IGpzb24", "e30"} { // garbage, "not json", "{}"
		req := scanReq()
		req.PageToken = tok
		if _, err := idx.Search(context.Background(), req); !errors.Is(err, ErrInvalidPageToken) {
			t.Fatalf("token %q: expected ErrInvalidPageToken, got %v", tok, err)
		}
	}
	if len(be.calls) != 0 {
		t.Fatalf("the backend was called: %+v", be.calls)
	}
}

func TestScan_TokenWithEmptyCursorIsInvalidPageTokenAndOpensNothing(t *testing.T) {
	be := &scanBackend{}
	idx, clock := newScanIndexer(t, be)

	req := scanReq()
	req.PageToken = encodeScanToken(scanToken{
		Start: clock.t.UnixNano(), Version: 1, Index: IndexName("product", 1),
		Fingerprint: scanFingerprint(normalizeScanPageSize(req), 1, IndexName("product", 1), idx.resources.Get("product").GetVersion(1)),
	})
	if _, err := idx.Search(context.Background(), req); !errors.Is(err, ErrInvalidPageToken) {
		t.Fatalf("expected ErrInvalidPageToken, got %v", err)
	}
	if len(be.calls) != 0 {
		t.Fatalf("the backend was called, so a point in time may have been opened: %+v", be.calls)
	}
}

func TestScan_NextTokenCopiesStartVersionAndIndex(t *testing.T) {
	be := &scanBackend{}
	idx, clock := newScanIndexer(t, be)
	start := clock.t

	resp1, err := idx.Search(context.Background(), scanReq())
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	clock.advance(time.Minute)
	resp2, err := idx.Search(context.Background(), nextPage(t, scanReq(), resp1))
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}

	tok1 := mustDecodeToken(t, resp1.NextPageToken)
	tok2 := mustDecodeToken(t, resp2.NextPageToken)
	if tok2.Start != start.UnixNano() || tok2.Start != tok1.Start {
		t.Fatalf("page 2's token start %d, want page 1's %d", tok2.Start, start.UnixNano())
	}
	if tok2.Version != 1 || tok2.Index != IndexName("product", 1) {
		t.Fatalf("page 2's token pins v%d %q, want v1 %q", tok2.Version, tok2.Index, IndexName("product", 1))
	}
	if tok1.Cursor != "cur-1" || tok2.Cursor != "cur-2" {
		t.Fatalf("cursors %q, %q; want cur-1, cur-2", tok1.Cursor, tok2.Cursor)
	}
}

// ---- Age and version ----

func TestScan_TokenOlderThanScanMaxAgeIsCursorExpired(t *testing.T) {
	be := &scanBackend{}
	idx, clock := newScanIndexer(t, be)

	resp1, err := idx.Search(context.Background(), scanReq())
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	clock.advance(59 * time.Minute)
	resp2, err := idx.Search(context.Background(), nextPage(t, scanReq(), resp1))
	if err != nil {
		t.Fatalf("page 2 within the default hour: %v", err)
	}
	clock.advance(time.Minute + time.Second)
	calls := len(be.calls)
	if _, err := idx.Search(context.Background(), nextPage(t, scanReq(), resp2)); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("expected ErrCursorExpired past ScanMaxAge, got %v", err)
	}
	if len(be.calls) != calls {
		t.Fatal("the backend was called for an expired token")
	}
}

func TestNew_ScanMaxAge(t *testing.T) {
	if _, err := New(Config{Resources: scanResources(1, scanProductV1()), ScanMaxAge: -time.Second}); err == nil {
		t.Fatal("New accepted a negative ScanMaxAge")
	}
	idx, err := New(Config{Resources: scanResources(1, scanProductV1())})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if idx.scanMaxAge != time.Hour {
		t.Fatalf("default ScanMaxAge %s, want 1h", idx.scanMaxAge)
	}
}

func TestScan_TokenIndexNotItsVersionsIndexNameIsCursorExpired(t *testing.T) {
	be := &scanBackend{}
	idx, clock := newScanIndexer(t, be)

	req := scanReq()
	req.PageToken = encodeScanToken(scanToken{
		Start: clock.t.UnixNano(), Version: 1, Index: IndexName("product", 2), Cursor: "cur-x",
	})
	if _, err := idx.Search(context.Background(), req); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("expected ErrCursorExpired, got %v", err)
	}
	if len(be.calls) != 0 {
		t.Fatal("the backend was called")
	}
}

func TestScan_TokenVersionRemovedFromConfigIsCursorExpired(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be)

	resp1, err := idx.Search(context.Background(), scanReq())
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if err := idx.SetPlans(nil, scanResources(2, scanProductV2())); err != nil {
		t.Fatalf("SetPlans: %v", err)
	}
	be.setAlias(AliasName("product"), IndexName("product", 2))
	calls := len(be.calls)
	if _, err := idx.Search(context.Background(), nextPage(t, scanReq(), resp1)); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("expected ErrCursorExpired, got %v", err)
	}
	if len(be.calls) != calls {
		t.Fatal("the backend was called")
	}
}

// ---- Page 1 ----

func TestScan_UnknownResourceIsUnknownResourceWithoutAliasLookup(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be)

	_, err := idx.Search(context.Background(), SearchRequest{Resource: "nope", Scan: true})
	if !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("expected ErrUnknownResource, got %v", err)
	}
	if be.aliasCalls != 0 {
		t.Fatalf("GetAliasTargets called %d times", be.aliasCalls)
	}
}

func TestScan_FirstPagePinsTheAliasTarget(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be)
	// The config reads version 2, but the alias still serves version 1: the
	// scan pins what the alias serves.
	if err := idx.SetPlans(nil, scanResources(2, scanProductV1(), scanProductV2())); err != nil {
		t.Fatalf("SetPlans: %v", err)
	}

	resp, err := idx.Search(context.Background(), scanReq())
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	c := be.lastPrimary(t)
	if c.index != IndexName("product", 1) {
		t.Fatalf("backend index %q, want %q", c.index, IndexName("product", 1))
	}
	if c.vc != idx.resources.Get("product").GetVersion(1) {
		t.Fatalf("backend config is version %d, want version 1's", c.vc.Version)
	}
	tok := mustDecodeToken(t, resp.NextPageToken)
	if tok.Version != 1 || tok.Index != IndexName("product", 1) {
		t.Fatalf("token pins v%d %q", tok.Version, tok.Index)
	}
}

func TestScan_UnpinnableAliasIsScanFault(t *testing.T) {
	for name, targets := range map[string][]string{
		"two targets":         {IndexName("product", 1), IndexName("product", 2)},
		"zero-padded version": {"product_search_v02"},
		"signed version":      {"product_search_v+1"},
		"unconfigured":        {IndexName("product", 9)},
		"foreign index":       {"something_else"},
	} {
		t.Run(name, func(t *testing.T) {
			be := &scanBackend{}
			idx, _ := newScanIndexer(t, be)
			if err := idx.SetPlans(nil, scanResources(1, scanProductV1(), scanProductV2())); err != nil {
				t.Fatalf("SetPlans: %v", err)
			}
			be.setAlias(AliasName("product"), targets...)

			if _, err := idx.Search(context.Background(), scanReq()); !errors.Is(err, ErrScanFault) {
				t.Fatalf("expected ErrScanFault, got %v", err)
			}
			if len(be.calls) != 0 {
				t.Fatal("the backend was searched")
			}
		})
	}
}

// Ruling R5: a failed alias lookup is returned wrapped, not as ErrScanFault.
func TestScan_AliasLookupErrorIsReturnedWrapped(t *testing.T) {
	boom := errors.New("transport down")
	be := &scanBackend{aliasErr: boom}
	idx, _ := newScanIndexer(t, be)

	_, err := idx.Search(context.Background(), scanReq())
	if !errors.Is(err, boom) {
		t.Fatalf("expected the lookup's error, got %v", err)
	}
	if errors.Is(err, ErrScanFault) {
		t.Fatalf("a transport error is not a scan fault: %v", err)
	}
}

func TestScan_NoAliasStillRunsTheChain(t *testing.T) {
	t.Run("a denying middleware denies", func(t *testing.T) {
		denied := errors.New("denied")
		be := &scanBackend{}
		idx, _ := newScanIndexer(t, be, func(SearchHandler) SearchHandler {
			return func(context.Context, SearchRequest) (SearchResponse, error) { return SearchResponse{}, denied }
		})
		be.setAlias(AliasName("product"))

		if _, err := idx.Search(context.Background(), scanReq()); !errors.Is(err, denied) {
			t.Fatalf("expected denied, got %v", err)
		}
	})
	t.Run("an allowing middleware gets an empty result", func(t *testing.T) {
		ran := false
		be := &scanBackend{}
		idx, _ := newScanIndexer(t, be, func(next SearchHandler) SearchHandler {
			return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
				ran = true
				return next(ctx, req)
			}
		})
		be.setAlias(AliasName("product"))

		resp, err := idx.Search(context.Background(), scanReq())
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if !ran {
			t.Fatal("the middleware didn't run")
		}
		if resp.Total != 0 || len(resp.Hits) != 0 || resp.NextPageToken != "" {
			t.Fatalf("expected an empty result with no token, got %+v", resp)
		}
		if len(be.calls) != 0 {
			t.Fatal("the backend was searched without an alias")
		}
	})
}

// ---- Pinning ----

func TestScan_LaterPageUsesThePinnedVersionAfterCutover(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be)

	req := scanReq()
	req.Filters = []Filter{{Field: "fields.old", Op: FilterOpNeq, Value: "x"}}
	resp1, err := idx.Search(context.Background(), req)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}

	// The cutover: read version 2, still listing version 1, alias on v2.
	if err := idx.SetPlans(nil, scanResources(2, scanProductV1(), scanProductV2())); err != nil {
		t.Fatalf("SetPlans: %v", err)
	}
	be.setAlias(AliasName("product"), IndexName("product", 2))

	if _, err := idx.Search(context.Background(), nextPage(t, req, resp1)); err != nil {
		t.Fatalf("page 2 with a filter on a field only the pinned version has: %v", err)
	}
	c := be.lastPrimary(t)
	if c.index != IndexName("product", 1) {
		t.Fatalf("page 2 index %q, want %q", c.index, IndexName("product", 1))
	}
	if c.vc != idx.resources.Get("product").GetVersion(1) {
		t.Fatalf("page 2 config is version %d, want the pinned version 1's", c.vc.Version)
	}

	onNew := nextPage(t, req, resp1)
	onNew.Filters = []Filter{{Field: "fields.new", Op: FilterOpNeq, Value: "x"}}
	_, err = idx.Search(context.Background(), onNew)
	assertInvalidArgument(t, err)
}

// ---- Context isolation ----

func TestScan_ReferenceChildSearchIsAPlainSearch(t *testing.T) {
	be := &scanBackend{}
	be.setChildHits(AliasName("brand"), SearchHit{ID: "b1"})
	idx, _ := newScanIndexer(t, be)

	req := scanReq()
	req.Filters = []Filter{{Field: "brand.name", Op: FilterOpEq, Value: "acme"}}
	resp1, err := idx.Search(context.Background(), req)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	// Between pages the child moves to a new read version with another
	// field; the parent's config stays as it was. Page 2's child search uses
	// the child's current config, not page 1's, nor the parent's pinned one.
	resources := scanResources(1, scanProductV1())
	brand := resources.Get("brand")
	brand.Versions = append(brand.Versions, resource.VersionConfig{
		Version: 2, Fields: []resource.FieldConfig{{Name: "name"}, {Name: "country"}},
	})
	brand.ReadVersion = 2
	if err := idx.SetPlans(nil, resources); err != nil {
		t.Fatalf("SetPlans: %v", err)
	}
	be.calls = nil
	if _, err := idx.Search(context.Background(), nextPage(t, req, resp1)); err != nil {
		t.Fatalf("page 2: %v", err)
	}

	var child *scanCall
	for i := range be.calls {
		if be.calls[i].index == AliasName("brand") {
			child = &be.calls[i]
		}
	}
	if child == nil {
		t.Fatalf("no child search reached the child's alias: %+v", be.calls)
	}
	if child.req.Scan || child.req.PageToken != "" || child.req.Resource != "brand" {
		t.Fatalf("child search Resource=%q Scan=%v PageToken=%q, want a plain search of brand",
			child.req.Resource, child.req.Scan, child.req.PageToken)
	}
	if child.vc == nil || child.vc.Version != 2 || len(child.vc.Fields) != 2 || child.vc.Fields[1].Name != "country" {
		t.Fatalf("child search got config %+v, want brand's current version 2 with field country", child.vc)
	}
	if p := be.lastPrimary(t); p.req.PageToken != "cur-1" || !p.req.Scan {
		t.Fatalf("primary got Scan=%v PageToken=%q", p.req.Scan, p.req.PageToken)
	}
}

func TestScan_SearchesInsideAScanMiddlewareRunWithoutItsState(t *testing.T) {
	be := &scanBackend{}
	var innerPagedState, innerFedState, outerAfter *scanState
	var idx *Indexer
	mw := func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			if !req.Scan {
				innerPagedState = scanStateFrom(ctx)
				return next(ctx, req)
			}
			if _, err := idx.Search(ctx, SearchRequest{Resource: "product"}); err != nil {
				return SearchResponse{}, err
			}
			if _, err := idx.FederatedSearch(ctx, FederatedSearchRequest{Resources: []string{"product"}}); err != nil {
				return SearchResponse{}, err
			}
			outerAfter = scanStateFrom(ctx)
			return next(ctx, req)
		}
	}
	idx, _ = newScanIndexer(t, be, mw)
	idx.federatedSearchChain = chain(idx.federatedSearchBase, []FederatedSearchMiddleware{
		func(next FederatedSearchHandler) FederatedSearchHandler {
			return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
				innerFedState = scanStateFrom(ctx)
				return next(ctx, req)
			}
		},
	})
	if err := idx.SetPlans(nil, scanResources(2, scanProductV1(), scanProductV2())); err != nil {
		t.Fatalf("SetPlans: %v", err)
	}

	resp, err := idx.Search(context.Background(), scanReq())
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if innerPagedState != nil {
		t.Fatal("a paged search inside the scan saw the scan's state")
	}
	if innerFedState != nil {
		t.Fatal("a federated search inside the scan saw the scan's state")
	}
	if outerAfter == nil || outerAfter.index != IndexName("product", 1) {
		t.Fatalf("the outer scan lost its state: %+v", outerAfter)
	}
	// The inner paged search went to the alias with the read config; the
	// scan to the pinned index with the pinned config.
	if len(be.primary) != 2 {
		t.Fatalf("expected 2 primary searches, got %d", len(be.primary))
	}
	paged, scan := be.primary[0], be.primary[1]
	if paged.req.Scan || paged.index != AliasName("product") || paged.vc.Version != 2 || paged.req.PageSize != 25 {
		t.Fatalf("inner paged search: scan=%v index=%q v%d size=%d", paged.req.Scan, paged.index, paged.vc.Version, paged.req.PageSize)
	}
	if !scan.req.Scan || scan.index != IndexName("product", 1) || scan.vc.Version != 1 {
		t.Fatalf("outer scan: scan=%v index=%q v%d", scan.req.Scan, scan.index, scan.vc.Version)
	}
	if resp.NextPageToken == "" {
		t.Fatal("the outer scan returned no token")
	}
}

func TestScan_MiddlewareDroppingTheContextIsScanFault(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be, func(next SearchHandler) SearchHandler {
		return func(_ context.Context, req SearchRequest) (SearchResponse, error) {
			return next(context.Background(), req)
		}
	})
	if err := idx.SetPlans(nil, scanResources(2, scanProductV1(), scanProductV2())); err != nil {
		t.Fatalf("SetPlans: %v", err)
	}

	if _, err := idx.Search(context.Background(), scanReq()); !errors.Is(err, ErrScanFault) {
		t.Fatalf("expected ErrScanFault, got %v", err)
	}
	if len(be.calls) != 0 {
		t.Fatalf("the backend was called: %+v", be.calls)
	}
}

func TestScan_MiddlewareChangingTheResourceIsScanFault(t *testing.T) {
	be := &scanBackend{}
	be.setAlias(AliasName("brand"), IndexName("brand", 1))
	idx, _ := newScanIndexer(t, be, func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			req.Resource = "brand"
			return next(ctx, req)
		}
	})

	if _, err := idx.Search(context.Background(), scanReq()); !errors.Is(err, ErrScanFault) {
		t.Fatalf("expected ErrScanFault, got %v", err)
	}
	if len(be.calls) != 0 {
		t.Fatalf("the backend was called: %+v", be.calls)
	}
}

// Every stage below the fingerprint middleware refuses a scan whose state is
// missing, for another resource, or without a config.
func TestScan_StagesRefuseAScanWithoutItsState(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be)
	pass := func(context.Context, SearchRequest) (SearchResponse, error) { return SearchResponse{}, nil }
	req := scanReq()

	states := map[string]context.Context{
		"no state":         context.Background(),
		"another resource": withScanState(context.Background(), &scanState{resource: "brand", vc: idx.resources.Get("brand").ReadVersionConfig()}),
		"nil config":       withScanState(context.Background(), &scanState{resource: "product", index: IndexName("product", 1)}),
	}
	stages := map[string]SearchHandler{
		"deriveNestedPath": idx.deriveNestedPath(pass),
		"referenceResolve": idx.referenceResolve(pass),
		"searchBase":       idx.searchBase,
	}
	for sn, ctx := range states {
		for gn, stage := range stages {
			if _, err := stage(ctx, req); !errors.Is(err, ErrScanFault) {
				t.Errorf("%s with %s: expected ErrScanFault, got %v", gn, sn, err)
			}
		}
	}
	if len(be.calls) != 0 {
		t.Fatalf("the backend was called: %+v", be.calls)
	}
}

// ---- Fingerprint ----

// TestScanFingerprint_CoversEveryRequestField changes each field of
// SearchRequest, and of its Filter and SortOption elements, by reflection,
// and expects the fingerprint to change: a field added to any of them that
// the fingerprint doesn't encode fails here. PageToken alone is exempt.
func TestScanFingerprint_CoversEveryRequestField(t *testing.T) {
	vc := scanProductV1()
	base := SearchRequest{
		Resource: "product", Query: "q", PageSize: 10, Scope: "s", Scan: true,
		Filters: []Filter{{Field: "fields.old", Op: FilterOpIn, Value: "v", Values: []string{"a"}, NestedPath: "n"}},
		Sort:    []SortOption{{Field: "fields.old", Desc: false}},
	}
	fp := func(r SearchRequest) string { return scanFingerprint(r, 1, IndexName("product", 1), &vc) }
	want := fp(base)

	mutate := func(t *testing.T, f reflect.Value) {
		t.Helper()
		switch f.Kind() {
		case reflect.String:
			f.SetString(f.String() + "-changed")
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			f.SetInt(f.Int() + 1)
		case reflect.Bool:
			f.SetBool(!f.Bool())
		case reflect.Slice:
			if f.Type().Elem().Kind() != reflect.String {
				t.Fatalf("unhandled slice type %s: extend this test", f.Type())
			}
			f.Set(reflect.Append(f, reflect.ValueOf("added")))
		default:
			t.Fatalf("unhandled field kind %s: extend the fingerprint and this test", f.Kind())
		}
	}

	rt := reflect.TypeFor[SearchRequest]()
	for i := 0; i < rt.NumField(); i++ {
		name := rt.Field(i).Name
		if name == "PageToken" {
			continue
		}
		switch name {
		case "Filters":
			ft := reflect.TypeFor[Filter]()
			for j := 0; j < ft.NumField(); j++ {
				r := base
				r.Filters = []Filter{base.Filters[0]}
				r.Filters[0].Values = append([]string(nil), base.Filters[0].Values...)
				mutate(t, reflect.ValueOf(&r.Filters[0]).Elem().Field(j))
				if fp(r) == want {
					t.Errorf("the fingerprint doesn't encode Filter.%s", ft.Field(j).Name)
				}
			}
			r := base
			r.Filters = append([]Filter{}, base.Filters[0], base.Filters[0])
			if fp(r) == want {
				t.Error("the fingerprint doesn't encode the number of filters")
			}
		case "Sort":
			st := reflect.TypeFor[SortOption]()
			for j := 0; j < st.NumField(); j++ {
				r := base
				r.Sort = []SortOption{base.Sort[0]}
				mutate(t, reflect.ValueOf(&r.Sort[0]).Elem().Field(j))
				if fp(r) == want {
					t.Errorf("the fingerprint doesn't encode SortOption.%s", st.Field(j).Name)
				}
			}
		default:
			r := base
			mutate(t, reflect.ValueOf(&r).Elem().Field(i))
			if fp(r) == want {
				t.Errorf("the fingerprint doesn't encode SearchRequest.%s", name)
			}
		}
	}

	// PageToken is not part of it; the pinned version, index and config are.
	r := base
	r.PageToken = "anything"
	if fp(r) != want {
		t.Error("the fingerprint encodes PageToken")
	}
	if scanFingerprint(base, 2, IndexName("product", 1), &vc) == want {
		t.Error("the fingerprint doesn't encode the pinned version")
	}
	if scanFingerprint(base, 1, "other", &vc) == want {
		t.Error("the fingerprint doesn't encode the pinned index")
	}
	edited := scanProductV1()
	edited.Fields[0].Name = "headline"
	if scanFingerprint(base, 1, IndexName("product", 1), &edited) == want {
		t.Error("the fingerprint doesn't encode the pinned config")
	}
	// Length prefixes keep adjacent strings apart.
	a, b := base, base
	a.Resource, a.Query = "productq", ""
	b.Resource, b.Query = "product", "q"
	if fp(a) == fp(b) {
		t.Error("the fingerprint runs adjacent strings together")
	}
}

func TestScan_TokenWithAChangedRequestIsInvalidPageToken(t *testing.T) {
	base := func() SearchRequest {
		r := scanReq()
		r.Query = "q"
		r.PageSize = 100
		r.Scope = "tenant-1"
		r.Filters = []Filter{
			{Field: "fields.old", Op: FilterOpEq, Value: "x"},
			{Field: "fields.brand_id", Op: FilterOpIn, Values: []string{"a", "b"}},
		}
		r.Sort = []SortOption{{Field: "fields.old"}}
		return r
	}
	for name, change := range map[string]func(r *SearchRequest){
		"query":              func(r *SearchRequest) { r.Query = "other" },
		"filter field":       func(r *SearchRequest) { r.Filters[0].Field = "fields.brand_id" },
		"filter op":          func(r *SearchRequest) { r.Filters[0].Op = FilterOpNeq },
		"filter value":       func(r *SearchRequest) { r.Filters[0].Value = "y" },
		"filter values":      func(r *SearchRequest) { r.Filters[1].Values = []string{"a", "c"} },
		"filter nested path": func(r *SearchRequest) { r.Filters[0].NestedPath = "brand" },
		"filter dropped":     func(r *SearchRequest) { r.Filters = r.Filters[:1] },
		"sort field":         func(r *SearchRequest) { r.Sort[0].Field = "fields.brand_id" },
		"sort direction":     func(r *SearchRequest) { r.Sort[0].Desc = true },
		"page size":          func(r *SearchRequest) { r.PageSize = 50 },
		"scope":              func(r *SearchRequest) { r.Scope = "tenant-2" },
	} {
		t.Run(name, func(t *testing.T) {
			be := &scanBackend{}
			idx, _ := newScanIndexer(t, be)
			resp1, err := idx.Search(context.Background(), base())
			if err != nil {
				t.Fatalf("page 1: %v", err)
			}
			req := nextPage(t, base(), resp1)
			change(&req)
			calls := len(be.calls)
			if _, err := idx.Search(context.Background(), req); !errors.Is(err, ErrInvalidPageToken) {
				t.Fatalf("expected ErrInvalidPageToken, got %v", err)
			}
			if len(be.calls) != calls {
				t.Fatal("the backend was called")
			}
		})
	}
}

func TestScan_PinnedVersionEditedInPlaceIsInvalidPageToken(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be)

	resp1, err := idx.Search(context.Background(), scanReq())
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	vc := idx.resources.Get("product").GetVersion(1)
	vc.Fields = append(vc.Fields, resource.FieldConfig{Name: "added"})

	if _, err := idx.Search(context.Background(), nextPage(t, scanReq(), resp1)); !errors.Is(err, ErrInvalidPageToken) {
		t.Fatalf("expected ErrInvalidPageToken, got %v", err)
	}
}

func TestScan_MiddlewareClearingScanOrRewritingTokenFails(t *testing.T) {
	for name, mw := range map[string]func(req *SearchRequest){
		"clears Scan":        func(req *SearchRequest) { req.Scan = false },
		"rewrites PageToken": func(req *SearchRequest) { req.PageToken = "rewritten" },
		"clears PageToken":   func(req *SearchRequest) { req.PageToken = "" },
	} {
		t.Run(name, func(t *testing.T) {
			be := &scanBackend{}
			armed := false
			idx, _ := newScanIndexer(t, be, func(next SearchHandler) SearchHandler {
				return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
					if armed {
						mw(&req)
					}
					return next(ctx, req)
				}
			})
			resp1, err := idx.Search(context.Background(), scanReq())
			if err != nil {
				t.Fatalf("page 1: %v", err)
			}
			armed = true
			calls := len(be.calls)
			if _, err := idx.Search(context.Background(), nextPage(t, scanReq(), resp1)); !errors.Is(err, ErrScanFault) {
				t.Fatalf("expected ErrScanFault, got %v", err)
			}
			if len(be.calls) != calls {
				t.Fatal("the backend was called")
			}
		})
	}
}

// ---- Paging edges ----

func TestScan_ChangedReferenceChildResultIsNotRejected(t *testing.T) {
	be := &scanBackend{}
	be.setChildHits(AliasName("brand"), SearchHit{ID: "b1"})
	idx, _ := newScanIndexer(t, be)

	req := scanReq()
	req.Filters = []Filter{{Field: "brand.name", Op: FilterOpEq, Value: "acme"}}
	resp1, err := idx.Search(context.Background(), req)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	be.setChildHits(AliasName("brand"), SearchHit{ID: "b2"}, SearchHit{ID: "b3"})

	if _, err := idx.Search(context.Background(), nextPage(t, req, resp1)); err != nil {
		t.Fatalf("page 2 after the child result changed: %v", err)
	}
	folded := be.lastPrimary(t).req.Filters
	if len(folded) != 1 || len(folded[0].Values) != 2 {
		t.Fatalf("page 2 didn't fold the child's current result: %+v", folded)
	}
}

func TestScan_ReferenceMatchingNothingOnALaterPageEndsTheScan(t *testing.T) {
	be := &scanBackend{}
	be.setChildHits(AliasName("brand"), SearchHit{ID: "b1"})
	idx, _ := newScanIndexer(t, be)

	req := scanReq()
	req.Filters = []Filter{{Field: "brand.name", Op: FilterOpEq, Value: "acme"}}
	resp1, err := idx.Search(context.Background(), req)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	be.setChildHits(AliasName("brand"))

	resp2, err := idx.Search(context.Background(), nextPage(t, req, resp1))
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if resp2.NextPageToken != "" || resp2.Total != 0 || len(resp2.Hits) != 0 {
		t.Fatalf("expected an empty last page, got %+v", resp2)
	}
}

func TestScan_EmptyBackendCursorIsEmptyNextPageToken(t *testing.T) {
	be := &scanBackend{respond: func(scanCall) (SearchResponse, error) {
		return SearchResponse{Total: 1, Hits: []SearchHit{{ID: "p1"}}}, nil
	}}
	idx, _ := newScanIndexer(t, be)

	resp, err := idx.Search(context.Background(), scanReq())
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if resp.NextPageToken != "" {
		t.Fatalf("expected no token, got %q", resp.NextPageToken)
	}
	if resp.Total != 1 || len(resp.Hits) != 1 {
		t.Fatalf("the page's hits were lost: %+v", resp)
	}
}

func TestScan_MiddlewareFilterReachesEveryPage(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be, appendFilter("fields.tenant"))

	resp, err := idx.Search(context.Background(), scanReq())
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	for page := 2; page <= 3; page++ {
		if resp, err = idx.Search(context.Background(), nextPage(t, scanReq(), resp)); err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
	}
	if len(be.primary) != 3 {
		t.Fatalf("expected 3 pages, got %d", len(be.primary))
	}
	for i, c := range be.primary {
		if len(c.req.Filters) != 1 || c.req.Filters[0].Field != "fields.tenant" {
			t.Fatalf("page %d reached the backend with filters %+v", i+1, c.req.Filters)
		}
	}
}

func TestScan_BackendGetsScanThenItsOwnCursor(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be)

	resp1, err := idx.Search(context.Background(), scanReq())
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if _, err := idx.Search(context.Background(), nextPage(t, scanReq(), resp1)); err != nil {
		t.Fatalf("page 2: %v", err)
	}
	p1, p2 := be.primary[0], be.primary[1]
	if !p1.req.Scan || p1.req.PageToken != "" {
		t.Fatalf("page 1: Scan=%v PageToken=%q", p1.req.Scan, p1.req.PageToken)
	}
	if !p2.req.Scan || p2.req.PageToken != "cur-1" {
		t.Fatalf("page 2: Scan=%v PageToken=%q, want cur-1", p2.req.Scan, p2.req.PageToken)
	}
	if resp1.NextPageToken == "cur-1" {
		t.Fatal("the backend cursor reached the caller unwrapped")
	}
}

// ---- Request shape ----

func TestScan_NonZeroPageIsInvalidArgument(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be)

	req := scanReq()
	req.Page = 1
	_, err := idx.Search(context.Background(), req)
	assertInvalidArgument(t, err)
	if len(be.calls) != 0 || be.aliasCalls != 0 {
		t.Fatal("the backend was called")
	}
}

func TestScan_PageSizeIsNormalizedBeforeFingerprinting(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be)

	req := scanReq() // PageSize 0
	resp, err := idx.Search(context.Background(), req)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	for _, size := range []int32{1000, 5000, 0} {
		req = nextPage(t, scanReq(), resp)
		req.PageSize = size
		if resp, err = idx.Search(context.Background(), req); err != nil {
			t.Fatalf("page size %d after 0: %v", size, err)
		}
	}
	for i, c := range be.primary {
		if c.req.PageSize != 1000 {
			t.Fatalf("page %d reached the backend with page size %d, want 1000", i+1, c.req.PageSize)
		}
	}

	// A size under the cap passes through.
	small := scanReq()
	small.PageSize = 7
	if _, err := idx.Search(context.Background(), small); err != nil {
		t.Fatalf("page size 7: %v", err)
	}
	if got := be.lastPrimary(t).req.PageSize; got != 7 {
		t.Fatalf("page size 7 reached the backend as %d", got)
	}
}

// Ruling R7: a page token without Scan is a caller's mistake, not page 1.
func TestScan_PageTokenWithoutScanIsInvalidArgument(t *testing.T) {
	be := &scanBackend{}
	idx, _ := newScanIndexer(t, be)

	resp1, err := idx.Search(context.Background(), scanReq())
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	req := nextPage(t, scanReq(), resp1)
	req.Scan = false
	calls := len(be.calls)
	_, err = idx.Search(context.Background(), req)
	assertInvalidArgument(t, err)
	if len(be.calls) != calls {
		t.Fatal("the backend was called")
	}
}
