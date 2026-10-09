package tests

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

// The scan cases page single-resource searches with Scan set, each page
// continuing from the previous page's NextPageToken, and check that the pages
// hold the scan's snapshot: every matching document exactly once, in the
// search's sort with resource_id last, whatever is written between pages.
//
// Field values are letter/digit runs of two or more characters, so each
// survives the n-gram analysis of the primary surface (see
// Test_ConcurrentRequests_RelatedParent_ConcurrentChildUpdatesConverge).

// ScanResourceConfig is the scan fixture's type "s": a keyword field and a
// date field to sort on.
var ScanResourceConfig = resource.Configs{
	{
		Resource: "s",
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields: []resource.FieldConfig{
				{Name: "name", Query: primaryTier},
				{Name: "rank", Query: primaryTier},
				// A date is no text to search; its tier is declared as none.
				{Name: "stamp", Type: "date", Query: resource.QueryConfig{Search: resource.SearchTierNone}},
			},
		}},
		ReadVersion: 1,
	},
}

// ScanCutoverConfig is type "cv" before its cutover: version 1 serves and has
// the field grade, which version 2 drops. ScanCutoverReadV2 is the same type
// after the cutover, still listing version 1 as a real cutover does until
// version 1 is retired.
var (
	ScanCutoverConfig = resource.Configs{
		{
			Resource: "cv",
			Versions: []resource.VersionConfig{
				{Version: 1, Fields: []resource.FieldConfig{
					{Name: "name", Query: primaryTier},
					{Name: "grade", Query: primaryTier},
				}},
				{Version: 2, Fields: []resource.FieldConfig{
					{Name: "name", Query: primaryTier},
				}},
			},
			ReadVersion: 1,
		},
	}
	ScanCutoverReadV2 = func() resource.Configs {
		cut := *ScanCutoverConfig[0]
		cut.ReadVersion = 2
		return resource.Configs{&cut}
	}()
)

// setScanConfig applies defaults to the config, validates it and makes it the
// suite indexer's, with its indexes and read alias.
func (t *TestSuite) setScanConfig(cfgs resource.Configs) {
	for _, c := range cfgs {
		c.ApplyDefaults()
	}
	t.Require().NoError(cfgs.Validate())
	t.setResourceConfig(cfgs)
}

// registerChunk is the most notifications one RegisterChanges call of the
// seeding carries before it drains: under the suite's QueueSize of 1,000, so
// no build is shed to the sweep.
const registerChunk = 500

// seedBuilt stores each id's data in the fake provider and builds them all
// through the real build path, registering at most registerChunk at a time
// and draining after each. The ids are new to the type: a plain search of it
// must count exactly len(ids) documents more afterwards.
func (t *TestSuite) seedBuilt(resourceType string, ids []string, data map[string]map[string]any) {
	t.T().Helper()
	count := func() int64 {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: resourceType})
		t.Require().NoError(err)
		return resp.Total
	}
	before := count()
	for _, id := range ids {
		t.fakeProvider.SetResource(resourceType, id, data[id])
	}
	for chunk := range slices.Chunk(ids, registerChunk) {
		ns := make([]core.Notification, len(chunk))
		for i, id := range chunk {
			ns[i] = core.Notification{ResourceType: resourceType, ResourceID: id, Kind: core.ChangeCreated}
		}
		statuses, err := t.idx.RegisterChanges(t.T().Context(), ns)
		t.Require().NoError(err)
		for i, s := range statuses {
			t.Require().Equalf(core.RegisterAccepted, s, "registration of %s/%s", resourceType, chunk[i])
		}
		t.worker.Drain(t.T().Context())
	}

	t.Require().EqualValuesf(before+int64(len(ids)), count(), "built documents of %s", resourceType)
}

// rebuildWith changes resourceType/id's data at the source and rebuilds it:
// a change with Version 0 (always accepted), then a drain.
func (t *TestSuite) rebuildWith(resourceType, id string, data map[string]any) {
	t.T().Helper()
	t.fakeProvider.SetResource(resourceType, id, data)
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: resourceType, ResourceID: id, Kind: core.ChangeUpdated, Version: 0,
	}))
	t.worker.Drain(t.T().Context())
}

// scanPage is one page of a scan: its hits' ids, its total and its token.
type scanPage struct {
	ids   []string
	total int64
	next  string
}

// scanNext takes one scan page on idx: req with Scan set, continuing from
// token (empty for page 1).
func (t *TestSuite) scanNext(idx *core.Indexer, req core.SearchRequest, token string) (scanPage, error) {
	req.Scan = true
	req.PageToken = token
	resp, err := idx.Search(t.T().Context(), req)
	if err != nil {
		return scanPage{}, err
	}
	p := scanPage{total: resp.Total, next: resp.NextPageToken}
	for _, h := range resp.Hits {
		p.ids = append(p.ids, h.ID)
	}
	return p, nil
}

// maxScanPages bounds a scan the cases run to its end, so a token that never
// empties fails the case instead of looping.
const maxScanPages = 20

// scanToEnd runs a scan on the suite indexer until a page comes back without
// a token. between runs after each page that has one, before the next is
// taken, with that page's number (1 for the first).
func (t *TestSuite) scanToEnd(req core.SearchRequest, between func(page int)) []scanPage {
	t.T().Helper()
	var (
		pages []scanPage
		token string
	)
	for n := 1; ; n++ {
		t.Require().LessOrEqualf(n, maxScanPages, "the scan has not ended after %d pages", maxScanPages)
		p, err := t.scanNext(t.idx, req, token)
		t.Require().NoErrorf(err, "scan page %d", n)
		pages = append(pages, p)
		if p.next == "" {
			return pages
		}
		if between != nil {
			between(n)
		}
		token = p.next
	}
}

// scanIDs concatenates the pages' ids in scan order.
func scanIDs(pages []scanPage) []string {
	var ids []string
	for _, p := range pages {
		ids = append(ids, p.ids...)
	}
	return ids
}

// requireExactlyOnce fails unless got holds every id of want exactly once
// and nothing else, in any order.
func (t *TestSuite) requireExactlyOnce(want, got []string, label string) {
	t.T().Helper()
	counts := map[string]int{}
	for _, id := range got {
		counts[id]++
	}
	var dup []string
	for id, n := range counts {
		if n > 1 {
			dup = append(dup, fmt.Sprintf("%s×%d", id, n))
		}
	}
	slices.Sort(dup)
	t.Require().Emptyf(dup, "%s: ids returned more than once", label)
	t.Require().Equalf(slices.Sorted(slices.Values(want)), slices.Sorted(slices.Values(got)),
		"%s: the ids returned must be exactly the matching ones", label)
}

// --- The large fixture: 2,500 documents of "s". ---

// The large fixture's sort-field roles. Each document takes one role per sort
// field from a position j in a permutation of its index (a different one per
// field, so neither field's order is the id order or the other's): the first
// 400 positions hold distinct low values, the next 1,250 — half the documents
// — one tied value, the next 750 distinct high values, and the last 100 no
// value at all.
const (
	scanLargeN    = 2500
	scanLowEnd    = 400
	scanTieEnd    = scanLowEnd + scanLargeN/2 // 1,650
	scanHighEnd   = scanLargeN - 100          // 2,400
	scanLargePage = 1000
	// scanMissingPage puts the scan's last page boundary, at 2,460, inside
	// the block of the 100 documents without the sort field.
	scanMissingPage = 820
)

// scanLargeDoc is one large-fixture document's sort values; a nil one is
// missing from the document.
type scanLargeDoc struct {
	id    string
	rank  *string
	stamp *time.Time
}

var scanTieStamp = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

// scanLargeDocs builds the large fixture's documents.
func scanLargeDocs() []scanLargeDoc {
	lowBase := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	highBase := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	docs := make([]scanLargeDoc, scanLargeN)
	for i := range docs {
		d := scanLargeDoc{id: fmt.Sprintf("s%04d", i)}

		// 1543 and 1031 are coprime with 2,500, so each is a permutation.
		switch j := (i * 1543) % scanLargeN; {
		case j < scanLowEnd:
			d.rank = new(fmt.Sprintf("k%04d", j))
		case j < scanTieEnd:
			d.rank = new("tie")
		case j < scanHighEnd:
			d.rank = new(fmt.Sprintf("x%04d", j))
		}
		switch j := (i*1031 + 7) % scanLargeN; {
		case j < scanLowEnd:
			d.stamp = new(lowBase.Add(time.Duration(j) * time.Minute))
		case j < scanTieEnd:
			d.stamp = new(scanTieStamp)
		case j < scanHighEnd:
			d.stamp = new(highBase.Add(time.Duration(j) * time.Minute))
		}
		docs[i] = d
	}
	return docs
}

// seedScanLarge builds the large fixture and returns its documents.
func (t *TestSuite) seedScanLarge() []scanLargeDoc {
	t.setScanConfig(ScanResourceConfig)
	docs := scanLargeDocs()
	ids := make([]string, len(docs))
	data := make(map[string]map[string]any, len(docs))
	for i, d := range docs {
		ids[i] = d.id
		m := map[string]any{"id": d.id, "name": "item" + d.id}
		if d.rank != nil {
			m["rank"] = *d.rank
		}
		if d.stamp != nil {
			m["stamp"] = d.stamp.Format(time.RFC3339)
		}
		data[d.id] = m
	}
	t.seedBuilt("s", ids, data)
	return docs
}

// expectedOrder sorts the documents as Elasticsearch sorts them by one field:
// by its value in the given direction, a document missing it last in either
// direction (the default missing: _last, which for a date is the type's
// maximum ascending and its minimum descending), then by id ascending.
func expectedOrder[V any](docs []scanLargeDoc, value func(scanLargeDoc) *V, compare func(a, b V) int, desc bool) []string {
	sorted := slices.Clone(docs)
	slices.SortStableFunc(sorted, func(a, b scanLargeDoc) int {
		va, vb := value(a), value(b)
		switch {
		case va == nil && vb == nil:
		case va == nil:
			return 1
		case vb == nil:
			return -1
		default:
			c := compare(*va, *vb)
			if desc {
				c = -c
			}
			if c != 0 {
				return c
			}
		}
		return cmp.Compare(a.id, b.id)
	})
	ids := make([]string, len(sorted))
	for i, d := range sorted {
		ids[i] = d.id
	}
	return ids
}

// pagedOrder walks a plain paged search of "s" in the given sort, a hundred
// a page, and returns its ids in order.
func (t *TestSuite) pagedOrder(sort core.SortOption, total int) []string {
	t.T().Helper()
	var ids []string
	for page := int32(0); int(page)*100 < total; page++ {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "s", Sort: []core.SortOption{sort}, Page: page, PageSize: 100,
		})
		t.Require().NoErrorf(err, "paged search page %d", page+1)
		for _, h := range resp.Hits {
			ids = append(ids, h.ID)
		}
	}
	return ids
}

// Test_Scan_LargeSortedWithTiesAndMissing scans the 2,500 documents of "s",
// half tied on the sort field and 100 without it, a thousand a page — by a
// keyword field, and by a date field in both directions — and requires the
// three pages to hold every id exactly once in sort order, each page with the
// whole total, and no token after the third.
func (t *TestSuite) Test_Scan_LargeSortedWithTiesAndMissing() {
	docs := t.seedScanLarge()

	rank := func(d scanLargeDoc) *string { return d.rank }
	stamp := func(d scanLargeDoc) *time.Time { return d.stamp }
	sorts := []struct {
		name string
		sort core.SortOption
		want []string
	}{
		{"keyword ascending", core.SortOption{Field: "fields.rank"},
			expectedOrder(docs, rank, cmp.Compare[string], false)},
		{"date ascending", core.SortOption{Field: "fields.stamp"},
			expectedOrder(docs, stamp, time.Time.Compare, false)},
		{"date descending", core.SortOption{Field: "fields.stamp", Desc: true},
			expectedOrder(docs, stamp, time.Time.Compare, true)},
	}

	for _, c := range sorts {
		// The fixture is what the case means it to be: a plain paged search
		// in the same sort, with no writes going on, returns the expected
		// order.
		t.Require().Equalf(c.want, t.pagedOrder(c.sort, scanLargeN),
			"%s: the paged search's order is the expected one", c.name)
	}

	// Each sort is scanned at two page sizes: 1,000, whose boundaries fall
	// inside the tied block and among the distinct high values, and 820,
	// whose last boundary, at 2,460, falls inside the block of documents
	// without the field.
	for _, pageSize := range []int{scanLargePage, scanMissingPage} {
		for _, c := range sorts {
			t.Run(fmt.Sprintf("%s, %d a page", c.name, pageSize), func() {
				pages := t.scanToEnd(core.SearchRequest{
					Resource: "s",
					Sort:     []core.SortOption{c.sort},
					PageSize: int32(pageSize),
				}, nil)

				var wantLens []int
				for left := scanLargeN; left > 0; left -= pageSize {
					wantLens = append(wantLens, min(pageSize, left))
				}
				gotLens := make([]int, len(pages))
				for i, p := range pages {
					gotLens[i] = len(p.ids)
				}
				t.Require().Equalf(wantLens, gotLens,
					"a scan of %d a page over %d documents: each page's hits", pageSize, scanLargeN)
				for i, p := range pages {
					t.Require().EqualValuesf(scanLargeN, p.total, "page %d's total", i+1)
					if i < len(pages)-1 {
						t.Require().NotEmptyf(p.next, "page %d's token", i+1)
					} else {
						t.Require().Emptyf(p.next, "page %d, the last, has no token", i+1)
					}
				}

				got := scanIDs(pages)
				t.requireExactlyOnce(c.want, got, c.name)
				t.Require().Equal(c.want, got, "the scan returns its hits in sort order, resource_id last")
			})
		}
	}
}

// --- The small fixture: 25 documents of "s", 10 a page. ---

const (
	scanSmallN    = 25
	scanSmallPage = 10
)

// scanSmallData is a small-fixture document's data with the given rank.
func scanSmallData(id, rank string) map[string]any {
	return map[string]any{"id": id, "name": "item" + id, "rank": rank}
}

// seedScanSmall builds 25 documents of "s" whose ranks, k0000 to k0024, are
// a permutation of their ids' order, and returns their ids in rank order.
func (t *TestSuite) seedScanSmall() []string {
	t.setScanConfig(ScanResourceConfig)
	ids := make([]string, scanSmallN)
	data := make(map[string]map[string]any, scanSmallN)
	byRank := make([]string, scanSmallN)
	for i := range scanSmallN {
		id := fmt.Sprintf("s%02d", i)
		r := (i * 7) % scanSmallN // 7 is coprime with 25
		ids[i] = id
		data[id] = scanSmallData(id, fmt.Sprintf("k%04d", r))
		byRank[r] = id
	}
	t.seedBuilt("s", ids, data)
	return byRank
}

// scanSmallReq is the small fixture's scan: by rank, 10 a page.
var scanSmallReq = core.SearchRequest{
	Resource: "s",
	Sort:     []core.SortOption{{Field: "fields.rank"}},
	PageSize: scanSmallPage,
}

// rankOf returns the rank a plain search finds stored for s/id now.
func (t *TestSuite) rankOf(id string) string {
	t.T().Helper()
	resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
		Resource: "s",
		PageSize: 100,
	})
	t.Require().NoError(err)
	for _, h := range resp.Hits {
		if h.ID == id {
			fields, _ := h.Source["fields"].(map[string]any)
			rank, _ := fields["rank"].(string)
			return rank
		}
	}
	t.FailNowf("not found", "a plain search does not find s/%s", id)
	return ""
}

// Test_Scan_RebuiltBetweenPagesAppearsOnce rebuilds two documents between
// page 1 and page 2, each with a rank that moves it across the scan's
// position: page 1's first hit to the end of the order, and the order's last
// document, which the scan has not reached, to its start. Each still comes
// back exactly once, where the scan's snapshot holds it.
func (t *TestSuite) Test_Scan_RebuiltBetweenPagesAppearsOnce() {
	order := t.seedScanSmall()

	var seen, unseen string
	pages := t.scanToEnd(scanSmallReq, func(page int) {
		if page != 1 {
			return
		}
		seen, unseen = order[0], order[len(order)-1]
		fetches := t.fakeProvider.FetchCount("s", seen) + t.fakeProvider.FetchCount("s", unseen)
		t.rebuildWith("s", seen, scanSmallData(seen, "x9999"))
		t.rebuildWith("s", unseen, scanSmallData(unseen, "a0000"))
		t.Require().Equal(fetches+2, t.fakeProvider.FetchCount("s", seen)+t.fakeProvider.FetchCount("s", unseen),
			"each rebuild between page 1 and page 2 fetches its document once")
		t.Require().Equal("x9999", t.rankOf(seen), "the rebuild of the seen document landed")
		t.Require().Equal("a0000", t.rankOf(unseen), "the rebuild of the unseen document landed")
	})

	t.Require().NotEmpty(seen, "the scan never got past page 1")
	t.Require().Contains(pages[0].ids, seen, "page 1 holds the document rebuilt after it")
	t.Require().NotContains(pages[0].ids, unseen, "page 1 does not hold the document the scan had not reached")
	got := scanIDs(pages)
	t.requireExactlyOnce(order, got, "scan across rebuilds")
	t.Require().Equal(order, got, "the scan returns the snapshot's order")
	for i, p := range pages {
		t.Require().EqualValuesf(scanSmallN, p.total, "page %d's total", i+1)
	}
}

// Test_Scan_IndexedAfterFirstPageIsNotSeen indexes a document after page 1
// whose rank sorts it among the pages still to come; the scan doesn't return
// it, and a plain search does.
func (t *TestSuite) Test_Scan_IndexedAfterFirstPageIsNotSeen() {
	order := t.seedScanSmall()

	const late = "s99"
	pages := t.scanToEnd(scanSmallReq, func(page int) {
		if page != 1 {
			return
		}
		// Between k0019 and k0020: on the scan's last page.
		t.seedBuilt("s", []string{late}, map[string]map[string]any{late: scanSmallData(late, "k0019x")})
	})

	t.Require().Greater(len(pages), 1, "the scan never got past page 1")
	got := scanIDs(pages)
	t.Require().NotContains(got, late, "a document indexed after page 1 is not in the scan")
	t.requireExactlyOnce(order, got, "scan across a new document")
	for i, p := range pages {
		t.Require().EqualValuesf(scanSmallN, p.total, "page %d's total", i+1)
	}
	t.Require().Equal("k0019x", t.rankOf(late), "a plain search finds the late document")
}

// Test_Scan_CutoverPartwayKeepsPinnedVersion starts a scan of "cv" with a neq
// filter on grade, a field of version 1 that version 2 drops, while the alias
// serves version 1; between page 1 and page 2 the config cuts over to read
// version 2 and the alias moves there. The pages after the cutover are still
// version 1's: the scan returns every document the filter admits exactly once
// and none it excludes. Under version 2's config the filter's field is unknown
// (a plain search shows it), and a neq on a field an index lacks would admit
// every document.
func (t *TestSuite) Test_Scan_CutoverPartwayKeepsPinnedVersion() {
	t.setScanConfig(ScanCutoverConfig)

	const n = 240
	ids := make([]string, n)
	data := make(map[string]map[string]any, n)
	var admitted []string
	for i := range n {
		id := fmt.Sprintf("c%03d", i)
		grade := "base"
		if i%2 == 0 {
			grade = "gold"
		} else {
			admitted = append(admitted, id)
		}
		ids[i] = id
		data[id] = map[string]any{"id": id, "name": "cut" + id, "grade": grade}
	}
	t.seedBuilt("cv", ids, data)

	notGold := core.Filter{Field: "fields.grade", Op: core.FilterOpNeq, Value: "gold"}
	before, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "cv", Filters: []core.Filter{notGold}})
	t.Require().NoError(err)
	t.Require().EqualValues(len(admitted), before.Total, "version 1 admits half the documents")

	pages := t.scanToEnd(core.SearchRequest{
		Resource: "cv",
		Filters:  []core.Filter{notGold},
		PageSize: 50,
	}, func(page int) {
		if page != 1 {
			return
		}
		t.setScanConfig(ScanCutoverReadV2)

		// The cutover happened: the alias serves version 2, which every
		// build also wrote, and which doesn't know grade.
		all, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "cv"})
		t.Require().NoError(err)
		t.Require().EqualValues(n, all.Total, "version 2 serves every document after the cutover")
		_, err = t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "cv", Filters: []core.Filter{notGold}})
		var invalid *core.InvalidArgumentError
		t.Require().ErrorAs(err, &invalid, "a plain search under version 2 rejects the dropped field")
	})

	t.Require().Greater(len(pages), 1, "the scan never got past page 1")
	got := scanIDs(pages)
	for _, id := range got {
		t.Require().Equalf("base", data[id]["grade"], "the scan returned %s, which the filter excludes", id)
	}
	t.requireExactlyOnce(admitted, got, "scan across the cutover")
	for i, p := range pages {
		t.Require().EqualValuesf(len(admitted), p.total, "page %d's total", i+1)
	}
}

// Test_Scan_LapsedKeepAliveIsCursorExpired scans on an indexer whose
// backend keeps a scan's point in time alive one second between pages: page
// 2, taken at once, continues the scan; page 3, taken three seconds later, is
// ErrCursorExpired.
func (t *TestSuite) Test_Scan_LapsedKeepAliveIsCursorExpired() {
	t.seedScanSmall()
	idx := t.newIndexer(ScanResourceConfig, core.Config{
		ES: elasticsearch.New(t.esClient, true, elasticsearch.WithScanKeepAlive(time.Second)),
	})
	req := scanSmallReq
	req.PageSize = 5

	first, err := t.scanNext(idx, req, "")
	t.Require().NoError(err, "page 1")
	t.Require().Len(first.ids, 5, "page 1's hits")
	t.Require().NotEmpty(first.next, "page 1's token")

	second, err := t.scanNext(idx, req, first.next)
	t.Require().NoError(err, "page 2, taken within the keep-alive")
	t.Require().Len(second.ids, 5, "page 2's hits")
	t.Require().NotEmpty(second.next, "page 2's token")

	time.Sleep(3 * time.Second)

	_, err = t.scanNext(idx, req, second.next)
	t.Require().ErrorIs(err, core.ErrCursorExpired, "page 3, taken after the keep-alive lapsed")
}
