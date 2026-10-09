# A scan reads one point-in-time snapshot of the read alias's index, under the Schema Version it pinned

_Accepted, 2026-10-09. Built by runbook step L4.1._

Single-resource search pages by `from`/`size` (`Page`, `PageSize`, capped at 100 by
`normalizePaging`). That serves a person paging through a result list, where a hit that enters,
leaves or moves between two requests shifts the later pages by one, so a row is shown twice or
skipped (seams S43 in laika-dev's `docs/seams.md`). An export can't accept that: it must read
every document a caller may see, each exactly once, while documents are written and while a
cutover moves the read alias to another Schema Version (ADR 0009). The first consumer is the
harness's export of a search's ids to source apps (`harness/docs/superpowers/specs/2026-10-06-search-export-design.md`).

The decision: **a *scan* is a single-resource search with `Scan` set. Its first page pins the one
concrete index the read alias serves and that index's Schema Version; every page reads one
Elasticsearch point in time of that index with `search_after`, built with the pinned version's
config, and is continued by an opaque page token the consumer is trusted to hand back.** It is a
paging mode with no policy (ADR 0001): the registered middlewares filter and scope every page as
they do any search.

## How it works

- **The API.** `core.SearchRequest` has `Scan bool` and `PageToken string`, and
  `core.SearchResponse` has `NextPageToken string`, empty after the last page and on every search
  that isn't a scan. A scan goes through `Indexer.Search` and the whole search chain on every
  page, page 1 included, and accepts exactly the filters and sort a paged search accepts,
  validated the same way. Its hits are in the search's sort with `resource_id` last, and carry
  an ID and a score but no `Source` (`_source: false`): a consumer reads what it exports from its
  own store, by id. Its `Total`, on every page, is the first page's, except on the empty page
  that ends a scan whose reference child stopped matching, which reports 0. `FederatedSearch` has no
  scan, and Laika's standalone app (`app/server`, `search.v1`) doesn't expose one.
- **Page 1 pins what the read alias serves.** `Indexer.Search` reports an unknown resource
  (`ErrUnknownResource`) before anything else, then resolves the alias with
  `SearchBackend.GetAliasTargets`, which returns every index it points to.
  - One target pins the configured version `v` whose `IndexName(resource, v)` is exactly that
    name, so a spelling an integer parse would accept (`_search_v02`) pins nothing.
  - More than one target, or a target no configured version owns (an index the naming scheme
    doesn't manage), is `ErrScanFault`. A scan never guesses a config.
  - No alias yet: the read version's config validates the request, the chain runs, so a
    middleware that would deny still denies, and the scan answers an empty result with no token,
    as a paged search's missing index does.

  The pinned version is the one the alias serves, which is the right config even while the
  alias serves a configured version other than `ReadVersion`.
- **Every page is built with the pinned version's config.** The scan's state — resource, pinned
  version and its `*VersionConfig`, pinned index, start time, decoded token — rides in the
  context from `Indexer.Search` down the chain. Caller-filter validation, `deriveNestedPath`,
  reference routing (`resolveReferenceFilters` takes the parent's config as a parameter) and
  `searchBase` read the state's config instead of `ReadVersionConfig()`, and `searchBase` hands
  the backend the pinned index instead of the alias, through `Search`'s existing `indexAlias`
  and `vc` arguments. A stage that sees `Scan` set but finds no state, a state for another
  resource, or one with no config returns `ErrScanFault`; it never falls back to the read
  version. `Indexer.Search` and `FederatedSearch` replace the context's state on entry — a scan
  sets its own, any other search clears it — so a search a middleware starts inside a scan never
  inherits its pinning.

  This is why the pinning matters. A later page built with a newer version's config against the
  old snapshot could fail open. A negation (`neq`, `not_in`, `not_exists`, sent as `must_not`)
  on a field the old index lacks matches every document. A scoped nested block the newer version
  dropped or renamed stops being enforced, since the backend builds a scope clause per scoped
  block of the config it is given, and a consumer's tenant scope (the harness's `authz` self
  scope for fiber operators) may be enforced only through those blocks.
- **The token** is base64url of a JSON payload: the scan's start time, the pinned version, the
  pinned index, a fingerprint of the request, and the backend's cursor. Each new token copies
  the start, version and index from the one it continues; only the cursor is new. A later page:
  1. is `ErrCursorExpired` when its start is older than `Config.ScanMaxAge` (default one hour;
     0 means the default and `New` rejects a negative one), so a scan can't read an old snapshot
     under an old version's scoping indefinitely by renewing its keep-alive;
  2. is `ErrCursorExpired` when the pinned version is no longer configured or its
     `IndexName` isn't the pinned index — which also catches a token sent for another resource;
  3. is `ErrInvalidPageToken` when the backend cursor is empty. A request carrying a token
     never opens a point in time.

  A token that doesn't decode, or lacks a start, version or index, is `ErrInvalidPageToken`.
- **The fingerprint** is SHA-256 over a canonical, length-prefixed encoding of every
  `SearchRequest` field but `PageToken` — resource, query, page, page size, each filter's field,
  op, value, values and nested path, each sort's field and direction, scope, scan — then the
  pinned version, the pinned index, and a hash of the pinned `VersionConfig`'s JSON, so a version
  edited in place is caught. A guard test fails when `SearchRequest`, `Filter` or `SortOption`
  gains a field it doesn't encode. A token sent with any other request is
  `ErrInvalidPageToken`: a consumer that changed its filters partway through an export, or that
  continues a token under another actor's scope.
- **The fingerprint middleware** (`scanPage`), which `New` places after the registered
  middlewares and before `deriveNestedPath` and reference resolution, takes it. So it sees the
  request as the registered middlewares leave it — their filters and `Scope` are fingerprinted —
  and before reference resolution folds child results into terms, which may change between pages
  (below). It checks that the middlewares kept the scan's resource, `Scan` and token
  (`ErrScanFault` otherwise), sets the page size — default and cap 1,000, in place of paged
  search's 25 and 100 — before fingerprinting, passes the backend's cursor down as the inner
  request's `PageToken`, and wraps a non-empty cursor coming back in the next token. An empty
  one stays empty: wrapping it would make the next call a first page.
- **Reference filters are resolved on every page,** on the child's current config and alias.
  A child search is a plain search, never a scan, so its folded terms may change between pages
  and membership through a reference filter can shift during a scan (deferral D9). A child that
  stops matching ends the scan: an empty page with no token, and the point in time left to
  expire.
- **The backend** (`backend/elasticsearch`, `scan.go`) decides scan mode from `req.Scan` alone
  and uses the page size it is given.
  - It refuses a scan on Elasticsearch below 8.11.0 (below). Before its first scan's point in
    time it reads the cluster's version from `GET /`, and caches only a passing version: a
    version below 8.11.0, or one it can't parse, is `ErrScanFault` and is looked up again on the
    next scan, so a cluster upgraded to 8.11 or later scans without a restart. A failed lookup
    is returned and asked again too. A 401 or 403 from `GET /` is `ErrScanFault` saying Laika's
    Elasticsearch credentials need the `monitor` cluster privilege: `GET /` is the only
    cluster-level call Laika makes, so a deployment that scans grants `monitor` beside its index
    privileges. Paged and federated search never look the version up.
  - The first page opens a point in time on the pinned index; every page searches with `pit`
    and no index in the path, sends the paged search's query and sort (the caller's, or `_score`
    descending, then `resource_id` ascending), `_source: false`, a `term` on `_index` equal to
    the pinned index, `track_total_hits` on the first page only, and
    `allow_partial_search_results=false` as a URL parameter.
  - A later page sends the previous page's last hit's `sort` array back unchanged as
    `search_after`, kept as raw JSON so a long above 2^53, a `null` and the point in time's
    implicit `_shard_doc` tiebreaker survive, with the latest point-in-time id, renewing the
    keep-alive (`WithScanKeepAlive`, default one minute).
  - The cursor is base64url JSON of the point-in-time id, those sort values and the first page's
    total, opaque to core. One without an id or sort values is `ErrInvalidPageToken`.
  - Any 404 in a scan — opening the point in time, the first search or a later one — is
    `ErrCursorExpired`, never the paged search's empty result, which would end a lost scan as a
    success.
  - A hit from another index, a failed shard, a timed-out search, or a full page whose last hit
    has no sort values fails the page with `ErrScanFault` and returns none of its hits.
  - A page shorter than the page size is the last: the point in time is closed, a failed close
    is logged at Warn and not returned, and no cursor goes back. A result that is a multiple of
    the page size ends with an empty page. A scan ended by an error leaves its point in time to
    expire.
- **The errors**, as a consumer reads them:

  | Error | Meaning | What the consumer does |
  |---|---|---|
  | `ErrInvalidPageToken` | the token doesn't decode, has no backend cursor, or was issued for another request | a consumer bug: it altered the token or sent it with another request |
  | `ErrCursorExpired` | the scan is older than `ScanMaxAge`, its pinned version or index is no longer the config's, or its point in time or index is gone | restart from page 1 |
  | `ErrScanFault` (wraps its cause) | a deployment or invariant fault: Elasticsearch below 8.11.0, credentials without the `monitor` cluster privilege, an alias it can't pin, a middleware that dropped or changed the scan, a hit from another index, a failed shard or timed-out page | report it; a retry may not help |
  | `*InvalidArgumentError` | a nonzero `Page`, a `PageToken` without `Scan`, or a filter a paged search would refuse | fix the request |
  | `ErrUnknownResource` | the resource isn't configured (or is empty), checked before anything else | fix the request |

  A transport error resolving the alias on page 1 is returned wrapped, as itself.

## The token is trusted to the consumer

The token is neither encrypted nor signed. A holder can read the last hit's sort values in it —
field values of a document the caller was shown — and can alter it. Its checks catch a consumer's
mistakes, not an attacker: nothing in them is keyed — the fingerprint is a plain hash of the
request and the pinned config — so a holder that knows the request and the config can build a
token that passes them all. A start time is not checked against a future date, so an altered
token can outlive `ScanMaxAge`, bounded then by the point in time's keep-alive. Laika leaves
sealing to the consumer, since only the consumer knows who holds its tokens: the harness's
`ExportIds` is to give them only to source apps (SB6.1 in laika-dev's runbooks), which already
hold an app token that reaches the services directly, so sealing would defend a case already
lost. **A consumer exposing scans to untrusted callers must seal the token first** — encrypt and
authenticate it (AEAD) on the way out and open it on the way in, around `Indexer.Search` — so a
caller can neither read its sort values nor alter it (seams S53 in laika-dev's `docs/seams.md`).

## A scan requires Elasticsearch 8.11.0 or later

On Elasticsearch 8.9.0 through 8.10.4, which bundle Lucene 9.7, a `search_after` page sorted on a
`date`, `long` or `double` field silently drops documents that lack the sort field whenever the
page has to reach into the block of missing values, on the first page past the last present value
or on a page whose cursor is a present document's value. Lucene's points-based skipping judges the
missing value non-competitive by comparing it with a queue bottom that isn't set yet while the
queue is filling: apache/lucene#12521, introduced by apache/lucene#12334 (Lucene 9.7.0) and fixed
by apache/lucene#12520 (Lucene 9.8.0, Elasticsearch 8.11.0). Whether a page loses some or all of
the block depends on the segment layout. Documents that have the field are never lost. Bisected
with `app/tests`' scan cases: 8.8.2 passes, 8.9.0 and 8.10.4 fail, 8.11.0 through 8.19.0 pass.
`keyword`, `integer` and `float` sorts were found unaffected (empirically, not from the source).
A scan that ends short with no error is the failure the scan exists to rule out, so the backend
refuses the whole range below 8.11.0 rather than only the affected sorts and versions, and
`app/tests` runs Elasticsearch 8.19.0. Which version the vxfiber deployment runs is open point Q27
in laika-dev's `docs/open-points.md`; its default is this refusal (seams S55).

The guard has one unguarded window. `GET /` reports the version of the node that answers, so
during a rolling upgrade from 8.10 an 8.11 node can answer while shards on 8.10 nodes still run
Lucene 9.7: the guard passes, caches the pass, and a scan whose page reaches into a missing-value
block on those shards can still lose documents. Accepted while nothing is live; checking the
oldest node's version (`GET _nodes`, also under `monitor`) would close it.

## Why not the alternatives

- **`from`/`size` deep paging.** Each page is a new search of the live index, so writes between
  pages shift it (S43), and Elasticsearch refuses `from + size` above `index.max_result_window`
  (10,000 by default), so an export of more is impossible without raising it per index at a cost
  in memory per request.
- **The scroll API.** It also reads one snapshot, but Elasticsearch no longer recommends it for
  deep paging, in favour of a point in time with `search_after`. A scroll's later pages run no
  query: they continue the search context page 1 opened, which keeps its per-shard query state
  for the scroll's life. A point in time is a view of the index's segments, searched by ordinary
  requests that each carry the query, the sort and `search_after`, so every scan page is built
  through the chain and checked against its request like any search.
- **Searching the alias on every page,** or building later pages with the current read version's
  config. A cutover between pages would switch the index or the config under the scan, which can
  fail open (above). Ending every scan at a cutover instead would fail exports the pinning can
  finish.
- **Resolving reference filters once,** on page 1, carrying the children's ids in the token. It
  would pin membership through a reference filter, at the cost of a token that grows with the
  child result, up to the 10,000-term ceiling. Deferred (D9).
- **Working around the Lucene bug on 8.9 and 8.10,** instead of refusing them. Two sort options
  avoid it on 8.9.0, and both turn off Lucene's sort skipping, so every page reads every
  matching document's doc values: O(N) per page and O(N²/page size) per scan, about 1.7 times a
  page's cost at 500,000 documents, growing with N.
  - `numeric_type: "long"` on a `date` sort gives the same values and order, but only for dates;
    a `long` or `double` field has no lossless counterpart.
  - `mode: "median"` on any numeric sort is exact only for single-valued fields; it changes the
    value a multi-valued field sorts by.

  A two-phase scan would keep the skipping: first the documents that have the sort field (an
  `exists` filter, so no missing value is ever competitive), then those that lack it
  (`must_not exists`, sorted by `resource_id`, Elasticsearch's own order inside the missing
  block), with the phase in the cursor. It is untested. Any of these would be gated on the
  cluster version, and is for Q27's answer to call for.
- **Signing or encrypting the token in Laika.** Laika would own a key, its distribution across
  instances and its rotation, to defend a holder the first consumer already trusts. A consumer
  that needs it seals the token around `Indexer.Search` with keys it already manages.

## Consequences

- **A scan sees one snapshot of one index.** Every document matching on page 1's point in time
  comes back once; one written later, or rebuilt between pages, is seen as of the snapshot. A
  document's later delete doesn't remove it from the scan.
- **A cutover doesn't break a scan in flight.** It keeps reading the old index under the old
  version's config while the old version stays configured; once the config drops the version,
  the next page is `ErrCursorExpired`. An index deleted under it is too (a 404).
- **Membership through a reference filter can shift** between pages, and a scan whose child
  stops matching ends early with no error (D9).
- **Open points in time cost the cluster.** Elasticsearch bounds them only by
  `search.max_open_pit_context` (300 per node by default), and a scan ended by an error or
  abandoned by its consumer holds its point in time until the keep-alive lapses, so a caller
  opening many scans can exhaust it for others meanwhile (seams S54).
- **A deployment that scans runs Elasticsearch 8.11.0 or later, with the `monitor` cluster
  privilege.** On an older cluster, or without `monitor`, every scan of an existing index fails
  with `ErrScanFault` on its first page; a scan of a type whose read alias doesn't exist yet
  answers its empty result without reaching the backend. Paged and federated search are
  unaffected. A rolling upgrade from 8.10 to 8.11 can pass the guard before every node runs 8.11
  (above).
- **The page token is a consumer's to protect** (above).

**Implication for contributors:** a stage of the search chain that reads a resource's config
reads it through the scan's state when `Scan` is set, and fails with `ErrScanFault` when the
state is missing — never the read version's config, never a pass-through. A new
`SearchRequest`, `Filter` or `SortOption` field goes into the fingerprint, which the guard test
enforces. Nothing in the scan path may turn a lost point in time into an empty success.
