# Design: `diff-mapping` utility command

Date: 2026-07-13
Scope: `laika/` and `harness/` (two sibling repos in the `laika-dev` multirepo)

## Problem

`gen-mapping` generates an Elasticsearch index mapping from the current
config/domain and can `-apply` it. But there is no way to see, *before*
applying, how a running index's mapping differs from what the current config
would produce. Operators need to know whether a pending change is:

- **additive** (new field) — Elasticsearch accepts it via `PUT _mapping`, or
- **breaking** (type of an existing field changed) — Elasticsearch rejects it;
  the index must be recreated/reindexed.

## Goal

Add a `diff-mapping` utility command to both `laika` and `harness` that, for
each versioned index, diffs the **running** ES mapping against the mapping the
**current config/domain** would generate, and reports the differences in a
readable form suitable for a pre-deploy / CI check.

Non-goals: modifying any runtime behavior; diffing the `settings`/analysis
block; applying any change (that stays with `gen-mapping`).

## Key subtlety

`GET <index>/_mapping` returns the mapping enriched with all defaults ES applied
at index-creation time, and omits `settings` unless explicitly requested. A raw
JSON deep-diff would therefore be almost entirely false positives. The diff must
be **semantic**: walk only the field paths and `type` values that the generator
actually sets. In particular ES omits `"type": "object"` on object containers
(object is the implicit default), so a typeless node with `properties` is
normalized to `object` before comparison.

## Design

### 1. Shared diff logic — laika `backend/elasticsearch` package

Both commands import this package already (harness via its `../laika` replace),
so the reusable, unit-testable pieces live here.

- **`(*Client).GetMapping(ctx, indexName) (map[string]any, bool, error)`**
  (`alias.go`) — `Indices.GetMapping`, returns the inner `mappings` object;
  `false` (nil error) on 404.
- **`(*Client).Dial(addrs, user, pass) (*Client, error)`** (`es.go`) — a
  convenience constructor so standalone commands don't each rewire `esv8`.
- **`DiffMapping(expected, actual) MappingDiff`** (`mapping_diff.go`) — flattens
  both `properties` trees to `path -> type` maps and set-diffs them into
  `Added` / `Changed` / `Removed` `FieldDiff`s. Accepts either a full mapping
  document or a bare mappings object on each side. `MappingDiff.Drift()` =
  Added or Changed present; `Empty()` = no difference at all.
- **`IndexDiff` + `HasDrift([]IndexDiff)` + `ReportDiffs(w, []IndexDiff)`**
  (`mapping_diff_report.go`) — per-index result, exit-code predicate, and the
  human-readable renderer shared by both commands. A not-yet-created index and
  any Added/Changed field count as drift; Removed-only does not.

### 2. The two commands (thin, mirror `gen-mapping`)

- **`laika/app/cmd/diff-mapping/main.go`** — flags `-config` (default
  `resources.yml`), `-index`, `-es-addr` (default `http://localhost:9200`),
  `-es-user`, `-es-pass`, `-json`. Expected mappings from `config.LoadConfig` →
  `GenerateMappings`.
- **`harness/cmd/diff-mapping/main.go`** — identical, but expected comes from
  `domain.Resources()` and there is no `-config` flag. Run with
  `GOWORK=off GOEXPERIMENT=jsonv2`.

Each dials ES, fetches each expected index's mapping (sorted for determinism),
diffs, then prints the human summary (or `-json` = `[]IndexDiff`) and exits
non-zero when `HasDrift` is true.

### 3. Output

```
population_search_v1
  + fields.phone        keyword          (added)
  ~ fields.zip          keyword → text   (CHANGED — reindex required)
  - fields.legacy_id    keyword          (removed from config)
access_point_search_v1  (not created yet)
subscription_search_v1  in sync
```

Exit non-zero on Added/Changed or a missing index; Removed-only keeps exit 0.

### 4. Testing

Table-driven unit tests for `DiffMapping` (in-sync w/ object-type normalization,
added, changed, removed-not-drift, nested-relation subtree, sorting, and a real
`GenerateMapping` output diffed clean against an equivalent ES mapping),
`GetMapping` (transport-mocked: unwrap, 404, error), and `HasDrift`/`ReportDiffs`
semantics. No Docker required.

## Files

New: `backend/elasticsearch/mapping_diff.go`, `mapping_diff_report.go`, their
tests, `get_mapping_test.go`, `app/cmd/diff-mapping/main.go`,
`harness/cmd/diff-mapping/main.go`.
Modified: `backend/elasticsearch/alias.go` (GetMapping), `es.go` (Dial), plus
docs in `laika/CLAUDE.md` and `harness/README.md`.

## Notes

These are standalone binaries under each repo's `cmd/`, like `gen-mapping`; no
change to indexer/server runtime behavior. Because harness builds against
`../laika`, the laika library changes and the harness command must land in the
sibling checkouts together for harness to compile.
