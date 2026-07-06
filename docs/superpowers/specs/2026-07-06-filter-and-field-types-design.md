# Filter Operations and Field Types — Design

Date: 2026-07-06
Status: approved for implementation

## Goal

Grow Laika's structured filtering from `EQ`/`IN` on effectively untyped fields
into a typed system: a whitelist of field types, each with a family of filter
operations, advertised through capabilities, validated at request time, and
usable from the demo UI.

## Non-goals

- **No OR composition.** The filter list stays implicitly AND-ed. "One of
  several values" is `IN`; a numeric/date range is two stacked AND filters
  (`gte a`, `lt b`). OR-groups across different fields/ops are deferred; the
  flat model is forward-compatible with adding them later.
- **No full-text filter ops.** `text` fields remain query-only (full-text
  `query` string), unfilterable and unsortable.
- **No typed wire values.** Filter values stay strings on the wire;
  Elasticsearch coerces them against the typed mapping.

## Field types

`FieldConfig.Type` becomes a validated whitelist (config-load error on unknown
types, replacing today's silent pass-through into the ES mapping):

| Family   | Config/ES types                    | Notes |
|----------|------------------------------------|-------|
| string   | `keyword` (default when omitted)   | IDs, enums, names |
| numeric  | `long`, `integer`, `double`, `float` | |
| temporal | `date`                             | accepts ISO-8601, epoch millis, and ES date-math strings (`now-7d`) |
| boolean  | `boolean`                          | |
| ip       | `ip`                               | `eq` with CIDR notation matches the block |
| fulltext | `text`                             | no filter ops; not sortable |

The type→family→ops table lives in one place in `core` and drives
capabilities, request validation, and (via capabilities) the demo UI.

## Filter operations

| Op | ES query | string | numeric | temporal | boolean | ip |
|----|----------|--------|---------|----------|---------|----|
| `EQ`         | `term`                | ✓ | ✓ | ✓ | ✓ | ✓ |
| `NEQ`        | `must_not(term)`      | ✓ | ✓ | ✓ | ✓ | ✓ |
| `IN`         | `terms`               | ✓ | ✓ | ✓ | – | ✓ |
| `NOT_IN`     | `must_not(terms)`     | ✓ | ✓ | ✓ | – | ✓ |
| `GT` `GTE` `LT` `LTE` | `range`      | – | ✓ | ✓ | – | ✓ |
| `PREFIX`     | `prefix`              | ✓ | – | – | – | – |
| `SUFFIX`     | `wildcard` `*v`       | ✓ | – | – | – | – |
| `CONTAINS`   | `wildcard` `*v*`      | ✓ | – | – | – | – |
| `EXISTS`     | `exists`              | ✓ | ✓ | ✓ | ✓ | ✓ |
| `NOT_EXISTS` | `must_not(exists)`    | ✓ | ✓ | ✓ | ✓ | ✓ |

Operand rules:

- `EQ`, `NEQ`, `GT`, `GTE`, `LT`, `LTE`, `PREFIX`, `SUFFIX`, `CONTAINS`
  require `value`, forbid `values`.
- `IN`, `NOT_IN` require non-empty `values`, forbid `value`.
- `EXISTS`, `NOT_EXISTS` take neither.

`SUFFIX`/`CONTAINS` use leading-wildcard queries — acceptable at current index
sizes; the implementation is swappable later (reversed `.rev` subfield +
`prefix`) without touching the API. User-supplied `*` and `?` are escaped
before being embedded in a `wildcard` query so values are always literal.

## Semantics

### Composition

All filters in a request AND together (unchanged). A range is expressed as two
filters on the same field.

### Negation on relations

Negation ops (`NEQ`, `NOT_IN`, `NOT_EXISTS`) are **document-level**: on a
nested many-relation the `must_not` wraps the `nested` query, meaning *"no
child matches"* — `b.state NEQ active` returns parents with no active child.

**Reference-relation fields do not support negation ops.** The two-phase join
structurally yields *"some joined child differs"*, which would silently give
the same filter a different meaning depending on the relation's strategy.
Capabilities omit negation ops on reference fields and validation rejects
them. Positive ops (`EQ`, `IN`, ranges, `PREFIX`, `SUFFIX`, `CONTAINS`,
`EXISTS`) flow through the existing child-search extraction unchanged.

### Request validation (strict)

Caller-supplied filters are validated at the top of `Indexer.Search` against
the resource's read-version config. Federated Search validates its global
filters the same way against **every** requested Type — a global filter must
be valid on all of them:

- Unknown field path → InvalidArgument.
- Op not in the field's family (e.g. `PREFIX` on a `long`) → InvalidArgument.
- Negation op on a reference-relation field → InvalidArgument.
- Operand-shape violations (see rules above) → InvalidArgument.

Internally injected clauses (secondary-scope term, reference-resolution `IN`
on the join key) are added after or below validation and are unaffected.

## Changes by component

### `proto/search/v1/search.proto`

`FilterOp` gains the new values; existing `FILTER_OP_EQ = 1`,
`FILTER_OP_IN = 2` keep their numbers (wire-compatible). The `Filter` message
shape is unchanged. Regenerate with `buf generate`.

### `core`

- `search_types.go`: new `FilterOp` constants mirroring the proto.
- New file (e.g. `core/filter_ops.go`): type → family → allowed-ops table plus
  a helper answering "is op allowed on this field config". Lives in `core`
  (not `core/resource`) because it references `FilterOp`.
- `capabilities.go`: `fieldCapability` derives `FilterOps` from the table
  instead of hardcoding `[EQ, IN]`; reference-relation fields get the
  type-derived set minus negation ops.
- `search.go` / `federated_search.go`: strict validation described above.
- `core/resource/validations.go`: `FieldConfig.Validate` enforces the type
  whitelist.

### `backend/elasticsearch`

`buildFilterClause` grows the new ops per the table, including:

- `must_not` wrapping outside the `nested` wrapper for negation ops.
- Wildcard metacharacter escaping for `SUFFIX`/`CONTAINS`.
- `range` clauses for `GT`/`GTE`/`LT`/`LTE` (one clause per filter; ES ANDs
  stacked range filters).

### `app/server`

`protoFilterOp` / capability mapping extended both directions. Validation
errors from core surface as gRPC `InvalidArgument`.

### Demo UI (`demo/index.html`)

- Op dropdown populated from the field's advertised `filterOps` (labels for
  all new ops).
- Value widget chosen by field `type`: number input for numeric, datetime
  picker (converted to ISO-8601) for `date`, true/false select for `boolean`,
  text input otherwise; chips input for `IN`/`NOT_IN`; no input for
  `EXISTS`/`NOT_EXISTS`.
- A per-field "+" adds another condition row on the same field so ranges
  (`GTE` + `LT`) are expressible.

## Testing

- **Unit** — table-driven `buildFilterClause` tests: every op, nested
  wrapping, negation placement, wildcard escaping, operand-shape errors.
- **Unit** — capabilities: ops derived per type; reference fields lose
  negation ops.
- **Unit** — config validation: type whitelist accept/reject.
- **Unit** — request validation: unknown field, wrong-family op, negation on
  reference field, operand shape.
- **Unit** — `app/server` proto↔core mapping round-trips all ops.
- **Integration** (`app/tests`, testcontainers): index typed documents and
  exercise ranges on numbers/dates, prefix/suffix/contains, neq/not_in on
  root and nested fields, exists/not_exists, and a stacked-range query.
