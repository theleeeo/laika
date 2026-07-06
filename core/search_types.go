package core

// FilterOp is the comparison operator for a structured search filter. The
// ops a field supports are derived from its type family; see opsByFamily in
// filter_ops.go.
type FilterOp int

const (
	FilterOpEq FilterOp = iota // term
	FilterOpIn                 // terms
	// New ops are appended so FilterOpEq/FilterOpIn keep their original values.
	FilterOpNeq       // must_not(term)
	FilterOpNotIn     // must_not(terms)
	FilterOpGt        // range
	FilterOpGte       // range
	FilterOpLt        // range
	FilterOpLte       // range
	FilterOpPrefix    // prefix
	FilterOpSuffix    // wildcard *v
	FilterOpContains  // wildcard *v*
	FilterOpExists    // exists
	FilterOpNotExists // must_not(exists)
)

// Filter is a structured field-level filter for a search request.
type Filter struct {
	Field      string
	Op         FilterOp
	Value      string   // single-value ops (eq, neq, ranges, prefix, suffix, contains)
	Values     []string // set ops (in, not_in)
	NestedPath string   // non-empty when the field lives inside a nested object
}

// SortOption specifies a field and direction for result ordering.
type SortOption struct {
	Field string
	Desc  bool
}

// SearchRequest is the core-level search request passed to SearchBackend.Search
// and returned by Indexer.Search.
type SearchRequest struct {
	Resource string
	Query    string
	Page     int32
	PageSize int32
	Filters  []Filter
	Sort     []SortOption

	// SecondaryScope is the caller's single tenant scope value for Federated
	// Search's secondary tier (spec D11.2, D14). It is the dedicated channel a
	// consumer's search middleware sets from the caller identity; Federated
	// Search harvests it in collect mode (see collectIndexFilterGroups) and
	// weaves a term into the nested search_scoped clause so secondary text
	// matches only inside entries the caller may see. Empty means unscoped
	// secondary (standalone-app behaviour). Single-resource Search ignores it.
	SecondaryScope string
}

// AddFilter appends a filter to the request. It is the explicit way a
// middleware adds a filter to a search.
//
// A filter is routed entirely by its Field path, opaquely to the strategy of
// the relation it names (see [Indexer.referenceResolve]):
//
//   - "b.name" filters the denormalized "b" block when "b" is a denormalize
//     relation, or is extracted onto the referenced "b" search when "b" is a
//     reference relation — the caller writes the same path either way.
//   - "fields.tenant_id" (a root field) applies to the primary search only; it
//     is not copied onto any referenced search.
//   - "b.tenant_id" applies to the referenced "b" search (because "b" is a
//     reference relation), not to the primary.
//
// So a middleware scopes a referenced child by naming that child's field
// explicitly; there is no implicit fan-out across searches.
func (r *SearchRequest) AddFilter(f Filter) {
	r.Filters = append(r.Filters, f)
}

// SearchHit is a single document returned by a search.
type SearchHit struct {
	ID     string
	Score  float64
	Source map[string]any
}

// SearchResponse is the result of a search operation.
type SearchResponse struct {
	Total int64
	Hits  []SearchHit
}

// CapabilitiesResponse describes the searchable fields for all configured resources.
type CapabilitiesResponse struct {
	Resources []ResourceCapability
}

// ResourceCapability describes the fields available for a single resource type.
type ResourceCapability struct {
	Resource string
	Fields   []FieldCapability
}

// FieldCapability describes a single searchable field and its supported operations.
type FieldCapability struct {
	Field      string
	Type       string
	Searchable bool
	Sortable   bool
	FilterOps  []FilterOp
}
