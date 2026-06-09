package core

// FilterOp is the comparison operator for a structured search filter.
type FilterOp int

const (
	FilterOpEq FilterOp = iota
	FilterOpIn
)

// Filter is a structured field-level filter for a search request.
type Filter struct {
	Field      string
	Op         FilterOp
	Value      string   // used by FilterOpEq
	Values     []string // used by FilterOpIn
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
