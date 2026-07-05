package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/theleeeo/laika/app/gen/search/v1"
	"github.com/theleeeo/laika/core"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"
)

type SearcherServer struct {
	idx *core.Indexer
}

func NewSearcher(idx *core.Indexer) *SearcherServer {
	return &SearcherServer{idx: idx}
}

func (s *SearcherServer) Search(ctx context.Context, req *connect.Request[search.SearchRequest]) (*connect.Response[search.SearchResponse], error) {
	resp, err := s.idx.Search(ctx, protoToSearchRequest(req.Msg))
	if err != nil {
		if errors.Is(err, core.ErrUnknownResource) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, core.ErrUnknownResource)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(searchResponseToProto(resp)), nil
}

// FederatedSearch runs one query across a caller-supplied set of Resource Types.
// It validates the request up front — a non-empty resource set (no implicit
// "all") and filters on fields common to every requested Type — before handing
// off to the core federated path.
func (s *SearcherServer) FederatedSearch(ctx context.Context, req *connect.Request[search.FederatedSearchRequest]) (*connect.Response[search.FederatedSearchResponse], error) {
	msg := req.Msg

	if len(msg.Resources) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("resources must not be empty"))
	}
	if err := validateFederatedFilters(msg.Resources, msg.Filters, s.idx.GetCapabilities()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	resp, err := s.idx.FederatedSearch(ctx, protoToFederatedRequest(msg))
	if err != nil {
		var invalid *core.InvalidArgumentError
		switch {
		case errors.As(err, &invalid):
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		case errors.Is(err, core.ErrUnknownResource):
			return nil, connect.NewError(connect.CodeFailedPrecondition, err)
		default:
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	return connect.NewResponse(federatedResponseToProto(resp)), nil
}

// validateFederatedFilters enforces D7: a global filter is accepted only when
// every requested Type carries that field, checked against the capabilities
// data. A field missing from any requested Type is a loud InvalidArgument rather
// than a silent drop. Unknown resources are left to the core path, which reports
// them as a distinct error.
func validateFederatedFilters(resources []string, filters []*search.Filter, caps core.CapabilitiesResponse) error {
	if len(filters) == 0 {
		return nil
	}

	fieldsByResource := make(map[string]map[string]bool, len(caps.Resources))
	for _, rc := range caps.Resources {
		set := make(map[string]bool, len(rc.Fields))
		for _, f := range rc.Fields {
			set[f.Field] = true
		}
		fieldsByResource[rc.Resource] = set
	}

	for _, f := range filters {
		if f == nil || f.Field == "" {
			continue
		}
		for _, res := range resources {
			fields, known := fieldsByResource[res]
			if !known {
				continue // unknown resource: reported by the core path
			}
			if !fields[f.Field] {
				return fmt.Errorf("filter field %q is not present on resource %q; federated filters must be common to every requested resource", f.Field, res)
			}
		}
	}
	return nil
}

func protoToFederatedRequest(req *search.FederatedSearchRequest) core.FederatedSearchRequest {
	filters := make([]core.Filter, 0, len(req.Filters))
	for _, f := range req.Filters {
		if f == nil || f.Field == "" {
			continue
		}
		filters = append(filters, core.Filter{
			Field:      f.Field,
			Op:         protoFilterOp(f.Op),
			Value:      f.Value,
			Values:     f.Values,
			NestedPath: f.NestedPath,
		})
	}

	return core.FederatedSearchRequest{
		Query:         req.Query,
		Resources:     req.Resources,
		Filters:       filters,
		Page:          req.Page,
		PageSize:      req.PageSize,
		IncludeSource: req.IncludeSource,
	}
}

func federatedResponseToProto(resp core.FederatedSearchResponse) *search.FederatedSearchResponse {
	hits := make([]*search.FederatedHit, 0, len(resp.Hits))
	for _, h := range resp.Hits {
		var src *structpb.Struct
		if h.Source != nil {
			s, err := structpb.NewStruct(h.Source)
			if err != nil {
				continue
			}
			src = s
		}
		hits = append(hits, &search.FederatedHit{
			Resource: h.Resource,
			Id:       h.ID,
			Score:    h.Score,
			Source:   src,
		})
	}

	counts := make([]*search.ResourceCount, 0, len(resp.Counts))
	for _, c := range resp.Counts {
		counts = append(counts, &search.ResourceCount{Resource: c.Resource, Count: c.Count})
	}

	return &search.FederatedSearchResponse{Total: resp.Total, Hits: hits, Counts: counts}
}

func (s *SearcherServer) GetCapabilities(_ context.Context, _ *connect.Request[search.GetCapabilitiesRequest]) (*connect.Response[search.GetCapabilitiesResponse], error) {
	return connect.NewResponse(capabilitiesToProto(s.idx.GetCapabilities())), nil
}

func protoToSearchRequest(req *search.SearchRequest) core.SearchRequest {
	filters := make([]core.Filter, 0, len(req.Filters))
	for _, f := range req.Filters {
		if f == nil || f.Field == "" {
			continue
		}
		filters = append(filters, core.Filter{
			Field:      f.Field,
			Op:         protoFilterOp(f.Op),
			Value:      f.Value,
			Values:     f.Values,
			NestedPath: f.NestedPath,
		})
	}

	sorts := make([]core.SortOption, 0, len(req.Sort))
	for _, s := range req.Sort {
		if s == nil || s.Field == "" {
			continue
		}
		sorts = append(sorts, core.SortOption{Field: s.Field, Desc: s.Desc})
	}

	return core.SearchRequest{
		Resource: req.Resource,
		Query:    req.Query,
		Page:     req.Page,
		PageSize: req.PageSize,
		Filters:  filters,
		Sort:     sorts,
	}
}

func protoFilterOp(op search.FilterOp) core.FilterOp {
	switch op {
	case search.FilterOp_FILTER_OP_IN:
		return core.FilterOpIn
	default:
		return core.FilterOpEq
	}
}

func searchResponseToProto(resp core.SearchResponse) *search.SearchResponse {
	hits := make([]*search.SearchHit, 0, len(resp.Hits))
	for _, h := range resp.Hits {
		st, err := structpb.NewStruct(h.Source)
		if err != nil {
			continue
		}
		hits = append(hits, &search.SearchHit{
			Id:     h.ID,
			Score:  h.Score,
			Source: st,
		})
	}
	return &search.SearchResponse{Total: resp.Total, Hits: hits}
}

func capabilitiesToProto(caps core.CapabilitiesResponse) *search.GetCapabilitiesResponse {
	resp := &search.GetCapabilitiesResponse{}
	for _, rc := range caps.Resources {
		cap := &search.ResourceCapability{Resource: rc.Resource}
		for _, f := range rc.Fields {
			pf := &search.FieldCapability{
				Field:      f.Field,
				Type:       f.Type,
				Searchable: f.Searchable,
				Sortable:   f.Sortable,
			}
			for _, op := range f.FilterOps {
				switch op {
				case core.FilterOpEq:
					pf.FilterOps = append(pf.FilterOps, search.FilterOp_FILTER_OP_EQ)
				case core.FilterOpIn:
					pf.FilterOps = append(pf.FilterOps, search.FilterOp_FILTER_OP_IN)
				}
			}
			cap.Fields = append(cap.Fields, pf)
		}
		resp.Resources = append(resp.Resources, cap)
	}
	return resp
}
