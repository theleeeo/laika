package server

import (
	"context"
	"errors"

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
