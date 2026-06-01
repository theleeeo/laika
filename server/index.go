package server

import (
	"context"
	"errors"

	"github.com/theleeeo/indexer/core"
	"github.com/theleeeo/indexer/gen/index/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type IndexerServer struct {
	index.UnimplementedIndexServiceServer

	idx *core.Indexer
}

func NewIndexer(idx *core.Indexer) *IndexerServer {
	return &IndexerServer{
		idx: idx,
	}
}

func (s *IndexerServer) NotifyChange(ctx context.Context, req *index.NotifyChangeRequest) (*index.NotifyChangeResponse, error) {
	if req.Notification == nil {
		return nil, status.Error(codes.InvalidArgument, "notification is required")
	}

	n := protoToNotification(req.Notification)

	if err := s.idx.RegisterChange(ctx, n); err != nil {
		return nil, mapAppError(err)
	}

	return &index.NotifyChangeResponse{}, nil
}

func (s *IndexerServer) NotifyChangeBatch(ctx context.Context, req *index.NotifyChangeBatchRequest) (*index.NotifyChangeBatchResponse, error) {
	if len(req.Notifications) == 0 {
		return &index.NotifyChangeBatchResponse{}, nil
	}

	for _, pn := range req.Notifications {
		if pn == nil {
			continue
		}
		n := protoToNotification(pn)
		if err := s.idx.RegisterChange(ctx, n); err != nil {
			return nil, mapAppError(err)
		}
	}

	return &index.NotifyChangeBatchResponse{}, nil
}

func protoToNotification(pn *index.ChangeNotification) core.Notification {
	n := core.Notification{
		ResourceType: pn.ResourceType,
		ResourceID:   pn.ResourceId,
		Metadata:     pn.Metadata,
		Version:      pn.Version,
	}

	switch pn.Kind {
	case index.ChangeKind_CHANGE_KIND_CREATED:
		n.Kind = core.ChangeCreated
	case index.ChangeKind_CHANGE_KIND_UPDATED:
		n.Kind = core.ChangeUpdated
	case index.ChangeKind_CHANGE_KIND_DELETED:
		n.Kind = core.ChangeDeleted
	}

	return n
}

func mapAppError(err error) error {
	if errors.Is(err, core.ErrUnknownResource) {
		return status.Error(codes.InvalidArgument, "unknown resource")
	}
	if errors.Is(err, core.ErrStaleVersion) {
		return status.Error(codes.FailedPrecondition, "stale version")
	}
	if invalidArgsErr, ok := errors.AsType[*core.InvalidArgumentError](err); ok {
		return status.Error(codes.InvalidArgument, invalidArgsErr.Msg)
	}
	return status.Error(codes.Internal, err.Error())
}

func (s *IndexerServer) Rebuild(ctx context.Context, req *index.RebuildRequest) (*index.RebuildResponse, error) {
	if len(req.Selectors) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one selector is required")
	}

	selectors := make([]core.ResourceSelector, len(req.Selectors))
	for i, ps := range req.Selectors {
		versions := make([]int, len(ps.Versions))
		for j, v := range ps.Versions {
			versions[j] = int(v)
		}
		selectors[i] = core.ResourceSelector{
			ResourceType: ps.ResourceType,
			Versions:     versions,
			ResourceIDs:  ps.ResourceIds,
		}
	}

	if err := s.idx.Rebuild(ctx, selectors); err != nil {
		return nil, mapAppError(err)
	}

	return &index.RebuildResponse{}, nil
}
