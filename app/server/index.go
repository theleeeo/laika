package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/theleeeo/laika/app/gen/index/v1"
	"github.com/theleeeo/laika/core"

	"connectrpc.com/connect"
)

type IndexerServer struct {
	idx *core.Indexer
}

func NewIndexer(idx *core.Indexer) *IndexerServer {
	return &IndexerServer{
		idx: idx,
	}
}

func (s *IndexerServer) NotifyChange(ctx context.Context, req *connect.Request[index.NotifyChangeRequest]) (*connect.Response[index.NotifyChangeResponse], error) {
	if req.Msg.Notification == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("notification is required"))
	}

	n := protoToNotification(req.Msg.Notification)

	if err := s.idx.RegisterChange(ctx, n); err != nil {
		return nil, mapAppError(err)
	}

	return connect.NewResponse(&index.NotifyChangeResponse{}), nil
}

func (s *IndexerServer) NotifyChangeBatch(ctx context.Context, req *connect.Request[index.NotifyChangeBatchRequest]) (*connect.Response[index.NotifyChangeBatchResponse], error) {
	ns := make([]core.Notification, len(req.Msg.Notifications))
	for i, pn := range req.Msg.Notifications {
		if pn == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("notification %d is required", i))
		}
		ns[i] = protoToNotification(pn)
	}

	statuses, err := s.idx.RegisterChanges(ctx, ns)
	if err != nil {
		return nil, mapAppError(err)
	}

	return connect.NewResponse(&index.NotifyChangeBatchResponse{Statuses: statusesToProto(statuses)}), nil
}

func statusesToProto(statuses []core.RegisterStatus) []index.ChangeStatus {
	out := make([]index.ChangeStatus, len(statuses))
	for i, st := range statuses {
		switch st {
		case core.RegisterAccepted:
			out[i] = index.ChangeStatus_CHANGE_STATUS_ACCEPTED
		case core.RegisterStale:
			out[i] = index.ChangeStatus_CHANGE_STATUS_STALE
		default:
			panic(fmt.Sprintf("unmapped core.RegisterStatus %d", st))
		}
	}
	return out
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
		return connect.NewError(connect.CodeInvalidArgument, errors.New("unknown resource"))
	}
	if errors.Is(err, core.ErrStaleVersion) {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("stale version"))
	}
	if errors.Is(err, core.ErrRegistrationAborted) {
		return connect.NewError(connect.CodeAborted, errors.New("registration aborted by a concurrent change; retry it"))
	}
	if invalidArgsErr, ok := errors.AsType[*core.InvalidArgumentError](err); ok {
		return connect.NewError(connect.CodeInvalidArgument, errors.New(invalidArgsErr.Msg))
	}
	return connect.NewError(connect.CodeInternal, err)
}

func (s *IndexerServer) Rebuild(ctx context.Context, req *connect.Request[index.RebuildRequest]) (*connect.Response[index.RebuildResponse], error) {
	if len(req.Msg.Selectors) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("at least one selector is required"))
	}

	selectors := make([]core.ResourceSelector, len(req.Msg.Selectors))
	for i, ps := range req.Msg.Selectors {
		versions := make([]int, len(ps.Versions))
		for j, v := range ps.Versions {
			versions[j] = int(v)
		}
		selectors[i] = core.ResourceSelector{
			ResourceType: ps.ResourceType,
			Versions:     versions,
			ResourceIDs:  ps.ResourceIds,
			Metadata:     ps.Metadata,
		}
	}

	workflowIDs, err := s.idx.Rebuild(ctx, selectors)
	if err != nil {
		return nil, mapAppError(err)
	}

	return connect.NewResponse(&index.RebuildResponse{WorkflowIds: workflowIDs}), nil
}
