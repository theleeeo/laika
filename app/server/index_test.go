package server

import (
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/app/gen/index/v1"
	"github.com/theleeeo/laika/core"
)

func TestProtoToNotification_Metadata(t *testing.T) {
	pn := &index.ChangeNotification{
		Kind:         index.ChangeKind_CHANGE_KIND_UPDATED,
		ResourceType: "order",
		ResourceId:   "42",
		Metadata: map[string]string{
			"tenant-id": "acme",
			"trace-id":  "trace-1",
		},
	}

	n := protoToNotification(pn)
	require.Equal(t, core.ChangeUpdated, n.Kind)
	require.Equal(t, "order", n.ResourceType)
	require.Equal(t, "42", n.ResourceID)
	require.Equal(t, pn.Metadata, n.Metadata)
}

func TestProtoToNotification_Version(t *testing.T) {
	pn := &index.ChangeNotification{
		Kind:         index.ChangeKind_CHANGE_KIND_CREATED,
		ResourceType: "product",
		ResourceId:   "99",
		Version:      7,
	}

	n := protoToNotification(pn)
	require.Equal(t, core.ChangeCreated, n.Kind)
	require.Equal(t, "product", n.ResourceType)
	require.Equal(t, "99", n.ResourceID)
	require.Equal(t, int64(7), n.Version)
}

func TestProtoToNotification_VersionZero(t *testing.T) {
	pn := &index.ChangeNotification{
		Kind:         index.ChangeKind_CHANGE_KIND_UPDATED,
		ResourceType: "order",
		ResourceId:   "1",
	}

	n := protoToNotification(pn)
	require.Equal(t, int64(0), n.Version)
}

func TestMapAppError_StaleVersion(t *testing.T) {
	err := mapAppError(core.ErrStaleVersion)

	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	require.Equal(t, connect.CodeFailedPrecondition, connectErr.Code())
	require.Equal(t, "stale version", connectErr.Message())
}

func TestStatusesToProto_IndexAligned(t *testing.T) {
	got := statusesToProto([]core.RegisterStatus{core.RegisterStale, core.RegisterAccepted, core.RegisterStale})
	require.Equal(t, []index.ChangeStatus{
		index.ChangeStatus_CHANGE_STATUS_STALE,
		index.ChangeStatus_CHANGE_STATUS_ACCEPTED,
		index.ChangeStatus_CHANGE_STATUS_STALE,
	}, got)
}

// A nil entry has no status to report: the batch is rejected rather than
// silently misaligning the statuses.
func TestNotifyChangeBatch_NilNotification_InvalidArgument(t *testing.T) {
	s := NewIndexer(nil)
	_, err := s.NotifyChangeBatch(t.Context(), connect.NewRequest(&index.NotifyChangeBatchRequest{
		Notifications: []*index.ChangeNotification{{ResourceType: "a", ResourceId: "1"}, nil},
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestMapAppError_RegistrationAborted(t *testing.T) {
	// As the Indexer returns it: the Store's wrap of the Postgres error, wrapped again.
	err := mapAppError(fmt.Errorf("registering 2 changes: %w: %w", core.ErrRegistrationAborted, errors.New("deadlock detected (SQLSTATE 40P01)")))

	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	require.Equal(t, connect.CodeAborted, connectErr.Code())
	require.Equal(t, "registration aborted by a concurrent change; retry it", connectErr.Message())
}

func TestMapAppError_InvalidArgument(t *testing.T) {
	err := mapAppError(&core.InvalidArgumentError{Msg: "resource a/1 appears more than once in the batch"})
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}
