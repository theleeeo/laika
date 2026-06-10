package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/indexer/core"
	"github.com/theleeeo/indexer/app/gen/index/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
	require.Equal(t, "stale version", st.Message())
}
