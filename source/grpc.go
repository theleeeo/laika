package source

import (
	"context"
	"fmt"

	pb "github.com/theleeeo/indexer/gen/provider/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCProvider implements Provider by calling a remote gRPC ProviderService.
type GRPCProvider struct {
	conn   *grpc.ClientConn
	client pb.ProviderServiceClient
}

// NewGRPCProvider dials the given address and returns a Provider backed by the
// remote ProviderService plugin.
func NewGRPCProvider(addr string) (*GRPCProvider, error) {
	if addr == "" {
		return nil, fmt.Errorf("provider address is required")
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial provider plugin %s: %w", addr, err)
	}
	return &GRPCProvider{
		conn:   conn,
		client: pb.NewProviderServiceClient(conn),
	}, nil
}

func (p *GRPCProvider) FetchResource(ctx context.Context, params FetchResourceParams) (FetchResourceResult, error) {
	resp, err := p.client.FetchResource(ctx, &pb.FetchResourceRequest{
		ResourceType: params.ResourceType,
		ResourceId:   params.ResourceID,
		Metadata:     params.Metadata,
	})
	if err != nil {
		return FetchResourceResult{}, err
	}
	if resp.Data == nil {
		return FetchResourceResult{}, nil
	}
	return FetchResourceResult{Data: resp.Data.AsMap()}, nil
}

func (p *GRPCProvider) FetchRelated(ctx context.Context, params FetchRelatedParams) (FetchRelatedResult, error) {
	resp, err := p.client.FetchRelated(ctx, &pb.FetchRelatedRequest{
		ResourceType: params.ResourceType,
		Key: &pb.ResourceKey{
			Field: params.Key.Field,
			Value: params.Key.Value,
		},
		RootResource: &pb.RootResource{
			Type: params.RootResource.Type,
			Id:   params.RootResource.Id,
		},
		Metadata: params.Metadata,
	})
	if err != nil {
		return FetchRelatedResult{}, err
	}
	result := make([]RelatedResource, len(resp.RelatedResources))
	for i, s := range resp.RelatedResources {
		rr := RelatedResource{
			Data:    s.Data.AsMap(),
			Version: s.Version,
		}
		result[i] = rr
	}
	return FetchRelatedResult{Related: result}, nil
}

// Close releases the underlying gRPC connection.
func (p *GRPCProvider) Close() error {
	return p.conn.Close()
}

func (p *GRPCProvider) ListResources(ctx context.Context, params ListResourcesParams) (ListResourcesResult, error) {
	resp, err := p.client.ListResources(ctx, &pb.ListResourcesRequest{
		ResourceType: params.ResourceType,
		PageToken:    params.PageToken,
		PageSize:     params.PageSize,
		Metadata:     params.Metadata,
	})
	if err != nil {
		return ListResourcesResult{}, err
	}

	resources := make([]ListedResource, len(resp.Resources))
	for i, r := range resp.Resources {
		var data map[string]any
		if r.Data != nil {
			data = r.Data.AsMap()
		}
		resources[i] = ListedResource{
			ID:   r.ResourceId,
			Data: data,
		}
	}
	return ListResourcesResult{
		Resources:     resources,
		NextPageToken: resp.NextPageToken,
	}, nil
}
