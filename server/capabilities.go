package server

import (
	"context"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
)

func (cs *cacheServer) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	resp := pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunction: []pb.DigestFunction_Value{
				pb.DigestFunction_SHA256,
			},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			CachePriorityCapabilities: &pb.PriorityCapabilities{
				Priorities: []*pb.PriorityCapabilities_PriorityRange{
					{
						MinPriority: 0,
						MaxPriority: 0,
					},
				},
			},
			MaxBatchTotalSizeBytes:      0,
			SymlinkAbsolutePathStrategy: pb.SymlinkAbsolutePathStrategy_ALLOWED,
		},
		LowApiVersion: &semver.SemVer{
			Major: 2,
		},
		HighApiVersion: &semver.SemVer{
			Major: 2,
			Minor: 1,
		},
	}

	return &resp, nil
}
