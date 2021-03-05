package server

import (
	"context"
	"sync"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	rpc_status "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/znly/bazel-cache/cache"
	"github.com/znly/bazel-cache/utils"
)

func (cs *cacheServer) FindMissingBlobs(ctx context.Context, req *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	logger := ctxzap.Extract(ctx).With(
		zap.String("cache.kind", string(cache.CAS)),
	)

	resp := &pb.FindMissingBlobsResponse{}
	missingBlobDigestsMu := &sync.Mutex{}

	eg, egCtx := errgroup.WithContext(ctx)
	for _, digest_ := range req.BlobDigests {
		digest := digest_
		if utils.IsEmptyHash(digest.Hash) {
			continue
		}
		if err := utils.ValidateHash(digest.Hash, digest.SizeBytes); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		eg.Go(func() error {
			found, err := cs.cache.Contains(egCtx, cache.CAS, digest)
			if found == false || err != nil {
				logger.With(zap.String("hash", digest.Hash), zap.Error(err)).Info("missing blob")
				missingBlobDigestsMu.Lock()
				resp.MissingBlobDigests = append(resp.MissingBlobDigests, digest)
				missingBlobDigestsMu.Unlock()
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

func (cs *cacheServer) BatchUpdateBlobs(ctx context.Context, req *pb.BatchUpdateBlobsRequest) (*pb.BatchUpdateBlobsResponse, error) {
	logger := ctxzap.Extract(ctx).With(
		zap.String("cache.kind", string(cache.CAS)),
	)

	resp := &pb.BatchUpdateBlobsResponse{
		Responses: make([]*pb.BatchUpdateBlobsResponse_Response, 0, len(req.Requests)),
	}

	eg := &errgroup.Group{}
	for _, updateReq_ := range req.Requests {
		updateReq := updateReq_
		if err := utils.ValidateHash(updateReq.Digest.Hash, updateReq.Digest.SizeBytes); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		updateResp := &pb.BatchUpdateBlobsResponse_Response{
			Digest: updateReq.Digest,
			Status: &rpc_status.Status{Code: int32(codes.NotFound)},
		}
		resp.Responses = append(resp.Responses, updateResp)

		if utils.IsEmptyHash(updateReq.Digest.Hash) {
			updateResp.Status.Code = int32(codes.OK)
			continue
		}
		eg.Go(func() error {
			if err := cs.cache.PutBytes(ctx, cache.CAS, updateReq.Digest, updateReq.Data); err == nil {
				updateResp.Status.Code = int32(codes.OK)
			} else {
				logger.With(zap.String("hash", updateReq.Digest.Hash), zap.Error(err)).Error("failed to update blob")
			}
			return nil
		})
	}

	return resp, nil
}

func (cs *cacheServer) BatchReadBlobs(ctx context.Context, req *pb.BatchReadBlobsRequest) (*pb.BatchReadBlobsResponse, error) {
	logger := ctxzap.Extract(ctx).With(
		zap.String("cache.kind", string(cache.CAS)),
	)

	resp := &pb.BatchReadBlobsResponse{
		Responses: make([]*pb.BatchReadBlobsResponse_Response, 0, len(req.Digests)),
	}

	eg := errgroup.Group{}
	for _, digest_ := range req.Digests {
		digest := digest_
		if err := utils.ValidateHash(digest.Hash, digest.SizeBytes); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		readResp := &pb.BatchReadBlobsResponse_Response{
			Digest: digest,
			Status: &rpc_status.Status{Code: int32(codes.DataLoss)},
		}
		resp.Responses = append(resp.Responses, readResp)
		if utils.IsEmptyHash(readResp.Digest.Hash) {
			readResp.Status.Code = int32(codes.OK)
			continue
		}
		eg.Go(func() error {
			if data, err := cs.cache.GetBytes(ctx, cache.CAS, readResp.Digest); err == nil {
				readResp.Data = data
				readResp.Status.Code = int32(codes.OK)
			} else {
				logger.With(zap.String("hash", readResp.Digest.Hash), zap.Error(err)).Error("failed to read blob")
			}
			return nil
		})
	}

	return resp, nil
}

func (cs *cacheServer) readTreeInto(ctx context.Context, directories []*pb.Directory, rootDigest *pb.Digest) error {
	rootDir := &pb.Directory{}
	if err := cs.cache.GetProto(ctx, cache.CAS, rootDigest, rootDir); err != nil {
		return err
	}

	directories = append(directories, rootDir)

	for _, subDir := range rootDir.Directories {
		if err := cs.readTreeInto(ctx, directories, subDir.Digest); err != nil {
			return err
		}
	}

	return nil
}

func (cs *cacheServer) GetTree(req *pb.GetTreeRequest, stream pb.ContentAddressableStorage_GetTreeServer) error {
	// easier on the eyes
	ctx := stream.Context()

	resp := &pb.GetTreeResponse{
		Directories: make([]*pb.Directory, 0),
	}

	if utils.IsEmptyHash(req.RootDigest.Hash) == false {
		if err := utils.ValidateHash(req.RootDigest.Hash, req.RootDigest.SizeBytes); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}

		if err := cs.readTreeInto(ctx, resp.Directories, req.RootDigest); err != nil {
			return status.Error(codes.DataLoss, err.Error())
		}
	}

	if err := stream.Send(resp); err != nil {
		return status.Error(codes.DataLoss, err.Error())
	}

	return nil
}
