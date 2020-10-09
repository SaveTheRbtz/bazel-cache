package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/znly/bazel-cache/utils"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/znly/bazel-cache/cache"
)

var (
	ErrACNotFound = errors.New("ActionResult not found or not complete")
)

func digestFromContent(data []byte) *pb.Digest {
	h := sha256.Sum256(data)
	return &pb.Digest{
		Hash:      hex.EncodeToString(h[:]),
		SizeBytes: int64(len(data)),
	}
}

func actionCacheKey(key, instance string) string {
	if instance == "" {
		return key
	}
	h := sha256.Sum256([]byte(key + instance))
	return hex.EncodeToString(h[:])
}

func shouldWrite(digest *pb.Digest, data []byte) bool {
	if digest == nil {
		return false
	}
	return digest.SizeBytes == int64(len(data))
}

func (cs *cacheServer) acGetWithValidatedDependencies(ctx context.Context, digest *pb.Digest) (*pb.ActionResult, error) {
	result := &pb.ActionResult{}

	if err := cs.cache.GetProto(ctx, cache.AC, digest, result); err != nil {
		return nil, err
	}

	digestsToCheck := []*pb.Digest{}
	digestsToCheckMu := &sync.Mutex{}

	if result.StdoutDigest != nil {
		digestsToCheck = append(digestsToCheck, result.StdoutDigest)
	}
	if result.StderrDigest != nil {
		digestsToCheck = append(digestsToCheck, result.StderrDigest)
	}

	for _, f := range result.OutputFiles {
		digestsToCheck = append(digestsToCheck, f.Digest)
	}

	directoriesGroup, directoriesCtx := errgroup.WithContext(ctx)
	for _, directory_ := range result.OutputDirectories {
		directory := directory_
		directoriesGroup.Go(func() error {
			tree := &pb.Tree{}
			if err := cs.cache.GetProto(directoriesCtx, cache.CAS, directory.TreeDigest, tree); err != nil {
				return err
			}

			for _, f := range tree.Root.GetFiles() {
				digestsToCheckMu.Lock()
				digestsToCheck = append(digestsToCheck, f.Digest)
				digestsToCheckMu.Unlock()
			}

			for _, child := range tree.GetChildren() {
				for _, f := range child.GetFiles() {
					digestsToCheckMu.Lock()
					digestsToCheck = append(digestsToCheck, f.Digest)
					digestsToCheckMu.Unlock()
				}
			}

			return nil
		})
	}

	if err := directoriesGroup.Wait(); err != nil {
		return nil, err
	}

	digestsGroup, digestsCtx := errgroup.WithContext(ctx)
	for _, digest_ := range digestsToCheck {
		digest := digest_
		digestsGroup.Go(func() error {
			_, err := cs.cache.Contains(digestsCtx, cache.CAS, digest)
			return err
		})
	}

	if err := digestsGroup.Wait(); err != nil {
		return nil, err
	}

	return result, nil
}

func (cs *cacheServer) GetActionResult(ctx context.Context, req *pb.GetActionResultRequest) (*pb.ActionResult, error) {
	logger := ctxzap.Extract(ctx).With(
		zap.String("request.hash", req.ActionDigest.Hash),
		zap.String("request.instance_name", req.InstanceName),
	)

	resp := &pb.ActionResult{}

	if err := utils.ValidateHash(req.ActionDigest.Hash, req.ActionDigest.SizeBytes); err != nil {
		logger.With(zap.Error(err)).Error("hash is not valid")
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	req.ActionDigest.SizeBytes = -1

	// Include InstanceName in key if needed
	// if req.InstanceName != "" {
	// 	req.ActionDigest.Hash = actionCacheKey(req.ActionDigest.Hash, req.InstanceName)
	// }

	resp, err := cs.acGetWithValidatedDependencies(ctx, req.ActionDigest)
	if err != nil {
		logger.With(zap.Bool("cache.hit", false)).Info("cache miss")
		return nil, status.Error(codes.NotFound, err.Error())
	}

	logger.With(zap.Bool("cache.hit", true)).Info("cache hit")
	return resp, nil
}

func (cs *cacheServer) UpdateActionResult(ctx context.Context, req *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	logger := ctxzap.Extract(ctx).With(
		zap.String("request.hash", req.ActionDigest.Hash),
		zap.String("request.instance_name", req.InstanceName),
	)

	if err := utils.ValidateHash(req.ActionDigest.Hash, req.ActionDigest.SizeBytes); err != nil {
		logger.With(zap.Error(err)).Error("hash is not valid")
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Include InstanceName in key if needed
	// if req.InstanceName != "" {
	// 	req.ActionDigest.Hash = actionCacheKey(req.ActionDigest.Hash, req.InstanceName)
	// }

	if err := cs.cache.PutProto(ctx, cache.AC, req.ActionDigest, req.ActionResult); err != nil {
		logger.With(zap.Error(err)).Error("unable to write ActionResult")
		return nil, status.Error(codes.Internal, err.Error())
	}

	eg := errgroup.Group{}

	if shouldWrite(req.ActionResult.StdoutDigest, req.ActionResult.StdoutRaw) {
		eg.Go(func() error {
			return cs.cache.PutBytes(ctx, cache.CAS, req.ActionResult.StdoutDigest, req.ActionResult.StdoutRaw)
		})
	}
	if shouldWrite(req.ActionResult.StderrDigest, req.ActionResult.StderrRaw) {
		eg.Go(func() error {
			return cs.cache.PutBytes(ctx, cache.CAS, req.ActionResult.StdoutDigest, req.ActionResult.StderrRaw)
		})
	}

	for _, f_ := range req.ActionResult.OutputFiles {
		f := f_
		if shouldWrite(f.Digest, f.Contents) {
			eg.Go(func() error {
				return cs.cache.PutBytes(ctx, cache.CAS, f.Digest, f.Contents)
			})
		}
	}

	if err := eg.Wait(); err != nil {
		logger.With(zap.Error(err)).Error("unable to put objects")
		return nil, status.Error(codes.Internal, err.Error())
	}

	return req.ActionResult, nil
}
