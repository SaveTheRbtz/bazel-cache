package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/znly/bazel-cache/cache"
	"github.com/znly/bazel-cache/utils"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// The maximum chunk size to write back to the client in Send calls.
	// Inspired by Goma's FileBlob.FILE_CHUNK maxium size.
	maxChunkSize = 2 * 1024 * 1024 // 2M
)

var (
	ErrInvalidResourceName = errors.New("invalid resource name")
)

type bytestreamResource struct {
	instanceName string
	digest       *pb.Digest
}

var (
	readRegexp  = regexp.MustCompile(`^(\w+/)?blobs/([0-9a-f]{64})/([0-9]+)$`)
	writeRegexp = regexp.MustCompile(`^(\w+/)?uploads/[0-9a-f-]+/blobs/([0-9a-f]{64})/([0-9]+)`)
)

func bufSize(size int64) int {
	if size > maxChunkSize {
		return maxChunkSize
	}
	return int(size)
}

func parseBytestreamResource(name string, isWrite bool) (*bytestreamResource, error) {
	re := readRegexp
	if isWrite {
		re = writeRegexp
	}

	parts := re.FindStringSubmatch(name)
	if len(parts) == 0 {
		return nil, ErrInvalidResourceName
	}

	strSize := ""
	res := &bytestreamResource{}
	res.instanceName = strings.TrimRight(parts[1], "/")

	res.digest = &pb.Digest{
		Hash: parts[2],
	}
	strSize = parts[3]

	size, err := strconv.ParseInt(strSize, 10, 64)
	if err != nil {
		return nil, err
	}
	res.digest.SizeBytes = size

	return res, nil
}

func (cs *cacheServer) Read(req *bytestream.ReadRequest, stream bytestream.ByteStream_ReadServer) error {
	ctx := stream.Context()

	logger := ctxzap.Extract(ctx).With(
		zap.String("cache.kind", string(cache.CAS)),
		zap.String("resource.name", req.ResourceName),
	)

	if req.ReadOffset < 0 || req.ReadLimit < 0 {
		return status.Error(codes.OutOfRange, "ReadOffset or ReadLimit is out of range")
	}

	resource, err := parseBytestreamResource(req.ResourceName, false)
	if err != nil {
		logger.With(zap.Error(fmt.Errorf("invalid resource name: %w", err))).Error("unable to parse resource name")
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if err := utils.ValidateHash(resource.digest.Hash, resource.digest.SizeBytes); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if req.ReadOffset > resource.digest.SizeBytes {
		err := fmt.Errorf("ReadOffset %d is bigger than size %d", req.ReadOffset, resource.digest.SizeBytes)
		logger.With(zap.Error(err)).Error("invalid argument")
		return status.Error(codes.OutOfRange, err.Error())
	}

	rdc, err := cs.cache.GetRange(ctx, cache.CAS, resource.digest, req.ReadOffset, req.ReadLimit)
	if err != nil {
		logger.With(zap.Bool("cache.hit", false)).Info("cache miss")
		return status.Error(codes.NotFound, fmt.Sprintf("CAS blob not found: %q", err.Error()))
	}
	defer rdc.Close()

	logger.With(zap.Bool("cache.hit", true)).Info("cache hit")

	bufRdr := bufio.NewReaderSize(rdc, bufSize(resource.digest.SizeBytes))
	buf := make([]byte, bufRdr.Size())

	respPool := sync.Pool{
		New: func() interface{} {
			return &bytestream.ReadResponse{}
		},
	}

	doSend := func(data []byte) error {
		resp := respPool.Get().(*bytestream.ReadResponse)
		defer respPool.Put(resp)
		resp.Data = data
		return stream.Send(resp)
	}

	for {
		n, err := bufRdr.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if err := doSend(buf[:n]); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}

	return nil
}

func (cs *cacheServer) Write(stream bytestream.ByteStream_WriteServer) error {
	ctx := stream.Context()
	resp := &bytestream.WriteResponse{}

	var (
		resource *bytestreamResource
		wc       io.WriteCloser
	)
loop:
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break loop
			}
			return status.Error(codes.Internal, err.Error())
		}

		if resource == nil && req.ResourceName != "" {
			resource, err = parseBytestreamResource(req.ResourceName, true)
			if err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}

			wc, err = cs.cache.Put(ctx, cache.CAS, resource.digest)
			if err != nil {
				return status.Error(codes.DataLoss, err.Error())
			}
			// Defer anyway
			defer wc.Close()
		}

		written, err := wc.Write(req.Data)
		if err != nil {
			return status.Error(codes.DataLoss, err.Error())
		}
		resp.CommittedSize += int64(written)

		if req.FinishWrite {
			break loop
		}
	}

	if err := wc.Close(); err != nil {
		return status.Error(codes.DataLoss, err.Error())
	}

	if err := stream.SendAndClose(resp); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}

func (cs *cacheServer) QueryWriteStatus(context.Context, *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	return &bytestream.QueryWriteStatusResponse{}, nil
}
