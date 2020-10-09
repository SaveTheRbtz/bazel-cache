package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"

	"github.com/znly/bazel-cache/cache"
	"github.com/znly/bazel-cache/utils"
)

var (
	ErrInvalidSize = errors.New("invalid size")
)

type CacheEx struct {
	cache.Cache
}

func NewCacheEx(c cache.Cache) *CacheEx {
	return &CacheEx{
		Cache: c,
	}
}

func (c *CacheEx) Contains(ctx context.Context, kind cache.EntryKind, digest *pb.Digest) (bool, error) {
	if utils.IsEmptyHash(digest.Hash) {
		return true, nil
	}

	found, size, err := c.Cache.Contains(ctx, kind, digest.Hash)
	if err == nil && digest.SizeBytes > 0 && size != digest.SizeBytes {
		return false, fmt.Errorf("size is %d, expected %d: %w", size, digest.SizeBytes, ErrInvalidSize)
	}
	return found, err
}

func (c *CacheEx) GetRange(ctx context.Context, kind cache.EntryKind, digest *pb.Digest, offset, length int64) (io.ReadCloser, error) {
	if utils.IsEmptyHash(digest.Hash) {
		return utils.NopReadWriteCloser, nil
	}

	rdc, size, err := c.Cache.Get(ctx, kind, digest.Hash, offset, length)
	if err == nil && digest.SizeBytes > 0 && size != digest.SizeBytes {
		defer rdc.Close()
		return nil, fmt.Errorf("size is %d, expected %d: %w", size, digest.SizeBytes, ErrInvalidSize)
	}
	return rdc, err
}

func (c *CacheEx) Get(ctx context.Context, kind cache.EntryKind, digest *pb.Digest) (io.ReadCloser, error) {
	return c.GetRange(ctx, kind, digest, 0, -1)
}

func (c *CacheEx) GetBytes(ctx context.Context, kind cache.EntryKind, digest *pb.Digest) ([]byte, error) {
	if utils.IsEmptyHash(digest.Hash) {
		return []byte{}, nil
	}

	rdc, err := c.Get(ctx, kind, digest)
	if err != nil {
		return nil, err
	}
	defer rdc.Close()

	return ioutil.ReadAll(rdc)
}

func (c *CacheEx) GetProto(ctx context.Context, kind cache.EntryKind, digest *pb.Digest, msg proto.Message) error {
	if utils.IsEmptyHash(digest.Hash) {
		return nil
	}

	data, err := c.GetBytes(ctx, kind, digest)
	if err != nil {
		return err
	}

	return proto.Unmarshal(data, msg)
}

func (c *CacheEx) Put(ctx context.Context, kind cache.EntryKind, digest *pb.Digest) (io.WriteCloser, error) {
	fmt.Println("PUT", digest.Hash)

	if utils.IsEmptyHash(digest.Hash) {
		return utils.NopReadWriteCloser, nil
	}

	return c.Cache.Put(ctx, kind, digest.Hash, digest.SizeBytes, 0)
}

func (c *CacheEx) PutBytes(ctx context.Context, kind cache.EntryKind, digest *pb.Digest, data []byte) error {
	fmt.Println("PUT BYTES", digest.Hash)

	if utils.IsEmptyHash(digest.Hash) {
		return nil
	}

	wc, err := c.Put(ctx, kind, digest)
	if err != nil {
		return err
	}
	defer wc.Close()

	if _, err := wc.Write(data); err != nil {
		return err
	}

	return nil
}

func (c *CacheEx) PutProto(ctx context.Context, kind cache.EntryKind, digest *pb.Digest, msg proto.Message) error {
	fmt.Println("PUT PROTO", digest.Hash)

	if utils.IsEmptyHash(digest.Hash) {
		return nil
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	digest.SizeBytes = int64(len(data))
	return c.PutBytes(ctx, kind, digest, data)
}
