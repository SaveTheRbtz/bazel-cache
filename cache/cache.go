package cache

import (
	"context"
	"errors"
	"io"

	"github.com/znly/bazel-cache/utils"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrInvalidSize = errors.New("invalid size")
)

type EntryKind string

const (
	AC  EntryKind = "ac"
	CAS EntryKind = "cas"
)

type Cache interface {
	Put(ctx context.Context, kind EntryKind, hash string, size, offset int64) (io.WriteCloser, error)
	Get(ctx context.Context, kind EntryKind, hash string, offset, length int64) (io.ReadCloser, int64, error)
	Contains(ctx context.Context, kind EntryKind, hash string) (bool, int64, error)
}

type GatedCache struct {
	cache     Cache
	readGate  utils.Gate
	writeGate utils.Gate
}

func NewGatedCache(cache Cache, maxConcurrentReads, maxConcurrentWrites int) (Cache, error) {
	return &GatedCache{
		cache:     cache,
		readGate:  utils.NewGate(maxConcurrentReads),
		writeGate: utils.NewGate(maxConcurrentWrites),
	}, nil
}
func (c *GatedCache) Contains(ctx context.Context, kind EntryKind, hash string) (bool, int64, error) {
	defer c.readGate.Start().Done()
	return c.cache.Contains(ctx, kind, hash)
}

func (c *GatedCache) Get(ctx context.Context, kind EntryKind, hash string, offset, length int64) (io.ReadCloser, int64, error) {
	defer c.readGate.Start().Done()
	return c.cache.Get(ctx, kind, hash, offset, length)
}

func (c *GatedCache) Put(ctx context.Context, kind EntryKind, hash string, size, offset int64) (io.WriteCloser, error) {
	defer c.writeGate.Start().Done()
	return c.cache.Put(ctx, kind, hash, size, offset)
}

// type CompressedCache struct {
// 	cache Cache
// }

// func NewCompressedCache(cache Cache, compressionLevel int) (Cache, error) {
// 	return &CompressedCache{
// 		cache: cache,
// 	}, nil
// }
// func (c *CompressedCache) Contains(ctx context.Context, kind EntryKind, hash string) (bool, int64, error) {
// 	return c.cache.Contains(ctx, kind, hash)
// }

// func (c *CompressedCache) Get(ctx context.Context, kind EntryKind, hash string, offset, length int64) (io.ReadCloser, int64, error) {
// 	return c.cache.Get(ctx, kind, hash, offset, length)
// }

// func (c *CompressedCache) Put(ctx context.Context, kind EntryKind, hash string, offset int64) (io.WriteCloser, error) {
// 	wc, err := c.cache.Put(ctx, kind, hash, offset)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return gzip.NewWriter(wc), nil
// }

// type CacheChain struct {
// 	caches []Cache
// }

// func NewCacheChain(caches ...Cache) *CacheChain {
// 	return &CacheChain{
// 		caches: caches,
// 	}
// }

// func (cc *CacheChain) Put(ctx context.Context, kind EntryKind, hash string, data []byte, offset int64) error {
// 	eg, ctx := errgroup.WithContext(ctx)
// 	for _, cache_ := range cc.caches {
// 		cache := cache_ // prevents shadowing
// 		eg.Go(func() error {
// 			return cache.Put(ctx, kind, hash, data, offset)
// 		})
// 	}
// 	return eg.Wait()
// }

// func (cc *CacheChain) Get(ctx context.Context, kind EntryKind, hash string) (io.ReadCloser, int64, error) {
// 	for _, cache := range cc.caches {
// 		rdc, size, err := cache.Get(ctx, kind, hash, 0, -1)
// 		if err == nil {
// 			return rdc, size, nil
// 		}
// 	}
// 	return nil, 0, ErrNotFound
// }

// func (cc *CacheChain) Contains(ctx context.Context, kind EntryKind, hash string) (bool, int64, error) {
// 	for _, cache := range cc.caches {
// 		found, size, err := cache.Contains(ctx, kind, hash)
// 		if found && err == nil {
// 			return found, size, nil
// 		}
// 	}
// 	return false, 0, ErrNotFound
// }
