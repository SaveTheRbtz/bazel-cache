package disk

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"github.com/znly/bazel-cache/cache"
)

const Scheme = "file"

func init() {
	cache.RegisterCache(Scheme, func(ctx context.Context, uri *url.URL) (cache.Cache, error) {
		return New(ctx, uri.Path)
	})
}

type SectionReadCloser struct {
	io.Reader
	io.Closer
}

type DiskCache struct {
	cache.Cache

	path string
}

func New(ctx context.Context, path string) (*DiskCache, error) {
	// Create the intermediate hash directories
	for _, kind := range []cache.EntryKind{cache.AC, cache.CAS} {
		for i := 0; i <= 0xFF; i++ {
			if err := os.MkdirAll(filepath.Join(path, string(kind), fmt.Sprintf("%02x", i)), 0700); err != nil {
				return nil, err
			}
		}
	}

	return &DiskCache{
		path: path,
	}, nil
}

func (c *DiskCache) objectPath(kind cache.EntryKind, hash string) string {
	return filepath.Join(c.path, string(kind), hash[:2], hash)
}

func (c *DiskCache) Contains(ctx context.Context, kind cache.EntryKind, hash string) (bool, int64, error) {
	fi, err := os.Stat(c.objectPath(kind, hash))
	if err != nil {
		return false, 0, err
	}
	return true, fi.Size(), nil
}

func (c *DiskCache) Get(ctx context.Context, kind cache.EntryKind, hash string, offset, length int64) (io.ReadCloser, int64, error) {
	var rdc io.ReadCloser

	f, err := os.Open(c.objectPath(kind, hash))
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		defer f.Close()
		return nil, 0, err
	}

	rdc = f
	if offset > 0 || length > 0 {
		rdc = &SectionReadCloser{
			Reader: io.NewSectionReader(f, offset, length),
			Closer: f,
		}
	}

	return rdc, fi.Size(), nil
}

func (c *DiskCache) Put(ctx context.Context, kind cache.EntryKind, hash string, size, offset int64) (io.WriteCloser, error) {
	f, err := os.OpenFile(c.objectPath(kind, hash), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	if offset > 0 {
		if _, err := f.Seek(offset, os.SEEK_SET); err != nil {
			return nil, err
		}
	}

	return f, nil
}
