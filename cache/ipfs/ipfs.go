package ipfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"path/filepath"

	"github.com/znly/bazel-cache/cache"

	shell "github.com/ipfs/go-ipfs-api"
)

const Scheme = "ipfs"

func init() {
	cache.RegisterCache(Scheme, func(ctx context.Context, uri *url.URL) (cache.Cache, error) {
		return New(ctx, uri.Host)
	})
}

type SectionReadCloser struct {
	io.Reader
	io.Closer
}

type IPFSCache struct {
	cache.Cache

	sh *shell.Shell
}

func New(ctx context.Context, path string) (*IPFSCache, error) {
	sh := shell.NewShell(path)

	for _, kind := range []cache.EntryKind{cache.AC, cache.CAS} {
		for i := 0; i <= 0xFF; i++ {
			if err := sh.FilesMkdir(ctx, filepath.Join("/", string(kind), fmt.Sprintf("%02x", i)), shell.FilesMkdir.Parents(true)); err != nil {
				return nil, err
			}
		}
	}

	_ = sh.FilesMkdir(ctx, "/tmp")

	return &IPFSCache{
		sh: sh,
	}, nil
}

func (c *IPFSCache) objectPath(kind cache.EntryKind, hash string) string {
	return filepath.Join("/", string(kind), hash[:2], hash)
}

func (c *IPFSCache) Contains(ctx context.Context, kind cache.EntryKind, hash string) (bool, int64, error) {
	ctx = context.TODO()

	fi, err := c.sh.FilesStat(ctx, c.objectPath(kind, hash))
	if err != nil {
		return false, 0, err
	}
	return true, int64(fi.Size), nil
}

func (c *IPFSCache) Get(ctx context.Context, kind cache.EntryKind, hash string, offset, length int64) (io.ReadCloser, int64, error) {
	ctx = context.TODO()

	var rdc io.ReadCloser

	_, size, err := c.Contains(ctx, kind, hash)
	if err != nil {
		return nil, 0, err
	}

	var opts []shell.FilesOpt
	if offset > 0 {
		opts = append(opts, shell.FilesRead.Offset(offset))
	}

	f, err := c.sh.FilesRead(ctx, c.objectPath(kind, hash), opts...)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	// XXXX
	s, err := io.ReadAll(f)
	if err != nil {
		return nil, 0, err
	}
	f = io.NopCloser(bytes.NewReader(s))

	rdc = f
	if length > 0 {
		rdc = &SectionReadCloser{
			Reader: io.LimitReader(f, length),
			Closer: f,
		}
	}

	return rdc, size, nil
}

func (c *IPFSCache) Put(ctx context.Context, kind cache.EntryKind, hash string, size, offset int64) (io.WriteCloser, error) {
	ctx = context.TODO()

	pr, pw := io.Pipe()

	// XXX atomic write
	// XXX limit concurrency
	go func() {
		tmpName := fmt.Sprintf("/tmp/%d", rand.Int63())
		err := c.sh.FilesWrite(
			ctx, tmpName, pr, shell.FilesWrite.Offset(offset), shell.FilesWrite.Parents(true), shell.FilesWrite.Create(true), shell.FilesWrite.Truncate(true))
		if err != nil {
			pw.CloseWithError(err)
			_ = c.sh.FilesRm(ctx, tmpName, true)
		} else {
			pw.Close()
			_, _ = c.sh.FilesFlush(ctx, tmpName)
			_ = c.sh.FilesMv(ctx, tmpName, c.objectPath(kind, hash))
		}
		pr.Close()
	}()

	return pw, nil
}
