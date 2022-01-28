package badger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
	badger "github.com/dgraph-io/badger/v3"
	options "github.com/dgraph-io/badger/v3/options"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/klauspost/compress/zstd"

	// TODO: use standard IPFS Splitter.
	fastcdc "github.com/reusee/fastcdc-go"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	ipfshttp "github.com/ipfs/go-ipfs-http-client"
	iface "github.com/ipfs/interface-go-ipfs-core"
	ifaceOpts "github.com/ipfs/interface-go-ipfs-core/options"
	ifacePath "github.com/ipfs/interface-go-ipfs-core/path"

	"github.com/znly/bazel-cache/cache"
)

const Scheme = "badger"

func init() {
	cache.RegisterCache(Scheme, func(ctx context.Context, uri *url.URL) (cache.Cache, error) {
		return New(ctx, uri.Path)
	})
}

// onceError is an object that will only store an error once.
type onceError struct {
	sync.Mutex // guards following
	err        error
}

func (a *onceError) Store(err error) {
	a.Lock()
	defer a.Unlock()
	if a.err != nil {
		return
	}
	a.err = err
}

func (a *onceError) Load() error {
	a.Lock()
	defer a.Unlock()
	return a.err
}

type chunkedWriter struct {
	ctx context.Context

	kind cache.EntryKind
	hash string
	size int64

	pr      *io.PipeReader
	pw      *io.PipeWriter
	chunker *fastcdc.Chunker

	se        seekable.Encoder
	numChunks int64

	cancel context.CancelFunc

	once     *sync.Once
	done     chan struct{}
	closeErr *onceError

	totalSize int

	ipfsAPI *ipfshttp.HttpApi
	db      *badger.DB
}

func objectPath(kind cache.EntryKind, hash string, index int64) []byte {
	return []byte(fmt.Sprintf("%s/%s.%d", kind, hash, index))
}

func NewChunkedWriter(ctx context.Context, logger *zap.Logger, db *badger.DB, ipfsAPI *ipfshttp.HttpApi, enc seekable.ZSTDEncoder, kind cache.EntryKind, hash string, size int64) (*chunkedWriter, error) {
	wCtx, cancel := context.WithCancel(ctx)

	pr, pw := io.Pipe()
	chunker, err := fastcdc.NewChunker(pr, fastcdc.Options{AverageSize: 32 * 1024, MaxSize: 127 * 1024})
	if err != nil {
		cancel()
		_ = pw.CloseWithError(err)
		return nil, err
	}

	w := chunkedWriter{
		ctx: wCtx,

		kind: kind,
		hash: hash,
		size: size,

		chunker: chunker,
		pw:      pw,
		pr:      pr,
		done:    make(chan struct{}),
		once:    &sync.Once{},

		cancel: cancel,

		closeErr: &onceError{},

		ipfsAPI: ipfsAPI,
		db:      db,
	}

	w.se, err = seekable.NewEncoder(enc, seekable.WithWLogger(logger))
	if err != nil {
		cancel()
		_ = pw.CloseWithError(err)
		return nil, err
	}

	go func() {
		err = w.syncChunks()
		if err != nil {
			cancel()
		}
	}()
	go func() {
		<-w.ctx.Done()
		_ = pw.CloseWithError(w.ctx.Err())
	}()

	return &w, nil
}

type readEnv struct {
	ctx context.Context

	kind cache.EntryKind
	hash string

	ipfsAPI *ipfshttp.HttpApi
	db      *badger.DB
}

func (r *readEnv) ReadFooter() ([]byte, error) {
	return r.GetFrameByIndex(seekable.FrameOffsetEntry{ID: -1})
}

func (r *readEnv) ReadSkipFrame(int64) ([]byte, error) {
	return r.ReadFooter()
}

func (r *readEnv) GetFrameByIndex(index seekable.FrameOffsetEntry) (p []byte, err error) {
	objPath := objectPath(r.kind, r.hash, index.ID)
	err = r.db.View(
		func(txn *badger.Txn) error {
			item, err := txn.Get(objPath)
			if err != nil {
				return err
			}

			valueCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if index.ID == -1 {
				p = valueCopy
				return nil
			}

			blockReader, err := r.ipfsAPI.Block().Get(r.ctx, ifacePath.New(string(valueCopy)))
			if err != nil {
				return err
			}
			p, err = io.ReadAll(blockReader)
			return err
		})
	if err != nil {
		return nil, fmt.Errorf("failed to get: %s: %+v", objPath, err)
	}
	return
}

func (w *chunkedWriter) syncChunks() (err error) {
	defer close(w.done)
	for {
		var chunk fastcdc.Chunk

		chunk, err = w.chunker.Next()
		if err == nil {
			var buf []byte

			buf, err = w.se.Encode(chunk.Data)
			if err != nil {
				return err
			}

			var stat iface.BlockStat
			stat, err = w.ipfsAPI.Block().Put(w.ctx, bytes.NewReader(buf),
				func(opts *ifaceOpts.BlockPutSettings) error {
					opts.Codec = "raw"
					return nil
				})
			if err != nil {
				return err
			}

			e := badger.NewEntry([]byte(objectPath(w.kind, w.hash, w.numChunks)), []byte(stat.Path().String())).WithTTL(7 * 24 * time.Hour)
			err = w.db.Update(func(txn *badger.Txn) error {
				return txn.SetEntry(e)
			})
			if err != nil {
				return
			}
			w.numChunks++
		} else {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return
		}
	}
}

func (w *chunkedWriter) Close() (err error) {
	w.once.Do(func() {
		err = multierr.Append(err, w.pw.Close())
		<-w.done
		err = multierr.Append(err, w.pr.Close())

		err = multierr.Append(err, w.db.Update(func(txn *badger.Txn) error {
			// TODO: move ttl to params
			buf, err := w.se.EndStream()
			if err != nil {
				return err
			}

			e := badger.NewEntry(objectPath(w.kind, w.hash, -1), buf).WithTTL(7 * 24 * time.Hour)
			return txn.SetEntry(e)
		}))

		w.cancel()
		w.closeErr.Store(err)
	})
	return w.closeErr.Load()
}

func (w *chunkedWriter) Write(p []byte) (n int, err error) {
	n, err = w.pw.Write(p)
	if err == nil {
		w.totalSize += n
	}
	return
}

type BadgerCache struct {
	cache.Cache

	logger *zap.Logger

	enc *zstd.Encoder
	dec *zstd.Decoder

	ipfsAPI *ipfshttp.HttpApi
	db      *badger.DB
}

func New(ctx context.Context, path string) (*BadgerCache, error) {
	opts := badger.DefaultOptions(path)
	opts.ValueThreshold = 4096
	opts.Compression = options.ZSTD
	opts.ZSTDCompressionLevel = 3
	opts.BlockCacheSize = 4 << 30

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}

	ipfsAPI, err := ipfshttp.NewLocalApi()
	if err != nil {
		return nil, err
	}

	return &BadgerCache{
		db:      db,
		ipfsAPI: ipfsAPI,

		logger: ctxzap.Extract(ctx),

		enc: enc,
		dec: dec,
	}, nil
}

func (c *BadgerCache) Contains(ctx context.Context, kind cache.EntryKind, hash string) (bool, int64, error) {
	env := &readEnv{
		ctx: ctx,

		kind: kind,
		hash: hash,

		ipfsAPI: c.ipfsAPI,
		db:      c.db,
	}

	r, err := seekable.NewReader(nil, c.dec, seekable.WithRLogger(c.logger), seekable.WithREnvironment(env))
	if err != nil {
		return false, 0, err
	}

	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return false, 0, err
	}

	return true, size, nil
}

func (c *BadgerCache) Get(ctx context.Context, kind cache.EntryKind, hash string, offset, length int64) (io.ReadCloser, int64, error) {
	env := &readEnv{
		ctx: ctx,

		kind: kind,
		hash: hash,

		ipfsAPI: c.ipfsAPI,
		db:      c.db,
	}

	r, err := seekable.NewReader(nil, c.dec, seekable.WithRLogger(c.logger), seekable.WithREnvironment(env))
	if err != nil {
		return nil, 0, err
	}

	if offset > 0 || length > 0 {
		return io.NopCloser(io.NewSectionReader(r, offset, length)), length, nil
	} else {
		size, err := r.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, 0, err
		}

		_, err = r.Seek(0, io.SeekStart)
		if err != nil {
			return nil, 0, err
		}
		return io.NopCloser(r), size, nil
	}
}

func (c *BadgerCache) Put(ctx context.Context, kind cache.EntryKind, hash string, size, offset int64) (io.WriteCloser, error) {
	if offset > 0 {
		return nil, fmt.Errorf("offsets are not supported yet")
	}

	w, err := NewChunkedWriter(ctx, c.logger, c.db, c.ipfsAPI, c.enc, kind, hash, size)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (c *BadgerCache) Close() error {
	return c.db.Close()
}
