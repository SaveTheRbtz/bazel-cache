package badger

import (
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
	fastcdc "github.com/reusee/fastcdc-go"
	"go.uber.org/multierr"
	"go.uber.org/zap"

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

	db *badger.DB
}

func objectPath(kind cache.EntryKind, hash string, index int64) []byte {
	return []byte(fmt.Sprintf("%s/%s.%d", kind, hash, index))
}

func NewChunkedWriter(ctx context.Context, logger *zap.Logger, db *badger.DB, enc seekable.ZSTDEncoder, kind cache.EntryKind, hash string, size int64) (*chunkedWriter, error) {
	wCtx, cancel := context.WithCancel(ctx)

	pr, pw := io.Pipe()
	chunker, err := fastcdc.NewChunker(pr, fastcdc.Options{AverageSize: 4 * 1024, MaxSize: 16 * 1024})
	if err != nil {
		cancel()
		pw.CloseWithError(err)
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

		db: db,
	}

	w.se, err = seekable.NewEncoder(enc, seekable.WithWLogger(logger))
	if err != nil {
		cancel()
		pw.CloseWithError(err)
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

type chunkedReader struct {
	ctx context.Context

	kind cache.EntryKind
	hash string

	dec    seekable.ZSTDDecoder
	sd     seekable.Decoder
	offset int64

	cancel context.CancelFunc

	db     *badger.DB
	logger *zap.Logger
}

func NewChunkedReader(ctx context.Context, logger *zap.Logger, db *badger.DB, dec seekable.ZSTDDecoder, kind cache.EntryKind, hash string) (*chunkedReader, error) {
	wCtx, cancel := context.WithCancel(ctx)

	r := chunkedReader{
		ctx: wCtx,

		kind: kind,
		hash: hash,

		dec:    dec,
		logger: logger,

		cancel: cancel,

		db: db,
	}

	footer, err := r.GetFrameByID(-1)
	if err != nil {
		return nil, err
	}

	r.sd, err = seekable.NewDecoder(footer, dec, seekable.WithRLogger(logger))
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (r *chunkedReader) Read(p []byte) (n int, err error) {
	offset, n, err := r.read(p, r.offset)

	if err != nil && !errors.Is(err, io.EOF) {
		return
	}
	r.offset = offset
	return
}

func (r *chunkedReader) read(dst []byte, off int64) (int64, int, error) {
	if off >= r.sd.Size() {
		return 0, 0, io.EOF
	}
	if off < 0 {
		return 0, 0, fmt.Errorf("offset before the start of the file: %d", off)
	}

	index := r.sd.GetIndexByDecompOffset(uint64(off))
	if index == nil {
		return 0, 0, fmt.Errorf("failed to get index by offest: %d", off)
	}
	if off < int64(index.DecompOffset) || off > int64(index.DecompOffset)+int64(index.DecompSize) {
		return 0, 0, fmt.Errorf("offset outside of index bounds: %d: min: %d, max: %d",
			off, int64(index.DecompOffset), int64(index.DecompOffset)+int64(index.DecompSize))
	}

	var decompressed []byte

	src, err := r.GetFrameByID(index.ID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get frame offset at: %d, %w", index.CompOffset, err)
	}

	if len(src) != int(index.CompSize) {
		return 0, 0, fmt.Errorf(
			"index data mismatch: %d, len(src): %d, len(dst): %d, index: %+v\n",
			off, len(src), len(dst), index)
	}

	decompressed, err = r.dec.DecodeAll(src, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decompress data data at: %d, %w", index.CompOffset, err)
	}

	if len(decompressed) != int(index.DecompSize) {
		return 0, 0, fmt.Errorf(
			"index data mismatch: %d, len(decompressed): %d, len(dst): %d, index: %+v\n",
			off, len(decompressed), len(dst), index)
	}

	offsetWithinFrame := uint64(off) - index.DecompOffset

	size := uint64(len(decompressed)) - offsetWithinFrame
	if size > uint64(len(dst)) {
		size = uint64(len(dst))
	}

	r.logger.Debug("decompressed", zap.Uint64("offsetWithinFrame", offsetWithinFrame), zap.Uint64("end", offsetWithinFrame+size),
		zap.Uint64("size", size), zap.Int("lenDecompressed", len(decompressed)), zap.Int("lenDst", len(dst)), zap.Object("index", index))
	copy(dst, decompressed[offsetWithinFrame:offsetWithinFrame+size])

	return off + int64(size), int(size), nil
}

func (r *chunkedReader) GetFrameByID(id int64) (p []byte, err error) {
	objPath := objectPath(r.kind, r.hash, id)
	err = r.db.View(
		func(txn *badger.Txn) error {
			item, err := txn.Get(objPath)
			if err != nil {
				return err
			}
			p, err = item.ValueCopy(nil)
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

			e := badger.NewEntry([]byte(objectPath(w.kind, w.hash, w.numChunks)), buf).WithTTL(7 * 24 * time.Hour)
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

	db *badger.DB
}

func New(ctx context.Context, path string) (*BadgerCache, error) {
	opts := badger.DefaultOptions(path)
	opts.ValueThreshold = 4096
	opts.Compression = options.None

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

	return &BadgerCache{
		db: db,

		logger: ctxzap.Extract(ctx),

		enc: enc,
		dec: dec,
	}, nil
}

func (c *BadgerCache) Contains(ctx context.Context, kind cache.EntryKind, hash string) (bool, int64, error) {
	r, err := NewChunkedReader(ctx, c.logger, c.db, c.dec, kind, hash)
	if err != nil {
		return false, 0, err
	}
	return true, r.sd.Size(), nil
}

func (c *BadgerCache) Get(ctx context.Context, kind cache.EntryKind, hash string, offset, length int64) (io.ReadCloser, int64, error) {
	r, err := NewChunkedReader(ctx, c.logger, c.db, c.dec, kind, hash)
	if err != nil {
		return nil, 0, err
	}

	return io.NopCloser(r), r.sd.Size() - offset, nil
}

func (c *BadgerCache) Put(ctx context.Context, kind cache.EntryKind, hash string, size, offset int64) (io.WriteCloser, error) {
	if offset > 0 {
		return nil, fmt.Errorf("offsets are not supported yet")
	}

	w, err := NewChunkedWriter(ctx, c.logger, c.db, c.enc, kind, hash, size)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (c *BadgerCache) Close() error {
	return c.db.Close()
}
