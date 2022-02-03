package ipfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"sync"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	chunker "github.com/ipfs/go-ipfs-chunker"
	ipld "github.com/ipfs/go-ipld-format"
	mfs "github.com/ipfs/go-mfs"
	ufsHelpers "github.com/ipfs/go-unixfs/importer/helpers"
	ufsDAG "github.com/ipfs/go-unixfs/importer/trickle"
	ufsIO "github.com/ipfs/go-unixfs/io"
	icore "github.com/ipfs/interface-go-ipfs-core"
	"github.com/klauspost/compress/zstd"
	fastcdc "github.com/reusee/fastcdc-go"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/znly/bazel-cache/cache"
	"github.com/znly/bazel-cache/cache/ipfs/api"
)

const Scheme = "ipfs"

func init() {
	cache.RegisterCache(Scheme, func(ctx context.Context, uri *url.URL) (cache.Cache, error) {
		return New(ctx, uri.Path)
	})
}

var _ chunker.Splitter = (*chunkedWriter)(nil)

// TODO: use generics
type result struct {
	r   ipld.Node
	err error
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

	pr *io.PipeReader
	pw *io.PipeWriter

	spl          *fastcdc.Chunker
	doneChunking bool

	se        seekable.Encoder
	numChunks int64

	cancel context.CancelFunc

	once     *sync.Once
	node     chan result
	closeErr *onceError

	totalSize int

	ipfsAPI icore.CoreAPI
	logger  *zap.Logger
	root    *mfs.Root
}

func objectPath(kind cache.EntryKind, hash string) string {
	return fmt.Sprintf("%s/%s", kind, hash)
}

func NewChunkedWriter(ctx context.Context, logger *zap.Logger, root *mfs.Root, ipfsAPI icore.CoreAPI, enc seekable.ZSTDEncoder, kind cache.EntryKind, hash string, size int64) (*chunkedWriter, error) {
	var err error
	wCtx, cancel := context.WithCancel(ctx)

	// TODO: limit pipe memory consumption.
	pr, pw := io.Pipe()

	w := chunkedWriter{
		ctx: wCtx,

		kind: kind,
		hash: hash,
		size: size,

		pw:   pw,
		pr:   pr,
		node: make(chan result, 1),
		once: &sync.Once{},

		cancel: cancel,

		closeErr: &onceError{},

		ipfsAPI: ipfsAPI,
		logger:  logger,
		root:    root,
	}

	w.spl, err = fastcdc.NewChunker(pr, fastcdc.Options{AverageSize: 256 * 1024, MaxSize: 1 * 1024 * 1024})
	if err != nil {
		cancel()
		_ = pw.CloseWithError(err)
		return nil, err
	}

	w.se, err = seekable.NewEncoder(enc, seekable.WithWLogger(logger))
	if err != nil {
		cancel()
		_ = pw.CloseWithError(err)
		return nil, err
	}

	go func() {
		node, err := w.dagBuilder()
		w.node <- result{r: node, err: err}
		close(w.node)
	}()
	go func() {
		<-w.ctx.Done()
		_ = pw.CloseWithError(w.ctx.Err())
	}()

	return &w, nil
}

func (w *chunkedWriter) Reader() io.Reader {
	return w.pr
}

func (w *chunkedWriter) NextBytes() ([]byte, error) {
	chunk, err := w.spl.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			if w.doneChunking {
				return nil, io.EOF
			}
			w.doneChunking = true

			buf, err := w.se.EndStream()
			if err != nil {
				return nil, err
			}
			return buf, io.EOF
		}
		return nil, err
	}
	return w.se.Encode(chunk.Data)
}

func (w *chunkedWriter) dagBuilder() (ipld.Node, error) {
	params := &ufsHelpers.DagBuilderParams{
		Maxlinks: math.MaxInt, Dagserv: w.ipfsAPI.Dag(), RawLeaves: true,
	}
	builder, err := params.New(w)
	if err != nil {
		return nil, err
	}

	node, err := ufsDAG.Layout(builder)
	if err != nil {
		return nil, err
	}
	return node, err
}

func (w *chunkedWriter) Close() (err error) {
	w.once.Do(func() {
		err = multierr.Append(err, w.pw.Close())
		res := <-w.node
		err = multierr.Append(err, res.err)
		if res.err == nil {
			w.logger.Info("finished", zap.Stringer("cid", res.r.Cid()), zap.String("hash", w.hash))
			path := objectPath(w.kind, w.hash)
			pErr := mfs.PutNode(w.root, path, res.r)
			err = multierr.Append(err, pErr)
			_, pErr = mfs.FlushPath(w.ctx, w.root, path)
			err = multierr.Append(err, pErr)
		}
		err = multierr.Append(err, w.pr.Close())
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

type IPFSCache struct {
	cache.Cache

	logger *zap.Logger

	enc *zstd.Encoder
	dec *zstd.Decoder

	ipfsAPI icore.CoreAPI
	root    *mfs.Root
}

func New(ctx context.Context, repoRoot string) (*IPFSCache, error) {
	logger := ctxzap.Extract(ctx)

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}

	ipfsAPI, repo, err := api.New(ctx, repoRoot)
	if err != nil {
		return nil, err
	}

	root, err := NewRoot(ctx, repo, ipfsAPI.Dag())
	if err != nil {
		return nil, err
	}

	for _, d := range []cache.EntryKind{cache.AC, cache.CAS} {
		mfs.Mkdir(root, objectPath(d, ""), mfs.MkdirOpts{Flush: true})
	}

	return &IPFSCache{
		ipfsAPI: ipfsAPI,
		root:    root,

		logger: logger,

		enc: enc,
		dec: dec,
	}, nil
}

func (c *IPFSCache) Contains(ctx context.Context, kind cache.EntryKind, hash string) (bool, int64, error) {
	fsn, err := mfs.Lookup(c.root, objectPath(kind, hash))
	if err != nil {
		return false, 0, err
	}

	node, err := fsn.GetNode()
	if err != nil {
		return false, 0, err
	}

	dagReader, err := ufsIO.NewDagReader(ctx, node, c.ipfsAPI.Dag())
	if err != nil {
		return false, 0, err
	}

	r, err := seekable.NewReader(dagReader, c.dec, seekable.WithRLogger(c.logger))
	if err != nil {
		return false, 0, err
	}

	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return false, 0, err
	}

	return true, size, nil
}

func (c *IPFSCache) Get(ctx context.Context, kind cache.EntryKind, hash string, offset, length int64) (io.ReadCloser, int64, error) {
	fsn, err := mfs.Lookup(c.root, objectPath(kind, hash))
	if err != nil {
		return nil, 0, err
	}

	node, err := fsn.GetNode()
	if err != nil {
		return nil, 0, err
	}

	dagReader, err := ufsIO.NewDagReader(ctx, node, c.ipfsAPI.Dag())
	if err != nil {
		return nil, 0, err
	}

	r, err := seekable.NewReader(dagReader, c.dec, seekable.WithRLogger(c.logger))
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

func (c *IPFSCache) Put(ctx context.Context, kind cache.EntryKind, hash string, size, offset int64) (io.WriteCloser, error) {
	if offset > 0 {
		return nil, fmt.Errorf("offsets are not supported yet")
	}

	w, err := NewChunkedWriter(ctx, c.logger, c.root, c.ipfsAPI, c.enc, kind, hash, size)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (c *IPFSCache) Close() (err error) {
	multierr.Append(err, c.root.Close())
	return err
}
