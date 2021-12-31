package badger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"

	"github.com/znly/bazel-cache/cache"
)

const Scheme = "badger"

func init() {
	cache.RegisterCache(Scheme, func(ctx context.Context, uri *url.URL) (cache.Cache, error) {
		return New(ctx, uri.Path)
	})
}

type WriteCloser struct {
	done chan struct{}
	once *sync.Once

	io.Writer
	io.Closer
}

func (w WriteCloser) Close() error {
	w.once.Do(func() {
		close(w.done)
	})
	return nil
}

type BadgerCache struct {
	cache.Cache

	db *badger.DB
}

func New(ctx context.Context, path string) (*BadgerCache, error) {
	opts := badger.DefaultOptions(path)
	opts.ValueThreshold = 1024
	opts.SyncWrites = false
	opts.TableLoadingMode = options.MemoryMap
	opts.ValueLogLoadingMode = options.MemoryMap

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	return &BadgerCache{
		db: db,
	}, nil
}

func (c *BadgerCache) objectPath(kind cache.EntryKind, hash string) []byte {
	return []byte(filepath.Join(string(kind), hash))
}

func (c *BadgerCache) Contains(ctx context.Context, kind cache.EntryKind, hash string) (bool, int64, error) {
	var size int64

	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(c.objectPath(kind, hash))
		if err != nil {
			return err
		}

		size = item.ValueSize()
		return nil
	})
	if err != nil {
		return false, 0, err
	}

	return true, size, nil
}

func (c *BadgerCache) Get(ctx context.Context, kind cache.EntryKind, hash string, offset, length int64) (io.ReadCloser, int64, error) {
	var size int64
	var value []byte

	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(c.objectPath(kind, hash))
		if err != nil {
			return err
		}

		size = item.ValueSize()
		err = item.Value(func(val []byte) error {
			if offset > 0 {
				val = val[offset:]
			}
			if length > 0 {
				val = val[:length]
			}
			value = append(value, val...)
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return io.NopCloser(bytes.NewReader(value)), size, nil
}

func (c *BadgerCache) Put(ctx context.Context, kind cache.EntryKind, hash string, size, offset int64) (io.WriteCloser, error) {
	if offset > 0 {
		return nil, fmt.Errorf("offsets are not supported yet")
	}

	buf := new(bytes.Buffer)
	done := make(chan struct{})
	go func() {
		<-done
		_ = c.db.Update(func(txn *badger.Txn) error {
			e := badger.NewEntry([]byte(c.objectPath(kind, hash)), buf.Bytes())
			err := txn.SetEntry(e)
			return err
		})
	}()

	return WriteCloser{
		once:   &sync.Once{},
		done:   done,
		Writer: buf,
	}, nil
}

func (c *BadgerCache) Close() error {
	return c.db.Close()
}
