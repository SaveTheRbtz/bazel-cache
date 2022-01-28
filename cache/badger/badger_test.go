package badger_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/znly/bazel-cache/cache"
	"github.com/znly/bazel-cache/cache/badger"
)

func TestEndToEnd(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tmpDir := os.Getenv("TEST_TMPDIR")
	c, err := badger.New(ctx, filepath.Join(tmpDir, "b1"))
	assert.NoError(t, err)
	defer c.Close()

	w, err := c.Put(ctx, cache.CAS, "h1", 100, 0)
	assert.NoError(t, err)

	n, err := w.Write([]byte("t1"))
	assert.Equal(t, 2, n)
	assert.NoError(t, err)
	n, err = w.Write([]byte("t2"))
	assert.Equal(t, 2, n)
	assert.NoError(t, err)

	err = w.Close()
	assert.NoError(t, err)

	r, m, err := c.Get(ctx, cache.CAS, "h1", 0, 4)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), m)
	buf, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, []byte("t1t2"), buf)

	r, m, err = c.Get(ctx, cache.CAS, "h1", 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), m)
	buf, err = io.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, []byte("1t"), buf)

	err = r.Close()
	assert.NoError(t, err)
}

func TestParallel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tmpDir := os.Getenv("TEST_TMPDIR")
	c, err := badger.New(ctx, filepath.Join(tmpDir, "b2"))
	assert.NoError(t, err)

	for i := 0; i <= 100; i++ {
		i := i
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Parallel()

			key := fmt.Sprintf("key-%d", i)
			w, err := c.Put(ctx, cache.AC, key, 100, 0)
			assert.NoError(t, err)

			source := []byte{}

			total := 0
			upTo := rand.Intn(10)
			for j := 0; j <= upTo; j++ {
				value := []byte(fmt.Sprintf("value-%d\n", j))
				total += len(value)
				n, err := w.Write(value)
				assert.Equal(t, len(value), n)
				assert.NoError(t, err)
				source = append(source, value...)
			}

			err = w.Close()
			assert.NoError(t, err)

			b, size, err := c.Contains(ctx, cache.AC, key)
			assert.NoError(t, err)
			assert.True(t, b)
			assert.Equal(t, int64(len(source)), size)

			r, size, err := c.Get(ctx, cache.AC, key, 0, int64(len(source)))
			assert.NoError(t, err)
			assert.Equal(t, int64(len(source)), size)

			buf1, err := io.ReadAll(r)
			assert.NoError(t, err)
			assert.Equal(t, len(source), len(buf1))
			assert.Equal(t, source, buf1)

			err = r.Close()
			assert.NoError(t, err)
		})
	}
}

func TestBig(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tmpDir := os.Getenv("TEST_TMPDIR")
	c, err := badger.New(ctx, filepath.Join(tmpDir, "b3"))
	assert.NoError(t, err)

	key := "key"
	w, err := c.Put(ctx, cache.AC, key, 100, 0)
	assert.NoError(t, err)

	source := []byte{}

	total := 0
	for j := 0; j <= 10000; j++ {
		value := []byte(fmt.Sprintf("value-%d\n", j))
		total += len(value)
		n, err := w.Write(value)
		assert.Equal(t, len(value), n)
		assert.NoError(t, err)
		source = append(source, value...)
	}

	err = w.Close()
	assert.NoError(t, err)

	b, size, err := c.Contains(ctx, cache.AC, key)
	assert.NoError(t, err)
	assert.True(t, b)
	assert.Equal(t, int64(len(source)), size)

	r0, size, err := c.Get(ctx, cache.AC, key, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(source)), size)
	tt := make([]byte, 10)
	ten, err := io.ReadFull(r0, tt)
	assert.NoError(t, err)
	assert.Equal(t, 10, ten)

	r, size, err := c.Get(ctx, cache.AC, key, 0, int64(len(source)))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(source)), size)

	buf1, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, len(source), len(buf1))
	assert.Equal(t, source, buf1)

	err = r.Close()
	assert.NoError(t, err)
}
