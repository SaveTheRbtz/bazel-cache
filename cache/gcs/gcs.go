package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/znly/bazel-cache/utils"

	"cloud.google.com/go/storage"

	"github.com/znly/bazel-cache/cache"
)

const Scheme = "gcs"

func init() {
	cache.RegisterCache(Scheme, func(ctx context.Context, uri *url.URL) (cache.Cache, error) {
		ttlInDays := utils.URLValuesGetInt(uri.Query(), "ttl_in_days")
		return New(ctx, uri.Host, uri.Path, ttlInDays)
	})
}

// See https://cloud.google.com/storage/docs/request-rate
const (
	maxConcurrentReads  = 200
	maxConcurrentWrites = 100
)

var (
	ErrIncompleteWrite       = errors.New("incomplete write")
	ErrMissingDigestMetadata = errors.New("missing digest metadata")
)

type GCSCache struct {
	client     *storage.Client
	bucket     *storage.BucketHandle
	pathPrefix string
}

func New(ctx context.Context, bucket, pathPrefix string, ttlInDays int) (cache.Cache, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	gcsCache := &GCSCache{
		client:     client,
		bucket:     client.Bucket(bucket),
		pathPrefix: pathPrefix,
	}

	if ttlInDays > 0 {
		if _, err := gcsCache.bucket.Update(ctx, storage.BucketAttrsToUpdate{
			Lifecycle: &storage.Lifecycle{
				Rules: []storage.LifecycleRule{
					{
						Action: storage.LifecycleAction{
							Type: storage.DeleteAction,
						},
						Condition: storage.LifecycleCondition{
							DaysSinceCustomTime: int64(ttlInDays),
						},
					},
				},
			},
		}); err != nil {
			return nil, fmt.Errorf("unable to apply TTL lifecycle condition: %w", err)
		}
	}

	return cache.NewGatedCache(gcsCache, maxConcurrentReads, maxConcurrentWrites)
}

func (g *GCSCache) object(kind cache.EntryKind, hash string) *storage.ObjectHandle {
	path := string(kind) + "/" + hash
	if g.pathPrefix != "" {
		path = g.pathPrefix + "/" + path
	}
	return g.bucket.Object(path)
}

func (g *GCSCache) touch(ctx context.Context, object *storage.ObjectHandle) (*storage.ObjectAttrs, error) {
	return object.Update(ctx, storage.ObjectAttrsToUpdate{
		CustomTime: time.Now(),
	})
}

// Before being downloaded, each object's existence is checked from the ActionResult object. Take that opportunity to
// touch the object and thus update its CustomTime attribute to time.Now. This will allow for the object lifecycle
// management system to kick in and garbage collect old objects.
func (g *GCSCache) Contains(ctx context.Context, kind cache.EntryKind, hash string) (bool, int64, error) {
	obj := g.object(kind, hash)

	attrs, err := g.touch(ctx, obj)
	if err != nil {
		return false, 0, err
	}

	return true, attrs.Size, nil
}

func (g *GCSCache) Get(ctx context.Context, kind cache.EntryKind, hash string, offset, length int64) (io.ReadCloser, int64, error) {
	obj := g.object(kind, hash)

	var (
		rdr *storage.Reader
		err error
	)

	if offset > 0 || length > 0 {
		rdr, err = obj.NewRangeReader(ctx, offset, length)
	} else {
		rdr, err = obj.NewReader(ctx)
	}
	if err != nil {
		return nil, 0, err
	}

	return rdr, rdr.Attrs.Size, nil
}

func (g *GCSCache) Put(ctx context.Context, kind cache.EntryKind, hash string, size, offset int64) (io.WriteCloser, error) {
	if offset != 0 {
		return nil, fmt.Errorf("writing to an offset is not supported by GCS")
	}

	wtr := g.object(kind, hash).NewWriter(ctx)
	wtr.ContentType = "application/octet-stream"
	wtr.CustomTime = time.Now()

	return wtr, nil
}
