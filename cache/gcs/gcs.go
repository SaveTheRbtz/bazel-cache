package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"

	"github.com/znly/bazel-cache/cache"
	"github.com/znly/bazel-cache/utils"
)

const Scheme = "gcs"

func init() {
	cache.RegisterCache(Scheme, func(ctx context.Context, uri *url.URL) (cache.Cache, error) {
		ttlInDays := utils.URLValuesGetInt(uri.Query(), "ttl_days", 0)
		maxReads := utils.URLValuesGetInt(uri.Query(), "max_reads", defaultMaxConcurrentReads)
		maxWrites := utils.URLValuesGetInt(uri.Query(), "max_writes", defaultMaxConcurrentWrites)
		return New(ctx, uri.Host, uri.Path, maxReads, maxWrites, ttlInDays)
	})
}

// Loosely based on https://cloud.google.com/storage/docs/request-rate
const (
	defaultMaxConcurrentReads  = 2000
	defaultMaxConcurrentWrites = 200
)

type GCSCache struct {
	client     *storage.Client
	bucket     *storage.BucketHandle
	pathPrefix string
}

func tomorrow() time.Time {
	return time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
}

func New(ctx context.Context, bucket, pathPrefix string, maxConcurrentReads, maxConcurrentWrites, ttlInDays int) (cache.Cache, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	gcsCache := &GCSCache{
		client:     client,
		bucket:     client.Bucket(bucket),
		pathPrefix: pathPrefix,
	}

	lifecycleRules := []storage.LifecycleRule{}
	if ttlInDays > 0 {
		lifecycleRules = []storage.LifecycleRule{
			{
				Action: storage.LifecycleAction{
					Type: storage.DeleteAction,
				},
				Condition: storage.LifecycleCondition{
					DaysSinceCustomTime: int64(ttlInDays),
				},
			},
		}
	}
	if _, err := gcsCache.bucket.Update(ctx, storage.BucketAttrsToUpdate{
		Lifecycle: &storage.Lifecycle{
			Rules: lifecycleRules,
		},
	}); err != nil {
		return nil, fmt.Errorf("unable to apply TTL lifecycle condition: %w", err)
	}

	return cache.NewGatedCache(gcsCache, maxConcurrentReads, maxConcurrentWrites)
}

func (g *GCSCache) object(kind cache.EntryKind, hash string) *storage.ObjectHandle {
	objPath := path.Join(string(kind), hash[:2], hash[2:4], hash[4:6], hash)
	if g.pathPrefix != "" {
		objPath = path.Join(g.pathPrefix, objPath)
	}
	return g.bucket.Object(objPath)
}

func (g *GCSCache) touch(ctx context.Context, object *storage.ObjectHandle, t time.Time) error {
	_, err := object.Update(ctx, storage.ObjectAttrsToUpdate{
		CustomTime: t,
	})
	var gerr *googleapi.Error
	if err != nil && errors.As(err, &gerr) {
		for _, e := range gerr.Errors {
			switch e.Reason {
			case "conflict":
				// Are we updating the object from too many calls in parallel?
				// If so, no big deal since somebody else is bumping the metadata,
				// so we can juse issue a Metadata fetch.
				return nil
			case "invalid":
				// Are we updating the object back in time? If so, it's fine too
				// since it means the object exists and that its CustomTime is in
				// the future.
				return nil
			}
		}
	}
	return err
}

// Before being downloaded, each object's existence is checked from the ActionResult object. Take that opportunity to
// touch the object and thus update its CustomTime attribute to time.Now. This will allow for the object lifecycle
// management system to kick in and garbage collect old objects.
func (g *GCSCache) Contains(ctx context.Context, kind cache.EntryKind, hash string) (bool, int64, error) {
	obj := g.object(kind, hash)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return false, 0, err
	}

	// We used to get the attributes by touch (bumping CustomTime), but it seems that that stresses GCS too much.
	// Instead since GCS only supports day-level policies, round to the next day, and bump the CustomTime
	// only if the existing one if different than what it should be. Concurrent updates are fine and should now
	// represent the vast minority of updates. However, it means that setting a TTL to 1 day could result in weird
	// behaviour.
	if tmrw := tomorrow(); attrs.CustomTime.Before(tmrw) {
		// if for some reason we can't update the time, since of object exists
		// consider it's fine  because, in either way, worst case:
		// 1. we return the error and bazel will re-executes the action
		// 2. we don't return the error and the object will maybe be garbage
		//    collected before it should, in which case it'll come back to 1.
		//
		// Let's be optimistic and chose 2 then.
		g.touch(ctx, obj, tmrw)
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
	wtr.CustomTime = tomorrow()

	return wtr, nil
}
