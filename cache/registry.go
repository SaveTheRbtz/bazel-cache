package cache

import (
	"context"
	"errors"
	"fmt"
	"net/url"
)

var (
	ErrInvalidScheme = errors.New("invalid scheme")
)

type NewCacheFunc func(ctx context.Context, uri *url.URL) (Cache, error)

var (
	caches = map[string]NewCacheFunc{}
)

func RegisterCache(scheme string, f NewCacheFunc) {
	caches[scheme] = f
}

func NewCacheFromURI(ctx context.Context, uri string) (Cache, error) {
	u, err := url.ParseRequestURI(uri)
	if err != nil {
		return nil, err
	}

	f, ok := caches[u.Scheme]
	if ok == false {
		return nil, fmt.Errorf("scheme %s is invalid: %w", u.Scheme, ErrInvalidScheme)
	}

	return f(ctx, u)
}
