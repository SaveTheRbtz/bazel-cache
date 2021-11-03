package hedged

import (
	"context"
	"time"
)

type result struct {
	v   interface{}
	err error
}

func Do(ctx context.Context, timeout time.Duration, f func(ctx context.Context) (interface{}, error)) (interface{}, error) {
	hedgeCtx, hedgeCtxCancel := context.WithTimeout(ctx, timeout)
	defer hedgeCtxCancel()

	resCh := make(chan result, 1)
	go func() {
		var res result
		res.v, res.err = f(hedgeCtx)
		resCh <- res
	}()

	select {
	case res := <-resCh:
		if res.err != hedgeCtx.Err() {
			return res.v, res.err
		}
	case <-hedgeCtx.Done():
	}

	// If the first request did timeout, issue a second one, this time with no
	// hedging.
	return f(ctx)
}
