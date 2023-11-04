package util

import (
	"context"
	"sync"
	"time"
)

const uvinf = 0x7FF0000000000000

func RetryOverTime(
	ctx context.Context,
	fn func(ctx context.Context) error,
	errh func(ctx context.Context, err error) bool,
	start time.Duration,
	step time.Duration,
	stop time.Duration,
	max int,
) error {
	var timer *Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	if max <= 0 {
		max = uvinf
	}

	var err error
	curr := start
	for count := 0; count <= max; count++ {
		if err = fn(ctx); err == nil {
			return nil
		}

		if errh != nil && !errh(ctx, err) {
			return err
		}

		if timer != nil {
			timer.Reset(curr)
		}

		if timer == nil {
			timer = NewTimer(start)
		}

		select {
		case <-timer.C:
			select {
			case <-ctx.Done():
				return err
			default:
			}
		case <-ctx.Done():
			return err
		}

		if curr += step; curr > stop {
			curr = stop
		}
	}

	return err
}

func NewStopRequest(ch <-chan struct{}, wg *sync.WaitGroup) StopRequest {
	wg.Add(1)
	return StopRequest{
		Ch: ch,
		Wg: wg,
	}
}

type StopRequest struct {
	Ch <-chan struct{}
	Wg *sync.WaitGroup
}

func NewStopRequestCtx(ctx context.Context, wg *sync.WaitGroup) StopRequestCtx {
	wg.Add(1)
	return StopRequestCtx{
		Ctx: ctx,
		Wg:  wg,
	}
}

type StopRequestCtx struct {
	Ctx context.Context
	Wg  *sync.WaitGroup
}
