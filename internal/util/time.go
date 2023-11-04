package util

import (
	"context"
	"time"
)

const MaxDuration time.Duration = 1<<63 - 1

func NewTimer(d time.Duration) *Timer {
	return &Timer{
		Timer: time.NewTimer(d),
	}
}

type Timer struct {
	*time.Timer
}

func (t *Timer) Stop() bool {
	var wasActive bool
	if wasActive = t.Timer.Stop(); !wasActive {
		select {
		case <-t.C:
		default:
		}
	}

	return wasActive
}

func (t *Timer) Reset(delay time.Duration) bool {
	t.Stop()
	return t.Timer.Reset(delay)
}

func NewContextTimer(ctx context.Context, d time.Duration) *ContextTimer {
	childCtx, cancel := context.WithTimeout(ctx, d)
	return &ContextTimer{
		Context:   childCtx,
		parentCtx: ctx,
		cancel:    cancel,
	}
}

type ContextTimer struct {
	context.Context
	parentCtx context.Context
	cancel    context.CancelFunc
}

func (t *ContextTimer) C() <-chan struct{} {
	return t.Context.Done()
}

func (t *ContextTimer) IsCancelled() bool {
	return t.Context.Err() == context.Canceled
}

func (t *ContextTimer) IsDue() bool {
	return t.Context.Err() == context.DeadlineExceeded
}

func (t *ContextTimer) Reset(d time.Duration) {
	t.cancel()
	t.Context, t.cancel = context.WithTimeout(t.parentCtx, d)
}

func (t *ContextTimer) Stop() bool {
	if t.Context.Err() != nil {
		return false
	}

	t.cancel()
	return true
}
