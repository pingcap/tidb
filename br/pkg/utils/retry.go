// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/multierr"
)

var retryableServerError = []string{
	"server closed",
	"connection refused",
	"connection reset by peer",
	"channel closed",
	"error trying to connect",
	"connection closed before message completed",
	"body write aborted",
	"error during dispatch",
	"put object timeout",
	"internalerror",
	"not read from or written to within the timeout period",
	"<code>requesttimeout</code>",
}

// RetryableFunc presents a retryable operation.
type RetryableFunc func() error

type RetryableFuncV2[T any] func(context.Context) (T, error)

// Backoffer implements a backoff policy for retrying operations.
type Backoffer interface {
	// NextBackoff returns a duration to wait before retrying again
	NextBackoff(err error) time.Duration
	// Attempt returns the remain attempt times
	Attempt() int
}

// WithRetry retries a given operation with a backoff policy.
//
// Returns nil if `retryableFunc` succeeded at least once. Otherwise, returns a
// multierr containing all errors encountered.
func WithRetry(
	ctx context.Context,
	retryableFunc RetryableFunc,
	backoffer Backoffer,
) error {
	_, err := WithRetryV2[struct{}](ctx, backoffer, func(ctx context.Context) (struct{}, error) {
		innerErr := retryableFunc()
		return struct{}{}, innerErr
	})
	return err
}

// WithRetryV2 retries a given operation with a backoff policy.
//
// Returns the returned value if `retryableFunc` succeeded at least once. Otherwise, returns a
// multierr that containing all errors encountered.
// Comparing with `WithRetry`, this function reordered the argument order and supports catching the return value.
func WithRetryV2[T any](
	ctx context.Context,
	backoffer Backoffer,
	fn RetryableFuncV2[T],
) (T, error) {
	var allErrors error
	for backoffer.Attempt() > 0 {
		res, err := fn(ctx)
		if err == nil {
			return res, nil
		}
		allErrors = multierr.Append(allErrors, err)
		select {
		case <-ctx.Done():
			return *new(T), allErrors
		case <-time.After(backoffer.NextBackoff(err)):
		}
	}
	return *new(T), allErrors // nolint:wrapcheck
}

// MessageIsRetryableStorageError checks whether the message returning from TiKV is retryable ExternalStorageError.
func MessageIsRetryableStorageError(msg string) bool {
	msgLower := strings.ToLower(msg)
	// UNSAFE! TODO: Add a error type for retryable connection error.
	for _, errStr := range retryableServerError {
		if strings.Contains(msgLower, errStr) {
			return true
		}
	}
	return false
}

func FallBack2CreateTable(err error) bool {
	switch nerr := errors.Cause(err).(type) {
	case *terror.Error:
		return nerr.Code() == tmysql.ErrInvalidDDLJob
	}
	return false
}

// RetryWithBackoffer is a simple context for a "mixed" retry.
// Some of TiDB APIs, say, `ResolveLock` requires a `tikv.Backoffer` as argument.
// But the `tikv.Backoffer` isn't pretty customizable, it has some sorts of predefined configuration but
// we cannot create new one. So we are going to mix up the flavour of `tikv.Backoffer` and our homemade
// back off strategy. That is what the `RetryWithBackoffer` did.
type RetryWithBackoffer struct {
	bo *tikv.Backoffer

	totalBackoff int
	maxBackoff   int
	baseErr      error

	mu          sync.Mutex
	nextBackoff int
}

// AdaptTiKVBackoffer creates an "ad-hoc" backoffer, which wraps a backoffer and provides some new functions:
// When backing off, we can manually provide it a specified sleep duration instead of directly provide a retry.Config
// Which is sealed in the "client-go/internal".
func AdaptTiKVBackoffer(ctx context.Context, maxSleepMs int, baseErr error) *RetryWithBackoffer {
	return &RetryWithBackoffer{
		bo:         tikv.NewBackoffer(ctx, maxSleepMs),
		maxBackoff: maxSleepMs,
		baseErr:    baseErr,
	}
}

// NextSleepInMS returns the time `BackOff` will sleep in ms of the state.
func (r *RetryWithBackoffer) NextSleepInMS() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.nextBackoff
}

// TotalSleepInMS returns the total sleeped time in ms.
func (r *RetryWithBackoffer) TotalSleepInMS() int {
	return r.totalBackoff + r.bo.GetTotalSleep()
}

// MaxSleepInMS returns the max sleep time for the retry context in ms.
func (r *RetryWithBackoffer) MaxSleepInMS() int {
	return r.maxBackoff
}

// BackOff executes the back off: sleep for a precalculated backoff time.
// See `RequestBackOff` for more details.
func (r *RetryWithBackoffer) BackOff() error {
	r.mu.Lock()
	nextBo := r.nextBackoff
	r.nextBackoff = 0
	r.mu.Unlock()

	if r.TotalSleepInMS() > r.maxBackoff {
		return errors.Annotatef(r.baseErr, "backoff exceeds the max backoff time %s", time.Duration(r.maxBackoff)*time.Millisecond)
	}
	time.Sleep(time.Duration(nextBo) * time.Millisecond)
	r.totalBackoff += nextBo
	return nil
}

// RequestBackOff register the intent of backing off at least n milliseconds.
// That intent will be fulfilled when calling `BackOff`.
func (r *RetryWithBackoffer) RequestBackOff(ms int) {
	r.mu.Lock()
	r.nextBackoff = mathutil.Max(r.nextBackoff, ms)
	r.mu.Unlock()
}

// Inner returns the reference to the inner `backoffer`.
func (r *RetryWithBackoffer) Inner() *tikv.Backoffer {
	return r.bo
}
