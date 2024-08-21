// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	stderrs "errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tmysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

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
			// allErrors must not be `nil` here, so ignore the context error.
			return *new(T), allErrors
		case <-time.After(backoffer.NextBackoff(err)):
		}
	}
	return *new(T), allErrors // nolint:wrapcheck
}

var sampleLoggerFactory = logutil.SampleLoggerFactory(
	time.Minute, 3, zap.String(logutil.LogFieldCategory, "utils"),
)

// WithRetryReturnLastErr is like WithRetry but the returned error is the last
// error during retry rather than a multierr.
func WithRetryReturnLastErr(
	ctx context.Context,
	retryableFunc RetryableFunc,
	backoffer Backoffer,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var lastErr error
	for backoffer.Attempt() > 0 {
		lastErr = retryableFunc()
		if lastErr == nil {
			return nil
		}
		backoff := backoffer.NextBackoff(lastErr)
		sampleLoggerFactory().Info(
			"retryable operation failed",
			zap.Error(lastErr), zap.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(backoff):
		}
	}

	return lastErr
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
	r.nextBackoff = max(r.nextBackoff, ms)
	r.mu.Unlock()
}

// Inner returns the reference to the inner `backoffer`.
func (r *RetryWithBackoffer) Inner() *tikv.Backoffer {
	return r.bo
}

type verboseBackoffer struct {
	inner   Backoffer
	logger  *zap.Logger
	groupID uuid.UUID
}

func (v *verboseBackoffer) NextBackoff(err error) time.Duration {
	nextBackoff := v.inner.NextBackoff(err)
	v.logger.Warn("Encountered err, retrying.",
		zap.Stringer("nextBackoff", nextBackoff),
		zap.String("err", err.Error()),
		zap.Stringer("gid", v.groupID))
	return nextBackoff
}

// Attempt returns the remain attempt times
func (v *verboseBackoffer) Attempt() int {
	attempt := v.inner.Attempt()
	if attempt > 0 {
		v.logger.Debug("Retry attempt hint.", zap.Int("attempt", attempt), zap.Stringer("gid", v.groupID))
	} else {
		v.logger.Warn("Retry limit exceeded.", zap.Stringer("gid", v.groupID))
	}
	return attempt
}

func VerboseRetry(bo Backoffer, logger *zap.Logger) Backoffer {
	if logger == nil {
		logger = log.L()
	}
	vlog := &verboseBackoffer{
		inner:   bo,
		logger:  logger,
		groupID: uuid.New(),
	}
	return vlog
}

type failedOnErr struct {
	inner    Backoffer
	failed   bool
	failedOn []error
}

// NextBackoff returns a duration to wait before retrying again
func (f *failedOnErr) NextBackoff(err error) time.Duration {
	for _, fatalErr := range f.failedOn {
		if stderrs.Is(errors.Cause(err), fatalErr) {
			f.failed = true
			return 0
		}
	}
	if !f.failed {
		return f.inner.NextBackoff(err)
	}
	return 0
}

// Attempt returns the remain attempt times
func (f *failedOnErr) Attempt() int {
	if f.failed {
		return 0
	}
	return f.inner.Attempt()
}

func GiveUpRetryOn(bo Backoffer, errs ...error) Backoffer {
	return &failedOnErr{
		inner:    bo,
		failed:   false,
		failedOn: errs,
	}
}
