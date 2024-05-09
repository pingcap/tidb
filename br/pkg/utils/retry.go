// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	stderrs "errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	tmysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/multierr"
	"go.uber.org/zap"
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
	"timeout after",
	"internalerror",
	"not read from or written to within the timeout period",
	"<code>requesttimeout</code>",
	"<code>invalidpart</code>",
	"end of file before message length reached",
}

type ErrorResult struct {
	Strategy ErrorStrategy
	Reason   string
}

type ErrorStrategy int

const (
	// This type can be retry but consume the backoffer attempts.
	RetryStrategy ErrorStrategy = iota
	// This type means unrecoverable error and the whole progress should exits
	// for example:
	// 1. permission not valid.
	// 2. data has not found.
	// 3. retry too many times
	GiveUpStrategy
	// This type represents Unknown error
	UnknownStrategy
)

type ErrorContext struct {
	mu sync.Mutex
	// encounter times for one context on a store
	// we may use this value to determine the retry policy
	encounterTimes map[uint64]int
	// unknown error retry limitation.
	// encouter many times error makes Retry to GiveUp.
	encounterTimesLimitation int
	// whether in backup or restore
	scenario string
}

func NewErrorContext(scenario string, limitation int) *ErrorContext {
	return &ErrorContext{
		scenario:                 scenario,
		encounterTimes:           make(map[uint64]int),
		encounterTimesLimitation: limitation,
	}
}

func NewDefaultContext() *ErrorContext {
	return &ErrorContext{
		scenario:                 "default",
		encounterTimes:           make(map[uint64]int),
		encounterTimesLimitation: 1,
	}
}

func (ec *ErrorContext) HandleError(err *backuppb.Error, uuid uint64) ErrorResult {
	if err == nil {
		return ErrorResult{RetryStrategy, "unreachable retry"}
	}
	res := ec.handleErrorPb(err, uuid)
	// try the best effort to save progress from error here
	if res.Strategy == UnknownStrategy && len(err.Msg) != 0 {
		return ec.HandleErrorMsg(err.Msg, uuid)
	}
	return res
}

func (ec *ErrorContext) HandleIgnorableError(err *backuppb.Error, uuid uint64) ErrorResult {
	if err == nil {
		return ErrorResult{RetryStrategy, "unreachable retry"}
	}
	res := ec.handleIgnorableErrorPb(err, uuid)
	// try the best effort to save progress from error here
	if res.Strategy == UnknownStrategy && len(err.Msg) != 0 {
		return ec.HandleErrorMsg(err.Msg, uuid)
	}
	return res
}

func (ec *ErrorContext) HandleErrorMsg(msg string, uuid uint64) ErrorResult {
	// UNSAFE! TODO: use meaningful error code instead of unstructured message to find failed to write error.
	logger := log.L().With(zap.String("scenario", ec.scenario))
	if messageIsNotFoundStorageError(msg) {
		reason := fmt.Sprintf("File or directory not found on TiKV Node (store id: %v). "+
			"work around:please ensure br and tikv nodes share a same storage and the user of br and tikv has same uid.",
			uuid)
		return ErrorResult{GiveUpStrategy, reason}
	}
	if messageIsPermissionDeniedStorageError(msg) {
		reason := fmt.Sprintf("I/O permission denied error occurs on TiKV Node(store id: %v). "+
			"work around:please ensure tikv has permission to read from & write to the storage.",
			uuid)
		return ErrorResult{GiveUpStrategy, reason}
	}
	msgLower := strings.ToLower(msg)
	if strings.Contains(msgLower, "context canceled") {
		return ErrorResult{GiveUpStrategy, "context canceled, give up"}
	}

	if MessageIsRetryableStorageError(msg) {
		logger.Warn("occur storage error", zap.String("error", msg))
		return ErrorResult{RetryStrategy, "retrable error"}
	}
	// retry enough on same store
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.encounterTimes[uuid]++
	if ec.encounterTimes[uuid] <= ec.encounterTimesLimitation {
		return ErrorResult{RetryStrategy, "unknown error, retry it for few times"}
	}
	return ErrorResult{GiveUpStrategy, "unknown error and retry too many times, give up"}
}

func (ec *ErrorContext) handleIgnorableErrorPb(e *backuppb.Error, uuid uint64) ErrorResult {
	switch e.Detail.(type) {
	case *backuppb.Error_KvError:
		return ErrorResult{RetryStrategy, "retry outside because the error can be ignored"}
	case *backuppb.Error_RegionError:
		return ErrorResult{RetryStrategy, "retry outside because the error can be ignored"}
	case *backuppb.Error_ClusterIdError:
		return ErrorResult{GiveUpStrategy, "cluster ID mismatch"}
	}
	return ErrorResult{UnknownStrategy, "unreachable code"}
}

func (ec *ErrorContext) handleErrorPb(e *backuppb.Error, uuid uint64) ErrorResult {
	logger := log.L().With(zap.String("scenario", ec.scenario))
	switch v := e.Detail.(type) {
	case *backuppb.Error_KvError:
		// should not meet error other than KeyLocked.
		return ErrorResult{GiveUpStrategy, "unknown kv error"}

	case *backuppb.Error_RegionError:
		regionErr := v.RegionError
		// Ignore following errors.
		if !(regionErr.EpochNotMatch != nil ||
			regionErr.NotLeader != nil ||
			regionErr.RegionNotFound != nil ||
			regionErr.ServerIsBusy != nil ||
			regionErr.StaleCommand != nil ||
			regionErr.StoreNotMatch != nil ||
			regionErr.ReadIndexNotReady != nil ||
			regionErr.ProposalInMergingMode != nil) {
			logger.Error("unexpect region error", zap.Reflect("RegionError", regionErr))
			return ErrorResult{GiveUpStrategy, "unknown kv error"}
		}
		logger.Warn("occur region error",
			zap.Reflect("RegionError", regionErr),
			zap.Uint64("uuid", uuid))
		return ErrorResult{RetryStrategy, "retrable error"}

	case *backuppb.Error_ClusterIdError:
		logger.Error("occur cluster ID error", zap.Reflect("error", v), zap.Uint64("uuid", uuid))
		return ErrorResult{GiveUpStrategy, "cluster ID mismatch"}
	}
	return ErrorResult{UnknownStrategy, "unreachable code"}
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
			// allErrors must not be `nil` here, so ignore the context error.
			return *new(T), allErrors
		case <-time.After(backoffer.NextBackoff(err)):
		}
	}
	return *new(T), allErrors // nolint:wrapcheck
}

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
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(backoffer.NextBackoff(lastErr)):
		}
	}

	return lastErr
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
