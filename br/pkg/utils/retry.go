// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
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
	"internalerror",
	"not read from or written to within the timeout period",
	"<code>requesttimeout</code>",
	"<code>invalidpart</code>",
	"end of file before message length reached",
}

type ErrorStrategy int

const (
	// This type can be retry but consume the backoffer attempts.
	Retry ErrorStrategy = iota
	// This type can be retry immediately.
	LosslessRetry
	// This type means un-recover error and the whole progress should exits
	// for example:
	// 1. permission not valid.
	// 2. data has not found.
	// 3. retry too many times
	GiveUp
	// Do nothing
	Ignore
	// This type means we need do some resolve work before retry.
	// current the work is only resolve locks.
	Resolve
)

type ErrorContext struct {
	// encounter times for one context on a store
	// we may use this value to determine the retry policy
	encounterTimesOnStore int
	// whether need retry
	needRetry bool
	// whether canbe ignore this error
	canbeIgnore bool
	// whether in backup or restore
	scenario string

	errorMsg string

	errorType errors.Error
}

func (ec *ErrorContext) HandleBackup(err backuppb.Error, storeID int) ErrorStrategy {
	if len(err.Msg) != 0 {
		return ec.handleErrorMsg(err.Msg, storeID)
	}
	return ec.handleErrorPb(err, storeID)
}

func (ec *ErrorContext) handleErrorMsg(msg string, storeID int) ErrorStrategy {
	// UNSAFE! TODO: use meaningful error code instead of unstructured message to find failed to write error.
	logger := log.L().With(zap.String("scenario", ec.scenario))
	if utils.MessageIsNotFoundStorageError(msg) {
		// giveup outside
		ec.errorMsg = fmt.Sprintf("File or directory not found on TiKV Node (store id: %v; Address: %s). "+
			"work around:please ensure br and tikv nodes share a same storage and the user of br and tikv has same uid.",
			storeID)
		return GiveUp
	}
	if utils.MessageIsPermissionDeniedStorageError(msg) {
		// giveup outside
		ec.errorMsg = fmt.Sprintf("I/O permission denied error occurs on TiKV Node(store id: %v; Address: %s). "+
			"work around:please ensure tikv has permission to read from & write to the storage.",
			storeID)
		return GiveUp
	}

	if utils.MessageIsRetryableStorageError(msg) {
		logger.Warn("occur storage error", zap.String("error", msg))
		// infinite retry outside
		// finite retry outside
		ec.errorType = *berrors.ErrStorageUnknown
		return Retry
	} else {
		// retry enough on same store
		// if not enough
		// return Retry
		// if enough
		// return GiveUp
	}
	return GiveUp
}

func (ec *ErrorContext) handleErrorPb(e backuppb.Error, storeID int) ErrorStrategy {
	logger := log.L().With(zap.String("scenario", ec.scenario))
	switch v := e.Detail.(type) {
	case *backuppb.Error_KvError:
		if ec.canbeIgnore {
			return Ignore
		}
		if lockErr := v.KvError.Locked; lockErr != nil {
			logger.Warn("occur kv error", zap.Reflect("error", v))
			// resolve outside     X
			// ignore outside
			ec.errorType = *berrors.ErrBackupKeyIsLocked
			return Resolve
		}
		// Backup should not meet error other than KeyLocked.
		// giveup outside         X
		// ignore outside
		ec.errorType = *berrors.ErrKVUnknown
		return GiveUp

	case *backuppb.Error_RegionError:
		if ec.canbeIgnore {
			return Ignore
		}
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
			// giveup outside
			// ignore outside
			ec.errorType = *berrors.ErrKVUnknown
			return GiveUp
		}
		logger.Warn("occur region error",
			zap.Reflect("RegionError", regionErr),
			zap.Int("storeID", storeID))
		// ignore outside
		// finite retry outside
		ec.errorType = *berrors.ErrBackupRegion
		return Retry

	case *backuppb.Error_ClusterIdError:
		logger.Error("occur cluster ID error", zap.Reflect("error", v), zap.Int("storeID", storeID))
		// giveup outside
		ec.errorType = *berrors.ErrKVUnknown
		return GiveUp
	}
	return GiveUp
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
	r.nextBackoff = max(r.nextBackoff, ms)
	r.mu.Unlock()
}

// Inner returns the reference to the inner `backoffer`.
func (r *RetryWithBackoffer) Inner() *tikv.Backoffer {
	return r.bo
}
