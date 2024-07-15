// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"io"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// importSSTRetryTimes specifies the retry time. Its longest time is about 90s-100s.
	importSSTRetryTimes      = 16
	importSSTWaitInterval    = 40 * time.Millisecond
	importSSTMaxWaitInterval = 10 * time.Second

	downloadSSTRetryTimes      = 8
	downloadSSTWaitInterval    = 1 * time.Second
	downloadSSTMaxWaitInterval = 4 * time.Second

	backupSSTRetryTimes      = 5
	backupSSTWaitInterval    = 2 * time.Second
	backupSSTMaxWaitInterval = 3 * time.Second

	resetTSRetryTime       = 32
	resetTSWaitInterval    = 50 * time.Millisecond
	resetTSMaxWaitInterval = 2 * time.Second

	resetTSRetryTimeExt       = 600
	resetTSWaitIntervalExt    = 500 * time.Millisecond
	resetTSMaxWaitIntervalExt = 300 * time.Second

	// region heartbeat are 10 seconds by default, if some region has 2 heartbeat missing (15 seconds), it appear to be a network issue between PD and TiKV.
	FlashbackRetryTime       = 3
	FlashbackWaitInterval    = 3 * time.Second
	FlashbackMaxWaitInterval = 15 * time.Second

	ChecksumRetryTime       = 8
	ChecksumWaitInterval    = 1 * time.Second
	ChecksumMaxWaitInterval = 30 * time.Second
)

// ConstantBackoff is a backoff strategy that retry forever until success.
type ConstantBackoff time.Duration

// NextBackoff returns a duration to wait before retrying again
func (c ConstantBackoff) NextBackoff(err error) time.Duration {
	return time.Duration(c)
}

// Attempt returns the remain attempt times
func (c ConstantBackoff) Attempt() int {
	// A large enough value. Also still safe for arithmetic operations (won't easily overflow).
	return math.MaxInt16
}

// RetryState is the mutable state needed for retrying.
// It likes the `utils.BackoffStrategy`, but more fundamental:
// this only control the backoff time and knows nothing about what error happens.
// NOTE: Maybe also implement the backoffer via this.
type RetryState struct {
	maxRetry   int
	retryTimes int

	maxBackoff  time.Duration
	nextBackoff time.Duration
}

// InitialRetryState make the initial state for retrying.
func InitialRetryState(maxRetryTimes int, initialBackoff, maxBackoff time.Duration) RetryState {
	return RetryState{
		maxRetry:    maxRetryTimes,
		maxBackoff:  maxBackoff,
		nextBackoff: initialBackoff,
	}
}

// Whether in the current state we can retry.
func (rs *RetryState) ShouldRetry() bool {
	return rs.retryTimes < rs.maxRetry
}

// Get the exponential backoff durion and transform the state.
func (rs *RetryState) ExponentialBackoff() time.Duration {
	rs.retryTimes++
	backoff := rs.nextBackoff
	rs.nextBackoff *= 2
	if rs.nextBackoff > rs.maxBackoff {
		rs.nextBackoff = rs.maxBackoff
	}
	return backoff
}

func (rs *RetryState) GiveUp() {
	rs.retryTimes = rs.maxRetry
}

// ReduceRetry reduces retry times for 1.
func (rs *RetryState) ReduceRetry() {
	rs.retryTimes--
}

// Attempt implements the `BackoffStrategy`.
// TODO: Maybe use this to replace the `exponentialBackoffer` (which is nearly homomorphic to this)?
func (rs *RetryState) Attempt() int {
	return rs.maxRetry - rs.retryTimes
}

// NextBackoff implements the `BackoffStrategy`.
func (rs *RetryState) NextBackoff(error) time.Duration {
	return rs.ExponentialBackoff()
}

type backoffStrategyImpl struct {
	attempt        int
	delayTime      time.Duration
	maxDelayTime   time.Duration
	errContext     *ErrorContext
	retryErrs      map[error]struct{}
	nonRetryErrs   map[error]struct{}
	grpcRetryCodes map[codes.Code]struct{}
}

// NewBackoffStrategy creates a new controller regulating a truncated exponential backoff.
func NewBackoffStrategy(attempt int, delayTime, maxDelayTime time.Duration, errContext *ErrorContext,
	retryErrs map[error]struct{}, nonRetryErrs map[error]struct{}, grpcRetryCodes map[codes.Code]struct{}) BackoffStrategy {
	return &backoffStrategyImpl{
		attempt:        attempt,
		delayTime:      delayTime,
		maxDelayTime:   maxDelayTime,
		errContext:     errContext,
		retryErrs:      retryErrs,
		nonRetryErrs:   nonRetryErrs,
		grpcRetryCodes: grpcRetryCodes,
	}
}

func NewTiKVStoreBackoffStrategy(maxRetry int, delayTime, maxDelayTime time.Duration,
	errContext *ErrorContext) BackoffStrategy {
	retryErrs := map[error]struct{}{
		berrors.ErrKVEpochNotMatch:  {},
		berrors.ErrKVDownloadFailed: {},
		berrors.ErrKVIngestFailed:   {},
		berrors.ErrPDLeaderNotFound: {},
	}
	nonRetryErrs := map[error]struct{}{
		context.Canceled: {},
	}
	grpcRetryCodes := map[codes.Code]struct{}{
		codes.Unavailable:       {},
		codes.Aborted:           {},
		codes.DeadlineExceeded:  {},
		codes.ResourceExhausted: {},
		codes.Internal:          {},
		codes.Canceled:          {},
	}

	return NewBackoffStrategy(maxRetry, delayTime, maxDelayTime, errContext, retryErrs, nonRetryErrs, grpcRetryCodes)
}

func NewImportSSTBackoffStrategy() BackoffStrategy {
	errContext := NewErrorContext("import sst", 3)
	return NewTiKVStoreBackoffStrategy(importSSTRetryTimes, importSSTWaitInterval, importSSTMaxWaitInterval, errContext)
}

func NewDownloadSSTBackoffStrategy() BackoffStrategy {
	errContext := NewErrorContext("download sst", 3)
	return NewTiKVStoreBackoffStrategy(downloadSSTRetryTimes, downloadSSTWaitInterval, downloadSSTMaxWaitInterval,
		errContext)
}

func NewBackupSSTBackoffStrategy() BackoffStrategy {
	errContext := NewErrorContext("backup sst", 3)
	return NewTiKVStoreBackoffStrategy(backupSSTRetryTimes, backupSSTWaitInterval, backupSSTMaxWaitInterval, errContext)
}

func NewPDBackoffStrategy(maxRetry int, delayTime, maxDelayTime time.Duration) BackoffStrategy {
	retryErrs := map[error]struct{}{
		berrors.ErrRestoreTotalKVMismatch: {},
		io.EOF:                            {},
	}
	nonRetryErrs := map[error]struct{}{
		context.Canceled: {},
	}
	grpcRetryCodes := map[codes.Code]struct{}{
		codes.DeadlineExceeded:  {},
		codes.Canceled:          {},
		codes.NotFound:          {},
		codes.AlreadyExists:     {},
		codes.PermissionDenied:  {},
		codes.ResourceExhausted: {},
		codes.Aborted:           {},
		codes.OutOfRange:        {},
		codes.Unavailable:       {},
		codes.DataLoss:          {},
		codes.Unknown:           {},
	}

	return NewBackoffStrategy(maxRetry, delayTime, maxDelayTime, NewNoRetryContext(),
		retryErrs, nonRetryErrs, grpcRetryCodes)
}

func NewAggressivePDBackoffStrategy() BackoffStrategy {
	return NewPDBackoffStrategy(resetTSRetryTime, resetTSWaitInterval, resetTSMaxWaitInterval)
}

func NewConservativePDBackoffStrategy() BackoffStrategy {
	return NewPDBackoffStrategy(resetTSRetryTimeExt, resetTSWaitIntervalExt, resetTSMaxWaitIntervalExt)
}

func NewDiskCheckBackoffStrategy() BackoffStrategy {
	retryErrs := map[error]struct{}{
		berrors.ErrPDInvalidResponse: {},
		berrors.ErrKVDiskFull:        {},
	}
	nonRetryErrs := map[error]struct{}{
		context.Canceled:         {},
		context.DeadlineExceeded: {},
	}
	grpcRetryCodes := map[codes.Code]struct{}{}
	return NewBackoffStrategy(resetTSRetryTime, resetTSWaitInterval, resetTSMaxWaitInterval, NewNoRetryContext(),
		retryErrs, nonRetryErrs, grpcRetryCodes)
}

func (bo *backoffStrategyImpl) NextBackoff(err error) time.Duration {
	// we don't care storeID here.
	errs := multierr.Errors(err)
	lastErr := errs[len(errs)-1]
	// TODO: should put all the error handling logic in one place.
	res := bo.errContext.HandleErrorMsg(lastErr.Error(), 0)
	if res.Strategy == RetryStrategy {
		bo.doBackoff()
	} else {
		e := errors.Cause(lastErr)
		grpcErrCode := status.Code(e)
		if _, ok := bo.retryErrs[e]; ok {
			bo.doBackoff()
		} else if _, ok := bo.nonRetryErrs[e]; ok {
			bo.stopBackoff()
		} else if _, ok := bo.grpcRetryCodes[grpcErrCode]; ok {
			bo.doBackoff()
		} else {
			log.Warn("stop retrying on error", zap.Error(err))
			bo.stopBackoff()
		}
	}

	failpoint.Inject("set-attempt-to-one", func(_ failpoint.Value) {
		bo.attempt = 1
	})

	if bo.delayTime > bo.maxDelayTime {
		return bo.maxDelayTime
	}
	return bo.delayTime
}

func (bo *backoffStrategyImpl) Attempt() int {
	return bo.attempt
}

func (bo *backoffStrategyImpl) doBackoff() {
	bo.delayTime = 2 * bo.delayTime
	bo.attempt--
}

func (bo *backoffStrategyImpl) stopBackoff() {
	bo.delayTime = 0
	bo.attempt = 0
}
