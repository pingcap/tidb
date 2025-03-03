// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"database/sql"
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

	recoveryMaxAttempts  = 16
	recoveryDelayTime    = 30 * time.Second
	recoveryMaxDelayTime = 4 * time.Minute

	rawClientMaxAttempts  = 5
	rawClientDelayTime    = 500 * time.Millisecond
	rawClientMaxDelayTime = 5 * time.Second
)

// BackoffStrategy implements a backoff strategy for retry operations.
type BackoffStrategy interface {
	// NextBackoff returns a duration to wait before retrying again
	NextBackoff(err error) time.Duration
	// RemainingAttempts returns the remaining number of attempts
	RemainingAttempts() int
}

// ConstantBackoff is a backoff strategy that retry forever until success.
type ConstantBackoff time.Duration

// NextBackoff returns a duration to wait before retrying again
func (c ConstantBackoff) NextBackoff(err error) time.Duration {
	return time.Duration(c)
}

// RemainingAttempts returns the remain attempt times
func (c ConstantBackoff) RemainingAttempts() int {
	// A large enough value. Also still safe for arithmetic operations (won't easily overflow).
	return math.MaxInt16
}

// RetryState is the mutable state needed for retrying.
// It likes the `utils.BackoffStrategy`, but more fundamental:
// this only control the backoff time and knows nothing about what error happens.
// NOTE: Maybe also implement the backoffer via this.
// TODO: merge with BackoffStrategy
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
	failpoint.Inject("set-remaining-attempts-to-one", func(_ failpoint.Value) {
		rs.retryTimes = rs.maxRetry
	})
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
func (rs *RetryState) RemainingAttempts() int {
	return rs.maxRetry - rs.retryTimes
}

// NextBackoff implements the `BackoffStrategy`.
func (rs *RetryState) NextBackoff(error) time.Duration {
	return rs.ExponentialBackoff()
}

type backoffStrategyImpl struct {
	remainingAttempts int
	delayTime         time.Duration
	maxDelayTime      time.Duration
	errContext        *ErrorContext
	isRetryErr        func(error) bool
	isNonRetryErr     func(error) bool
}

// BackoffOption defines a function type for configuring backoffStrategyImpl
type BackoffOption func(*backoffStrategyImpl)

// WithRemainingAttempts sets the remaining attempts
func WithRemainingAttempts(attempts int) BackoffOption {
	return func(b *backoffStrategyImpl) {
		b.remainingAttempts = attempts
	}
}

// WithDelayTime sets the initial delay time
func WithDelayTime(delay time.Duration) BackoffOption {
	return func(b *backoffStrategyImpl) {
		b.delayTime = delay
	}
}

// WithMaxDelayTime sets the maximum delay time
func WithMaxDelayTime(maxDelay time.Duration) BackoffOption {
	return func(b *backoffStrategyImpl) {
		b.maxDelayTime = maxDelay
	}
}

// WithErrorContext sets the error context
func WithErrorContext(errContext *ErrorContext) BackoffOption {
	return func(b *backoffStrategyImpl) {
		b.errContext = errContext
	}
}

// WithRetryErrorFunc sets the retry error checking function
func WithRetryErrorFunc(isRetryErr func(error) bool) BackoffOption {
	return func(b *backoffStrategyImpl) {
		b.isRetryErr = isRetryErr
	}
}

// WithNonRetryErrorFunc sets the non-retry error checking function
func WithNonRetryErrorFunc(isNonRetryErr func(error) bool) BackoffOption {
	return func(b *backoffStrategyImpl) {
		b.isNonRetryErr = isNonRetryErr
	}
}

// NewBackoffStrategy creates a new backoff strategy with custom retry logic
func NewBackoffStrategy(opts ...BackoffOption) BackoffStrategy {
	// Default values
	bs := &backoffStrategyImpl{
		remainingAttempts: 1,
		delayTime:         time.Second,
		maxDelayTime:      10 * time.Second,
		errContext:        NewZeroRetryContext("default"),
		isRetryErr:        alwaysTrueFunc(),
		isNonRetryErr:     alwaysFalseFunc(),
	}

	for _, opt := range opts {
		opt(bs)
	}

	return bs
}

func NewBackoffRetryAllErrorStrategy(remainingAttempts int, delayTime, maxDelayTime time.Duration) BackoffStrategy {
	errContext := NewZeroRetryContext("retry all errors")
	return NewBackoffStrategy(
		WithRemainingAttempts(remainingAttempts),
		WithDelayTime(delayTime),
		WithMaxDelayTime(maxDelayTime),
		WithErrorContext(errContext),
		WithRetryErrorFunc(alwaysTrueFunc()),
		WithNonRetryErrorFunc(alwaysFalseFunc()),
	)
}

func NewBackoffRetryAllExceptStrategy(remainingAttempts int, delayTime, maxDelayTime time.Duration, isNonRetryFunc func(error) bool) BackoffStrategy {
	errContext := NewZeroRetryContext("retry all except")
	return NewBackoffStrategy(
		WithRemainingAttempts(remainingAttempts),
		WithDelayTime(delayTime),
		WithMaxDelayTime(maxDelayTime),
		WithErrorContext(errContext),
		WithRetryErrorFunc(alwaysTrueFunc()),
		WithNonRetryErrorFunc(isNonRetryFunc),
	)
}

func NewTiKVStoreBackoffStrategy(maxRetry int, delayTime, maxDelayTime time.Duration,
	errContext *ErrorContext) BackoffStrategy {
	retryErrs := map[error]struct{}{
		berrors.ErrKVEpochNotMatch:  {},
		berrors.ErrKVDownloadFailed: {},
		berrors.ErrKVIngestFailed:   {},
		berrors.ErrPDLeaderNotFound: {},
	}
	grpcRetryCodes := map[codes.Code]struct{}{
		codes.Canceled:          {},
		codes.Unavailable:       {},
		codes.Aborted:           {},
		codes.DeadlineExceeded:  {},
		codes.ResourceExhausted: {},
		codes.Internal:          {},
	}
	nonRetryErrs := map[error]struct{}{
		context.Canceled:                 {},
		berrors.ErrKVRangeIsEmpty:        {},
		berrors.ErrKVRewriteRuleNotFound: {},
	}

	isRetryErrFunc := buildIsRetryErrFunc(retryErrs, grpcRetryCodes)
	isNonRetryErrFunc := buildIsNonRetryErrFunc(nonRetryErrs)

	return NewBackoffStrategy(
		WithRemainingAttempts(maxRetry),
		WithDelayTime(delayTime),
		WithMaxDelayTime(maxDelayTime),
		WithErrorContext(errContext),
		WithRetryErrorFunc(isRetryErrFunc),
		WithNonRetryErrorFunc(isNonRetryErrFunc),
	)
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
	grpcRetryCodes := map[codes.Code]struct{}{
		codes.Canceled:          {},
		codes.DeadlineExceeded:  {},
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
	nonRetryErrs := map[error]struct{}{
		context.Canceled:         {},
		context.DeadlineExceeded: {},
		sql.ErrNoRows:            {},
	}

	isRetryErrFunc := buildIsRetryErrFunc(retryErrs, grpcRetryCodes)
	isNonRetryErrFunc := buildIsNonRetryErrFunc(nonRetryErrs)

	return NewBackoffStrategy(
		WithRemainingAttempts(maxRetry),
		WithDelayTime(delayTime),
		WithMaxDelayTime(maxDelayTime),
		WithErrorContext(NewZeroRetryContext("connect PD")),
		WithRetryErrorFunc(isRetryErrFunc),
		WithNonRetryErrorFunc(isNonRetryErrFunc),
	)
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
	grpcRetryCodes := map[codes.Code]struct{}{}

	isRetryErrFunc := buildIsRetryErrFunc(retryErrs, grpcRetryCodes)

	return NewBackoffStrategy(
		WithRemainingAttempts(resetTSRetryTime),
		WithDelayTime(resetTSWaitInterval),
		WithErrorContext(NewZeroRetryContext("disk check")),
		WithRetryErrorFunc(isRetryErrFunc),
		WithNonRetryErrorFunc(alwaysFalseFunc()),
	)
}

func NewRecoveryBackoffStrategy(isRetryErrFunc func(error) bool) BackoffStrategy {
	return NewBackoffStrategy(
		WithRemainingAttempts(recoveryMaxAttempts),
		WithDelayTime(recoveryDelayTime),
		WithErrorContext(NewZeroRetryContext("recovery")),
		WithRetryErrorFunc(isRetryErrFunc),
		WithNonRetryErrorFunc(alwaysFalseFunc()),
	)
}

func NewFlashBackBackoffStrategy() BackoffStrategy {
	return NewBackoffStrategy(
		WithRemainingAttempts(FlashbackRetryTime),
		WithDelayTime(FlashbackWaitInterval),
		WithErrorContext(NewZeroRetryContext("flashback")),
		WithRetryErrorFunc(alwaysTrueFunc()),
		WithNonRetryErrorFunc(alwaysFalseFunc()),
	)
}

func NewChecksumBackoffStrategy() BackoffStrategy {
	return NewBackoffStrategy(
		WithRemainingAttempts(ChecksumRetryTime),
		WithDelayTime(ChecksumWaitInterval),
		WithErrorContext(NewZeroRetryContext("checksum")),
		WithRetryErrorFunc(alwaysTrueFunc()),
		WithNonRetryErrorFunc(alwaysFalseFunc()),
	)
}

func NewRawClientBackoffStrategy() BackoffStrategy {
	return NewBackoffStrategy(
		WithRemainingAttempts(rawClientMaxAttempts),
		WithDelayTime(rawClientDelayTime),
		WithMaxDelayTime(rawClientMaxDelayTime),
		WithErrorContext(NewZeroRetryContext("raw client")),
		WithRetryErrorFunc(alwaysTrueFunc()),
		WithNonRetryErrorFunc(alwaysFalseFunc()),
	)
}

func (bo *backoffStrategyImpl) NextBackoff(err error) time.Duration {
	errs := multierr.Errors(err)
	lastErr := errs[len(errs)-1]
	// we don't care storeID here.
	// TODO: should put all the retry logic in one place, right now errContext has internal retry counter as well
	res := HandleUnknownBackupError(lastErr.Error(), 0, bo.errContext)
	if res.Strategy == StrategyRetry {
		bo.doBackoff()
	} else if res.Reason == contextCancelledMsg {
		// have to hack here due to complex context.cancel/grpc cancel
		bo.stopBackoff()
	} else {
		e := errors.Cause(lastErr)
		if bo.isNonRetryErr(e) {
			bo.stopBackoff()
		} else if bo.isRetryErr(e) {
			bo.doBackoff()
		} else {
			log.Warn("stop retrying on error", zap.Error(err))
			bo.stopBackoff()
		}
	}

	failpoint.Inject("set-remaining-attempts-to-one", func(_ failpoint.Value) {
		if bo.remainingAttempts > 1 {
			bo.remainingAttempts = 1
		}
	})

	if bo.delayTime > bo.maxDelayTime {
		return bo.maxDelayTime
	}
	return bo.delayTime
}

func (bo *backoffStrategyImpl) RemainingAttempts() int {
	return bo.remainingAttempts
}

func (bo *backoffStrategyImpl) doBackoff() {
	bo.delayTime = 2 * bo.delayTime
	bo.remainingAttempts--
}

func (bo *backoffStrategyImpl) stopBackoff() {
	bo.delayTime = 0
	bo.remainingAttempts = 0
}

func buildIsRetryErrFunc(retryErrs map[error]struct{}, grpcRetryCodes map[codes.Code]struct{}) func(error) bool {
	return func(err error) bool {
		_, brRetryOk := retryErrs[err]
		_, grpcRetryOk := grpcRetryCodes[status.Code(err)]
		return brRetryOk || grpcRetryOk
	}
}

func buildIsNonRetryErrFunc(nonRetryErrs map[error]struct{}) func(error) bool {
	return func(err error) bool {
		_, brNonRetryOk := nonRetryErrs[err]
		return brNonRetryOk
	}
}

func alwaysTrueFunc() func(error) bool {
	return func(err error) bool {
		return true
	}
}

func alwaysFalseFunc() func(error) bool {
	return func(err error) bool {
		return false
	}
}
