// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"database/sql"
	"io"
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
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

	gRPC_Cancel = "the client connection is closing"
)

// At least, there are two possible cancel() call,
// one from go context, another from gRPC, here we retry when gRPC cancel with connection closing
func isGRPCCancel(err error) bool {
	if s, ok := status.FromError(err); ok {
		if strings.Contains(s.Message(), gRPC_Cancel) {
			return true
		}
	}
	return false
}

// ConstantBackoff is a backoffer that retry forever until success.
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
// It likes the `utils.Backoffer`, but more fundamental:
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

// Attempt implements the `Backoffer`.
// TODO: Maybe use this to replace the `exponentialBackoffer` (which is nearly homomorphic to this)?
func (rs *RetryState) Attempt() int {
	return rs.maxRetry - rs.retryTimes
}

// NextBackoff implements the `Backoffer`.
func (rs *RetryState) NextBackoff(error) time.Duration {
	return rs.ExponentialBackoff()
}

type importerBackoffer struct {
	attempt      int
	delayTime    time.Duration
	maxDelayTime time.Duration
	errContext   *ErrorContext
}

// NewBackoffer creates a new controller regulating a truncated exponential backoff.
func NewBackoffer(attempt int, delayTime, maxDelayTime time.Duration, errContext *ErrorContext) Backoffer {
	return &importerBackoffer{
		attempt:      attempt,
		delayTime:    delayTime,
		maxDelayTime: maxDelayTime,
		errContext:   errContext,
	}
}

func NewImportSSTBackoffer() Backoffer {
	errContext := NewErrorContext("import sst", 3)
	return NewBackoffer(importSSTRetryTimes, importSSTWaitInterval, importSSTMaxWaitInterval, errContext)
}

func NewDownloadSSTBackoffer() Backoffer {
	errContext := NewErrorContext("download sst", 3)
	return NewBackoffer(downloadSSTRetryTimes, downloadSSTWaitInterval, downloadSSTMaxWaitInterval, errContext)
}

func NewBackupSSTBackoffer() Backoffer {
	errContext := NewErrorContext("backup sst", 3)
	return NewBackoffer(backupSSTRetryTimes, backupSSTWaitInterval, backupSSTMaxWaitInterval, errContext)
}

func (bo *importerBackoffer) NextBackoff(err error) time.Duration {
	// we don't care storeID here.
	res := bo.errContext.HandleErrorMsg(err.Error(), 0)
	if res.Strategy == RetryStrategy {
		bo.delayTime = 2 * bo.delayTime
		bo.attempt--
	} else {
		e := errors.Cause(err)
		switch e { // nolint:errorlint
		case berrors.ErrKVEpochNotMatch, berrors.ErrKVDownloadFailed, berrors.ErrKVIngestFailed, berrors.ErrPDLeaderNotFound:
			bo.delayTime = 2 * bo.delayTime
			bo.attempt--
		case berrors.ErrKVRangeIsEmpty, berrors.ErrKVRewriteRuleNotFound:
			// Expected error, finish the operation
			bo.delayTime = 0
			bo.attempt = 0
		default:
			switch status.Code(e) {
			case codes.Unavailable, codes.Aborted, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Internal:
				bo.delayTime = 2 * bo.delayTime
				bo.attempt--
			case codes.Canceled:
				if isGRPCCancel(err) {
					bo.delayTime = 2 * bo.delayTime
					bo.attempt--
				} else {
					bo.delayTime = 0
					bo.attempt = 0
				}
			default:
				// Unexpected error
				bo.delayTime = 0
				bo.attempt = 0
				log.Warn("unexpected error, stop retrying", zap.Error(err))
			}
		}
	}
	if bo.delayTime > bo.maxDelayTime {
		return bo.maxDelayTime
	}
	return bo.delayTime
}

func (bo *importerBackoffer) Attempt() int {
	return bo.attempt
}

type pdReqBackoffer struct {
	attempt      int
	delayTime    time.Duration
	maxDelayTime time.Duration
}

func NewPDReqBackoffer() Backoffer {
	return &pdReqBackoffer{
		attempt:      resetTSRetryTime,
		delayTime:    resetTSWaitInterval,
		maxDelayTime: resetTSMaxWaitInterval,
	}
}

func NewPDReqBackofferExt() Backoffer {
	return &pdReqBackoffer{
		attempt:      resetTSRetryTimeExt,
		delayTime:    resetTSWaitIntervalExt,
		maxDelayTime: resetTSMaxWaitIntervalExt,
	}
}

func (bo *pdReqBackoffer) NextBackoff(err error) time.Duration {
	// bo.delayTime = 2 * bo.delayTime
	// bo.attempt--
	e := errors.Cause(err)
	switch e { // nolint:errorlint
	case nil, context.Canceled, context.DeadlineExceeded, sql.ErrNoRows:
		// Excepted error, finish the operation
		bo.delayTime = 0
		bo.attempt = 0
	case berrors.ErrRestoreTotalKVMismatch, io.EOF:
		bo.delayTime = 2 * bo.delayTime
		bo.attempt--
	default:
		// If the connection timeout, pd client would cancel the context, and return grpc context cancel error.
		// So make the codes.Canceled retryable too.
		// It's OK to retry the grpc context cancel error, because the parent context cancel returns context.Canceled.
		// For example, cancel the `ectx` and then pdClient.GetTS(ectx) returns context.Canceled instead of grpc context canceled.
		switch status.Code(e) {
		case codes.DeadlineExceeded, codes.Canceled, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.ResourceExhausted, codes.Aborted, codes.OutOfRange, codes.Unavailable, codes.DataLoss, codes.Unknown:
			bo.delayTime = 2 * bo.delayTime
			bo.attempt--
		default:
			// Unexcepted error
			bo.delayTime = 0
			bo.attempt = 0
			log.Warn("unexcepted error, stop to retry", zap.Error(err))
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

func (bo *pdReqBackoffer) Attempt() int {
	return bo.attempt
}
