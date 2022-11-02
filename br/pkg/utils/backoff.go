// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"database/sql"
	"io"
	"time"

	"github.com/pingcap/errors"
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

	resetTSRetryTime       = 16
	resetTSWaitInterval    = 50 * time.Millisecond
	resetTSMaxWaitInterval = 500 * time.Millisecond
)

type importerBackoffer struct {
	attempt      int
	delayTime    time.Duration
	maxDelayTime time.Duration
}

// NewBackoffer creates a new controller regulating a truncated exponential backoff.
func NewBackoffer(attempt int, delayTime, maxDelayTime time.Duration) Backoffer {
	return &importerBackoffer{
		attempt:      attempt,
		delayTime:    delayTime,
		maxDelayTime: maxDelayTime,
	}
}

func NewImportSSTBackoffer() Backoffer {
	return NewBackoffer(importSSTRetryTimes, importSSTWaitInterval, importSSTMaxWaitInterval)
}

func NewDownloadSSTBackoffer() Backoffer {
	return NewBackoffer(downloadSSTRetryTimes, downloadSSTWaitInterval, downloadSSTMaxWaitInterval)
}

func (bo *importerBackoffer) NextBackoff(err error) time.Duration {
	if MessageIsRetryableStorageError(err.Error()) {
		bo.delayTime = 2 * bo.delayTime
		bo.attempt--
	} else {
		e := errors.Cause(err)
		switch e { // nolint:errorlint
		case berrors.ErrKVEpochNotMatch, berrors.ErrKVDownloadFailed, berrors.ErrKVIngestFailed:
			bo.delayTime = 2 * bo.delayTime
			bo.attempt--
		case berrors.ErrKVRangeIsEmpty, berrors.ErrKVRewriteRuleNotFound:
			// Excepted error, finish the operation
			bo.delayTime = 0
			bo.attempt = 0
		default:
			switch status.Code(e) {
			case codes.Unavailable, codes.Aborted:
				bo.delayTime = 2 * bo.delayTime
				bo.attempt--
			default:
				// Unexcepted error
				bo.delayTime = 0
				bo.attempt = 0
				log.Warn("unexcepted error, stop to retry", zap.Error(err))
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

func (bo *pdReqBackoffer) NextBackoff(err error) time.Duration {
	// bo.delayTime = 2 * bo.delayTime
	// bo.attempt--
	e := errors.Cause(err)
	switch e { // nolint:errorlint
	case nil, context.Canceled, context.DeadlineExceeded, io.EOF, sql.ErrNoRows:
		// Excepted error, finish the operation
		bo.delayTime = 0
		bo.attempt = 0
	default:
		switch status.Code(e) {
		case codes.DeadlineExceeded, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.ResourceExhausted, codes.Aborted, codes.OutOfRange, codes.Unavailable, codes.DataLoss, codes.Unknown:
			bo.delayTime = 2 * bo.delayTime
			bo.attempt--
		default:
			// Unexcepted error
			bo.delayTime = 0
			bo.attempt = 0
			log.Warn("unexcepted error, stop to retry", zap.Error(err))
		}
	}

	if bo.delayTime > bo.maxDelayTime {
		return bo.maxDelayTime
	}
	return bo.delayTime
}

func (bo *pdReqBackoffer) Attempt() int {
	return bo.attempt
}
