// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"context"
	"testing"
	"time"

	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBackoffWithSuccess(t *testing.T) {
	var counter int
	backoffer := utils.NewBackoffer(10, time.Nanosecond, time.Nanosecond)
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		switch counter {
		case 0:
			return status.Error(codes.Unavailable, "transport is closing")
		case 1:
			return berrors.ErrKVEpochNotMatch
		case 2:
			return nil
		}
		return nil
	}, backoffer)
	require.Equal(t, 3, counter)
	require.NoError(t, err)
}

func TestBackoffWithFatalError(t *testing.T) {
	var counter int
	backoffer := utils.NewBackoffer(10, time.Nanosecond, time.Nanosecond)
	gRPCError := status.Error(codes.Unavailable, "transport is closing")
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		switch counter {
		case 0:
			return gRPCError // nolint:wrapcheck
		case 1:
			return berrors.ErrKVEpochNotMatch
		case 2:
			return berrors.ErrKVDownloadFailed
		case 3:
			return berrors.ErrKVRangeIsEmpty
		}
		return nil
	}, backoffer)
	require.Equal(t, 4, counter)
	require.Equal(t, []error{
		gRPCError,
		berrors.ErrKVEpochNotMatch,
		berrors.ErrKVDownloadFailed,
		berrors.ErrKVRangeIsEmpty,
	}, multierr.Errors(err))
}

func TestBackoffWithFatalRawGRPCError(t *testing.T) {
	var counter int
	canceledError := status.Error(codes.Canceled, "context canceled")
	backoffer := utils.NewBackoffer(10, time.Nanosecond, time.Nanosecond)
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		return canceledError // nolint:wrapcheck
	}, backoffer)
	require.Equal(t, 1, counter)
	require.Equal(t, []error{canceledError}, multierr.Errors(err))
}

func TestBackoffWithRetryableError(t *testing.T) {
	var counter int
	backoffer := utils.NewBackoffer(10, time.Nanosecond, time.Nanosecond)
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		return berrors.ErrKVEpochNotMatch
	}, backoffer)
	require.Equal(t, 10, counter)
	require.Equal(t, []error{
		berrors.ErrKVEpochNotMatch,
		berrors.ErrKVEpochNotMatch,
		berrors.ErrKVEpochNotMatch,
		berrors.ErrKVEpochNotMatch,
		berrors.ErrKVEpochNotMatch,
		berrors.ErrKVEpochNotMatch,
		berrors.ErrKVEpochNotMatch,
		berrors.ErrKVEpochNotMatch,
		berrors.ErrKVEpochNotMatch,
		berrors.ErrKVEpochNotMatch,
	}, multierr.Errors(err))
}

func TestPdBackoffWithRetryableError(t *testing.T) {
	var counter int
	backoffer := utils.NewPDReqBackoffer()
	gRPCError := status.Error(codes.Unavailable, "transport is closing")
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		return gRPCError
	}, backoffer)
	require.Equal(t, 16, counter)
	require.Equal(t, []error{
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
		gRPCError,
	}, multierr.Errors(err))
}

func TestNewImportSSTBackofferWithSucess(t *testing.T) {
	var counter int
	backoffer := utils.NewImportSSTBackoffer()
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		if counter == 15 {
			return nil
		} else {
			return berrors.ErrKVDownloadFailed
		}
	}, backoffer)
	require.Equal(t, 16, counter)
	require.Nil(t, err)
}

func TestNewDownloadSSTBackofferWithCancel(t *testing.T) {
	var counter int
	backoffer := utils.NewDownloadSSTBackoffer()
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		if counter == 3 {
			return context.Canceled
		} else {
			return berrors.ErrKVIngestFailed
		}

	}, backoffer)
	require.Equal(t, 4, counter)
	require.Equal(t, []error{
		berrors.ErrKVIngestFailed,
		berrors.ErrKVIngestFailed,
		berrors.ErrKVIngestFailed,
		context.Canceled,
	}, multierr.Errors(err))
}
