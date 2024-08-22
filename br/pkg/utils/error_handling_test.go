package utils

import (
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/stretchr/testify/require"
)

func TestHandleError(t *testing.T) {
	ec := NewErrorContext("test", 3)
	// Test case 1: Error is nil
	result := HandleBackupError(nil, 123, ec)
	require.Equal(t, ErrorHandlingResult{Strategy: StrategyRetry, Reason: unreachableRetryMsg}, result)

	// Test case 2: Error is KvError and can be ignored
	kvError := &backuppb.Error_KvError{}
	result = HandleBackupError(&backuppb.Error{Detail: kvError}, 123, ec)
	require.Equal(t, ErrorHandlingResult{Strategy: StrategyRetry, Reason: retryOnKvErrorMsg}, result)

	// Test case 3: Error is RegionError and can be ignored
	regionError := &backuppb.Error_RegionError{
		RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{RegionId: 1}}}
	result = HandleBackupError(&backuppb.Error{Detail: regionError}, 123, ec)
	require.Equal(t, ErrorHandlingResult{Strategy: StrategyRetry, Reason: retryOnRegionErrorMsg}, result)

	// Test case 4: Error is ClusterIdError
	clusterIdError := &backuppb.Error_ClusterIdError{}
	result = HandleBackupError(&backuppb.Error{Detail: clusterIdError}, 123, ec)
	require.Equal(t, ErrorHandlingResult{Strategy: StrategyGiveUp, Reason: clusterIdMismatchMsg}, result)
}

func TestHandleErrorMsg(t *testing.T) {
	ec := NewErrorContext("test", 3)

	// Test messageIsNotFoundStorageError
	msg := "IO: files Notfound error"
	uuid := uint64(456)
	expectedReason := "File or directory not found on TiKV Node (store id: 456). workaround: please ensure br and tikv nodes share a same storage and the user of br and tikv has same uid."
	expectedResult := ErrorHandlingResult{Strategy: StrategyGiveUp, Reason: expectedReason}
	actualResult := HandleUnknownBackupError(msg, uuid, ec)
	require.Equal(t, expectedResult, actualResult)

	// Test messageIsPermissionDeniedStorageError
	msg = "I/O permissiondenied error occurs on TiKV Node(store id: 456)."
	expectedReason = "I/O permission denied error occurs on TiKV Node(store id: 456). workaround: please ensure tikv has permission to read from & write to the storage."
	expectedResult = ErrorHandlingResult{Strategy: StrategyGiveUp, Reason: expectedReason}
	actualResult = HandleUnknownBackupError(msg, uuid, ec)
	require.Equal(t, expectedResult, actualResult)

	// Test MessageIsRetryableStorageError
	msg = "server closed"
	expectedResult = ErrorHandlingResult{Strategy: StrategyRetry, Reason: retryableStorageErrorMsg}
	actualResult = HandleUnknownBackupError(msg, uuid, ec)
	require.Equal(t, expectedResult, actualResult)

	// Test unknown error
	msg = "unknown error"
	expectedResult = ErrorHandlingResult{Strategy: StrategyRetry, Reason: retryOnUnknownErrorMsg}
	actualResult = HandleUnknownBackupError(msg, uuid, ec)
	require.Equal(t, expectedResult, actualResult)

	// Test retry too many times
	_ = HandleUnknownBackupError(msg, uuid, ec)
	_ = HandleUnknownBackupError(msg, uuid, ec)
	expectedResult = ErrorHandlingResult{Strategy: StrategyGiveUp, Reason: noRetryOnUnknownErrorMsg}
	actualResult = HandleUnknownBackupError(msg, uuid, ec)
	require.Equal(t, expectedResult, actualResult)
}
