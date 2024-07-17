// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestRetryAdapter(t *testing.T) {
	req := require.New(t)

	begin := time.Now()
	bo := utils.AdaptTiKVBackoffer(context.Background(), 200, errors.New("everything is alright"))
	// This should sleep for 100ms.
	bo.Inner().Backoff(tikv.BoTiKVRPC(), errors.New("TiKV is in a deep dream"))
	sleeped := bo.TotalSleepInMS()
	req.GreaterOrEqual(sleeped, 50)
	req.LessOrEqual(sleeped, 150)
	requestedBackOff := [...]int{10, 20, 5, 0, 42, 48}
	wg := new(sync.WaitGroup)
	wg.Add(len(requestedBackOff))
	for _, bms := range requestedBackOff {
		bms := bms
		go func() {
			bo.RequestBackOff(bms)
			wg.Done()
		}()
	}
	wg.Wait()
	req.Equal(bo.NextSleepInMS(), 48)
	req.NoError(bo.BackOff())
	req.Equal(bo.TotalSleepInMS(), sleeped+48)

	bo.RequestBackOff(150)
	req.NoError(bo.BackOff())

	bo.RequestBackOff(150)
	req.ErrorContains(bo.BackOff(), "everything is alright", "total = %d / %d", bo.TotalSleepInMS(), bo.MaxSleepInMS())

	req.Greater(time.Since(begin), 200*time.Millisecond)
}

func TestFailNowIf(t *testing.T) {
	mockBO := utils.InitialRetryState(100, time.Second, time.Second)
	err1 := errors.New("error1")
	err2 := errors.New("error2")
	assert := require.New(t)

	bo := utils.GiveUpRetryOn(&mockBO, err1)

	// Test NextBackoff with an error that is not in failedOn
	assert.Equal(time.Second, bo.NextBackoff(err2))
	assert.NotEqualValues(0, bo.Attempt())

	annotatedErr := errors.Annotate(errors.Annotate(err1, "meow?"), "nya?")
	assert.Equal(time.Duration(0), bo.NextBackoff(annotatedErr))
	assert.Equal(0, bo.Attempt())

	mockBO = utils.InitialRetryState(100, time.Second, time.Second)
	bo = utils.GiveUpRetryOn(&mockBO, berrors.ErrBackupNoLeader)
	annotatedErr = berrors.ErrBackupNoLeader.FastGen("leader is taking an adventure")
	assert.Equal(time.Duration(0), bo.NextBackoff(annotatedErr))
	assert.Equal(0, bo.Attempt())
}

func TestHandleError(t *testing.T) {
	ec := utils.NewErrorContext("test", 3)
	// Test case 1: Error is nil
	result := utils.HandleBackupError(nil, 123, ec)
	require.Equal(t, utils.ErrorHandlingResult{Strategy: utils.Retry, Reason: utils.UnreachableRetryMsg}, result)

	// Test case 2: Error is KvError and can be ignored
	kvError := &backuppb.Error_KvError{}
	result = utils.HandleBackupError(&backuppb.Error{Detail: kvError}, 123, ec)
	require.Equal(t, utils.ErrorHandlingResult{Strategy: utils.Retry, Reason: utils.RetryOnKvErrorMsg}, result)

	// Test case 3: Error is RegionError and can be ignored
	regionError := &backuppb.Error_RegionError{
		RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{RegionId: 1}}}
	result = utils.HandleBackupError(&backuppb.Error{Detail: regionError}, 123, ec)
	require.Equal(t, utils.ErrorHandlingResult{Strategy: utils.Retry, Reason: utils.RetryOnRegionErrorMsg}, result)

	// Test case 4: Error is ClusterIdError
	clusterIdError := &backuppb.Error_ClusterIdError{}
	result = utils.HandleBackupError(&backuppb.Error{Detail: clusterIdError}, 123, ec)
	require.Equal(t, utils.ErrorHandlingResult{Strategy: utils.GiveUp, Reason: utils.ClusterIdMismatchMsg}, result)
}

func TestHandleErrorMsg(t *testing.T) {
	ec := utils.NewErrorContext("test", 3)

	// Test messageIsNotFoundStorageError
	msg := "IO: files Notfound error"
	uuid := uint64(456)
	expectedReason := "File or directory not found on TiKV Node (store id: 456). workaround: please ensure br and tikv nodes share a same storage and the user of br and tikv has same uid."
	expectedResult := utils.ErrorHandlingResult{Strategy: utils.GiveUp, Reason: expectedReason}
	actualResult := utils.HandleUnknownError(msg, uuid, ec)
	require.Equal(t, expectedResult, actualResult)

	// Test messageIsPermissionDeniedStorageError
	msg = "I/O permissiondenied error occurs on TiKV Node(store id: 456)."
	expectedReason = "I/O permission denied error occurs on TiKV Node(store id: 456). workaround: please ensure tikv has permission to read from & write to the storage."
	expectedResult = utils.ErrorHandlingResult{Strategy: utils.GiveUp, Reason: expectedReason}
	actualResult = utils.HandleUnknownError(msg, uuid, ec)
	require.Equal(t, expectedResult, actualResult)

	// Test MessageIsRetryableStorageError
	msg = "server closed"
	expectedResult = utils.ErrorHandlingResult{Strategy: utils.Retry, Reason: utils.RetryableStorageErrorMsg}
	actualResult = utils.HandleUnknownError(msg, uuid, ec)
	require.Equal(t, expectedResult, actualResult)

	// Test unknown error
	msg = "unknown error"
	expectedResult = utils.ErrorHandlingResult{Strategy: utils.Retry, Reason: utils.RetryOnUnknownErrorMsg}
	actualResult = utils.HandleUnknownError(msg, uuid, ec)
	require.Equal(t, expectedResult, actualResult)

	// Test retry too many times
	_ = utils.HandleUnknownError(msg, uuid, ec)
	_ = utils.HandleUnknownError(msg, uuid, ec)
	expectedResult = utils.ErrorHandlingResult{Strategy: utils.GiveUp, Reason: utils.NoRetryOnUnknownErrorMsg}
	actualResult = utils.HandleUnknownError(msg, uuid, ec)
	require.Equal(t, expectedResult, actualResult)
}
