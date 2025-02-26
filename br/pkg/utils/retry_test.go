// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
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
	assert.NotEqualValues(0, bo.RemainingAttempts())

	annotatedErr := errors.Annotate(errors.Annotate(err1, "meow?"), "nya?")
	assert.Equal(time.Duration(0), bo.NextBackoff(annotatedErr))
	assert.Equal(0, bo.RemainingAttempts())

	mockBO = utils.InitialRetryState(100, time.Second, time.Second)
	bo = utils.GiveUpRetryOn(&mockBO, berrors.ErrBackupNoLeader)
	annotatedErr = berrors.ErrBackupNoLeader.FastGen("leader is taking an adventure")
	assert.Equal(time.Duration(0), bo.NextBackoff(annotatedErr))
	assert.Equal(0, bo.RemainingAttempts())
}
