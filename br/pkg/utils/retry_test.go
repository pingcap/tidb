// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
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
