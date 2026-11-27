// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
)

const (
	testimeout = "timeout"
	Error      = "error"
	Normal     = "normal"
)

type mockDetectClient struct {
	errortestype string
	probeErr     func(context.Context) error
}

func (t *mockDetectClient) CloseAddr(string) error {
	return nil
}

func (t *mockDetectClient) Close() error {
	return nil
}

func (t *mockDetectClient) SendRequest(
	ctx context.Context,
	addr string,
	req *tikvrpc.Request,
	timeout time.Duration,
) (*tikvrpc.Response, error) {
	if t.probeErr != nil {
		if err := t.probeErr(ctx); err != nil {
			return nil, err
		}
	}
	if t.errortestype == Error {
		return nil, errors.New("store error")
	} else if t.errortestype == testimeout {
		return &tikvrpc.Response{Resp: &mpp.IsAliveResponse{}}, nil
	}

	return &tikvrpc.Response{Resp: &mpp.IsAliveResponse{Available: true}}, nil
}

func (t *mockDetectClient) SendRequestAsync(
	ctx context.Context,
	addr string,
	req *tikvrpc.Request,
	cb async.Callback[*tikvrpc.Response],
) {
	go func() {
		cb.Schedule(t.SendRequest(ctx, addr, req, tikv.ReadTimeoutMedium))
	}()
}

func (t *mockDetectClient) SetEventListener(_ tikv.ClientEventListener) {}

type ProbeTest map[string]*mockDetectClient

func (t ProbeTest) add(ctx context.Context) {
	for k, v := range t {
		GlobalMPPFailedStoreProber.Add(ctx, k, v)
	}
}

func (t ProbeTest) reSetErrortestype(to string) {
	for k, v := range t {
		if to == Normal {
			v.errortestype = Normal
		} else {
			v.errortestype = k
		}
	}
}

func (t ProbeTest) judge(ctx context.Context, test *testing.T, recoveryTTL time.Duration, need bool) {
	for k := range t {
		ok := GlobalMPPFailedStoreProber.IsRecovery(ctx, k, recoveryTTL)
		require.Equal(test, need, ok)
	}
}

func failedStoreSizeJudge(ctx context.Context, test *testing.T, need int) {
	var l int
	GlobalMPPFailedStoreProber.scan(ctx)
	time.Sleep(time.Second / 10)
	GlobalMPPFailedStoreProber.failedMPPStores.Range(func(k, v any) bool {
		l++
		return true
	})
	require.Equal(test, need, l)
}

func testFlow(ctx context.Context, probetestest ProbeTest, test *testing.T, flow []string) {
	probetestest.add(ctx)
	for _, to := range flow {
		probetestest.reSetErrortestype(to)

		GlobalMPPFailedStoreProber.scan(ctx)
		time.Sleep(time.Second / 10) //wait detect goroutine finish

		var need bool
		if to == Normal {
			need = true
		}
		probetestest.judge(ctx, test, 0, need)
		probetestest.judge(ctx, test, time.Minute, false)
	}

	lastTo := flow[len(flow)-1]
	cleanRecover := func(need int) {
		GlobalMPPFailedStoreProber.maxRecoveryTimeLimit = 0 - time.Second
		failedStoreSizeJudge(ctx, test, need)
		GlobalMPPFailedStoreProber.maxRecoveryTimeLimit = MaxRecoveryTimeLimit
	}

	cleanObsolet := func(need int) {
		GlobalMPPFailedStoreProber.maxObsoletTimeLimit = 0 - time.Second
		failedStoreSizeJudge(ctx, test, need)
		GlobalMPPFailedStoreProber.maxObsoletTimeLimit = MaxObsoletTimeLimit
	}

	if lastTo == Error {
		cleanRecover(2)
		cleanObsolet(0)
	} else if lastTo == Normal {
		cleanObsolet(2)
		cleanRecover(0)
	}
}

func TestMPPFailedStoreProbe(t *testing.T) {
	ctx := context.Background()

	notExistAddress := "not exist address"

	GlobalMPPFailedStoreProber.detectPeriod = 0 - time.Second

	// check not exist address
	ok := GlobalMPPFailedStoreProber.IsRecovery(ctx, notExistAddress, 0)
	require.True(t, ok)

	GlobalMPPFailedStoreProber.scan(ctx)

	probetestest := map[string]*mockDetectClient{
		testimeout: {errortestype: testimeout},
		Error:      {errortestype: Error},
	}

	testFlowFinallyRecover := []string{Error, Normal, Error, Error, Normal}
	testFlow(ctx, probetestest, t, testFlowFinallyRecover)
	testFlowFinallyDesert := []string{Error, Normal, Normal, Error, Error}
	testFlow(ctx, probetestest, t, testFlowFinallyDesert)
}

func TestFilterAliveStoresCancellation(t *testing.T) {
	for _, mode := range []string{"cancel_during_probe", "expired_deadline", "rpc_error"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if mode == "expired_deadline" {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			}
			defer cancel()

			client := &mockDetectClient{probeErr: func(probeCtx context.Context) error {
				if mode == "cancel_during_probe" {
					cancel()
				}
				if mode == "rpc_error" {
					return errors.New("probe RPC failed")
				}
				return probeCtx.Err()
			}}
			store := &tikv.KVStore{}
			store.SetTiKVClient(client)
			copStore := &kvStore{store: store}
			addr := t.Name()
			t.Cleanup(func() { GlobalMPPFailedStoreProber.failedMPPStores.Delete(addr) })

			require.Empty(t, filterAliveStoresStr(ctx, []string{addr}, 0, copStore))
			_, failed := GlobalMPPFailedStoreProber.failedMPPStores.Load(addr)
			require.Equal(t, mode == "rpc_error", failed)

			// A different query must be able to use the healthy store immediately,
			// without waiting for the background failed-store prober to run.
			client.probeErr = nil
			alive := filterAliveStoresStr(context.Background(), []string{addr}, 0, copStore)
			if mode == "rpc_error" {
				require.Empty(t, alive)
			} else {
				require.Equal(t, []string{addr}, alive)
			}
		})
	}
}

func TestMPPFailedStoreProbeGoroutineTask(t *testing.T) {
	// Confirm that multiple tasks are not allowed
	GlobalMPPFailedStoreProber.lock.Lock()
	GlobalMPPFailedStoreProber.Run()
	GlobalMPPFailedStoreProber.lock.Unlock()

	GlobalMPPFailedStoreProber.Run()
	GlobalMPPFailedStoreProber.Stop()
}

func TestMPPFailedStoreAssertFailed(t *testing.T) {
	ctx := context.Background()

	GlobalMPPFailedStoreProber.failedMPPStores.Store("errorinfo", nil)
	GlobalMPPFailedStoreProber.scan(ctx)

	GlobalMPPFailedStoreProber.failedMPPStores.Store("errorinfo", nil)
	GlobalMPPFailedStoreProber.IsRecovery(ctx, "errorinfo", 0)
}

func TestMppServerInfoManager(t *testing.T) {
	manager := newMppServerInfoManager()
	manager.Delete("123") // Should happen nothing
	manager.Add(&MPPServerInfo{
		Address:         "123",
		LogicalCPUCount: 123,
		StartTimestamp:  456,
	})
	require.Equal(t, 1, manager.cachedStores.Size())
	info := manager.Get("123")
	require.True(t, info != nil)
	require.Equal(t, info.Address, "123")
	require.Equal(t, info.LogicalCPUCount, uint64(123))
	require.Equal(t, info.StartTimestamp, int64(456))

	manager.Delete("123")
	require.Equal(t, 0, manager.cachedStores.Size())
	info = manager.Get("123")
	require.True(t, info == nil)

	for i := 0; i < mppServerInfoManagerCacheSize; i++ {
		manager.Add(&MPPServerInfo{
			Address: fmt.Sprintf("store-%d", i),
		})
	}
	require.Equal(t, mppServerInfoManagerCacheSize, manager.cachedStores.Size())
	require.NotNil(t, manager.Get("store-0"))

	manager.Add(&MPPServerInfo{
		Address: fmt.Sprintf("store-%d", mppServerInfoManagerCacheSize),
	})
	require.Equal(t, mppServerInfoManagerCacheSize, manager.cachedStores.Size())
	require.NotNil(t, manager.Get("store-0"))
	require.Nil(t, manager.Get("store-1"))
}
