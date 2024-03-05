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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

const (
	testimeout = "timeout"
	Error      = "error"
	Normal     = "normal"
)

type mockDetectClient struct {
	errortestype string
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
	if t.errortestype == Error {
		return nil, errors.New("store error")
	} else if t.errortestype == testimeout {
		return &tikvrpc.Response{Resp: &mpp.IsAliveResponse{}}, nil
	}

	return &tikvrpc.Response{Resp: &mpp.IsAliveResponse{Available: true}}, nil
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
