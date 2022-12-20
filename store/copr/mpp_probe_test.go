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

type ProbeTest map[string]*mockDetectClient

func (t ProbeTest) add(ctx context.Context) {
	for k, v := range t {
		globalMPPFailedStoreProbe.Add(ctx, k, v)
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
		ok := globalMPPFailedStoreProbe.IsRecovery(ctx, k, recoveryTTL)
		require.Equal(test, need, ok)
	}
}

func failedStoreSizeJudge(ctx context.Context, test *testing.T, need int) {
	var l int
	globalMPPFailedStoreProbe.scan(ctx)
	time.Sleep(time.Second / 10)
	globalMPPFailedStoreProbe.failedMPPStores.Range(func(k, v interface{}) bool {
		l++
		return true
	})
	require.Equal(test, need, l)
}

func testFlow(ctx context.Context, probetestest ProbeTest, test *testing.T, flow []string) {
	probetestest.add(ctx)
	for _, to := range flow {
		probetestest.reSetErrortestype(to)

		globalMPPFailedStoreProbe.scan(ctx)
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
		globalMPPFailedStoreProbe.maxRecoveryTimeLimit = 0 - time.Second
		failedStoreSizeJudge(ctx, test, need)
		globalMPPFailedStoreProbe.maxRecoveryTimeLimit = MaxRecoveryTimeLimit
	}

	cleanObsolet := func(need int) {
		globalMPPFailedStoreProbe.maxObsoletTimeLimit = 0 - time.Second
		failedStoreSizeJudge(ctx, test, need)
		globalMPPFailedStoreProbe.maxObsoletTimeLimit = MaxObsoletTimeLimit
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

	globalMPPFailedStoreProbe.detectPeriod = 0 - time.Second

	// Confirm that multiple tasks are not allowed
	globalMPPFailedStoreProbe.lock.Lock()
	globalMPPFailedStoreProbe.run(ctx)

	// check not exist address
	ok := globalMPPFailedStoreProbe.IsRecovery(ctx, notExistAddress, 0)
	require.True(t, ok)

	globalMPPFailedStoreProbe.scan(ctx)

	probetestest := map[string]*mockDetectClient{
		testimeout: {errortestype: testimeout},
		Error:      {errortestype: Error},
	}

	testFlowFinallyRecover := []string{Error, Normal, Error, Error, Normal}
	testFlow(ctx, probetestest, t, testFlowFinallyRecover)
	testFlowFinallyDesert := []string{Error, Normal, Normal, Error, Error}
	testFlow(ctx, probetestest, t, testFlowFinallyDesert)
}
