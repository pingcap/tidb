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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
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

func TestMPPFailedStoreProbeGoroutineTask(t *testing.T) {
	// Confirm that multiple tasks are not allowed
	GlobalMPPFailedStoreProber.lock.Lock()
	GlobalMPPFailedStoreProber.Run()
	GlobalMPPFailedStoreProber.lock.Unlock()

	GlobalMPPFailedStoreProber.Run()
	GlobalMPPFailedStoreProber.Stop()
}

func TestNextMPPInfoPruneInterval(t *testing.T) {
	for range 100 {
		interval := nextMPPInfoPruneInterval()
		require.GreaterOrEqual(t, interval, MPPInfoPruneMinInterval)
		require.LessOrEqual(t, interval, MPPInfoPruneMaxInterval)
	}
}

func TestMPPFailedStoreAssertFailed(t *testing.T) {
	ctx := context.Background()

	GlobalMPPFailedStoreProber.failedMPPStores.Store("errorinfo", nil)
	GlobalMPPFailedStoreProber.scan(ctx)

	GlobalMPPFailedStoreProber.failedMPPStores.Store("errorinfo", nil)
	GlobalMPPFailedStoreProber.IsRecovery(ctx, "errorinfo", 0)
}

type diagnosticsServerForTest struct {
	diagnosticspb.UnimplementedDiagnosticsServer
	calls atomic.Int32
	fail  atomic.Bool
}

func (s *diagnosticsServerForTest) ServerInfo(context.Context, *diagnosticspb.ServerInfoRequest) (*diagnosticspb.ServerInfoResponse, error) {
	s.calls.Add(1)
	if s.fail.Swap(false) {
		return nil, status.Error(codes.Unavailable, "mock server info error")
	}
	return &diagnosticspb.ServerInfoResponse{
		Items: []*diagnosticspb.ServerInfoItem{
			{
				Tp:   "test",
				Name: "test",
				Pairs: []*diagnosticspb.ServerInfoPair{
					{Key: "key", Value: "value"},
				},
			},
		},
	}, nil
}

type connCountingStatsHandler struct {
	connBegins atomic.Int32
}

func (h *connCountingStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (*connCountingStatsHandler) HandleRPC(context.Context, stats.RPCStats) {}

func (h *connCountingStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h *connCountingStatsHandler) HandleConn(_ context.Context, stat stats.ConnStats) {
	if _, ok := stat.(*stats.ConnBegin); ok {
		h.connBegins.Add(1)
	}
}

func startDiagnosticsGRPCServerForTest(t *testing.T) (string, *diagnosticsServerForTest, *connCountingStatsHandler) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	statsHandler := &connCountingStatsHandler{}
	server := grpc.NewServer(grpc.StatsHandler(statsHandler))
	diagnosticsServer := &diagnosticsServerForTest{}
	diagnosticspb.RegisterDiagnosticsServer(server, diagnosticsServer)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(lis)
	}()
	t.Cleanup(func() {
		server.Stop()
		<-errCh
	})

	return lis.Addr().String(), diagnosticsServer, statsHandler
}

func TestGetServerInfoByGRPCReusesConnection(t *testing.T) {
	closeGRPCConnsForTest()
	t.Cleanup(closeGRPCConnsForTest)

	address, diagnosticsServer, statsHandler := startDiagnosticsGRPCServerForTest(t)

	for i := 0; i < 2; i++ {
		items, err := GetServerInfoByGRPC(context.Background(), address, diagnosticspb.ServerInfoType_All)
		require.NoError(t, err)
		require.Len(t, items, 1)
	}

	require.Equal(t, int32(2), diagnosticsServer.calls.Load())
	require.Eventually(t, func() bool {
		return statsHandler.connBegins.Load() == 1
	}, time.Second, 10*time.Millisecond)

	Prune(context.Background())

	require.Equal(t, int32(3), diagnosticsServer.calls.Load())
	require.Eventually(t, func() bool {
		return statsHandler.connBegins.Load() == 1
	}, time.Second, 10*time.Millisecond)

	items, err := GetServerInfoByGRPC(context.Background(), address, diagnosticspb.ServerInfoType_All)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, int32(4), diagnosticsServer.calls.Load())
	require.Eventually(t, func() bool {
		return statsHandler.connBegins.Load() == 1
	}, time.Second, 10*time.Millisecond)

	diagnosticsServer.fail.Store(true)
	Prune(context.Background())
	require.Equal(t, int32(5), diagnosticsServer.calls.Load())

	items, err = GetServerInfoByGRPC(context.Background(), address, diagnosticspb.ServerInfoType_All)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, int32(6), diagnosticsServer.calls.Load())
	require.Eventually(t, func() bool {
		return statsHandler.connBegins.Load() == 2
	}, time.Second, 10*time.Millisecond)

	diagnosticsServer.fail.Store(true)
	_, err = GetServerInfoByGRPC(context.Background(), address, diagnosticspb.ServerInfoType_All)
	require.Error(t, err)
	require.Equal(t, int32(7), diagnosticsServer.calls.Load())

	items, err = GetServerInfoByGRPC(context.Background(), address, diagnosticspb.ServerInfoType_All)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, int32(8), diagnosticsServer.calls.Load())
	require.Eventually(t, func() bool {
		return statsHandler.connBegins.Load() == 3
	}, time.Second, 10*time.Millisecond)
}

func TestMppInfoManager(t *testing.T) {
	closeGRPCConnsForTest()
	t.Cleanup(closeGRPCConnsForTest)

	manager := &MppInfoManager{cachedStores: make(map[string]*MPPInfo)}
	manager.Delete("123") // Should happen nothing
	manager.Add(&MPPInfo{
		Address:         "123",
		LogicalCPUCount: 123,
		StartTimestamp:  456,
	})
	require.Equal(t, len(manager.cachedStores), 1)
	info := manager.Get("123")
	require.True(t, info != nil)
	require.Equal(t, info.Address, "123")
	require.Equal(t, info.LogicalCPUCount, uint64(123))
	require.Equal(t, info.StartTimestamp, int64(456))

	manager.Delete("123")
	require.Equal(t, len(manager.cachedStores), 0)
	info = manager.Get("123")
	require.True(t, info == nil)

	staleAddress, diagnosticsServer, _ := startDiagnosticsGRPCServerForTest(t)
	_, err := GetServerInfoByGRPC(context.Background(), staleAddress, diagnosticspb.ServerInfoType_All)
	require.NoError(t, err)
	activeAddress := "789"
	GlobalMPPInfoManager.Add(&MPPInfo{
		Address:         staleAddress,
		LogicalCPUCount: 456,
		StartTimestamp:  789,
	})
	GlobalMPPInfoManager.Add(&MPPInfo{
		Address:         activeAddress,
		LogicalCPUCount: 789,
		StartTimestamp:  123,
	})
	t.Cleanup(func() {
		GlobalMPPInfoManager.Delete(staleAddress)
		GlobalMPPInfoManager.Delete(activeAddress)
	})
	diagnosticsServer.fail.Store(true)
	Prune(context.Background())
	require.Nil(t, GlobalMPPInfoManager.Get(staleAddress))
	require.NotNil(t, GlobalMPPInfoManager.Get(activeAddress))
}
