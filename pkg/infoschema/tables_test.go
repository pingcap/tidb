// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package infoschema

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

func TestIsTiFlashStore(t *testing.T) {
	// Test with TiFlash store
	tiflashStore := &metapb.Store{
		Labels: []*metapb.StoreLabel{
			{Key: placement.EngineLabelKey, Value: placement.EngineLabelTiFlash},
		},
	}
	require.True(t, isTiFlashStore(tiflashStore))

	// Test with non-TiFlash store
	nonTiflashStore := &metapb.Store{
		Labels: []*metapb.StoreLabel{
			{Key: placement.EngineLabelKey, Value: "tikv"},
		},
	}
	require.False(t, isTiFlashStore(nonTiflashStore))

	// Test with empty labels
	emptyStore := &metapb.Store{
		Labels: []*metapb.StoreLabel{},
	}
	require.False(t, isTiFlashStore(emptyStore))

	// Test with multiple labels including TiFlash
	multiLabelStore := &metapb.Store{
		Labels: []*metapb.StoreLabel{
			{Key: "zone", Value: "zone1"},
			{Key: placement.EngineLabelKey, Value: placement.EngineLabelTiFlash},
			{Key: "region", Value: "us-west"},
		},
	}
	require.True(t, isTiFlashStore(multiLabelStore))
}

func TestIsTiFlashWriteNode(t *testing.T) {
	// Test with TiFlash write node
	writeNode := &metapb.Store{
		Labels: []*metapb.StoreLabel{
			{Key: placement.EngineRoleLabelKey, Value: placement.EngineRoleLabelWrite},
		},
	}
	require.True(t, isTiFlashWriteNode(writeNode))

	// Test with non-write node
	nonWriteNode := &metapb.Store{
		Labels: []*metapb.StoreLabel{
			{Key: placement.EngineRoleLabelKey, Value: "read"},
		},
	}
	require.False(t, isTiFlashWriteNode(nonWriteNode))

	// Test with empty labels
	emptyStore := &metapb.Store{
		Labels: []*metapb.StoreLabel{},
	}
	require.False(t, isTiFlashWriteNode(emptyStore))

	// Test with multiple labels including write role
	multiLabelStore := &metapb.Store{
		Labels: []*metapb.StoreLabel{
			{Key: "zone", Value: "zone1"},
			{Key: placement.EngineRoleLabelKey, Value: placement.EngineRoleLabelWrite},
			{Key: "region", Value: "us-west"},
		},
	}
	require.True(t, isTiFlashWriteNode(multiLabelStore))
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

func TestGetServerInfoByGRPCReusesConnection(t *testing.T) {
	closeGRPCConnsForTest()
	t.Cleanup(closeGRPCConnsForTest)

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

	for i := 0; i < 2; i++ {
		items, err := getServerInfoByGRPC(context.Background(), lis.Addr().String(), diagnosticspb.ServerInfoType_All)
		require.NoError(t, err)
		require.Len(t, items, 1)
	}

	require.Equal(t, int32(2), diagnosticsServer.calls.Load())
	require.Eventually(t, func() bool {
		return statsHandler.connBegins.Load() == 1
	}, time.Second, 10*time.Millisecond)

	copr.GlobalMPPInfoManager.Add(&copr.MPPInfo{Address: lis.Addr().String()})
	copr.GlobalMPPInfoManager.Delete(lis.Addr().String())

	items, err := getServerInfoByGRPC(context.Background(), lis.Addr().String(), diagnosticspb.ServerInfoType_All)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, int32(3), diagnosticsServer.calls.Load())
	require.Eventually(t, func() bool {
		return statsHandler.connBegins.Load() == 2
	}, time.Second, 10*time.Millisecond)

	diagnosticsServer.fail.Store(true)
	_, err = getServerInfoByGRPC(context.Background(), lis.Addr().String(), diagnosticspb.ServerInfoType_All)
	require.Error(t, err)
	require.Equal(t, int32(4), diagnosticsServer.calls.Load())

	items, err = getServerInfoByGRPC(context.Background(), lis.Addr().String(), diagnosticspb.ServerInfoType_All)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, int32(5), diagnosticsServer.calls.Load())
	require.Eventually(t, func() bool {
		return statsHandler.connBegins.Load() == 3
	}, time.Second, 10*time.Millisecond)
}
