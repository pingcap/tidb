// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	lightningtikv "github.com/pingcap/tidb/pkg/lightning/tikv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	tikv "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	sd "github.com/tikv/pd/client/servicediscovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type mockCodec struct {
	tikv.Codec
}

func (mockCodec) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	return append([]byte("k:"), start...), append([]byte("k:"), end...)
}

func (mockCodec) DecodeRegionRange(start, end []byte) ([]byte, []byte, error) {
	return bytes.TrimPrefix(start, []byte("k:")), bytes.TrimPrefix(end, []byte("k:")), nil
}

type mockHelperStorage struct {
	helper.Storage
	codec tikv.Codec
	pdCli pdhttp.Client
}

func (s mockHelperStorage) GetCodec() tikv.Codec {
	return s.codec
}

func (s mockHelperStorage) GetPDHTTPClient() pdhttp.Client {
	return s.pdCli
}

func (mockHelperStorage) GetRegionCache() *tikv.RegionCache {
	return nil
}

type mockStorageWithPDClient struct {
	mockHelperStorage
	pdClient pd.Client
}

func (s mockStorageWithPDClient) GetPDClient() pd.Client {
	return s.pdClient
}

type mockPDClient struct {
	pd.Client
}

type blockSamplePredictionTestStore struct {
	helper.Storage
	codec          tikv.Codec
	pdCli          pdhttp.Client
	snapshot       kv.Snapshot
	currentVersion kv.Version
}

func (s blockSamplePredictionTestStore) GetSnapshot(ver kv.Version) kv.Snapshot {
	return s.snapshot
}

func (s blockSamplePredictionTestStore) CurrentVersion(txnScope string) (kv.Version, error) {
	return s.currentVersion, nil
}

func (s blockSamplePredictionTestStore) GetCodec() tikv.Codec {
	return s.codec
}

func (s blockSamplePredictionTestStore) GetPDHTTPClient() pdhttp.Client {
	return s.pdCli
}

func (s blockSamplePredictionTestStore) GetRegionCache() *tikv.RegionCache {
	return nil
}

type blockSamplePredictionKV struct {
	key   kv.Key
	value []byte
}

type blockSamplePredictionSnapshot struct {
	kvs []blockSamplePredictionKV
}

func (s blockSamplePredictionSnapshot) Get(_ context.Context, key kv.Key, _ ...kv.GetOption) (kv.ValueEntry, error) {
	idx := slices.IndexFunc(s.kvs, func(item blockSamplePredictionKV) bool {
		return bytes.Equal(item.key, key)
	})
	if idx < 0 {
		return kv.ValueEntry{}, kv.ErrNotExist
	}
	return kv.NewValueEntry(s.kvs[idx].value, 0), nil
}

func (s blockSamplePredictionSnapshot) Iter(key kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	idx, _ := slices.BinarySearchFunc(s.kvs, key, func(item blockSamplePredictionKV, target kv.Key) int {
		return bytes.Compare(item.key, target)
	})
	return &blockSamplePredictionIterator{kvs: s.kvs, idx: idx, upperBound: upperBound}, nil
}

func (s blockSamplePredictionSnapshot) IterReverse(k, lowerBound kv.Key) (kv.Iterator, error) {
	return &kv.EmptyIterator{}, nil
}

func (s blockSamplePredictionSnapshot) BatchGet(ctx context.Context, keys []kv.Key, _ ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	result := make(map[string]kv.ValueEntry, len(keys))
	for _, key := range keys {
		entry, err := s.Get(ctx, key)
		if err == nil {
			result[string(key)] = entry
			continue
		}
		if !kv.ErrNotExist.Equal(err) {
			return nil, err
		}
	}
	return result, nil
}

func (s blockSamplePredictionSnapshot) SetOption(opt int, val any) {}

type blockSamplePredictionIterator struct {
	kvs        []blockSamplePredictionKV
	idx        int
	upperBound kv.Key
}

func (it *blockSamplePredictionIterator) Valid() bool {
	if it.idx >= len(it.kvs) {
		return false
	}
	return len(it.upperBound) == 0 || bytes.Compare(it.kvs[it.idx].key, it.upperBound) < 0
}

func (it *blockSamplePredictionIterator) Key() kv.Key {
	if !it.Valid() {
		return nil
	}
	return it.kvs[it.idx].key
}

func (it *blockSamplePredictionIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.kvs[it.idx].value
}

func (it *blockSamplePredictionIterator) Next() error {
	it.idx++
	return nil
}

func (it *blockSamplePredictionIterator) Close() {}

func sampleScopeCountsForTest(kvs []sampledIndexKV, count int) map[sampledIndexKVScope]int {
	scopeCounts := make(map[sampledIndexKVScope]int)
	for scope := range collectSampledIndexKVScopes(kvs) {
		scopeCounts[scope] = count
	}
	return scopeCounts
}

func sortedKVsForTest(kvs []sampledIndexKV) []sampledIndexKV {
	return sortSampledIndexKVs(kvs)
}

type mockPDHTTPClient struct {
	pdhttp.Client
	regionInfos     []*pdhttp.RegionsInfo
	replicateConfig map[string]any
	callCount       int
	firstRange      *pdhttp.KeyRange
	firstLimit      int
}

func (c *mockPDHTTPClient) WithCallerID(string) pdhttp.Client {
	return c
}

func (c *mockPDHTTPClient) GetRegionsByKeyRange(_ context.Context, keyRange *pdhttp.KeyRange, limit int) (*pdhttp.RegionsInfo, error) {
	if c.callCount == 0 {
		c.firstRange = keyRange
		c.firstLimit = limit
	}
	if c.callCount >= len(c.regionInfos) {
		return &pdhttp.RegionsInfo{}, nil
	}
	info := c.regionInfos[c.callCount]
	c.callCount++
	return info, nil
}

func (c *mockPDHTTPClient) GetReplicateConfig(context.Context) (map[string]any, error) {
	return c.replicateConfig, nil
}

type mockPDStoreStatsClient struct {
	clusterID        uint64
	serviceDiscovery sd.ServiceDiscovery
}

func (c mockPDStoreStatsClient) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

func (c mockPDStoreStatsClient) GetServiceDiscovery() sd.ServiceDiscovery {
	return c.serviceDiscovery
}

type mockServiceDiscovery struct {
	serviceClient sd.ServiceClient
}

func (d mockServiceDiscovery) Init() error { return nil }
func (d mockServiceDiscovery) Close()      {}
func (mockServiceDiscovery) GetClusterID() uint64 {
	return 0
}
func (mockServiceDiscovery) GetKeyspaceID() uint32 {
	return 0
}
func (mockServiceDiscovery) SetKeyspaceID(uint32) {}
func (mockServiceDiscovery) GetKeyspaceGroupID() uint32 {
	return 0
}
func (mockServiceDiscovery) GetServiceURLs() []string { return nil }
func (mockServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn {
	return nil
}
func (mockServiceDiscovery) GetClientConns() *sync.Map { return nil }
func (mockServiceDiscovery) GetServingURL() string     { return "" }
func (mockServiceDiscovery) GetBackupURLs() []string   { return nil }
func (d mockServiceDiscovery) GetServiceClient() sd.ServiceClient {
	return d.serviceClient
}
func (d mockServiceDiscovery) GetServiceClientByKind(sd.APIKind) sd.ServiceClient {
	return d.serviceClient
}
func (d mockServiceDiscovery) GetAllServiceClients() []sd.ServiceClient {
	if d.serviceClient == nil {
		return nil
	}
	return []sd.ServiceClient{d.serviceClient}
}
func (mockServiceDiscovery) GetOrCreateGRPCConn(string) (*grpc.ClientConn, error) {
	return nil, nil
}
func (mockServiceDiscovery) ScheduleCheckMemberChanged() {}
func (mockServiceDiscovery) CheckMemberChanged() error   { return nil }
func (mockServiceDiscovery) ExecAndAddLeaderSwitchedCallback(sd.LeaderSwitchedCallbackFunc) {
}
func (mockServiceDiscovery) AddLeaderSwitchedCallback(sd.LeaderSwitchedCallbackFunc) {}
func (mockServiceDiscovery) AddMembersChangedCallback(func())                        {}

type mockServiceClient struct {
	conn *grpc.ClientConn
}

func (c mockServiceClient) GetURL() string { return "" }
func (c mockServiceClient) GetClientConn() *grpc.ClientConn {
	return c.conn
}
func (c mockServiceClient) BuildGRPCTargetContext(ctx context.Context, _ bool) context.Context {
	return ctx
}
func (c mockServiceClient) IsConnectedToLeader() bool { return true }
func (c mockServiceClient) Available() bool           { return true }
func (c mockServiceClient) NeedRetry(*pdpb.Error, error) bool {
	return false
}

type mockPDServer struct {
	pdpb.UnimplementedPDServer
	store *metapb.Store
	stats *pdpb.StoreStats
	err   error
}

func (s *mockPDServer) GetStore(context.Context, *pdpb.GetStoreRequest) (*pdpb.GetStoreResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &pdpb.GetStoreResponse{
		Header: &pdpb.ResponseHeader{},
		Store:  s.store,
		Stats:  s.stats,
	}, nil
}

func newMockPDStoreStatsClient(t *testing.T, store *metapb.Store, stats *pdpb.StoreStats, err error) mockPDStoreStatsClient {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pdpb.RegisterPDServer(server, &mockPDServer{store: store, stats: stats, err: err})
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
	})
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})
	return mockPDStoreStatsClient{
		clusterID:        1,
		serviceDiscovery: mockServiceDiscovery{serviceClient: mockServiceClient{conn: conn}},
	}
}

func expectedRegionRange(tableID int64) ([]byte, []byte) {
	tableStart, tableEnd := tablecodec.GetTableHandleKeyRange(tableID)
	return mockCodec{}.EncodeRegionRange(tableStart, tableEnd)
}

func tablePDRegionForTest(t *testing.T, store helper.Storage, physicalID int64, approximateKeys int64) pdhttp.RegionInfo {
	t.Helper()
	tableStart, tableEnd := tablecodec.GetTableHandleKeyRange(physicalID)
	start, end := store.GetCodec().EncodeRegionRange(tableStart, tableEnd)
	return pdhttp.RegionInfo{
		ID:              physicalID,
		StartKey:        hex.EncodeToString(start),
		EndKey:          hex.EncodeToString(end),
		ApproximateKeys: approximateKeys,
	}
}

func blockSamplePredictionTableForTest() table.PhysicalTable {
	idType := types.NewFieldType(mysql.TypeLonglong)
	idType.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	intType := types.NewFieldType(mysql.TypeLonglong)
	intType.AddFlag(mysql.NotNullFlag)
	stringType := types.NewFieldType(mysql.TypeVarString)
	stringType.SetCharset(charset.CharsetUTF8MB4)
	stringType.SetCollate(charset.CollationBin)

	tblInfo := &model.TableInfo{
		ID:         60101,
		Name:       ast.NewCIStr("t_block_sample_prediction"),
		State:      model.StatePublic,
		PKIsHandle: true,
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), Offset: 0, State: model.StatePublic, FieldType: *idType},
			{ID: 2, Name: ast.NewCIStr("b"), Offset: 1, State: model.StatePublic, FieldType: *intType},
			{ID: 3, Name: ast.NewCIStr("c"), Offset: 2, State: model.StatePublic, FieldType: *stringType},
		},
		Indices: []*model.IndexInfo{
			{
				ID:    60102,
				Name:  ast.NewCIStr("idx_b"),
				State: model.StatePublic,
				Columns: []*model.IndexColumn{
					{Name: ast.NewCIStr("b"), Offset: 1, Length: types.UnspecifiedLength},
				},
			},
		},
	}
	return tables.MockTableFromMeta(tblInfo).(table.PhysicalTable)
}

func blockSamplePredictionRowKVForTest(t *testing.T, physicalTbl table.PhysicalTable, handle int64, b int64, c string) blockSamplePredictionKV {
	t.Helper()
	value, err := tablecodec.EncodeOldRow(
		time.UTC,
		[]types.Datum{types.NewIntDatum(b), types.NewStringDatum(c)},
		[]int64{2, 3},
		nil,
		nil,
	)
	require.NoError(t, err)
	return blockSamplePredictionKV{
		key:   tablecodec.EncodeRowKeyWithHandle(physicalTbl.GetPhysicalID(), kv.IntHandle(handle)),
		value: value,
	}
}

func TestEstimateTableSizeByIDUsesMaxApproximateSizes(t *testing.T) {
	t.Run("TiKVSpacePrecheckCapability", func(t *testing.T) {
		ok, err := canRunTiKVSpacePrecheck(mockHelperStorage{codec: mockCodec{}})
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = canRunTiKVSpacePrecheck(mockHelperStorage{
			codec: mockCodec{},
			pdCli: &mockPDHTTPClient{},
		})
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = canRunTiKVSpacePrecheck(mockStorageWithPDClient{
			mockHelperStorage: mockHelperStorage{codec: mockCodec{}},
			pdClient:          &mockPDClient{},
		})
		require.NoError(t, err)
		require.True(t, ok)
	})

	pdCli := &mockPDHTTPClient{
		regionInfos: []*pdhttp.RegionsInfo{
			{
				Count: 3,
				Regions: []pdhttp.RegionInfo{
					// kv > size -> use kv
					{ID: 1, ApproximateSize: 5, ApproximateKvSize: 64},
					// size > kv -> use size
					{ID: 2, ApproximateSize: 16, ApproximateKvSize: 7},
					// zero still follows max()
					{ID: 3, ApproximateSize: 0, ApproximateKvSize: 9},
				},
			},
			{},
		},
	}

	size, err := estimateTableSizeByID(context.Background(), pdCli, mockHelperStorage{codec: mockCodec{}}, 42)
	require.NoError(t, err)
	require.Equal(t, int64(89*units.MiB), size)
	require.Equal(t, 2, pdCli.callCount)
	expectedStart, expectedEnd := expectedRegionRange(42)
	require.NotNil(t, pdCli.firstRange)
	require.Equal(t, 128, pdCli.firstLimit)
	require.Equal(t, expectedStart, pdCli.firstRange.StartKey)
	require.Equal(t, expectedEnd, pdCli.firstRange.EndKey)

	t.Run("EstimateRowSizeFromRegionUsesMaxApproximateSizes", func(t *testing.T) {
		tableID := int64(1024)
		tbl := tables.MockTableFromMeta(&model.TableInfo{ID: tableID})
		testCases := []struct {
			name            string
			approxSizeMiB   int64
			approxKvSizeMiB int64
			approxKeys      int64
			expectedBytes   int
		}{
			{
				name:            "kv-greater-than-size-uses-kv",
				approxSizeMiB:   4,
				approxKvSizeMiB: 10,
				approxKeys:      2,
				expectedBytes:   int(10 * units.MiB / 2),
			},
			{
				name:            "size-greater-than-kv-uses-size",
				approxSizeMiB:   12,
				approxKvSizeMiB: 3,
				approxKeys:      3,
				expectedBytes:   int(12 * units.MiB / 3),
			},
			{
				name:            "zero-kv-size-still-uses-max",
				approxSizeMiB:   9,
				approxKvSizeMiB: 0,
				approxKeys:      3,
				expectedBytes:   int(9 * units.MiB / 3),
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				rowPD := &mockPDHTTPClient{
					regionInfos: []*pdhttp.RegionsInfo{
						{
							Count: 3,
							Regions: []pdhttp.RegionInfo{
								{ID: 1, ApproximateSize: 1, ApproximateKvSize: 1, ApproximateKeys: 1},
								{
									ID:                2,
									ApproximateSize:   tc.approxSizeMiB,
									ApproximateKvSize: tc.approxKvSizeMiB,
									ApproximateKeys:   tc.approxKeys,
								},
								{ID: 3, ApproximateSize: 1, ApproximateKvSize: 1, ApproximateKeys: 1},
							},
						},
					},
				}
				rowSize, err := estimateRowSizeFromRegion(
					context.Background(),
					mockHelperStorage{codec: mockCodec{}, pdCli: rowPD},
					tbl,
				)
				require.NoError(t, err)
				require.Equal(t, tc.expectedBytes, rowSize)
				require.Equal(t, 1, rowPD.callCount)
				require.Equal(t, 3, rowPD.firstLimit)
				expectedStart, expectedEnd := expectedRegionRange(tableID)
				require.NotNil(t, rowPD.firstRange)
				require.Equal(t, expectedStart, rowPD.firstRange.StartKey)
				require.Equal(t, expectedEnd, rowPD.firstRange.EndKey)
			})
		}
	})
}

func TestPredictTiKVIndexBytesBlockSample(t *testing.T) {
	physicalTbl := blockSamplePredictionTableForTest()
	idxInfo := physicalTbl.Meta().FindIndexByName("idx_b")
	require.NotNil(t, idxInfo)

	rowKVs := make([]blockSamplePredictionKV, 0, 16)
	for i := 1; i <= 16; i++ {
		rowKVs = append(rowKVs, blockSamplePredictionRowKVForTest(t, physicalTbl, int64(i), int64(i%4), fmt.Sprintf("v%02d", i)))
	}
	slices.SortFunc(rowKVs, func(a, b blockSamplePredictionKV) int {
		return bytes.Compare(a.key, b.key)
	})

	pdCli := &mockPDHTTPClient{
		regionInfos: []*pdhttp.RegionsInfo{
			{
				Count: 1,
				Regions: []pdhttp.RegionInfo{
					tablePDRegionForTest(t, mockHelperStorage{codec: mockCodec{}}, physicalTbl.GetPhysicalID(), 16),
				},
			},
		},
	}
	workerStore := blockSamplePredictionTestStore{
		codec:          mockCodec{},
		pdCli:          pdCli,
		snapshot:       blockSamplePredictionSnapshot{kvs: rowKVs},
		currentVersion: kv.Version{Ver: 404411537129996288},
	}
	w := &worker{
		ddlCtx: &ddlCtx{
			store: workerStore,
		},
	}
	sctx := mock.NewContext()

	result, err := w.predictTiKVIndexBytesBlockSample(context.Background(), sctx, physicalTbl, &reorgInfo{
		Job: &model.Job{
			ID:        9527,
			ReorgMeta: &model.DDLReorgMeta{},
		},
		elements: []*meta.Element{
			{ID: idxInfo.ID, TypeKey: meta.IndexElementKey},
		},
	})
	require.NoError(t, err)
	require.False(t, result.UseStats)
	require.Equal(t, 1, result.SampledRegionCount)
	require.Equal(t, 16, result.SampledRowCount)
	require.Zero(t, result.ReadErrorCount)
	require.Positive(t, result.PredictedBytes)
	require.Positive(t, result.MVCCOverheadBytes)
	require.GreaterOrEqual(t, result.EncodedKeySharedPrefixAvgBytes, 0.0)
	require.GreaterOrEqual(t, result.RawKeySharedPrefixAvgBytes, 0.0)
	require.Positive(t, result.RawKeyLengthAvgBytes)
	require.Equal(t, 1, pdCli.callCount)
}

func TestGetPDStoreStatsUsesRawUsedSize(t *testing.T) {
	store := &metapb.Store{Id: 1}
	stats := &pdpb.StoreStats{
		StoreId:   1,
		Capacity:  1024,
		Available: 960,
		UsedSize:  321,
	}
	pdCli := newMockPDStoreStatsClient(t, store, stats, nil)

	got, err := collectTiKVStoreCapacityFromStoreStats(context.Background(), pdCli, store)
	require.NoError(t, err)
	require.EqualValues(t, 1, got.StoreID)
	require.EqualValues(t, 1024, got.TotalBytes)
	require.EqualValues(t, 960, got.AvailableBytes)
	require.EqualValues(t, 321, got.UsedBytes)
}

func TestObservedTiKVUsageTaskTiming(t *testing.T) {
	start := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	end := start.Add(4 * time.Second)

	task := &proto.Task{
		StartTime:       start,
		StateUpdateTime: end,
	}
	taskEndTime, taskExecutionDuration := observedTiKVUsageTaskTiming(task, time.Time{})
	require.Equal(t, end, taskEndTime)
	require.Equal(t, 4*time.Second, taskExecutionDuration)

	fallbackObservedAt := start.Add(5 * time.Second)
	task = &proto.Task{StartTime: start}
	taskEndTime, taskExecutionDuration = observedTiKVUsageTaskTiming(task, fallbackObservedAt)
	require.Equal(t, fallbackObservedAt, taskEndTime)
	require.Equal(t, 5*time.Second, taskExecutionDuration)

	task = &proto.Task{
		StartTime:       start,
		StateUpdateTime: start.Add(-time.Second),
	}
	taskEndTime, taskExecutionDuration = observedTiKVUsageTaskTiming(task, time.Time{})
	require.Equal(t, start.Add(-time.Second), taskEndTime)
	require.Zero(t, taskExecutionDuration)

	t.Run("deduplicate ingested SSTs", func(t *testing.T) {
		summary := &execute.SubtaskSummary{}
		recorder := newIngestedSSTRecorder(summary)
		recorder.RecordIngestedSST("classic/uuid/default", 100)
		recorder.RecordIngestedSST("classic/uuid/default", 100)
		recorder.RecordIngestedSST("classic/uuid/write", 40)
		recorder.RecordIngestedSST("classic/zero/default", 0)
		recorder.RecordIngestedSST("", 10)
		require.EqualValues(t, 140, summary.IngestedSSTBytes.Load())
		require.EqualValues(t, 3, summary.IngestedSSTCount.Load())
		require.EqualValues(t, 1, summary.IngestedSSTZeroSizeCount.Load())
		require.EqualValues(t, 1, summary.IngestedSSTInvalidIdentityCount.Load())

		recorder.Reset()
		require.Zero(t, summary.IngestedSSTBytes.Load())
		require.Zero(t, summary.IngestedSSTCount.Load())
		recorder.RecordIngestedSST("classic/uuid/default", 100)
		require.EqualValues(t, 100, summary.IngestedSSTBytes.Load())
	})

	summarySubtask := func(t *testing.T, bytes, count, zeroSizeCount, invalidIdentityCount uint64) *proto.Subtask {
		summary := &execute.SubtaskSummary{}
		summary.IngestedSSTBytes.Store(bytes)
		summary.IngestedSSTCount.Store(count)
		summary.IngestedSSTZeroSizeCount.Store(zeroSizeCount)
		summary.IngestedSSTInvalidIdentityCount.Store(invalidIdentityCount)
		data, err := json.Marshal(summary)
		require.NoError(t, err)
		return &proto.Subtask{Summary: string(data)}
	}

	t.Run("summarize classic ingested SSTs", func(t *testing.T) {
		observation, err := summarizeIngestedSSTBytes(ingestedSSTBytesSourceClassic, 200,
			[]*proto.Subtask{summarySubtask(t, 100, 1, 0, 0)},
			[]*proto.Subtask{summarySubtask(t, 40, 1, 0, 0)})
		require.NoError(t, err)
		require.EqualValues(t, 140, observation.bytes)
		require.True(t, observation.reliable)
		require.Equal(t, "ok", observation.reason)

		observation, err = summarizeIngestedSSTBytes(
			ingestedSSTBytesSourceClassic, 200,
			[]*proto.Subtask{summarySubtask(t, 0, 1, 1, 0)})
		require.NoError(t, err)
		require.False(t, observation.reliable)
		require.Equal(t, "classic_tikv_data_metric_unsupported", observation.reason)

		observation, err = summarizeIngestedSSTBytes(
			ingestedSSTBytesSourceClassic, 200,
			[]*proto.Subtask{summarySubtask(t, 100, 2, 1, 0)})
		require.NoError(t, err)
		require.EqualValues(t, 100, observation.bytes)
		require.False(t, observation.reliable)
		require.Equal(t, "classic_tikv_data_metric_unsupported", observation.reason)

		observation, err = summarizeIngestedSSTBytes(ingestedSSTBytesSourceClassic, 200, nil)
		require.NoError(t, err)
		require.False(t, observation.reliable)
		require.Equal(t, "ingested_sst_summary_unavailable", observation.reason)
	})

	t.Run("summarize next-gen ingested SSTs", func(t *testing.T) {
		observation, err := summarizeIngestedSSTBytes(
			ingestedSSTBytesSourceNextGen, 200,
			[]*proto.Subtask{summarySubtask(t, 0, 1, 1, 0)})
		require.NoError(t, err)
		require.False(t, observation.reliable)
		require.Equal(t, "next_gen_sst_size_unavailable", observation.reason)

		observation, err = summarizeIngestedSSTBytes(
			ingestedSSTBytesSourceNextGen, 200,
			[]*proto.Subtask{summarySubtask(t, 100, 1, 0, 1)})
		require.NoError(t, err)
		require.False(t, observation.reliable)
		require.Equal(t, "ingested_sst_identity_unavailable", observation.reason)
	})

	t.Run("decode legacy block sample prediction", func(t *testing.T) {
		taskMeta := &BackfillTaskMeta{}
		err := json.Unmarshal([]byte(`{
			"block_sample_steady_predicted_tikv_index_bytes":123,
			"tikv_replica_count":3
		}`), taskMeta)
		require.NoError(t, err)
		require.EqualValues(t, 123, taskMeta.blockSamplePredictedTiKVIndexAllReplicaBytes())
		require.EqualValues(t, 41, taskMeta.blockSamplePredictedTiKVIndexSingleReplicaBytes())
	})
}

func TestCollectTiKVStoreCapacity(t *testing.T) {
	t.Run("space precheck", func(t *testing.T) {
		err := checkTiKVSpaceForAddIndex(&TiKVClusterCapacity{
			TotalBytes:     1000,
			AvailableBytes: 380,
			StoreCount:     2,
			Stores: []TiKVStoreCapacity{
				{StoreID: 1, TotalBytes: 500, AvailableBytes: 260},
				{StoreID: 2, TotalBytes: 500, AvailableBytes: 120},
			},
		}, 60)
		require.NoError(t, err)

		err = checkTiKVSpaceForAddIndex(&TiKVClusterCapacity{
			TotalBytes:     1000,
			AvailableBytes: 210,
			StoreCount:     2,
			Stores: []TiKVStoreCapacity{
				{StoreID: 1, TotalBytes: 500, AvailableBytes: 160},
				{StoreID: 2, TotalBytes: 500, AvailableBytes: 50},
			},
		}, 20)
		require.ErrorContains(t, err, "TiKV cluster capacity")

		err = checkTiKVSpaceForAddIndex(&TiKVClusterCapacity{
			TotalBytes:     1000,
			AvailableBytes: 320,
			StoreCount:     2,
			Stores: []TiKVStoreCapacity{
				{StoreID: 1, TotalBytes: 500, AvailableBytes: 260},
				{StoreID: 2, TotalBytes: 500, AvailableBytes: 60},
			},
		}, 120)
		require.ErrorContains(t, err, "TiKV store capacity")
	})

	t.Run("space precheck enforce behavior", func(t *testing.T) {
		capacity := &TiKVClusterCapacity{
			TotalBytes:     1000,
			AvailableBytes: 210,
			StoreCount:     2,
			Stores: []TiKVStoreCapacity{
				{StoreID: 1, TotalBytes: 500, AvailableBytes: 130},
				{StoreID: 2, TotalBytes: 500, AvailableBytes: 80},
			},
		}
		checkErr, rejectErr := evaluateAddIndexTiKVSpacePrecheck(capacity, 20, false)
		require.ErrorContains(t, checkErr, "TiKV cluster capacity")
		require.NoError(t, rejectErr)

		checkErr, rejectErr = evaluateAddIndexTiKVSpacePrecheck(capacity, 20, true)
		require.ErrorContains(t, checkErr, "TiKV cluster capacity")
		require.EqualError(t, rejectErr, checkErr.Error())

		checkErr, rejectErr = evaluateAddIndexTiKVSpacePrecheck(capacity, 0, true)
		require.NoError(t, checkErr)
		require.NoError(t, rejectErr)
	})

	t.Run("replica count from placement bundle excludes non-TiKV rules", func(t *testing.T) {
		bundle := &placement.Bundle{
			Rules: []*pdhttp.Rule{
				{Role: pdhttp.Leader, Count: 1},
				{Role: pdhttp.Voter, Count: 2},
				{Role: pdhttp.Follower, Count: 1},
				{Role: pdhttp.Learner, Count: 1, GroupID: placement.TiFlashRuleGroupID},
				{
					Role:  pdhttp.Learner,
					Count: 1,
					LabelConstraints: []pdhttp.LabelConstraint{
						{Key: placement.EngineLabelKey, Op: pdhttp.In, Values: []string{placement.EngineLabelTiFlash}},
					},
				},
				{
					Role:  pdhttp.Learner,
					Count: 1,
					LabelConstraints: []pdhttp.LabelConstraint{
						{Key: placement.EngineLabelKey, Op: pdhttp.NotIn, Values: []string{placement.EngineLabelTiFlash}},
					},
				},
			},
		}

		require.EqualValues(t, 5, tiKVReplicaCountFromBundle(bundle, 0))

		tableStartKey, tableEndKey := placementBundleRuleKeyRangeHex(100)
		firstPartitionStartKey, firstPartitionEndKey := placementBundleRuleKeyRangeHex(101)
		secondPartitionStartKey, secondPartitionEndKey := placementBundleRuleKeyRangeHex(102)
		partitionedBundle := &placement.Bundle{
			Rules: []*pdhttp.Rule{
				{Role: pdhttp.Voter, Count: 2, StartKeyHex: tableStartKey, EndKeyHex: tableEndKey},
				{Role: pdhttp.Voter, Count: 4, StartKeyHex: firstPartitionStartKey, EndKeyHex: firstPartitionEndKey},
				{Role: pdhttp.Voter, Count: 6, StartKeyHex: secondPartitionStartKey, EndKeyHex: secondPartitionEndKey},
			},
		}
		require.EqualValues(t, 4, tiKVReplicaCountFromBundle(partitionedBundle, 101))
		require.EqualValues(t, 2, tiKVReplicaCountFromBundle(partitionedBundle, 100))
	})

	t.Run("representative replica physical ID uses global index before first partition", func(t *testing.T) {
		tblInfo := &model.TableInfo{
			ID: 100,
			Partition: &model.PartitionInfo{
				Enable: true,
				Definitions: []model.PartitionDefinition{
					{ID: 101},
					{ID: 102},
				},
			},
		}
		localIdx := &model.IndexInfo{ID: 1}
		globalIdx := &model.IndexInfo{ID: 2, Global: true}

		require.EqualValues(t, 101, representativeAddIndexTiKVReplicaPhysicalID(tblInfo, []*model.IndexInfo{localIdx}))
		require.EqualValues(t, 100, representativeAddIndexTiKVReplicaPhysicalID(tblInfo, []*model.IndexInfo{globalIdx}))
		require.EqualValues(t, 100, representativeAddIndexTiKVReplicaPhysicalID(tblInfo, []*model.IndexInfo{localIdx, globalIdx}))
		require.EqualValues(t, 100, representativeAddIndexTiKVReplicaPhysicalID(&model.TableInfo{ID: 100}, []*model.IndexInfo{localIdx}))
	})

	t.Run("PD max replicas parsing and collection", func(t *testing.T) {
		replicaCount, ok := pdReplicateConfigMaxReplicas(map[string]any{"max-replicas": float64(5)})
		require.True(t, ok)
		require.EqualValues(t, 5, replicaCount)

		replicaCount, ok = pdReplicateConfigMaxReplicas(map[string]any{"max-replicas": "7"})
		require.True(t, ok)
		require.EqualValues(t, 7, replicaCount)

		_, ok = pdReplicateConfigMaxReplicas(map[string]any{"max-replicas": float64(2.5)})
		require.False(t, ok)

		replicaCount, err := collectPDMaxReplicas(context.Background(), mockHelperStorage{
			codec: mockCodec{},
			pdCli: &mockPDHTTPClient{replicateConfig: map[string]any{
				"max-replicas": float64(6),
			}},
		})
		require.NoError(t, err)
		require.EqualValues(t, 6, replicaCount)
	})

	t.Run("prediction bytes scale by replica count with saturation", func(t *testing.T) {
		require.EqualValues(t, 300, scalePredictedBytesByReplicaCount(100, 3))
		require.EqualValues(t, 100, scalePredictedBytesByReplicaCount(100, 1))
		require.Equal(t, uint64(math.MaxUint64), scalePredictedBytesByReplicaCount(math.MaxUint64, 2))
	})

	t.Run("virtual column filler populates generated column values", func(t *testing.T) {
		sctx := mock.NewContext()
		intType := types.NewFieldType(mysql.TypeLonglong)
		intType.AddFlag(mysql.NotNullFlag)
		tblInfo := &model.TableInfo{
			ID:    200,
			Name:  ast.NewCIStr("t"),
			State: model.StatePublic,
			Columns: []*model.ColumnInfo{
				{ID: 1, Name: ast.NewCIStr("a"), Offset: 0, State: model.StatePublic, FieldType: *intType},
				{
					ID:                  2,
					Name:                ast.NewCIStr("b"),
					Offset:              1,
					State:               model.StatePublic,
					FieldType:           *intType,
					GeneratedExprString: "a + 1",
					GeneratedStored:     false,
				},
			},
			Indices: []*model.IndexInfo{
				{
					ID:    20,
					State: model.StatePublic,
					Columns: []*model.IndexColumn{
						{Name: ast.NewCIStr("b"), Offset: 1, Length: types.UnspecifiedLength},
					},
				},
			},
		}
		physicalTbl := tables.MockTableFromMeta(tblInfo).(table.PhysicalTable)
		filler, err := newSamplePredictionVirtualColumnFiller(sctx.GetExprCtx(), tblInfo, physicalTbl.Cols())
		require.NoError(t, err)
		require.NotNil(t, filler)

		row, err := filler.fill([]types.Datum{types.NewIntDatum(41), {}})
		require.NoError(t, err)
		require.EqualValues(t, 42, row[1].GetInt64())

		rowKVs, rowBytes, err := collectIndexKVsForSampledRow(sctx, physicalTbl, physicalTbl.Indices(), row, kv.IntHandle(1))
		require.NoError(t, err)
		require.Len(t, rowKVs, 1)
		require.Positive(t, rowBytes)
		require.NotEmpty(t, rowKVs[0].rawKey)
	})

	t.Run("sample prediction region selection caps at five", func(t *testing.T) {
		regions := make([]samplePredictionRegion, 0, 8)
		for i := 0; i < 8; i++ {
			regions = append(regions, samplePredictionRegion{
				StartKey: kv.Key{byte(i)},
				EndKey:   kv.Key{byte(i + 1)},
			})
		}
		selected := pickSamplePredictionRegionsWithLimit(regions, 12345, samplePredictionMaxRegionCount)
		require.Len(t, selected, samplePredictionMaxRegionCount)
	})

	t.Run("sample prediction physical table selection caps at five total regions", func(t *testing.T) {
		selections := pickSamplePredictionPhysicalTables([]samplePredictionPhysicalTable{
			{rowCount: 100},
		}, 12345)
		require.Len(t, selections, 1)
		require.Equal(t, samplePredictionMaxRegionCount, selections[0].regionCount)

		selections = pickSamplePredictionPhysicalTables([]samplePredictionPhysicalTable{
			{rowCount: 100},
			{rowCount: 200},
			{rowCount: 300},
			{rowCount: 400},
			{rowCount: 500},
			{rowCount: 600},
			{rowCount: 700},
			{rowCount: 800},
		}, 54321)
		totalSelected := 0
		for _, selection := range selections {
			totalSelected += selection.regionCount
		}
		require.LessOrEqual(t, len(selections), samplePredictionMaxRegionCount)
		require.Equal(t, samplePredictionMaxRegionCount, totalSelected)
	})

	t.Run("block sample prediction bounds", func(t *testing.T) {
		require.Equal(t, blockSamplePredictionProbeRows, blockSamplePredictionTargetRows(10, 1<<30))
		require.Equal(t, blockSamplePredictionMaxRows, blockSamplePredictionTargetRows(10, 10))
		require.Equal(t, blockSamplePredictionMaxRows, blockSamplePredictionTargetRows(10, 0))

		require.Zero(t, maxBlockSamplePredictionSkipRows(samplePredictionRegion{ApproximateKeys: int64(blockSamplePredictionMaxRows)}))
		require.EqualValues(t, 1, maxBlockSamplePredictionSkipRows(samplePredictionRegion{ApproximateKeys: int64(blockSamplePredictionMaxRows + 1)}))
		require.EqualValues(t, samplePredictionMaxSkipRows, maxBlockSamplePredictionSkipRows(samplePredictionRegion{ApproximateKeys: int64(blockSamplePredictionMaxRows + samplePredictionMaxSkipRows + 1)}))
	})

	t.Run("block sample prediction uses scope aware TiKV write CF simulation", func(t *testing.T) {
		ts := uint64(404411537129996288)
		key := []byte("idx-key-00000001")
		writeKey := lightningtikv.EncodeTxnSSTWriteCFKey(key, ts)
		require.Equal(t, byte('z'), writeKey[0])
		require.Equal(t, ^ts, binary.BigEndian.Uint64(writeKey[len(writeKey)-tikvMVCCTimestampBytes:]))
		require.Greater(t, len(writeKey), len(key)+tikvMVCCTimestampBytes)

		writeValue := lightningtikv.EncodeTxnSSTWriteCFValue(ts, []byte("x"), true)
		require.Equal(t, byte('P'), writeValue[0])
		require.Equal(t, byte('v'), writeValue[len(writeValue)-3])
		require.Equal(t, byte(1), writeValue[len(writeValue)-2])
		require.Equal(t, byte('x'), writeValue[len(writeValue)-1])
		emptyWriteValue := lightningtikv.EncodeTxnSSTWriteCFValue(ts, nil, true)
		require.Equal(t, byte('v'), emptyWriteValue[len(emptyWriteValue)-2])
		require.Equal(t, byte(0), emptyWriteValue[len(emptyWriteValue)-1])
		longWriteValue := lightningtikv.EncodeTxnSSTWriteCFValue(ts, nil, false)
		require.Equal(t, byte('P'), longWriteValue[0])
		require.NotContains(t, string(longWriteValue), "v")

		localKVs := []sampledIndexKV{
			{key: []byte("same-index-key"), value: []byte("x"), physicalID: 101, indexID: 7},
			{key: []byte("same-index-key"), value: []byte("x"), physicalID: 102, indexID: 7},
		}
		localScopes := collectSampledIndexKVScopes(localKVs)
		require.Len(t, localScopes, 2)
		localScopeCounts := map[sampledIndexKVScope]int{
			{physicalID: 101, indexID: 7}: 1,
			{physicalID: 102, indexID: 7}: 1,
		}
		localPrediction := estimateBlockSampledIndexKVPredictionBytes(localKVs, localScopeCounts, ts)
		require.NoError(t, localPrediction.Err)
		require.Positive(t, localPrediction.PredictedBytes)

		globalKVs := slices.Clone(localKVs)
		for i := range globalKVs {
			globalKVs[i].isGlobalIndex = true
		}
		globalScopes := collectSampledIndexKVScopes(globalKVs)
		require.Len(t, globalScopes, 1)
		globalScopeCounts := map[sampledIndexKVScope]int{
			{indexID: 7, isGlobalIndex: true}: 1,
		}
		globalPrediction := estimateBlockSampledIndexKVPredictionBytes(globalKVs, globalScopeCounts, ts)
		require.NoError(t, globalPrediction.Err)
		require.Positive(t, globalPrediction.PredictedBytes)
		require.Greater(t, localPrediction.PredictedBytes, globalPrediction.PredictedBytes)

		mvccKVs := buildBlockSampledTiKVMVCCKVs([]sampledIndexKV{
			{key: []byte("empty"), value: nil},
			{key: []byte("short"), value: []byte("short-value")},
			{key: []byte("long"), value: []byte(strings.Repeat("v", tikvMVCCShortValueMaxBytes+1))},
		}, ts)
		require.Len(t, mvccKVs.defaultKVs, 1)
		require.Len(t, mvccKVs.writeKVs, 3)
		require.Equal(t, byte('z'), mvccKVs.writeKVs[0].key[0])
		require.Len(t, mvccKVs.defaultKVs[0].value, tikvMVCCShortValueMaxBytes+1)
	})

	t.Run("block sample prediction estimates next-gen CSE data blocks", func(t *testing.T) {
		ts := uint64(404411537129996288)
		key := []byte("t_index_prefix_000001")
		value := []byte("index-value")
		commonPrefixLen := len("t_index_prefix_")
		entry, err := appendNextGenCSEBlockEntry(nil, sampledIndexKV{key: key, value: value}, commonPrefixLen, ts)
		require.NoError(t, err)
		require.Len(t, entry, nextGenCSEBlockEntrySize(sampledIndexKV{key: key, value: value}, commonPrefixLen))
		require.Equal(t, uint16(len("000001")), binary.LittleEndian.Uint16(entry[:2]))
		pos := 2
		require.Equal(t, []byte("000001"), entry[pos:pos+len("000001")])
		pos += len("000001")
		require.Equal(t, byte(nextGenCSEValueMeta), entry[pos])
		pos++
		require.Equal(t, ts, binary.LittleEndian.Uint64(entry[pos:pos+nextGenCSEValueVersionLen]))
		pos += nextGenCSEValueVersionLen
		require.Equal(t, byte(nextGenCSEValueUserMetaSize), entry[pos])
		pos++
		require.Equal(t, byte(nextGenCSEValueUserMetaFormat), entry[pos])
		require.Equal(t, ts, binary.LittleEndian.Uint64(entry[pos+1:pos+9]))
		require.Equal(t, ts, binary.LittleEndian.Uint64(entry[pos+9:pos+17]))
		pos += nextGenCSEValueUserMetaSize
		require.Equal(t, value, entry[pos:])

		prefixKVs := make([]sampledIndexKV, 0, 2048)
		for i := 0; i < cap(prefixKVs); i++ {
			prefixKVs = append(prefixKVs, sampledIndexKV{
				key:   []byte(fmt.Sprintf("t_index_prefix_payload_%08d", i)),
				value: []byte(strings.Repeat("same-value-", 8)),
			})
		}
		sortedKVs := sortedKVsForTest(prefixKVs)
		nextGenBytes, err := estimateSortedSampledIndexKVCSEPhysicalBytes(sortedKVs, ts)
		require.NoError(t, err)
		require.Positive(t, nextGenBytes)
		require.Less(t, nextGenBytes, sampledIndexKVLogicalBytes(sortedKVs))
		splitNextGenBytes, err := estimateSortedSampledIndexKVCSEPhysicalBytesWithSplit(sortedKVs, 8, ts)
		require.NoError(t, err)
		require.Greater(t, splitNextGenBytes, nextGenBytes)
	})

	t.Run("block sample prediction uses physical estimate for all encodings", func(t *testing.T) {
		ts := uint64(404411537129996288)
		sctx := mock.NewContext()
		intType := types.NewFieldType(mysql.TypeLonglong)
		intType.AddFlag(mysql.NotNullFlag)
		binStringType := types.NewFieldType(mysql.TypeVarString)
		binStringType.SetCharset(charset.CharsetUTF8MB4)
		binStringType.SetCollate(charset.CollationBin)
		restoredStringType := types.NewFieldType(mysql.TypeVarString)
		restoredStringType.SetCharset(charset.CharsetUTF8MB4)
		restoredStringType.SetCollate("utf8mb4_general_ci")
		encodedKeyOf := func(kv sampledIndexKV) []byte { return kv.key }
		rawKeyOf := func(kv sampledIndexKV) []byte { return kv.rawKey }

		numericTblInfo := &model.TableInfo{
			ID:    300,
			State: model.StatePublic,
			Columns: []*model.ColumnInfo{
				{ID: 1, Offset: 0, State: model.StatePublic, FieldType: *intType},
				{ID: 2, Offset: 1, State: model.StatePublic, FieldType: *intType},
				{ID: 3, Offset: 2, State: model.StatePublic, FieldType: *binStringType},
			},
			Indices: []*model.IndexInfo{
				{
					ID:    30,
					State: model.StatePublic,
					Columns: []*model.IndexColumn{
						{Name: ast.NewCIStr("k1"), Offset: 0, Length: types.UnspecifiedLength},
						{Name: ast.NewCIStr("k2"), Offset: 1, Length: types.UnspecifiedLength},
					},
				},
			},
		}
		numericTbl := tables.MockTableFromMeta(numericTblInfo).(table.PhysicalTable)
		numericIdx := numericTbl.Indices()
		var numericKVs []sampledIndexKV
		var numericLogicalBytes int64
		for i := 0; i < 1024; i++ {
			row := []types.Datum{
				types.NewIntDatum(int64(i + 1)),
				types.NewIntDatum(int64((i + 1) * 10)),
				types.NewCollationStringDatum(strings.Repeat("x", 32), charset.CollationBin),
			}
			rowKVs, rowBytes, err := collectIndexKVsForSampledRow(sctx, numericTbl, numericIdx, row, kv.IntHandle(i+1))
			require.NoError(t, err)
			numericKVs = append(numericKVs, rowKVs...)
			numericLogicalBytes += rowBytes
		}
		numericPrediction := estimateBlockSampledIndexKVPredictionBytes(numericKVs, sampleScopeCountsForTest(numericKVs, 1), ts)
		require.NoError(t, numericPrediction.Err)
		require.Positive(t, numericLogicalBytes)
		require.Positive(t, numericPrediction.PredictedBytes)
		require.Positive(t, numericPrediction.MVCCOverheadBytes)
		require.NotEmpty(t, numericKVs[0].rawKey)
		require.Less(t,
			estimateSortedSampledIndexKVSharedPrefixAvg(sortedKVsForTest(numericKVs), rawKeyOf),
			estimateSortedSampledIndexKVSharedPrefixAvg(sortedKVsForTest(numericKVs), encodedKeyOf))
		require.Positive(t, estimateSampledIndexKVRawKeyLengthAvg(numericKVs))

		restoredTblInfo := &model.TableInfo{
			ID:    301,
			State: model.StatePublic,
			Columns: []*model.ColumnInfo{
				{ID: 1, Offset: 0, State: model.StatePublic, FieldType: *intType},
				{ID: 2, Offset: 1, State: model.StatePublic, FieldType: *restoredStringType},
			},
			Indices: []*model.IndexInfo{
				{
					ID:    31,
					State: model.StatePublic,
					Columns: []*model.IndexColumn{
						{Name: ast.NewCIStr("k"), Offset: 0, Length: types.UnspecifiedLength},
						{Name: ast.NewCIStr("pad"), Offset: 1, Length: 32},
					},
				},
			},
		}
		restoredTbl := tables.MockTableFromMeta(restoredTblInfo).(table.PhysicalTable)
		restoredIdx := restoredTbl.Indices()
		var restoredKVs []sampledIndexKV
		var restoredLogicalBytes int64
		for i := 0; i < 1024; i++ {
			row := []types.Datum{
				types.NewIntDatum(int64(((i + 1) * 17) % 1000003)),
				types.NewCollationStringDatum(
					fmt.Sprintf("payload-%09d-%s", i+1, strings.Repeat("x", 96)),
					"utf8mb4_general_ci",
				),
			}
			rowKVs, rowBytes, err := collectIndexKVsForSampledRow(sctx, restoredTbl, restoredIdx, row, kv.IntHandle(i+1))
			require.NoError(t, err)
			restoredKVs = append(restoredKVs, rowKVs...)
			restoredLogicalBytes += rowBytes
		}
		restoredPrediction := estimateBlockSampledIndexKVPredictionBytes(restoredKVs, sampleScopeCountsForTest(restoredKVs, 1), ts)
		require.NoError(t, restoredPrediction.Err)
		require.Positive(t, restoredLogicalBytes)
		require.Positive(t, restoredPrediction.PredictedBytes)

		longValueKVs := []sampledIndexKV{{
			key:   []byte("short-key"),
			value: []byte(strings.Repeat("v", tikvMVCCShortValueMaxBytes+1)),
		}}
		longValuePrediction := estimateBlockSampledIndexKVPredictionBytes(longValueKVs, sampleScopeCountsForTest(longValueKVs, 1), ts)
		require.NoError(t, longValuePrediction.Err)
		longValueMVCCPhysicalBytes, err := estimateBlockSampledIndexKVMVCCPhysicalBytesWithSplit(sortSampledIndexKVs(longValueKVs), 1, ts)
		require.NoError(t, err)
		require.Equal(t, longValueMVCCPhysicalBytes, longValuePrediction.PredictedBytes)
		require.Positive(t, longValuePrediction.MVCCOverheadBytes)

		mixedKVs := append(slices.Clone(numericKVs), restoredKVs...)
		mixedPrediction := estimateBlockSampledIndexKVPredictionBytes(mixedKVs, sampleScopeCountsForTest(mixedKVs, 1), ts)
		require.NoError(t, mixedPrediction.Err)
		require.Positive(t, mixedPrediction.PredictedBytes)

		prefixKVs := make([]sampledIndexKV, 0, 512)
		for i := 0; i < cap(prefixKVs); i++ {
			prefixKVs = append(prefixKVs, sampledIndexKV{
				key:   []byte(fmt.Sprintf("t_index_order_no_000006a2-e4ec-4d93-bbf0-d47edf77ec9f-%07d", i)),
				value: nil,
			})
		}
		prefixPrediction := estimateBlockSampledIndexKVPredictionBytes(prefixKVs, sampleScopeCountsForTest(prefixKVs, 1), ts)
		require.NoError(t, prefixPrediction.Err)
		require.Positive(t, prefixPrediction.MVCCOverheadBytes)
		prefixSplitPrediction := estimateBlockSampledIndexKVPredictionBytes(prefixKVs, sampleScopeCountsForTest(prefixKVs, 5), ts)
		require.NoError(t, prefixSplitPrediction.Err)
		require.Greater(t, prefixSplitPrediction.PredictedBytes, prefixPrediction.PredictedBytes)
		require.Zero(t, estimateSortedSampledIndexKVSharedPrefixAvg(nil, encodedKeyOf))
		require.Zero(t, estimateSortedSampledIndexKVSharedPrefixAvg(sortedKVsForTest([]sampledIndexKV{{key: []byte("single")}}), encodedKeyOf))
		require.InDelta(t, 1.5, estimateSortedSampledIndexKVSharedPrefixAvg(sortedKVsForTest([]sampledIndexKV{
			{key: []byte("b001")},
			{key: []byte("a001")},
			{key: []byte("a000")},
		}), encodedKeyOf), 1e-9)
		require.Greater(t, estimateSortedSampledIndexKVSharedPrefixAvg(sortedKVsForTest(prefixKVs), encodedKeyOf), float64(50))
		require.Zero(t, estimateSortedSampledIndexKVSharedPrefixAvg(nil, rawKeyOf))
		require.Zero(t, estimateSortedSampledIndexKVSharedPrefixAvg(sortedKVsForTest([]sampledIndexKV{{key: []byte("single"), rawKey: []byte("single")}}), rawKeyOf))
		require.Zero(t, estimateSampledIndexKVRawKeyLengthAvg(nil))
		encodedPrefixKVs := []sampledIndexKV{
			{key: []byte("t_index_b001"), rawKey: []byte("b001")},
			{key: []byte("t_index_a001"), rawKey: []byte("a001")},
			{key: []byte("t_index_a000"), rawKey: []byte("a000")},
		}
		require.InDelta(t, 1.5, estimateSortedSampledIndexKVSharedPrefixAvg(sortedKVsForTest(encodedPrefixKVs), rawKeyOf), 1e-9)
		require.InDelta(t, 4, estimateSampledIndexKVRawKeyLengthAvg(encodedPrefixKVs), 1e-9)
		require.Greater(t,
			estimateSortedSampledIndexKVSharedPrefixAvg(sortedKVsForTest(encodedPrefixKVs), encodedKeyOf),
			estimateSortedSampledIndexKVSharedPrefixAvg(sortedKVsForTest(encodedPrefixKVs), rawKeyOf))
	})
}
