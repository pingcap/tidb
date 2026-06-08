// Copyright 2019 PingCAP, Inc.
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

package helper_test

import (
	"bufio"
	"cmp"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/teststore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client/http"
	"go.opencensus.io/stats/view"
	"go.uber.org/zap"
)

// getKeyspaceAwareKey uses the store codec to encode keys properly in NextGen mode
// This ensures compatibility with keyspace encoding
func getKeyspaceAwareKey(store kv.Storage, key []byte) []byte {
	if !kerneltype.IsNextGen() || store == nil {
		return key
	}

	// Use the store's codec to encode the key - single source of truth
	codec := store.GetCodec()
	return codec.EncodeKey(key)
}

func TestHotRegion(t *testing.T) {
	store := createMockStore(t)

	h := helper.Helper{
		Store:       store,
		RegionCache: store.GetRegionCache(),
	}
	regionMetric, err := h.FetchHotRegion(context.Background(), "read")
	require.NoError(t, err)

	expected := map[uint64]helper.RegionMetric{
		2: {
			FlowBytes:    100,
			MaxHotDegree: 1,
			Count:        0,
		},
		4: {
			FlowBytes:    200,
			MaxHotDegree: 2,
			Count:        0,
		},
	}
	require.Equal(t, expected, regionMetric)

	dbInfo := &model.DBInfo{
		Name: ast.NewCIStr("test"),
	}
	require.NoError(t, err)

	res, err := h.FetchRegionTableIndex(regionMetric, infoschema.DBInfoAsInfoSchema([]*model.DBInfo{dbInfo}), nil)
	require.NotEqual(t, res[0].RegionMetric, res[1].RegionMetric)
	require.NoError(t, err)
}

func TestGetRegionsTableInfo(t *testing.T) {
	// Use a nil-store helper so GetTablesInfoWithKeyRange uses V1 codec, matching the
	// hardcoded V1 region keys in getMockTiKVRegionsInfo. Keyspace-aware (V2) behavior
	// is covered by TestGetRegionsTableInfoWithKeyspace.
	h := &helper.Helper{}
	regionsInfo := getMockTiKVRegionsInfo()
	schemas := getMockRegionsTableInfoSchema()
	tableInfos := h.GetRegionsTableInfo(regionsInfo, infoschema.DBInfoAsInfoSchema(schemas), nil)
	require.Equal(t, getRegionsTableInfoAns(schemas), tableInfos)
}

// TestGetRegionsTableInfoWithKeyspace verifies that ParseRegionsTableInfos correctly
// matches region-to-table mappings when both region keys and table key ranges include
// a keyspace prefix (API V2 / keyspace-aware mode).
func TestGetRegionsTableInfoWithKeyspace(t *testing.T) {
	keyspaceID := uint32(1)
	codecV2, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Id:   keyspaceID,
		Name: "test_keyspace",
	})
	require.NoError(t, err)

	schemas := getMockRegionsTableInfoSchema()
	db := schemas[0]
	// Build table info key ranges using the V2 codec (with keyspace prefix).
	tables := make([]helper.TableInfoWithKeyRange, 0, 6)
	for _, table := range db.Deprecated.Tables {
		tables = append(tables, helper.NewTableWithKeyRange(db, table, codecV2))
		for _, index := range table.Indices {
			tables = append(tables, helper.NewIndexWithKeyRange(db, table, index, codecV2))
		}
	}
	// Sort tables by start key to match the production contract expected by ParseRegionsTableInfos.
	slices.SortFunc(tables, func(i, j helper.TableInfoWithKeyRange) int {
		return cmp.Compare(i.StartKey, j.StartKey)
	})

	// Construct mock region info using the keyspace-encoded key ranges.
	// Region 1: ends before all tables (should be empty).
	tbl41 := helper.NewTableWithKeyRange(db, db.Deprecated.Tables[0], codecV2)
	tbl41Idx1 := helper.NewIndexWithKeyRange(db, db.Deprecated.Tables[0], db.Deprecated.Tables[0].Indices[0], codecV2)
	// Region 2: spans table 41's index 1 range and record range.
	tbl63 := helper.NewTableWithKeyRange(db, db.Deprecated.Tables[1], codecV2)
	// Region 3: spans table 63 record range.
	tbl66 := helper.NewTableWithKeyRange(db, db.Deprecated.Tables[2], codecV2)
	// Region 4: spans table 66 record range.
	regions := []*pd.RegionInfo{
		{ID: 1, StartKey: "", EndKey: tbl41Idx1.StartKey},
		{ID: 2, StartKey: tbl41Idx1.StartKey, EndKey: tbl41.EndKey},
		{ID: 3, StartKey: tbl63.StartKey, EndKey: tbl63.EndKey},
		{ID: 4, StartKey: tbl66.StartKey, EndKey: tbl66.EndKey},
	}

	h := &helper.Helper{}
	tableInfos := h.ParseRegionsTableInfos(regions, tables)

	// Region 1 is before all tables — should be empty.
	require.Empty(t, tableInfos[1])
	// Region 2 spans table 41's index and record range.
	require.Len(t, tableInfos[2], 2) // index 1 + record
	require.Equal(t, int64(41), tableInfos[2][0].Table.ID)
	// Region 3 spans table 63.
	require.NotEmpty(t, tableInfos[3])
	require.Equal(t, int64(63), tableInfos[3][0].Table.ID)
	// Region 4 spans table 66.
	require.NotEmpty(t, tableInfos[4])
	require.Equal(t, int64(66), tableInfos[4][0].Table.ID)

	// Verify that V2 keys differ from V1 keys (keyspace prefix is present).
	codecV1 := tikv.NewCodecV1(tikv.ModeTxn)
	tbl41V1 := helper.NewTableWithKeyRange(db, db.Deprecated.Tables[0], codecV1)
	require.NotEqual(t, tbl41.StartKey, tbl41V1.StartKey, "V2 keys should differ from V1 due to keyspace prefix")

	// Verify the public path: GetRegionsTableInfo (wrapping GetTablesInfoWithKeyRange)
	// must produce the same mapping when backed by a V2-codec store. A regression that
	// reverts GetTablesInfoWithKeyRange to V1 encoding would produce a mismatched result.
	regionsInfoForAPI := &pd.RegionsInfo{
		Count:   int64(len(regions)),
		Regions: make([]pd.RegionInfo, len(regions)),
	}
	for i, r := range regions {
		regionsInfoForAPI.Regions[i] = *r
	}
	hV2 := &helper.Helper{Store: &codecOnlyStorage{codec: codecV2}}
	tableInfosViaAPI := hV2.GetRegionsTableInfo(regionsInfoForAPI, infoschema.DBInfoAsInfoSchema(schemas), nil)
	require.Equal(t, tableInfos, tableInfosViaAPI)
}

// TestGetPDRegionStatsKeyspaceEncoding verifies that GetPDRegionStats encodes the table
// key range with the store's codec before querying PD. Without this the request would
// carry a V1 key range and return stats for the wrong set of regions in keyspace-aware
// clusters.
func TestGetPDRegionStatsKeyspaceEncoding(t *testing.T) {
	keyspaceID := uint32(1)
	keyspaceMeta := &keyspacepb.KeyspaceMeta{Id: keyspaceID, Name: "test_keyspace"}
	codecV2, err := tikv.NewCodecV2(tikv.ModeTxn, keyspaceMeta)
	require.NoError(t, err)

	type capturedKeys struct{ start, end []byte }
	captured := make(chan capturedKeys, 1)
	router := mux.NewRouter()
	router.HandleFunc(pd.StatsRegion, func(w http.ResponseWriter, r *http.Request) {
		captured <- capturedKeys{
			start: []byte(r.URL.Query().Get("start_key")),
			end:   []byte(r.URL.Query().Get("end_key")),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"count":0,"empty_region":0,"region_count":0}`))
	})
	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)
	server := httptest.NewServer(serverMux)

	pdAddr := server.URL[len("http://"):]
	pdAddrs := []string{"invalid_pd_address", pdAddr}
	store, err := mockstore.NewMockStore(
		mockstore.WithCurrentKeyspaceMeta(keyspaceMeta),
		mockstore.WithTiKVOptions(tikv.WithPDHTTPClient("pd-stats-test", pdAddrs)),
		mockstore.WithPDAddr(pdAddrs),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		server.Close()
		view.Stop()
		require.NoError(t, store.Close())
	})

	h := &helper.Helper{Store: store.(helper.Storage)}
	_, err = h.GetPDRegionStats(context.Background(), 41, false)
	require.NoError(t, err)

	keys := <-captured
	// The key range sent to PD must match what codecV2.EncodeRegionRange produces for
	// table 41 with noIndexStats=false (uses EncodeTablePrefix, not GenTableRecordPrefix).
	tableStart := tablecodec.EncodeTablePrefix(41)
	tableEnd := tableStart.PrefixNext()
	expectedStart, expectedEnd := codecV2.EncodeRegionRange(tableStart, tableEnd)
	require.Equal(t, expectedStart, keys.start, "GetPDRegionStats must encode start key with the store's codec")
	require.Equal(t, expectedEnd, keys.end, "GetPDRegionStats must encode end key with the store's codec")
}

func TestTiKVRegionsInfo(t *testing.T) {
	store := createMockStore(t)

	h := helper.Helper{
		Store:       store,
		RegionCache: store.GetRegionCache(),
	}
	pdCli, err := h.TryGetPDHTTPClient()
	require.NoError(t, err)
	regionsInfo, err := pdCli.GetRegions(context.Background())
	require.NoError(t, err)
	require.Equal(t, getMockTiKVRegionsInfo(), regionsInfo)
}

func TestTiKVStoresStat(t *testing.T) {
	store := createMockStore(t)

	h := helper.Helper{
		Store:       store,
		RegionCache: store.GetRegionCache(),
	}

	pdCli, err := h.TryGetPDHTTPClient()
	require.NoError(t, err)

	stat, err := pdCli.GetStores(context.Background())
	require.NoError(t, err)

	data, err := json.Marshal(stat)
	require.NoError(t, err)

	expected := `{"count":1,"stores":[{"store":{"id":1,"address":"127.0.0.1:20160","state":0,"state_name":"Up","version":"3.0.0-beta","labels":[{"key":"test","value":"test"}],"status_address":"","git_hash":"","start_timestamp":0},"status":{"capacity":"60 GiB","available":"100 GiB","leader_count":10,"leader_weight":999999.999999,"leader_score":999999.999999,"leader_size":1000,"region_count":200,"region_weight":999999.999999,"region_score":999999.999999,"region_size":1000,"start_ts":"2019-04-23T19:30:30+08:00","last_heartbeat_ts":"2019-04-23T19:31:30+08:00","uptime":"1h30m"}}]}`
	require.Equal(t, expected, string(data))
}

// codecOnlyStorage is a minimal helper.Storage stub whose only working method is
// GetCodec. All other methods panic if called. It is only safe to use with code
// paths that exclusively call GetCodec, such as GetTablesInfoWithKeyRange.
type codecOnlyStorage struct {
	helper.Storage
	codec tikv.Codec
}

func (s *codecOnlyStorage) GetCodec() tikv.Codec { return s.codec }

type mockStore struct {
	helper.Storage
	pdAddrs []string
}

func (s *mockStore) EtcdAddrs() ([]string, error) {
	return s.pdAddrs, nil
}

func (s *mockStore) StartGCWorker() error {
	panic("not implemented")
}

func (s *mockStore) TLSConfig() *tls.Config {
	panic("not implemented")
}

func (s *mockStore) Name() string {
	return "mock store"
}

func (s *mockStore) Describe() string {
	return ""
}

func createMockStore(t *testing.T) (store helper.Storage) {
	server := mockPDHTTPServer()

	pdAddrs := []string{"invalid_pd_address", server.URL[len("http://"):]}

	// Get keyspace-aware region boundary by creating a temp store to access codec
	tempStore, err := teststore.NewMockStoreWithoutBootstrap()
	require.NoError(t, err)
	xKey := getKeyspaceAwareKey(tempStore, []byte("x"))
	tempStore.Close()

	s, err := teststore.NewMockStoreWithoutBootstrap(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithMultiRegions(c, xKey)
		}),
		mockstore.WithTiKVOptions(tikv.WithPDHTTPClient("store-helper-test", pdAddrs)),
		mockstore.WithPDAddr(pdAddrs),
	)
	require.NoError(t, err)

	store = &mockStore{
		s.(helper.Storage),
		pdAddrs,
	}

	t.Cleanup(func() {
		server.Close()
		view.Stop()
		require.NoError(t, store.Close())
	})

	return
}

func mockPDHTTPServer() *httptest.Server {
	router := mux.NewRouter()
	router.HandleFunc(pd.HotRead, mockHotRegionResponse)
	router.HandleFunc(pd.Regions, mockTiKVRegionsInfoResponse)
	router.HandleFunc(pd.Stores, mockStoreStatResponse)
	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)
	return httptest.NewServer(serverMux)
}

func mockHotRegionResponse(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	regionsStat := pd.HotPeersStat{
		Stats: []pd.HotPeerStatShow{
			{
				ByteRate:  100,
				RegionID:  2,
				HotDegree: 1,
			},
			{
				ByteRate:  200,
				RegionID:  4,
				HotDegree: 2,
			},
		},
	}
	resp := pd.StoreHotPeersInfos{
		AsLeader: make(pd.StoreHotPeersStat),
	}
	resp.AsLeader[0] = &regionsStat
	data, err := json.MarshalIndent(resp, "", "	")
	if err != nil {
		log.Panic("json marshal failed", zap.Error(err))
	}
	_, err = w.Write(data)
	if err != nil {
		log.Panic("write http response failed", zap.Error(err))
	}
}

func getMockRegionsTableInfoSchema() []*model.DBInfo {
	dbInfo := &model.DBInfo{Name: ast.NewCIStr("test")}
	dbInfo.Deprecated.Tables = []*model.TableInfo{
		{
			ID:      41,
			Indices: []*model.IndexInfo{{ID: 1}},
		},
		{
			ID:      63,
			Indices: []*model.IndexInfo{{ID: 1}, {ID: 2}},
		},
		{
			ID:      66,
			Indices: []*model.IndexInfo{{ID: 1}, {ID: 2}, {ID: 3}},
		},
	}

	return []*model.DBInfo{dbInfo}
}

func getRegionsTableInfoAns(dbs []*model.DBInfo) map[int64][]helper.TableInfo {
	ans := make(map[int64][]helper.TableInfo)
	db := dbs[0]
	ans[1] = []helper.TableInfo{}
	ans[2] = []helper.TableInfo{
		{db, db.Deprecated.Tables[0], false, nil, true, db.Deprecated.Tables[0].Indices[0]},
		{db, db.Deprecated.Tables[0], false, nil, false, nil},
	}
	ans[3] = []helper.TableInfo{
		{db, db.Deprecated.Tables[1], false, nil, true, db.Deprecated.Tables[1].Indices[0]},
		{db, db.Deprecated.Tables[1], false, nil, true, db.Deprecated.Tables[1].Indices[1]},
		{db, db.Deprecated.Tables[1], false, nil, false, nil},
	}
	ans[4] = []helper.TableInfo{
		{db, db.Deprecated.Tables[2], false, nil, false, nil},
	}
	ans[5] = []helper.TableInfo{
		{db, db.Deprecated.Tables[2], false, nil, true, db.Deprecated.Tables[2].Indices[2]},
		{db, db.Deprecated.Tables[2], false, nil, false, nil},
	}
	ans[6] = []helper.TableInfo{
		{db, db.Deprecated.Tables[2], false, nil, true, db.Deprecated.Tables[2].Indices[0]},
	}
	ans[7] = []helper.TableInfo{
		{db, db.Deprecated.Tables[2], false, nil, true, db.Deprecated.Tables[2].Indices[1]},
	}
	ans[8] = []helper.TableInfo{
		{db, db.Deprecated.Tables[2], false, nil, true, db.Deprecated.Tables[2].Indices[1]},
		{db, db.Deprecated.Tables[2], false, nil, true, db.Deprecated.Tables[2].Indices[2]},
		{db, db.Deprecated.Tables[2], false, nil, false, nil},
	}
	return ans
}

func getMockTiKVRegionsInfo() *pd.RegionsInfo {
	regions := []pd.RegionInfo{
		{
			ID:       1,
			StartKey: "",
			EndKey:   "12341234",
			Epoch: pd.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: []pd.RegionPeer{
				{ID: 2, StoreID: 1},
				{ID: 15, StoreID: 51},
				{ID: 66, StoreID: 99, IsLearner: true},
				{ID: 123, StoreID: 111, IsLearner: true},
			},
			Leader: pd.RegionPeer{
				ID:      2,
				StoreID: 1,
			},
			DownPeers: []pd.RegionPeerStat{
				{
					Peer:    pd.RegionPeer{ID: 66, StoreID: 99, IsLearner: true},
					DownSec: 120,
				},
			},
			PendingPeers: []pd.RegionPeer{
				{ID: 15, StoreID: 51},
			},
			WrittenBytes:    100,
			ReadBytes:       1000,
			ApproximateKeys: 200,
			ApproximateSize: 500,
		},
		// table: 41, record + index: 1
		{
			ID:       2,
			StartKey: "7480000000000000FF295F698000000000FF0000010000000000FA",
			EndKey:   "7480000000000000FF2B5F698000000000FF0000010000000000FA",
			Epoch:    pd.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []pd.RegionPeer{{ID: 3, StoreID: 1}},
			Leader:   pd.RegionPeer{ID: 3, StoreID: 1},
		},
		// table: 63, record + index: 1, 2
		{
			ID:       3,
			StartKey: "7480000000000000FF3F5F698000000000FF0000010000000000FA",
			EndKey:   "7480000000000000FF425F698000000000FF0000010000000000FA",
			Epoch:    pd.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []pd.RegionPeer{{ID: 4, StoreID: 1}},
			Leader:   pd.RegionPeer{ID: 4, StoreID: 1},
		},
		// table: 66, record
		{
			ID:       4,
			StartKey: "7480000000000000FF425F72C000000000FF0000000000000000FA",
			EndKey:   "",
			Epoch:    pd.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []pd.RegionPeer{{ID: 5, StoreID: 1}},
			Leader:   pd.RegionPeer{ID: 5, StoreID: 1},
		},
		// table: 66, record + index: 3
		{
			ID:       5,
			StartKey: "7480000000000000FF425F698000000000FF0000030000000000FA",
			EndKey:   "7480000000000000FF425F72C000000000FF0000000000000000FA",
			Epoch:    pd.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []pd.RegionPeer{{ID: 6, StoreID: 1}},
			Leader:   pd.RegionPeer{ID: 6, StoreID: 1},
		},
		// table: 66, index: 1
		{
			ID:       6,
			StartKey: "7480000000000000FF425F698000000000FF0000010000000000FA",
			EndKey:   "7480000000000000FF425F698000000000FF0000020000000000FA",
			Epoch:    pd.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []pd.RegionPeer{{ID: 7, StoreID: 1}},
			Leader:   pd.RegionPeer{ID: 7, StoreID: 1},
		},
		// table: 66, index: 2
		{
			ID:       7,
			StartKey: "7480000000000000FF425F698000000000FF0000020000000000FA",
			EndKey:   "7480000000000000FF425F698000000000FF0000030000000000FA",
			Epoch:    pd.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []pd.RegionPeer{{ID: 8, StoreID: 1}},
			Leader:   pd.RegionPeer{ID: 8, StoreID: 1},
		},
		// merge region 7, 5
		{
			ID:       8,
			StartKey: "7480000000000000FF425F698000000000FF0000020000000000FA",
			EndKey:   "7480000000000000FF425F72C000000000FF0000000000000000FA",
			Epoch:    pd.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []pd.RegionPeer{{ID: 9, StoreID: 1}},
			Leader:   pd.RegionPeer{ID: 9, StoreID: 1},
		},
	}
	return &pd.RegionsInfo{
		Count:   int64(len(regions)),
		Regions: regions,
	}
}

func mockTiKVRegionsInfoResponse(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	resp := getMockTiKVRegionsInfo()
	data, err := json.MarshalIndent(resp, "", "	")
	if err != nil {
		log.Panic("json marshal failed", zap.Error(err))
	}
	_, err = w.Write(data)
	if err != nil {
		log.Panic("write http response failed", zap.Error(err))
	}
}

func mockStoreStatResponse(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	startTs, err := time.Parse(time.RFC3339, "2019-04-23T19:30:30+08:00")
	if err != nil {
		log.Panic("mock tikv store api response failed", zap.Error(err))
	}
	lastHeartbeatTs, err := time.Parse(time.RFC3339, "2019-04-23T19:31:30+08:00")
	if err != nil {
		log.Panic("mock tikv store api response failed", zap.Error(err))
	}
	storesStat := pd.StoresInfo{
		Count: 1,
		Stores: []pd.StoreInfo{
			{
				Store: pd.MetaStore{
					ID:        1,
					Address:   "127.0.0.1:20160",
					State:     0,
					StateName: "Up",
					Version:   "3.0.0-beta",
					Labels: []pd.StoreLabel{
						{
							Key:   "test",
							Value: "test",
						},
					},
				},
				Status: pd.StoreStatus{
					Capacity:        "60 GiB",
					Available:       "100 GiB",
					LeaderCount:     10,
					LeaderWeight:    999999.999999,
					LeaderScore:     999999.999999,
					LeaderSize:      1000,
					RegionCount:     200,
					RegionWeight:    999999.999999,
					RegionScore:     999999.999999,
					RegionSize:      1000,
					StartTS:         startTs,
					LastHeartbeatTS: lastHeartbeatTs,
					Uptime:          "1h30m",
				},
			},
		},
	}
	data, err := json.MarshalIndent(storesStat, "", "	")
	if err != nil {
		log.Panic("json marshal failed", zap.Error(err))
	}
	_, err = w.Write(data)
	if err != nil {
		log.Panic("write http response failed", zap.Error(err))
	}
}

func TestComputeTiFlashStatus(t *testing.T) {
	regionReplica := make(map[int64]int)
	// There are no region in this TiFlash store.
	br1 := bufio.NewReader(strings.NewReader("0\n\n"))
	// There are 2 regions 1009/1010 in this TiFlash store.
	br2 := bufio.NewReader(strings.NewReader("2\n1009 1010 \n"))
	err := helper.ComputeTiFlashStatus(br1, &regionReplica)
	require.NoError(t, err)
	err = helper.ComputeTiFlashStatus(br2, &regionReplica)
	require.NoError(t, err)
	require.Equal(t, len(regionReplica), 2)
	v, ok := regionReplica[1009]
	require.Equal(t, v, 1)
	require.Equal(t, ok, true)
	v, ok = regionReplica[1010]
	require.Equal(t, v, 1)
	require.Equal(t, ok, true)

	regionReplica2 := make(map[int64]int)
	var sb strings.Builder
	for i := 1000; i < 3000; i++ {
		sb.WriteString(fmt.Sprintf("%v ", i))
	}
	s := fmt.Sprintf("2000\n%v\n", sb.String())
	require.NoError(t, helper.ComputeTiFlashStatus(bufio.NewReader(strings.NewReader(s)), &regionReplica2))
	require.Equal(t, 2000, len(regionReplica2))
	for i := 1000; i < 3000; i++ {
		_, ok := regionReplica2[int64(i)]
		require.True(t, ok)
	}
}

// TestTableRange tests the first part of GetPDRegionStats.
func TestTableRange(t *testing.T) {
	startKey := tablecodec.GenTableRecordPrefix(1)
	endKey := startKey.PrefixNext()
	// t+id+_r
	require.Equal(t, "7480000000000000015f72", startKey.String())
	// t+id+_s
	require.Equal(t, "7480000000000000015f73", endKey.String())

	startKey = tablecodec.EncodeTablePrefix(1)
	endKey = startKey.PrefixNext()
	// t+id
	require.Equal(t, "748000000000000001", startKey.String())
	// t+(id+1)
	require.Equal(t, "748000000000000002", endKey.String())
}
