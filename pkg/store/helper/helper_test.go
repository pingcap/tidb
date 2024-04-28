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
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client/http"
	"go.opencensus.io/stats/view"
	"go.uber.org/zap"
)

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
		Name: model.NewCIStr("test"),
	}
	require.NoError(t, err)

	res, err := h.FetchRegionTableIndex(regionMetric, []*model.DBInfo{dbInfo})
	require.NotEqual(t, res[0].RegionMetric, res[1].RegionMetric)
	require.NoError(t, err)
}

func TestGetRegionsTableInfo(t *testing.T) {
	store := createMockStore(t)

	h := helper.NewHelper(store)
	regionsInfo := getMockTiKVRegionsInfo()
	schemas := getMockRegionsTableInfoSchema()
	tableInfos := h.GetRegionsTableInfo(regionsInfo, schemas)
	require.Equal(t, getRegionsTableInfoAns(schemas), tableInfos)
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
	s, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithMultiRegions(c, []byte("x"))
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
	return []*model.DBInfo{
		{
			Name: model.NewCIStr("test"),
			Tables: []*model.TableInfo{
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
			},
		},
	}
}

func getRegionsTableInfoAns(dbs []*model.DBInfo) map[int64][]helper.TableInfo {
	ans := make(map[int64][]helper.TableInfo)
	db := dbs[0]
	ans[1] = []helper.TableInfo{}
	ans[2] = []helper.TableInfo{
		{db, db.Tables[0], false, nil, true, db.Tables[0].Indices[0]},
		{db, db.Tables[0], false, nil, false, nil},
	}
	ans[3] = []helper.TableInfo{
		{db, db.Tables[1], false, nil, true, db.Tables[1].Indices[0]},
		{db, db.Tables[1], false, nil, true, db.Tables[1].Indices[1]},
		{db, db.Tables[1], false, nil, false, nil},
	}
	ans[4] = []helper.TableInfo{
		{db, db.Tables[2], false, nil, false, nil},
	}
	ans[5] = []helper.TableInfo{
		{db, db.Tables[2], false, nil, true, db.Tables[2].Indices[2]},
		{db, db.Tables[2], false, nil, false, nil},
	}
	ans[6] = []helper.TableInfo{
		{db, db.Tables[2], false, nil, true, db.Tables[2].Indices[0]},
	}
	ans[7] = []helper.TableInfo{
		{db, db.Tables[2], false, nil, true, db.Tables[2].Indices[1]},
	}
	ans[8] = []helper.TableInfo{
		{db, db.Tables[2], false, nil, true, db.Tables[2].Indices[1]},
		{db, db.Tables[2], false, nil, true, db.Tables[2].Indices[2]},
		{db, db.Tables[2], false, nil, false, nil},
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
