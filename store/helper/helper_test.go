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
// See the License for the specific language governing permissions and
// limitations under the License.

package helper_test

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/pdapi"
	"go.uber.org/zap"
)

type HelperTestSuite struct {
	store tikv.Storage
}

var _ = Suite(new(HelperTestSuite))

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type mockStore struct {
	tikv.Storage
	pdAddrs []string
}

func (s *mockStore) EtcdAddrs() []string {
	return s.pdAddrs
}

func (s *mockStore) StartGCWorker() error {
	panic("not implemented")
}

func (s *mockStore) TLSConfig() *tls.Config {
	panic("not implemented")
}

func (s *HelperTestSuite) SetUpSuite(c *C) {
	url := s.mockPDHTTPServer(c)
	time.Sleep(100 * time.Millisecond)
	mvccStore := mocktikv.MustNewMVCCStore()
	mockTikvStore, err := mockstore.NewMockTikvStore(mockstore.WithMVCCStore(mvccStore))
	s.store = &mockStore{
		mockTikvStore.(tikv.Storage),
		[]string{url[len("http://"):]},
	}
	c.Assert(err, IsNil)
}

func (s *HelperTestSuite) TestHotRegion(c *C) {
	h := helper.Helper{
		Store:       s.store,
		RegionCache: s.store.GetRegionCache(),
	}
	regionMetric, err := h.FetchHotRegion(pdapi.HotRead)
	c.Assert(err, IsNil, Commentf("err: %+v", err))
	expected := make(map[uint64]helper.RegionMetric)
	expected[1] = helper.RegionMetric{
		FlowBytes:    100,
		MaxHotDegree: 1,
		Count:        0,
	}
	c.Assert(regionMetric, DeepEquals, expected)
	dbInfo := &model.DBInfo{
		Name: model.NewCIStr("test"),
	}
	c.Assert(err, IsNil)
	_, err = h.FetchRegionTableIndex(regionMetric, []*model.DBInfo{dbInfo})
	c.Assert(err, IsNil, Commentf("err: %+v", err))
}

func (s *HelperTestSuite) TestGetRegionsTableInfo(c *C) {
	h := helper.NewHelper(s.store)
	regionsInfo := getMockTiKVRegionsInfo()
	schemas := getMockRegionsTableInfoSchema()
	tableInfos := h.GetRegionsTableInfo(regionsInfo, schemas)
	c.Assert(tableInfos, DeepEquals, getRegionsTableInfoAns(schemas))
}

func (s *HelperTestSuite) TestTiKVRegionsInfo(c *C) {
	h := helper.Helper{
		Store:       s.store,
		RegionCache: s.store.GetRegionCache(),
	}
	regionsInfo, err := h.GetRegionsInfo()
	c.Assert(err, IsNil, Commentf("err: %+v", err))
	c.Assert(regionsInfo, DeepEquals, getMockTiKVRegionsInfo())
}

func (s *HelperTestSuite) TestTiKVStoresStat(c *C) {
	h := helper.Helper{
		Store:       s.store,
		RegionCache: s.store.GetRegionCache(),
	}
	stat, err := h.GetStoresStat()
	c.Assert(err, IsNil, Commentf("err: %+v", err))
	data, err := json.Marshal(stat)
	c.Assert(err, IsNil)
	c.Assert(string(data), Equals, `{"count":1,"stores":[{"store":{"id":1,"address":"127.0.0.1:20160","state":0,"state_name":"Up","version":"3.0.0-beta","labels":[{"key":"test","value":"test"}],"status_address":"","git_hash":"","start_timestamp":0},"status":{"capacity":"60 GiB","available":"100 GiB","leader_count":10,"leader_weight":999999.999999,"leader_score":999999.999999,"leader_size":1000,"region_count":200,"region_weight":999999.999999,"region_score":999999.999999,"region_size":1000,"start_ts":"2019-04-23T19:30:30+08:00","last_heartbeat_ts":"2019-04-23T19:31:30+08:00","uptime":"1h30m"}}]}`)
}

func (s *HelperTestSuite) mockPDHTTPServer(c *C) (url string) {
	router := mux.NewRouter()
	router.HandleFunc(pdapi.HotRead, s.mockHotRegionResponse)
	router.HandleFunc(pdapi.Regions, s.mockTiKVRegionsInfoResponse)
	router.HandleFunc(pdapi.Stores, s.mockStoreStatResponse)
	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)
	server := httptest.NewServer(serverMux)
	return server.URL
}

func (s *HelperTestSuite) mockHotRegionResponse(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	regionsStat := helper.HotRegionsStat{
		RegionsStat: []helper.RegionStat{
			{
				FlowBytes: 100,
				RegionID:  1,
				HotDegree: 1,
			},
		},
	}
	resp := helper.StoreHotRegionInfos{
		AsLeader: make(map[uint64]*helper.HotRegionsStat),
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
		{db, db.Tables[0], true, db.Tables[0].Indices[0]},
		{db, db.Tables[0], false, nil},
	}
	ans[3] = []helper.TableInfo{
		{db, db.Tables[1], true, db.Tables[1].Indices[0]},
		{db, db.Tables[1], true, db.Tables[1].Indices[1]},
		{db, db.Tables[1], false, nil},
	}
	ans[4] = []helper.TableInfo{
		{db, db.Tables[2], false, nil},
	}
	ans[5] = []helper.TableInfo{
		{db, db.Tables[2], true, db.Tables[2].Indices[2]},
		{db, db.Tables[2], false, nil},
	}
	ans[6] = []helper.TableInfo{
		{db, db.Tables[2], true, db.Tables[2].Indices[0]},
	}
	ans[7] = []helper.TableInfo{
		{db, db.Tables[2], true, db.Tables[2].Indices[1]},
	}
	ans[8] = []helper.TableInfo{
		{db, db.Tables[2], true, db.Tables[2].Indices[1]},
		{db, db.Tables[2], true, db.Tables[2].Indices[2]},
		{db, db.Tables[2], false, nil},
	}
	return ans
}

func getMockTiKVRegionsInfo() *helper.RegionsInfo {
	regions := []helper.RegionInfo{
		{
			ID:       1,
			StartKey: "",
			EndKey:   "12341234",
			Epoch: helper.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: []helper.RegionPeer{
				{ID: 2, StoreID: 1},
				{ID: 15, StoreID: 51},
				{ID: 66, StoreID: 99, IsLearner: true},
				{ID: 123, StoreID: 111, IsLearner: true},
			},
			Leader: helper.RegionPeer{
				ID:      2,
				StoreID: 1,
			},
			DownPeers: []helper.RegionPeerStat{
				{
					helper.RegionPeer{ID: 66, StoreID: 99, IsLearner: true},
					120,
				},
			},
			PendingPeers: []helper.RegionPeer{
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
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 3, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 3, StoreID: 1},
		},
		// table: 63, record + index: 1, 2
		{
			ID:       3,
			StartKey: "7480000000000000FF3F5F698000000000FF0000010000000000FA",
			EndKey:   "7480000000000000FF425F698000000000FF0000010000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 4, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 4, StoreID: 1},
		},
		// table: 66, record
		{
			ID:       4,
			StartKey: "7480000000000000FF425F72C000000000FF0000000000000000FA",
			EndKey:   "",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 5, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 5, StoreID: 1},
		},
		// table: 66, record + index: 3
		{
			ID:       5,
			StartKey: "7480000000000000FF425F698000000000FF0000030000000000FA",
			EndKey:   "7480000000000000FF425F72C000000000FF0000000000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 6, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 6, StoreID: 1},
		},
		// table: 66, index: 1
		{
			ID:       6,
			StartKey: "7480000000000000FF425F698000000000FF0000010000000000FA",
			EndKey:   "7480000000000000FF425F698000000000FF0000020000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 7, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 7, StoreID: 1},
		},
		// table: 66, index: 2
		{
			ID:       7,
			StartKey: "7480000000000000FF425F698000000000FF0000020000000000FA",
			EndKey:   "7480000000000000FF425F698000000000FF0000030000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 8, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 8, StoreID: 1},
		},
		// merge region 7, 5
		{
			ID:       8,
			StartKey: "7480000000000000FF425F698000000000FF0000020000000000FA",
			EndKey:   "7480000000000000FF425F72C000000000FF0000000000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 9, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 9, StoreID: 1},
		},
	}
	return &helper.RegionsInfo{
		Count:   int64(len(regions)),
		Regions: regions,
	}
}

func (s *HelperTestSuite) mockTiKVRegionsInfoResponse(w http.ResponseWriter, req *http.Request) {
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

func (s *HelperTestSuite) mockStoreStatResponse(w http.ResponseWriter, req *http.Request) {
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
	storesStat := helper.StoresStat{
		Count: 1,
		Stores: []helper.StoreStat{
			{
				Store: helper.StoreBaseStat{
					ID:        1,
					Address:   "127.0.0.1:20160",
					State:     0,
					StateName: "Up",
					Version:   "3.0.0-beta",
					Labels: []helper.StoreLabel{
						{
							Key:   "test",
							Value: "test",
						},
					},
				},
				Status: helper.StoreDetailStat{
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
					StartTs:         startTs,
					LastHeartbeatTs: lastHeartbeatTs,
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
