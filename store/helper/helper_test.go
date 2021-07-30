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
	"fmt"
	"net/http"
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
	go s.mockPDHTTPServer(c)
	time.Sleep(100 * time.Millisecond)
	mockTikvStore, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(func() *mocktikv.Cluster {
			cluster := mocktikv.NewCluster()
			mocktikv.BootstrapWithMultiRegions(cluster, []byte("x"))
			return cluster
		}()),
	)
	s.store = &mockStore{
		mockTikvStore.(tikv.Storage),
		[]string{"127.0.0.1:10100/"},
	}
	c.Assert(err, IsNil)
}

func (s *HelperTestSuite) TestHotRegion(c *C) {
	helper := helper.Helper{
		Store:       s.store,
		RegionCache: s.store.GetRegionCache(),
	}
	regionMetric, err := helper.FetchHotRegion(pdapi.HotRead)
	c.Assert(err, IsNil, Commentf("err: %+v", err))
	dbInfo := &model.DBInfo{
		Name: model.NewCIStr("test"),
	}
	c.Assert(fmt.Sprintf("%v", regionMetric), Equals, "map[3:{100 1 0} 4:{200 2 0}]")
	res, err := helper.FetchRegionTableIndex(regionMetric, []*model.DBInfo{dbInfo})
	c.Assert(err, IsNil, Commentf("err: %+v", err))
	c.Assert(res[0].RegionMetric, Not(Equals), res[1].RegionMetric)
}

func (s *HelperTestSuite) TestTiKVRegionsInfo(c *C) {
	h := helper.Helper{
		Store:       s.store,
		RegionCache: s.store.GetRegionCache(),
	}
	regionsInfo, err := h.GetRegionsInfo()
	c.Assert(err, IsNil, Commentf("err: %+v", err))
	c.Assert(fmt.Sprintf("%v", regionsInfo), Equals, "&{1 [{1 test testtest {1 1} [{2 1 false}] {2 1 false} [] [] 100 1000 500 200}]}")
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
	c.Assert(fmt.Sprintf("%s", data), Equals, "{\"count\":1,\"stores\":[{\"store\":{\"id\":1,\"address\":\"127.0.0.1:20160\",\"state\":0,\"state_name\":\"Up\",\"version\":\"3.0.0-beta\",\"labels\":[{\"key\":\"test\",\"value\":\"test\"}]},\"status\":{\"capacity\":\"60 GiB\",\"available\":\"100 GiB\",\"leader_count\":10,\"leader_weight\":1,\"leader_score\":1000,\"leader_size\":1000,\"region_count\":200,\"region_weight\":1,\"region_score\":1000,\"region_size\":1000,\"start_ts\":\"2019-04-23T19:30:30+08:00\",\"last_heartbeat_ts\":\"2019-04-23T19:31:30+08:00\",\"uptime\":\"1h30m\"}}]}")
}

func (s *HelperTestSuite) mockPDHTTPServer(c *C) {
	router := mux.NewRouter()
	router.HandleFunc(pdapi.HotRead, s.mockHotRegionResponse)
	router.HandleFunc(pdapi.Regions, s.mockTiKVRegionsInfoResponse)
	router.HandleFunc(pdapi.Stores, s.mockStoreStatResponse)
	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)
	server := &http.Server{Addr: "127.0.0.1:10100", Handler: serverMux}
	err := server.ListenAndServe()
	c.Assert(err, IsNil)
}

func (s *HelperTestSuite) mockHotRegionResponse(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	regionsStat := helper.HotRegionsStat{
		RegionsStat: []helper.RegionStat{
			{
				FlowBytes: 100,
				RegionID:  3,
				HotDegree: 1,
			},
			{
				FlowBytes: 200,
				RegionID:  4,
				HotDegree: 2,
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

func (s *HelperTestSuite) mockTiKVRegionsInfoResponse(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	resp := helper.RegionsInfo{
		Count: 1,
		Regions: []helper.RegionInfo{
			{
				ID:       1,
				StartKey: "test",
				EndKey:   "testtest",
				Epoch: helper.RegionEpoch{
					ConfVer: 1,
					Version: 1,
				},
				Peers: []helper.RegionPeer{
					{
						ID:        2,
						StoreID:   1,
						IsLearner: false,
					},
				},
				Leader: helper.RegionPeer{
					ID:        2,
					StoreID:   1,
					IsLearner: false,
				},
				DownPeers:       nil,
				PendingPeers:    nil,
				WrittenBytes:    100,
				ReadBytes:       1000,
				ApproximateKeys: 200,
				ApproximateSize: 500,
			},
		},
	}
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
					LeaderWeight:    1,
					LeaderScore:     1000,
					LeaderSize:      1000,
					RegionCount:     200,
					RegionWeight:    1,
					RegionScore:     1000,
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
