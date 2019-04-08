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

package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/pdapi"
	"go.uber.org/zap"
)

var errStopped = errors.New("stopped")

// For MVCC test
type DebugMVCCStore struct {
	mocktikv.MVCCStore
}

func (d DebugMVCCStore) MvccGetByStartTS(startKey, endKey []byte, starTS uint64) (*kvrpcpb.MvccInfo, []byte) {
	// TODO: add mock codes
	return new(kvrpcpb.MvccInfo), nil
}

func (d DebugMVCCStore) MvccGetByKey(key []byte) *kvrpcpb.MvccInfo {
	// TODO: add mock codes
	return nil
}

// For PD test
type mockBackend struct {
	tikv.Storage
	pdAddrs []string
}

func (m *mockBackend) EtcdAddrs() []string {
	return m.pdAddrs
}

// For PD test
func mockPDHTTPServer() *http.Server {
	router := mux.NewRouter()
	router.HandleFunc(pdapi.HotRead, mockHotRegionResponse)
	router.HandleFunc(pdapi.HotWrite, mockHotRegionResponse)
	router.HandleFunc(pdapi.Regions, mockTiKVRegionsInfoResponse)
	router.HandleFunc(pdapi.Stores, mockStoreStatResponse)
	router.HandleFunc("/pd/api/v1/stats/region", mockStatsRegionResponse)
	router.HandleFunc("/pd/api/v1/schedulers", mockScatterRegionsResponse).Methods(http.MethodPost)
	router.
		HandleFunc("/pd/api/v1/schedulers/scatter-range-{name}", mockDeleteScatterResponse).
		Methods(http.MethodDelete)
	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)
	server := &http.Server{Addr: "127.0.0.1:10100", Handler: serverMux}
	return server
}

func mockScatterRegionsResponse(w http.ResponseWriter, req *http.Request) {
	var input map[string]interface{}
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Panic("read request body failed", zap.Error(err))
	}

	err = json.Unmarshal(b, &input)
	if err != nil {
		log.Panic("json unmarshal failed", zap.Error(err))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}

func mockDeleteScatterResponse(w http.ResponseWriter, req *http.Request) {
	name := mux.Vars(req)["name"]
	parts := strings.Split(name, "-")
	if len(parts) > 2 {
		log.Panic("invalid name: " + name)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}

func mockHotRegionResponse(w http.ResponseWriter, req *http.Request) {
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
		AsLeader: make(map[uint64]*helper.HotRegionsStat, 1),
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

func mockStatsRegionResponse(w http.ResponseWriter, req *http.Request) {
	mockStats := pdRegionStats{
		Count:            4,
		EmptyCount:       1,
		StorageSize:      351,
		StorageKeys:      221,
		StoreLeaderCount: map[uint64]int{1: 1, 4: 2, 5: 1},
		StorePeerCount:   map[uint64]int{1: 3, 2: 1, 3: 1, 4: 2, 5: 2},
		StoreLeaderSize:  map[uint64]int64{1: 100, 4: 250, 5: 1},
		StoreLeaderKeys:  map[uint64]int64{1: 50, 4: 170, 5: 1},
		StorePeerSize:    map[uint64]int64{1: 301, 2: 100, 3: 100, 4: 250, 5: 201},
		StorePeerKeys:    map[uint64]int64{1: 201, 2: 50, 3: 50, 4: 170, 5: 151},
	}
	data, err := json.MarshalIndent(mockStats, "", "	")
	if err != nil {
		log.Panic("json marshal failed", zap.Error(err))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(data)
	if err != nil {
		log.Panic("write http response failed", zap.Error(err))
	}
}

func mockTiKVRegionsInfoResponse(w http.ResponseWriter, req *http.Request) {
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

func mockStoreStatResponse(w http.ResponseWriter, req *http.Request) {
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
