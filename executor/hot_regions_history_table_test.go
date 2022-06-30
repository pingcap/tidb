// Copyright 2022 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/fn"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/stretchr/testify/require"
)

type mockStoreWithMultiPD struct {
	helper.Storage
	hosts []string
}

var hotRegionsResponses = make(map[string]*executor.HistoryHotRegions, 3)

func (s *mockStoreWithMultiPD) EtcdAddrs() ([]string, error) { return s.hosts, nil }
func (s *mockStoreWithMultiPD) TLSConfig() *tls.Config       { panic("not implemented") }
func (s *mockStoreWithMultiPD) StartGCWorker() error         { panic("not implemented") }
func (s *mockStoreWithMultiPD) Name() string                 { return "mockStore" }
func (s *mockStoreWithMultiPD) Describe() string             { return "" }

type hotRegionsHistoryTableSuite struct {
	store       kv.Storage
	clean       func()
	httpServers []*httptest.Server
	startTime   time.Time
}

func createHotRegionsHistoryTableSuite(t *testing.T) *hotRegionsHistoryTableSuite {
	var clean func()

	s := new(hotRegionsHistoryTableSuite)
	s.store, clean = testkit.CreateMockStore(t)
	store := &mockStoreWithMultiPD{
		s.store.(helper.Storage),
		make([]string, 3),
	}
	// start 3 PD server with hotRegionsServer and store them in s.store
	for i := 0; i < 3; i++ {
		httpServer, mockAddr := s.setUpMockPDHTTPServer()
		require.NotNil(t, httpServer)
		s.httpServers = append(s.httpServers, httpServer)
		store.hosts[i] = mockAddr
	}
	s.store = store
	s.startTime = time.Now()
	s.clean = func() {
		for _, server := range s.httpServers {
			server.Close()
		}
		clean()
	}
	return s
}

func writeResp(w http.ResponseWriter, resp interface{}) {
	w.WriteHeader(http.StatusOK)
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "unable to marshal resp", err)
		return
	}
	_, _ = w.Write(jsonResp)
}

func writeJSONError(w http.ResponseWriter, code int, prefix string, err error) {
	type errorResponse struct {
		Error string `json:"error"`
	}
	w.WriteHeader(code)
	if err != nil {
		prefix += ": " + err.Error()
	}
	_ = json.NewEncoder(w).Encode(errorResponse{Error: prefix})
}

func hisHotRegionsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "unable to read req", err)
		return
	}
	_ = r.Body.Close()
	req := &executor.HistoryHotRegionsRequest{}
	err = json.Unmarshal(data, req)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "unable to serialize req", err)
		return
	}
	resp := &executor.HistoryHotRegions{}
	for _, typ := range req.HotRegionTypes {
		resp.HistoryHotRegion = append(resp.HistoryHotRegion, hotRegionsResponses[typ+r.Host].HistoryHotRegion...)
	}
	w.WriteHeader(http.StatusOK)
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "unable to marshal resp", err)
		return
	}
	_, _ = w.Write(jsonResp)
}

func (s *hotRegionsHistoryTableSuite) setUpMockPDHTTPServer() (*httptest.Server, string) {
	// mock PD http server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	mockAddr := strings.TrimPrefix(server.URL, "http://")
	// mock PD API
	router.Handle(pdapi.Status, fn.Wrap(func() (interface{}, error) {
		return struct {
			Version        string `json:"version"`
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{
			Version:        "4.0.0-alpha",
			GitHash:        "mock-pd-githash",
			StartTimestamp: s.startTime.Unix(),
		}, nil
	}))
	// mock history hot regions response
	router.HandleFunc(pdapi.HotHistory, hisHotRegionsHandler)
	return server, mockAddr
}

func TestTiDBHotRegionsHistory(t *testing.T) {
	s := createHotRegionsHistoryTableSuite(t)
	defer s.clean()

	var unixTimeMs = func(v string) int64 {
		tt, err := time.ParseInLocation("2006-01-02 15:04:05", v, time.Local)
		require.NoError(t, err)
		return tt.UnixNano() / int64(time.Millisecond)
	}

	tk := testkit.NewTestKit(t, s.store)
	tablesPrivTid := external.GetTableByName(t, tk, "mysql", "TABLES_PRIV").Meta().ID
	tablesPrivTidStr := strconv.FormatInt(tablesPrivTid, 10)
	statsMetaTid := external.GetTableByName(t, tk, "mysql", "STATS_META").Meta().ID
	statsMetaTidStr := strconv.FormatInt(statsMetaTid, 10)

	fullHotRegions := [][]string{
		// mysql table_id = 11, table_name = TABLES_PRIV
		{"2019-10-10 10:10:11", "MYSQL", "TABLES_PRIV", tablesPrivTidStr, "<nil>", "<nil>", "1", "1", "11111", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:12", "MYSQL", "TABLES_PRIV", tablesPrivTidStr, "<nil>", "<nil>", "2", "2", "22222", "0", "0", "WRITE", "99", "99", "99", "99"},
		// mysql table_id = 21, table_name = STATS_META
		{"2019-10-10 10:10:13", "MYSQL", "STATS_META", statsMetaTidStr, "<nil>", "<nil>", "3", "3", "33333", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:14", "MYSQL", "STATS_META", statsMetaTidStr, "<nil>", "<nil>", "4", "4", "44444", "0", "0", "WRITE", "99", "99", "99", "99"},
		// table_id = 1313, deleted schema
		{"2019-10-10 10:10:15", "UNKNOWN", "UNKNOWN", "1313", "UNKNOWN", "<nil>", "5", "5", "55555", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:16", "UNKNOWN", "UNKNOWN", "1313", "UNKNOWN", "<nil>", "6", "6", "66666", "0", "0", "WRITE", "99", "99", "99", "99"},
		// mysql table_id = 11, index_id = 1, table_name = TABLES_PRIV, index_name = PRIMARY
		{"2019-10-10 10:10:17", "MYSQL", "TABLES_PRIV", tablesPrivTidStr, "PRIMARY", "1", "1", "1", "11111", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:18", "MYSQL", "TABLES_PRIV", tablesPrivTidStr, "PRIMARY", "1", "2", "2", "22222", "0", "0", "WRITE", "99", "99", "99", "99"},
		// mysql table_id = 21 ,index_id = 1, table_name = STATS_META, index_name = IDX_VER
		{"2019-10-10 10:10:19", "MYSQL", "STATS_META", statsMetaTidStr, "IDX_VER", "1", "3", "3", "33333", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:20", "MYSQL", "STATS_META", statsMetaTidStr, "IDX_VER", "1", "4", "4", "44444", "0", "0", "WRITE", "99", "99", "99", "99"},
		// mysql table_id = 21 ,index_id = 2, table_name = STATS_META, index_name = TBL
		{"2019-10-10 10:10:21", "MYSQL", "STATS_META", statsMetaTidStr, "TBL", "2", "5", "5", "55555", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:22", "MYSQL", "STATS_META", statsMetaTidStr, "TBL", "2", "6", "6", "66666", "0", "0", "WRITE", "99", "99", "99", "99"},
		// table_id = 1313, index_id = 1, deleted schema
		{"2019-10-10 10:10:23", "UNKNOWN", "UNKNOWN", "1313", "UNKNOWN", "1", "7", "7", "77777", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:24", "UNKNOWN", "UNKNOWN", "1313", "UNKNOWN", "1", "8", "8", "88888", "0", "0", "WRITE", "99", "99", "99", "99"},
	}

	mockDB := &model.DBInfo{}
	pdResps := []map[string]*executor.HistoryHotRegions{
		{
			core.HotRegionTypeRead: {
				HistoryHotRegion: []*executor.HistoryHotRegion{
					// mysql table_id = 11, table_name = TABLES_PRIV
					{UpdateTime: unixTimeMs("2019-10-10 10:10:11"), RegionID: 1, StoreID: 1, PeerID: 11111, IsLearner: false,
						IsLeader: true, HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: tablesPrivTid}).StartKey,
						EndKey:   helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: tablesPrivTid}).EndKey,
					},
					// mysql table_id = 21, table_name = STATS_META
					{UpdateTime: unixTimeMs("2019-10-10 10:10:13"), RegionID: 3, StoreID: 3, PeerID: 33333, IsLearner: false,
						IsLeader: true, HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: statsMetaTid}).StartKey,
						EndKey:   helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: statsMetaTid}).EndKey,
					},
				},
			},
			core.HotRegionTypeWrite: {
				HistoryHotRegion: []*executor.HistoryHotRegion{
					// mysql table_id = 11, table_name = TABLES_PRIV
					{UpdateTime: unixTimeMs("2019-10-10 10:10:12"), RegionID: 2, StoreID: 2, PeerID: 22222, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: tablesPrivTid}).StartKey,
						EndKey:   helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: tablesPrivTid}).EndKey,
					},
					// mysql table_id = 21, table_name = STATS_META
					{UpdateTime: unixTimeMs("2019-10-10 10:10:14"), RegionID: 4, StoreID: 4, PeerID: 44444, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: statsMetaTid}).StartKey,
						EndKey:   helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: statsMetaTid}).EndKey,
					},
				},
			},
		},
		{
			core.HotRegionTypeRead: {
				HistoryHotRegion: []*executor.HistoryHotRegion{
					// table_id = 1313, deleted schema
					{UpdateTime: unixTimeMs("2019-10-10 10:10:15"), RegionID: 5, StoreID: 5, PeerID: 55555, IsLearner: false,
						IsLeader: true, HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 1313}).StartKey,
						EndKey:   helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 1313}).EndKey,
					},
					// mysql table_id = 11, index_id = 1, table_name = TABLES_PRIV, index_name = PRIMARY
					{UpdateTime: unixTimeMs("2019-10-10 10:10:17"), RegionID: 1, StoreID: 1, PeerID: 11111, IsLearner: false,
						IsLeader: true, HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: tablesPrivTid}, &model.IndexInfo{ID: 1}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: tablesPrivTid}, &model.IndexInfo{ID: 1}).EndKey,
					},
				},
			},
			core.HotRegionTypeWrite: {
				HistoryHotRegion: []*executor.HistoryHotRegion{
					// table_id = 1313, deleted schema
					{UpdateTime: unixTimeMs("2019-10-10 10:10:16"), RegionID: 6, StoreID: 6, PeerID: 66666, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 1313}).StartKey,
						EndKey:   helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 1313}).EndKey,
					},
					// mysql table_id = 11, index_id = 1, table_name = TABLES_PRIV, index_name = PRIMARY
					{UpdateTime: unixTimeMs("2019-10-10 10:10:18"), RegionID: 2, StoreID: 2, PeerID: 22222, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: tablesPrivTid}, &model.IndexInfo{ID: 1}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: tablesPrivTid}, &model.IndexInfo{ID: 1}).EndKey,
					},
				},
			},
		},
		{
			core.HotRegionTypeRead: {
				HistoryHotRegion: []*executor.HistoryHotRegion{
					// mysql table_id = 21 ,index_id = 1, table_name = STATS_META, index_name = IDX_VER
					{UpdateTime: unixTimeMs("2019-10-10 10:10:19"), RegionID: 3, StoreID: 3, PeerID: 33333, IsLearner: false,
						IsLeader: true, HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: statsMetaTid}, &model.IndexInfo{ID: 1}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: statsMetaTid}, &model.IndexInfo{ID: 1}).EndKey,
					},
					// mysql table_id = 21 ,index_id = 2, table_name = STATS_META, index_name = TBL
					{UpdateTime: unixTimeMs("2019-10-10 10:10:21"), RegionID: 5, StoreID: 5, PeerID: 55555, IsLearner: false,
						IsLeader: true, HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: statsMetaTid}, &model.IndexInfo{ID: 2}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: statsMetaTid}, &model.IndexInfo{ID: 2}).EndKey,
					},
					//      table_id = 1313, index_id = 1, deleted schema
					{UpdateTime: unixTimeMs("2019-10-10 10:10:23"), RegionID: 7, StoreID: 7, PeerID: 77777, IsLeader: true,
						HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 1313}, &model.IndexInfo{ID: 1}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 1313}, &model.IndexInfo{ID: 1}).EndKey,
					},
				},
			},
			core.HotRegionTypeWrite: {
				HistoryHotRegion: []*executor.HistoryHotRegion{
					// mysql table_id = 21 ,index_id = 1, table_name = STATS_META, index_name = IDX_VER
					{UpdateTime: unixTimeMs("2019-10-10 10:10:20"), RegionID: 4, StoreID: 4, PeerID: 44444, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: statsMetaTid}, &model.IndexInfo{ID: 1}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: statsMetaTid}, &model.IndexInfo{ID: 1}).EndKey,
					},
					// mysql table_id = 21 ,index_id = 2, table_name = STATS_META, index_name = TBL
					{UpdateTime: unixTimeMs("2019-10-10 10:10:22"), RegionID: 6, StoreID: 6, PeerID: 66666, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: statsMetaTid}, &model.IndexInfo{ID: 2}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: statsMetaTid}, &model.IndexInfo{ID: 2}).EndKey,
					},
					//      table_id = 1313, index_id = 1, deleted schema
					{UpdateTime: unixTimeMs("2019-10-10 10:10:24"), RegionID: 8, StoreID: 8, PeerID: 88888, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 1313}, &model.IndexInfo{ID: 1}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 1313}, &model.IndexInfo{ID: 1}).EndKey,
					},
				},
			},
		},
	}

	var cases = []struct {
		conditions []string
		reqCount   int32
		expected   [][]string
	}{
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
			}, // time filtered by PD, assume response suit time range, and ignore deleted schemas
			expected: [][]string{
				fullHotRegions[0], fullHotRegions[1], fullHotRegions[2],
				fullHotRegions[3],
				fullHotRegions[6], fullHotRegions[7], fullHotRegions[8],
				fullHotRegions[9], fullHotRegions[10], fullHotRegions[11],
			},
		},
		{
			conditions: []string{
				"update_time>=TIMESTAMP('2019-10-10 10:10:10')",
				"update_time<=TIMESTAMP('2019-10-11 10:10:10')",
			}, // test support of timestamp
			expected: [][]string{
				fullHotRegions[0], fullHotRegions[1], fullHotRegions[2],
				fullHotRegions[3],
				fullHotRegions[6], fullHotRegions[7], fullHotRegions[8],
				fullHotRegions[9], fullHotRegions[10], fullHotRegions[11],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_id=" + tablesPrivTidStr,
			},
			expected: [][]string{
				fullHotRegions[0], fullHotRegions[1], fullHotRegions[6], fullHotRegions[7],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_name='TABLES_PRIV'",
			},
			expected: [][]string{
				fullHotRegions[0], fullHotRegions[1], fullHotRegions[6], fullHotRegions[7],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_id=" + statsMetaTidStr,
				"index_id=1",
			},
			expected: [][]string{
				fullHotRegions[8], fullHotRegions[9],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_id=" + statsMetaTidStr,
				"index_id=1",
				"table_name='TABLES_PRIV'",
			}, // table_id != table_name -> nil
			expected: [][]string{},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_id=" + statsMetaTidStr,
				"index_id=1",
				"table_name='STATS_META'",
			}, // table_id = table_name
			expected: [][]string{
				fullHotRegions[8], fullHotRegions[9],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_id=" + statsMetaTidStr,
				"index_id=1",
				"index_name='UNKNOWN'",
			}, // index_id != index_name -> nil
			expected: [][]string{},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_id=" + statsMetaTidStr,
				"index_id=1",
				"index_name='IDX_VER'",
			}, // index_id = index_name
			expected: [][]string{
				fullHotRegions[8], fullHotRegions[9],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"index_id=1",
				"index_name='IDX_VER'",
				"table_id>=" + statsMetaTidStr, // unpushed down predicates 21>=21
			},
			expected: [][]string{
				fullHotRegions[8], fullHotRegions[9],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"index_id=1",
				"index_name='IDX_VER'",
				"table_id>" + statsMetaTidStr, // unpushed down predicates
			}, // 21!>21 -> nil
			expected: [][]string{},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"index_id=1",
				"index_name='IDX_VER'",
				"table_id>=" + statsMetaTidStr, // unpushed down predicates
				"db_name='MYSQL'",
			},
			expected: [][]string{
				fullHotRegions[8], fullHotRegions[9],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"index_id=1",
				"index_name='IDX_VER'",
				"table_id>=" + statsMetaTidStr, // unpushed down predicates
				"db_name='MYSQL'",
				"peer_id>=33334",
			},
			expected: [][]string{
				fullHotRegions[9],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"index_id=1",
				"index_name='IDX_VER'",
				"table_id>=" + statsMetaTidStr, // unpushed down predicates
				"db_name='UNKNOWN'",
			},
			expected: [][]string{},
		},
	}

	// mock http resp
	store := s.store.(*mockStoreWithMultiPD)
	for i, resp := range pdResps {
		for k, v := range resp {
			hotRegionsResponses[k+store.hosts[i]] = v
		}
	}

	for _, cas := range cases {
		sql := "select * from information_schema.tidb_hot_regions_history"
		if len(cas.conditions) > 0 {
			sql = fmt.Sprintf("%s where %s", sql, strings.Join(cas.conditions, " and "))
		}
		result := tk.MustQuery(sql)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		require.Len(t, warnings, 0, fmt.Sprintf("unexpected warnings: %+v, sql: %s", warnings, sql))
		var expected []string
		for _, row := range cas.expected {
			expectedRow := row
			expected = append(expected, strings.Join(expectedRow, " "))
		}
		result.Check(testkit.Rows(expected...))
	}
}

func TestTiDBHotRegionsHistoryError(t *testing.T) {
	s := createHotRegionsHistoryTableSuite(t)
	defer s.clean()

	tk := testkit.NewTestKit(t, s.store)

	// Test without start time error
	rs, err := tk.Exec("select * from information_schema.tidb_hot_regions_history")
	require.NoError(t, err)
	_, err = session.ResultSetToStringSlice(context.Background(), tk.Session(), rs)
	require.EqualError(t, err, "denied to scan hot regions, please specified the start time, such as `update_time > '2020-01-01 00:00:00'`")
	require.NoError(t, rs.Close())

	// Test without end time error.
	rs, err = tk.Exec("select * from information_schema.tidb_hot_regions_history where update_time>='2019/08/26 06:18:13.011'")
	require.NoError(t, err)
	_, err = session.ResultSetToStringSlice(context.Background(), tk.Session(), rs)
	require.EqualError(t, err, "denied to scan hot regions, please specified the end time, such as `update_time < '2020-01-01 00:00:00'`")
	require.NoError(t, rs.Close())
}
