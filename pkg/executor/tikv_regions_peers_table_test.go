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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/fn"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client/http"
)

var regionsInfo = map[uint64]pd.RegionInfo{
	1: {
		ID:     1,
		Peers:  []pd.RegionPeer{{ID: 11, StoreID: 1, IsLearner: false}, {ID: 12, StoreID: 2, IsLearner: false}, {ID: 13, StoreID: 3, IsLearner: false}},
		Leader: pd.RegionPeer{ID: 11, StoreID: 1, IsLearner: false},
	},
	2: {
		ID:     2,
		Peers:  []pd.RegionPeer{{ID: 21, StoreID: 1, IsLearner: false}, {ID: 22, StoreID: 2, IsLearner: false}, {ID: 23, StoreID: 3, IsLearner: false}},
		Leader: pd.RegionPeer{ID: 22, StoreID: 2, IsLearner: false},
	},
	3: {
		ID:     3,
		Peers:  []pd.RegionPeer{{ID: 31, StoreID: 1, IsLearner: false}, {ID: 32, StoreID: 2, IsLearner: false}, {ID: 33, StoreID: 3, IsLearner: false}},
		Leader: pd.RegionPeer{ID: 33, StoreID: 3, IsLearner: false},
	},
}

var storeRegionsInfo = &pd.RegionsInfo{
	Count: 3,
	Regions: []pd.RegionInfo{
		regionsInfo[1],
		regionsInfo[2],
		regionsInfo[3],
	},
}

var storesRegionsInfo = map[uint64]*pd.RegionsInfo{
	1: storeRegionsInfo,
	2: storeRegionsInfo,
	3: storeRegionsInfo,
}

func storesRegionsInfoHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "unable to parse id", err)
		return
	}
	writeResp(w, storesRegionsInfo[uint64(id)])
}

func regionsInfoHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "unable to parse id", err)
		return
	}
	writeResp(w, regionsInfo[uint64(id)])
}

func TestTikvRegionPeers(t *testing.T) {
	startTime := time.Now()
	// mock PD http server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	// mock store stats stat
	mockAddr := strings.TrimPrefix(server.URL, "http://")
	// mock PD API
	router.Handle(pd.Status, fn.Wrap(func() (any, error) {
		return struct {
			Version        string `json:"version"`
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{
			Version:        "4.0.0-alpha",
			GitHash:        "mock-pd-githash",
			StartTimestamp: startTime.Unix(),
		}, nil
	}))
	// mock get regionsInfo by store id
	router.HandleFunc(pd.RegionsByStoreIDPrefix+"/"+"{id}", storesRegionsInfoHandler)
	// mock get regionInfo by region id
	router.HandleFunc(pd.RegionByIDPrefix+"/"+"{id}", regionsInfoHandler)
	defer server.Close()

	pdAddrs := []string{mockAddr}
	store := testkit.CreateMockStore(t,
		mockstore.WithTiKVOptions(tikv.WithPDHTTPClient("tikv-regions-peers-table-test", pdAddrs)),
		mockstore.WithPDAddr(pdAddrs),
	)

	store = &mockStore{
		store.(helper.Storage),
		mockAddr,
	}

	fullRegionPeers := [][]string{
		{"1", "11", "1", "0", "1", "NORMAL", "<nil>"},
		{"1", "12", "2", "0", "0", "NORMAL", "<nil>"},
		{"1", "13", "3", "0", "0", "NORMAL", "<nil>"},

		{"2", "21", "1", "0", "0", "NORMAL", "<nil>"},
		{"2", "22", "2", "0", "1", "NORMAL", "<nil>"},
		{"2", "23", "3", "0", "0", "NORMAL", "<nil>"},

		{"3", "31", "1", "0", "0", "NORMAL", "<nil>"},
		{"3", "32", "2", "0", "0", "NORMAL", "<nil>"},
		{"3", "33", "3", "0", "1", "NORMAL", "<nil>"},
	}

	var cases = []struct {
		conditions []string
		reqCount   int32
		expected   [][]string
	}{
		{
			conditions: []string{
				"store_id in (1,2,3)",
				"region_id in (1,2,3)",
			},
			expected: fullRegionPeers,
		},
		{
			conditions: []string{
				"store_id in (1,2)",
				"region_id=1",
			},
			expected: [][]string{
				fullRegionPeers[0], fullRegionPeers[1],
			},
		},
		{
			conditions: []string{
				"store_id in (1,2)",
				"region_id=1",
				"is_leader=1",
			},
			expected: [][]string{
				fullRegionPeers[0],
			},
		},
		{
			conditions: []string{
				"store_id in (1,2)",
				"region_id=1",
				"is_leader=0",
			},
			expected: [][]string{
				fullRegionPeers[1],
			},
		},
		{
			conditions: []string{
				"store_id =1",
				"region_id =1",
				"is_leader =0",
			},
			expected: [][]string{},
		},
	}

	tk := testkit.NewTestKit(t, store)
	for _, cas := range cases {
		sql := "select * from information_schema.tikv_region_peers"
		if len(cas.conditions) > 0 {
			sql = fmt.Sprintf("%s where %s", sql, strings.Join(cas.conditions, " and "))
		}
		result := tk.MustQuery(sql)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		require.Lenf(t, warnings, 0, "unexpected warnings: %+v", warnings)
		var expected []string
		for _, row := range cas.expected {
			expectedRow := row
			expected = append(expected, strings.Join(expectedRow, " "))
		}
		result.Check(testkit.Rows(expected...))
	}
}
