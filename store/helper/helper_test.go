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

package helper

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
	mvccStore := mocktikv.MustNewMVCCStore()
	mockTikvStore, err := mockstore.NewMockTikvStore(mockstore.WithMVCCStore(mvccStore))
	s.store = &mockStore{
		mockTikvStore.(tikv.Storage),
		[]string{"127.0.0.1:10090/"},
	}
	c.Assert(err, IsNil)
}

func (s *HelperTestSuite) TestHotRegion(c *C) {
	helper := Helper{
		Store:       s.store,
		RegionCache: s.store.GetRegionCache(),
	}
	regionMetric, err := helper.FetchHotRegion(pdapi.HotRead)
	c.Assert(err, IsNil, Commentf("err: %+v", err))
	c.Assert(fmt.Sprintf("%v", regionMetric), Equals, "map[1:{100 1 0}]")
}

func (s *HelperTestSuite) mockPDHTTPServer(c *C) {
	router := mux.NewRouter()
	router.HandleFunc("/pd/api/v1/hotspot/regions/read", s.mockHotRegionResponse)
	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)
	server := &http.Server{Addr: "127.0.0.1:10090", Handler: serverMux}
	err := server.ListenAndServe()
	c.Assert(err, IsNil)
}

func (s *HelperTestSuite) mockHotRegionResponse(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	regionsStat := hotRegionsStat{
		[]regionStat{
			{
				FlowBytes: 100,
				RegionID:  1,
				HotDegree: 1,
			},
		},
	}
	resp := StoreHotRegionInfos{
		AsLeader: make(map[uint64]*hotRegionsStat),
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
