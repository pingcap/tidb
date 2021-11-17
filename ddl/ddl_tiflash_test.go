package ddl_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	. "github.com/pingcap/check"
	"github.com/pingcap/fn"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/gcworker"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mockstorage"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/tikv/client-go/v2/testutils"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type tiflashDDLTestSuite struct {
	store        kv.Storage
	dom          *domain.Domain
	pdHttpServer *httptest.Server
	pdMockAddr   string
	startTime    time.Time
	tiflash      mockTiFlash
}

var _ = Suite(&tiflashDDLTestSuite{})

func (s *tiflashDDLTestSuite) SetUpSuite(c *C) {
	var err error

	s.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < 2 {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				mockCluster.AddStore(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
				tiflashIdx++
			}
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)

	s.pdHttpServer, s.pdMockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	server, addr := s.setUpMockTiFlashHTTPServer()
	s.tiflash = mockTiFlash{
		Addr:         "",
		StatusAddr:   addr,
		StatusServer: server,
		SyncStatus:   make(map[int]mockTiFlashTableInfo),
	}

	c.Assert(err, IsNil)

	session.SetSchemaLease(0)
	session.DisableStats4Test()

	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.dom.SetStatsUpdating(true)

	mockstorage.ModifyPdAddrs(s.store, []string{s.pdMockAddr})
}

func (s *tiflashDDLTestSuite) TearDownSuite(c *C) {
	if s.pdHttpServer != nil {
		s.pdHttpServer.Close()
	}

	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *tiflashDDLTestSuite) CheckPlacementRule(rule placement.Rule) (bool, error) {
	matched := false
	for _, r := range globalTiFlashPlacementRules {
		if r.StartKeyHex == rule.StartKeyHex && r.EndKeyHex == rule.EndKeyHex && r.Count == rule.Count && len(r.LocationLabels) == len(rule.LocationLabels) {
			matched = true
			for i, l := range r.LocationLabels {
				if l != rule.LocationLabels[i] {
					matched = false
					break
				}
			}
			if matched {
				fmt.Printf("!!!! Matched %v %v\n", r, rule)
				break
			}
		}
	}

	if matched {
		return true, nil
	}
	return false, nil
}

func (s *tiflashDDLTestSuite) TestTiFlashReplicaPartitionTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10),PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30))")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("ALTER TABLE t ADD PARTITION (PARTITION pn VALUES LESS THAN (40))")

	time.Sleep(ddl.PollTiFlashInterval * 5)
	// Should get schema right now
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	replica := tb.Meta().TiFlashReplica
	c.Assert(replica, NotNil)
	c.Assert(replica.Available, Equals, true)
	c.Assert(replica.Count, Equals, uint64(1))
	c.Assert(replica.LocationLabels, DeepEquals, []string{})

	pi := tb.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	for _, p := range pi.Definitions {
		c.Assert(tb.Meta().TiFlashReplica.IsPartitionAvailable(p.ID), Equals, true)
		if p.ID == 63 {
			table, ok := s.tiflash.SyncStatus[int(p.ID)]
			c.Assert(ok, Equals, true)
			c.Assert(table.Accel, Equals, true)
		}
	}
	c.Assert(len(pi.AddingDefinitions), Equals, 0)
}

func (s *tiflashDDLTestSuite) TestRemoveTiFlashReplica(c *C) {

}

// TiFlash Table shall be eventually available.
func (s *tiflashDDLTestSuite) TestTiFlashReplicaAvailable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(z int)")
	tk.MustExec("alter table t set tiflash replica 1")

	time.Sleep(ddl.PollTiFlashInterval * 2)
	// Should get schema right now
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	replica := tb.Meta().TiFlashReplica
	c.Assert(replica, NotNil)
	c.Assert(replica.Available, Equals, true)
	c.Assert(replica.Count, Equals, uint64(1))
	c.Assert(replica.LocationLabels, DeepEquals, []string{})
}

// When set TiFlash replica, tidb shall add one Pd Rule for this table.
// When drop/truncate table, Pd Rule shall be removed in limited time.
func (s *tiflashDDLTestSuite) TestSetPlacementRule(c *C) {
	gcworker.SetGcSafePointCacheInterval(time.Second * 1)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(z int)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	expectRule := ddl.MakeNewRule(tb.Meta().ID, 1, []string{})
	res, _ := s.CheckPlacementRule(*expectRule)
	c.Assert(res, Equals, true)

	tk.MustExec("drop table t")
	expectRule = ddl.MakeNewRule(tb.Meta().ID, 1, []string{})
	res, _ = s.CheckPlacementRule(*expectRule)
	c.Assert(res, Equals, true)

	time.Sleep(ddl.PollTiFlashInterval * 5)
	res, _ = s.CheckPlacementRule(*expectRule)
	c.Assert(res, Equals, false)

}

type mockTiFlashTableInfo struct {
	Regions []int
	Accel bool
}

func (m *mockTiFlashTableInfo) String() string {
	regionStr := ""
	for _, s := range m.Regions {
		regionStr = regionStr + strconv.Itoa(s) + "\n"
	}
	if regionStr == "" {
		regionStr = "\n"
	}
	return fmt.Sprintf("%v\n%v", len(m.Regions), regionStr)
}

type mockTiFlash struct {
	Addr         string
	StatusAddr   string
	StatusServer *httptest.Server
	SyncStatus   map[int]mockTiFlashTableInfo
}

var globalTiFlashPlacementRules = make(map[string]placement.Rule)

func (s *tiflashDDLTestSuite) setUpMockTiFlashHTTPServer() (*httptest.Server, string) {
	// mock TiFlash http server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	// mock store stats stat
	statusAddr := strings.TrimPrefix(server.URL, "http://")
	router.HandleFunc("/tiflash/sync-status/{tableid:\\d+}", func(w http.ResponseWriter, req *http.Request) {
		params := mux.Vars(req)
		fmt.Printf("!!!! handle sync-status %v\n", params["tableid"])
		tableId, err := strconv.Atoi(params["tableid"])
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		} else {
			table, ok := s.tiflash.SyncStatus[tableId]
			if !ok {
				b := []byte("0\n\n")
				w.WriteHeader(http.StatusOK)
				w.Write(b)
				return
			}
			sync := table.String()
			fmt.Printf("!!!! table.String() %v \n", sync)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(sync))
		}
	})
	return server, statusAddr
}

func (s *tiflashDDLTestSuite) setUpMockPDHTTPServer() (*httptest.Server, string) {
	// mock PD http server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	// mock store stats stat
	mockAddr := strings.TrimPrefix(server.URL, "http://")
	router.Handle(pdapi.Stores, fn.Wrap(func() (*helper.StoresStat, error) {
		return &helper.StoresStat{
			Count: 1,
			Stores: []helper.StoreStat{
				{
					Store: helper.StoreBaseStat{
						ID:             1,
						Address:        "127.0.0.1:3930",
						State:          0,
						StateName:      "Up",
						Version:        "4.0.0-alpha",
						StatusAddress:  s.tiflash.StatusAddr,
						GitHash:        "mock-tikv-githash",
						StartTimestamp: s.startTime.Unix(),
						Labels: []helper.StoreLabel{{
							Key:   "engine",
							Value: "tiflash",
						}},
					},
				},
			},
		}, nil
	}))
	// mock PD API
	router.Handle(pdapi.ClusterVersion, fn.Wrap(func() (string, error) { return "4.0.0-alpha", nil }))
	router.Handle(pdapi.Status, fn.Wrap(func() (interface{}, error) {
		return struct {
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{
			GitHash:        "mock-pd-githash",
			StartTimestamp: s.startTime.Unix(),
		}, nil
	}))
	router.HandleFunc("/pd/api/v1/config/rules/group/tiflash", func(w http.ResponseWriter, req *http.Request) {
		fmt.Printf("!!!! handle rules %v %v %v\n", globalTiFlashPlacementRules, req.URL.Path, req.Method)
		var result = make([]placement.Rule, 0)
		for _, item := range globalTiFlashPlacementRules {
			result = append(result, item)
		}
		w.WriteHeader(http.StatusOK)
		m, _ := json.Marshal(result)
		w.Write(m)
	}) //.Methods(http.MethodGet)
	router.HandleFunc("/pd/api/v1/config/rule", func(w http.ResponseWriter, req *http.Request) {
		fmt.Printf("!!!! handle set url %v %v\n", req.URL.Path, req.Method)
		buf := new(bytes.Buffer)
		_, err := buf.ReadFrom(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var rule placement.Rule
		err = json.Unmarshal(buf.Bytes(), &rule)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		globalTiFlashPlacementRules[rule.ID] = rule
		w.WriteHeader(http.StatusOK)
		// Pd shall schedule TiFlash, we can mock here
		tid := 0
		_, err = fmt.Sscanf(rule.ID, "table-%d-r", &tid)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// TODO Shall mock "/pd/api/v1/stats/region", and set correct region here, according to actual pd rule
		if z, ok := s.tiflash.SyncStatus[tid]; ok {
			z.Regions = []int{1}
			s.tiflash.SyncStatus[tid] = z
		}else{
			s.tiflash.SyncStatus[tid] = mockTiFlashTableInfo {
				Regions: []int{1},
				Accel: false,
			}
		}
	}).Methods(http.MethodPost)
	router.HandleFunc("/pd/api/v1/config/rule/tiflash/{ruleid:.+}", func(w http.ResponseWriter, req *http.Request) {
		fmt.Println("!!!! handle DELETE")
		params := mux.Vars(req)
		ruleId := params["ruleid"]
		ruleId = strings.Trim(ruleId, "/")
		delete(globalTiFlashPlacementRules, ruleId)
		w.WriteHeader(http.StatusOK)
	}).Methods(http.MethodDelete)
	var mockConfig = func() (map[string]interface{}, error) {
		configuration := map[string]interface{}{
			"key1": "value1",
			"key2": map[string]string{
				"nest1": "n-value1",
				"nest2": "n-value2",
			},
			"key3": map[string]interface{}{
				"nest1": "n-value1",
				"nest2": "n-value2",
				"key4": map[string]string{
					"nest3": "n-value4",
					"nest4": "n-value5",
				},
			},
		}
		return configuration, nil
	}
	// PD config.
	router.Handle(pdapi.Config, fn.Wrap(mockConfig))
	// TiDB/TiKV config.
	router.Handle("/config", fn.Wrap(mockConfig))
	// PD region.
	router.Handle("/pd/api/v1/stats/region", fn.Wrap(func() (*helper.PDRegionStats, error) {
		return &helper.PDRegionStats{
			Count:            1,
			EmptyCount:       1,
			StorageSize:      1,
			StorageKeys:      1,
			StoreLeaderCount: map[uint64]int{1: 1},
			StorePeerCount:   map[uint64]int{1: 1},
		}, nil
	}))

	router.HandleFunc("/pd/api/v1/regions/accelerate-schedule", func(w http.ResponseWriter, req *http.Request) {
		fmt.Println("!!!! handle accelerate")

		buf := new(bytes.Buffer)
		_, err := buf.ReadFrom(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		var dat = make(map[string]interface{})
		err = json.Unmarshal(buf.Bytes(), &dat)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		endKey, ok := dat["end_key"].(string)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		endKey, _ = url.QueryUnescape(endKey)
		_, decodedEndKey, _ := codec.DecodeBytes([]byte(endKey), []byte{})
		tableId := tablecodec.DecodeTableID(decodedEndKey)
		tableId -= 1

		table, ok := s.tiflash.SyncStatus[int(tableId)]
		if ok {
			fmt.Printf("!!!! Set to true1 %v\n", int(tableId))
			table.Accel = true
			s.tiflash.SyncStatus[int(tableId)] = table
		} else{
			fmt.Printf("!!!! Set to true2 %v\n", int(tableId))
			s.tiflash.SyncStatus[int(tableId)] = mockTiFlashTableInfo{
				Regions: []int{},
				Accel: true,
			}
		}

		fmt.Printf("!!!! resultin %v\n", s.tiflash.SyncStatus[int(tableId)].Accel)
		w.WriteHeader(http.StatusOK)
	}).Methods(http.MethodPost)
	return server, mockAddr
}
