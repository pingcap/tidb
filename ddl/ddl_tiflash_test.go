// Copyright 2016 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/domain/infosync"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/fn"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/gcworker"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/tikv/client-go/v2/testutils"
	"go.uber.org/zap"
)

type tiflashDDLTestSuite struct {
	store        kv.Storage
	dom          *domain.Domain
	pdHTTPServer *httptest.Server
	pdMockAddr   string
	tiflash      infosync.MockTiFlash
	cluster      *unistore.Cluster
}

var _ = SerialSuites(&tiflashDDLTestSuite{})

const (
	RoundToBeAvailable               = 2
	RoundToBeAvailablePartitionTable = 3
)

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
			s.cluster = mockCluster
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)

	s.pdHTTPServer, s.pdMockAddr = s.setUpMockPDHTTPServer()
	server, addr := s.setUpMockTiFlashHTTPServer()
	s.tiflash = infosync.MockTiFlash{
		StatusAddr:                  addr,
		StatusServer:                server,
		SyncStatus:                  make(map[int]infosync.MockTiFlashTableInfo),
		GlobalTiFlashPlacementRules: make(map[string]placement.Rule),
		PdEnabled:                   true,
		TiflashDelay:                0,
		StartTime:                   time.Now(),
	}

	c.Assert(err, IsNil)

	session.SetSchemaLease(0)
	session.DisableStats4Test()

	s.dom, err = session.BootstrapSession(s.store)

	c.Assert(err, IsNil)
	s.dom.SetStatsUpdating(true)

	infosync.GlobalInfoSyncerInit2(context.Background(), s.dom.DDL().GetID(), s.dom.ServerID, s.dom.GetEtcdClient(), true, []string{s.pdMockAddr})

	// mockstorage.ModifyPdAddrs(s.store, []string{s.pdMockAddr})
	log.Info("Mock stat", zap.String("pd address", s.pdMockAddr), zap.Any("infosyncer", s.dom.InfoSyncer()))
	ddl.EnableTiFlashPoll(s.dom.DDL())
	ddl.PollTiFlashInterval = 1000 * time.Millisecond
	ddl.PullTiFlashPdTick = 60
}

func (s *tiflashDDLTestSuite) TearDownSuite(c *C) {
	if s.pdHTTPServer != nil {
		s.pdHTTPServer.Close()
	}
	if s.tiflash.StatusServer != nil {
		s.tiflash.StatusServer.Close()
	}

	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
	ddl.PollTiFlashInterval = 2 * time.Second
}

// Compare supposed rule, and we actually get from TableInfo
func isRuleMatch(rule placement.Rule, startKey string, endKey string, count int, labels []string) bool {
	// Compute startKey
	if rule.StartKeyHex == startKey && rule.EndKeyHex == endKey {
		ok := false
		for _, c := range rule.Constraints {
			if c.Key == "engine" && len(c.Values) == 1 && c.Values[0] == "tiflash" && c.Op == placement.In {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}

		if len(rule.LocationLabels) == len(labels) {
			for i, lb := range labels {
				if lb != rule.LocationLabels[i] {
					return false
				}
			}
		} else {
			return false
		}

		if rule.Count != count {
			return false
		}
		if rule.Role != placement.Learner {
			return false
		}
	} else {
		return false
	}
	return true
}

func (s *tiflashDDLTestSuite) CheckPlacementRule(rule placement.Rule) bool {
	for _, r := range s.tiflash.GlobalTiFlashPlacementRules {
		if isRuleMatch(rule, r.StartKeyHex, r.EndKeyHex, r.Count, r.LocationLabels) {
			return true
		}
	}
	return false
}

func (s *tiflashDDLTestSuite) CheckFlashback(tk *testkit.TestKit, c *C) {
	// If table is dropped after tikv_gc_safe_point, it can be recovered
	defer func(originGC bool) {
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())
	// Disable emulator GC.
	ddl.EmulatorGCDisable()
	tk.MustExec("drop table if exists ddltiflash")
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	lastSafePoint := time.Now().Add(0 - 3000*time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', ''),('tikv_gc_enable','true','')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk.MustExec(fmt.Sprintf(safePointSQL, lastSafePoint))
	tk.MustExec("flashback table ddltiflash")
	time.Sleep(ddl.PollTiFlashInterval * 3)
	CheckTableAvailable(s.dom, c, 1, []string{})
}

func (s *tiflashDDLTestSuite) TestTiFlashReplicaPartitionTableNormal(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10),PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30))")

	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	c.Assert(err, IsNil)
	replica := tb.Meta().TiFlashReplica
	c.Assert(replica, IsNil)

	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	lessThan := "40"
	tk.MustExec(fmt.Sprintf("ALTER TABLE ddltiflash ADD PARTITION (PARTITION pn VALUES LESS THAN (%v))", lessThan))

	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	// Should get schema again
	CheckTableAvailable(s.dom, c, 1, []string{})

	tb2, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	c.Assert(err, IsNil)
	c.Assert(tb2, NotNil)
	pi := tb2.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	c.Assert(tb2.Meta().TiFlashReplica, NotNil)
	for _, p := range pi.Definitions {
		c.Assert(tb2.Meta().TiFlashReplica.IsPartitionAvailable(p.ID), Equals, true)
		if len(p.LessThan) == 1 && p.LessThan[0] == lessThan {
			table, ok := s.tiflash.SyncStatus[int(p.ID)]
			c.Assert(ok, Equals, true)
			c.Assert(table.Accel, Equals, true)
		}
	}
	c.Assert(len(pi.AddingDefinitions), Equals, 0)

	s.CheckFlashback(tk, c)
}

// When block add partition, new partition shall be available even we break `UpdateTableReplicaInfo`
func (s *tiflashDDLTestSuite) TestTiFlashReplicaPartitionTableBlock(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10),PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30))")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	// Make sure is available
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	CheckTableAvailable(s.dom, c, 1, []string{})

	lessThan := "40"
	// Stop loop
	tk.MustExec(fmt.Sprintf("ALTER TABLE ddltiflash ADD PARTITION (PARTITION pn VALUES LESS THAN (%v))", lessThan))
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/BeforePollTiFlashReplicaStatusLoop", `return`), IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/ddl/BeforePollTiFlashReplicaStatusLoop")
	}()

	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	c.Assert(err, IsNil)
	pi := tb.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)

	// Partition `lessThan` shall be ready
	for _, p := range pi.Definitions {
		c.Assert(tb.Meta().TiFlashReplica.IsPartitionAvailable(p.ID), Equals, true)
		if len(p.LessThan) == 1 && p.LessThan[0] == lessThan {
			table, ok := s.tiflash.SyncStatus[int(p.ID)]
			c.Assert(ok, Equals, true)
			c.Assert(table.Accel, Equals, true)
		}
	}
	c.Assert(len(pi.AddingDefinitions), Equals, 0)
	tk.MustExec("drop table if exists ddltiflash")

	s.CheckFlashback(tk, c)
}

// TiFlash Table shall be eventually available.
func (s *tiflashDDLTestSuite) TestTiFlashReplicaAvailable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int)")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")

	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable * 3)
	CheckTableAvailable(s.dom, c, 1, []string{})

	s.CheckFlashback(tk, c)

	tk.MustExec("alter table ddltiflash set tiflash replica 0")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	c.Assert(err, IsNil)
	replica := tb.Meta().TiFlashReplica
	c.Assert(replica, IsNil)
}

// Truncate partition shall not block.
func (s *tiflashDDLTestSuite) TestTiFlashTruncatePartition(c *C) {

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(i int not null, s varchar(255)) partition by range (i) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")

	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)

	tk.MustExec("insert into ddltiflash values(1, 'abc'), (11, 'def')")
	tk.MustExec("alter table ddltiflash truncate partition p1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	CheckTableAvailableWithTableName(s.dom, c, 1, []string{}, "test", "ddltiflash2")
}

// Drop partition shall not block.
func (s *tiflashDDLTestSuite) TestTiFlashDropPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(i int not null, s varchar(255)) partition by range (i) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	tk.MustExec("alter table ddltiflash drop partition p1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable * 5)
	CheckTableAvailableWithTableName(s.dom, c, 1, []string{}, "test", "ddltiflash")
}

func CheckTableAvailableWithTableName(dom *domain.Domain, c *C, count uint64, labels []string, db string, table string) {
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	c.Assert(err, IsNil)
	replica := tb.Meta().TiFlashReplica
	c.Assert(replica, NotNil)
	c.Assert(replica.Available, Equals, true)
	c.Assert(replica.Count, Equals, count)
	c.Assert(replica.LocationLabels, DeepEquals, labels)
}

func CheckTableAvailable(dom *domain.Domain, c *C, count uint64, labels []string) {
	CheckTableAvailableWithTableName(dom, c, count, labels, "test", "ddltiflash")
}

// Truncate table shall not block.
func (s *tiflashDDLTestSuite) TestTiFlashTruncateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists ddltiflashp")
	tk.MustExec("create table ddltiflashp(z int not null) partition by range (z) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("alter table ddltiflashp set tiflash replica 1")

	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	// Should get schema right now

	tk.MustExec("truncate table ddltiflashp")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	CheckTableAvailable(s.dom, c, 1, []string{})

	tk.MustExec("drop table if exists ddltiflash2")
	tk.MustExec("create table ddltiflash2(z int)")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	// Should get schema right now

	tk.MustExec("truncate table ddltiflash2")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	CheckTableAvailable(s.dom, c, 1, []string{})
}

// TiFlash Table shall be eventually available, even with lots of small table created.
func (s *tiflashDDLTestSuite) TestTiFlashMassiveReplicaAvailable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("drop table if exists ddltiflash%v", i))
		tk.MustExec(fmt.Sprintf("create table ddltiflash%v(z int)", i))
		tk.MustExec(fmt.Sprintf("alter table ddltiflash%v set tiflash replica 1", i))
	}

	time.Sleep(ddl.PollTiFlashInterval * 10)
	// Should get schema right now
	for i := 0; i < 100; i++ {
		CheckTableAvailableWithTableName(s.dom, c, 1, []string{}, "test", fmt.Sprintf("ddltiflash%v", i))
	}
}

// When set TiFlash replica, tidb shall add one Pd Rule for this table.
// When drop/truncate table, Pd Rule shall be removed in limited time.
func (s *tiflashDDLTestSuite) TestSetPlacementRuleNormal(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int)")
	tk.MustExec("alter table ddltiflash set tiflash replica 1 location labels 'a','b'")
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	c.Assert(err, IsNil)

	expectRule := infosync.MakeNewRule(tb.Meta().ID, 1, []string{"a", "b"})
	res := s.CheckPlacementRule(*expectRule)
	c.Assert(res, Equals, true)

	// Set lastSafePoint to a timepoint in future, so all dropped table can be reckon as gc-ed.
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	lastSafePoint := time.Now().Add(0 + 3*time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', ''),('tikv_gc_enable','true','')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk.MustExec(fmt.Sprintf(safePointSQL, lastSafePoint))

	originValue := ddl.PullTiFlashPdTick
	ddl.PullTiFlashPdTick = 1
	defer func() {
		ddl.PullTiFlashPdTick = originValue
	}()
	tk.MustExec("drop table ddltiflash")
	expectRule = infosync.MakeNewRule(tb.Meta().ID, 1, []string{"a", "b"})
	res = s.CheckPlacementRule(*expectRule)
	c.Assert(res, Equals, true)

	// Wait GC
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	res = s.CheckPlacementRule(*expectRule)
	c.Assert(res, Equals, false)
}

// When gc worker works, it will automatically remove pd rule for TiFlash.
func (s *tiflashDDLTestSuite) TestSetPlacementRuleWithGCWorker(c *C) {
	_, _, cluster, err := unistore.New("")
	for _, s := range s.cluster.GetAllStores() {
		cluster.AddStore(s.Id, s.Address, s.Labels...)
	}

	failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed")
	}()

	c.Assert(err, IsNil)
	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	c.Assert(err, IsNil)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int)")
	tk.MustExec("alter table ddltiflash set tiflash replica 1 location labels 'a','b'")
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	c.Assert(err, IsNil)

	expectRule := infosync.MakeNewRule(tb.Meta().ID, 1, []string{"a", "b"})
	res := s.CheckPlacementRule(*expectRule)
	c.Assert(res, Equals, true)

	originValue := ddl.PullTiFlashPdTick
	ddl.PullTiFlashPdTick = 1000
	defer func() {
		ddl.PullTiFlashPdTick = originValue
	}()

	tk.MustExec("drop table ddltiflash")

	// Now gc will trigger, and will remove dropped table.
	c.Assert(gcWorker.DeleteRanges(context.TODO(), math.MaxInt64), IsNil)

	// Wait GC
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	res = s.CheckPlacementRule(*expectRule)
	c.Assert(res, Equals, false)
}

func (s *tiflashDDLTestSuite) TestSetPlacementRuleFail(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int)")
	s.tiflash.PdEnabled = false
	defer func() {
		s.tiflash.PdEnabled = true
	}()
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	c.Assert(err, IsNil)

	expectRule := infosync.MakeNewRule(tb.Meta().ID, 1, []string{})
	res := s.CheckPlacementRule(*expectRule)
	c.Assert(res, Equals, false)
}

func (s *tiflashDDLTestSuite) setUpMockTiFlashHTTPServer() (*httptest.Server, string) {
	// mock TiFlash http server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	// mock store stats stat
	statusAddr := strings.TrimPrefix(server.URL, "http://")
	statusAddrVec := strings.Split(statusAddr, ":")
	statusPort, _ := strconv.Atoi(statusAddrVec[1])
	router.HandleFunc("/tiflash/sync-status/{tableid:\\d+}", func(w http.ResponseWriter, req *http.Request) {
		params := mux.Vars(req)
		tableID, err := strconv.Atoi(params["tableid"])
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		table, ok := s.tiflash.SyncStatus[tableID]
		log.Info("Mock TiFlash returns", zap.Bool("ok", ok), zap.Int("tableID", tableID))
		if !ok {
			b := []byte("0\n\n")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}
		sync := table.String()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(sync))
	})
	router.HandleFunc("/config", func(w http.ResponseWriter, req *http.Request) {
		s := fmt.Sprintf("{\n    \"engine-store\": {\n        \"http_port\": %v\n    }\n}", statusPort)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(s))
	})
	return server, statusAddr
}

func (s *tiflashDDLTestSuite) setUpMockPDHTTPServer() (*httptest.Server, string) {
	// mock PD http server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	// mock store stats stat
	mockAddr := strings.TrimPrefix(server.URL, "http://")
	s.tiflash.PdEnabled = true
	router.Handle(pdapi.Stores, fn.Wrap(func() (*helper.StoresStat, error) {
		return s.tiflash.HandleGetStoresStat(), nil
	}))
	// mock PD API
	router.Handle(pdapi.Status, fn.Wrap(func() (interface{}, error) {
		return struct {
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{
			GitHash:        "mock-pd-githash",
			StartTimestamp: s.tiflash.StartTime.Unix(),
		}, nil
	}))
	router.HandleFunc("/pd/api/v1/config/rules/group/tiflash", func(w http.ResponseWriter, req *http.Request) {
		// GetGroupRules
		result, err := s.tiflash.HandleGetGroupRules()
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusOK)
			m, _ := json.Marshal(result)
			w.Write(m)
		}
	}) //.Methods(http.MethodGet)
	router.HandleFunc("/pd/api/v1/config/rule", func(w http.ResponseWriter, req *http.Request) {
		// SetPlacementRule
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
		err = s.tiflash.HandleSetPlacementRule(rule)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}).Methods(http.MethodPost)
	router.HandleFunc("/pd/api/v1/config/rule/tiflash/{ruleid:.+}", func(w http.ResponseWriter, req *http.Request) {
		// Delete placement rule
		params := mux.Vars(req)
		ruleID := params["ruleid"]
		ruleID = strings.Trim(ruleID, "/")
		s.tiflash.HandleDeletePlacementRule(ruleID)
		w.WriteHeader(http.StatusOK)
	}).Methods(http.MethodDelete)
	var mockConfig = func() (map[string]interface{}, error) {
		configuration := map[string]interface{}{
			"key1": "value1",
		}
		return configuration, nil
	}
	// PD config.
	router.Handle(pdapi.Config, fn.Wrap(mockConfig))
	// TiDB/TiKV config.
	router.Handle("/config", fn.Wrap(mockConfig))
	// PD region.
	router.Handle("/pd/api/v1/stats/region", fn.Wrap(func() (*helper.PDRegionStats, error) {
		return s.tiflash.HandleGetPDRegionRecordStats(0), nil
	}))

	router.HandleFunc("/pd/api/v1/regions/accelerate-schedule", func(w http.ResponseWriter, req *http.Request) {
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

		if e := s.tiflash.HandlePostAccelerateSchedule(endKey); e != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}).Methods(http.MethodPost)
	return server, mockAddr
}
