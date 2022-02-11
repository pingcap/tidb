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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl_test

import (
	"context"
	"fmt"
	"math"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/gcworker"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/tikv/client-go/v2/testutils"
	"go.uber.org/zap"
)

type tiflashDDLTestSuite struct {
	store   kv.Storage
	dom     *domain.Domain
	tiflash *infosync.MockTiFlash
	cluster *unistore.Cluster
}

var _ = SerialSuites(&tiflashDDLTestSuite{})

const (
	RoundToBeAvailable               = 2
	RoundToBeAvailablePartitionTable = 3
)

func (s *tiflashDDLTestSuite) SetUpSuite(c *C) {
	var err error

	ddl.PollTiFlashInterval = 1000 * time.Millisecond
	ddl.PullTiFlashPdTick.Store(60)
	s.tiflash = infosync.NewMockTiFlash()
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

	c.Assert(err, IsNil)

	session.SetSchemaLease(0)
	session.DisableStats4Test()

	s.dom, err = session.BootstrapSession(s.store)

	infosync.SetMockTiFlash(s.tiflash)
	c.Assert(err, IsNil)
	s.dom.SetStatsUpdating(true)

	log.Info("Mock stat", zap.Any("infosyncer", s.dom.InfoSyncer()))
}

func (s *tiflashDDLTestSuite) TearDownSuite(c *C) {
	s.tiflash.Lock()
	s.tiflash.StatusServer.Close()
	s.tiflash.Unlock()
	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
	ddl.PollTiFlashInterval = 2 * time.Second
}

func ChangeGCSafePoint(tk *testkit.TestKit, t time.Time, enable string, lifeTime string) {
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	lastSafePoint := t.Format(gcTimeFormat)
	s := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	s = fmt.Sprintf(s, lastSafePoint)
	tk.MustExec(s)
	s = `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_enable','%[1]s','')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	s = fmt.Sprintf(s, enable)
	tk.MustExec(s)
	s = `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_life_time','%[1]s','')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	s = fmt.Sprintf(s, lifeTime)
	tk.MustExec(s)
}

func (s *tiflashDDLTestSuite) CheckPlacementRule(rule placement.TiFlashRule) bool {
	return s.tiflash.CheckPlacementRule(rule)
}

func (s *tiflashDDLTestSuite) CheckFlashback(tk *testkit.TestKit, c *C) {
	// If table is dropped after tikv_gc_safe_point, it can be recovered

	ChangeGCSafePoint(tk, time.Now().Add(-time.Hour), "false", "10m0s")
	defer func() {
		ChangeGCSafePoint(tk, time.Now(), "true", "10m0s")
	}()

	fCancel := TempDisableEmulatorGC()
	defer fCancel()
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("flashback table ddltiflash")
	time.Sleep(ddl.PollTiFlashInterval * 3)
	CheckTableAvailable(s.dom, c, 1, []string{})

	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	c.Assert(err, IsNil)
	c.Assert(tb, NotNil)
	if tb.Meta().Partition != nil {
		for _, e := range tb.Meta().Partition.Definitions {
			ruleName := fmt.Sprintf("table-%v-r", e.ID)
			_, ok := s.tiflash.GetPlacementRule(ruleName)
			c.Assert(ok, Equals, true)
		}
	} else {
		ruleName := fmt.Sprintf("table-%v-r", tb.Meta().ID)
		_, ok := s.tiflash.GetPlacementRule(ruleName)
		c.Assert(ok, Equals, true)
	}
}

func TempDisableEmulatorGC() func() {
	ori := ddl.IsEmulatorGCEnable()
	f := func() {
		if ori {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}
	ddl.EmulatorGCDisable()
	return f
}

func (s *tiflashDDLTestSuite) SetPdLoop(tick uint64) func() {
	originValue := ddl.PullTiFlashPdTick.Swap(tick)
	return func() {
		ddl.PullTiFlashPdTick.Store(originValue)
	}
}

// Run all kinds of DDLs, and will create no redundant pd rules for TiFlash.
func (s *tiflashDDLTestSuite) TestTiFlashNoRedundantPDRules(c *C) {
	rpcClient, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	defer func() {
		rpcClient.Close()
		pdClient.Close()
		cluster.Close()
	}()
	for _, store := range s.cluster.GetAllStores() {
		cluster.AddStore(store.Id, store.Address, store.Labels...)
	}
	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, s.store)
	fCancel := TempDisableEmulatorGC()
	defer fCancel()
	// Disable emulator GC, otherwise delete range will be automatically called.

	failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed")
	}()
	fCancelPD := s.SetPdLoop(10000)
	defer fCancelPD()

	// Clean all rules
	s.tiflash.CleanPlacementRules()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("drop table if exists ddltiflashp")
	tk.MustExec("create table ddltiflash(z int)")
	tk.MustExec("create table ddltiflashp(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10),PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30))")

	total := 0
	c.Assert(s.tiflash.PlacementRulesLen(), Equals, total)

	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	total += 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	c.Assert(s.tiflash.PlacementRulesLen(), Equals, total)

	tk.MustExec("alter table ddltiflashp set tiflash replica 1")
	total += 3
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	c.Assert(s.tiflash.PlacementRulesLen(), Equals, total)

	lessThan := 40
	tk.MustExec(fmt.Sprintf("ALTER TABLE ddltiflashp ADD PARTITION (PARTITION pn VALUES LESS THAN (%v))", lessThan))
	total += 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	c.Assert(s.tiflash.PlacementRulesLen(), Equals, total)

	tk.MustExec("alter table ddltiflashp truncate partition p1")
	total += 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	c.Assert(s.tiflash.PlacementRulesLen(), Equals, total)

	// Now gc will trigger, and will remove dropped partition.
	c.Assert(gcWorker.DeleteRanges(context.TODO(), math.MaxInt64), IsNil)
	total -= 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	c.Assert(s.tiflash.PlacementRulesLen(), Equals, total)

	tk.MustExec("alter table ddltiflashp drop partition p2")
	c.Assert(gcWorker.DeleteRanges(context.TODO(), math.MaxInt64), IsNil)
	total -= 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	c.Assert(s.tiflash.PlacementRulesLen(), Equals, total)

	tk.MustExec("truncate table ddltiflash")
	total += 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	c.Assert(s.tiflash.PlacementRulesLen(), Equals, total)

	c.Assert(gcWorker.DeleteRanges(context.TODO(), math.MaxInt64), IsNil)
	total -= 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	c.Assert(s.tiflash.PlacementRulesLen(), Equals, total)

	tk.MustExec("drop table ddltiflash")
	total -= 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	c.Assert(gcWorker.DeleteRanges(context.TODO(), math.MaxInt64), IsNil)
	c.Assert(s.tiflash.PlacementRulesLen(), Equals, total)
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
			table, ok := s.tiflash.GetTableSyncStatus(int(p.ID))
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
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/BeforePollTiFlashReplicaStatusLoop", `return`), IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/ddl/BeforePollTiFlashReplicaStatusLoop")
	}()

	tk.MustExec(fmt.Sprintf("ALTER TABLE ddltiflash ADD PARTITION (PARTITION pn VALUES LESS THAN (%v))", lessThan))
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	c.Assert(err, IsNil)
	pi := tb.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)

	// Partition `lessThan` shall be ready
	for _, p := range pi.Definitions {
		c.Assert(tb.Meta().TiFlashReplica.IsPartitionAvailable(p.ID), Equals, true)
		if len(p.LessThan) == 1 && p.LessThan[0] == lessThan {
			table, ok := s.tiflash.GetTableSyncStatus(int(p.ID))
			c.Assert(ok, Equals, true)
			c.Assert(table.Accel, Equals, true)
		}
	}
	c.Assert(len(pi.AddingDefinitions), Equals, 0)
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

	tk.MustExec("drop table if exists ddltiflash2")
	tk.MustExec("create table ddltiflash2 like ddltiflash")
	tk.MustExec("alter table ddltiflash2 set tiflash replica 1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable * 3)
	CheckTableAvailableWithTableName(s.dom, c, 1, []string{}, "test", "ddltiflash2")

	s.CheckFlashback(tk, c)
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	c.Assert(err, IsNil)
	r, ok := s.tiflash.GetPlacementRule(fmt.Sprintf("table-%v-r", tb.Meta().ID))
	c.Assert(r, NotNil)
	c.Assert(ok, Equals, true)
	tk.MustExec("alter table ddltiflash set tiflash replica 0")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	tb, err = s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	c.Assert(err, IsNil)
	replica := tb.Meta().TiFlashReplica
	c.Assert(replica, IsNil)
	r, ok = s.tiflash.GetPlacementRule(fmt.Sprintf("table-%v-r", tb.Meta().ID))
	c.Assert(r, IsNil)
	c.Assert(ok, Equals, false)
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
	CheckTableAvailableWithTableName(s.dom, c, 1, []string{}, "test", "ddltiflash")
}

// Fail truncate partition.
func (s *tiflashDDLTestSuite) TestTiFlashFailTruncatePartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(i int not null, s varchar(255)) partition by range (i) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/FailTiFlashTruncatePartition", `return`), IsNil)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/ddl/FailTiFlashTruncatePartition")
	}()
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)

	tk.MustExec("insert into ddltiflash values(1, 'abc'), (11, 'def')")
	tk.MustGetErrMsg("alter table ddltiflash truncate partition p1", "[ddl:-1]enforced error")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	CheckTableAvailableWithTableName(s.dom, c, 1, []string{}, "test", "ddltiflash")
}

// Drop partition shall not block.
func (s *tiflashDDLTestSuite) TestTiFlashDropPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(i int not null, s varchar(255)) partition by range (i) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	CheckTableAvailableWithTableName(s.dom, c, 1, []string{}, "test", "ddltiflash")
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
	CheckTableAvailableWithTableName(s.dom, c, 1, []string{}, "test", "ddltiflashp")
	tk.MustExec("drop table if exists ddltiflash2")
	tk.MustExec("create table ddltiflash2(z int)")
	tk.MustExec("alter table ddltiflash2 set tiflash replica 1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	// Should get schema right now

	tk.MustExec("truncate table ddltiflash2")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	CheckTableAvailableWithTableName(s.dom, c, 1, []string{}, "test", "ddltiflash2")
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
	ChangeGCSafePoint(tk, time.Now().Add(+3*time.Second), "true", "10m0s")
	defer func() {
		ChangeGCSafePoint(tk, time.Now(), "true", "10m0s")
	}()
	fCancelPD := s.SetPdLoop(1)
	defer fCancelPD()
	tk.MustExec("drop table ddltiflash")
	expectRule = infosync.MakeNewRule(tb.Meta().ID, 1, []string{"a", "b"})
	res = s.CheckPlacementRule(*expectRule)
	c.Assert(res, Equals, true)
}

// When gc worker works, it will automatically remove pd rule for TiFlash.
func (s *tiflashDDLTestSuite) TestSetPlacementRuleWithGCWorker(c *C) {
	rpcClient, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	defer func() {
		rpcClient.Close()
		pdClient.Close()
		cluster.Close()
	}()
	for _, store := range s.cluster.GetAllStores() {
		cluster.AddStore(store.Id, store.Address, store.Labels...)
	}
	failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed")
	}()
	fCancelPD := s.SetPdLoop(10000)
	defer fCancelPD()
	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	c.Assert(err, IsNil)
	// Make SetPdLoop take effects.
	time.Sleep(time.Second)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash_gc")
	tk.MustExec("create table ddltiflash_gc(z int)")
	tk.MustExec("alter table ddltiflash_gc set tiflash replica 1 location labels 'a','b'")
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash_gc"))
	c.Assert(err, IsNil)

	expectRule := infosync.MakeNewRule(tb.Meta().ID, 1, []string{"a", "b"})
	res := s.CheckPlacementRule(*expectRule)
	c.Assert(res, Equals, true)

	ChangeGCSafePoint(tk, time.Now().Add(-time.Hour), "true", "10m0s")
	tk.MustExec("drop table ddltiflash_gc")
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
	s.tiflash.PdSwitch(false)
	defer func() {
		s.tiflash.PdSwitch(true)
	}()
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	c.Assert(err, IsNil)

	expectRule := infosync.MakeNewRule(tb.Meta().ID, 1, []string{})
	res := s.CheckPlacementRule(*expectRule)
	c.Assert(res, Equals, false)
}


func (s *tiflashDDLTestSuite) TestAlterDatabaseFail(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("ALTER DATABASE t SET TIFLASH REPLICA 1 SET TIFLASH REPLICA 2 LOCATION LABELS 'a','b'")
}
