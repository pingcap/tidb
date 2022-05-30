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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/placement"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/gcworker"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"go.uber.org/zap"
)

type tiflashContext struct {
	store   kv.Storage
	dom     *domain.Domain
	tiflash *infosync.MockTiFlash
	cluster *unistore.Cluster
}

const (
	RoundToBeAvailable               = 2
	RoundToBeAvailablePartitionTable = 3
)

func createTiFlashContext(t *testing.T) (*tiflashContext, func()) {
	s := &tiflashContext{}
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

	require.NoError(t, err)
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	infosync.SetMockTiFlash(s.tiflash)
	require.NoError(t, err)
	s.dom.SetStatsUpdating(true)

	tearDown := func() {
		s.tiflash.Lock()
		s.tiflash.StatusServer.Close()
		s.tiflash.Unlock()
		s.dom.Close()
		require.NoError(t, s.store.Close())
		ddl.PollTiFlashInterval = 2 * time.Second
	}
	return s, tearDown
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

func CheckPlacementRule(tiflash *infosync.MockTiFlash, rule placement.TiFlashRule) bool {
	return tiflash.CheckPlacementRule(rule)
}

func (s *tiflashContext) CheckFlashback(tk *testkit.TestKit, t *testing.T) {
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
	CheckTableAvailable(s.dom, t, 1, []string{})

	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	require.NotNil(t, tb)
	if tb.Meta().Partition != nil {
		for _, e := range tb.Meta().Partition.Definitions {
			ruleName := fmt.Sprintf("table-%v-r", e.ID)
			_, ok := s.tiflash.GetPlacementRule(ruleName)
			require.True(t, ok)
		}
	} else {
		ruleName := fmt.Sprintf("table-%v-r", tb.Meta().ID)
		_, ok := s.tiflash.GetPlacementRule(ruleName)
		require.True(t, ok)
	}
}

func TempDisableEmulatorGC() func() {
	ori := ddlutil.IsEmulatorGCEnable()
	f := func() {
		if ori {
			ddlutil.EmulatorGCEnable()
		} else {
			ddlutil.EmulatorGCDisable()
		}
	}
	ddlutil.EmulatorGCDisable()
	return f
}

func (s *tiflashContext) SetPdLoop(tick uint64) func() {
	originValue := ddl.PullTiFlashPdTick.Swap(tick)
	return func() {
		ddl.PullTiFlashPdTick.Store(originValue)
	}
}

// Run all kinds of DDLs, and will create no redundant pd rules for TiFlash.
func TestTiFlashNoRedundantPDRules(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()

	rpcClient, pdClient, cluster, err := unistore.New("")
	require.NoError(t, err)
	defer func() {
		rpcClient.Close()
		pdClient.Close()
		cluster.Close()
	}()
	for _, store := range s.cluster.GetAllStores() {
		cluster.AddStore(store.Id, store.Address, store.Labels...)
	}
	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, s.store)
	fCancel := TempDisableEmulatorGC()
	defer fCancel()
	// Disable emulator GC, otherwise delete range will be automatically called.

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed"))
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
	require.Equal(t, total, s.tiflash.PlacementRulesLen())

	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	total += 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	require.Equal(t, total, s.tiflash.PlacementRulesLen())

	tk.MustExec("alter table ddltiflashp set tiflash replica 1")
	total += 3
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	require.Equal(t, total, s.tiflash.PlacementRulesLen())

	lessThan := 40
	tk.MustExec(fmt.Sprintf("ALTER TABLE ddltiflashp ADD PARTITION (PARTITION pn VALUES LESS THAN (%v))", lessThan))
	total += 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	require.Equal(t, total, s.tiflash.PlacementRulesLen())

	tk.MustExec("alter table ddltiflashp truncate partition p1")
	total += 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	require.Equal(t, total, s.tiflash.PlacementRulesLen())
	// Now gc will trigger, and will remove dropped partition.
	require.NoError(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))
	total -= 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	require.Equal(t, total, s.tiflash.PlacementRulesLen())

	tk.MustExec("alter table ddltiflashp drop partition p2")
	require.NoError(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))
	total -= 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	require.Equal(t, total, s.tiflash.PlacementRulesLen())

	tk.MustExec("truncate table ddltiflash")
	total += 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	require.Equal(t, total, s.tiflash.PlacementRulesLen())
	require.NoError(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))
	total -= 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	require.Equal(t, total, s.tiflash.PlacementRulesLen())

	tk.MustExec("drop table ddltiflash")
	total -= 1
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	require.NoError(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))
	require.Equal(t, total, s.tiflash.PlacementRulesLen())
}

func TestTiFlashReplicaPartitionTableNormal(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10),PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30))")

	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	replica := tb.Meta().TiFlashReplica
	require.Nil(t, replica)

	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	lessThan := "40"
	tk.MustExec(fmt.Sprintf("ALTER TABLE ddltiflash ADD PARTITION (PARTITION pn VALUES LESS THAN (%v))", lessThan))

	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	// Should get schema again
	CheckTableAvailable(s.dom, t, 1, []string{})

	tb2, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	require.NotNil(t, tb2)
	pi := tb2.Meta().GetPartitionInfo()
	require.NotNil(t, pi)
	require.NotNil(t, tb2.Meta().TiFlashReplica)
	for _, p := range pi.Definitions {
		require.True(t, tb2.Meta().TiFlashReplica.IsPartitionAvailable(p.ID))
		if len(p.LessThan) == 1 && p.LessThan[0] == lessThan {
			table, ok := s.tiflash.GetTableSyncStatus(int(p.ID))
			require.True(t, ok)
			require.True(t, table.Accel)
		}
	}
	require.Zero(t, len(pi.AddingDefinitions))
	s.CheckFlashback(tk, t)
}

// When block add partition, new partition shall be available even we break `UpdateTableReplicaInfo`
func TestTiFlashReplicaPartitionTableBlock(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10),PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30))")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	// Make sure is available
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	CheckTableAvailable(s.dom, t, 1, []string{})

	lessThan := "40"
	// Stop loop
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/BeforePollTiFlashReplicaStatusLoop", `return`))
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/ddl/BeforePollTiFlashReplicaStatusLoop")
	}()

	tk.MustExec(fmt.Sprintf("ALTER TABLE ddltiflash ADD PARTITION (PARTITION pn VALUES LESS THAN (%v))", lessThan))
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	pi := tb.Meta().GetPartitionInfo()
	require.NotNil(t, pi)

	// Partition `lessThan` shall be ready
	for _, p := range pi.Definitions {
		require.True(t, tb.Meta().TiFlashReplica.IsPartitionAvailable(p.ID))
		if len(p.LessThan) == 1 && p.LessThan[0] == lessThan {
			table, ok := s.tiflash.GetTableSyncStatus(int(p.ID))
			require.True(t, ok)
			require.True(t, table.Accel)
		}
	}
	require.Equal(t, 0, len(pi.AddingDefinitions))
	s.CheckFlashback(tk, t)
}

// TiFlash Table shall be eventually available.
func TestTiFlashReplicaAvailable(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int)")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable * 3)
	CheckTableAvailable(s.dom, t, 1, []string{})

	tk.MustExec("drop table if exists ddltiflash2")
	tk.MustExec("create table ddltiflash2 like ddltiflash")
	tk.MustExec("alter table ddltiflash2 set tiflash replica 1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable * 3)
	CheckTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash2")

	s.CheckFlashback(tk, t)
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	r, ok := s.tiflash.GetPlacementRule(fmt.Sprintf("table-%v-r", tb.Meta().ID))
	require.NotNil(t, r)
	require.True(t, ok)
	tk.MustExec("alter table ddltiflash set tiflash replica 0")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	tb, err = s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	replica := tb.Meta().TiFlashReplica
	require.Nil(t, replica)
	r, ok = s.tiflash.GetPlacementRule(fmt.Sprintf("table-%v-r", tb.Meta().ID))
	require.Nil(t, r)
	require.False(t, ok)
}

// Truncate partition shall not block.
func TestTiFlashTruncatePartition(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(i int not null, s varchar(255)) partition by range (i) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	tk.MustExec("insert into ddltiflash values(1, 'abc'), (11, 'def')")
	tk.MustExec("alter table ddltiflash truncate partition p1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	CheckTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
}

// Fail truncate partition.
func TestTiFlashFailTruncatePartition(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(i int not null, s varchar(255)) partition by range (i) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/FailTiFlashTruncatePartition", `return`))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/ddl/FailTiFlashTruncatePartition")
	}()
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)

	tk.MustExec("insert into ddltiflash values(1, 'abc'), (11, 'def')")
	tk.MustGetErrMsg("alter table ddltiflash truncate partition p1", "[ddl:-1]enforced error")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	CheckTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
}

// Drop partition shall not block.
func TestTiFlashDropPartition(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(i int not null, s varchar(255)) partition by range (i) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	CheckTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
	tk.MustExec("alter table ddltiflash drop partition p1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable * 5)
	CheckTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
}

func CheckTableAvailableWithTableName(dom *domain.Domain, t *testing.T, count uint64, labels []string, db string, table string) {
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	require.NoError(t, err)
	replica := tb.Meta().TiFlashReplica
	require.NotNil(t, replica)
	require.True(t, replica.Available)
	require.Equal(t, count, replica.Count)
	require.ElementsMatch(t, labels, replica.LocationLabels)
}

func CheckTableAvailable(dom *domain.Domain, t *testing.T, count uint64, labels []string) {
	CheckTableAvailableWithTableName(dom, t, count, labels, "test", "ddltiflash")
}

func CheckTableNoReplica(dom *domain.Domain, t *testing.T, db string, table string) {
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	require.NoError(t, err)
	replica := tb.Meta().TiFlashReplica
	require.Nil(t, replica)
}

// Truncate table shall not block.
func TestTiFlashTruncateTable(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflashp")
	tk.MustExec("create table ddltiflashp(z int not null) partition by range (z) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("alter table ddltiflashp set tiflash replica 1")

	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	// Should get schema right now
	tk.MustExec("truncate table ddltiflashp")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)
	CheckTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflashp")
	tk.MustExec("drop table if exists ddltiflash2")
	tk.MustExec("create table ddltiflash2(z int)")
	tk.MustExec("alter table ddltiflash2 set tiflash replica 1")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	// Should get schema right now

	tk.MustExec("truncate table ddltiflash2")
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	CheckTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash2")
}

// TiFlash Table shall be eventually available, even with lots of small table created.
func TestTiFlashMassiveReplicaAvailable(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("drop table if exists ddltiflash%v", i))
		tk.MustExec(fmt.Sprintf("create table ddltiflash%v(z int)", i))
		tk.MustExec(fmt.Sprintf("alter table ddltiflash%v set tiflash replica 1", i))
	}

	time.Sleep(ddl.PollTiFlashInterval * 10)
	// Should get schema right now
	for i := 0; i < 100; i++ {
		CheckTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", fmt.Sprintf("ddltiflash%v", i))
	}
}

// When set TiFlash replica, tidb shall add one Pd Rule for this table.
// When drop/truncate table, Pd Rule shall be removed in limited time.
func TestSetPlacementRuleNormal(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int)")
	tk.MustExec("alter table ddltiflash set tiflash replica 1 location labels 'a','b'")
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	expectRule := infosync.MakeNewRule(tb.Meta().ID, 1, []string{"a", "b"})
	res := CheckPlacementRule(s.tiflash, *expectRule)
	require.True(t, res)

	// Set lastSafePoint to a timepoint in future, so all dropped table can be reckon as gc-ed.
	ChangeGCSafePoint(tk, time.Now().Add(+3*time.Second), "true", "10m0s")
	defer func() {
		ChangeGCSafePoint(tk, time.Now(), "true", "10m0s")
	}()
	fCancelPD := s.SetPdLoop(1)
	defer fCancelPD()
	tk.MustExec("drop table ddltiflash")
	expectRule = infosync.MakeNewRule(tb.Meta().ID, 1, []string{"a", "b"})
	res = CheckPlacementRule(s.tiflash, *expectRule)
	require.True(t, res)
}

// When gc worker works, it will automatically remove pd rule for TiFlash.

func TestSetPlacementRuleWithGCWorker(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()

	rpcClient, pdClient, cluster, err := unistore.New("")
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

	require.NoError(t, err)
	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	require.NoError(t, err)
	// Make SetPdLoop take effects.
	time.Sleep(time.Second)

	fCancel := TempDisableEmulatorGC()
	defer fCancel()

	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash_gc")
	tk.MustExec("create table ddltiflash_gc(z int)")
	tk.MustExec("alter table ddltiflash_gc set tiflash replica 1 location labels 'a','b'")
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash_gc"))
	require.NoError(t, err)

	expectRule := infosync.MakeNewRule(tb.Meta().ID, 1, []string{"a", "b"})
	res := CheckPlacementRule(s.tiflash, *expectRule)
	require.True(t, res)

	ChangeGCSafePoint(tk, time.Now().Add(-time.Hour), "true", "10m0s")
	tk.MustExec("drop table ddltiflash_gc")
	// Now gc will trigger, and will remove dropped table.
	require.Nil(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))

	// Wait GC
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	res = CheckPlacementRule(s.tiflash, *expectRule)
	require.False(t, res)
}

func TestSetPlacementRuleFail(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int)")
	s.tiflash.PdSwitch(false)
	defer func() {
		s.tiflash.PdSwitch(true)
	}()
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	require.NoError(t, err)

	expectRule := infosync.MakeNewRule(tb.Meta().ID, 1, []string{})
	res := CheckPlacementRule(s.tiflash, *expectRule)
	require.False(t, res)
}

// Test standalone backoffer
func TestTiFlashBackoffer(t *testing.T) {
	var maxTick ddl.TiFlashTick = 10
	var rate ddl.TiFlashTick = 1.5
	c := 2
	backoff, err := ddl.NewPollTiFlashBackoffContext(1, maxTick, c, rate)
	require.NoError(t, err)
	mustGet := func(ID int64) *ddl.PollTiFlashBackoffElement {
		e, ok := backoff.Get(ID)
		require.True(t, ok)
		return e
	}
	mustNotGrow := func(ID int64) {
		e := mustGet(ID)
		ori := e.Threshold
		oriTotal := e.TotalCounter
		c := e.Counter
		growed, ok, total := backoff.Tick(ID)
		require.True(t, ok)
		require.False(t, growed)
		require.Equal(t, ori, e.Threshold)
		require.Equal(t, c+1, e.Counter)
		require.Equal(t, oriTotal+1, total)
	}
	mustGrow := func(ID int64) {
		e := mustGet(ID)
		ori := e.Threshold
		oriTotal := e.TotalCounter
		growed, ok, total := backoff.Tick(ID)
		require.True(t, ok)
		require.True(t, growed)
		require.Equal(t, e.Threshold, rate*ori)
		require.Equal(t, 1, e.Counter)
		require.Equal(t, oriTotal+1, total)
	}
	// Test grow
	ok := backoff.Put(1)
	require.True(t, ok)
	require.False(t, mustGet(1).NeedGrow())
	mustNotGrow(1) // 0;1 -> 1;1
	mustGrow(1)    // 1;1 -> 0;1.5 -> 1;1.5
	mustGrow(1)    // 1;1.5 -> 0;2.25 -> 1;2.25
	mustNotGrow(1) // 1;2.25 -> 2;2.25
	mustGrow(1)    // 2;2.25 -> 0;3.375 -> 1;3.375
	mustNotGrow(1) // 1;3.375 -> 2;3.375
	mustNotGrow(1) // 2;3.375 -> 3;3.375
	mustGrow(1)    // 3;3.375 -> 0;5.0625
	require.Equal(t, 8, mustGet(1).TotalCounter)

	// Test converge
	backoff.Put(2)
	for i := 0; i < 20; i++ {
		backoff.Tick(2)
	}
	require.Equal(t, maxTick, mustGet(2).Threshold)
	require.Equal(t, 20, mustGet(2).TotalCounter)

	// Test context
	ok = backoff.Put(3)
	require.False(t, ok)
	_, ok, _ = backoff.Tick(3)
	require.False(t, ok)

	require.True(t, backoff.Remove(1))
	require.False(t, backoff.Remove(1))
	require.Equal(t, 1, backoff.Len())

	// Test error context
	_, err = ddl.NewPollTiFlashBackoffContext(0.5, 1, 1, 1)
	require.Error(t, err)
	_, err = ddl.NewPollTiFlashBackoffContext(10, 1, 1, 1)
	require.Error(t, err)
	_, err = ddl.NewPollTiFlashBackoffContext(1, 10, 0, 1)
	require.Error(t, err)
	_, err = ddl.NewPollTiFlashBackoffContext(1, 10, 1, 0.5)
	require.Error(t, err)
	_, err = ddl.NewPollTiFlashBackoffContext(1, 10, 1, -1)
	require.Error(t, err)
}

// Test backoffer in TiFlash.
func TestTiFlashBackoff(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int)")

	// Not available for all tables
	ddl.DisableTiFlashPoll(s.dom.DDL())
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/PollTiFlashReplicaStatusReplacePrevAvailableValue", `return(false)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/PollTiFlashReplicaStatusReplaceCurAvailableValue", `return(false)`))
	ddl.EnableTiFlashPoll(s.dom.DDL())
	tk.MustExec("alter table ddltiflash set tiflash replica 1")

	// 1, 1.5, 2.25, 3.375, 5.5625
	// (1), 1, 1, 2, 3, 5
	time.Sleep(ddl.PollTiFlashInterval * 5)
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	require.NotNil(t, tb)
	require.False(t, tb.Meta().TiFlashReplica.Available)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/PollTiFlashReplicaStatusReplacePrevAvailableValue"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/PollTiFlashReplicaStatusReplaceCurAvailableValue"))

	time.Sleep(ddl.PollTiFlashInterval * 3)
	tb, err = s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	require.NotNil(t, tb)
	require.True(t, tb.Meta().TiFlashReplica.Available)
}

func TestAlterDatabaseErrorGrammar(t *testing.T) {
	store, tear := testkit.CreateMockStore(t)
	defer tear()

	tk := testkit.NewTestKit(t, store)
	tk.MustGetErrMsg("ALTER DATABASE t SET TIFLASH REPLICA 1 SET TIFLASH REPLICA 2 LOCATION LABELS 'a','b'", "[ddl:8200]Unsupported multi schema change")
	tk.MustGetErrMsg("ALTER DATABASE t SET TIFLASH REPLICA 1 SET TIFLASH REPLICA 2", "[ddl:8200]Unsupported multi schema change")
	tk.MustGetErrMsg("ALTER DATABASE t SET TIFLASH REPLICA 1 LOCATION LABELS 'a','b' SET TIFLASH REPLICA 2", "[ddl:8200]Unsupported multi schema change")
	tk.MustGetErrMsg("ALTER DATABASE t SET TIFLASH REPLICA 1 LOCATION LABELS 'a','b' SET TIFLASH REPLICA 2 LOCATION LABELS 'a','b'", "[ddl:8200]Unsupported multi schema change")
}

func TestAlterDatabaseBasic(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("drop database if exists tiflash_ddl")
	tk.MustExec("create database tiflash_ddl")
	tk.MustExec("create table tiflash_ddl.ddltiflash(z int)")
	tk.MustExec("create table tiflash_ddl.ddltiflash2(z int)")
	// ALTER DATABASE can override previous ALTER TABLE.
	tk.MustExec("alter table tiflash_ddl.ddltiflash set tiflash replica 1")
	tk.MustExec("alter database tiflash_ddl set tiflash replica 2")
	require.Equal(t, "In total 2 tables: 2 succeed, 0 failed, 0 skipped", tk.Session().GetSessionVars().StmtCtx.GetMessage())
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable * 2)
	CheckTableAvailableWithTableName(s.dom, t, 2, []string{}, "tiflash_ddl", "ddltiflash")
	CheckTableAvailableWithTableName(s.dom, t, 2, []string{}, "tiflash_ddl", "ddltiflash2")

	// Skip already set TiFlash tables.
	tk.MustExec("alter database tiflash_ddl set tiflash replica 2")
	require.Equal(t, "In total 2 tables: 0 succeed, 0 failed, 2 skipped", tk.Session().GetSessionVars().StmtCtx.GetMessage())
	CheckTableAvailableWithTableName(s.dom, t, 2, []string{}, "tiflash_ddl", "ddltiflash")
	CheckTableAvailableWithTableName(s.dom, t, 2, []string{}, "tiflash_ddl", "ddltiflash2")

	// There is no existing database.
	tk.MustExec("drop database if exists tiflash_ddl_missing")
	tk.MustGetErrMsg("alter database tiflash_ddl_missing set tiflash replica 2", "[schema:1049]Unknown database 'tiflash_ddl_missing'")

	// There is no table in database
	tk.MustExec("drop database if exists tiflash_ddl_empty")
	tk.MustExec("create database tiflash_ddl_empty")
	tk.MustGetErrMsg("alter database tiflash_ddl_empty set tiflash replica 2", "[schema:1049]Empty database 'tiflash_ddl_empty'")

	// There is less TiFlash store
	tk.MustGetErrMsg("alter database tiflash_ddl set tiflash replica 3", "the tiflash replica count: 3 should be less than the total tiflash server count: 2")
}

func checkBatchPandingNum(t *testing.T, tkx *testkit.TestKit, level string, value string, ok bool) {
	l := len(tkx.MustQuery(fmt.Sprintf("show %v variables where Variable_name='tidb_batch_pending_tiflash_count' and Value='%v'", level, value)).Rows())
	if ok {
		require.Equal(t, 1, l)
	} else {
		require.Equal(t, 0, l)
	}
}

func TestTiFlashBatchAddVariables(t *testing.T) {
	store, tear := testkit.CreateMockStore(t)
	defer tear()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set SESSION tidb_batch_pending_tiflash_count=5")
	tk.MustExec("set GLOBAL tidb_batch_pending_tiflash_count=6")

	checkBatchPandingNum(t, tk, "session", "5", true)
	checkBatchPandingNum(t, tk, "global", "6", true)
	checkBatchPandingNum(t, tk, "global", "1.5", false)

	tk.MustGetErrMsg("set GLOBAL tidb_batch_pending_tiflash_count=1.5", "[variable:1232]Incorrect argument type to variable 'tidb_batch_pending_tiflash_count'")
	checkBatchPandingNum(t, tk, "global", "6", true)

	tk2 := testkit.NewTestKit(t, store)
	checkBatchPandingNum(t, tk2, "session", "6", true)
}

func execWithTimeout(t *testing.T, tk *testkit.TestKit, to time.Duration, sql string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()
	doneCh := make(chan error, 1)

	go func() {
		_, err := tk.Exec(sql)
		doneCh <- err
	}()

	select {
	case e := <-doneCh:
		// Exit normally
		return false, e
	case <-ctx.Done():
		// Exceed given timeout
		logutil.BgLogger().Info("execWithTimeout meet timeout", zap.String("sql", sql))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/BatchAddTiFlashSendDone", "return(true)"))
	}

	e := <-doneCh
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/BatchAddTiFlashSendDone"))
	return true, e
}

func TestTiFlashBatchRateLimiter(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	threshold := 2
	tk.MustExec("create database tiflash_ddl_limit")
	tk.MustExec(fmt.Sprintf("set SESSION tidb_batch_pending_tiflash_count=%v", threshold))
	for i := 0; i < threshold; i++ {
		tk.MustExec(fmt.Sprintf("create table tiflash_ddl_limit.t%v(z int)", i))
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/PollTiFlashReplicaStatusReplaceCurAvailableValue", `return(false)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/PollTiFlashReplicaStatusReplaceCurAvailableValue"))
	}()

	tk.MustExec("alter database tiflash_ddl_limit set tiflash replica 1")
	tk.MustExec(fmt.Sprintf("create table tiflash_ddl_limit.t%v(z int)", threshold))
	// The following statement shall fail, because it reaches limit
	timeOut, err := execWithTimeout(t, tk, time.Second*1, "alter database tiflash_ddl_limit set tiflash replica 1")
	require.NoError(t, err)
	require.True(t, timeOut)

	// There must be one table with no TiFlashReplica.
	check := func(expected int, total int) {
		cnt := 0
		for i := 0; i < total; i++ {
			tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("tiflash_ddl_limit"), model.NewCIStr(fmt.Sprintf("t%v", i)))
			require.NoError(t, err)
			if tb.Meta().TiFlashReplica != nil {
				cnt += 1
			}
		}
		require.Equal(t, expected, cnt)
	}
	check(2, 3)

	// If we exec in another session, it will not trigger limit. Since DefTiDBBatchPendingTiFlashCount is more than 3.
	tk2 := testkit.NewTestKit(t, s.store)
	tk2.MustExec("alter database tiflash_ddl_limit set tiflash replica 1")
	check(3, 3)

	loop := 3
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/FastFailCheckTiFlashPendingTables", fmt.Sprintf("return(%v)", loop)))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/FastFailCheckTiFlashPendingTables"))
	}()
	// We will force trigger its DDL to update schema cache.
	tk.MustExec(fmt.Sprintf("create table tiflash_ddl_limit.t%v(z int)", threshold+1))
	timeOut, err = execWithTimeout(t, tk, time.Millisecond*time.Duration(200*(loop+1)), "alter database tiflash_ddl_limit set tiflash replica 1")
	require.NoError(t, err)
	require.False(t, timeOut)
	check(4, 4)

	// However, forceCheck is true, so we will still enter try loop.
	tk.MustExec(fmt.Sprintf("create table tiflash_ddl_limit.t%v(z int)", threshold+2))
	timeOut, err = execWithTimeout(t, tk, time.Millisecond*200, "alter database tiflash_ddl_limit set tiflash replica 1")
	require.NoError(t, err)
	require.True(t, timeOut)
	check(4, 5)

	// Retrigger, but close session before the whole job ends.
	var wg util.WaitGroupWrapper
	var mu sync.Mutex
	wg.Run(func() {
		time.Sleep(time.Millisecond * 20)
		mu.Lock()
		defer mu.Unlock()
		tk.Session().Close()
		logutil.BgLogger().Info("session closed")
	})
	mu.Lock()
	timeOut, err = execWithTimeout(t, tk, time.Second*2, "alter database tiflash_ddl_limit set tiflash replica 1")
	mu.Unlock()
	require.NoError(t, err)
	require.False(t, timeOut)
	check(5, 5)
	wg.Wait()
}

func TestTiFlashBatchKill(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("create database tiflash_ddl_limit")
	tk.MustExec("set SESSION tidb_batch_pending_tiflash_count=0")
	tk.MustExec("create table tiflash_ddl_limit.t0(z int)")

	var wg util.WaitGroupWrapper
	wg.Run(func() {
		time.Sleep(time.Millisecond * 100)
		sessVars := tk.Session().GetSessionVars()
		atomic.StoreUint32(&sessVars.Killed, 1)
	})

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/FastFailCheckTiFlashPendingTables", `return(2)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/FastFailCheckTiFlashPendingTables"))
	}()
	timeOut, err := execWithTimeout(t, tk, time.Second*2000, "alter database tiflash_ddl_limit set tiflash replica 1")
	require.Error(t, err, "[executor:1317]Query execution was interrupted")
	require.False(t, timeOut)
	wg.Wait()
}

func TestTiFlashBatchUnsupported(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("create database tiflash_ddl_view")
	tk.MustExec("create table tiflash_ddl_view.t(z int)")
	tk.MustExec("insert into tiflash_ddl_view.t values (1)")
	tk.MustExec("CREATE VIEW tiflash_ddl_view.v AS select * from tiflash_ddl_view.t")
	tk.MustExec("alter database tiflash_ddl_view set tiflash replica 1")
	require.Equal(t, "In total 2 tables: 1 succeed, 0 failed, 1 skipped", tk.Session().GetSessionVars().StmtCtx.GetMessage())
	tk.MustGetErrCode("alter database information_schema set tiflash replica 1", 8200)
}
