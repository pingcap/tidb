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

package tiflashtest

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store/gcworker"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/teststore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/testkit/testflag"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/constants"
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

	pollInterval := 1000 * time.Millisecond
	if !testflag.Long() {
		pollInterval = 200 * time.Millisecond
	}
	ddl.PollTiFlashInterval = pollInterval
	ddl.PullTiFlashPdTick.Store(60)
	s.tiflash = infosync.NewMockTiFlash()
	s.store, err = teststore.NewMockStoreWithoutBootstrap(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < 2 {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				s.tiflash.AddStore(store2, addr2)
				mockCluster.AddStore(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
				tiflashIdx++
			}
			s.cluster = mockCluster
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)

	require.NoError(t, err)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	infosync.SetMockTiFlash(s.tiflash)
	require.NoError(t, err)
	s.dom.SetStatsUpdating(true)

	tearDown := func() {
		// Stop background TiFlash polling before tearing down the mock TiFlash HTTP server.
		ddl.DisableTiFlashPoll(s.dom.DDL())
		s.dom.Close()
		s.tiflash.Lock()
		s.tiflash.StatusServer.Close()
		s.tiflash.Unlock()
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
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")

	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	require.NotNil(t, tb)
	if tb.Meta().Partition != nil {
		for _, e := range tb.Meta().Partition.Definitions {
			ruleName := infosync.MakeRuleID(e.ID)
			_, ok := s.tiflash.GetPlacementRule(ruleName)
			require.True(t, ok)
		}
	} else {
		ruleName := infosync.MakeRuleID(tb.Meta().ID)
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

	rpcClient, pdClient, cluster, err := unistore.New("", nil, constants.NullKeyspaceID, nil)
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

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/gcworker/ignoreDeleteRangeFailed", `return`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/ignoreDeleteRangeFailed"))
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
	waitPlacementRulesLen(t, s.tiflash, total)

	tk.MustExec("alter table ddltiflashp set tiflash replica 1")
	total += 3
	waitPlacementRulesLen(t, s.tiflash, total)

	lessThan := 40
	tk.MustExec(fmt.Sprintf("ALTER TABLE ddltiflashp ADD PARTITION (PARTITION pn VALUES LESS THAN (%v))", lessThan))
	total += 1
	waitPlacementRulesLen(t, s.tiflash, total)

	tk.MustExec("alter table ddltiflashp truncate partition p1")
	total += 1
	waitPlacementRulesLen(t, s.tiflash, total)
	// Now gc will trigger, and will remove dropped partition.
	require.NoError(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))
	total -= 1
	waitPlacementRulesLen(t, s.tiflash, total)

	tk.MustExec("alter table ddltiflashp drop partition p2")
	require.NoError(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))
	total -= 1
	waitPlacementRulesLen(t, s.tiflash, total)

	tk.MustExec("truncate table ddltiflash")
	total += 1
	waitPlacementRulesLen(t, s.tiflash, total)
	require.NoError(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))
	total -= 1
	waitPlacementRulesLen(t, s.tiflash, total)

	tk.MustExec("drop table ddltiflash")
	total -= 1
	require.NoError(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))
	waitPlacementRulesLen(t, s.tiflash, total)
}

func TestTiFlashReplicaPartitionTableNormal(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10),PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30))")

	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	replica := tb.Meta().TiFlashReplica
	require.Nil(t, replica)

	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	lessThan := "40"
	tk.MustExec(fmt.Sprintf("ALTER TABLE ddltiflash ADD PARTITION (PARTITION pn VALUES LESS THAN (%v))", lessThan))

	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")

	tb2, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
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
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")

	lessThan := "40"
	// Stop loop
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/BeforeRefreshTiFlashTickerLoop", `return`))
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/BeforeRefreshTiFlashTickerLoop")
	}()

	tk.MustExec(fmt.Sprintf("ALTER TABLE ddltiflash ADD PARTITION (PARTITION pn VALUES LESS THAN (%v))", lessThan))
	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
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
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")

	tk.MustExec("drop table if exists ddltiflash2")
	tk.MustExec("create table ddltiflash2 like ddltiflash")
	tk.MustExec("alter table ddltiflash2 set tiflash replica 1")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash2")

	s.CheckFlashback(tk, t)
	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	r, ok := s.tiflash.GetPlacementRule(infosync.MakeRuleID(tb.Meta().ID))
	require.NotNil(t, r)
	require.True(t, ok)
	tk.MustExec("alter table ddltiflash set tiflash replica 0")
	waitTableNoReplicaWithTableName(s.dom, t, "test", "ddltiflash")
	require.Eventually(t, func() bool {
		_, ok := s.tiflash.GetPlacementRule(infosync.MakeRuleID(tb.Meta().ID))
		return !ok
	}, 10*time.Second, 20*time.Millisecond)
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
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
	tk.MustExec("insert into ddltiflash values(1, 'abc'), (11, 'def')")
	tk.MustExec("alter table ddltiflash truncate partition p1")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
}

// Fail truncate partition.
func TestTiFlashFailTruncatePartition(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 3")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(i int not null, s varchar(255)) partition by range (i) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/FailTiFlashTruncatePartition", `return`))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/FailTiFlashTruncatePartition")
	}()
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")

	tk.MustExec("insert into ddltiflash values(1, 'abc'), (11, 'def')")
	tk.MustGetErrMsg("alter table ddltiflash truncate partition p1", "[ddl:-1]DDL job rollback, error msg: enforced error")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
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
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
	tk.MustExec("alter table ddltiflash drop partition p1")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
}

func TestTiFlashFlashbackCluster(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	tk.MustExec("alter table t set tiflash replica 1")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "t")

	injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))

	ChangeGCSafePoint(tk, time.Now().Add(-10*time.Second), "true", "10m0s")
	defer func() {
		ChangeGCSafePoint(tk, time.Now(), "true", "10m0s")
	}()

	errorMsg := fmt.Sprintf("[ddl:-1]Detected unsupported DDL job type(%s) during [%s, now), can't do flashback",
		model.ActionSetTiFlashReplica.String(), oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat))
	tk.MustGetErrMsg(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)), errorMsg)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
}

func CheckTableAvailableWithTableName(dom *domain.Domain, t *testing.T, count uint64, labels []string, db string, table string) {
	tb, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr(db), ast.NewCIStr(table))
	require.NoError(t, err)
	replica := tb.Meta().TiFlashReplica
	require.NotNil(t, replica)
	require.True(t, replica.Available)
	require.Equal(t, count, replica.Count)
	require.ElementsMatch(t, labels, replica.LocationLabels)
}

func sameStringSet(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int, len(a))
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		if m[s] == 0 {
			return false
		}
		m[s]--
	}
	for _, v := range m {
		if v != 0 {
			return false
		}
	}
	return true
}

func isTableAvailableWithTableName(dom *domain.Domain, count uint64, labels []string, db string, table string) bool {
	tb, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr(db), ast.NewCIStr(table))
	if err != nil {
		return false
	}
	replica := tb.Meta().TiFlashReplica
	if replica == nil {
		return false
	}
	if !replica.Available {
		return false
	}
	if replica.Count != count {
		return false
	}
	return sameStringSet(replica.LocationLabels, labels)
}

func waitTableAvailableWithTableName(dom *domain.Domain, t *testing.T, count uint64, labels []string, db string, table string) {
	require.Eventually(t, func() bool {
		return isTableAvailableWithTableName(dom, count, labels, db, table)
	}, 10*time.Second, 20*time.Millisecond)
}

func isTableNoReplicaWithTableName(dom *domain.Domain, db string, table string) bool {
	tb, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr(db), ast.NewCIStr(table))
	if err != nil {
		return false
	}
	return tb.Meta().TiFlashReplica == nil
}

func waitTableNoReplicaWithTableName(dom *domain.Domain, t *testing.T, db string, table string) {
	require.Eventually(t, func() bool {
		return isTableNoReplicaWithTableName(dom, db, table)
	}, 10*time.Second, 20*time.Millisecond)
}

func waitPlacementRulesLen(t *testing.T, tiflash *infosync.MockTiFlash, expected int) {
	require.Eventually(t, func() bool {
		return tiflash.PlacementRulesLen() == expected
	}, 10*time.Second, 20*time.Millisecond)
}

func waitTiFlashProgress(t *testing.T, tableID int64, expected float64) {
	require.Eventually(t, func() bool {
		progress, exists := infosync.GetTiFlashProgressFromCache(tableID)
		return exists && progress == expected
	}, 10*time.Second, 20*time.Millisecond)
}

func CheckTableAvailable(dom *domain.Domain, t *testing.T, count uint64, labels []string) {
	CheckTableAvailableWithTableName(dom, t, count, labels, "test", "ddltiflash")
}

func CheckTableNoReplica(dom *domain.Domain, t *testing.T, db string, table string) {
	tb, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr(db), ast.NewCIStr(table))
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

	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflashp")
	tk.MustExec("truncate table ddltiflashp")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflashp")
	tk.MustExec("drop table if exists ddltiflash2")
	tk.MustExec("create table ddltiflash2(z int)")
	tk.MustExec("alter table ddltiflash2 set tiflash replica 1")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash2")

	tk.MustExec("truncate table ddltiflash2")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash2")
}

// TiFlash Table shall be eventually available, even with lots of small table created.
func TestTiFlashMassiveReplicaAvailable(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tableCount := 100
	timeout := 30 * time.Second
	if !testflag.Long() {
		tableCount = 20
		timeout = 10 * time.Second
	}

	tk.MustExec("use test")
	for i := range tableCount {
		tk.MustExec(fmt.Sprintf("drop table if exists ddltiflash%v", i))
		tk.MustExec(fmt.Sprintf("create table ddltiflash%v(z int)", i))
		tk.MustExec(fmt.Sprintf("alter table ddltiflash%v set tiflash replica 1", i))
	}

	require.Eventually(t, func() bool {
		for i := 0; i < tableCount; i++ {
			if !isTableAvailableWithTableName(s.dom, 1, []string{}, "test", fmt.Sprintf("ddltiflash%v", i)) {
				return false
			}
		}
		return true
	}, timeout, 50*time.Millisecond)
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
	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	expectRule := infosync.MakeNewRule(tb.Meta().ID, 1, []string{"a", "b"})
	res := s.tiflash.CheckPlacementRule(expectRule)
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
	res = s.tiflash.CheckPlacementRule(expectRule)
	require.True(t, res)
}

// When gc worker works, it will automatically remove pd rule for TiFlash.

func TestSetPlacementRuleWithGCWorker(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()

	rpcClient, pdClient, cluster, err := unistore.New("", nil, constants.NullKeyspaceID, nil)
	defer func() {
		rpcClient.Close()
		pdClient.Close()
		cluster.Close()
	}()
	for _, store := range s.cluster.GetAllStores() {
		cluster.AddStore(store.Id, store.Address, store.Labels...)
	}
	failpoint.Enable("github.com/pingcap/tidb/pkg/store/gcworker/ignoreDeleteRangeFailed", `return`)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/ignoreDeleteRangeFailed")
	}()
	fCancelPD := s.SetPdLoop(10000)
	defer fCancelPD()

	require.NoError(t, err)
	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	require.NoError(t, err)

	fCancel := TempDisableEmulatorGC()
	defer fCancel()

	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash_gc")
	tk.MustExec("create table ddltiflash_gc(z int)")
	tk.MustExec("alter table ddltiflash_gc set tiflash replica 1 location labels 'a','b'")
	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash_gc"))
	require.NoError(t, err)

	expectRule := infosync.MakeNewRule(tb.Meta().ID, 1, []string{"a", "b"})
	res := s.tiflash.CheckPlacementRule(expectRule)
	require.True(t, res)

	ChangeGCSafePoint(tk, time.Now().Add(-time.Hour), "true", "10m0s")
	tk.MustExec("drop table ddltiflash_gc")
	// Now gc will trigger, and will remove dropped table.
	require.Nil(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))

	require.Eventually(t, func() bool {
		return !s.tiflash.CheckPlacementRule(expectRule)
	}, 10*time.Second, 20*time.Millisecond)
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
	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
	require.NoError(t, err)

	expectRule := infosync.MakeNewRule(tb.Meta().ID, 1, []string{})
	res := s.tiflash.CheckPlacementRule(expectRule)
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
	for range 20 {
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/PollTiFlashReplicaStatusReplacePrevAvailableValue", `return(false)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/PollTiFlashReplicaStatusReplaceCurAvailableValue", `return(false)`))
	ddl.EnableTiFlashPoll(s.dom.DDL())
	tk.MustExec("alter table ddltiflash set tiflash replica 1")

	// 1, 1.5, 2.25, 3.375, 5.5625
	// (1), 1, 1, 2, 3, 5
	require.Eventually(t, func() bool {
		tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
		if err != nil {
			return false
		}
		replica := tb.Meta().TiFlashReplica
		return replica != nil && !replica.Available
	}, 10*time.Second, 20*time.Millisecond)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/PollTiFlashReplicaStatusReplacePrevAvailableValue"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/PollTiFlashReplicaStatusReplaceCurAvailableValue"))

	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
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
	waitTableAvailableWithTableName(s.dom, t, 2, []string{}, "tiflash_ddl", "ddltiflash")
	waitTableAvailableWithTableName(s.dom, t, 2, []string{}, "tiflash_ddl", "ddltiflash2")

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

	// Test Issue #51990, alter database skip set tiflash replica on sequence and view.
	tk.MustExec("create database tiflash_ddl_skip;")
	tk.MustExec("use tiflash_ddl_skip")
	tk.MustExec("create table t (id int);")
	tk.MustExec("create sequence t_seq;")
	tk.MustExec("create view t_view as select id from t;")
	tk.MustExec("create global temporary table t_temp (id int) on commit delete rows;")
	tk.MustExec("alter database tiflash_ddl_skip set tiflash replica 1;")
	require.Equal(t, "In total 4 tables: 1 succeed, 0 failed, 3 skipped", tk.Session().GetSessionVars().StmtCtx.GetMessage())
	tk.MustQuery(`show warnings;`).Sort().Check(testkit.Rows(
		"Note 1347 'tiflash_ddl_skip.t_seq' is not BASE TABLE",
		"Note 1347 'tiflash_ddl_skip.t_view' is not BASE TABLE",
		"Note 8006 `set TiFlash replica` is unsupported on temporary tables."))
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
		logutil.DDLLogger().Info("execWithTimeout meet timeout", zap.String("sql", sql))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/BatchAddTiFlashSendDone", "return(true)"))
	}

	e := <-doneCh
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/BatchAddTiFlashSendDone"))
	return true, e
}

func TestTiFlashBatchRateLimiter(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	threshold := 2
	tk.MustExec("create database tiflash_ddl_limit")
	tk.MustExec(fmt.Sprintf("set SESSION tidb_batch_pending_tiflash_count=%v", threshold))
	for i := range threshold {
		tk.MustExec(fmt.Sprintf("create table tiflash_ddl_limit.t%v(z int)", i))
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/PollTiFlashReplicaStatusReplaceCurAvailableValue", `return(false)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/PollTiFlashReplicaStatusReplaceCurAvailableValue"))
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
		for i := range total {
			tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("tiflash_ddl_limit"), ast.NewCIStr(fmt.Sprintf("t%v", i)))
			require.NoError(t, err)
			if tb.Meta().TiFlashReplica != nil {
				cnt++
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/FastFailCheckTiFlashPendingTables", fmt.Sprintf("return(%v)", loop)))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/FastFailCheckTiFlashPendingTables"))
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
		logutil.DDLLogger().Info("session closed")
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
		sessVars.SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	})

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/FastFailCheckTiFlashPendingTables", `return(2)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/FastFailCheckTiFlashPendingTables"))
	}()
	timeOut, err := execWithTimeout(t, tk, time.Second*2000, "alter database tiflash_ddl_limit set tiflash replica 1")
	require.ErrorContains(t, err, "[executor:1317]Query execution was interrupted")
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

func TestTiFlashProgress(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	s.tiflash.NotAvailable = true
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("create database tiflash_d")
	tk.MustExec("create table tiflash_d.t(z int)")
	tk.MustExec("alter table tiflash_d.t set tiflash replica 1")
	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("tiflash_d"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, tb)
	mustExist := func(tid int64) {
		_, isExist := infosync.GetTiFlashProgressFromCache(tid)
		require.True(t, isExist)
	}
	mustAbsent := func(tid int64) {
		_, isExist := infosync.GetTiFlashProgressFromCache(tid)
		require.False(t, isExist)
	}
	infosync.UpdateTiFlashProgressCache(tb.Meta().ID, 5.0)
	mustExist(tb.Meta().ID)
	_ = infosync.DeleteTiFlashTableSyncProgress(tb.Meta())
	mustAbsent(tb.Meta().ID)

	infosync.UpdateTiFlashProgressCache(tb.Meta().ID, 5.0)
	tk.MustExec("truncate table tiflash_d.t")
	mustAbsent(tb.Meta().ID)

	tb, _ = s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("tiflash_d"), ast.NewCIStr("t"))
	infosync.UpdateTiFlashProgressCache(tb.Meta().ID, 5.0)
	tk.MustExec("alter table tiflash_d.t set tiflash replica 0")
	mustAbsent(tb.Meta().ID)
	tk.MustExec("alter table tiflash_d.t set tiflash replica 1")

	tb, _ = s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("tiflash_d"), ast.NewCIStr("t"))
	infosync.UpdateTiFlashProgressCache(tb.Meta().ID, 5.0)
	tk.MustExec("drop table tiflash_d.t")
	mustAbsent(tb.Meta().ID)

	time.Sleep(100 * time.Millisecond)
}

func TestTiFlashProgressForPartitionTable(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	s.tiflash.NotAvailable = true
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("create database tiflash_d")
	tk.MustExec("create table tiflash_d.t(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10))")
	tk.MustExec("alter table tiflash_d.t set tiflash replica 1")
	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("tiflash_d"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, tb)
	mustExist := func(tid int64) {
		_, isExist := infosync.GetTiFlashProgressFromCache(tid)
		require.True(t, isExist)
	}
	mustAbsent := func(tid int64) {
		_, isExist := infosync.GetTiFlashProgressFromCache(tid)
		require.False(t, isExist)
	}
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailable)
	mustExist(tb.Meta().Partition.Definitions[0].ID)
	_ = infosync.DeleteTiFlashTableSyncProgress(tb.Meta())
	mustAbsent(tb.Meta().Partition.Definitions[0].ID)

	infosync.UpdateTiFlashProgressCache(tb.Meta().Partition.Definitions[0].ID, 5.0)
	tk.MustExec("truncate table tiflash_d.t")
	mustAbsent(tb.Meta().Partition.Definitions[0].ID)

	tb, _ = s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("tiflash_d"), ast.NewCIStr("t"))
	infosync.UpdateTiFlashProgressCache(tb.Meta().Partition.Definitions[0].ID, 5.0)
	tk.MustExec("alter table tiflash_d.t set tiflash replica 0")
	mustAbsent(tb.Meta().Partition.Definitions[0].ID)
	tk.MustExec("alter table tiflash_d.t set tiflash replica 1")

	tb, _ = s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("tiflash_d"), ast.NewCIStr("t"))
	infosync.UpdateTiFlashProgressCache(tb.Meta().Partition.Definitions[0].ID, 5.0)
	tk.MustExec("drop table tiflash_d.t")
	mustAbsent(tb.Meta().Partition.Definitions[0].ID)

	time.Sleep(100 * time.Millisecond)
}

func TestTiFlashGroupIndexWhenStartup(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	tiflash := s.tiflash
	defer teardown()
	_ = testkit.NewTestKit(t, s.store)
	timeout := time.Now().Add(10 * time.Second)
	errMsg := "time out"
	for time.Now().Before(timeout) {
		time.Sleep(100 * time.Millisecond)
		if tiflash.GetRuleGroupIndex() != 0 {
			errMsg = "invalid group index"
			break
		}
	}
	require.Equal(t, placement.RuleIndexTiFlash, tiflash.GetRuleGroupIndex(), errMsg)
	require.Greater(t, tiflash.GetRuleGroupIndex(), placement.RuleIndexTable)
	require.Greater(t, tiflash.GetRuleGroupIndex(), placement.RuleIndexPartition)
}

func TestTiFlashFailureProgressAfterAvailable(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int)")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")

	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	require.NotNil(t, tb)
	// after available, progress should can be updated.
	// s.tiflash.ResetSyncStatus(int(tb.Meta().ID), false)

	s.tiflash.SetNetworkError(true)
	pool := s.dom.SysSessionPool()
	se, err := pool.Get()
	require.NoError(t, err)
	sctx := se.(sessionctx.Context)
	defer pool.Put(se)
	pollTiflashContext, err := ddl.NewTiFlashManagementContext()
	pollTiflashContext.UpdatingProgressTables.PushBack(ddl.AvailableTableID{
		ID:          tb.Meta().ID,
		IsPartition: false,
	})
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ddl.PollAvailableTableProgress(s.dom.InfoSchema(), sctx, pollTiflashContext)
	}()

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return
	case <-time.After(time.Second):
		panic("DDL can't finish")
	}
}

func TestTiFlashProgressAfterAvailable(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int)")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")

	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	require.NotNil(t, tb)
	// after available, progress should can be updated.
	s.tiflash.ResetSyncStatus(int(tb.Meta().ID), false)
	waitTiFlashProgress(t, tb.Meta().ID, 0)

	s.tiflash.ResetSyncStatus(int(tb.Meta().ID), true)
	waitTiFlashProgress(t, tb.Meta().ID, 1)
}

func TestTiFlashProgressAfterAvailableForPartitionTable(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10))")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")

	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	require.NotNil(t, tb)
	// after available, progress should can be updated.
	s.tiflash.ResetSyncStatus(int(tb.Meta().Partition.Definitions[0].ID), false)
	waitTiFlashProgress(t, tb.Meta().Partition.Definitions[0].ID, 0)

	s.tiflash.ResetSyncStatus(int(tb.Meta().Partition.Definitions[0].ID), true)
	waitTiFlashProgress(t, tb.Meta().Partition.Definitions[0].ID, 1)
}

func TestTiFlashProgressCache(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int)")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")

	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	require.NotNil(t, tb)
	infosync.UpdateTiFlashProgressCache(tb.Meta().ID, 0)
	// after available, it will still update progress cache.
	waitTiFlashProgress(t, tb.Meta().ID, 1)
}

func TestTiFlashProgressAvailableList(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tableCount := 8
	if !testflag.Long() {
		tableCount = 4
	}
	tableNames := make([]string, tableCount)
	tableIDs := make([]int64, tableCount)

	tk.MustExec("use test")
	for i := range tableCount {
		tableNames[i] = fmt.Sprintf("ddltiflash%d", i)
		tk.MustExec(fmt.Sprintf("drop table if exists %s", tableNames[i]))
		tk.MustExec(fmt.Sprintf("create table %s(z int)", tableNames[i]))
		tk.MustExec(fmt.Sprintf("alter table %s set tiflash replica 1", tableNames[i]))
	}
	require.Eventually(t, func() bool {
		for i := range tableCount {
			if !isTableAvailableWithTableName(s.dom, 1, []string{}, "test", tableNames[i]) {
				return false
			}
		}
		return true
	}, 10*time.Second, 20*time.Millisecond)

	// After available, reset TiFlash sync status.
	for i := range tableCount {
		tbl, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tableNames[i]))
		require.NoError(t, err)
		require.NotNil(t, tbl)
		tableIDs[i] = tbl.Meta().ID
		s.tiflash.ResetSyncStatus(int(tableIDs[i]), false)
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/PollAvailableTableProgressMaxCount", `return(2)`))
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/PollAvailableTableProgressMaxCount")
	}()

	countUpdated := func() (updated int, ok bool) {
		for _, id := range tableIDs {
			progress, exists := infosync.GetTiFlashProgressFromCache(id)
			if !exists {
				return 0, false
			}
			if progress == 0 {
				updated++
			}
		}
		return updated, true
	}
	require.Eventually(t, func() bool {
		updated, ok := countUpdated()
		return ok && updated > 0 && updated < tableCount
	}, 10*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		updated, ok := countUpdated()
		return ok && updated == tableCount
	}, 10*time.Second, 20*time.Millisecond)
}

func TestTiFlashAvailableAfterResetReplica(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int)")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"))
	}()

	tk.MustExec("alter table ddltiflash set tiflash replica 2")
	waitTableAvailableWithTableName(s.dom, t, 2, []string{}, "test", "ddltiflash")

	tk.MustExec("alter table ddltiflash set tiflash replica 0")
	waitTableNoReplicaWithTableName(s.dom, t, "test", "ddltiflash")
}

func TestTiFlashPartitionNotAvailable(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10))")

	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	require.NotNil(t, tb)
	partID := tb.Meta().Partition.Definitions[0].ID

	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	s.tiflash.ResetSyncStatus(int(partID), false)
	require.Eventually(t, func() bool {
		tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
		if err != nil {
			return false
		}
		replica := tb.Meta().TiFlashReplica
		return replica != nil && !replica.Available
	}, 10*time.Second, 20*time.Millisecond)

	s.tiflash.ResetSyncStatus(int(partID), true)
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")

	s.tiflash.ResetSyncStatus(int(partID), false)
	stableChecks := 3
	timeout := time.Second
	if testflag.Long() {
		stableChecks = 10
		timeout = 3 * time.Second
	}
	consecutive := 0
	require.Eventually(t, func() bool {
		tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
		if err != nil {
			consecutive = 0
			return false
		}
		replica := tb.Meta().TiFlashReplica
		if replica == nil || !replica.Available {
			consecutive = 0
			return false
		}
		consecutive++
		return consecutive >= stableChecks
	}, timeout, 20*time.Millisecond)
}

func TestTiFlashAvailableAfterAddPartition(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10))")
	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")

	tb, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	require.NotNil(t, tb)

	// still available after adding partition.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/sleepBeforeReplicaOnly", `return(2)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/waitForAddPartition", `return(3)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/PollTiFlashReplicaStatusReplaceCurAvailableValue", `return(false)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/sleepBeforeReplicaOnly"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/waitForAddPartition"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/PollTiFlashReplicaStatusReplaceCurAvailableValue"))
	}()
	tk.MustExec("ALTER TABLE ddltiflash ADD PARTITION (PARTITION pn VALUES LESS THAN (20))")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
	tb, err = s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ddltiflash"))
	require.NoError(t, err)
	pi := tb.Meta().GetPartitionInfo()
	require.NotNil(t, pi)
	require.Equal(t, len(pi.Definitions), 2)
}

func TestTiFlashAvailableAfterDownOneStore(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")
	tk.MustExec("create table ddltiflash(z int) PARTITION BY RANGE(z) (PARTITION p0 VALUES LESS THAN (10))")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/OneTiFlashStoreDown", `return`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/infosync/OneTiFlashStoreDown", `return`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/OneTiFlashStoreDown"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/infosync/OneTiFlashStoreDown"))
	}()

	tk.MustExec("alter table ddltiflash set tiflash replica 1")
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
}

func TestTiFlashReorgPartition(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	fCancel := TempDisableEmulatorGC()
	defer fCancel()
	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddltiflash")

	tk.MustExec(`create table ddltiflash (id int, vc varchar(255), i int, key (vc), key(i,vc))` +
		` partition by range (id)` +
		` (partition p0 values less than (1000000), partition p1 values less than (2000000))`)
	tk.MustExec(`alter table ddltiflash set tiflash replica 1`)
	waitTableAvailableWithTableName(s.dom, t, 1, []string{}, "test", "ddltiflash")
	tb := external.GetTableByName(t, tk, "test", "ddltiflash")
	firstPartitionID := tb.Meta().Partition.Definitions[0].ID
	ruleName := fmt.Sprintf("table-%v-r", firstPartitionID)
	_, ok := s.tiflash.GetPlacementRule(ruleName)
	require.True(t, ok)

	// Note that the mock TiFlash does not have any data or regions, so the wait for regions being available will fail
	done := false

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if !done && job.Type == model.ActionReorganizePartition && job.SchemaState == model.StateDeleteOnly {
			// Let it fail once (to check that code path) then increase the count to skip retry
			if job.ErrorCount > 0 {
				job.ErrorCount = 1000
				done = true
			}
		}
	})
	tk.MustContainErrMsg(`alter table ddltiflash reorganize partition p0 into (partition p0 values less than (500000), partition p500k values less than (1000000))`, "[ddl] add partition wait for tiflash replica to complete")

	done = false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if !done && job.Type == model.ActionReorganizePartition && job.SchemaState == model.StateDeleteOnly {
			// Let it fail once (to check that code path) then mock the regions into the partitions
			if job.ErrorCount > 0 {
				// Add the tiflash stores as peers for the new regions, to fullfil the check
				// in checkPartitionReplica
				pdCli := s.store.(tikv.Storage).GetRegionCache().PDClient()
				args, err := model.GetTablePartitionArgs(job)
				require.NoError(t, err)
				ctx := context.Background()
				stores, _ := pdCli.GetAllStores(ctx)
				for _, pDef := range args.PartInfo.Definitions {
					startKey, endKey := tablecodec.GetTableHandleKeyRange(pDef.ID)
					regions, _ := pdCli.BatchScanRegions(ctx, []router.KeyRange{{StartKey: startKey, EndKey: endKey}}, -1)
					for i := range regions {
						// similar as storeHasEngineTiFlashLabel
						for _, store := range stores {
							for _, label := range store.Labels {
								if label.Key == placement.EngineLabelKey && label.Value == placement.EngineLabelTiFlash {
									s.cluster.MockRegionManager.AddPeer(regions[i].Meta.Id, store.Id, 1)
									break
								}
							}
						}
					}
				}
				done = true
			}
		}
	})
	tk.MustExec(`alter table ddltiflash reorganize partition p0 into (partition p0 values less than (500000), partition p500k values less than (1000000))`)
	tk.MustExec(`admin check table ddltiflash`)
	_, ok = s.tiflash.GetPlacementRule(ruleName)
	require.True(t, ok)
	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	require.NoError(t, err)
	require.Nil(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))
	_, ok = s.tiflash.GetPlacementRule(ruleName)
	require.False(t, ok)
	tk.MustExec(`drop table ddltiflash`)
}
