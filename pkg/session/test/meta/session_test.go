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

package meta_test

import (
	"cmp"
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestInitDDLTables(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	allTables := append(append(append(append([]session.TableBasicInfo{},
		session.DDLJobTables...), session.MDLTables...),
		session.BackfillTables...), session.DDLNotifierTables...)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	for _, c := range []struct {
		initVer meta.DDLTableVersion
		tables  []session.TableBasicInfo
	}{
		{meta.InitDDLTableVersion, allTables},
		{meta.BaseDDLTableVersion, allTables[3:]},
		{meta.MDLTableVersion, allTables[4:]},
		{meta.BackfillTableVersion, allTables[6:]},
		{meta.DDLNotifierTableVersion, []session.TableBasicInfo{}},
	} {
		if c.initVer != meta.InitDDLTableVersion {
			require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
				m := meta.NewMutator(txn)
				require.NoError(t, m.SetDDLTableVersion(c.initVer))
				return nil
			}))
		}
		require.NoError(t, session.InitDDLTables(store))
		require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
			m := meta.NewMutator(txn)
			systemDBID, err2 := m.GetSystemDBID()
			require.NoError(t, err2)

			tables, err2 := m.ListTables(ctx, systemDBID)
			require.NoError(t, err2)
			require.Len(t, tables, len(c.tables))
			gotTables := make([]session.TableBasicInfo, 0, len(tables))
			for _, tbl := range tables {
				gotTables = append(gotTables, session.TableBasicInfo{ID: tbl.ID, Name: tbl.Name.L})
			}
			slices.SortFunc(gotTables, func(a, b session.TableBasicInfo) int {
				return cmp.Compare(b.ID, a.ID)
			})
			require.True(t, slices.EqualFunc(c.tables, gotTables, func(a, b session.TableBasicInfo) bool {
				return a.ID == b.ID && a.Name == b.Name
			}))
			postVer, err2 := m.GetDDLTableVersion()
			require.NoError(t, err2)
			require.Equal(t, meta.DDLNotifierTableVersion, postVer)

			require.NoError(t, m.SetDDLTableVersion(meta.InitDDLTableVersion))
			require.NoError(t, m.DropDatabase(systemDBID))
			return nil
		}))
	}
}

func TestInitMetaTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, sql := range session.DDLJobTables {
		theSQL := strings.Replace(sql.SQL, "mysql.", "", 1)
		tk.MustExec(theSQL)
	}

	for _, sql := range session.BackfillTables {
		theSQL := strings.Replace(sql.SQL, "mysql.", "", 1)
		tk.MustExec(theSQL)
	}

	tbls := map[string]struct{}{
		"tidb_ddl_job":                    {},
		"tidb_ddl_reorg":                  {},
		"tidb_ddl_history":                {},
		"tidb_background_subtask":         {},
		"tidb_background_subtask_history": {},
	}

	for tbl := range tbls {
		metaInMySQL := external.GetTableByName(t, tk, "mysql", tbl).Meta().Clone()
		metaInTest := external.GetTableByName(t, tk, "test", tbl).Meta().Clone()

		require.Greater(t, metaInMySQL.ID, int64(0))
		require.Greater(t, metaInMySQL.UpdateTS, uint64(0))

		metaInTest.ID = metaInMySQL.ID
		metaInMySQL.UpdateTS = metaInTest.UpdateTS
		metaInTest.DBID = 0
		metaInMySQL.DBID = 0
		require.True(t, reflect.DeepEqual(metaInMySQL, metaInTest))
	}
}

func TestMetaTableRegion(t *testing.T) {
	enableSplitTableRegionVal := atomic.LoadUint32(&ddl.EnableSplitTableRegion)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, enableSplitTableRegionVal)
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))

	tk := testkit.NewTestKit(t, store)

	ddlReorgTableRegionID := tk.MustQuery("show table mysql.tidb_ddl_reorg regions").Rows()[0][0]
	ddlReorgTableRegionStartKey := tk.MustQuery("show table mysql.tidb_ddl_reorg regions").Rows()[0][1]
	require.Equal(t, ddlReorgTableRegionStartKey, fmt.Sprintf("%s_%d_", tablecodec.TablePrefix(), metadef.TiDBDDLReorgTableID))

	ddlJobTableRegionID := tk.MustQuery("show table mysql.tidb_ddl_job regions").Rows()[0][0]
	ddlJobTableRegionStartKey := tk.MustQuery("show table mysql.tidb_ddl_job regions").Rows()[0][1]
	require.Equal(t, ddlJobTableRegionStartKey, fmt.Sprintf("%s_%d_", tablecodec.TablePrefix(), metadef.TiDBDDLJobTableID))

	require.NotEqual(t, ddlJobTableRegionID, ddlReorgTableRegionID)

	ddlBackfillTableRegionID := tk.MustQuery("show table mysql.tidb_background_subtask regions").Rows()[0][0]
	ddlBackfillTableRegionStartKey := tk.MustQuery("show table mysql.tidb_background_subtask regions").Rows()[0][1]
	require.Equal(t, ddlBackfillTableRegionStartKey, fmt.Sprintf("%s_%d_", tablecodec.TablePrefix(), metadef.TiDBBackgroundSubtaskTableID))
	ddlBackfillHistoryTableRegionID := tk.MustQuery("show table mysql.tidb_background_subtask_history regions").Rows()[0][0]
	ddlBackfillHistoryTableRegionStartKey := tk.MustQuery("show table mysql.tidb_background_subtask_history regions").Rows()[0][1]
	require.Equal(t, ddlBackfillHistoryTableRegionStartKey, fmt.Sprintf("%s_%d_", tablecodec.TablePrefix(), metadef.TiDBBackgroundSubtaskHistoryTableID))

	require.NotEqual(t, ddlBackfillTableRegionID, ddlBackfillHistoryTableRegionID)
}

func MustReadCounter(t *testing.T, m prometheus.Counter) float64 {
	pb := &dto.Metric{}
	require.NoError(t, m.Write(pb))
	return pb.GetCounter().GetValue()
}

func TestRecordTTLRows(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(created_at datetime) TTL = created_at + INTERVAL 1 DAY")
	// simple insert should be recorded
	tk.MustExec("insert into t values (NOW())")
	require.Equal(t, 1.0, MustReadCounter(t, metrics.TTLInsertRowsCount))

	// insert in a explicit transaction should be recorded
	tk.MustExec("begin")
	tk.MustExec("insert into t values (NOW())")
	tk.MustExec("commit")
	require.Equal(t, 2.0, MustReadCounter(t, metrics.TTLInsertRowsCount))

	// insert multiple rows should be the same
	tk.MustExec("begin")
	tk.MustExec("insert into t values (NOW())")
	tk.MustExec("insert into t values (NOW())")
	tk.MustExec("commit")
	require.Equal(t, 4.0, MustReadCounter(t, metrics.TTLInsertRowsCount))

	// rollback will remove all recorded TTL rows
	tk.MustExec("begin")
	tk.MustExec("insert into t values (NOW())")
	tk.MustExec("insert into t values (NOW())")
	tk.MustExec("rollback")
	require.Equal(t, 6.0, MustReadCounter(t, metrics.TTLInsertRowsCount))

	// savepoint will save the recorded TTL rows
	tk.MustExec("begin")
	tk.MustExec("insert into t values (NOW())")
	tk.MustExec("savepoint insert1")
	tk.MustExec("insert into t values (NOW())")
	tk.MustExec("rollback to insert1")
	tk.MustExec("commit")
	require.Equal(t, 7.0, MustReadCounter(t, metrics.TTLInsertRowsCount))
}

func TestInformationSchemaCreateTime(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c int)")
	tk.MustExec(`set @@time_zone = 'Asia/Shanghai'`)
	ret := tk.MustQuery("select create_time from information_schema.tables where table_name='t';")
	// Make sure t1 is greater than t.
	time.Sleep(time.Second)
	tk.MustExec("alter table t modify c int default 11")
	ret1 := tk.MustQuery("select create_time from information_schema.tables where table_name='t';")
	ret2 := tk.MustQuery("show table status like 't'")
	require.Equal(t, ret2.Rows()[0][11].(string), ret1.Rows()[0][0].(string))
	typ1, err := types.ParseDatetime(types.DefaultStmtNoWarningContext, ret.Rows()[0][0].(string))
	require.NoError(t, err)
	typ2, err := types.ParseDatetime(types.DefaultStmtNoWarningContext, ret1.Rows()[0][0].(string))
	require.NoError(t, err)
	r := typ2.Compare(typ1)
	require.Equal(t, 1, r)
	// Check that time_zone changes makes the create_time different
	tk.MustExec(`set @@time_zone = 'Europe/Amsterdam'`)
	ret = tk.MustQuery(`select create_time from information_schema.tables where table_name='t'`)
	ret2 = tk.MustQuery(`show table status like 't'`)
	require.Equal(t, ret2.Rows()[0][11].(string), ret.Rows()[0][0].(string))
	typ3, err := types.ParseDatetime(types.DefaultStmtNoWarningContext, ret.Rows()[0][0].(string))
	require.NoError(t, err)
	// Asia/Shanghai 2022-02-17 17:40:05 > Europe/Amsterdam 2022-02-17 10:40:05
	r = typ2.Compare(typ3)
	require.Equal(t, 1, r)
}

func TestNextgenBootstrap(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("This test is only for nextgen kernel.")
	}
	ctx := context.Background()
	_, dom := testkit.CreateMockStoreAndDomain(t)

	checkReservedIDFn := func(id int64, name string) {
		t.Helper()
		require.Greater(t, id, metadef.ReservedGlobalIDLowerBound, "the id of %s must be a reserved ID", name)
		require.LessOrEqual(t, id, metadef.ReservedGlobalIDUpperBound, "the id of %s must be a reserved ID", name)
	}

	is := dom.InfoSchema()
	var reservedSchemaCnt, reservedTableCnt int
	for _, sch := range is.AllSchemas() {
		if !metadef.IsSystemRelatedDB(sch.Name.L) {
			continue
		}
		reservedSchemaCnt++
		checkReservedIDFn(sch.ID, sch.Name.L)
		tblInfos, err := is.SchemaTableInfos(ctx, sch.Name)
		require.NoError(t, err)
		for _, tblInfo := range tblInfos {
			if !tblInfo.IsBaseTable() {
				continue
			}
			reservedTableCnt++
			checkReservedIDFn(tblInfo.ID, tblInfo.Name.L)
		}
	}
	require.EqualValues(t, 2, reservedSchemaCnt)
	require.EqualValues(t, 59, reservedTableCnt)
}
