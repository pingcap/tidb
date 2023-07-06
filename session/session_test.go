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

package session_test

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestInitMetaTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, sql := range session.DDLJobTables {
		tk.MustExec(sql.SQL)
	}

	for _, sql := range session.BackfillTables {
		tk.MustExec(sql.SQL)
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
		require.True(t, reflect.DeepEqual(metaInMySQL, metaInTest))
	}
}

func TestMetaTableRegion(t *testing.T) {
	enableSplitTableRegionVal := atomic.LoadUint32(&ddl.EnableSplitTableRegion)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, enableSplitTableRegionVal)
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	ddlReorgTableRegionID := tk.MustQuery("show table mysql.tidb_ddl_reorg regions").Rows()[0][0]
	ddlReorgTableRegionStartKey := tk.MustQuery("show table mysql.tidb_ddl_reorg regions").Rows()[0][1]
	require.Equal(t, ddlReorgTableRegionStartKey, fmt.Sprintf("%s_%d_", tablecodec.TablePrefix(), ddl.ReorgTableID))

	ddlJobTableRegionID := tk.MustQuery("show table mysql.tidb_ddl_job regions").Rows()[0][0]
	ddlJobTableRegionStartKey := tk.MustQuery("show table mysql.tidb_ddl_job regions").Rows()[0][1]
	require.Equal(t, ddlJobTableRegionStartKey, fmt.Sprintf("%s_%d_", tablecodec.TablePrefix(), ddl.JobTableID))

	require.NotEqual(t, ddlJobTableRegionID, ddlReorgTableRegionID)

	ddlBackfillTableRegionID := tk.MustQuery("show table mysql.tidb_background_subtask regions").Rows()[0][0]
	ddlBackfillTableRegionStartKey := tk.MustQuery("show table mysql.tidb_background_subtask regions").Rows()[0][1]
	require.Equal(t, ddlBackfillTableRegionStartKey, fmt.Sprintf("%s_%d_", tablecodec.TablePrefix(), ddl.BackgroundSubtaskTableID))
	ddlBackfillHistoryTableRegionID := tk.MustQuery("show table mysql.tidb_background_subtask_history regions").Rows()[0][0]
	ddlBackfillHistoryTableRegionStartKey := tk.MustQuery("show table mysql.tidb_background_subtask_history regions").Rows()[0][1]
	require.Equal(t, ddlBackfillHistoryTableRegionStartKey, fmt.Sprintf("%s_%d_", tablecodec.TablePrefix(), ddl.BackgroundSubtaskHistoryTableID))

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
