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
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestInitMetaTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, sql := range session.DDLJobTables {
		tk.MustExec(sql.SQL)
	}

	tbls := map[string]struct{}{
		"tidb_ddl_job":     {},
		"tidb_ddl_reorg":   {},
		"tidb_ddl_history": {},
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

	ddlBackfillTableRegionID := tk.MustQuery("show table mysql.tidb_ddl_backfill regions").Rows()[0][0]
	ddlBackfillTableRegionStartKey := tk.MustQuery("show table mysql.tidb_ddl_backfill regions").Rows()[0][1]
	require.Equal(t, ddlBackfillTableRegionStartKey, fmt.Sprintf("%s_%d_", tablecodec.TablePrefix(), ddl.BackfillTableID))
	ddlBackfillHistoryTableRegionID := tk.MustQuery("show table mysql.tidb_ddl_backfill_history regions").Rows()[0][0]
	ddlBackfillHistoryTableRegionStartKey := tk.MustQuery("show table mysql.tidb_ddl_backfill_history regions").Rows()[0][1]
	require.Equal(t, ddlBackfillHistoryTableRegionStartKey, fmt.Sprintf("%s_%d_", tablecodec.TablePrefix(), ddl.BackfillHistoryTableID))

	require.NotEqual(t, ddlBackfillTableRegionID, ddlBackfillHistoryTableRegionID)
}
