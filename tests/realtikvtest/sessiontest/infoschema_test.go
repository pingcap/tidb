// Copyright 2025 PingCAP, Inc.
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

package sessiontest

import (
	"fmt"
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestTiKVRegionStatus(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, key idx(a));")
	tk.MustExec("split table t between (0) and (10000) regions 4")
	tk.MustExec("split table t index idx between (0) and (10000) regions 4")

	tableID := tk.MustQuery(`select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 't'`).Rows()[0][0]
	showRegions := uniqueSortedRegionIDs(tk.MustQuery("show table t regions").Rows())
	showIndexRegions := uniqueSortedRegionIDs(tk.MustQuery("show table t index idx regions").Rows())
	tikvRegions := tk.MustQuery(fmt.Sprintf(
		"select region_id from information_schema.tikv_region_status where table_id = %v", tableID)).Rows()
	tikvIndexRegions := tk.MustQuery(fmt.Sprintf(
		"select region_id from information_schema.tikv_region_status where table_id = %v and is_index = 1", tableID)).Rows()
	require.Equal(t, showRegions, uniqueSortedRegionIDs(tikvRegions))
	require.Equal(t, showIndexRegions, uniqueSortedRegionIDs(tikvIndexRegions))
}

func uniqueSortedRegionIDs(rows [][]any) []string {
	seen := make(map[string]struct{}, len(rows))
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		id := row[0].(string)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func TestTableReaderWithSnapshot(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	querySafePoint := "select variable_value from mysql.tidb where variable_name = 'tikv_gc_safe_point'"
	originalSafePoint := tk.MustQuery(querySafePoint).Rows()
	restoreSafePoint := func(tk *testkit.TestKit, rows [][]any) {
		// End the historical transaction before restoring the persistent row.
		tk.MustExec("rollback")
		tk.MustExec("set @@tidb_snapshot = ''")
		if len(rows) == 0 {
			tk.MustExec("delete from mysql.tidb where variable_name = 'tikv_gc_safe_point'")
		} else {
			tk.MustExec("update mysql.tidb set variable_value = ? where variable_name = 'tikv_gc_safe_point'", rows[0][0])
		}
	}
	t.Cleanup(func() { restoreSafePoint(tk, originalSafePoint) })

	for _, tc := range []struct {
		name          string
		seedSafePoint bool
	}{
		{name: "original prior state"},
		{name: "existing prior safe point", seedSafePoint: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.seedSafePoint {
				tk.MustExec("insert into mysql.tidb(variable_name, variable_value) values('tikv_gc_safe_point', ?) on duplicate key update variable_value = values(variable_value)", "20170102-15:04:05 -0700")
			}
			previousSafePoint := tk.MustQuery(querySafePoint).Rows()
			t.Run("historical metadata", func(t *testing.T) {
				tk := testkit.NewTestKit(t, store)
				// Child cleanups run before its session closes, while the parent
				// session and store remain available to check the persistent state.
				t.Cleanup(func() { restoreSafePoint(tk, previousSafePoint) })
				tk.MockGCSavePoint()

				tk.MustExec("use test")
				tk.MustExec("create table t(id int);")
				tk.MustExec("begin")
				tk.MustExec("set @ts := @@tidb_current_ts;")
				tk.MustExec("rollback")
				tk.MustQuery("select sleep(2);")
				tk.MustExec("drop table t;")
				tk.MustExec("begin")
				tk.MustExec("set @@tidb_snapshot=@ts;")
				tk.MustQuery("SELECT TABLE_NAME,TABLE_TYPE,AVG_ROW_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='test' AND (TABLE_TYPE='BASE TABLE')").Check(testkit.Rows("t BASE TABLE 0"))
			})
			require.Equal(t, previousSafePoint, tk.MustQuery(querySafePoint).Rows())
		})
	}
}
