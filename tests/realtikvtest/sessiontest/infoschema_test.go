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
