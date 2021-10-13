// Copyright 2021 PingCAP, Inc.
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

package statistics_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestOutdatedStatsCheck(t *testing.T) {
	domain.RunAutoAnalyze = false
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")

	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 19)) // 20 rows
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	is := dom.InfoSchema()
	require.NoError(t, h.Update(is))
	// To pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 1")
	require.NoError(t, h.LoadNeededHistograms())

	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 13)) // 34 rows
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	require.False(t, hasPseudoStats(tk.MustQuery("explain select * from t where a = 1").Rows()))

	tk.MustExec("insert into t values (1)") // 35 rows
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	require.True(t, hasPseudoStats(tk.MustQuery("explain select * from t where a = 1").Rows()))

	tk.MustExec("analyze table t")

	tk.MustExec("delete from t limit 24") // 11 rows
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	require.False(t, hasPseudoStats(tk.MustQuery("explain select * from t where a = 1").Rows()))

	tk.MustExec("delete from t limit 1") // 10 rows
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	require.True(t, hasPseudoStats(tk.MustQuery("explain select * from t where a = 1").Rows()))
}

func hasPseudoStats(rows [][]interface{}) bool {
	for i := range rows {
		if strings.Contains(rows[i][4].(string), "stats:pseudo") {
			return true
		}
	}
	return false
}
