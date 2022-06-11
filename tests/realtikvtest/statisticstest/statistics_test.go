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

package statisticstest

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestNewCollationStatsWithPrefixIndex(t *testing.T) {
	store, dom, clean := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	defer clean()
	defer func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		r := tk.MustQuery("show tables")
		for _, tb := range r.Rows() {
			tableName := tb[0]
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
		tk.MustExec("delete from mysql.stats_meta")
		tk.MustExec("delete from mysql.stats_histograms")
		tk.MustExec("delete from mysql.stats_buckets")
		dom.StatsHandle().Clear()
	}()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(40) collate utf8mb4_general_ci, index ia3(a(3)), index ia10(a(10)), index ia(a))")
	tk.MustExec("insert into t values('aaAAaaaAAAabbc'), ('AaAaAaAaAaAbBC'), ('AAAaabbBBbbb'), ('AAAaabbBBbbbccc'), ('aaa'), ('Aa'), ('A'), ('ab')")
	tk.MustExec("insert into t values('b'), ('bBb'), ('Bb'), ('bA'), ('BBBB'), ('BBBBBDDDDDdd'), ('bbbbBBBBbbBBR'), ('BBbbBBbbBBbbBBRRR')")
	h := dom.StatsHandle()
	tk.MustExec("set @@session.tidb_analyze_version=1")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))

	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 'aaa'")
	require.NoError(t, h.LoadNeededHistograms())

	tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 0 1 1 \x00A \x00A 0",
		"test t  a 0 1 2 1 \x00A\x00A \x00A\x00A 0",
		"test t  a 0 10 12 1 \x00B\x00B\x00B \x00B\x00B\x00B 0",
		"test t  a 0 11 13 1 \x00B\x00B\x00B\x00B \x00B\x00B\x00B\x00B 0",
		"test t  a 0 12 14 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 0",
		"test t  a 0 13 15 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R 0",
		"test t  a 0 14 16 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 0",
		"test t  a 0 2 3 1 \x00A\x00A\x00A \x00A\x00A\x00A 0",
		"test t  a 0 3 5 2 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 0",
		"test t  a 0 4 6 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B 0",
		"test t  a 0 5 7 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C 0",
		"test t  a 0 6 8 1 \x00A\x00B \x00A\x00B 0",
		"test t  a 0 7 9 1 \x00B \x00B 0",
		"test t  a 0 8 10 1 \x00B\x00A \x00B\x00A 0",
		"test t  a 0 9 11 1 \x00B\x00B \x00B\x00B 0",
		"test t  ia 1 0 1 1 \x00A \x00A 0",
		"test t  ia 1 1 2 1 \x00A\x00A \x00A\x00A 0",
		"test t  ia 1 10 12 1 \x00B\x00B\x00B \x00B\x00B\x00B 0",
		"test t  ia 1 11 13 1 \x00B\x00B\x00B\x00B \x00B\x00B\x00B\x00B 0",
		"test t  ia 1 12 14 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 0",
		"test t  ia 1 13 15 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R 0",
		"test t  ia 1 14 16 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 0",
		"test t  ia 1 2 3 1 \x00A\x00A\x00A \x00A\x00A\x00A 0",
		"test t  ia 1 3 5 2 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 0",
		"test t  ia 1 4 6 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B 0",
		"test t  ia 1 5 7 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C 0",
		"test t  ia 1 6 8 1 \x00A\x00B \x00A\x00B 0",
		"test t  ia 1 7 9 1 \x00B \x00B 0",
		"test t  ia 1 8 10 1 \x00B\x00A \x00B\x00A 0",
		"test t  ia 1 9 11 1 \x00B\x00B \x00B\x00B 0",
		"test t  ia10 1 0 1 1 \x00A \x00A 0",
		"test t  ia10 1 1 2 1 \x00A\x00A \x00A\x00A 0",
		"test t  ia10 1 10 13 1 \x00B\x00B\x00B\x00B \x00B\x00B\x00B\x00B 0",
		"test t  ia10 1 11 15 2 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B 0",
		"test t  ia10 1 12 16 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D 0",
		"test t  ia10 1 2 3 1 \x00A\x00A\x00A \x00A\x00A\x00A 0",
		"test t  ia10 1 3 5 2 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A 0",
		"test t  ia10 1 4 7 2 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B 0",
		"test t  ia10 1 5 8 1 \x00A\x00B \x00A\x00B 0",
		"test t  ia10 1 6 9 1 \x00B \x00B 0",
		"test t  ia10 1 7 10 1 \x00B\x00A \x00B\x00A 0",
		"test t  ia10 1 8 11 1 \x00B\x00B \x00B\x00B 0",
		"test t  ia10 1 9 12 1 \x00B\x00B\x00B \x00B\x00B\x00B 0",
		"test t  ia3 1 0 1 1 \x00A \x00A 0",
		"test t  ia3 1 1 2 1 \x00A\x00A \x00A\x00A 0",
		"test t  ia3 1 2 7 5 \x00A\x00A\x00A \x00A\x00A\x00A 0",
		"test t  ia3 1 3 8 1 \x00A\x00B \x00A\x00B 0",
		"test t  ia3 1 4 9 1 \x00B \x00B 0",
		"test t  ia3 1 5 10 1 \x00B\x00A \x00B\x00A 0",
		"test t  ia3 1 6 11 1 \x00B\x00B \x00B\x00B 0",
		"test t  ia3 1 7 16 5 \x00B\x00B\x00B \x00B\x00B\x00B 0",
	))
	tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 2",
	))
	tk.MustQuery("select is_index, hist_id, distinct_count, null_count, stats_ver, correlation from mysql.stats_histograms").Sort().Check(testkit.Rows(
		"0 1 15 0 1 0.8411764705882353",
		"1 1 8 0 1 0",
		"1 2 13 0 1 0",
		"1 3 15 0 1 0",
	))

	tk.MustExec("set @@session.tidb_analyze_version=2")
	h = dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))

	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 'aaa'")
	require.NoError(t, h.LoadNeededHistograms())

	tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows())
	tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 \x00A 1",
		"test t  a 0 \x00A\x00A 1",
		"test t  a 0 \x00A\x00A\x00A 1",
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 2",
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B 1",
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C 1",
		"test t  a 0 \x00A\x00B 1",
		"test t  a 0 \x00B 1",
		"test t  a 0 \x00B\x00A 1",
		"test t  a 0 \x00B\x00B 1",
		"test t  a 0 \x00B\x00B\x00B 1",
		"test t  a 0 \x00B\x00B\x00B\x00B 1",
		"test t  a 0 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 1",
		"test t  a 0 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R 1",
		"test t  a 0 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 1",
		"test t  ia 1 \x00A 1",
		"test t  ia 1 \x00A\x00A 1",
		"test t  ia 1 \x00A\x00A\x00A 1",
		"test t  ia 1 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 2",
		"test t  ia 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B 1",
		"test t  ia 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C 1",
		"test t  ia 1 \x00A\x00B 1",
		"test t  ia 1 \x00B 1",
		"test t  ia 1 \x00B\x00A 1",
		"test t  ia 1 \x00B\x00B 1",
		"test t  ia 1 \x00B\x00B\x00B 1",
		"test t  ia 1 \x00B\x00B\x00B\x00B 1",
		"test t  ia 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 1",
		"test t  ia 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R 1",
		"test t  ia 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 1",
		"test t  ia10 1 \x00A 1",
		"test t  ia10 1 \x00A\x00A 1",
		"test t  ia10 1 \x00A\x00A\x00A 1",
		"test t  ia10 1 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A 2",
		"test t  ia10 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B 2",
		"test t  ia10 1 \x00A\x00B 1",
		"test t  ia10 1 \x00B 1",
		"test t  ia10 1 \x00B\x00A 1",
		"test t  ia10 1 \x00B\x00B 1",
		"test t  ia10 1 \x00B\x00B\x00B 1",
		"test t  ia10 1 \x00B\x00B\x00B\x00B 1",
		"test t  ia10 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B 2",
		"test t  ia10 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D 1",
		"test t  ia3 1 \x00A 1",
		"test t  ia3 1 \x00A\x00A 1",
		"test t  ia3 1 \x00A\x00A\x00A 5",
		"test t  ia3 1 \x00A\x00B 1",
		"test t  ia3 1 \x00B 1",
		"test t  ia3 1 \x00B\x00A 1",
		"test t  ia3 1 \x00B\x00B 1",
		"test t  ia3 1 \x00B\x00B\x00B 5",
	))
	tk.MustQuery("select is_index, hist_id, distinct_count, null_count, stats_ver, correlation from mysql.stats_histograms").Sort().Check(testkit.Rows(
		"0 1 15 0 2 0.8411764705882353",
		"1 1 8 0 2 0",
		"1 2 13 0 2 0",
		"1 3 15 0 2 0",
	))
}
