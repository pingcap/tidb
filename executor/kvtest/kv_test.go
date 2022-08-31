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

package kvtest

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestDropStatsFromKV(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 varchar(20), c2 varchar(20))")
	tk.MustExec(`insert into t values("1","1"),("2","2"),("3","3"),("4","4")`)
	tk.MustExec("insert into t select * from t")
	tk.MustExec("insert into t select * from t")
	tk.MustExec("analyze table t with 2 topn")
	tblID := tk.MustQuery(`select tidb_table_id from information_schema.tables where table_name = "t" and table_schema = "test"`).Rows()[0][0].(string)
	tk.MustQuery("select modify_count, count from mysql.stats_meta where table_id = " + tblID).Check(
		testkit.Rows("0 16"))
	tk.MustQuery("select hist_id from mysql.stats_histograms where table_id = " + tblID).Check(
		testkit.Rows("1", "2"))
	ret := tk.MustQuery("select hist_id, bucket_id from mysql.stats_buckets where table_id = " + tblID)
	require.True(t, len(ret.Rows()) > 0)
	ret = tk.MustQuery("select hist_id from mysql.stats_top_n where table_id = " + tblID)
	require.True(t, len(ret.Rows()) > 0)

	tk.MustExec("drop stats t")
	tk.MustQuery("select modify_count, count from mysql.stats_meta where table_id = " + tblID).Check(
		testkit.Rows("0 16"))
	tk.MustQuery("select hist_id from mysql.stats_histograms where table_id = " + tblID).Check(
		testkit.Rows())
	tk.MustQuery("select hist_id, bucket_id from mysql.stats_buckets where table_id = " + tblID).Check(
		testkit.Rows())
	tk.MustQuery("select hist_id from mysql.stats_top_n where table_id = " + tblID).Check(
		testkit.Rows())
}
