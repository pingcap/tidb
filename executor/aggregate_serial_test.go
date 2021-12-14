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

package executor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestAggInDisk(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_hashagg_final_concurrency = 1;")
	tk.MustExec("set tidb_hashagg_partial_concurrency = 1;")
	tk.MustExec("set tidb_mem_quota_query = 4194304")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(a int)")
	sql := "insert into t values (0)"
	for i := 1; i <= 200; i++ {
		sql += fmt.Sprintf(",(%v)", i)
	}
	sql += ";"
	tk.MustExec(sql)
	rows := tk.MustQuery("desc analyze select /*+ HASH_AGG() */ avg(t1.a) from t t1 join t t2 group by t1.a, t2.a;").Rows()
	for _, row := range rows {
		length := len(row)
		line := fmt.Sprintf("%v", row)
		disk := fmt.Sprintf("%v", row[length-1])
		if strings.Contains(line, "HashAgg") {
			require.False(t, strings.Contains(disk, "0 Bytes"))
			require.True(t, strings.Contains(disk, "MB") ||
				strings.Contains(disk, "KB") ||
				strings.Contains(disk, "Bytes"))
		}
	}

	// Add code cover
	// Test spill chunk. Add a line to avoid tmp spill chunk is always full.
	tk.MustExec("insert into t values(0)")
	tk.MustQuery("select sum(tt.b) from ( select /*+ HASH_AGG() */ avg(t1.a) as b from t t1 join t t2 group by t1.a, t2.a) as tt").Check(
		testkit.Rows("4040100.0000"))
	// Test no groupby and no data.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(c int, c1 int);")
	tk.MustQuery("select /*+ HASH_AGG() */ count(c) from t;").Check(testkit.Rows("0"))
	tk.MustQuery("select /*+ HASH_AGG() */ count(c) from t group by c1;").Check(testkit.Rows())
}

func TestRandomPanicAggConsume(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_max_chunk_size=32")
	tk.MustExec("set @@tidb_init_chunk_size=1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	for i := 0; i <= 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%v),(%v),(%v)", i, i, i))
	}

	fpName := "github.com/pingcap/tidb/executor/ConsumeRandomPanic"
	require.NoError(t, failpoint.Enable(fpName, "5%panic(\"ERROR 1105 (HY000): Out Of Memory Quota![conn_id=1]\")"))
	defer func() {
		require.NoError(t, failpoint.Disable(fpName))
	}()

	// Test 10 times panic for each AggExec.
	var res sqlexec.RecordSet
	for i := 1; i <= 10; i++ {
		var err error
		for err == nil {
			// Test paralleled hash agg.
			res, err = tk.Exec("select /*+ HASH_AGG() */ count(a) from t group by a")
			if err == nil {
				_, err = session.GetRows4Test(context.Background(), tk.Session(), res)
				require.NoError(t, res.Close())
			}
		}
		require.EqualError(t, err, "failpoint panic: ERROR 1105 (HY000): Out Of Memory Quota![conn_id=1]")

		err = nil
		for err == nil {
			// Test unparalleled hash agg.
			res, err = tk.Exec("select /*+ HASH_AGG() */ count(distinct a) from t")
			if err == nil {
				_, err = session.GetRows4Test(context.Background(), tk.Session(), res)
				require.NoError(t, res.Close())
			}
		}
		require.EqualError(t, err, "failpoint panic: ERROR 1105 (HY000): Out Of Memory Quota![conn_id=1]")

		err = nil
		for err == nil {
			// Test stream agg.
			res, err = tk.Exec("select /*+ STREAM_AGG() */ count(a) from t")
			if err == nil {
				_, err = session.GetRows4Test(context.Background(), tk.Session(), res)
				require.NoError(t, res.Close())
			}
		}
		require.EqualError(t, err, "failpoint panic: ERROR 1105 (HY000): Out Of Memory Quota![conn_id=1]")
	}
}
