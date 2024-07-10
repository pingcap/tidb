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

package core_test

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/golang/snappy"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestBinaryPlanSwitch(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, f.Close())
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))

	origin := tk.MustQuery("SELECT @@global.tidb_generate_binary_plan")
	originStr := origin.Rows()[0][0].(string)
	defer func() {
		tk.MustExec("set @@global.tidb_generate_binary_plan = '" + originStr + "'")
	}()

	tk.MustExec("use test")
	// 1. assert binary plan is generated if the variable is turned on
	tk.MustExec("set global tidb_generate_binary_plan = 1")
	tk.MustQuery("select sleep(1)")

	result := testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.slow_query " +
		`where query like "%select sleep(1)%" and query not like "%like%" ` +
		"limit 1;").Rows())
	require.Len(t, result, 1)
	s := result[0]
	b, err := base64.StdEncoding.DecodeString(s)
	require.NoError(t, err)
	b, err = snappy.Decode(nil, b)
	require.NoError(t, err)
	binary := &tipb.ExplainData{}
	err = binary.Unmarshal(b)
	require.NoError(t, err)

	result = testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.statements_summary " +
		`where QUERY_SAMPLE_TEXT like "%select sleep(1)%" and QUERY_SAMPLE_TEXT not like "%like%" ` +
		"limit 1;").Rows())
	require.Len(t, result, 1)
	s = result[0]
	b, err = base64.StdEncoding.DecodeString(s)
	require.NoError(t, err)
	b, err = snappy.Decode(nil, b)
	require.NoError(t, err)
	binary = &tipb.ExplainData{}
	err = binary.Unmarshal(b)
	require.NoError(t, err)

	// 2. assert binary plan is not generated if the variable is turned off
	tk.MustExec("set global tidb_generate_binary_plan = 0")
	tk.MustQuery("select 1 > sleep(1)")

	result = testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.slow_query " +
		`where query like "%select 1 > sleep(1)%" and query not like "%like%" ` +
		"limit 1;").Rows())
	require.Len(t, result, 1)
	s = result[0]
	require.Empty(t, s)

	result = testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.statements_summary " +
		`where QUERY_SAMPLE_TEXT like "%select 1 > sleep(1)%" and QUERY_SAMPLE_TEXT not like "%like%" ` +
		"limit 1;").Rows())
	require.Len(t, result, 1)
	s = result[0]
	require.Empty(t, s)
}

// TestTooLongBinaryPlan asserts that if the binary plan is larger than 1024*1024 bytes, it should be output to slow query but not to stmt summary.
func TestTooLongBinaryPlan(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, f.Close())
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	if tk.MustQuery("select @@tidb_schema_cache_size > 0").Equal(testkit.Rows("1")) {
		t.Skip("TODO: the performance is poor for this test under infoschema v2")
	}

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))

	origin := tk.MustQuery("SELECT @@global.tidb_enable_stmt_summary")
	originStr := origin.Rows()[0][0].(string)
	defer func() {
		tk.MustExec("set @@global.tidb_enable_stmt_summary = '" + originStr + "'")
	}()

	// Trigger clear the stmt summary in memory to prevent this case from being affected by other cases.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists th")
	tk.MustExec("set @@session.tidb_enable_table_partition = 1")
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	tk.MustExec("create table th (i int, a int,b int, c int, index (a)) partition by hash (a) partitions 8192;")
	tk.MustQuery("select count(*) from th t1 join th t2 join th t3 join th t4 join th t5 join th t6 where t1.i=t2.a and t1.i=t3.i and t3.i=t4.i and t4.i=t5.i and t5.i=t6.i")

	result := testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.slow_query " +
		`where query like "%th t1 join th t2 join th t3%" and query not like "%like%" ` +
		"limit 1;").Rows())
	require.Len(t, result, 1)
	s := result[0]
	require.Greater(t, len(s), stmtsummary.MaxEncodedPlanSizeInBytes)
	b, err := base64.StdEncoding.DecodeString(s)
	require.NoError(t, err)
	b, err = snappy.Decode(nil, b)
	require.NoError(t, err)
	binary := &tipb.ExplainData{}
	err = binary.Unmarshal(b)
	require.NoError(t, err)
	require.False(t, binary.DiscardedDueToTooLong)
	require.True(t, binary.WithRuntimeStats)
	require.NotNil(t, binary.Main)

	result = testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.statements_summary " +
		`where QUERY_SAMPLE_TEXT like "%th t1 join th t2 join th t3%" and QUERY_SAMPLE_TEXT not like "%like%" ` +
		"limit 1;").Rows())
	require.Len(t, result, 1)
	s = result[0]
	b, err = base64.StdEncoding.DecodeString(s)
	require.NoError(t, err)
	b, err = snappy.Decode(nil, b)
	require.NoError(t, err)
	binary = &tipb.ExplainData{}
	err = binary.Unmarshal(b)
	require.NoError(t, err)
	require.True(t, binary.DiscardedDueToTooLong)
	require.Nil(t, binary.Main)
	require.Nil(t, binary.Ctes)
}

// TestLongBinaryPlan asserts that if the binary plan is smaller than 1024*1024 bytes, it should be output to both slow query and stmt summary.
func TestLongBinaryPlan(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, f.Close())
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))

	tk.MustExec("use test")

	tk.MustExec("drop table if exists th")
	tk.MustExec("set @@session.tidb_enable_table_partition = 1")
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	tk.MustExec("create table th (i int, a int,b int, c int, index (a)) partition by hash (a) partitions 1000;")
	tk.MustQuery("select count(*) from th t1 join th t2 join th t3 join th t4 join th t5 join th t6 where t1.i=t2.a and t1.i=t3.i and t3.i=t4.i and t4.i=t5.i and t5.i=t6.i")

	result := testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.slow_query " +
		`where query like "%th t1 join th t2 join th t3%" and query not like "%like%" ` +
		"limit 1;").Rows())
	require.Len(t, result, 1)
	s1 := result[0]
	// The binary plan in this test case is expected to be smaller than MaxEncodedPlanSizeInBytes.
	// If the size of the binary plan changed and this case failed in the future, you can adjust the partition numbers in the CREATE TABLE statement above.
	require.Less(t, len(s1), stmtsummary.MaxEncodedPlanSizeInBytes)
	b, err := base64.StdEncoding.DecodeString(s1)
	require.NoError(t, err)
	b, err = snappy.Decode(nil, b)
	require.NoError(t, err)
	binary := &tipb.ExplainData{}
	err = binary.Unmarshal(b)
	require.NoError(t, err)
	require.False(t, binary.DiscardedDueToTooLong)
	require.True(t, binary.WithRuntimeStats)
	require.NotNil(t, binary.Main)

	result = testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.statements_summary " +
		`where QUERY_SAMPLE_TEXT like "%th t1 join th t2 join th t3%" and QUERY_SAMPLE_TEXT not like "%like%" ` +
		"limit 1;").Rows())
	require.Len(t, result, 1)
	s2 := result[0]
	require.Equal(t, s1, s2)
}

func TestBinaryPlanOfPreparedStmt(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, f.Close())
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))

	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t value(30,30);")
	tk.MustExec(`prepare stmt from "select sleep(1), b from t where a > ?"`)
	tk.MustExec("set @a = 20")
	tk.MustQuery("execute stmt using @a")

	result := testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.slow_query " +
		`where query like "%select sleep%" and query not like "%like%" ` +
		"limit 1;").Rows())
	require.Len(t, result, 1)
	s1 := result[0]
	b, err := base64.StdEncoding.DecodeString(s1)
	require.NoError(t, err)
	b, err = snappy.Decode(nil, b)
	require.NoError(t, err)
	binary := &tipb.ExplainData{}
	err = binary.Unmarshal(b)
	require.NoError(t, err)
	require.False(t, binary.DiscardedDueToTooLong)
	require.True(t, binary.WithRuntimeStats)
	require.NotNil(t, binary.Main)

	result = testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.statements_summary " +
		`where QUERY_SAMPLE_TEXT like "%select sleep%" and QUERY_SAMPLE_TEXT not like "%like%" ` +
		"limit 1;").Rows())
	require.Len(t, result, 1)
	s2 := result[0]
	require.Equal(t, s1, s2)
}

// TestDecodeBinaryPlan asserts that the result of EXPLAIN ANALYZE FORMAT = 'verbose' is the same as tidb_decode_binary_plan().
func TestDecodeBinaryPlan(t *testing.T) {
	// Prepare the slow log
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, f.Close())
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	tk.MustExec("set tidb_slow_log_threshold=0;")
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
	}()
	tk.MustExec(`create table tp (a int, b int) partition by range(a) (
		partition p0 values less than (100),
		partition p1 values less than (200),
		partition p2 values less than (300),
		partition p3 values less than maxvalue
	)`)
	tk.MustExec("insert into tp value(1,1), (10,10), (123,234), (213, 234);")
	tk.MustExec("create table t(a int, b int, c int, index ia(a));")
	tk.MustExec("insert into t value(1,1,1), (10,10,10), (123,234,345), (-213, -234, -234);")
	cases := []string{
		"explain analyze format = 'verbose' select * from t",
		"explain analyze format = 'verbose' select * from t where a > 10",
		"explain analyze format = 'verbose' select /*+ inl_join(t1) */ * from t t1 join t t2 where t1.a = t2.a",
		"explain analyze format = 'verbose' WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 5) SELECT * FROM cte",
		"set @@tidb_partition_prune_mode='static'",
		"explain analyze format = 'verbose' select * from tp",
		"explain analyze format = 'verbose' select * from tp t1 join tp t2 on t1.b > t2.b",
		"explain analyze format = 'verbose' select * from tp where a > 400",
		"explain analyze format = 'verbose' select * from tp where a < 30",
		"explain analyze format = 'verbose' select * from tp where a > 0",
		"set @@tidb_partition_prune_mode='dynamic'",
		"explain analyze format = 'verbose' select * from tp",
		"explain analyze format = 'verbose' select * from tp t1 join tp t2 on t1.b > t2.b",
		"explain analyze format = 'verbose' select * from tp where a > 400",
		"explain analyze format = 'verbose' select * from tp where a < 30",
		"explain analyze format = 'verbose' select * from tp where a > 0",
	}

	for _, c := range cases {
		if len(c) < 7 || c[:7] != "explain" {
			tk.MustExec(c)
			continue
		}
		comment := fmt.Sprintf("sql:%s", c)

		var res1, res2 []string

		explainResult := tk.MustQuery(c).Rows()
		for _, row := range explainResult {
			for _, val := range row {
				str := val.(string)
				str = strings.TrimSpace(str)
				if len(str) > 0 {
					res1 = append(res1, str)
				}
			}
		}

		slowLogResult := testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.slow_query " +
			`where query = "` + c + `;" ` +
			"order by time desc limit 1").Rows())
		require.Lenf(t, slowLogResult, 1, comment)
		decoded := testdata.ConvertRowsToStrings(tk.MustQuery(`select tidb_decode_binary_plan('` + slowLogResult[0] + `')`).Rows())[0]
		decodedRows := strings.Split(decoded, "\n")
		// remove the first newline and the title row
		decodedRows = decodedRows[2:]
		for _, decodedRow := range decodedRows {
			vals := strings.Split(decodedRow, "|")
			for _, val := range vals {
				val = strings.TrimSpace(val)
				if len(val) > 0 {
					res2 = append(res2, val)
				}
			}
		}

		require.Equalf(t, res1, res2, comment)
	}
}

func TestUnnecessaryBinaryPlanInSlowLog(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, f.Close())
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))

	origin := tk.MustQuery("SELECT @@global.tidb_slow_log_threshold")
	originStr := origin.Rows()[0][0].(string)
	defer func() {
		tk.MustExec("set @@global.tidb_slow_log_threshold = '" + originStr + "'")
	}()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists th")
	tk.MustExec("set global tidb_slow_log_threshold = 1;")
	tk.MustExec("create table th (i int, a int,b int, c int, index (a)) partition by hash (a) partitions 100;")
	slowLogBytes, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NotContains(t, string(slowLogBytes), `tidb_decode_binary_plan('')`)
}
