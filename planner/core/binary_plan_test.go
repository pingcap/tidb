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
	"os"
	"regexp"
	"testing"

	"github.com/golang/snappy"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stmtsummary"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func simplifyAndCheckBinaryPlan(t *testing.T, pb *tipb.ExplainData) {
	if pb.Main != nil {
		simplifyAndCheckBinaryOperator(t, pb.Main, pb.WithRuntimeStats)
	}
	for _, cte := range pb.Ctes {
		if cte != nil {
			simplifyAndCheckBinaryOperator(t, cte, pb.WithRuntimeStats)
		}
	}
}

func simplifyAndCheckBinaryOperator(t *testing.T, pb *tipb.ExplainOperator, withRuntimeStats bool) {
	if withRuntimeStats {
		if pb.TaskType == tipb.TaskType_root {
			require.NotEmpty(t, pb.RootBasicExecInfo)
		} else if pb.TaskType != tipb.TaskType_unknown {
			require.NotEmpty(t, pb.CopExecInfo)
		}
	}
	pb.RootBasicExecInfo = ""
	pb.RootGroupExecInfo = nil
	pb.CopExecInfo = ""
	match, err := regexp.MatchString("((Table|Index).*Scan)|CTEFullScan|Point_Get", pb.Name)
	if err == nil && match {
		require.NotNil(t, pb.AccessObject)
	}
	// AccessObject field is an interface and json.Unmarshall can't handle it, so we don't check it against the json output.
	pb.AccessObject = nil
	// MemoryBytes and DiskBytes are not stable sometimes.
	pb.MemoryBytes = 0
	pb.DiskBytes = 0
	if len(pb.Children) > 0 {
		for _, op := range pb.Children {
			if op != nil {
				simplifyAndCheckBinaryOperator(t, op, withRuntimeStats)
			}
		}
	}
}

func TestBinaryPlanInExplainAndSlowLog(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// If we don't set this, it will be false sometimes and the cost in the result will be different.
	tk.MustExec("set @@tidb_enable_chunk_rpc=true")

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
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	tk.MustExec("set tidb_slow_log_threshold=0;")
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
	}()

	var input []string
	var output []struct {
		SQL        string
		BinaryPlan *tipb.ExplainData
	}
	planSuiteData := core.GetExplainBinarySuiteData()
	planSuiteData.GetTestCases(t, &input, &output)

	for i, test := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, test)
		if len(test) < 7 || test[:7] != "explain" {
			tk.MustExec(test)
			testdata.OnRecord(func() {
				output[i].SQL = test
				output[i].BinaryPlan = nil
			})
			continue
		}
		result := testdata.ConvertRowsToStrings(tk.MustQuery(test).Rows())
		require.Equal(t, len(result), 1, comment)
		s := result[0]

		// assert that the binary plan in the slow log is the same as the result in the EXPLAIN statement
		slowLogResult := testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.slow_query " +
			`where query = "` + test + `;" ` +
			"order by time desc limit 1").Rows())
		require.Lenf(t, slowLogResult, 1, comment)
		require.Equal(t, s, slowLogResult[0], comment)

		b, err := base64.StdEncoding.DecodeString(s)
		require.NoError(t, err)
		b, err = snappy.Decode(nil, b)
		require.NoError(t, err)
		binary := &tipb.ExplainData{}
		err = binary.Unmarshal(b)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = test
			output[i].BinaryPlan = binary
		})
		simplifyAndCheckBinaryPlan(t, binary)
		require.Equal(t, output[i].BinaryPlan, binary)
	}
}

func TestBinaryPlanSwitch(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))

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
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))

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
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))

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
// The size of the binary plan in this test case is designed to be larger than 1024*1024*0.85 bytes but smaller than 1024*1024 bytes.
func TestLongBinaryPlan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))

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
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))

	tk.MustExec("use test")

	tk.MustExec("drop table if exists th")
	tk.MustExec("set @@session.tidb_enable_table_partition = 1")
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	tk.MustExec("create table th (i int, a int,b int, c int, index (a)) partition by hash (a) partitions 1700;")
	tk.MustQuery("select count(*) from th t1 join th t2 join th t3 join th t4 join th t5 join th t6 where t1.i=t2.a and t1.i=t3.i and t3.i=t4.i and t4.i=t5.i and t5.i=t6.i")

	result := testdata.ConvertRowsToStrings(tk.MustQuery("select binary_plan from information_schema.slow_query " +
		`where query like "%th t1 join th t2 join th t3%" and query not like "%like%" ` +
		"limit 1;").Rows())
	require.Len(t, result, 1)
	s := result[0]
	// The binary plan in this test case is expected to be slightly smaller than MaxEncodedPlanSizeInBytes.
	// If the size of the binary plan changed and this case failed in the future, you can adjust the partition numbers in the CREATE TABLE statement above.
	require.Less(t, len(s), stmtsummary.MaxEncodedPlanSizeInBytes)
	require.Greater(t, len(s), int(float64(stmtsummary.MaxEncodedPlanSizeInBytes)*0.85))
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
	require.Less(t, len(s), stmtsummary.MaxEncodedPlanSizeInBytes)
	require.Greater(t, len(s), int(float64(stmtsummary.MaxEncodedPlanSizeInBytes)*0.85))
	b, err = base64.StdEncoding.DecodeString(s)
	require.NoError(t, err)
	b, err = snappy.Decode(nil, b)
	require.NoError(t, err)
	binary = &tipb.ExplainData{}
	err = binary.Unmarshal(b)
	require.NoError(t, err)
	require.False(t, binary.DiscardedDueToTooLong)
	require.True(t, binary.WithRuntimeStats)
	require.NotNil(t, binary.Main)
}
