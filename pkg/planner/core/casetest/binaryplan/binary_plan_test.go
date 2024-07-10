// Copyright 2023 PingCAP, Inc.
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

package binaryplan

import (
	"encoding/base64"
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/golang/snappy"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

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
		require.NotNil(t, pb.AccessObjects)
	}
	// AccessObject field is an interface and json.Unmarshall can't handle it, so we don't check it against the json output.
	pb.AccessObjects = nil
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
func TestBinaryPlanInExplainAndSlowLog(t *testing.T) {
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
	tk.MustExec("set tidb_cost_model_version=2")
	// If we don't set this, it will be false sometimes and the cost in the result will be different.
	tk.MustExec("set @@tidb_enable_chunk_rpc=true")
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
	planSuiteData := GetBinaryPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

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
		require.Equal(t, output[i].BinaryPlan, binary, comment)
	}
}
