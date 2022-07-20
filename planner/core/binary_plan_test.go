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
	"regexp"
	"testing"

	"github.com/golang/snappy"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
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

func TestBinaryPlanInExplainAndSlowLog(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// If we don't set this, it will be false sometimes and the cost in the result will be different.
	tk.MustExec("set @@tidb_enable_chunk_rpc=true")

	var input []string
	var output []struct {
		SQL        string
		BinaryPlan *tipb.ExplainData
	}
	planSuiteData := core.GetBinaryPlanSuiteData()
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

func TestInvalidDecodeBinaryPlan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	str1 := "some random bytes"
	str2 := base64.StdEncoding.EncodeToString([]byte(str1))
	str3 := base64.StdEncoding.EncodeToString(snappy.Encode(nil, []byte(str1)))

	tk.MustQuery(`select tidb_decode_binary_plan('` + str1 + `')`).Check(testkit.Rows(""))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 illegal base64 data at input byte 4"))
	tk.MustQuery(`select tidb_decode_binary_plan('` + str2 + `')`).Check(testkit.Rows(""))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 snappy: corrupt input"))
	tk.MustQuery(`select tidb_decode_binary_plan('` + str3 + `')`).Check(testkit.Rows(""))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 proto: illegal wireType 7"))
}
