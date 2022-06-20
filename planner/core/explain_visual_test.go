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
	"testing"

	"github.com/golang/snappy"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func simplifyBinaryPlan(pb *tipb.ExplainData) {
	if pb.Main != nil {
		simplifyBinaryOperator(pb.Main)
	}
	for _, cte := range pb.Ctes {
		if cte != nil {
			simplifyBinaryOperator(cte)
		}
	}
}

func simplifyBinaryOperator(pb *tipb.ExplainOperator) {
	pb.RootBasicExecInfo = ""
	pb.RootGroupExecInfo = nil
	pb.CopExecInfo = ""
	// AccessObject field is an interface and json.Unmarshall can't handle it, so we don't check it.
	pb.AccessObject = nil
	if len(pb.Children) > 0 {
		for _, op := range pb.Children {
			if op != nil {
				simplifyBinaryOperator(op)
			}
		}
	}
}

func TestExplainBinary(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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
		simplifyBinaryPlan(binary)
		simplifyBinaryPlan(output[i].BinaryPlan)
		require.Equal(t, output[i].BinaryPlan, binary)
	}
}
