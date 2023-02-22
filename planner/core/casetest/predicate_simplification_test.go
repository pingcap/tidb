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

package casetest

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

// Test redundant conditions in single table and join predicates.
func TestRemoveRedundantPredicates(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set tidb_opt_limit_push_down_threshold=0")
	var input []string
	var output []struct {
		SQL  string
		Best string
	}
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	for i, tt := range input {
		comment := fmt.Sprintf("case: %v, sql: %s", i, tt)
		stmt, err := p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
		p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, is)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Best = core.ToString(p)
		})
		require.Equal(t, output[i].Best, core.ToString(p), comment)
	}
}
