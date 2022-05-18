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
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestTraceCE(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, d varchar(10), index idx(a, b))")
	tk.MustExec(`insert into t values(1, 1, "aaa"),
		(1, 1, "bbb"),
		(1, 2, "ccc"),
		(1, 2, "ddd"),
		(2, 2, "aaa"),
		(2, 3, "bbb")`)
	tk.MustExec("analyze table t")
	var (
		in  []string
		out []struct {
			Expr  string
			Trace []*tracing.CETraceRecord
		}
	)
	traceSuiteData := statistics.GetTraceSuiteData()
	traceSuiteData.GetTestCases(t, &in, &out)

	// Load needed statistics.
	for _, tt := range in {
		sql := "explain select * from t where " + tt
		tk.MustExec(sql)
	}
	statsHandle := dom.StatsHandle()
	err := statsHandle.LoadNeededHistograms()
	require.NoError(t, err)

	sctx := tk.Session().(sessionctx.Context)
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	p := parser.New()
	for i, expr := range in {
		stmtCtx := sctx.GetSessionVars().StmtCtx
		sql := "explain select * from t where " + expr
		stmtCtx.EnableOptimizerCETrace = true
		stmtCtx.OptimizerCETrace = nil
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)
		_, _, err = plannercore.OptimizeAstNode(context.Background(), sctx, stmt, is)
		require.NoError(t, err)

		traceResult := sctx.GetSessionVars().StmtCtx.OptimizerCETrace
		// Ignore the TableID field because this field is unexported when marshalling to JSON.
		for _, rec := range traceResult {
			rec.TableID = 0
		}

		testdata.OnRecord(func() {
			out[i].Expr = expr
			out[i].Trace = traceResult
		})
		// Assert using the result in the stmtCtx
		require.ElementsMatch(t, traceResult, out[i].Trace)

		sql = "trace plan target='estimation' select * from t where " + expr
		result := tk.MustQuery(sql)
		require.Len(t, result.Rows(), 1)
		resultStr := result.Rows()[0][0].(string)
		var resultJSON []*tracing.CETraceRecord
		err = json.Unmarshal([]byte(resultStr), &resultJSON)
		require.NoError(t, err)
		// Assert using the result of trace plan SQL
		require.ElementsMatch(t, resultJSON, out[i].Trace)
	}
}
