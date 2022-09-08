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
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, d varchar(10), index idx(a, b))")
	tk.MustExec("create table t1(a int, b int, c int)")
	tk.MustExec("create table t2(a int, b int, c int)")
	tk.MustExec("create table t3(a int, b int, c int)")
	tk.MustExec("create table t4(a int, b int, c int)")
	tk.MustExec(`insert into t values(1, 1, "aaa"),
		(1, 1, "bbb"),
		(1, 2, "ccc"),
		(1, 2, "ddd"),
		(2, 2, "aaa"),
		(2, 3, "bbb")`)
	tk.MustExec("analyze table t")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	tk.MustExec("analyze table t3")
	tk.MustExec("analyze table t4")
	var (
		in  []string
		out []struct {
			Expr  string
			Trace []*tracing.CETraceRecord
		}
	)
	traceSuiteData := statistics.GetTraceSuiteData()
	traceSuiteData.LoadTestCases(t, &in, &out)

	// Load needed statistics.
	for _, tt := range in {
		sql := "explain " + tt
		tk.MustExec(sql)
	}
	statsHandle := dom.StatsHandle()
	err := statsHandle.LoadNeededHistograms()
	require.NoError(t, err)

	sctx := tk.Session().(sessionctx.Context)
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	p := parser.New()
	for i, sql := range in {
		stmtCtx := sctx.GetSessionVars().StmtCtx
		stmtCtx.EnableOptimizerCETrace = true
		stmtCtx.OptimizerCETrace = nil
		stmtCtx.CETraceColNameAlloc.Store(0)
		stmtCtx.CETraceTblNameAlloc.Store(0)
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
			out[i].Expr = sql
			out[i].Trace = traceResult
		})
		// Assert using the result in the stmtCtx
		require.ElementsMatch(t, traceResult, out[i].Trace)

		sql = "trace plan target='estimation' " + sql
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

func TestTraceCEPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, d varchar(10), index idx(a, b)) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN MAXVALUE);")
	tk.MustExec(`insert into t values(1, 1, "aaa"),
		(1, 1, "bbb"),
		(1, 2, "ccc"),
		(1, 2, "ddd"),
		(2, 2, "aaa"),
		(2, 3, "bbb")`)
	tk.MustExec("analyze table t")
	result := tk.MustQuery("trace plan target='estimation' select * from t where a >=1")
	require.Len(t, result.Rows(), 1)
	resultStr := result.Rows()[0][0].(string)
	var resultJSON []*tracing.CETraceRecord
	err := json.Unmarshal([]byte(resultStr), &resultJSON)
	require.NoError(t, err)
	for _, r := range resultJSON {
		require.Equal(t, "t", r.TableName)
	}
}
