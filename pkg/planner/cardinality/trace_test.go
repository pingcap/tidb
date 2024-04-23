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

package cardinality_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestTraceCE(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	traceSuiteData := cardinality.GetCardinalitySuiteData()
	traceSuiteData.LoadTestCases(t, &in, &out)

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

func TestTraceDebugSelectivity(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	statsHandle := dom.StatsHandle()

	// Make the result of v1 analyze result stable
	// 1. make sure all rows are always collect as samples
	originalSampleSize := executor.MaxRegionSampleSize
	executor.MaxRegionSampleSize = 10000
	defer func() {
		executor.MaxRegionSampleSize = originalSampleSize
	}()
	// 2. make the order of samples for building TopN stable
	// (the earlier TopN entry will modify the CMSketch, therefore influence later TopN entry's row count,
	// see (*SampleCollector).ExtractTopN() for details)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/StabilizeV1AnalyzeTopN", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/StabilizeV1AnalyzeTopN"))
	}()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index iab(a, b), index ib(b))")
	require.NoError(t, statsHandle.HandleDDLEvent(<-statsHandle.DDLEventCh()))

	// Prepare the data.

	// For column a, from -1000 to 999, each value appears 1 time,
	// but if it's dividable by 100, make this value appear 50 times.
	// For column b, it's always a+500.
	start := -1000
	for i := 0; i < 2000; i += 50 {
		sql := "insert into t values "
		// 50 rows as a batch
		values := make([]string, 0, 50)
		for j := 0; j < 50; j++ {
			values = append(values, fmt.Sprintf("(%d,%d)", start+i+j, start+i+j+500))
		}
		sql = sql + strings.Join(values, ",")
		tk.MustExec(sql)

		if i%100 == 0 {
			sql := "insert into t values "
			topNValue := fmt.Sprintf("(%d,%d) ,", start+i, start+i+500)
			sql = sql + strings.Repeat(topNValue, 49)
			sql = sql[0 : len(sql)-1]
			tk.MustExec(sql)
		}
	}
	require.Nil(t, statsHandle.DumpStatsDeltaToKV(true))
	tk.MustExec("analyze table t with 1 samplerate, 20 topn")
	require.Nil(t, statsHandle.Update(dom.InfoSchema()))
	// Add 100 modify count
	sql := "insert into t values "
	topNValue := fmt.Sprintf("(%d,%d) ,", 5000, 5000)
	sql = sql + strings.Repeat(topNValue, 100)
	sql = sql[0 : len(sql)-1]
	tk.MustExec(sql)
	require.Nil(t, statsHandle.DumpStatsDeltaToKV(true))
	require.Nil(t, statsHandle.Update(dom.InfoSchema()))

	var (
		in  []string
		out []struct {
			ResultForV1 any
			ResultForV2 any
		}
	)
	traceSuiteData := cardinality.GetCardinalitySuiteData()
	traceSuiteData.LoadTestCases(t, &in, &out)

	// Trigger loading needed statistics.
	for _, tt := range in {
		sql := "explain " + tt
		tk.MustExec(sql)
	}
	err := statsHandle.LoadNeededHistograms()
	require.NoError(t, err)

	sctx := tk.Session().(sessionctx.Context)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tb.Meta()
	statsTbl := statsHandle.GetTableStats(tblInfo)
	stmtCtx := sctx.GetSessionVars().StmtCtx
	stmtCtx.EnableOptimizerDebugTrace = true

	// Collect common information for the following tests.
	p := parser.New()
	dsSchemaCols := make([][]*expression.Column, 0, len(in))
	selConditions := make([][]expression.Expression, 0, len(in))
	tblInfos := make([]*model.TableInfo, 0, len(in))
	for _, sql := range in {
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)
		ret := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(context.Background(), sctx, stmt, plannercore.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		p, err := plannercore.BuildLogicalPlanForTest(context.Background(), sctx, stmt, ret.InfoSchema)
		require.NoError(t, err)

		sel := p.(base.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
		ds := sel.Children()[0].(*plannercore.DataSource)

		dsSchemaCols = append(dsSchemaCols, ds.Schema().Columns)
		selConditions = append(selConditions, sel.Conditions)
		tblInfos = append(tblInfos, ds.TableInfo())
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)

	// Test using ver2 stats.
	for i, sql := range in {
		stmtCtx.OptimizerDebugTrace = nil
		histColl := statsTbl.GenerateHistCollFromColumnInfo(tblInfos[i], dsSchemaCols[i])
		_, _, err = cardinality.Selectivity(sctx.GetPlanCtx(), histColl, selConditions[i], nil)
		require.NoError(t, err, sql, "For ver2")
		traceInfo := stmtCtx.OptimizerDebugTrace
		buf.Reset()
		require.NoError(t, encoder.Encode(traceInfo), sql, "For ver2")
		var res any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &res), sql, "For ver2")
		testdata.OnRecord(func() {
			out[i].ResultForV2 = res
		})
		require.Equal(t, out[i].ResultForV2, res, sql, fmt.Sprintf("For ver2: test#%d", i))
	}

	tk.MustExec("set tidb_analyze_version = 1")
	tk.MustExec("analyze table t with 20 topn")
	require.Nil(t, statsHandle.Update(dom.InfoSchema()))
	statsTbl = statsHandle.GetTableStats(tblInfo)

	// Test using ver1 stats.
	stmtCtx = sctx.GetSessionVars().StmtCtx
	stmtCtx.EnableOptimizerDebugTrace = true
	for i, sql := range in {
		stmtCtx.OptimizerDebugTrace = nil
		histColl := statsTbl.GenerateHistCollFromColumnInfo(tblInfos[i], dsSchemaCols[i])
		_, _, err = cardinality.Selectivity(sctx.GetPlanCtx(), histColl, selConditions[i], nil)
		require.NoError(t, err, sql, "For ver1")
		traceInfo := stmtCtx.OptimizerDebugTrace
		buf.Reset()
		require.NoError(t, encoder.Encode(traceInfo), sql, "For ver1")
		var res any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &res), sql, "For ver1")
		testdata.OnRecord(func() {
			out[i].ResultForV1 = res
		})
		require.Equal(t, out[i].ResultForV1, res, sql, "For ver1")
	}
}
