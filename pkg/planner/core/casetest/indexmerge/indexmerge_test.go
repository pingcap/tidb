// Copyright 2019 PingCAP, Inc.
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

package indexmerge

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func getIndexMergePathDigest(ctx expression.EvalContext, paths []*util.AccessPath, startIndex int) string {
	if len(paths) == startIndex {
		return "[]"
	}
	resultStrs := make([]string, 0, len(paths)-startIndex)
	for _, path := range paths[startIndex:] {
		partialPathsStrs := make([]string, 0, len(path.PartialIndexPaths))
		for _, partial := range path.PartialAlternativeIndexPaths {
			// for every ONE index partial alternatives, output a set.
			oneAlternativeStrs := make([]string, 0, len(partial))
			for _, oneAlternative := range partial {
				if len(oneAlternative) == 1 {
					oneAlternativeStrs = append(oneAlternativeStrs, oneAlternative[0].Index.Name.L)
					continue
				}
				pathStrs := make([]string, 0, len(oneAlternative))
				for _, singlePath := range oneAlternative {
					pathStrs = append(pathStrs, singlePath.Index.Name.L)
				}
				oneAlternativeStrs = append(oneAlternativeStrs, "{"+strings.Join(pathStrs, ",")+"}")
			}
			partialPathsStrs = append(partialPathsStrs, "{"+strings.Join(oneAlternativeStrs, ",")+"}")
		}

		filterStrs := make([]string, 0, len(path.TableFilters))
		for _, filter := range path.TableFilters {
			filterStrs = append(filterStrs, filter.StringWithCtx(ctx, errors.RedactLogDisable))
		}
		resultStrs = append(resultStrs, "{Idxs:["+
			strings.Join(partialPathsStrs, ",")+
			"],TbFilters:["+
			strings.Join(filterStrs, ",")+
			"]}")
	}
	return "[" + strings.Join(resultStrs, ",") + "]"
}

func TestIndexMergePathGeneration(t *testing.T) {
	var input, output []string
	GetIndexMergeSuiteData().LoadTestCases(t, &input, &output)
	ctx := context.TODO()
	sctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()
	is := infoschema.MockInfoSchema([]*model.TableInfo{coretestsdk.MockSignedTable(), coretestsdk.MockView()})

	parser := parser.New()

	for i, tc := range input {
		stmt, err := parser.ParseOneStmt(tc, "", "")
		require.NoErrorf(t, err, "case:%v sql:%s", i, tc)
		nodeW := resolve.NewNodeW(stmt)
		err = plannercore.Preprocess(context.Background(), sctx, nodeW, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err)
		sctx := coretestsdk.MockContext()
		builder, _ := plannercore.NewPlanBuilder().Init(sctx, is, hint.NewQBHintHandler(nil))
		p, err := builder.Build(ctx, nodeW)
		if err != nil {
			testdata.OnRecord(func() {
				output[i] = err.Error()
			})
			require.Equal(t, output[i], err.Error(), "case:%v sql:%s", i, tc)
			continue
		}
		require.NoError(t, err)
		p, err = plannercore.LogicalOptimizeTest(ctx, builder.GetOptFlag(), p.(base.LogicalPlan))
		require.NoError(t, err)
		lp := p.(base.LogicalPlan)
		var ds *logicalop.DataSource
		for ds == nil {
			switch v := lp.(type) {
			case *logicalop.DataSource:
				ds = v
			default:
				lp = lp.Children()[0]
			}
		}
		ds.SCtx().GetSessionVars().SetEnableIndexMerge(true)
		idxMergeStartIndex := len(ds.PossibleAccessPaths)
		_, _, err = lp.RecursiveDeriveStats(nil)
		require.NoError(t, err)
		result := getIndexMergePathDigest(sctx.GetExprCtx().GetEvalCtx(), ds.PossibleAccessPaths, idxMergeStartIndex)
		testdata.OnRecord(func() {
			output[i] = result
		})
		require.Equalf(t, output[i], result, "case:%v sql:%s", i, tc)
		domain.GetDomain(sctx).StatsHandle().Close()
	}
}

func TestOrderedIndexMergeLimitCost(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("create table t (id int primary key, a int, b int, k int, r int, key ia(a, k), key ib(b, k), key ik(k))")
		values := make([]string, 0, 256)
		for i := range 256 {
			values = append(values, fmt.Sprintf("(%d,%d,%d,%d,%d)", i, i%10, i%11, i, i%2))
		}
		tk.MustExec("insert into t values " + strings.Join(values, ","))
		tk.MustExec("analyze table t")
		// The old estimate favors an ordered union even though its k < 255 branch
		// must be exhausted before the first lookup. Let both paths compete.
		tk.MustExec("set tidb_opt_fix_control='52869:ON'")
		tk.MustHavePlan("select * from t where (k<255 or (b=2 and k>=255)) and r=1 order by k desc, id desc limit 1", "IndexLookUp")

		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/forceIndexMergeKeepOrder", "return(true)")

		parseFloat := func(value any) float64 {
			n, err := strconv.ParseFloat(value.(string), 64)
			require.NoError(t, err)
			return n
		}
		for _, version := range []int{1, 2} {
			tk.MustExec(fmt.Sprintf("set tidb_cost_model_version=%d", version))
			for _, direction := range []string{"asc", "desc"} {
				t.Run(fmt.Sprintf("v%d/%s", version, direction), func(t *testing.T) {
					query := "select /*+ use_index_merge(t, ia, ib) */ * from t where (a=1 or b=1) and r=1 order by k " + direction
					full := tk.MustQuery("explain format=verbose " + query).Rows()
					limited := tk.MustQuery("explain format=verbose " + query + " limit 1").Rows()
					t.Logf("without LIMIT: %v\nwith LIMIT: %v", full, limited)
					require.Contains(t, limited[0][0], "Limit")
					require.Contains(t, limited[1][0], "IndexMerge")
					require.Contains(t, limited[1][5], "type: union")
					require.NotContains(t, limited[1][5], "limit embedded")
					for i := 1; i <= 2; i++ {
						require.Contains(t, full[i][0], "IndexRangeScan")
						require.Contains(t, limited[i+1][0], "IndexRangeScan")
						require.Contains(t, limited[i+1][5], "keep order:true")
						// Residual r=1 prevents an embedded limit. The blocking ordered union
						// must read every index candidate even though only one row is returned.
						require.Equal(t, full[i][1], limited[i+1][1], "LIMIT must not discount blocking index candidates")
						require.Equal(t, full[i][2], limited[i+1][2], "LIMIT must not discount blocking index scan cost")
					}
					require.Less(t, parseFloat(limited[len(limited)-1][1]), parseFloat(full[len(full)-1][1]),
						"table lookups can still stop after residual filters pass")

					// An ordered single-index lookup streams its candidates and can stop early.
					lookupQuery := "select * from t use index(ik) where (a=1 or b=1) and r=1 order by k " + direction
					lookupFull := tk.MustQuery("explain format=verbose " + lookupQuery).Rows()
					lookupLimited := tk.MustQuery("explain format=verbose " + lookupQuery + " limit 1").Rows()
					require.Contains(t, lookupLimited[1][0], "IndexLookUp")
					require.Contains(t, lookupLimited[2][0], "IndexFullScan")
					require.Contains(t, lookupLimited[2][5], "keep order:true")
					require.Less(t, parseFloat(lookupLimited[2][1]), parseFloat(lookupFull[1][1]))
					require.Less(t, parseFloat(lookupLimited[0][2]), parseFloat(lookupFull[0][2]))
					tk.MustQuery(query + " limit 1").Check(tk.MustQuery(lookupQuery + " limit 1").Rows())

					// Without the residual filter, an embedded limit can bound index candidates.
					embeddedQuery := strings.Replace(query, " and r=1", "", 1)
					embedded := tk.MustQuery("explain format=verbose " + embeddedQuery + " limit 1").Rows()
					require.Contains(t, embedded[0][0], "IndexMerge")
					require.Contains(t, embedded[0][5], "limit embedded(offset:0, count:1)")
					require.Less(t, parseFloat(embedded[0][2]), parseFloat(limited[0][2]))
				})
			}
		}

		// Unordered union can stream candidates even with residual table filters.
		testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/planner/core/forceIndexMergeKeepOrder")
		scanRows := func(query string) float64 {
			var rows float64
			var scans int
			for _, row := range tk.MustQuery("explain format=verbose " + query).Rows() {
				if strings.Contains(row[0].(string), "IndexRangeScan") {
					require.Contains(t, row[5], "keep order:false")
					rows += parseFloat(row[1])
					scans++
				}
			}
			require.Equal(t, 2, scans)
			return rows
		}
		unordered := "select /*+ use_index_merge(t, ia, ib) */ * from t where (a=1 or b=1) and r=1"
		require.Less(t, scanRows(unordered+" limit 1"), scanRows(unordered))
	})
}
