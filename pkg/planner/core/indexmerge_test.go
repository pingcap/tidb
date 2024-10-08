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

package core

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func getIndexMergePathDigest(ctx expression.EvalContext, paths []*util.AccessPath, startIndex int) string {
	if len(paths) == startIndex {
		return "[]"
	}
	idxMergeDigest := "["
	for i := startIndex; i < len(paths); i++ {
		if i != startIndex {
			idxMergeDigest += ","
		}
		path := paths[i]
		idxMergeDigest += "{Idxs:["
		for j := 0; j < len(path.PartialAlternativeIndexPaths); j++ {
			if j > 0 {
				idxMergeDigest += ","
			}
			idxMergeDigest += "{"
			// for every ONE index partial alternatives, output a set.
			for k, one := range path.PartialAlternativeIndexPaths[j] {
				if k != 0 {
					idxMergeDigest += ","
				}
				idxMergeDigest += one.Index.Name.L
			}
			idxMergeDigest += "}"
		}
		idxMergeDigest += "],TbFilters:["
		for j := 0; j < len(path.TableFilters); j++ {
			if j > 0 {
				idxMergeDigest += ","
			}
			idxMergeDigest += path.TableFilters[j].StringWithCtx(ctx, errors.RedactLogDisable)
		}
		idxMergeDigest += "]}"
	}
	idxMergeDigest += "]"
	return idxMergeDigest
}

func TestIndexMergePathGeneration(t *testing.T) {
	var input, output []string
	indexMergeSuiteData.LoadTestCases(t, &input, &output)
	ctx := context.TODO()
	sctx := MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()
	is := infoschema.MockInfoSchema([]*model.TableInfo{MockSignedTable(), MockView()})

	parser := parser.New()

	for i, tc := range input {
		stmt, err := parser.ParseOneStmt(tc, "", "")
		require.NoErrorf(t, err, "case:%v sql:%s", i, tc)
		nodeW := resolve.NewNodeW(stmt)
		err = Preprocess(context.Background(), sctx, nodeW, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err)
		sctx := MockContext()
		builder, _ := NewPlanBuilder().Init(sctx, is, hint.NewQBHintHandler(nil))
		p, err := builder.Build(ctx, nodeW)
		if err != nil {
			testdata.OnRecord(func() {
				output[i] = err.Error()
			})
			require.Equal(t, output[i], err.Error(), "case:%v sql:%s", i, tc)
			continue
		}
		require.NoError(t, err)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(base.LogicalPlan))
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
		_, err = lp.RecursiveDeriveStats(nil)
		require.NoError(t, err)
		result := getIndexMergePathDigest(sctx.GetExprCtx().GetEvalCtx(), ds.PossibleAccessPaths, idxMergeStartIndex)
		testdata.OnRecord(func() {
			output[i] = result
		})
		require.Equalf(t, output[i], result, "case:%v sql:%s", i, tc)
		domain.GetDomain(sctx).StatsHandle().Close()
	}
}
