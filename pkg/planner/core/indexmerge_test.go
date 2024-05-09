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

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func getIndexMergePathDigest(paths []*util.AccessPath, startIndex int) string {
	if len(paths) == startIndex {
		return "[]"
	}
	idxMergeDisgest := "["
	for i := startIndex; i < len(paths); i++ {
		if i != startIndex {
			idxMergeDisgest += ","
		}
		path := paths[i]
		idxMergeDisgest += "{Idxs:["
		for j := 0; j < len(path.PartialAlternativeIndexPaths); j++ {
			if j > 0 {
				idxMergeDisgest += ","
			}
			idxMergeDisgest += "{"
			// for every ONE index partial alternatives, output a set.
			for k, one := range path.PartialAlternativeIndexPaths[j] {
				if k != 0 {
					idxMergeDisgest += ","
				}
				idxMergeDisgest += one.Index.Name.L
			}
			idxMergeDisgest += "}"
		}
		idxMergeDisgest += "],TbFilters:["
		for j := 0; j < len(path.TableFilters); j++ {
			if j > 0 {
				idxMergeDisgest += ","
			}
			idxMergeDisgest += path.TableFilters[j].String()
		}
		idxMergeDisgest += "]}"
	}
	idxMergeDisgest += "]"
	return idxMergeDisgest
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
		err = Preprocess(context.Background(), sctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err)
		sctx := MockContext()
		builder, _ := NewPlanBuilder().Init(sctx, is, hint.NewQBHintHandler(nil))
		p, err := builder.Build(ctx, stmt)
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
		var ds *DataSource
		for ds == nil {
			switch v := lp.(type) {
			case *DataSource:
				ds = v
			default:
				lp = lp.Children()[0]
			}
		}
		ds.SCtx().GetSessionVars().SetEnableIndexMerge(true)
		idxMergeStartIndex := len(ds.possibleAccessPaths)
		_, err = lp.RecursiveDeriveStats(nil)
		require.NoError(t, err)
		result := getIndexMergePathDigest(ds.possibleAccessPaths, idxMergeStartIndex)
		testdata.OnRecord(func() {
			output[i] = result
		})
		require.Equalf(t, output[i], result, "case:%v sql:%s", i, tc)
		domain.GetDomain(sctx).StatsHandle().Close()
	}
}
