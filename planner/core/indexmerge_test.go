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

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util/hint"
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
		for j := 0; j < len(path.PartialIndexPaths); j++ {
			if j > 0 {
				idxMergeDisgest += ","
			}
			idxMergeDisgest += path.PartialIndexPaths[j].Index.Name.L
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
	t.Parallel()
	var input, output []string
	indexMergeSuiteData.GetTestCases(t, &input, &output)
	ctx := context.TODO()
	sctx := MockContext()
	is := infoschema.MockInfoSchema([]*model.TableInfo{MockSignedTable(), MockView()})

	parser := parser.New()

	for i, tc := range input {
		stmt, err := parser.ParseOneStmt(tc, "", "")
		require.NoErrorf(t, err, "case:%v sql:%s", i, tc)
		err = Preprocess(sctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err)
		builder, _ := NewPlanBuilder().Init(MockContext(), is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		if err != nil {
			testdata.OnRecord(func() {
				output[i] = err.Error()
			})
			require.Equal(t, output[i], err.Error(), "case:%v sql:%s", i, tc)
			continue
		}
		require.NoError(t, err)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		require.NoError(t, err)
		lp := p.(LogicalPlan)
		var ds *DataSource
		for ds == nil {
			switch v := lp.(type) {
			case *DataSource:
				ds = v
			default:
				lp = lp.Children()[0]
			}
		}
		ds.ctx.GetSessionVars().SetEnableIndexMerge(true)
		idxMergeStartIndex := len(ds.possibleAccessPaths)
		_, err = lp.recursiveDeriveStats(nil)
		require.NoError(t, err)
		result := getIndexMergePathDigest(ds.possibleAccessPaths, idxMergeStartIndex)
		testdata.OnRecord(func() {
			output[i] = result
		})
		require.Equalf(t, output[i], result, "case:%v sql:%s", i, tc)
	}
}
