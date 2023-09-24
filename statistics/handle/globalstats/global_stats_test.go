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

package globalstats_test

import (
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/globalstats"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/testkit"
	"github.com/tiancaiamao/gp"
)

var analyzeOptionDefaultV2 = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    256,
	ast.AnalyzeOptNumTopN:       500,
	ast.AnalyzeOptCMSketchWidth: 2048,
	ast.AnalyzeOptCMSketchDepth: 5,
	ast.AnalyzeOptNumSamples:    0,
	ast.AnalyzeOptSampleRate:    math.Float64bits(-1),
}

func buildFuke() (*model.TableInfo, []int64) {
	columns := make([]*model.ColumnInfo, 0, 2)
	hists := make([]int64, 0)
	for i := 0; i < 2; i++ {
		columns = append(columns, &model.ColumnInfo{})
		hists = append(hists, int64(i))
	}
	var partition = &model.PartitionInfo{}
	partition.Definitions = make([]model.PartitionDefinition, 0, 2)
	return &model.TableInfo{
		ID:        1,
		Columns:   columns,
		Partition: partition,
	}, hists
}

func mockGetTableByPhysicalIDFunc(is infoschema.InfoSchema, physicalID int64) (table.Table, bool) {
	return nil, false
}

func mockLoadTablePartitionStatsFunc(tableInfo *model.TableInfo, partitionDef *model.PartitionDefinition) (*statistics.Table, error) {
	return nil, nil
}

func TestMergePartitionStats2GlobalStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	ctx := tk.Session().(sessionctx.Context)
	g := gp.New(math.MaxInt16, 10*time.Minute)
	tblInfo, hists := buildFuke()
	globalstats.MergePartitionStats2GlobalStats(
		ctx, g, analyzeOptionDefaultV2, dom.InfoSchema(),
		tblInfo,
		true,
		hists,
		mockGetTableByPhysicalIDFunc,
		mockLoadTablePartitionStatsFunc,
	)
}
