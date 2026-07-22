// Copyright 2026 PingCAP, Inc.
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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// TestFilterNeverAnalyzedColumns verifies that columns whose statistics are never
// collected (vector, plus the tidb_analyze_skip_column_types set) are dropped from the
// stats-load request, while normal columns and all index items are retained — the latter
// so that indexes built over such columns (e.g. a multi-valued index over a JSON column)
// still get their statistics loaded.
func TestFilterNeverAnalyzedColumns(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().AnalyzeSkipColumnTypes = map[string]struct{}{"json": {}, "blob": {}}

	intFT := types.NewFieldType(mysql.TypeLong)
	jsonFT := types.NewFieldType(mysql.TypeJSON)
	vecFT := types.NewFieldType(mysql.TypeTiDBVectorFloat32)
	// blob is skipped only via the configured AnalyzeSkipColumnTypes lookup (not the
	// unconditional vector/JSON handling), so it exercises that branch.
	blobFT := types.NewFieldType(mysql.TypeBlob)
	tbl := &model.TableInfo{
		ID: 100,
		Columns: []*model.ColumnInfo{
			{ID: 1, State: model.StatePublic, FieldType: *intFT},
			{ID: 2, State: model.StatePublic, FieldType: *jsonFT},
			{ID: 3, State: model.StatePublic, FieldType: *vecFT},
			{ID: 4, State: model.StatePublic, FieldType: *blobFT},
		},
	}
	tblID2TblInfo := map[int64]*model.TableInfo{100: tbl}
	items := []model.StatsLoadItem{
		{TableItemID: model.TableItemID{TableID: 100, ID: 1}},                // int column - keep
		{TableItemID: model.TableItemID{TableID: 100, ID: 2}},                // json column - drop (unconditional)
		{TableItemID: model.TableItemID{TableID: 100, ID: 3}},                // vector column - drop (unconditional)
		{TableItemID: model.TableItemID{TableID: 100, ID: 4}},                // blob column - drop (configured skip-type)
		{TableItemID: model.TableItemID{TableID: 100, ID: 9, IsIndex: true}}, // index over json - keep
	}

	got := filterNeverAnalyzedColumns(sctx, items, tblID2TblInfo)

	var keptCols, keptIdx []int64
	for _, it := range got {
		if it.IsIndex {
			keptIdx = append(keptIdx, it.ID)
		} else {
			keptCols = append(keptCols, it.ID)
		}
	}
	require.Equal(t, []int64{1}, keptCols, "only the non-skip column should remain")
	require.Equal(t, []int64{9}, keptIdx, "index items must be kept even when built over a skipped column")
}
