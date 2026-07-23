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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestEffectiveIndexColumnIDsWithUnresolvedColumn covers an index whose declared columns
// cannot all be resolved to schema columns: util.IndexInfo2FullCols leaves a nil entry in
// FullIdxCols for such a position. The effective key must stop crediting columns there and
// must not append the clustered handle, matching what fillIndexPath derives later from the
// resolved prefix.
func TestEffectiveIndexColumnIDsWithUnresolvedColumn(t *testing.T) {
	newIntCol := func(id int64) *expression.Column {
		return &expression.Column{ID: id, UniqueID: id, RetType: types.NewFieldType(mysql.TypeLonglong)}
	}
	handleCol := newIntCol(3)
	ds := &logicalop.DataSource{
		TableInfo:        &model.TableInfo{IsCommonHandle: true, CommonHandleVersion: 1},
		CommonHandleCols: []*expression.Column{handleCol},
		CommonHandleLens: []int{types.UnspecifiedLength},
	}
	newPath := func(fullIdxCols []*expression.Column) *util.AccessPath {
		return &util.AccessPath{
			Index: &model.IndexInfo{
				Columns: []*model.IndexColumn{{Name: ast.NewCIStr("a")}, {Name: ast.NewCIStr("b")}},
			},
			FullIdxCols: fullIdxCols,
		}
	}

	resolved := effectiveIndexColumnIDs(ds, newPath([]*expression.Column{newIntCol(1), newIntCol(2)}))
	require.Equal(t, []int64{1, 2, handleCol.ID}, resolved)

	unresolvedPath := newPath([]*expression.Column{newIntCol(1), nil})
	appendCols, appendLens := ds.HandleColsToAppend(unresolvedPath, unresolvedPath.FullIdxCols)
	require.Nil(t, appendCols)
	require.Nil(t, appendLens)
	require.Equal(t, []int64{1, -1}, effectiveIndexColumnIDs(ds, unresolvedPath))
}
