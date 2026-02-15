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

package physicalop

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestTryToPassTiCITopN_MatchBySort(t *testing.T) {
	// Table columns: v1(id=1), v2(id=2)
	tbl := &model.TableInfo{Columns: []*model.ColumnInfo{{ID: 1}, {ID: 2}}}

	// Hybrid sort metadata: order by v1 asc, v2 desc
	hybrid := &model.HybridIndexInfo{
		Sort: &model.HybridSortSpec{
			Columns: []*model.IndexColumn{{Offset: 0}, {Offset: 1}},
			IsAsc:   []bool{true, false},
		},
	}
	idx := &model.IndexInfo{HybridInfo: hybrid}

	scan := &PhysicalIndexScan{Table: tbl, Index: idx, FtsQueryInfo: &tipb.FTSQueryInfo{}}

	// topN matches the sort metadata exactly
	topN := &PhysicalTopN{
		ByItems: []*plannerutil.ByItems{
			{Expr: &expression.Column{ID: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}, Desc: false},
			{Expr: &expression.Column{ID: 2, RetType: types.NewFieldType(mysql.TypeLonglong)}, Desc: true},
		},
		Count:  4,
		Offset: 1,
	}

	scan.TryToPassTiCITopN(topN)

	require.NotNil(t, scan.FtsQueryInfo.TopK)
	require.Equal(t, uint32(5), *scan.FtsQueryInfo.TopK)
	require.Equal(t, []int64{1, 2}, scan.FtsQueryInfo.SortColumnIds)
	require.Equal(t, []bool{true, false}, scan.FtsQueryInfo.SortColumnAsc)
}

func TestTryToPassTiCITopN_FallbackToInvertedWhenSortMismatch(t *testing.T) {
	// Table columns: v1(id=1), v2(id=2)
	tbl := &model.TableInfo{Columns: []*model.ColumnInfo{{ID: 1}, {ID: 2}}}

	// Sort metadata says: v1 asc only (so ORDER BY v1,v2 won't be fully covered by Sort)
	// Inverted metadata contains both columns, so fallback should succeed.
	hybrid := &model.HybridIndexInfo{
		Sort: &model.HybridSortSpec{
			Columns: []*model.IndexColumn{{Offset: 0}},
			IsAsc:   []bool{true},
		},
		Inverted: []*model.HybridInvertedSpec{{
			Columns: []*model.IndexColumn{{Offset: 0}, {Offset: 1}},
		}},
	}
	idx := &model.IndexInfo{HybridInfo: hybrid}

	scan := &PhysicalIndexScan{Table: tbl, Index: idx, FtsQueryInfo: &tipb.FTSQueryInfo{}}

	topN := &PhysicalTopN{
		ByItems: []*plannerutil.ByItems{
			{Expr: &expression.Column{ID: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}, Desc: false},
			{Expr: &expression.Column{ID: 2, RetType: types.NewFieldType(mysql.TypeLonglong)}, Desc: false},
		},
		Count:  4,
		Offset: 0,
	}

	scan.TryToPassTiCITopN(topN)

	require.NotNil(t, scan.FtsQueryInfo.TopK)
	require.Equal(t, uint32(4), *scan.FtsQueryInfo.TopK)
	require.Equal(t, []int64{1, 2}, scan.FtsQueryInfo.SortColumnIds)
	require.Equal(t, []bool{true, true}, scan.FtsQueryInfo.SortColumnAsc)
}

func TestTryToPassTiCITopN_ScalarFunctionReturn(t *testing.T) {
	// Table columns: v1(id=1)
	tbl := &model.TableInfo{Columns: []*model.ColumnInfo{{ID: 1}}}

	hybrid := &model.HybridIndexInfo{
		Sort: &model.HybridSortSpec{
			Columns: []*model.IndexColumn{{Offset: 0}},
			IsAsc:   []bool{true},
		},
		Inverted: []*model.HybridInvertedSpec{{
			Columns: []*model.IndexColumn{{Offset: 0}},
		}},
	}
	idx := &model.IndexInfo{HybridInfo: hybrid}

	scan := &PhysicalIndexScan{Table: tbl, Index: idx, FtsQueryInfo: &tipb.FTSQueryInfo{}}

	// ORDER BY abs(v1) -> ScalarFunction
	fn := &expression.ScalarFunction{RetType: types.NewFieldType(mysql.TypeLonglong)}
	topN := &PhysicalTopN{
		ByItems: []*plannerutil.ByItems{{Expr: fn, Desc: false}},
		Count:   4,
		Offset:  0,
	}

	// should just return without filling TopK / sort columns
	scan.TryToPassTiCITopN(topN)
	require.Nil(t, scan.FtsQueryInfo.TopK)
	require.Nil(t, scan.FtsQueryInfo.SortColumnIds)
	require.Nil(t, scan.FtsQueryInfo.SortColumnAsc)
}
