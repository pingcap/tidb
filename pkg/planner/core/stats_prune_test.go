// Copyright 2024 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestPruneIndexesByAccessConds(t *testing.T) {
	// Create a mock DataSource
	ds := &logicalop.DataSource{}
	ds.AllPossibleAccessPaths = make([]*util.AccessPath, 0)

	// Create mock access conditions (simplified for testing)
	condA := &expression.Constant{Value: types.NewIntDatum(1)}
	condB := &expression.Constant{Value: types.NewIntDatum(2)}
	condC := &expression.Constant{Value: types.NewIntDatum(3)}

	// Create mock index info
	idxA := &model.IndexInfo{Name: ast.NewCIStr("idx_a"), Columns: []*model.IndexColumn{{Name: ast.NewCIStr("a")}}}
	idxAB := &model.IndexInfo{Name: ast.NewCIStr("idx_a_b"), Columns: []*model.IndexColumn{{Name: ast.NewCIStr("a")}, {Name: ast.NewCIStr("b")}}}
	idxABC := &model.IndexInfo{Name: ast.NewCIStr("idx_a_b_c"), Columns: []*model.IndexColumn{{Name: ast.NewCIStr("a")}, {Name: ast.NewCIStr("b")}, {Name: ast.NewCIStr("c")}}}
	idxB := &model.IndexInfo{Name: ast.NewCIStr("idx_b"), Columns: []*model.IndexColumn{{Name: ast.NewCIStr("b")}}}
	idxC := &model.IndexInfo{Name: ast.NewCIStr("idx_c"), Columns: []*model.IndexColumn{{Name: ast.NewCIStr("c")}}}

	// Test case 1: Indexes with same access columns but different number of access conditions
	// idx_a_b_c has 3 access conditions, idx_a_b has 2, idx_a has 1
	// Should keep idx_a_b_c and remove idx_a_b and idx_a
	ds.AllPossibleAccessPaths = []*util.AccessPath{
		{Index: idxA, AccessConds: []expression.Expression{condA}, TableFilters: []expression.Expression{condB, condC}},
		{Index: idxAB, AccessConds: []expression.Expression{condA, condB}, TableFilters: []expression.Expression{condC}},
		{Index: idxABC, AccessConds: []expression.Expression{condA, condB, condC}, TableFilters: []expression.Expression{}},
	}

	pruneIndexesByAccessConds(ds)
	require.Equal(t, 1, len(ds.AllPossibleAccessPaths))
	require.Equal(t, "idx_a_b_c", ds.AllPossibleAccessPaths[0].Index.Name.O)

	// Test case 2: Indexes with same access columns but different table filters
	// idx_a_b has fewer table filters than idx_a, so should keep both
	ds.AllPossibleAccessPaths = []*util.AccessPath{
		{Index: idxA, AccessConds: []expression.Expression{condA}, TableFilters: []expression.Expression{condB, condC}},
		{Index: idxAB, AccessConds: []expression.Expression{condA, condB}, TableFilters: []expression.Expression{condC}},
	}

	pruneIndexesByAccessConds(ds)
	require.Equal(t, 2, len(ds.AllPossibleAccessPaths))

	// Test case 3: Indexes with different access columns should not be pruned
	ds.AllPossibleAccessPaths = []*util.AccessPath{
		{Index: idxA, AccessConds: []expression.Expression{condA}, TableFilters: []expression.Expression{}},
		{Index: idxB, AccessConds: []expression.Expression{condB}, TableFilters: []expression.Expression{}},
		{Index: idxC, AccessConds: []expression.Expression{condC}, TableFilters: []expression.Expression{}},
	}

	pruneIndexesByAccessConds(ds)
	require.Equal(t, 3, len(ds.AllPossibleAccessPaths))

	// Test case 4: Indexes with no access conditions but singleScan should be kept
	ds.AllPossibleAccessPaths = []*util.AccessPath{
		{Index: idxA, AccessConds: []expression.Expression{}, TableFilters: []expression.Expression{condA}, IsSingleScan: true},
		{Index: idxB, AccessConds: []expression.Expression{}, TableFilters: []expression.Expression{condB}, IsSingleScan: false},
	}

	pruneIndexesByAccessConds(ds)
	require.Equal(t, 1, len(ds.AllPossibleAccessPaths))
	require.Equal(t, "idx_a", ds.AllPossibleAccessPaths[0].Index.Name.O)

	// Test case 5: Table paths should always be kept
	tablePath := &util.AccessPath{IsIntHandlePath: true, AccessConds: []expression.Expression{}, TableFilters: []expression.Expression{}}
	ds.AllPossibleAccessPaths = []*util.AccessPath{
		tablePath,
		{Index: idxA, AccessConds: []expression.Expression{condA}, TableFilters: []expression.Expression{}},
		{Index: idxAB, AccessConds: []expression.Expression{condA, condB}, TableFilters: []expression.Expression{}},
	}

	pruneIndexesByAccessConds(ds)
	require.Equal(t, 2, len(ds.AllPossibleAccessPaths))
	// Table path should be the first one
	require.True(t, ds.AllPossibleAccessPaths[0].IsTablePath())
	// Index path should be the second one (idx_a_b should be kept as it has more access conditions)
	require.Equal(t, "idx_a_b", ds.AllPossibleAccessPaths[1].Index.Name.O)
}
