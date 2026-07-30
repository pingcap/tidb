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

package joinorder_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestMatchLeadingHintTableToOperandFailClosed(t *testing.T) {
	newDataSource := func(ctx *mock.Context, offset int, table string) *logicalop.DataSource {
		plan := logicalop.DataSource{}.Init(ctx, offset)
		plan.SetOutputNames(types.NameSlice{
			&types.FieldName{
				DBName:  ast.NewCIStr("test"),
				TblName: ast.NewCIStr(table),
				ColName: ast.NewCIStr("a"),
			},
		})
		return plan
	}
	assertMatch := func(t *testing.T, plan base.Plan, table string, owner int, matched bool) {
		match := plannerutil.MatchLeadingHintTableToOperand(
			plan,
			&ast.HintTable{TableName: ast.NewCIStr(table)},
			owner,
		)
		require.Equal(t, matched, match.Matched)
		require.False(t, match.OwnerVisible)
	}

	tests := []struct {
		name        string
		ownerOffset int
		matched     bool
	}{
		{
			name:        "invalid owner rejects unique raw identity",
			ownerOffset: -1,
		},
		{
			name:        "no derived candidate allows semi rewrite compatibility",
			ownerOffset: 1,
			matched:     true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := mock.NewContext()
			plan := newDataSource(ctx, 2, "t3")
			assertMatch(t, plan, "t3", test.ownerOffset, test.matched)
		})
	}

	t.Run("broken derived alias remains rejected while concrete base is compatible", func(t *testing.T) {
		ctx := mock.NewContext()
		leaf := newDataSource(ctx, 2, "t2")
		wrapper := logicalop.LogicalProjection{}.Init(ctx, 2)
		wrapper.SetOutputNames(types.NameSlice{
			&types.FieldName{
				DBName:  ast.NewCIStr("test"),
				TblName: ast.NewCIStr("d2"),
				ColName: ast.NewCIStr("a"),
			},
		})
		wrapper.SetChildren(leaf)
		direct := []ast.HintTable{
			{},
			{},
			{DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("d2")},
		}
		aliases := []h.SelectBlockAlias{
			{},
			{},
			{
				SelectOffset:  2,
				VisibleOffset: 0,
				DBName:        ast.NewCIStr("test"),
				TableName:     ast.NewCIStr("d2"),
			},
		}
		ctx.GetSessionVars().PlannerSelectBlockAsName.Store(&direct)
		ctx.GetSessionVars().PlannerSelectBlockAliasInfo.Store(&aliases)

		assertMatch(t, wrapper, "d2", 1, false)
		assertMatch(t, wrapper, "t2", 1, true)
	})

	t.Run("single-leaf wrapper requires matching leaf provenance", func(t *testing.T) {
		ctx := mock.NewContext()
		leaf := newDataSource(ctx, 2, "t2")
		wrapper := logicalop.LogicalProjection{}.Init(ctx, 2)
		wrapper.SetOutputNames(types.NameSlice{
			&types.FieldName{
				DBName:  ast.NewCIStr("test"),
				TblName: ast.NewCIStr("d2"),
				ColName: ast.NewCIStr("a"),
			},
		})
		wrapper.SetChildren(leaf)

		assertMatch(t, wrapper, "d2", 1, false)
		assertMatch(t, wrapper, "t2", 1, true)
	})

	t.Run("same-name wrapper is concrete when its unique leaf proves it", func(t *testing.T) {
		ctx := mock.NewContext()
		leaf := newDataSource(ctx, 2, "t3")
		wrapper := logicalop.LogicalProjection{}.Init(ctx, 2)
		wrapper.SetOutputNames(types.NameSlice{
			&types.FieldName{
				DBName:  ast.NewCIStr("test"),
				TblName: ast.NewCIStr("t3"),
				ColName: ast.NewCIStr("a"),
			},
		})
		wrapper.SetChildren(leaf)

		assertMatch(t, wrapper, "t3", 1, true)
	})

	t.Run("multi-leaf wrapper cannot prove one concrete identity", func(t *testing.T) {
		ctx := mock.NewContext()
		left := newDataSource(ctx, 1, "t2")
		right := newDataSource(ctx, 1, "t3")
		join := logicalop.LogicalJoin{}.Init(ctx, 2)
		join.SetOutputNames(types.NameSlice{
			&types.FieldName{
				DBName:  ast.NewCIStr("test"),
				TblName: ast.NewCIStr("t3"),
				ColName: ast.NewCIStr("a"),
			},
		})
		join.SetChildren(left, right)

		assertMatch(t, join, "t3", 1, false)
	})

	t.Run("same-name self-join leaves remain distinct raw occurrences", func(t *testing.T) {
		ctx := mock.NewContext()
		left := newDataSource(ctx, 2, "t3")
		right := newDataSource(ctx, 2, "t3")
		join := logicalop.LogicalJoin{}.Init(ctx, 2)
		join.SetOutputNames(types.NameSlice{
			&types.FieldName{
				DBName:  ast.NewCIStr("test"),
				TblName: ast.NewCIStr("t3"),
				ColName: ast.NewCIStr("a"),
			},
		})
		join.SetChildren(left, right)

		assertMatch(t, join, "t3", 1, false)
	})

	t.Run("qualified same-offset self-join leaves are ambiguous", func(t *testing.T) {
		ctx := mock.NewContext()
		left := newDataSource(ctx, 2, "t3")
		right := newDataSource(ctx, 2, "t3")
		join := logicalop.LogicalJoin{}.Init(ctx, 2)
		join.SetOutputNames(types.NameSlice{
			&types.FieldName{
				DBName:  ast.NewCIStr("test"),
				TblName: ast.NewCIStr("t3"),
				ColName: ast.NewCIStr("a"),
			},
		})
		join.SetChildren(left, right)
		direct := []ast.HintTable{
			{},
			{},
			{DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("dt")},
		}
		aliases := []h.SelectBlockAlias{
			{},
			{},
			{
				SelectOffset:  2,
				VisibleOffset: 1,
				DBName:        ast.NewCIStr("test"),
				TableName:     ast.NewCIStr("dt"),
			},
		}
		ctx.GetSessionVars().PlannerSelectBlockAsName.Store(&direct)
		ctx.GetSessionVars().PlannerSelectBlockAliasInfo.Store(&aliases)

		match := plannerutil.MatchLeadingHintTableToOperand(
			join,
			&ast.HintTable{
				TableName: ast.NewCIStr("t3"),
				QBName:    ast.NewCIStr("sel_2"),
			},
			1,
		)
		require.False(t, match.Matched)
		require.False(t, match.OwnerVisible)
	})
}
