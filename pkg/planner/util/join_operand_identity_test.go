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

package util_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestExtractOperandIdentityFacts(t *testing.T) {
	newDataSource := func(ctx *mock.Context, offset int, table string) *logicalop.DataSource {
		plan := logicalop.DataSource{}.Init(ctx, offset)
		plan.SetOutputNames(types.NameSlice{&types.FieldName{
			DBName:  ast.NewCIStr("test"),
			TblName: ast.NewCIStr(table),
			ColName: ast.NewCIStr("a"),
		}})
		return plan
	}
	newProjection := func(ctx *mock.Context, offset int, table string, child *logicalop.DataSource) *logicalop.LogicalProjection {
		plan := logicalop.LogicalProjection{}.Init(ctx, offset)
		plan.SetOutputNames(types.NameSlice{&types.FieldName{
			DBName:  ast.NewCIStr("test"),
			TblName: ast.NewCIStr(table),
			ColName: ast.NewCIStr("a"),
		}})
		plan.SetChildren(child)
		return plan
	}
	setAliases := func(ctx *mock.Context, direct []ast.HintTable, aliases []h.SelectBlockAlias) {
		ctx.GetSessionVars().PlannerSelectBlockAsName.Store(&direct)
		ctx.GetSessionVars().PlannerSelectBlockAliasInfo.Store(&aliases)
	}

	t.Run("owner unique", func(t *testing.T) {
		ctx := mock.NewContext()
		facts := plannerutil.ExtractOperandIdentityFacts(newDataSource(ctx, 1, "t1"), 1)
		require.Equal(t, plannerutil.OwnerUnique, facts.Owner.Kind)
		require.Equal(t, "t1", facts.Owner.Identity.TblName.L)
	})

	t.Run("owner absent", func(t *testing.T) {
		ctx := mock.NewContext()
		plan := newDataSource(ctx, 2, "t2")
		facts := plannerutil.ExtractOperandIdentityFacts(plan, 1)
		require.Equal(t, plannerutil.OwnerAbsent, facts.Owner.Kind)
		require.Len(t, facts.Occurrences, 1)
		require.Equal(t, plannerutil.ConcreteOccurrence, facts.Occurrences[0].Kind)
		identity, ok := plannerutil.ResolveJoinOperandHintIdentity(plan, 1)
		require.False(t, ok)
		require.Nil(t, identity)
	})

	t.Run("broken alias chain retains concrete provenance", func(t *testing.T) {
		ctx := mock.NewContext()
		leaf := newDataSource(ctx, 2, "t2")
		plan := newProjection(ctx, 2, "d2", leaf)
		setAliases(ctx,
			[]ast.HintTable{{}, {}, {DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("d2")}},
			[]h.SelectBlockAlias{{}, {}, {
				SelectOffset: 2, VisibleOffset: 0,
				DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("d2"),
			}},
		)

		facts := plannerutil.ExtractOperandIdentityFacts(plan, 1)
		require.Equal(t, plannerutil.OwnerBrokenAliasChain, facts.Owner.Kind)
		require.Nil(t, facts.Owner.Identity)
		require.Contains(t, facts.Occurrences, plannerutil.IdentityOccurrence{
			Identity: *plannerutil.ExtractTableAlias(leaf, 2), StartQB: 2, NodeID: leaf.ID(), Kind: plannerutil.ConcreteOccurrence,
		})
		identity, ok := plannerutil.ResolveJoinOperandHintIdentity(plan, 1)
		require.False(t, ok)
		require.Nil(t, identity)
	})

	t.Run("owner ambiguous after strict projection", func(t *testing.T) {
		ctx := mock.NewContext()
		left := newDataSource(ctx, 2, "t2")
		right := newDataSource(ctx, 3, "t3")
		plan := logicalop.LogicalJoin{}.Init(ctx, 0)
		plan.SetChildren(left, right)
		setAliases(ctx,
			[]ast.HintTable{
				{}, {},
				{DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("d2")},
				{DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("d3")},
			},
			[]h.SelectBlockAlias{
				{}, {},
				{SelectOffset: 2, VisibleOffset: 1, DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("d2")},
				{SelectOffset: 3, VisibleOffset: 1, DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("d3")},
			},
		)

		facts := plannerutil.ExtractOperandIdentityFacts(plan, 1)
		require.Equal(t, plannerutil.OwnerAmbiguous, facts.Owner.Kind)
		require.Nil(t, facts.Owner.Identity)
		identity, ok := plannerutil.ResolveJoinOperandHintIdentity(plan, 1)
		require.False(t, ok)
		require.Nil(t, identity)
	})

	t.Run("broken alias wins over one resolvable owner candidate", func(t *testing.T) {
		ctx := mock.NewContext()
		left := newDataSource(ctx, 2, "t2")
		right := newDataSource(ctx, 3, "t3")
		plan := logicalop.LogicalJoin{}.Init(ctx, 0)
		plan.SetChildren(left, right)
		setAliases(ctx,
			[]ast.HintTable{
				{}, {},
				{DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("broken")},
				{DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("dt")},
			},
			[]h.SelectBlockAlias{
				{}, {},
				{SelectOffset: 2, VisibleOffset: 0, DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("broken")},
				{SelectOffset: 3, VisibleOffset: 1, DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("dt")},
			},
		)

		facts := plannerutil.ExtractOperandIdentityFacts(plan, 1)
		require.Equal(t, plannerutil.OwnerBrokenAliasChain, facts.Owner.Kind)
		identity, ok := plannerutil.ResolveJoinOperandHintIdentity(plan, 1)
		require.False(t, ok)
		require.Nil(t, identity)
	})

	t.Run("same-name wrapper replaces its unique leaf occurrence", func(t *testing.T) {
		ctx := mock.NewContext()
		leaf := newDataSource(ctx, 2, "t2")
		plan := newProjection(ctx, 2, "t2", leaf)
		facts := plannerutil.ExtractOperandIdentityFacts(plan, 1)
		require.Len(t, facts.Occurrences, 1)
		require.Equal(t, plan.ID(), facts.Occurrences[0].NodeID)
		require.Equal(t, plannerutil.ConcreteOccurrence, facts.Occurrences[0].Kind)
	})

	t.Run("different wrapper keeps leaf occurrence", func(t *testing.T) {
		ctx := mock.NewContext()
		leaf := newDataSource(ctx, 2, "t2")
		plan := newProjection(ctx, 2, "d2", leaf)
		facts := plannerutil.ExtractOperandIdentityFacts(plan, 1)
		require.Len(t, facts.Occurrences, 1)
		require.Equal(t, leaf.ID(), facts.Occurrences[0].NodeID)
		require.Equal(t, "t2", facts.Occurrences[0].Identity.TblName.L)
	})

	t.Run("same-name self join preserves distinct node occurrences", func(t *testing.T) {
		ctx := mock.NewContext()
		left := newDataSource(ctx, 2, "t2")
		right := newDataSource(ctx, 2, "t2")
		plan := logicalop.LogicalJoin{}.Init(ctx, 0)
		plan.SetChildren(left, right)
		facts := plannerutil.ExtractOperandIdentityFacts(plan, 1)
		require.Len(t, facts.Occurrences, 2)
		require.NotEqual(t, facts.Occurrences[0].NodeID, facts.Occurrences[1].NodeID)
	})
}
