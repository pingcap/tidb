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

package joinorder

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	utilhint "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func newOrderedLeadingTestJoin(t *testing.T, ownerQB, carrierQB int, carrierName, otherName string) (*mock.Context, *logicalop.LogicalJoin, *logicalop.DataSource) {
	t.Helper()
	ctx := mock.NewContext()
	ctx.GetSessionVars().CurrentDB = "test"
	newSource := func(name string, qb int) *logicalop.DataSource {
		ds := logicalop.DataSource{}.Init(ctx, qb)
		ds.SetSchema(expression.NewSchema(&expression.Column{
			UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  types.NewFieldType(mysql.TypeLonglong),
		}))
		ds.SetOutputNames(types.NameSlice{{
			DBName: pmodel.NewCIStr("test"), TblName: pmodel.NewCIStr(name), ColName: pmodel.NewCIStr("a"),
		}})
		return ds
	}
	carrier := newSource(carrierName, carrierQB)
	other := newSource(otherName, ownerQB)
	eq, err := expression.NewFunction(ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeLonglong),
		carrier.Schema().Columns[0], other.Schema().Columns[0])
	require.NoError(t, err)
	join := logicalop.LogicalJoin{
		JoinType:        logicalop.InnerJoin,
		EqualConditions: []*expression.ScalarFunction{eq.(*expression.ScalarFunction)},
	}.Init(ctx, ownerQB)
	join.SetChildren(other, carrier)
	join.SetSchema(expression.MergeSchema(other.Schema(), carrier.Schema()))
	return ctx, join, carrier
}

func captureInternalLeadingLogs(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zap.DebugLevel)
	previous := logutil.AppLogger
	logutil.AppLogger = zap.New(core)
	t.Cleanup(func() { logutil.AppLogger = previous })
	return logs
}

func TestOrderedLeadingAnnotationRequiresUniqueCarrierMatch(t *testing.T) {
	logs := captureInternalLeadingLogs(t)
	for _, test := range []struct {
		name        string
		ownerQB     int
		carrierQB   int
		carrierName string
		otherName   string
		alias       string
		accepted    bool
	}{
		{name: "same block", ownerQB: 1, carrierQB: 1, carrierName: "l", otherName: "r", accepted: true},
		{name: "decorrelated table", ownerQB: 1, carrierQB: 2, carrierName: "l", otherName: "r", accepted: true},
		{name: "qualified decorrelated table disambiguates the owner table", ownerQB: 1, carrierQB: 2, carrierName: "l", otherName: "l", accepted: true},
		{name: "owner visible alias", ownerQB: 2, carrierQB: 3, carrierName: "dt", otherName: "r", alias: "dt", accepted: true},
		{name: "normalized offset cannot match the source block", ownerQB: 2, carrierQB: 3, carrierName: "l", otherName: "r", alias: "dt"},
		{name: "multiple operands match the same name", ownerQB: 1, carrierQB: 1, carrierName: "l", otherName: "l"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, root, carrier := newOrderedLeadingTestJoin(t, test.ownerQB, test.carrierQB, test.carrierName, test.otherName)
			if test.alias != "" {
				blockNames := make([]ast.HintTable, test.carrierQB+1)
				blockNames[test.carrierQB] = ast.HintTable{TableName: pmodel.NewCIStr(test.alias)}
				ctx.GetSessionVars().PlannerSelectBlockAsName.Store(&blockNames)
				aliases := make([]utilhint.SelectBlockAlias, test.carrierQB+1)
				aliases[test.carrierQB] = utilhint.SelectBlockAlias{
					SelectOffset: test.carrierQB, VisibleOffset: test.ownerQB, TableName: pmodel.NewCIStr(test.alias),
				}
				ctx.GetSessionVars().PlannerSelectBlockAliasInfo.Store(&aliases)
			}
			choice, ok := FindOrderedLeadingChoice(root, []property.SortItem{{Col: carrier.Schema().Columns[0]}})
			require.True(t, ok)
			require.Same(t, carrier, choice.Carrier)
			annotation, ok := BuildOrderedLeadingAnnotation(root, choice)
			require.Equal(t, test.accepted, ok)
			require.False(t, root.InternalPreferJoinOrder, "preflight must not mutate the plan")
			require.Zero(t, ctx.GetSessionVars().StmtCtx.WarningCount())
			if !test.accepted {
				require.Nil(t, annotation)
				entries := logs.TakeAll()
				require.Len(t, entries, 1)
				require.Equal(t, zap.DebugLevel, entries[0].Level)
				require.Equal(t, "hint does not uniquely match the carrier", entries[0].ContextMap()["reason"])
				return
			}
			require.Empty(t, logs.TakeAll())
			require.False(t, annotation.Hints.LeadingJoinOrder[0].Matched)
			ApplyOrderedLeadingAnnotations([]*OrderedLeadingAnnotation{annotation})
			group := extractJoinGroup(root)
			require.True(t, group.hasInternalLeadingHint)
			nodes := []*Node{{p: group.vertexes[0]}, {p: group.vertexes[1]}}
			// A single-carrier hint performs no joins, so it needs no detector.
			matched, remaining, err := newJoinOrderGreedy(ctx, group).buildJoinByHint(nil, nodes)
			require.NoError(t, err)
			require.NotNil(t, matched)
			require.Same(t, carrier, matched.p)
			require.Len(t, remaining, 1)
			require.True(t, annotation.Hints.LeadingJoinOrder[0].Matched)
			require.Zero(t, ctx.GetSessionVars().StmtCtx.WarningCount())
		})
	}

	t.Run("a unique match to another operand is rejected", func(t *testing.T) {
		_, root, carrier := newOrderedLeadingTestJoin(t, 1, 1, "l", "r")
		choice, ok := FindOrderedLeadingChoice(root, []property.SortItem{{Col: carrier.Schema().Columns[0]}})
		require.True(t, ok)
		choice.Table.TblName = pmodel.NewCIStr("r")
		annotation, ok := BuildOrderedLeadingAnnotation(root, choice)
		require.False(t, ok)
		require.Nil(t, annotation)
	})

	t.Run("ambiguous owner facts reject a uniform output alias", func(t *testing.T) {
		ctx, innerJoin, carrier := newOrderedLeadingTestJoin(t, 1, 1, "l", "r")
		proj := logicalop.LogicalProjection{Exprs: expression.Column2Exprs(innerJoin.Schema().Columns)}.Init(ctx, 1)
		proj.SetChildren(innerJoin)
		proj.SetSchema(innerJoin.Schema().Clone())
		names := make(types.NameSlice, proj.Schema().Len())
		for i := range names {
			names[i] = &types.FieldName{DBName: pmodel.NewCIStr("test"), TblName: pmodel.NewCIStr("derived")}
		}
		proj.SetOutputNames(names)
		other := logicalop.LogicalTableDual{}.Init(ctx, 1)
		other.SetSchema(expression.NewSchema(&expression.Column{
			UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  types.NewFieldType(mysql.TypeLonglong),
		}))
		eq, err := expression.NewFunction(ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeLonglong),
			carrier.Schema().Columns[0], other.Schema().Columns[0])
		require.NoError(t, err)
		root := logicalop.LogicalJoin{
			JoinType:        logicalop.InnerJoin,
			EqualConditions: []*expression.ScalarFunction{eq.(*expression.ScalarFunction)},
		}.Init(ctx, 1)
		root.SetChildren(proj, other)
		root.SetSchema(expression.MergeSchema(proj.Schema(), other.Schema()))

		choice, ok := FindOrderedLeadingChoice(root, []property.SortItem{{Col: carrier.Schema().Columns[0]}})
		require.True(t, ok)
		annotation, ok := BuildOrderedLeadingAnnotation(root, choice)
		require.False(t, ok)
		require.Nil(t, annotation)
	})
}

func TestInternalLeadingFallbackLogsWithoutSQLWarnings(t *testing.T) {
	logs := captureInternalLeadingLogs(t)
	ctx, root, carrier := newOrderedLeadingTestJoin(t, 1, 1, "l", "r")
	choice, ok := FindOrderedLeadingChoice(root, []property.SortItem{{Col: carrier.Schema().Columns[0]}})
	require.True(t, ok)
	annotation, ok := BuildOrderedLeadingAnnotation(root, choice)
	require.True(t, ok)
	ApplyOrderedLeadingAnnotations([]*OrderedLeadingAnnotation{annotation})

	// Simulate a later rewrite changing the operand identity after preflight.
	carrier.SetOutputNames(types.NameSlice{{TblName: pmodel.NewCIStr("renamed")}})
	group := extractJoinGroup(root)
	nodes := []*Node{{p: group.vertexes[0]}, {p: group.vertexes[1]}}
	matched, remaining, err := newJoinOrderGreedy(ctx, group).buildJoinByHint(nil, nodes)
	require.NoError(t, err)
	require.Nil(t, matched)
	require.Equal(t, nodes, remaining)
	require.False(t, annotation.Hints.LeadingJoinOrder[0].Matched)
	require.Zero(t, ctx.GetSessionVars().StmtCtx.WarningCount())
	entries := logs.TakeAll()
	require.Len(t, entries, 1)
	require.Equal(t, zap.DebugLevel, entries[0].Level)
	require.Equal(t, int64(root.ID()), entries[0].ContextMap()["joinPlanID"])
	require.Equal(t, int64(1), entries[0].ContextMap()["ownerQueryBlock"])
	require.Equal(t, "cannot match or apply the leading hint", entries[0].ContextMap()["reason"])

	root.StraightJoin = true
	_, err = Optimize(root)
	require.NoError(t, err)
	require.Zero(t, ctx.GetSessionVars().StmtCtx.WarningCount())
	entries = logs.TakeAll()
	require.Len(t, entries, 1)
	require.Equal(t, "join group has only one operand", entries[0].ContextMap()["reason"])
}
