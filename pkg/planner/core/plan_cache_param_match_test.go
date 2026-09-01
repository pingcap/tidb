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

package core

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
)

func TestParamMatchRefreshEligibility(t *testing.T) {
	selectStmt := &PlanCacheStmt{
		StmtCacheable: true,
		PreparedAst:   &ast.Prepared{Stmt: &ast.SelectStmt{}},
		Params:        make([]ast.ParamMarkerExpr, 1),
	}
	refreshParamMatchModes(nil, selectStmt)
	require.Nil(t, selectStmt.paramMatchModes)

	uncacheableDML := &PlanCacheStmt{
		PreparedAst: &ast.Prepared{Stmt: &ast.UpdateStmt{}},
		Params:      make([]ast.ParamMarkerExpr, 1),
	}
	refreshParamMatchModes(nil, uncacheableDML)
	require.Nil(t, uncacheableDML.paramMatchModes)

}

func TestParamMatchRejectedSetVarHintFiltering(t *testing.T) {
	rejected := &ast.TableOptimizerHint{
		HintName: model.NewCIStr("SET_VAR"),
		HintData: ast.HintSetVar{VarName: "TIDB_ENABLE_PLAN_CACHE_DML_PARAM_TYPE_MATCH", Value: "OFF"},
	}
	require.False(t, hasUnsupportedParamMatchHints([]*ast.TableOptimizerHint{rejected}))

	otherSetVar := &ast.TableOptimizerHint{
		HintName: model.NewCIStr("SET_VAR"),
		HintData: ast.HintSetVar{VarName: "tidb_enable_index_merge", Value: "ON"},
	}
	require.True(t, hasUnsupportedParamMatchHints([]*ast.TableOptimizerHint{rejected, otherSetVar}))
	require.True(t, hasUnsupportedParamMatchHints([]*ast.TableOptimizerHint{{HintName: model.NewCIStr("USE_INDEX")}}))
}

func TestParamMatchInputContracts(t *testing.T) {
	stringType := types.NewFieldType(mysql.TypeVarString)
	intType := types.NewFieldType(mysql.TypeLong)
	actual := []*types.FieldType{stringType, intType}
	stmt := &PlanCacheStmt{StmtCacheable: true, PreparedAst: &ast.Prepared{Stmt: &ast.UpdateStmt{}},
		Params: make([]ast.ParamMarkerExpr, 2), RelateVersion: map[int64]uint64{}}
	sctx := MockContext()

	nonPrepared := currentPlanCacheDMLExecutionConfig(sctx, stmt, true)
	require.Equal(t, planCacheDMLExecutionConfig{}, nonPrepared)

	off := currentPlanCacheDMLExecutionConfig(sctx, stmt, false)
	require.Equal(t, planCacheDMLExecutionConfig{applicable: true}, off)
	input, err := buildParamMatchInput(stmt, actual, off)
	require.NoError(t, err)
	require.Nil(t, input.modes)
	require.Same(t, stringType, input.actualTypes[0])

	sctx.GetSessionVars().EnablePlanCacheDMLParamTypeMatch = true
	sctx.GetSessionVars().StmtCtx.PlanCacheDMLParamTypeMatchEnabled = true
	on := currentPlanCacheDMLExecutionConfig(sctx, stmt, false)
	refreshParamMatchModes(nil, stmt)
	input, err = buildParamMatchInput(stmt, actual, on)
	require.NoError(t, err)
	require.Equal(t, []paramMatchMode{matchStrict, matchStrict}, input.modes)
	require.False(t, sameParamMatchModes(nil, input.modes))
}

func TestParamMatchAwareCompatibility(t *testing.T) {
	cachedTypes := []*types.FieldType{types.NewFieldType(mysql.TypeVarString), types.NewFieldType(mysql.TypeLong)}
	modes := []paramMatchMode{matchIgnoreSourceType, matchStrict}
	cached := newPlanCacheValue(&Update{}, nil, paramMatchInput{actualTypes: cachedTypes, modes: modes}, &hint.StmtHints{})
	modes[0] = matchStrict
	cachedTypes[0].SetType(mysql.TypeTiny)
	require.Equal(t, []paramMatchMode{matchIgnoreSourceType, matchStrict}, cached.paramMatchModes)
	require.Equal(t, mysql.TypeVarString, cached.paramTypes[0].GetType())

	require.True(t, checkPlanCacheParamCompatibility(cached, paramMatchInput{
		actualTypes: []*types.FieldType{types.NewFieldType(mysql.TypeNewDecimal), types.NewFieldType(mysql.TypeLong)},
		modes:       []paramMatchMode{matchIgnoreSourceType, matchStrict},
	}))
	require.False(t, checkPlanCacheParamCompatibility(cached, paramMatchInput{
		actualTypes: []*types.FieldType{types.NewFieldType(mysql.TypeNewDecimal), types.NewFieldType(mysql.TypeLong)},
		modes:       []paramMatchMode{matchStrict, matchStrict},
	}))
	require.False(t, checkPlanCacheParamCompatibility(cached, paramMatchInput{
		actualTypes: []*types.FieldType{types.NewFieldType(mysql.TypeNewDecimal), types.NewFieldType(mysql.TypeDouble)},
		modes:       []paramMatchMode{matchIgnoreSourceType, matchStrict},
	}))
}

func TestParamMatchModesMemoryUsage(t *testing.T) {
	const modeCapacity = 7
	value := &PlanCacheValue{
		paramMatchModes: make([]paramMatchMode, 0, modeCapacity),
	}
	// Three cache payload slices (output columns, parameter types, and modes)
	// plus the two warning slices are part of the fixed structure cost.
	expected := unKnownMemoryUsage + size.SizeOfInterface + size.SizeOfSlice*5 +
		size.SizeOfMap + size.SizeOfInt64*2 + modeCapacity*size.SizeOfByte
	require.Equal(t, expected, value.MemoryUsage())
}

func TestParamMatchModesIsolateSessionAndInstanceCaches(t *testing.T) {
	ctx := MockContext()
	paramTypes := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	strictInput := paramMatchInput{actualTypes: paramTypes, modes: []paramMatchMode{matchStrict}}
	ignoredInput := paramMatchInput{actualTypes: paramTypes, modes: []paramMatchMode{matchIgnoreSourceType}}
	newValue := func(input paramMatchInput) *PlanCacheValue {
		return newPlanCacheValue(&Update{}, nil, input, &hint.StmtHints{})
	}

	t.Run("session", func(t *testing.T) {
		cache := NewLRUPlanCache(4, 0, 0, ctx, false)
		cache.Put("dml", newValue(strictInput), strictInput)
		cache.Put("dml", newValue(ignoredInput), ignoredInput)
		require.Equal(t, 2, cache.Size())
		value, ok := cache.Get("dml", ignoredInput)
		require.True(t, ok)
		require.Equal(t, ignoredInput.modes, value.(*PlanCacheValue).paramMatchModes)
	})

	t.Run("instance", func(t *testing.T) {
		cache := NewInstancePlanCache(1<<30, 1<<30)
		require.True(t, cache.Put("dml", newValue(strictInput), strictInput))
		require.True(t, cache.Put("dml", newValue(ignoredInput), ignoredInput))
		require.Equal(t, int64(2), cache.Size())
		value, ok := cache.Get("dml", strictInput)
		require.True(t, ok)
		require.Equal(t, strictInput.modes, value.(*PlanCacheValue).paramMatchModes)
	})
}

func TestDMLFKPlansFailClosedForInstancePlanCache(t *testing.T) {
	ctx := MockContext()
	tests := []struct {
		name string
		plan base.Plan
	}{
		{name: "insert check", plan: Insert{FKChecks: []*FKCheck{{}}}.Init(ctx)},
		{name: "insert cascade", plan: Insert{FKCascades: []*FKCascade{{}}}.Init(ctx)},
		{name: "update check", plan: Update{FKChecks: map[int64][]*FKCheck{1: {{}}}}.Init(ctx)},
		{name: "update cascade", plan: Update{FKCascades: map[int64][]*FKCascade{1: {{}}}}.Init(ctx)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.plan.Schema()
			_, ok := tt.plan.CloneForPlanCache(ctx)
			require.False(t, ok, "FK-bearing plans must not enter the instance plan cache")
		})
	}
}

func buildPhaseBRuleTestStmt(t *testing.T, suite *plannerSuite, sql string) *PlanCacheStmt {
	stmtNode, err := suite.p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	markers := ExtractAndSortParamMarkers(stmtNode)
	nodeW := resolve.NewNodeW(stmtNode)
	err = Preprocess(context.Background(), suite.sctx, nodeW, InPrepare,
		WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: suite.is}))
	require.NoError(t, err)
	stmt := &PlanCacheStmt{
		PreparedAst:   &ast.Prepared{Stmt: stmtNode},
		ResolveCtx:    nodeW.GetResolveContext(),
		Params:        markers,
		StmtCacheable: true,
		SchemaVersion: suite.is.SchemaMetaVersion(),
		RelateVersion: make(map[int64]uint64),
	}
	FinalizePreparedPlanCacheStmtInfo(context.Background(), suite.is, stmt, stmtNode)
	return stmt
}

func requireMatchModes(t *testing.T, modes []paramMatchMode, expected ...paramMatchMode) {
	require.Equal(t, expected, modes)
}

func TestPhaseBUpdateDirectWriteBindings(t *testing.T) {
	suite := createPlannerSuite()
	suite.sctx.GetSessionVars().EnablePlanCacheDMLParamTypeMatch = true

	for _, testCase := range []struct {
		name     string
		sql      string
		expected []paramMatchMode
	}{
		{
			name:     "write and where global order",
			sql:      "update t set b=?, c_str='ready' where a=?",
			expected: []paramMatchMode{matchIgnoreSourceType, matchStrict},
		},
		{
			name:     "numeric and character targets",
			sql:      "update t set b=?, c_str=? where a=?",
			expected: []paramMatchMode{matchIgnoreSourceType, matchIgnoreSourceType, matchStrict},
		},
		{
			name:     "alias",
			sql:      "update t as target set target.b=? where target.a=?",
			expected: []paramMatchMode{matchIgnoreSourceType, matchStrict},
		},
		{
			name:     "complex write marker fails whole statement",
			sql:      "update t set b=?+1, c=? where a=?",
			expected: []paramMatchMode{matchStrict, matchStrict, matchStrict},
		},
		{
			name:     "explicit cast marker fails whole statement",
			sql:      "update t set b=cast(? as signed), c=? where a=?",
			expected: []paramMatchMode{matchStrict, matchStrict, matchStrict},
		},
		{
			name:     "duplicate target fails whole statement",
			sql:      "update t set b=?, b=? where a=?",
			expected: []paramMatchMode{matchStrict, matchStrict, matchStrict},
		},
		{
			name:     "literal then marker duplicate fails whole statement",
			sql:      "update t set b=1, b=? where a=?",
			expected: []paramMatchMode{matchStrict, matchStrict},
		},
		{
			name:     "marker then literal duplicate fails whole statement",
			sql:      "update t set b=?, b=1 where a=?",
			expected: []paramMatchMode{matchStrict, matchStrict},
		},
		{
			name:     "marker then default duplicate fails whole statement",
			sql:      "update t set b=?, b=default where a=?",
			expected: []paramMatchMode{matchStrict, matchStrict},
		},
		{
			name:     "partition target fails closed",
			sql:      "update pt1 set b=? where a=?",
			expected: []paramMatchMode{matchStrict, matchStrict},
		},
		{
			name:     "use index fails closed",
			sql:      "update t use index(primary) set b=? where a=?",
			expected: []paramMatchMode{matchStrict, matchStrict},
		},
		{
			name:     "force index fails closed",
			sql:      "update t force index(primary) set b=? where a=?",
			expected: []paramMatchMode{matchStrict, matchStrict},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			stmt := buildPhaseBRuleTestStmt(t, suite, testCase.sql)
			suite.sctx.GetSessionVars().PlanCacheParams.Reset()
			for range stmt.Params {
				suite.sctx.GetSessionVars().PlanCacheParams.Append(types.NewIntDatum(1))
			}
			modes := getParamMatchModes(stmt)
			requireMatchModes(t, modes, testCase.expected...)
		})
	}
}

func TestPhaseBInsertDirectWriteBindings(t *testing.T) {
	suite := createPlannerSuite()
	suite.sctx.GetSessionVars().EnablePlanCacheDMLParamTypeMatch = true

	for _, testCase := range []struct {
		name     string
		sql      string
		expected []paramMatchMode
	}{
		{
			name:     "multi row values and on duplicate",
			sql:      "insert into t (b,c_str) values (?,?),(?,?) on duplicate key update c=?",
			expected: []paramMatchMode{matchIgnoreSourceType, matchIgnoreSourceType, matchIgnoreSourceType, matchIgnoreSourceType, matchIgnoreSourceType},
		},
		{
			name:     "insert set",
			sql:      "insert into t set b=?, c_str=?",
			expected: []paramMatchMode{matchIgnoreSourceType, matchIgnoreSourceType},
		},
		{
			name: "implicit visible column order",
			sql:  "insert into t values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			expected: []paramMatchMode{
				matchIgnoreSourceType, matchIgnoreSourceType, matchIgnoreSourceType, matchIgnoreSourceType,
				matchIgnoreSourceType, matchIgnoreSourceType, matchIgnoreSourceType, matchIgnoreSourceType,
				matchIgnoreSourceType, matchIgnoreSourceType, matchIgnoreSourceType, matchIgnoreSourceType,
			},
		},
		{
			name:     "complex values marker fails whole statement",
			sql:      "insert into t (b,c) values (?+1, ?)",
			expected: []paramMatchMode{matchStrict, matchStrict},
		},
		{
			name:     "explicit cast marker fails whole statement",
			sql:      "insert into t (b,c) values (cast(? as signed), ?)",
			expected: []paramMatchMode{matchStrict, matchStrict},
		},
		{
			name:     "complex on duplicate marker fails whole statement",
			sql:      "insert into t (b) values (?) on duplicate key update c=?+1",
			expected: []paramMatchMode{matchStrict, matchStrict},
		},
		{
			name:     "duplicate values target fails whole statement",
			sql:      "insert into t (b,b) values (?,?)",
			expected: []paramMatchMode{matchStrict, matchStrict},
		},
		{
			name:     "duplicate on duplicate target fails whole statement",
			sql:      "insert into t (b) values (?) on duplicate key update c=?, c=?",
			expected: []paramMatchMode{matchStrict, matchStrict, matchStrict},
		},
		{
			name:     "ignore fails closed",
			sql:      "insert ignore into t (b) values (?)",
			expected: []paramMatchMode{matchStrict},
		},
		{
			name:     "insert select fails closed",
			sql:      "insert into t (b) select ?",
			expected: []paramMatchMode{matchStrict},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			stmt := buildPhaseBRuleTestStmt(t, suite, testCase.sql)
			modes := getParamMatchModes(stmt)
			requireMatchModes(t, modes, testCase.expected...)
		})
	}
}

func TestNewPlanCacheKeyCompatibilityWrapper(t *testing.T) {
	suite := createPlannerSuite()
	const sql = "insert into t (b,c_str) values (?,?)"
	stmt := buildPhaseBRuleTestStmt(t, suite, sql)
	stmt.StmtText = sql

	legacyKey, legacyBinding, legacyCacheable, legacyReason, err := NewPlanCacheKey(suite.sctx, stmt)
	require.NoError(t, err)
	preparedKey, preparedBinding, preparedCacheable, preparedReason, err := NewPreparedPlanCacheKey(suite.sctx, stmt)
	require.NoError(t, err)
	require.Equal(t, preparedKey, legacyKey)
	require.Equal(t, preparedBinding, legacyBinding)
	require.Equal(t, preparedCacheable, legacyCacheable)
	require.Equal(t, preparedReason, legacyReason)
}
