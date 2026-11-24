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
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
)

func TestMaskingPolicyExprCache(t *testing.T) {
	sv := variable.NewSessionVars(nil)
	ctx := exprstatic.NewExprContext()
	tblInfo := &model.TableInfo{Name: ast.NewCIStr("t")}
	colInfo := &model.ColumnInfo{
		ID:        1,
		Name:      ast.NewCIStr("c"),
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeString),
	}
	tblInfo.Columns = []*model.ColumnInfo{colInfo}

	policy := &model.MaskingPolicyInfo{
		ID:         1,
		Name:       ast.NewCIStr("p"),
		DBName:     ast.NewCIStr("test"),
		TableName:  tblInfo.Name,
		TableID:    100,
		ColumnName: colInfo.Name,
		ColumnID:   colInfo.ID,
		Expression: "c",
	}

	expr1, col1, err := getMaskingPolicyExpr(ctx, sv, 1, policy, tblInfo, colInfo)
	require.NoError(t, err)
	require.NotNil(t, expr1)
	require.NotNil(t, col1)

	cache := getMaskingPolicyExprCache(sv, 1)
	entry := cache.entries[policy.ID]
	require.NotNil(t, entry)

	// The same inputs on the same schema version reuse the cached entry.
	_, _, err = getMaskingPolicyExpr(ctx, sv, 1, policy, tblInfo, colInfo)
	require.NoError(t, err)
	require.Same(t, entry, cache.entries[policy.ID])

	// A policy or table taken from a newer schema under the same version, as
	// for a table replaced under MDL, rebuilds the expression.
	policy.Expression = "upper(c)"
	_, _, err = getMaskingPolicyExpr(ctx, sv, 1, policy, tblInfo, colInfo)
	require.NoError(t, err)
	require.NotSame(t, entry, cache.entries[policy.ID])
	entry = cache.entries[policy.ID]
	replaced := tblInfo.Clone()
	replaced.ID = 101
	_, _, err = getMaskingPolicyExpr(ctx, sv, 1, policy, replaced, replaced.Columns[0])
	require.NoError(t, err)
	require.NotSame(t, entry, cache.entries[policy.ID])
	entry = cache.entries[policy.ID]
	retyped := replaced.Columns[0].Clone()
	retyped.FieldType = *types.NewFieldType(mysql.TypeVarchar)
	_, _, err = getMaskingPolicyExpr(ctx, sv, 1, policy, replaced, retyped)
	require.NoError(t, err)
	require.NotSame(t, entry, cache.entries[policy.ID])
	// A change of nullability alone also rebuilds the expression: the loose
	// FieldType.Equal ignores the NOT NULL flag.
	entry = cache.entries[policy.ID]
	notNull := retyped.Clone()
	notNull.AddFlag(mysql.NotNullFlag)
	_, _, err = getMaskingPolicyExpr(ctx, sv, 1, policy, replaced, notNull)
	require.NoError(t, err)
	require.NotSame(t, entry, cache.entries[policy.ID])
	entry = cache.entries[policy.ID]
	_, _, err = getMaskingPolicyExpr(ctx, sv, 1, policy, replaced, notNull.Clone())
	require.NoError(t, err)
	require.Same(t, entry, cache.entries[policy.ID])

	// Bumping the schema version should create a fresh cache.
	_, _, err = getMaskingPolicyExpr(ctx, sv, 2, policy, tblInfo, colInfo)
	require.NoError(t, err)
	newCache := getMaskingPolicyExprCache(sv, 2)
	require.NotSame(t, cache, newCache)
	require.Len(t, newCache.entries, 1)

	// Functions capturing session settings also bypass this expression cache.
	policy.Expression = "concat(c, 'x')"
	_, _, err = getMaskingPolicyExpr(ctx, sv, 3, policy, tblInfo, colInfo)
	require.NoError(t, err)
	require.Empty(t, getMaskingPolicyExprCache(sv, 3).entries)
}

func TestMaskingPolicyExprCacheRecursion(t *testing.T) {
	sv := variable.NewSessionVars(nil)
	ctx := exprstatic.NewExprContext()
	tblInfo := &model.TableInfo{Name: ast.NewCIStr("t")}
	colInfo := &model.ColumnInfo{
		ID:        1,
		Name:      ast.NewCIStr("c"),
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeString),
	}
	tblInfo.Columns = []*model.ColumnInfo{colInfo}

	policy := &model.MaskingPolicyInfo{
		ID:         1,
		Name:       ast.NewCIStr("p"),
		DBName:     ast.NewCIStr("test"),
		TableName:  tblInfo.Name,
		TableID:    100,
		ColumnName: colInfo.Name,
		ColumnID:   colInfo.ID,
		Expression: "c",
	}

	cache := getMaskingPolicyExprCache(sv, 1)
	cache.building[policy.ID] = struct{}{}
	_, _, err := getMaskingPolicyExpr(ctx, sv, 1, policy, tblInfo, colInfo)
	require.Error(t, err)
}

func TestMaskingPolicyPointGetMemoryUsage(t *testing.T) {
	exprs := []expression.Expression{&expression.Column{RetType: types.NewFieldType(mysql.TypeVarchar)}}
	expected := int64(cap(exprs))*size.SizeOfInterface + exprs[0].MemoryUsage()
	require.Positive(t, expected)

	pointGet := &physicalop.PointGetPlan{}
	base := pointGet.MemoryUsage()
	pointGet.MaskingExprs = exprs
	require.Equal(t, base+expected, pointGet.MemoryUsage())

	batchPointGet := &physicalop.BatchPointGetPlan{}
	base = batchPointGet.MemoryUsage()
	batchPointGet.MaskingExprs = exprs
	require.Equal(t, base+expected, batchPointGet.MemoryUsage())
}

func TestHasNewColumnID(t *testing.T) {
	col := func(id int64, state model.SchemaState) *model.ColumnInfo {
		return &model.ColumnInfo{ID: id, State: state}
	}
	pub := model.StatePublic
	tbl := func(ts uint64, maxID int64, cols ...*model.ColumnInfo) *model.TableInfo {
		return &model.TableInfo{ID: 1, Columns: cols, MaxColumnID: maxID, UpdateTS: ts}
	}
	old := tbl(1, 3, col(1, pub), col(2, pub), col(3, pub))
	require.False(t, hasNewColumnID(old, old))
	// The same version of the table, even in another object.
	require.False(t, hasNewColumnID(old, tbl(1, 3, col(1, pub), col(2, pub), col(3, pub))))
	// A dropped column, or a column added and dropped again, brings no new ID.
	require.False(t, hasNewColumnID(old, tbl(2, 4, col(1, pub), col(3, pub))))
	// An online MODIFY COLUMN: the changing column takes ID 4 before it is
	// public, and replaces column 2 when it becomes public. Both versions have
	// the same MaxColumnID.
	changing := tbl(2, 4, col(1, pub), col(2, pub), col(3, pub), col(4, model.StateWriteReorganization))
	public := tbl(3, 4, col(1, pub), col(4, pub), col(3, pub))
	require.False(t, hasNewColumnID(old, changing))
	require.True(t, hasNewColumnID(old, public))
	require.True(t, hasNewColumnID(changing, public))
	// A new public column.
	require.True(t, hasNewColumnID(old, tbl(4, 5, col(1, pub), col(2, pub), col(3, pub), col(5, pub))))
}
