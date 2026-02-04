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

	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
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

	policy.Expression = "concat(c, 'x')"
	_, _, err = getMaskingPolicyExpr(ctx, sv, 1, policy, tblInfo, colInfo)
	require.NoError(t, err)
	require.Same(t, entry, cache.entries[policy.ID])

	_, _, err = getMaskingPolicyExpr(ctx, sv, 2, policy, tblInfo, colInfo)
	require.NoError(t, err)
	newCache := getMaskingPolicyExprCache(sv, 2)
	require.NotSame(t, cache, newCache)
	require.Len(t, newCache.entries, 1)
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
