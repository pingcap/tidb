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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

type maskingPolicyExprCacheEntry struct {
	expr   expression.Expression
	column *expression.Column
}

type maskingPolicyExprCache struct {
	schemaVersion int64
	sqlMode       mysql.SQLMode
	charset       string
	collation     string
	entries       map[int64]*maskingPolicyExprCacheEntry
	building      map[int64]struct{}
}

func getMaskingPolicyExprCache(sv *variable.SessionVars, schemaVersion int64) *maskingPolicyExprCache {
	cacheObj, cacheVer := sv.GetMaskingPolicyExprCache()
	charset, collation := sv.GetCharsetInfo()
	if cacheObj != nil {
		if cache, ok := cacheObj.(*maskingPolicyExprCache); ok && cacheVer == schemaVersion && cache.sqlMode == sv.SQLMode &&
			cache.charset == charset && cache.collation == collation {
			return cache
		}
	}
	cache := &maskingPolicyExprCache{
		schemaVersion: schemaVersion,
		sqlMode:       sv.SQLMode,
		charset:       charset,
		collation:     collation,
		entries:       make(map[int64]*maskingPolicyExprCacheEntry),
		building:      make(map[int64]struct{}),
	}
	sv.SetMaskingPolicyExprCache(cache, schemaVersion)
	return cache
}

func getMaskingPolicyExpr(
	ctx expression.BuildContext,
	sv *variable.SessionVars,
	schemaVersion int64,
	policy *model.MaskingPolicyInfo,
	tblInfo *model.TableInfo,
	colInfo *model.ColumnInfo,
) (expression.Expression, *expression.Column, error) {
	cache := getMaskingPolicyExprCache(sv, schemaVersion)
	if entry, ok := cache.entries[policy.ID]; ok {
		return entry.expr.Clone(), entry.column, nil
	}
	if _, ok := cache.building[policy.ID]; ok {
		return nil, nil, errors.New("masking policy expression recursion detected")
	}
	cache.building[policy.ID] = struct{}{}
	expr, placeholder, err := buildMaskingPolicyExpr(ctx, policy, tblInfo, colInfo)
	delete(cache.building, policy.ID)
	if err != nil {
		return nil, nil, err
	}
	if !isMaskingPolicyExprCacheable(expr) {
		ctx.SetSkipPlanCache("masking policy contains a function that cannot be safely cached")
		return expr, placeholder, nil
	}
	cache.entries[policy.ID] = &maskingPolicyExprCacheEntry{
		expr:   expr,
		column: placeholder,
	}
	return expr.Clone(), placeholder, nil
}

func buildMaskingPolicyExpr(
	ctx expression.BuildContext,
	policy *model.MaskingPolicyInfo,
	tblInfo *model.TableInfo,
	colInfo *model.ColumnInfo,
) (expression.Expression, *expression.Column, error) {
	if policy == nil || tblInfo == nil || colInfo == nil {
		return nil, nil, errors.New("masking policy expression requires policy/table/column info")
	}
	cols, names, err := expression.ColumnInfos2ColumnsAndNames(ctx, policy.DBName, tblInfo.Name, []*model.ColumnInfo{colInfo}, tblInfo)
	if err != nil {
		return nil, nil, err
	}
	schema := expression.NewSchema(cols...)
	expr, err := expression.ParseSimpleExpr(maskingPolicyBuildContext{ctx}, policy.Expression,
		expression.WithInputSchemaAndNames(schema, names, tblInfo),
		expression.WithDisableConstantFolding(), expression.WithDeferredFunctions(maskingPolicyIdentityFunctions))
	if err != nil {
		return nil, nil, err
	}
	if len(cols) != 1 {
		return nil, nil, errors.New("masking policy expression expects one input column")
	}
	return expr, cols[0], nil
}

// Keep lazy branches intact even when building the session-local expression cache
// outside a prepared execution. Returned expressions are cloned before optimization.
type maskingPolicyBuildContext struct{ expression.BuildContext }

func (maskingPolicyBuildContext) IsUseCache() bool { return true }

// User variables in policy SQL are absent from the outer AST cache checker.
// Preserve them so the cacheability check below can reject the dependency.
func (maskingPolicyBuildContext) IsReadonlyUserVar(string) bool { return false }

var maskingPolicyIdentityFunctions = map[string]struct{}{
	ast.CurrentUser: {}, ast.CurrentRole: {}, ast.User: {}, ast.SessionUser: {}, ast.SystemUser: {},
}

func isMaskingPolicyExprCacheable(expr expression.Expression) bool {
	switch x := expr.(type) {
	case *expression.Column:
		return true
	case *expression.Constant:
		if x.ParamMarker != nil {
			return false
		}
		return x.DeferredExpr == nil || isMaskingPolicyExprCacheable(x.DeferredExpr)
	case *expression.ScalarFunction:
		if _, ok := maskingPolicyIdentityFunctions[x.FuncName.L]; ok {
			return true
		}
		// The ordinary AST cache checker cannot inspect policy SQL. Keep an
		// explicit set of functions whose build-time dependencies are covered by
		// the plan-cache key and the expression-cache settings above. Other
		// functions may capture settings such as max_allowed_packet or the
		// decimal division precision, so rebuild them conservatively.
		switch x.FuncName.L {
		case ast.MaskFull, ast.MaskPartial, ast.MaskNull, ast.MaskDate,
			ast.Case, ast.If, ast.Ifnull, ast.Cast,
			ast.EQ, ast.NE, ast.LT, ast.LE, ast.GT, ast.GE, ast.NullEQ, ast.In,
			ast.LogicAnd, ast.LogicOr, ast.LogicXor, ast.UnaryNot,
			ast.IsNull, ast.IsTruthWithoutNull, ast.IsTruthWithNull, ast.IsFalsity,
			ast.Plus, ast.Minus, ast.Mul, ast.Mod, ast.UnaryMinus,
			ast.Abs, ast.Ceil, ast.Floor,
			ast.Lower, ast.Upper, ast.Length, ast.CharLength, ast.Substring,
			ast.Left, ast.Right, ast.Trim, ast.LTrim, ast.RTrim:
		default:
			return false
		}
		for _, arg := range x.GetArgs() {
			if !isMaskingPolicyExprCacheable(arg) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
