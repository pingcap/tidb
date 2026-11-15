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

package planner

import (
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
)

// deferSPFreshTxnWarmup lets a simple vector read reach physical-plan selection
// without prefetching an unused transaction timestamp. Planning starts the fresh
// timestamp that the SPFresh executor will actually consume. The compiler warms any plan
// that needs an ordinary transaction; optimizer reads activate it on demand.
// This changes neither index admission nor plan-cache/schema/MDL checks.
func deferSPFreshTxnWarmup(ctx base.PlanContext, node *resolve.NodeW) bool {
	if !config.SPFreshEnabled() || ctx.GetSessionVars().EnableCascadesPlanner {
		return false
	}
	s, ok := node.Node.(*ast.SelectStmt)
	if !ok || s.Limit == nil || s.OrderBy == nil || len(s.OrderBy.Items) != 1 ||
		s.OrderBy.Items[0].Desc || s.From == nil || s.From.TableRefs == nil ||
		s.From.TableRefs.Right != nil || s.LockInfo != nil || s.SelectIntoOpt != nil ||
		s.Distinct || s.GroupBy != nil || s.Having != nil || len(s.WindowSpecs) != 0 || s.With != nil {
		return false
	}
	call, ok := s.OrderBy.Items[0].Expr.(*ast.FuncCallExpr)
	if !ok || (call.FnName.L != ast.VecL2Distance && call.FnName.L != ast.VecCosineDistance) || len(call.Args) != 2 {
		return false
	}
	if _, ok := call.Args[0].(*ast.ColumnNameExpr); !ok {
		return false
	}
	switch call.Args[1].(type) {
	case ast.ValueExpr, ast.ParamMarkerExpr:
	default:
		return false
	}
	if s.Where != nil && s.Where.GetFlag()&ast.FlagHasSubquery != 0 {
		return false
	}
	for _, field := range s.Fields.Fields {
		if field.Expr != nil && field.Expr.GetFlag()&ast.FlagHasSubquery != 0 {
			return false
		}
	}
	source, ok := s.From.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return false
	}
	table, ok := source.Source.(*ast.TableName)
	if !ok {
		return false
	}
	resolved := node.GetResolveContext().GetTableName(table)
	for _, index := range resolved.TableInfo.Indices {
		if index.State == model.StatePublic && index.IsSPFreshIndex() {
			return true
		}
	}
	return false
}
