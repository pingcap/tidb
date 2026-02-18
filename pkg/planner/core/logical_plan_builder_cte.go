// Copyright 2016 PingCAP, Inc.
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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func (b *PlanBuilder) buildCte(ctx context.Context, cte *ast.CommonTableExpression, isRecursive bool) (p base.LogicalPlan, err error) {
	saveBuildingCTE := b.buildingCTE
	b.buildingCTE = true
	defer func() {
		b.buildingCTE = saveBuildingCTE
	}()

	if isRecursive {
		// buildingRecursivePartForCTE likes a stack. We save it before building a recursive CTE and restore it after building.
		// We need a stack because we need to handle the nested recursive CTE. And buildingRecursivePartForCTE indicates the innermost CTE.
		saveCheck := b.buildingRecursivePartForCTE
		b.buildingRecursivePartForCTE = false
		err = b.buildRecursiveCTE(ctx, cte.Query.Query)
		if err != nil {
			return nil, err
		}
		b.buildingRecursivePartForCTE = saveCheck
	} else {
		p, err = b.buildResultSetNode(ctx, cte.Query.Query, true)
		if err != nil {
			return nil, err
		}

		p, err = b.adjustCTEPlanOutputName(p, cte)
		if err != nil {
			return nil, err
		}

		cInfo := b.outerCTEs[len(b.outerCTEs)-1]
		cInfo.seedLP = p
	}
	return nil, nil
}

// buildRecursiveCTE handles the with clause `with recursive xxx as xx`.
// It will push one entry into b.handleHelper.
func (b *PlanBuilder) buildRecursiveCTE(ctx context.Context, cte ast.ResultSetNode) error {
	b.isCTE = true
	cInfo := b.outerCTEs[len(b.outerCTEs)-1]
	switch x := (cte).(type) {
	case *ast.SetOprStmt:
		// 1. Handle the WITH clause if exists.
		if x.With != nil {
			l := len(b.outerCTEs)
			sw := x.With
			defer func() {
				b.outerCTEs = b.outerCTEs[:l]
				x.With = sw
			}()
			_, err := b.buildWith(ctx, x.With)
			if err != nil {
				return err
			}
		}
		// Set it to nil, so that when builds the seed part, it won't build again. Reset it in defer so that the AST doesn't change after this function.
		x.With = nil

		// 2. Build plans for each part of SetOprStmt.
		recursive := make([]base.LogicalPlan, 0)
		tmpAfterSetOptsForRecur := []*ast.SetOprType{nil}

		expectSeed := true
		for i := 0; i < len(x.SelectList.Selects); i++ {
			var p base.LogicalPlan
			var err error
			originalLen := b.handleHelper.stackTail

			var afterOpr *ast.SetOprType
			switch y := x.SelectList.Selects[i].(type) {
			case *ast.SelectStmt:
				p, err = b.buildSelect(ctx, y)
				afterOpr = y.AfterSetOperator
			case *ast.SetOprSelectList:
				p, err = b.buildSetOpr(ctx, &ast.SetOprStmt{SelectList: y, With: y.With})
				afterOpr = y.AfterSetOperator
			}

			// This is for maintain b.handleHelper instead of normal error handling. Since one error is expected if
			// expectSeed && cInfo.useRecursive, error handling is in the "if expectSeed" block below.
			if err == nil {
				b.handleHelper.popMap()
			} else {
				// Be careful with this tricky case. One error is expected here when building the first recursive
				// part, however, the b.handleHelper won't be restored if error occurs, which means there could be
				// more than one entry pushed into b.handleHelper without being poped.
				// For example: with recursive cte1 as (select ... union all select ... from tbl join cte1 ...) ...
				// This violates the semantic of buildSelect() and buildSetOpr(), which should only push exactly one
				// entry into b.handleHelper. So we use a special logic to restore the b.handleHelper here.
				for b.handleHelper.stackTail > originalLen {
					b.handleHelper.popMap()
				}
			}

			if expectSeed {
				if cInfo.useRecursive {
					// 3. If it fail to build a plan, it may be the recursive part. Then we build the seed part plan, and rebuild it.
					if i == 0 {
						return plannererrors.ErrCTERecursiveRequiresNonRecursiveFirst.GenWithStackByArgs(cInfo.def.Name.String())
					}

					// It's the recursive part. Build the seed part, and build this recursive part again.
					// Before we build the seed part, do some checks.
					if x.OrderBy != nil {
						return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("ORDER BY over UNION in recursive Common Table Expression")
					}
					// Limit clause is for the whole CTE instead of only for the seed part.
					oriLimit := x.Limit
					x.Limit = nil

					// Check union type.
					if afterOpr != nil {
						if *afterOpr != ast.Union && *afterOpr != ast.UnionAll {
							return plannererrors.ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("%s between seed part and recursive part, hint: The operator between seed part and recursive part must bu UNION[DISTINCT] or UNION ALL", afterOpr.String()))
						}
						cInfo.isDistinct = *afterOpr == ast.Union
					}

					expectSeed = false
					cInfo.useRecursive = false

					// Build seed part plan.
					saveSelect := x.SelectList.Selects
					x.SelectList.Selects = x.SelectList.Selects[:i]
					p, err = b.buildSetOpr(ctx, x)
					if err != nil {
						return err
					}
					b.handleHelper.popMap()
					x.SelectList.Selects = saveSelect
					p, err = b.adjustCTEPlanOutputName(p, cInfo.def)
					if err != nil {
						return err
					}
					cInfo.seedLP = p

					// Rebuild the plan.
					i--
					b.buildingRecursivePartForCTE = true
					x.Limit = oriLimit
					continue
				}
				if err != nil {
					return err
				}
			} else {
				if err != nil {
					return err
				}
				if afterOpr != nil {
					if *afterOpr != ast.Union && *afterOpr != ast.UnionAll {
						return plannererrors.ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("%s between recursive part's selects, hint: The operator between recursive part's selects must bu UNION[DISTINCT] or UNION ALL", afterOpr.String()))
					}
				}
				if !cInfo.useRecursive {
					return plannererrors.ErrCTERecursiveRequiresNonRecursiveFirst.GenWithStackByArgs(cInfo.def.Name.String())
				}
				cInfo.useRecursive = false
				recursive = append(recursive, p)
				tmpAfterSetOptsForRecur = append(tmpAfterSetOptsForRecur, afterOpr)
			}
		}

		if len(recursive) == 0 {
			// In this case, even if SQL specifies "WITH RECURSIVE", the CTE is non-recursive.
			p, err := b.buildSetOpr(ctx, x)
			if err != nil {
				return err
			}
			p, err = b.adjustCTEPlanOutputName(p, cInfo.def)
			if err != nil {
				return err
			}
			cInfo.seedLP = p
			return nil
		}

		// Build the recursive part's logical plan.
		recurPart, err := b.buildUnion(ctx, recursive, tmpAfterSetOptsForRecur)
		if err != nil {
			return err
		}
		recurPart, err = b.buildProjection4CTEUnion(ctx, cInfo.seedLP, recurPart)
		if err != nil {
			return err
		}
		// 4. Finally, we get the seed part plan and recursive part plan.
		cInfo.recurLP = recurPart
		// Only need to handle limit if x is SetOprStmt.
		if x.Limit != nil {
			limit, err := b.buildLimit(cInfo.seedLP, x.Limit)
			if err != nil {
				return err
			}
			limit.SetChildren(limit.Children()[:0]...)
			cInfo.limitLP = limit
		}
		b.handleHelper.pushMap(nil)
		return nil
	default:
		p, err := b.buildResultSetNode(ctx, x, true)
		if err != nil {
			// Refine the error message.
			if errors.ErrorEqual(err, plannererrors.ErrCTERecursiveRequiresNonRecursiveFirst) {
				err = plannererrors.ErrCTERecursiveRequiresUnion.GenWithStackByArgs(cInfo.def.Name.String())
			}
			return err
		}
		p, err = b.adjustCTEPlanOutputName(p, cInfo.def)
		if err != nil {
			return err
		}
		cInfo.seedLP = p
		return nil
	}
}

func (b *PlanBuilder) adjustCTEPlanOutputName(p base.LogicalPlan, def *ast.CommonTableExpression) (base.LogicalPlan, error) {
	outPutNames := p.OutputNames()
	for _, name := range outPutNames {
		name.TblName = def.Name
		if name.DBName.String() == "" {
			name.DBName = ast.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
		}
	}
	if len(def.ColNameList) > 0 {
		if len(def.ColNameList) != len(p.OutputNames()) {
			return nil, dbterror.ErrViewWrongList
		}
		for i, n := range def.ColNameList {
			outPutNames[i].ColName = n
		}
	}
	p.SetOutputNames(outPutNames)
	return p, nil
}

// prepareCTECheckForSubQuery prepares the check that the recursive CTE can't be referenced in subQuery. It's used before building a subQuery.
// For example: with recursive cte(n) as (select 1 union select * from (select * from cte) c1) select * from cte;
func (b *PlanBuilder) prepareCTECheckForSubQuery() []*cteInfo {
	modifiedCTE := make([]*cteInfo, 0)
	for _, cte := range b.outerCTEs {
		if cte.isBuilding && !cte.enterSubquery {
			cte.enterSubquery = true
			modifiedCTE = append(modifiedCTE, cte)
		}
	}
	return modifiedCTE
}

// resetCTECheckForSubQuery resets the related variable. It's used after leaving a subQuery.
func resetCTECheckForSubQuery(ci []*cteInfo) {
	for _, cte := range ci {
		cte.enterSubquery = false
	}
}

// genCTETableNameForError find the nearest CTE name.
func (b *PlanBuilder) genCTETableNameForError() string {
	name := ""
	for i := len(b.outerCTEs) - 1; i >= 0; i-- {
		if b.outerCTEs[i].isBuilding {
			name = b.outerCTEs[i].def.Name.String()
			break
		}
	}
	return name
}

func (b *PlanBuilder) buildWith(ctx context.Context, w *ast.WithClause) ([]*cteInfo, error) {
	// Check CTE name must be unique.
	b.nameMapCTE = make(map[string]struct{})
	for _, cte := range w.CTEs {
		if _, ok := b.nameMapCTE[cte.Name.L]; ok {
			return nil, plannererrors.ErrNonUniqTable
		}
		b.nameMapCTE[cte.Name.L] = struct{}{}
	}
	ctes := make([]*cteInfo, 0, len(w.CTEs))
	for _, cte := range w.CTEs {
		b.outerCTEs = append(b.outerCTEs, &cteInfo{def: cte, nonRecursive: !w.IsRecursive, isBuilding: true, storageID: b.allocIDForCTEStorage, seedStat: &property.StatsInfo{}, consumerCount: cte.ConsumerCount})
		b.allocIDForCTEStorage++
		saveFlag := b.optFlag
		// Init the flag to flagPrunColumns, otherwise it's missing.
		b.optFlag = rule.FlagPruneColumns
		if b.ctx.GetSessionVars().EnableForceInlineCTE() {
			b.outerCTEs[len(b.outerCTEs)-1].forceInlineByHintOrVar = true
		}
		_, err := b.buildCte(ctx, cte, w.IsRecursive)
		if err != nil {
			return nil, err
		}
		b.outerCTEs[len(b.outerCTEs)-1].optFlag = b.optFlag
		b.outerCTEs[len(b.outerCTEs)-1].isBuilding = false
		b.optFlag = saveFlag
		// buildCte() will push one entry into handleHelper. As said in comments for b.handleHelper, building CTE
		// should not affect the handleColHelper, so we pop it out here, then buildWith() as a whole will not modify
		// the handleColHelper.
		b.handleHelper.popMap()
		ctes = append(ctes, b.outerCTEs[len(b.outerCTEs)-1])
	}
	return ctes, nil
}

func (b *PlanBuilder) buildProjection4CTEUnion(_ context.Context, seed base.LogicalPlan, recur base.LogicalPlan) (base.LogicalPlan, error) {
	if seed.Schema().Len() != recur.Schema().Len() {
		return nil, plannererrors.ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
	}
	exprs := make([]expression.Expression, len(seed.Schema().Columns))
	resSchema := getResultCTESchema(seed.Schema(), b.ctx.GetSessionVars())
	for i, col := range recur.Schema().Columns {
		if !resSchema.Columns[i].RetType.Equal(col.RetType) {
			exprs[i] = expression.BuildCastFunction4Union(b.ctx.GetExprCtx(), col, resSchema.Columns[i].RetType)
		} else {
			exprs[i] = col
		}
	}
	b.optFlag |= rule.FlagEliminateProjection
	proj := logicalop.LogicalProjection{Exprs: exprs}.Init(b.ctx, b.getSelectOffset())
	proj.SetSchema(resSchema)
	proj.SetChildren(recur)
	return proj, nil
}

// The recursive part/CTE's schema is nullable, and the UID should be unique.
func getResultCTESchema(seedSchema *expression.Schema, svar *variable.SessionVars) *expression.Schema {
	res := seedSchema.Clone()
	for _, col := range res.Columns {
		col.RetType = col.RetType.Clone()
		col.UniqueID = svar.AllocPlanColumnID()
		col.RetType.DelFlag(mysql.NotNullFlag)
		// Since you have reallocated unique id here, the old-cloned-cached hash code is not valid anymore.
		col.CleanHashCode()
	}
	return res
}
