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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/set"
)

func (b *PlanBuilder) buildJoin(ctx context.Context, joinNode *ast.Join) (base.LogicalPlan, error) {
	// We will construct a "Join" node for some statements like "INSERT",
	// "DELETE", "UPDATE", "REPLACE". For this scenario "joinNode.Right" is nil
	// and we only build the left "ResultSetNode".
	if joinNode.Right == nil {
		return b.buildResultSetNode(ctx, joinNode.Left, false)
	}

	b.optFlag = b.optFlag | rule.FlagPredicatePushDown
	// Add join reorder flag regardless of inner join or outer join.
	b.optFlag = b.optFlag | rule.FlagJoinReOrder
	b.optFlag |= rule.FlagPredicateSimplification
	b.optFlag |= rule.FlagEmptySelectionEliminator

	leftPlan, err := b.buildResultSetNode(ctx, joinNode.Left, false)
	if err != nil {
		return nil, err
	}

	rightPlan, err := b.buildResultSetNode(ctx, joinNode.Right, false)
	if err != nil {
		return nil, err
	}

	// The recursive part in CTE must not be on the right side of a LEFT JOIN.
	if lc, ok := rightPlan.(*logicalop.LogicalCTETable); ok && joinNode.Tp == ast.LeftJoin {
		return nil, plannererrors.ErrCTERecursiveForbiddenJoinOrder.GenWithStackByArgs(lc.Name)
	}

	handleMap1 := b.handleHelper.popMap()
	handleMap2 := b.handleHelper.popMap()
	b.handleHelper.mergeAndPush(handleMap1, handleMap2)

	joinPlan := logicalop.LogicalJoin{StraightJoin: joinNode.StraightJoin || b.inStraightJoin}.Init(b.ctx, b.getSelectOffset())
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema()))
	joinPlan.SetOutputNames(make([]*types.FieldName, leftPlan.Schema().Len()+rightPlan.Schema().Len()))
	copy(joinPlan.OutputNames(), leftPlan.OutputNames())
	copy(joinPlan.OutputNames()[leftPlan.Schema().Len():], rightPlan.OutputNames())

	// Set join type.
	switch joinNode.Tp {
	case ast.LeftJoin:
		// left outer join need to be checked elimination
		b.optFlag = b.optFlag | rule.FlagEliminateOuterJoin | rule.FlagOuterJoinToSemiJoin
		joinPlan.JoinType = base.LeftOuterJoin
		util.ResetNotNullFlag(joinPlan.Schema(), leftPlan.Schema().Len(), joinPlan.Schema().Len())
	case ast.RightJoin:
		// right outer join need to be checked elimination
		b.optFlag = b.optFlag | rule.FlagEliminateOuterJoin | rule.FlagOuterJoinToSemiJoin
		joinPlan.JoinType = base.RightOuterJoin
		util.ResetNotNullFlag(joinPlan.Schema(), 0, leftPlan.Schema().Len())
	default:
		joinPlan.JoinType = base.InnerJoin
	}

	// Merge sub-plan's FullSchema into this join plan.
	// Please read the comment of LogicalJoin.FullSchema for the details.
	var (
		lFullSchema, rFullSchema *expression.Schema
		lFullNames, rFullNames   types.NameSlice
	)
	if left, ok := leftPlan.(*logicalop.LogicalJoin); ok && left.FullSchema != nil {
		lFullSchema = left.FullSchema
		lFullNames = left.FullNames
	} else {
		lFullSchema = leftPlan.Schema()
		lFullNames = leftPlan.OutputNames()
	}
	if right, ok := rightPlan.(*logicalop.LogicalJoin); ok && right.FullSchema != nil {
		rFullSchema = right.FullSchema
		rFullNames = right.FullNames
	} else {
		rFullSchema = rightPlan.Schema()
		rFullNames = rightPlan.OutputNames()
	}
	if joinNode.Tp == ast.RightJoin {
		// Make sure lFullSchema means outer full schema and rFullSchema means inner full schema.
		lFullSchema, rFullSchema = rFullSchema, lFullSchema
		lFullNames, rFullNames = rFullNames, lFullNames
	}
	joinPlan.FullSchema = expression.MergeSchema(lFullSchema, rFullSchema)

	// Clear NotNull flag for the inner side schema if it's an outer join.
	if joinNode.Tp == ast.LeftJoin || joinNode.Tp == ast.RightJoin {
		util.ResetNotNullFlag(joinPlan.FullSchema, lFullSchema.Len(), joinPlan.FullSchema.Len())
	}

	// Merge sub-plan's FullNames into this join plan, similar to the FullSchema logic above.
	joinPlan.FullNames = make([]*types.FieldName, 0, len(lFullNames)+len(rFullNames))
	for _, lName := range lFullNames {
		name := *lName
		joinPlan.FullNames = append(joinPlan.FullNames, &name)
	}
	for _, rName := range rFullNames {
		name := *rName
		joinPlan.FullNames = append(joinPlan.FullNames, &name)
	}

	// Set preferred join algorithm if some join hints is specified by user.
	joinPlan.SetPreferredJoinTypeAndOrder(b.TableHints())

	// "NATURAL JOIN" doesn't have "ON" or "USING" conditions.
	//
	// The "NATURAL [LEFT] JOIN" of two tables is defined to be semantically
	// equivalent to an "INNER JOIN" or a "LEFT JOIN" with a "USING" clause
	// that names all columns that exist in both tables.
	//
	// See https://dev.mysql.com/doc/refman/5.7/en/join.html for more detail.
	if joinNode.NaturalJoin {
		err = b.buildNaturalJoin(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			return nil, err
		}
	} else if joinNode.Using != nil {
		err = b.buildUsingClause(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			return nil, err
		}
	} else if joinNode.On != nil {
		b.curClause = onClause
		onExpr, newPlan, err := b.rewrite(ctx, joinNode.On.Expr, joinPlan, nil, false)
		if err != nil {
			return nil, err
		}
		if newPlan != joinPlan {
			return nil, errors.New("ON condition doesn't support subqueries yet")
		}
		onCondition := expression.SplitCNFItems(onExpr)
		// Keep these expressions as a LogicalSelection upon the inner join, in order to apply
		// possible decorrelate optimizations. The ON clause is actually treated as a WHERE clause now.
		if joinPlan.JoinType == base.InnerJoin {
			sel := logicalop.LogicalSelection{Conditions: onCondition}.Init(b.ctx, b.getSelectOffset())
			sel.SetChildren(joinPlan)
			return sel, nil
		}
		joinPlan.AttachOnConds(onCondition)
	}
	return joinPlan, nil
}

// buildUsingClause eliminate the redundant columns and ordering columns based
// on the "USING" clause.
//
// According to the standard SQL, columns are ordered in the following way:
//  1. coalesced common columns of "leftPlan" and "rightPlan", in the order they
//     appears in "leftPlan".
//  2. the rest columns in "leftPlan", in the order they appears in "leftPlan".
//  3. the rest columns in "rightPlan", in the order they appears in "rightPlan".
func (b *PlanBuilder) buildUsingClause(p *logicalop.LogicalJoin, leftPlan, rightPlan base.LogicalPlan, join *ast.Join) error {
	filter := make(map[string]bool, len(join.Using))
	for _, col := range join.Using {
		filter[col.Name.L] = true
	}
	err := b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp, filter)
	if err != nil {
		return err
	}
	// We do not need to coalesce columns for update and delete.
	if b.inUpdateStmt || b.inDeleteStmt {
		p.SetSchemaAndNames(expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema()),
			append(p.Children()[0].OutputNames(), p.Children()[1].OutputNames()...))
	}
	return nil
}

// buildNaturalJoin builds natural join output schema. It finds out all the common columns
// then using the same mechanism as buildUsingClause to eliminate redundant columns and build join conditions.
// According to standard SQL, producing this display order:
//
//	All the common columns
//	Every column in the first (left) table that is not a common column
//	Every column in the second (right) table that is not a common column
func (b *PlanBuilder) buildNaturalJoin(p *logicalop.LogicalJoin, leftPlan, rightPlan base.LogicalPlan, join *ast.Join) error {
	err := b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp, nil)
	if err != nil {
		return err
	}
	// We do not need to coalesce columns for update and delete.
	if b.inUpdateStmt || b.inDeleteStmt {
		p.SetSchemaAndNames(expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema()),
			append(p.Children()[0].OutputNames(), p.Children()[1].OutputNames()...))
	}
	return nil
}

// coalesceCommonColumns is used by buildUsingClause and buildNaturalJoin. The filter is used by buildUsingClause.
func (b *PlanBuilder) coalesceCommonColumns(p *logicalop.LogicalJoin, leftPlan, rightPlan base.LogicalPlan, joinTp ast.JoinType, filter map[string]bool) error {
	lsc := leftPlan.Schema().Clone()
	rsc := rightPlan.Schema().Clone()
	if joinTp == ast.LeftJoin {
		util.ResetNotNullFlag(rsc, 0, rsc.Len())
	} else if joinTp == ast.RightJoin {
		util.ResetNotNullFlag(lsc, 0, lsc.Len())
	}
	lColumns, rColumns := lsc.Columns, rsc.Columns
	lNames, rNames := leftPlan.OutputNames().Shallow(), rightPlan.OutputNames().Shallow()
	if joinTp == ast.RightJoin {
		leftPlan, rightPlan = rightPlan, leftPlan
		lNames, rNames = rNames, lNames
		lColumns, rColumns = rsc.Columns, lsc.Columns
	}

	// Check using clause with ambiguous columns.
	if filter != nil {
		checkAmbiguous := func(names types.NameSlice) error {
			columnNameInFilter := set.StringSet{}
			for _, name := range names {
				if _, ok := filter[name.ColName.L]; !ok {
					continue
				}
				if columnNameInFilter.Exist(name.ColName.L) {
					return plannererrors.ErrAmbiguous.GenWithStackByArgs(name.ColName.L, "from clause")
				}
				columnNameInFilter.Insert(name.ColName.L)
			}
			return nil
		}
		err := checkAmbiguous(lNames)
		if err != nil {
			return err
		}
		err = checkAmbiguous(rNames)
		if err != nil {
			return err
		}
	} else {
		// Even with no using filter, we still should check the checkAmbiguous name before we try to find the common column from both side.
		// (t3 cross join t4) natural join t1
		//  t1 natural join (t3 cross join t4)
		// t3 and t4 may generate the same name column from cross join.
		// for every common column of natural join, the name from right or left should be exactly one.
		commonNames := make([]string, 0, len(lNames))
		lNameMap := make(map[string]int, len(lNames))
		rNameMap := make(map[string]int, len(rNames))
		for _, name := range lNames {
			// Natural join should ignore _tidb_rowid and _tidb_commit_ts
			if name.ColName.L == model.ExtraHandleName.L ||
				name.ColName.L == model.ExtraCommitTSName.L ||
				name.ColName.L == model.ExtraPhysTblIDName.L {
				continue
			}
			// record left map
			if cnt, ok := lNameMap[name.ColName.L]; ok {
				lNameMap[name.ColName.L] = cnt + 1
			} else {
				lNameMap[name.ColName.L] = 1
			}
		}
		for _, name := range rNames {
			// Natural join should ignore _tidb_rowid and _tidb_commit_ts
			if name.ColName.L == model.ExtraHandleName.L ||
				name.ColName.L == model.ExtraCommitTSName.L ||
				name.ColName.L == model.ExtraPhysTblIDName.L {
				continue
			}
			// record right map
			if cnt, ok := rNameMap[name.ColName.L]; ok {
				rNameMap[name.ColName.L] = cnt + 1
			} else {
				rNameMap[name.ColName.L] = 1
			}
			// check left map
			if cnt, ok := lNameMap[name.ColName.L]; ok {
				if cnt > 1 {
					return plannererrors.ErrAmbiguous.GenWithStackByArgs(name.ColName.L, "from clause")
				}
				commonNames = append(commonNames, name.ColName.L)
			}
		}
		// check right map
		for _, commonName := range commonNames {
			if rNameMap[commonName] > 1 {
				return plannererrors.ErrAmbiguous.GenWithStackByArgs(commonName, "from clause")
			}
		}
	}

	// Find out all the common columns and put them ahead.
	commonLen := 0
	for i, lName := range lNames {
		// Natural join should ignore _tidb_rowid and _tidb_commit_ts
		if lName.ColName.L == model.ExtraHandleName.L ||
			lName.ColName.L == model.ExtraCommitTSName.L ||
			lName.ColName.L == model.ExtraPhysTblIDName.L {
			continue
		}
		for j := commonLen; j < len(rNames); j++ {
			if lName.ColName.L != rNames[j].ColName.L {
				continue
			}

			if len(filter) > 0 {
				if !filter[lName.ColName.L] {
					break
				}
				// Mark this column exist.
				filter[lName.ColName.L] = false
			}

			col := lColumns[i]
			copy(lColumns[commonLen+1:i+1], lColumns[commonLen:i])
			lColumns[commonLen] = col

			name := lNames[i]
			copy(lNames[commonLen+1:i+1], lNames[commonLen:i])
			lNames[commonLen] = name

			col = rColumns[j]
			copy(rColumns[commonLen+1:j+1], rColumns[commonLen:j])
			rColumns[commonLen] = col

			name = rNames[j]
			copy(rNames[commonLen+1:j+1], rNames[commonLen:j])
			rNames[commonLen] = name

			commonLen++
			break
		}
	}

	if len(filter) > 0 && len(filter) != commonLen {
		for col, notExist := range filter {
			if notExist {
				return plannererrors.ErrUnknownColumn.GenWithStackByArgs(col, "from clause")
			}
		}
	}

	schemaCols := make([]*expression.Column, len(lColumns)+len(rColumns)-commonLen)
	copy(schemaCols[:len(lColumns)], lColumns)
	copy(schemaCols[len(lColumns):], rColumns[commonLen:])
	names := make(types.NameSlice, len(schemaCols))
	copy(names, lNames)
	copy(names[len(lNames):], rNames[commonLen:])

	conds := make([]expression.Expression, 0, commonLen)
	for i := range commonLen {
		lc, rc := lsc.Columns[i], rsc.Columns[i]
		cond, err := expression.NewFunction(b.ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), lc, rc)
		if err != nil {
			return err
		}
		conds = append(conds, cond)
		if p.FullSchema != nil {
			// since FullSchema is derived from left and right schema in upper layer, so rc/lc must be in FullSchema.
			if joinTp == ast.RightJoin {
				p.FullNames[p.FullSchema.ColumnIndex(lc)].Redundant = true
			} else {
				p.FullNames[p.FullSchema.ColumnIndex(rc)].Redundant = true
			}
		}
	}

	p.SetSchema(expression.NewSchema(schemaCols...))
	p.SetOutputNames(names)

	p.OtherConditions = append(conds, p.OtherConditions...)

	return nil
}
