// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/autocommit"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must be the same one used in InfoBinder.
type executorBuilder struct {
	ctx context.Context
	is  infoschema.InfoSchema
	err error
}

func newExecutorBuilder(ctx context.Context, is infoschema.InfoSchema) *executorBuilder {
	return &executorBuilder{
		ctx: ctx,
		is:  is,
	}
}

func (b *executorBuilder) build(p plan.Plan) Executor {
	switch v := p.(type) {
	case nil:
		return nil
	case *plan.CheckTable:
		return b.buildCheckTable(v)
	case *plan.DDL:
		return b.buildDDL(v)
	case *plan.Deallocate:
		return b.buildDeallocate(v)
	case *plan.NewDelete:
		return b.buildNewDelete(v)
	case *plan.Distinct:
		return b.buildDistinct(v)
	case *plan.Execute:
		return b.buildExecute(v)
	case *plan.Explain:
		return b.buildExplain(v)
	case *plan.Filter:
		src := b.build(v.GetChildByIndex(0))
		return b.buildFilter(src, v.Conditions)
	case *plan.Insert:
		return b.buildInsert(v)
	case *plan.Limit:
		return b.buildLimit(v)
	case *plan.Prepare:
		return b.buildPrepare(v)
	case *plan.SelectLock:
		return b.buildSelectLock(v)
	case *plan.ShowDDL:
		return b.buildShowDDL(v)
	case *plan.Show:
		return b.buildShow(v)
	case *plan.Simple:
		return b.buildSimple(v)
	case *plan.NewSort:
		return b.buildNewSort(v)
	case *plan.NewUnion:
		return b.buildNewUnion(v)
	case *plan.NewUpdate:
		return b.buildNewUpdate(v)
	case *plan.PhysicalHashJoin:
		return b.buildJoin(v)
	case *plan.PhysicalHashSemiJoin:
		return b.buildSemiJoin(v)
	case *plan.Selection:
		return b.buildSelection(v)
	case *plan.PhysicalHashAggregation:
		return b.buildHashAgg(v)
	case *plan.PhysicalStreamedAggregation:
		return b.buildAggregation(v)
	case *plan.Projection:
		return b.buildProjection(v)
	case *plan.PhysicalTableScan:
		return b.buildNewTableScan(v, nil)
	case *plan.PhysicalIndexScan:
		return b.buildNewIndexScan(v, nil)
	case *plan.NewTableDual:
		return b.buildNewTableDual(v)
	case *plan.PhysicalApply:
		return b.buildApply(v)
	case *plan.Exists:
		return b.buildExists(v)
	case *plan.MaxOneRow:
		return b.buildMaxOneRow(v)
	case *plan.Trim:
		return b.buildTrim(v)
	case *plan.PhysicalDummyScan:
		return b.buildDummyScan(v)
	default:
		b.err = ErrUnknownPlan.Gen("Unknown Plan %T", p)
		return nil
	}
}

func (b *executorBuilder) buildFilter(src Executor, conditions []ast.ExprNode) Executor {
	if len(conditions) == 0 {
		return src
	}
	return &FilterExec{
		Src:       src,
		Condition: b.joinConditions(conditions),
		ctx:       b.ctx,
	}
}

func (b *executorBuilder) buildShowDDL(v *plan.ShowDDL) Executor {
	return &ShowDDLExec{
		fields: v.Fields(),
		ctx:    b.ctx,
	}
}

func (b *executorBuilder) buildCheckTable(v *plan.CheckTable) Executor {
	return &CheckTableExec{
		tables: v.Tables,
		ctx:    b.ctx,
	}
}

func (b *executorBuilder) buildDeallocate(v *plan.Deallocate) Executor {
	return &DeallocateExec{
		ctx:  b.ctx,
		Name: v.Name,
	}
}

func (b *executorBuilder) joinConditions(conditions []ast.ExprNode) ast.ExprNode {
	if len(conditions) == 0 {
		return nil
	}
	if len(conditions) == 1 {
		return conditions[0]
	}
	condition := &ast.BinaryOperationExpr{
		Op: opcode.AndAnd,
		L:  conditions[0],
		R:  b.joinConditions(conditions[1:]),
	}
	ast.MergeChildrenFlags(condition, condition.L, condition.R)
	return condition
}

func (b *executorBuilder) buildSelectLock(v *plan.SelectLock) Executor {
	src := b.build(v.GetChildByIndex(0))
	ac, err := autocommit.ShouldAutocommit(b.ctx)
	if err != nil {
		b.err = errors.Trace(err)
		return src
	}
	if ac {
		// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
		// is disabled (either by beginning transaction with START TRANSACTION or by setting
		// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
		// See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
		return src
	}
	e := &SelectLockExec{
		Src:    src,
		Lock:   v.Lock,
		ctx:    b.ctx,
		schema: v.GetSchema(),
	}
	return e
}

func (b *executorBuilder) buildLimit(v *plan.Limit) Executor {
	src := b.build(v.GetChildByIndex(0))
	if x, ok := src.(NewXExecutor); ok {
		if x.AddLimit(v) && v.Offset == 0 {
			return src
		}
	}
	e := &LimitExec{
		Src:    src,
		Offset: v.Offset,
		Count:  v.Count,
		schema: v.GetSchema(),
	}
	return e
}

func (b *executorBuilder) buildDistinct(v *plan.Distinct) Executor {
	return &DistinctExec{Src: b.build(v.GetChildByIndex(0)), schema: v.GetSchema()}
}

func (b *executorBuilder) buildPrepare(v *plan.Prepare) Executor {
	return &PrepareExec{
		Ctx:     b.ctx,
		IS:      b.is,
		Name:    v.Name,
		SQLText: v.SQLText,
	}
}

func (b *executorBuilder) buildExecute(v *plan.Execute) Executor {
	return &ExecuteExec{
		Ctx:       b.ctx,
		IS:        b.is,
		Name:      v.Name,
		UsingVars: v.UsingVars,
		ID:        v.ID,
	}
}

func (b *executorBuilder) buildShow(v *plan.Show) Executor {
	e := &ShowExec{
		Tp:          v.Tp,
		DBName:      model.NewCIStr(v.DBName),
		Table:       v.Table,
		Column:      v.Column,
		User:        v.User,
		Flag:        v.Flag,
		Full:        v.Full,
		GlobalScope: v.GlobalScope,
		ctx:         b.ctx,
		is:          b.is,
		fields:      v.Fields(),
	}
	if e.Tp == ast.ShowGrants && len(e.User) == 0 {
		e.User = variable.GetSessionVars(e.ctx).User
	}
	return e
}

func (b *executorBuilder) buildSimple(v *plan.Simple) Executor {
	switch s := v.Statement.(type) {
	case *ast.GrantStmt:
		return b.buildGrant(s)
	}
	return &SimpleExec{Statement: v.Statement, ctx: b.ctx}
}

func (b *executorBuilder) buildInsert(v *plan.Insert) Executor {
	ivs := &InsertValues{
		ctx:     b.ctx,
		Columns: v.Columns,
		Lists:   v.Lists,
		Setlist: v.Setlist,
	}
	if v.SelectPlan != nil {
		ivs.SelectExec = b.build(v.SelectPlan)
	}
	// Get Table
	ts, ok := v.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		b.err = errors.New("Can not get table")
		return nil
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		b.err = errors.New("Can not get table")
		return nil
	}
	tableInfo := tn.TableInfo
	tbl, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get table %d", tableInfo.ID)
		return nil
	}
	ivs.Table = tbl
	if v.IsReplace {
		return b.buildReplace(ivs)
	}
	insert := &InsertExec{
		InsertValues: ivs,
		OnDuplicate:  v.OnDuplicate,
		Priority:     v.Priority,
		Ignore:       v.Ignore,
	}
	// fields is used to evaluate values expr.
	insert.fields = ts.GetResultFields()
	return insert
}

func (b *executorBuilder) buildReplace(vals *InsertValues) Executor {
	return &ReplaceExec{
		InsertValues: vals,
	}
}

func (b *executorBuilder) buildGrant(grant *ast.GrantStmt) Executor {
	return &GrantExec{
		ctx:        b.ctx,
		Privs:      grant.Privs,
		ObjectType: grant.ObjectType,
		Level:      grant.Level,
		Users:      grant.Users,
	}
}

func (b *executorBuilder) buildDDL(v *plan.DDL) Executor {
	return &DDLExec{Statement: v.Statement, ctx: b.ctx, is: b.is}
}

func (b *executorBuilder) buildExplain(v *plan.Explain) Executor {
	return &ExplainExec{
		StmtPlan: v.StmtPlan,
		fields:   v.Fields(),
	}
}

func (b *executorBuilder) buildNewUnionScanExec(src Executor, condition expression.Expression) *UnionScanExec {
	us := &UnionScanExec{ctx: b.ctx, Src: src}
	switch x := src.(type) {
	case *NewXSelectTableExec:
		us.desc = x.desc
		us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
		us.newCondition = condition
		us.newBuildAndSortAddedRows(x.table, x.asName)
	case *NewXSelectIndexExec:
		us.desc = x.indexPlan.Desc
		for _, ic := range x.indexPlan.Index.Columns {
			for i, col := range x.indexPlan.GetSchema() {
				if col.ColName.L == ic.Name.L {
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
		us.newCondition = condition
		us.newBuildAndSortAddedRows(x.table, x.asName)
	default:
		b.err = ErrUnknownPlan.Gen("Unknown Plan %T", src)
	}
	return us
}
