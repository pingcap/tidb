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
	"math"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/autocommit"
	"github.com/pingcap/tidb/util/types"
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
	case *plan.Aggregate:
		return b.buildAggregate(v)
	case *plan.CheckTable:
		return b.buildCheckTable(v)
	case *plan.Deallocate:
		return b.buildDeallocate(v)
	case *plan.Execute:
		return b.buildExecute(v)
	case *plan.Having:
		return b.buildHaving(v)
	case *plan.IndexScan:
		return b.buildIndexScan(v)
	case *plan.Limit:
		return b.buildLimit(v)
	case *plan.Prepare:
		return b.buildPrepare(v)
	case *plan.SelectFields:
		return b.buildSelectFields(v)
	case *plan.SelectLock:
		return b.buildSelectLock(v)
	case *plan.ShowDDL:
		return b.buildShowDDL(v)
	case *plan.Sort:
		return b.buildSort(v)
	case *plan.TableScan:
		return b.buildTableScan(v)
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

func (b *executorBuilder) buildTableScan(v *plan.TableScan) Executor {
	table, _ := b.is.TableByID(v.Table.ID)
	e := &TableScanExec{
		t:          table,
		fields:     v.Fields(),
		ctx:        b.ctx,
		ranges:     v.Ranges,
		seekHandle: math.MinInt64,
	}
	return b.buildFilter(e, v.FilterConditions)
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

func (b *executorBuilder) buildIndexScan(v *plan.IndexScan) Executor {
	tbl, _ := b.is.TableByID(v.Table.ID)
	var idx *column.IndexedCol
	for _, val := range tbl.Indices() {
		if val.IndexInfo.Name.L == v.Index.Name.L {
			idx = val
			break
		}
	}
	e := &IndexScanExec{
		tbl:        tbl,
		idx:        idx,
		fields:     v.Fields(),
		ctx:        b.ctx,
		Desc:       v.Desc,
		valueTypes: make([]*types.FieldType, len(idx.Columns)),
	}

	for i, ic := range idx.Columns {
		col := tbl.Cols()[ic.Offset]
		e.valueTypes[i] = &col.FieldType
	}

	e.Ranges = make([]*IndexRangeExec, len(v.Ranges))
	for i, val := range v.Ranges {
		e.Ranges[i] = b.buildIndexRange(e, val)
	}
	return b.buildFilter(e, v.FilterConditions)
}

func (b *executorBuilder) buildIndexRange(scan *IndexScanExec, v *plan.IndexRange) *IndexRangeExec {
	ran := &IndexRangeExec{
		scan:        scan,
		lowVals:     v.LowVal,
		lowExclude:  v.LowExclude,
		highVals:    v.HighVal,
		highExclude: v.HighExclude,
	}
	return ran
}

func (b *executorBuilder) joinConditions(conditions []ast.ExprNode) ast.ExprNode {
	if len(conditions) == 1 {
		return conditions[0]
	}
	condition := &ast.BinaryOperationExpr{
		Op: opcode.AndAnd,
		L:  conditions[0],
		R:  b.joinConditions(conditions[1:]),
	}
	return condition
}

func (b *executorBuilder) buildSelectLock(v *plan.SelectLock) Executor {
	src := b.build(v.Src())
	if autocommit.ShouldAutocommit(b.ctx) {
		// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
		// is disabled (either by beginning transaction with START TRANSACTION or by setting
		// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
		// See: https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
		return src
	}
	e := &SelectLockExec{
		Src:  src,
		Lock: v.Lock,
		ctx:  b.ctx,
	}
	return e
}

func (b *executorBuilder) buildSelectFields(v *plan.SelectFields) Executor {
	src := b.build(v.Src())
	e := &SelectFieldsExec{
		Src:          src,
		ResultFields: v.Fields(),
		ctx:          b.ctx,
	}
	return e
}

func (b *executorBuilder) buildAggregate(v *plan.Aggregate) Executor {
	src := b.build(v.Src())
	e := &AggregateExec{
		Src:          src,
		ResultFields: v.Fields(),
		ctx:          b.ctx,
		AggFuncs:     v.AggFuncs,
		GroupByItems: v.GroupByItems,
	}
	return e
}

func (b *executorBuilder) buildHaving(v *plan.Having) Executor {
	src := b.build(v.Src())
	return b.buildFilter(src, v.Conditions)
}

func (b *executorBuilder) buildSort(v *plan.Sort) Executor {
	src := b.build(v.Src())
	e := &SortExec{
		Src:     src,
		ByItems: v.ByItems,
		ctx:     b.ctx,
	}
	return e
}

func (b *executorBuilder) buildLimit(v *plan.Limit) Executor {
	src := b.build(v.Src())
	e := &LimitExec{
		Src:    src,
		Offset: v.Offset,
		Count:  v.Count,
	}
	return e
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
