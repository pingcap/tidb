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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/parser/opcode"
)

// executorBuilder builds an Executor from a Plan.
// the InfoSchema must be the same one used in InfoBinder.
type executorBuilder struct {
	ctx context.Context
	is  infoschema.InfoSchema
}

func newExecutorBuilder(ctx context.Context, is infoschema.InfoSchema) *executorBuilder {
	return &executorBuilder{
		ctx: ctx,
		is:  is,
	}
}

func (b *executorBuilder) build(p plan.Plan) Executor {
	switch v := p.(type) {
	case *plan.TableScan:
		return b.buildTableScan(v)
	case *plan.IndexScan:
		return b.buildIndexScan(v)
	case *plan.Filter:
		return b.buildFilter(v)
	case *plan.SelectFields:
		return b.buildSelectFields(v)
	case *plan.Sort:
		return b.buildSort(v)
	case *plan.Limit:
		return b.buildLimit(v)
	}
	return nil
}

func (b *executorBuilder) buildTableScan(v *plan.TableScan) Executor {
	table, _ := b.is.TableByID(v.Table.ID)
	return &TableScanExec{
		t:      table,
		fields: v.Fields(),
		ctx:    b.ctx,
	}
}

func (b *executorBuilder) buildIndexScan(v *plan.IndexScan) Executor {
	table, _ := b.is.TableByID(v.Table.ID)
	e := &IndexScanExec{
		Table:  table,
		fields: v.Fields(),
	}
	return e
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

func (b *executorBuilder) buildFilter(v *plan.Filter) Executor {
	src := b.build(v.Src)
	e := &FilterExec{
		Src:       src,
		Condition: b.joinConditions(v.Conditions),
	}
	return e
}

func (b *executorBuilder) buildSelectFields(v *plan.SelectFields) Executor {
	src := b.build(v.Src)
	e := &SelectFieldsExec{
		Src:          src,
		ResultFields: v.Fields(),
	}
	return e
}

func (b *executorBuilder) buildSort(v *plan.Sort) Executor {
	src := b.build(v.Src)
	e := &SortExec{
		Src:     src,
		ByItems: v.ByItems,
	}
	return e
}

func (b *executorBuilder) buildLimit(v *plan.Limit) Executor {
	src := b.build(v.Src)
	e := &LimitExec{
		Src:    src,
		Offset: v.Offset,
		Count:  v.Count,
	}
	return e
}
