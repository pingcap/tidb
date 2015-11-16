// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package plans

import (
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ plan.Plan = (*TableDefaultPlan)(nil)
	_ plan.Plan = (*TableNilPlan)(nil)
)

// TableNilPlan iterates rows but does nothing, e.g. SELECT 1 FROM t;
type TableNilPlan struct {
	T    table.Table
	iter kv.Iterator
}

// Explain implements the plan.Plan interface.
func (r *TableNilPlan) Explain(w format.Formatter) {
	w.Format("┌Iterate all rows of table %q\n└Output field names %v\n", r.T.TableName(), field.RFQNames(r.GetFields()))
}

// GetFields implements the plan.Plan interface.
func (r *TableNilPlan) GetFields() []*field.ResultField {
	return []*field.ResultField{}
}

// Filter implements the plan.Plan Filter interface.
func (r *TableNilPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return r, false, nil
}

// Next implements plan.Plan Next interface.
func (r *TableNilPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if r.iter == nil {
		var txn kv.Transaction
		txn, err = ctx.GetTxn(false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		r.iter, err = txn.Seek([]byte(r.T.FirstKey()))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if !r.iter.Valid() || !strings.HasPrefix(r.iter.Key(), r.T.KeyPrefix()) {
		return
	}
	handle, err := util.DecodeHandleFromRowKey(r.iter.Key())
	if err != nil {
		return nil, errors.Trace(err)
	}
	rk := r.T.RecordKey(handle, nil)
	// Even though the data is nil, we should return not nil row,
	// or the iteration will stop.
	row = &plan.Row{}
	err = kv.NextUntil(r.iter, util.RowKeyPrefixFilter(rk))
	return
}

// Close implements plan.Plan Close interface.
func (r *TableNilPlan) Close() error {
	if r.iter != nil {
		r.iter.Close()
	}
	r.iter = nil
	return nil
}

// TableDefaultPlan iterates rows from a table, in general case
// it performs a full table scan, but using Filter function,
// it will return a new IndexPlan if an index is found in Filter function.
type TableDefaultPlan struct {
	T      table.Table
	Fields []*field.ResultField
	iter   kv.Iterator
}

// Explain implements the plan.Plan Explain interface.
func (r *TableDefaultPlan) Explain(w format.Formatter) {
	w.Format("┌Iterate all rows of table %q\n└Output field names %v\n", r.T.TableName(), field.RFQNames(r.Fields))
}

func (r *TableDefaultPlan) filterBinOp(ctx context.Context, x *expression.BinaryOperation) (plan.Plan, bool, error) {
	ok, name, rval, err := x.IsIdentCompareVal()
	if err != nil {
		return r, false, err
	}
	if !ok {
		return r, false, nil
	}
	if rval == nil {
		// if nil, any <, <=, >, >=, =, != operator will do nothing
		// any value compared null returns null
		// TODO: if we support <=> later, we must handle null
		return &NullPlan{r.GetFields()}, true, nil
	}

	_, tn, cn := field.SplitQualifiedName(name)
	t := r.T
	if tn != "" && tn != t.TableName().L {
		return r, false, nil
	}
	c := column.FindCol(t.Cols(), cn)
	if c == nil {
		return nil, false, errors.Errorf("No such column: %s", cn)
	}

	ix := t.FindIndexByColName(cn)
	if ix == nil { // Column cn has no index.
		return r, false, nil
	}

	var seekVal interface{}
	if seekVal, err = types.Convert(rval, &c.FieldType); err != nil {
		return nil, false, err
	}
	return &indexPlan{
		src:     t,
		col:     c,
		unique:  ix.Unique,
		idxName: ix.Name.O,
		idx:     ix.X,
		spans:   toSpans(x.Op, rval, seekVal),
	}, true, nil
}

func (r *TableDefaultPlan) filterIdent(ctx context.Context, x *expression.Ident, trueValue bool) (plan.Plan, bool, error) { //TODO !ident
	t := r.T
	for _, v := range t.Cols() {
		if x.L != v.Name.L {
			continue
		}

		xi := v.Offset
		if xi >= len(t.Indices()) {
			return r, false, nil
		}

		ix := t.Indices()[xi]
		if ix == nil { // Column cn has no index.
			return r, false, nil
		}
		var spans []*indexSpan
		if trueValue {
			spans = toSpans(opcode.NE, 0, 0)
		} else {
			spans = toSpans(opcode.EQ, 0, 0)
		}
		return &indexPlan{
			src:     t,
			col:     v,
			unique:  ix.Unique,
			idxName: ix.Name.L,
			idx:     ix.X,
			spans:   spans,
		}, true, nil
	}
	return r, false, nil
}

func (r *TableDefaultPlan) filterIsNull(ctx context.Context, x *expression.IsNull) (plan.Plan, bool, error) {
	if _, ok := x.Expr.(*expression.Ident); !ok {
		// if expression is not Ident expression, we cannot use index
		// e.g, "(x > null) is not null", (x > null) is a binary expression, we must evaluate it first
		return r, false, nil
	}

	cns := expression.MentionedColumns(x.Expr)
	if len(cns) == 0 {
		return r, false, nil
	}

	cn := cns[0]
	t := r.T
	ix := t.FindIndexByColName(cn)
	if ix == nil { // Column cn has no index.
		return r, false, nil
	}
	col := column.FindCol(t.Cols(), cn)
	var spans []*indexSpan
	if x.Not {
		spans = toSpans(opcode.GE, minNotNullVal, nil)
	} else {
		spans = toSpans(opcode.EQ, nil, nil)
	}
	return &indexPlan{
		src:     t,
		col:     col,
		unique:  ix.Unique,
		idxName: ix.Name.L,
		idx:     ix.X,
		spans:   spans,
	}, true, nil
}

// FilterForUpdateAndDelete is for updating and deleting (without checking return
// columns), in order to check whether if we can use IndexPlan or not.
func (r *TableDefaultPlan) FilterForUpdateAndDelete(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	// disable column check
	return r.filter(ctx, expr, false)
}

// Filter implements plan.Plan Filter interface.
func (r *TableDefaultPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return r.filter(ctx, expr, true)
}

func (r *TableDefaultPlan) filter(ctx context.Context, expr expression.Expression, checkColumns bool) (plan.Plan, bool, error) {
	if checkColumns {
		colNames := expression.MentionedColumns(expr)
		// make sure all mentioned column names are in Fields
		// if not, e.g. the expr has two table like t1.c1 = t2.c2, we can't use filter
		if !field.ContainAllFieldNames(colNames, r.Fields) {
			return r, false, nil
		}
	}

	switch x := expr.(type) {
	case *expression.BinaryOperation:
		return r.filterBinOp(ctx, x)
	case *expression.Ident:
		return r.filterIdent(ctx, x, true)
	case *expression.IsNull:
		return r.filterIsNull(ctx, x)
	case *expression.UnaryOperation:
		if x.Op != '!' {
			break
		}
		if operand, ok := x.V.(*expression.Ident); ok {
			return r.filterIdent(ctx, operand, false)
		}
	}
	return r, false, nil
}

// GetFields implements the plan.Plan GetFields interface.
func (r *TableDefaultPlan) GetFields() []*field.ResultField {
	return r.Fields
}

// Next implements plan.Plan Next interface.
func (r *TableDefaultPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if r.iter == nil {
		var txn kv.Transaction
		txn, err = ctx.GetTxn(false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		r.iter, err = txn.Seek([]byte(r.T.FirstKey()))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if !r.iter.Valid() || !strings.HasPrefix(r.iter.Key(), r.T.KeyPrefix()) {
		return
	}
	// TODO: check if lock valid
	// the record layout in storage (key -> value):
	// r1 -> lock-version
	// r1_col1 -> r1 col1 value
	// r1_col2 -> r1 col2 value
	// r2 -> lock-version
	// r2_col1 -> r2 col1 value
	// r2_col2 -> r2 col2 value
	// ...
	rowKey := r.iter.Key()
	handle, err := util.DecodeHandleFromRowKey(rowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	txn, err := ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// It is very likely that we will fetch rows after current row later, enable the RangePrefetchOnCacheMiss
	// option may help reducing RPC calls.
	// TODO: choose a wiser option value.
	txn.SetOption(kv.RangePrefetchOnCacheMiss, nil)
	defer txn.DelOption(kv.RangePrefetchOnCacheMiss)

	// TODO: we could just fetch mentioned columns' values
	row = &plan.Row{}
	row.Data, err = r.T.Row(ctx, handle)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Put rowKey to the tail of record row
	rke := &plan.RowKeyEntry{
		Tbl: r.T,
		Key: rowKey,
	}
	row.RowKeys = append(row.RowKeys, rke)

	rk := r.T.RecordKey(handle, nil)
	err = kv.NextUntil(r.iter, util.RowKeyPrefixFilter(rk))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return
}

// Close implements plan.Plan Close interface.
func (r *TableDefaultPlan) Close() error {
	if r.iter != nil {
		r.iter.Close()
		r.iter = nil
	}
	return nil
}
