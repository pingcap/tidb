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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
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

// Do implements the plan.Plan interface, iterates rows but does nothing.
func (r *TableNilPlan) Do(ctx context.Context, f plan.RowIterFunc) error {
	h := r.T.FirstKey()
	prefix := r.T.KeyPrefix()
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return err
	}
	it, err := txn.Seek([]byte(h), nil)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Valid() && strings.HasPrefix(it.Key(), prefix) {
		var id int64
		id, err = util.DecodeHandleFromRowKey(it.Key())
		if err != nil {
			return err
		}

		// do nothing
		var m bool
		if m, err = f(id, nil); !m || err != nil {
			return err
		}

		rk := r.T.RecordKey(id, nil)
		if it, err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk)); err != nil {
			return err
		}
	}
	return nil
}

// Next implements plan.Plan Next interface.
func (r *TableNilPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if r.iter == nil {
		var txn kv.Transaction
		txn, err = ctx.GetTxn(false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		r.iter, err = txn.Seek([]byte(r.T.FirstKey()), nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if !r.iter.Valid() || !strings.HasPrefix(r.iter.Key(), r.T.KeyPrefix()) {
		return
	}
	id, err := util.DecodeHandleFromRowKey(r.iter.Key())
	if err != nil {
		return nil, errors.Trace(err)
	}
	rk := r.T.RecordKey(id, nil)
	row = &plan.Row{}
	r.iter, err = kv.NextUntil(r.iter, util.RowKeyPrefixFilter(rk))
	return
}

// Close implements plan.Plan Close interface.
func (r *TableNilPlan) Close() error {
	r.iter.Close()
	return nil
}

// UseNext implements NextPlan interface
func (r *TableNilPlan) UseNext() bool {
	log.Warn("use next")
	return true
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

func (r *TableDefaultPlan) filterBinOp(ctx context.Context, x *expressions.BinaryOperation) (plan.Plan, bool, error) {
	ok, cn, rval, err := x.IsIdentRelOpVal()
	if err != nil {
		return r, false, err
	}
	if !ok {
		return r, false, nil
	}

	t := r.T
	c := column.FindCol(t.Cols(), cn)
	if c == nil {
		return nil, false, errors.Errorf("No such column: %s", cn)
	}

	ix := t.FindIndexByColName(cn)
	if ix == nil { // Column cn has no index.
		return r, false, nil
	}

	if rval, err = types.Convert(rval, &c.FieldType); err != nil {
		return nil, false, err
	}

	if rval == nil {
		// if nil, any <, <=, >, >=, =, != operator will do nothing
		// any value compared null returns null
		// TODO: if we support <=> later, we must handle null
		return &NullPlan{r.GetFields()}, true, nil
	}
	return &indexPlan{
		src:     t,
		colName: cn,
		idxName: ix.Name.O,
		idx:     ix.X,
		spans:   toSpans(x.Op, rval),
	}, true, nil
}

func (r *TableDefaultPlan) filterIdent(ctx context.Context, x *expressions.Ident, trueValue bool) (plan.Plan, bool, error) { //TODO !ident
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
			spans = toSpans(opcode.NE, 0)
		} else {
			spans = toSpans(opcode.EQ, 0)
		}
		return &indexPlan{
			src:     t,
			colName: x.L,
			idxName: ix.Name.L,
			idx:     ix.X,
			spans:   spans,
		}, true, nil
	}
	return r, false, nil
}

func (r *TableDefaultPlan) filterIsNull(ctx context.Context, x *expressions.IsNull) (plan.Plan, bool, error) {
	if _, ok := x.Expr.(*expressions.Ident); !ok {
		// if expression is not Ident expression, we cannot use index
		// e.g, "(x > null) is not null", (x > null) is a binary expression, we must evaluate it first
		return r, false, nil
	}

	cns := expressions.MentionedColumns(x.Expr)
	if len(cns) == 0 {
		return r, false, nil
	}

	cn := cns[0]
	t := r.T
	ix := t.FindIndexByColName(cn)
	if ix == nil { // Column cn has no index.
		return r, false, nil
	}
	var spans []*indexSpan
	if x.Not {
		spans = toSpans(opcode.GE, minNotNullVal)
	} else {
		spans = toSpans(opcode.EQ, nil)
	}
	return &indexPlan{
		src:     t,
		colName: cn,
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
		colNames := expressions.MentionedColumns(expr)
		// make sure all mentioned column names are in Fields
		// if not, e.g. the expr has two table like t1.c1 = t2.c2, we can't use filter
		if !field.ContainAllFieldNames(colNames, r.Fields, field.DefaultFieldFlag) {
			return r, false, nil
		}
	}

	switch x := expr.(type) {
	case *expressions.BinaryOperation:
		return r.filterBinOp(ctx, x)
	case *expressions.Ident:
		return r.filterIdent(ctx, x, true)
	case *expressions.IsNull:
		return r.filterIsNull(ctx, x)
	case *expressions.UnaryOperation:
		if x.Op != '!' {
			break
		}
		if operand, ok := x.V.(*expressions.Ident); ok {
			return r.filterIdent(ctx, operand, false)
		}
	}
	return r, false, nil
}

// Do scans over rows' kv pair in the table, and constructs them into row data.
func (r *TableDefaultPlan) Do(ctx context.Context, f plan.RowIterFunc) error {
	t := r.T
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return err
	}
	head := t.FirstKey()
	prefix := t.KeyPrefix()

	it, err := txn.Seek([]byte(head), nil)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Valid() && strings.HasPrefix(it.Key(), prefix) {
		// TODO: check if lock valid
		// the record layout in storage (key -> value):
		// r1 -> lock-version
		// r1_col1 -> r1 col1 value
		// r1_col2 -> r1 col2 value
		// r2 -> lock-version
		// r2_col1 -> r2 col1 value
		// r2_col2 -> r2 col2 value
		// ...
		var err error
		rowKey := it.Key()
		h, err := util.DecodeHandleFromRowKey(rowKey)
		if err != nil {
			return err
		}

		// TODO: we could just fetch mentioned columns' values
		rec, err := t.Row(ctx, h)
		if err != nil {
			return err
		}
		// Put rowKey to the tail of record row
		rks := &RowKeyList{}
		rke := &plan.RowKeyEntry{
			Tbl: t,
			Key: rowKey,
		}
		rks.appendKeys(rke)
		rec = append(rec, rks)
		m, err := f(int64(0), rec)
		if !m || err != nil {
			return err
		}

		rk := t.RecordKey(h, nil)
		it, err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			return err
		}
	}
	return nil
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
		r.iter, err = txn.Seek([]byte(r.T.FirstKey()), nil)
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
	h, err := util.DecodeHandleFromRowKey(rowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// TODO: we could just fetch mentioned columns' values
	row = &plan.Row{}
	row.Data, err = r.T.Row(ctx, h)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Put rowKey to the tail of record row
	rke := &plan.RowKeyEntry{
		Tbl: r.T,
		Key: rowKey,
	}
	row.RowKeys = append(row.RowKeys, rke)

	rk := r.T.RecordKey(h, nil)
	r.iter, err = kv.NextUntil(r.iter, util.RowKeyPrefixFilter(rk))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return
}

// Close implements plan.Plan Close interface.
func (r *TableDefaultPlan) Close() error {
	r.iter.Close()
	return nil
}

// UseNext implements NextPlan interface
func (r *TableDefaultPlan) UseNext() bool {
	log.Warn("use next")
	return true
}
