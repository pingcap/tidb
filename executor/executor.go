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
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/inspectkv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/forupdate"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/distinct"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ Executor = &CheckTableExec{}
	_ Executor = &LimitExec{}
	_ Executor = &SelectLockExec{}
	_ Executor = &ShowDDLExec{}
	_ Executor = &TableDualExec{}
)

// Error instances.
var (
	ErrUnknownPlan     = terror.ClassExecutor.New(CodeUnknownPlan, "Unknown plan")
	ErrPrepareMulti    = terror.ClassExecutor.New(CodePrepareMulti, "Can not prepare multiple statements")
	ErrStmtNotFound    = terror.ClassExecutor.New(CodeStmtNotFound, "Prepared statement not found")
	ErrSchemaChanged   = terror.ClassExecutor.New(CodeSchemaChanged, "Schema has changed")
	ErrWrongParamCount = terror.ClassExecutor.New(CodeWrongParamCount, "Wrong parameter count")
	ErrRowKeyCount     = terror.ClassExecutor.New(CodeRowKeyCount, "Wrong row key entry count")
)

// Error codes.
const (
	CodeUnknownPlan     terror.ErrCode = 1
	CodePrepareMulti    terror.ErrCode = 2
	CodeStmtNotFound    terror.ErrCode = 3
	CodeSchemaChanged   terror.ErrCode = 4
	CodeWrongParamCount terror.ErrCode = 5
	CodeRowKeyCount     terror.ErrCode = 6
)

// Row represents a record row.
type Row struct {
	// Data is the output record data for current Plan.
	Data []types.Datum

	RowKeys []*RowKeyEntry
}

// RowKeyEntry is designed for Update/Delete statement in multi-table mode,
// we should know which table this row comes from.
type RowKeyEntry struct {
	// The table which this row come from.
	Tbl table.Table
	// Row key.
	Handle int64
	// Table alias name.
	TableAsName *model.CIStr
}

// Executor executes a query.
type Executor interface {
	Fields() []*ast.ResultField
	Next() (*Row, error)
	Close() error
	Schema() expression.Schema
}

// ShowDDLExec represents a show DDL executor.
type ShowDDLExec struct {
	fields []*ast.ResultField
	ctx    context.Context
	done   bool
}

// Schema implements Executor Schema interface.
func (e *ShowDDLExec) Schema() expression.Schema {
	return nil
}

// Fields implements Executor Fields interface.
func (e *ShowDDLExec) Fields() []*ast.ResultField {
	return e.fields
}

// Next implements Executor Next interface.
func (e *ShowDDLExec) Next() (*Row, error) {
	if e.done {
		return nil, nil
	}

	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ddlInfo, err := inspectkv.GetDDLInfo(txn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bgInfo, err := inspectkv.GetBgDDLInfo(txn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var ddlOwner, ddlJob string
	if ddlInfo.Owner != nil {
		ddlOwner = ddlInfo.Owner.String()
	}
	if ddlInfo.Job != nil {
		ddlJob = ddlInfo.Job.String()
	}

	var bgOwner, bgJob string
	if bgInfo.Owner != nil {
		bgOwner = bgInfo.Owner.String()
	}
	if bgInfo.Job != nil {
		bgJob = bgInfo.Job.String()
	}

	row := &Row{}
	row.Data = types.MakeDatums(
		ddlInfo.SchemaVer,
		ddlOwner,
		ddlJob,
		bgInfo.SchemaVer,
		bgOwner,
		bgJob,
	)
	for i, f := range e.fields {
		f.Expr.SetValue(row.Data[i].GetValue())
	}
	e.done = true

	return row, nil
}

// Close implements Executor Close interface.
func (e *ShowDDLExec) Close() error {
	return nil
}

// CheckTableExec represents a check table executor.
type CheckTableExec struct {
	tables []*ast.TableName
	ctx    context.Context
	done   bool
}

// Schema implements Executor Schema interface.
func (e *CheckTableExec) Schema() expression.Schema {
	return nil
}

// Fields implements Executor Fields interface.
func (e *CheckTableExec) Fields() []*ast.ResultField {
	return nil
}

// Next implements Executor Next interface.
func (e *CheckTableExec) Next() (*Row, error) {
	if e.done {
		return nil, nil
	}

	dbName := model.NewCIStr(db.GetCurrentSchema(e.ctx))
	is := sessionctx.GetDomain(e.ctx).InfoSchema()

	for _, t := range e.tables {
		tb, err := is.TableByName(dbName, t.Name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, idx := range tb.Indices() {
			txn, err := e.ctx.GetTxn(false)
			if err != nil {
				return nil, errors.Trace(err)
			}
			err = inspectkv.CompareIndexData(txn, tb, idx)
			if err != nil {
				return nil, errors.Errorf("%v err:%v", t.Name, err)
			}
		}
	}
	e.done = true

	return nil, nil
}

// Close implements plan.Plan Close interface.
func (e *CheckTableExec) Close() error {
	return nil
}

// TableDualExec represents a dual table executor.
type TableDualExec struct {
	fields   []*ast.ResultField
	executed bool
}

// Schema implements Executor Schema interface.
func (e *TableDualExec) Schema() expression.Schema {
	return nil
}

// Fields implements Executor Fields interface.
func (e *TableDualExec) Fields() []*ast.ResultField {
	return e.fields
}

// Next implements Executor Next interface.
func (e *TableDualExec) Next() (*Row, error) {
	if e.executed {
		return nil, nil
	}
	e.executed = true
	return &Row{}, nil
}

// Close implements plan.Plan Close interface.
func (e *TableDualExec) Close() error {
	return nil
}

// FilterExec represents a filter executor.
type FilterExec struct {
	Src       Executor
	Condition ast.ExprNode
	ctx       context.Context
}

// Schema implements Executor Schema interface.
func (e *FilterExec) Schema() expression.Schema {
	return e.Src.Schema()
}

// Fields implements Executor Fields interface.
func (e *FilterExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Next implements Executor Next interface.
func (e *FilterExec) Next() (*Row, error) {
	for {
		srcRow, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return nil, nil
		}
		match, err := evaluator.EvalBool(e.ctx, e.Condition)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if match {
			return srcRow, nil
		}
	}
}

// Close implements Executor Close interface.
func (e *FilterExec) Close() error {
	return e.Src.Close()
}

// SelectLockExec represents a select lock executor.
type SelectLockExec struct {
	Src    Executor
	Lock   ast.SelectLockType
	ctx    context.Context
	schema expression.Schema
}

// Schema implements Executor Schema interface.
func (e *SelectLockExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *SelectLockExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Next implements Executor Next interface.
func (e *SelectLockExec) Next() (*Row, error) {
	row, err := e.Src.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if row == nil {
		return nil, nil
	}
	if len(row.RowKeys) != 0 && e.Lock == ast.SelectLockForUpdate {
		forupdate.SetForUpdate(e.ctx)
		for _, k := range row.RowKeys {
			err = k.Tbl.LockRow(e.ctx, k.Handle, true)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	return row, nil
}

// Close implements Executor Close interface.
func (e *SelectLockExec) Close() error {
	return e.Src.Close()
}

// LimitExec represents limit executor
type LimitExec struct {
	Src    Executor
	Offset uint64
	Count  uint64
	Idx    uint64
	schema expression.Schema
}

// Schema implements Executor Schema interface.
func (e *LimitExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *LimitExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Next implements Executor Next interface.
func (e *LimitExec) Next() (*Row, error) {
	for e.Idx < e.Offset {
		srcRow, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return nil, nil
		}
		e.Idx++
	}
	if e.Idx >= e.Count+e.Offset {
		return nil, nil
	}
	srcRow, err := e.Src.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if srcRow == nil {
		return nil, nil
	}
	e.Idx++
	return srcRow, nil
}

// Close implements Executor Close interface.
func (e *LimitExec) Close() error {
	e.Idx = 0
	return e.Src.Close()
}

// orderByRow binds a row to its order values, so it can be sorted.
type orderByRow struct {
	key []types.Datum
	row *Row
}

// SortBufferSize represents the total extra row count that sort can use.
var SortBufferSize = 500

// For select stmt with aggregate function but without groupby clasue,
// We consider there is a single group with key singleGroup.
var singleGroup = []byte("SingleGroup")

// DistinctExec represents Distinct executor.
type DistinctExec struct {
	Src     Executor
	checker *distinct.Checker
	schema  expression.Schema
}

// Schema implements Executor Schema interface.
func (e *DistinctExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *DistinctExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Next implements Executor Next interface.
func (e *DistinctExec) Next() (*Row, error) {
	if e.checker == nil {
		e.checker = distinct.CreateDistinctChecker()
	}
	for {
		row, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			return nil, nil
		}
		ok, err := e.checker.Check(types.DatumsToInterfaces(row.Data))
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ok {
			continue
		}
		return row, nil
	}
}

// Close implements Executor Close interface.
func (e *DistinctExec) Close() error {
	return e.Src.Close()
}

// ReverseExec produces reverse ordered result, it is used to wrap executors that do not support reverse scan.
type ReverseExec struct {
	Src    Executor
	rows   []*Row
	cursor int
	done   bool
}

// Schema implements Executor Schema interface.
func (e *ReverseExec) Schema() expression.Schema {
	return e.Src.Schema()
}

// Fields implements Executor Fields interface.
func (e *ReverseExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Next implements Executor Next interface.
func (e *ReverseExec) Next() (*Row, error) {
	if !e.done {
		for {
			row, err := e.Src.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if row == nil {
				break
			}
			e.rows = append(e.rows, row)
		}
		e.cursor = len(e.rows) - 1
		e.done = true
	}
	if e.cursor < 0 {
		return nil, nil
	}
	row := e.rows[e.cursor]
	e.cursor--
	for i, field := range e.Src.Fields() {
		field.Expr.SetDatum(row.Data[i])
	}
	return row, nil
}

// Close implements Executor Close interface.
func (e *ReverseExec) Close() error {
	return e.Src.Close()
}

func init() {
	plan.EvalSubquery = func(p plan.PhysicalPlan, is infoschema.InfoSchema, ctx context.Context) (d []types.Datum, err error) {
		e := &executorBuilder{is: is, ctx: ctx}
		exec := e.build(p)
		row, err := exec.Next()
		if err != nil {
			return d, errors.Trace(err)
		}
		if row == nil {
			return
		}
		return row.Data, nil
	}
}
