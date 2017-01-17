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
	"github.com/pingcap/tidb/plan"
)

// recordSet wraps an executor, implements ast.RecordSet interface
type recordSet struct {
	fields   []*ast.ResultField
	executor Executor
	schema   expression.Schema
	ctx      context.Context
}

func (a *recordSet) Fields() ([]*ast.ResultField, error) {
	if len(a.fields) == 0 {
		for _, col := range a.schema.Columns {
			rf := &ast.ResultField{
				ColumnAsName: col.ColName,
				TableAsName:  col.TblName,
				DBName:       col.DBName,
				Column: &model.ColumnInfo{
					FieldType: *col.RetType,
					Name:      col.ColName,
				},
			}
			a.fields = append(a.fields, rf)
		}
	}
	return a.fields, nil
}

func (a *recordSet) Next() (*ast.Row, error) {
	row, err := a.executor.Next()
	if err != nil || row == nil {
		return nil, errors.Trace(err)
	}
	return &ast.Row{Data: row.Data}, nil
}

func (a *recordSet) Close() error {
	return a.executor.Close()
}

// statement implements the ast.Statement interface, it builds a plan.Plan to an ast.Statement.
type statement struct {
	// The InfoSchema cannot change during execution, so we hold a reference to it.
	is   infoschema.InfoSchema
	plan plan.Plan
	text string
}

func (a *statement) OriginText() string {
	return a.text
}

func (a *statement) SetText(text string) {
	a.text = text
	return
}

// Exec implements the ast.Statement Exec interface.
// This function builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UPDATE statements, it executes in this function, if the Executor returns
// result, execution is done after this function returns, in the returned ast.RecordSet Next method.
func (a *statement) Exec(ctx context.Context) (ast.RecordSet, error) {
	if _, ok := a.plan.(*plan.Execute); !ok {
		// Do not sync transaction for Execute statement, because the real optimization work is done in
		// "ExecuteExec.Build".
		err := ctx.ActivePendingTxn()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	b := newExecutorBuilder(ctx, a.is)
	e := b.build(a.plan)
	if b.err != nil {
		return nil, errors.Trace(b.err)
	}

	// ExecuteExec is not a real Executor, we only use it to build another Executor from a prepared statement.
	if executorExec, ok := e.(*ExecuteExec); ok {
		err := executorExec.Build()
		if err != nil {
			return nil, errors.Trace(err)
		}
		stmtCount(executorExec.Stmt, executorExec.Plan)
		e = executorExec.StmtExec
	}

	// Fields or Schema are only used for statements that return result set.
	if e.Schema().Len() == 0 {
		// Check if "tidb_snapshot" is set for the write executors.
		// In history read mode, we can not do write operations.
		switch e.(type) {
		case *DeleteExec, *InsertExec, *UpdateExec, *ReplaceExec, *LoadData, *DDLExec:
			snapshotTS := ctx.GetSessionVars().SnapshotTS
			if snapshotTS != 0 {
				return nil, errors.New("can not execute write statement when 'tidb_snapshot' is set")
			}
		}

		defer e.Close()
		for {
			row, err := e.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			// Even though there isn't any result set, the row is still used to indicate if there is
			// more work to do.
			// For example, the UPDATE statement updates a single row on a Next call, we keep calling Next until
			// There is no more rows to update.
			if row == nil {
				return nil, nil
			}
		}
	}
	return &recordSet{
		executor: e,
		schema:   e.Schema(),
		ctx:      ctx,
	}, nil
}
