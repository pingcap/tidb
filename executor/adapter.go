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
	"github.com/pingcap/tidb/plan"
)

// recordSet wraps an executor, implements ast.RecordSet interface
type recordSet struct {
	fields   []*ast.ResultField
	executor Executor
	schema   expression.Schema
}

func (a *recordSet) Fields() ([]*ast.ResultField, error) {
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

type statement struct {
	is    infoschema.InfoSchema
	plan  plan.Plan
	text  string
	isDDL bool
}

func (a *statement) OriginText() string {
	return a.text
}

func (a *statement) SetText(text string) {
	a.text = text
	return
}

func (a *statement) IsDDL() bool {
	return a.isDDL
}

func (a *statement) Exec(ctx context.Context) (ast.RecordSet, error) {
	b := newExecutorBuilder(ctx, a.is)
	e := b.build(a.plan)
	if b.err != nil {
		return nil, errors.Trace(b.err)
	}

	if executorExec, ok := e.(*ExecuteExec); ok {
		err := executorExec.Build()
		if err != nil {
			return nil, errors.Trace(err)
		}
		e = executorExec.StmtExec
	}

	if len(e.Fields()) == 0 && len(e.Schema()) == 0 {
		// No result fields means no Recordset.
		defer e.Close()
		for {
			row, err := e.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if row == nil {
				return nil, nil
			}
		}
	}

	fs := e.Fields()
	for _, f := range fs {
		if len(f.ColumnAsName.O) == 0 {
			f.ColumnAsName = f.Column.Name
		}
	}
	return &recordSet{
		executor: e,
		fields:   fs,
	}, nil
}
