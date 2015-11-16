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
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/optimizer/plan"
	oplan "github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/util/format"
)

// adapter wraps a executor, implements rset.Recordset interface
type recordsetAdapter struct {
	fields   []*field.ResultField
	executor Executor
}

func (a *recordsetAdapter) Do(f func(data []interface{}) (bool, error)) error {
	return nil
}

func (a *recordsetAdapter) Fields() ([]*field.ResultField, error) {
	return a.fields, nil
}

func (a *recordsetAdapter) FirstRow() ([]interface{}, error) {
	return nil, nil
}

func (a *recordsetAdapter) Rows(limit, offset int) ([][]interface{}, error) {
	return nil, nil
}

func (a *recordsetAdapter) Next() (*oplan.Row, error) {
	row, err := a.executor.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if row == nil {
		return nil, nil
	}
	oRow := &oplan.Row{
		Data: row.Data,
	}
	for _, v := range row.RowKeys {
		oldRowKey := &oplan.RowKeyEntry{
			Key: v.Key,
			Tbl: v.Tbl,
		}
		oRow.RowKeys = append(oRow.RowKeys, oldRowKey)
	}
	return oRow, nil
}

func (a *recordsetAdapter) Close() error {
	return a.executor.Close()
}

type statementAdapter struct {
	is   infoschema.InfoSchema
	plan plan.Plan
}

func (a *statementAdapter) Explain(ctx context.Context, w format.Formatter) {
	return
}

func (a *statementAdapter) OriginText() string {
	return ""
}

func (a *statementAdapter) SetText(text string) {
	return
}

func (a *statementAdapter) IsDDL() bool {
	return false
}

func (a *statementAdapter) Exec(ctx context.Context) (rset.Recordset, error) {
	b := newExecutorBuilder(ctx, a.is)
	e := b.build(a.plan)
	var fields []*field.ResultField
	for _, v := range e.Fields() {
		f := &field.ResultField{
			Col:       column.Col{ColumnInfo: *v.Column},
			Name:      v.ColumnAsName.O,
			TableName: v.TableAsName.O,
			DBName:    v.DBName.O,
		}
		if v.Table != nil {
			f.OrgTableName = v.Table.Name.O
		}
		if f.Name == "" {
			f.Name = f.ColumnInfo.Name.O
		}
		fields = append(fields, f)
	}
	return &recordsetAdapter{
		executor: e,
		fields:   fields,
	}, nil
}
