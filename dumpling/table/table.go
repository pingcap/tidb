// Copyright 2013 The ql Authors. All rights reserved.
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

package table

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/optimizer/evaluator"
	"github.com/pingcap/tidb/sessionctx/db"
)

// RecordIterFunc is used for low-level record iteration.
type RecordIterFunc func(h int64, rec []interface{}, cols []*column.Col) (more bool, err error)

// Table is used to retrieve and modify rows in table.
type Table interface {
	// IterRecords iterates records in the table and calls fn.
	IterRecords(ctx context.Context, startKey kv.Key, cols []*column.Col, fn RecordIterFunc) error

	// RowWithCols returns a row that contains the given cols.
	RowWithCols(ctx context.Context, h int64, cols []*column.Col) ([]interface{}, error)

	// Row returns a row for all columns.
	Row(ctx context.Context, h int64) ([]interface{}, error)

	// Cols returns the columns of the table which is used in select.
	Cols() []*column.Col

	// Indices returns the indices of the table.
	Indices() []*column.IndexedCol

	// RecordPrefix returns the record key prefix.
	RecordPrefix() kv.Key

	// IndexPrefix returns the index key prefix.
	IndexPrefix() kv.Key

	// FirstKey returns the first key.
	FirstKey() kv.Key

	// RecordKey returns the key in KV storage for the column.
	RecordKey(h int64, col *column.Col) kv.Key

	// Truncate truncates the table.
	Truncate(ctx context.Context) (err error)

	// AddRecord inserts a row into the table.
	AddRecord(ctx context.Context, r []interface{}) (recordID int64, err error)

	// UpdateRecord updates a row in the table.
	UpdateRecord(ctx context.Context, h int64, currData []interface{}, newData []interface{}, touched map[int]bool) error

	// RemoveRecord removes a row in the table.
	RemoveRecord(ctx context.Context, h int64, r []interface{}) error

	// AllocAutoID allocates an auto_increment ID for a new row.
	AllocAutoID() (int64, error)

	// Meta returns TableInfo.
	Meta() *model.TableInfo

	// LockRow locks a row.
	LockRow(ctx context.Context, h int64, forRead bool) error

	// Seek returns the handle greater or equal to h.
	Seek(ctx context.Context, h int64) (handle int64, found bool, err error)
}

// TableFromMeta builds a table.Table from *model.TableInfo.
// Currently, it is assigned to tables.TableFromMeta in tidb package's init function.
var TableFromMeta func(alloc autoid.Allocator, tblInfo *model.TableInfo) (Table, error)

// Ident is the table identifier composed of schema name and table name.
// TODO: Move out
type Ident struct {
	Schema model.CIStr
	Name   model.CIStr
}

// Full returns an Ident which set schema to the current schema if it is empty.
func (i Ident) Full(ctx context.Context) (full Ident) {
	full.Name = i.Name
	full.Schema = i.Schema
	if i.Schema.O != "" {
		full.Schema = i.Schema
	} else {
		full.Schema = model.NewCIStr(db.GetCurrentSchema(ctx))
	}
	return
}

// String implements fmt.Stringer interface
func (i Ident) String() string {
	if i.Schema.O == "" {
		return i.Name.O
	}
	return fmt.Sprintf("%s.%s", i.Schema, i.Name)
}

// GetColDefaultValue gets default value of the column.
func GetColDefaultValue(ctx context.Context, col *model.ColumnInfo) (interface{}, bool, error) {
	// Check no default value flag.
	if mysql.HasNoDefaultValueFlag(col.Flag) && col.Tp != mysql.TypeEnum {
		return nil, false, errors.Errorf("Field '%s' doesn't have a default value", col.Name)
	}

	// Check and get timestamp/datetime default value.
	if col.Tp == mysql.TypeTimestamp || col.Tp == mysql.TypeDatetime {
		if col.DefaultValue == nil {
			return nil, true, nil
		}

		value, err := evaluator.GetTimeValue(ctx, col.DefaultValue, col.Tp, col.Decimal)
		if err != nil {
			return nil, true, errors.Errorf("Field '%s' get default value fail - %s", col.Name, errors.Trace(err))
		}
		return value, true, nil
	} else if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		if col.DefaultValue == nil && mysql.HasNotNullFlag(col.Flag) {
			return col.FieldType.Elems[0], true, nil
		}
	}

	return col.DefaultValue, true, nil
}
