// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/types"
)

// Row represents a result set row, it may be returned from a table, a join, or a projection.
type Row struct {
	RawRow
	// Data is the output record data for current Plan.
	Data []types.Datum
	// RowKeys contains all table row keys in the row.
	RowKeys []*RowKeyEntry
}

// RawRow represents a un-decoded row which use much less memory than decoded row.
// RawData maybe nil if it is
type RawRow struct {
	RawData []byte
	Offsets []uint32
}

// RowKeyEntry represents a row key read from a table.
type RowKeyEntry struct {
	// The table which this row come from.
	Tbl table.Table
	// Row key.
	Handle int64
	// Table alias name.
	TableName string
}

// DecodeValuesTo decodes the given columns to values without allocating memory.
// cols has the same schema length as the row, but non interested columns is nil.
func (r *Row) DecodeValuesTo(cols []*expression.Column, values []types.Datum) error {
	if r.Data != nil {
		// Already decoded.
		copy(values, r.Data)
		return nil
	}
	for i, col := range cols {
		if col != nil {
			var colData []byte
			if i != len(r.Offsets)-1 {
				colData = r.RawData[r.Offsets[i]:r.Offsets[i+1]]
			} else {
				colData = r.RawData[r.Offsets[i]:]
			}
			var err error
			values[i], err = tablecodec.DecodeColumnValue(colData, col.RetType)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// DecodeValues decodes the row values.
func (r *Row) DecodeValues(schema *expression.Schema) error {
	if r.Data != nil {
		return nil
	}
	values := make([]types.Datum, len(r.Offsets))
	err := r.DecodeValuesTo(schema.Columns, values)
	if err != nil {
		return errors.Trace(err)
	}
	r.Data = values
	return nil
}

// setRowKeyEntry sets RowKeyEntry for the row.
func (r *Row) setRowKeyEntry(t table.Table, h int64, tableAsName *model.CIStr) {
	entry := &RowKeyEntry{
		Handle: h,
		Tbl:    t,
	}
	if tableAsName != nil && tableAsName.L != "" {
		entry.TableName = tableAsName.L
	} else {
		entry.TableName = t.Meta().Name.L
	}
	r.RowKeys = []*RowKeyEntry{entry}
}
