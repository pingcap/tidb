// Copyright 2019 PingCAP, Inc.
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

package tiniub

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

type slowQueryTable struct {
	meta *model.TableInfo
	cols []*table.Column
}

func tableFromMeta(alloc autoid.Allocator, meta *model.TableInfo) (table.Table, error) {
	return createSlowQueryTable(meta), nil
}

// createSlowQueryTable creates all slowQueryTables
func createSlowQueryTable(meta *model.TableInfo) *slowQueryTable {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	t := &slowQueryTable{
		meta: meta,
		cols: columns,
	}
	return t
}

// IterRecords rewrites the IterRecords method of slowQueryTable.
func (s slowQueryTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column, fn table.RecordIterFunc) error {
	dom := domain.GetDomain(ctx)
	result := dom.ShowSlowQuery(&ast.ShowSlow{Tp: 42})

	for i, item := range result {
		row := make([]types.Datum, 0, len(cols))
		row = append(row, types.NewDatum(item.SQL))

		ts := types.NewTimeDatum(types.Time{types.FromGoTime(item.Start), mysql.TypeTimestamp, types.MaxFsp})
		row = append(row, ts)
		row = append(row, types.NewDurationDatum(types.Duration{item.Duration, types.MaxFsp}))
		row = append(row, types.NewDatum(item.Detail.String()))
		row = append(row, types.NewDatum(item.Succ))
		row = append(row, types.NewDatum(item.ConnID))
		row = append(row, types.NewDatum(item.TxnTS))
		row = append(row, types.NewDatum(item.User))
		row = append(row, types.NewDatum(item.DB))
		row = append(row, types.NewDatum(item.TableIDs))
		row = append(row, types.NewDatum(item.IndexIDs))
		row = append(row, types.NewDatum(item.Internal))

		more, err := fn(int64(i), row, cols)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}

// createPerfSchemaTable creates all slowQueryTables
func createPerfSchemaTable(meta *model.TableInfo) *slowQueryTable {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	t := &slowQueryTable{
		meta: meta,
		cols: columns,
	}
	return t
}

// RowWithCols implements table.Table Type interface.
func (s *slowQueryTable) RowWithCols(ctx sessionctx.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

// Row implements table.Table Type interface.
func (s *slowQueryTable) Row(ctx sessionctx.Context, h int64) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

// Cols implements table.Table Type interface.
func (s *slowQueryTable) Cols() []*table.Column {
	return s.cols
}

// WritableCols implements table.Table Type interface.
func (s *slowQueryTable) WritableCols() []*table.Column {
	return s.cols
}

// Indices implements table.Table Type interface.
func (s *slowQueryTable) Indices() []table.Index {
	return nil
}

// WritableIndices implements table.Table Type interface.
func (s *slowQueryTable) WritableIndices() []table.Index {
	return nil
}

// DeletableIndices implements table.Table Type interface.
func (s *slowQueryTable) DeletableIndices() []table.Index {
	return nil
}

// RecordPrefix implements table.Table Type interface.
func (s *slowQueryTable) RecordPrefix() kv.Key {
	return nil
}

// IndexPrefix implements table.Table Type interface.
func (s *slowQueryTable) IndexPrefix() kv.Key {
	return nil
}

// FirstKey implements table.Table Type interface.
func (s *slowQueryTable) FirstKey() kv.Key {
	return nil
}

// RecordKey implements table.Table Type interface.
func (s *slowQueryTable) RecordKey(h int64) kv.Key {
	return nil
}

// AddRecord implements table.Table Type interface.
func (s *slowQueryTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...*table.AddRecordOpt) (recordID int64, err error) {
	return 0, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table Type interface.
func (s *slowQueryTable) RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error {
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table Type interface.
func (s *slowQueryTable) UpdateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	return table.ErrUnsupportedOp
}

// AllocAutoID implements table.Table Type interface.
func (s *slowQueryTable) AllocAutoID(ctx sessionctx.Context) (int64, error) {
	return 0, table.ErrUnsupportedOp
}

// Allocator implements table.Table Type interface.
func (s *slowQueryTable) Allocator(ctx sessionctx.Context) autoid.Allocator {
	return nil
}

// RebaseAutoID implements table.Table Type interface.
func (s *slowQueryTable) RebaseAutoID(ctx sessionctx.Context, newBase int64, isSetStep bool) error {
	return table.ErrUnsupportedOp
}

// Meta implements table.Table Type interface.
func (s *slowQueryTable) Meta() *model.TableInfo {
	return s.meta
}

// GetID implements table.Table GetID interface.
func (s *slowQueryTable) GetPhysicalID() int64 {
	return s.meta.ID
}

// Seek implements table.Table Type interface.
func (s *slowQueryTable) Seek(ctx sessionctx.Context, h int64) (int64, bool, error) {
	return 0, false, table.ErrUnsupportedOp
}

// Type implements table.Table Type interface.
func (s *slowQueryTable) Type() table.Type {
	return table.VirtualTable
}
