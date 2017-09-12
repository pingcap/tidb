// Copyright 2016 PingCAP, Inc.
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

package tables

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/types"
)

type SysVarHandle interface {
	// GetRows is used to extract data from struct in memory
	GetRows(ctx context.Context, cols []*table.Column) (fullRows [][]types.Datum, err error)
}

// SysVarTable stands for the fake table all its data is in the memory.
// handle: the function to get rows
// meta: table meta data
// cols: column info for the table
// @TODO this table is almost the same as the infoschema tables, but we need to use it in performance schema.
// @TODO So we have to move it here, sometimes we need to refactor the infoschema tables to decrease the multiplicity of the codes
type SysVarTable struct {
	handle SysVarHandle
	meta   *model.TableInfo
	cols   []*table.Column
}

func CreateSysVarTable(handle SysVarHandle, meta *model.TableInfo) *SysVarTable {
	columns := make([]*table.Column, len(meta.Columns))
	for i, col := range meta.Columns {
		columns[i] = table.ToColumn(col)
	}
	return &SysVarTable{
		handle: handle,
		meta:   meta,
		cols:   columns,
	}
}

func (svt *SysVarTable) IterRecords(ctx context.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}
	rows, err := svt.handle.GetRows(ctx, cols)
	if err != nil {
		return errors.Trace(err)
	}
	for i, row := range rows {
		more, err := fn(int64(i), row, cols)
		if err != nil {
			return errors.Trace(err)
		}
		if !more {
			break
		}
	}
	return nil
}

func (svt *SysVarTable) RowWithCols(ctx context.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

func (svt *SysVarTable) Row(ctx context.Context, h int64) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

func (svt *SysVarTable) Cols() []*table.Column {
	return svt.cols
}

func (svt *SysVarTable) WritableCols() []*table.Column {
	return svt.cols
}

func (svt *SysVarTable) Indices() []table.Index {
	return nil
}

func (svt *SysVarTable) WritableIndices() []table.Index {
	return nil
}

func (svt *SysVarTable) DeletableIndices() []table.Index {
	return nil
}

func (svt *SysVarTable) RecordPrefix() kv.Key {
	return nil
}

func (svt *SysVarTable) IndexPrefix() kv.Key {
	return nil
}

func (svt *SysVarTable) FirstKey() kv.Key {
	return nil
}

func (svt *SysVarTable) RecordKey(h int64) kv.Key {
	return nil
}

func (svt *SysVarTable) AddRecord(ctx context.Context, r []types.Datum) (recordID int64, err error) {
	return 0, table.ErrUnsupportedOp
}

func (svt *SysVarTable) RemoveRecord(ctx context.Context, h int64, r []types.Datum) error {
	return table.ErrUnsupportedOp
}

func (svt *SysVarTable) UpdateRecord(ctx context.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	return table.ErrUnsupportedOp
}

func (svt *SysVarTable) AllocAutoID() (int64, error) {
	return 0, table.ErrUnsupportedOp
}

func (svt *SysVarTable) Allocator() autoid.Allocator {
	return nil
}

func (svt *SysVarTable) RebaseAutoID(newBase int64, isSetStep bool) error {
	return table.ErrUnsupportedOp
}

func (svt *SysVarTable) Meta() *model.TableInfo {
	return svt.meta
}

func (svt *SysVarTable) Seek(ctx context.Context, h int64) (int64, bool, error) {
	return 0, false, table.ErrUnsupportedOp
}

func (svt *SysVarTable) TableType() table.TableType {
	return table.SystemVarTale
}
