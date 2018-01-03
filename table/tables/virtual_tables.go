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

package tables

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

// VirtualDataSource is used to extract data from the struct in memory.
type VirtualDataSource interface {
	// GetRows do the actual job
	GetRows(ctx context.Context) (fullRows [][]types.Datum, err error)
	// Meta return the meta of table
	Meta() *model.TableInfo
	// Cols return the cols of table
	Cols() []*table.Column
}

// VirtualTable stands for the fake table all its data is in the memory.
// dataSource: the function to get rows
// @TODO this table is almost the same as the infoschema tables, but we need to use it in performance schema.
// @TODO So we have to move it here, sometimes we need to refactor the infoschema tables to decrease the multiplicity of the codes
type VirtualTable struct {
	dataSource VirtualDataSource
}

// CreateVirtualTable as its name
func CreateVirtualTable(dataSource VirtualDataSource) *VirtualTable {
	return &VirtualTable{dataSource: dataSource}
}

// IterRecords implements table.Table Type interface.
func (vt *VirtualTable) IterRecords(ctx context.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}
	rows, err := vt.dataSource.GetRows(ctx)
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

// RowWithCols implements table.Table Type interface.
func (vt *VirtualTable) RowWithCols(ctx context.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

// Row implements table.Table Type interface.
func (vt *VirtualTable) Row(ctx context.Context, h int64) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

// Cols implements table.Table Type interface.
func (vt *VirtualTable) Cols() []*table.Column {
	return vt.dataSource.Cols()
}

// WritableCols implements table.Table Type interface.
func (vt *VirtualTable) WritableCols() []*table.Column {
	return vt.dataSource.Cols()
}

// Indices implements table.Table Type interface.
func (vt *VirtualTable) Indices() []table.Index {
	return nil
}

// WritableIndices implements table.Table Type interface.
func (vt *VirtualTable) WritableIndices() []table.Index {
	return nil
}

// DeletableIndices implements table.Table Type interface.
func (vt *VirtualTable) DeletableIndices() []table.Index {
	return nil
}

// RecordPrefix implements table.Table Type interface.
func (vt *VirtualTable) RecordPrefix() kv.Key {
	return nil
}

// IndexPrefix implements table.Table Type interface.
func (vt *VirtualTable) IndexPrefix() kv.Key {
	return nil
}

// FirstKey implements table.Table Type interface.
func (vt *VirtualTable) FirstKey() kv.Key {
	return nil
}

// RecordKey implements table.Table Type interface.
func (vt *VirtualTable) RecordKey(h int64) kv.Key {
	return nil
}

// AddRecord implements table.Table Type interface.
func (vt *VirtualTable) AddRecord(ctx context.Context, r []types.Datum, skipHandleCheck bool, bs *kv.BufferStore) (recordID int64, err error) {
	return 0, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table Type interface.
func (vt *VirtualTable) RemoveRecord(ctx context.Context, h int64, r []types.Datum) error {
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table Type interface.
func (vt *VirtualTable) UpdateRecord(ctx context.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	return table.ErrUnsupportedOp
}

// AllocAutoID implements table.Table Type interface.
func (vt *VirtualTable) AllocAutoID(ctx context.Context) (int64, error) {
	return 0, table.ErrUnsupportedOp
}

// Allocator implements table.Table Type interface.
func (vt *VirtualTable) Allocator(ctx context.Context) autoid.Allocator {
	return nil
}

// RebaseAutoID implements table.Table Type interface.
func (vt *VirtualTable) RebaseAutoID(ctx context.Context, newBase int64, isSetStep bool) error {
	return table.ErrUnsupportedOp
}

// Meta implements table.Table Type interface.
func (vt *VirtualTable) Meta() *model.TableInfo {
	return vt.dataSource.Meta()
}

// Seek implements table.Table Type interface.
func (vt *VirtualTable) Seek(ctx context.Context, h int64) (int64, bool, error) {
	return 0, false, table.ErrUnsupportedOp
}

// Type implements table.Table Type interface.
func (vt *VirtualTable) Type() table.Type {
	return table.VirtualTable
}
