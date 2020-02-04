// Copyright 2020 PingCAP, Inc.
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

package sysschema

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
)

type sysSchemaTable struct {
	infoschema.VirtualTable
	meta *model.TableInfo
	cols []*table.Column
	tp   table.Type
}

func tableFromMeta(alloc autoid.Allocators, meta *model.TableInfo) (table.Table, error) {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	t := &sysSchemaTable{
		meta: meta,
		cols: columns,
		tp:   table.VirtualTable,
	}
	return t, nil
}

// Cols implements table.Table Type interface.
func (vt *sysSchemaTable) Cols() []*table.Column {
	return vt.cols
}

// VisibleCols implements table.Table VisibleCols interface.
func (vt *sysSchemaTable) VisibleCols() []*table.Column {
	return vt.cols
}

// WritableCols implements table.Table Type interface.
func (vt *sysSchemaTable) WritableCols() []*table.Column {
	return vt.cols
}

// GetID implements table.Table GetID interface.
func (vt *sysSchemaTable) GetPhysicalID() int64 {
	return vt.meta.ID
}

// Meta implements table.Table Type interface.
func (vt *sysSchemaTable) Meta() *model.TableInfo {
	return vt.meta
}

// Type implements table.Table Type interface.
func (vt *sysSchemaTable) Type() table.Type {
	return vt.tp
}

// IterRecords implements table.Table IterRecords interface.
func (vt *sysSchemaTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column, fn table.RecordIterFunc) error {
	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}
	return nil
}
