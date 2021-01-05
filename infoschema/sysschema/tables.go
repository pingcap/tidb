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

package sysschema

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
)

const (
	viewNameUnusedIndexes = "schema_unused_indexes"
)

var tableIDMap = map[string]int64{
	viewNameUnusedIndexes: autoid.SysSchemaDBID + 1,
}

type sysTable struct {
	infoschema.VirtualTable
	meta *model.TableInfo
	cols []*table.Column
	tp   table.Type
}

var pluginTable = make(map[string]func(autoid.Allocators, *model.TableInfo) (table.Table, error))

// RegisterTable registers a new table into TiDB.
func RegisterTable(tableName, sql string,
	tableFromMeta func(autoid.Allocators, *model.TableInfo) (table.Table, error)) {
	sysSchemaTables = append(sysSchemaTables, sql)
	pluginTable[tableName] = tableFromMeta
}

func tableFromMeta(allocs autoid.Allocators, meta *model.TableInfo) (table.Table, error) {
	if f, ok := pluginTable[meta.Name.L]; ok {
		ret, err := f(allocs, meta)
		return ret, err
	}
	return createSysTable(meta), nil
}

func createSysTable(meta *model.TableInfo) *sysTable {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	t := &sysTable{
		meta: meta,
		cols: columns,
	}
	return t
}

// Cols implements table.Table Type interface.
func (vt *sysTable) Cols() []*table.Column {
	return vt.cols
}

// VisibleCols implements table.Table VisibleCols interface.
func (vt *sysTable) VisibleCols() []*table.Column {
	return vt.cols
}

// HiddenCols implements table.Table HiddenCols interface.
func (vt *sysTable) HiddenCols() []*table.Column {
	return nil
}

// WritableCols implements table.Table Type interface.
func (vt *sysTable) WritableCols() []*table.Column {
	return vt.cols
}

// FullHiddenColsAndVisibleCols implements table FullHiddenColsAndVisibleCols interface.
func (vt *sysTable) FullHiddenColsAndVisibleCols() []*table.Column {
	return vt.cols
}

// GetID implements table.Table GetID interface.
func (vt *sysTable) GetPhysicalID() int64 {
	return vt.meta.ID
}

// Meta implements table.Table Type interface.
func (vt *sysTable) Meta() *model.TableInfo {
	return vt.meta
}

// Type implements table.Table Type interface.
func (vt *sysTable) Type() table.Type {
	return vt.tp
}
