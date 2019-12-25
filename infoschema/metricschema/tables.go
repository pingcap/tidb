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

package metricschema

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
)

// metricSchemaTable stands for the fake table all its data is in the memory.
type metricSchemaTable struct {
	infoschema.VirtualTable
	meta *model.TableInfo
	cols []*table.Column
}

func tableFromMeta(alloc autoid.Allocators, meta *model.TableInfo) (table.Table, error) {
	return createMetricSchemaTable(meta), nil
}

// createMetricSchemaTable creates all metricSchemaTables
func createMetricSchemaTable(meta *model.TableInfo) *metricSchemaTable {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	t := &metricSchemaTable{
		meta: meta,
		cols: columns,
	}
	return t
}

// Cols implements table.Table Type interface.
func (vt *metricSchemaTable) Cols() []*table.Column {
	return vt.cols
}

// WritableCols implements table.Table Type interface.
func (vt *metricSchemaTable) WritableCols() []*table.Column {
	return vt.cols
}

// GetID implements table.Table GetID interface.
func (vt *metricSchemaTable) GetPhysicalID() int64 {
	return vt.meta.ID
}

// Meta implements table.Table Type interface.
func (vt *metricSchemaTable) Meta() *model.TableInfo {
	return vt.meta
}

// IterRecords implements table.Table IterRecords interface.
func (vt *metricSchemaTable) IterRecords(_ sessionctx.Context, _ kv.Key, _ []*table.Column, _ table.RecordIterFunc) error {
	return nil
}

// Type implements table.Table Type interface.
func (vt *metricSchemaTable) Type() table.Type {
	return table.VirtualTable
}
