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

package infoschema

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
)

// The `inspection_schema` is used to provide a consistent view of `information_schema` tables,
// so the related table should have the same table name within `information_schema`.
// The data will be obtained lazily from `information_schema` and cache in `SessionVars`, and
// the cached data will be cleared at `InspectionExec` closing.
var inspectionTables = map[string][]columnInfo{
	TableClusterInfo:       tableClusterInfoCols,
	TableClusterConfig:     tableClusterConfigCols,
	TableClusterLoad:       tableClusterLoadCols,
	TableClusterHardware:   tableClusterHardwareCols,
	TableClusterSystemInfo: tableClusterSystemInfoCols,
}

type inspectionSchemaTable struct {
	infoschemaTable
}

// IterRecords implements table.Table IterRecords interface.
func (it *inspectionSchemaTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	sessionVars := ctx.GetSessionVars()
	// The `InspectionTableCache` will be assigned in the begin of retrieving` and be
	// cleaned at the end of retrieving, so nil represents currently in non-inspection mode.
	if sessionVars.InspectionTableCache == nil {
		return errors.New("not currently in inspection mode")
	}

	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}

	// Obtain data from cache first.
	cached, found := sessionVars.InspectionTableCache[it.meta.Name.L]
	if !found {
		// Retrieve data from `information_schema` if cannot found in cache.
		sql := "select * from information_schema." + it.meta.Name.L
		results, fieldTypes, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
		var rows [][]types.Datum
		if len(results) > 0 {
			fields := make([]*types.FieldType, 0, len(fieldTypes))
			for _, field := range fieldTypes {
				fields = append(fields, &field.Column.FieldType)
			}
			rows = make([][]types.Datum, 0, len(results))
			for _, result := range results {
				rows = append(rows, result.GetDatumRow(fields))
			}
		}
		cached = variable.TableSnapshot{
			Rows: rows,
			Err:  err,
		}
		sessionVars.InspectionTableCache[it.meta.Name.L] = cached
	}
	if cached.Err != nil {
		return cached.Err
	}

	for i, row := range cached.Rows {
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

func init() {
	// Initialize the inspection schema database and register the driver to `drivers`.
	dbID := autoid.InspectionSchemaDBID
	tables := make([]*model.TableInfo, 0, len(inspectionTables))
	for name, cols := range inspectionTables {
		tableInfo := buildTableMeta(name, cols)
		tables = append(tables, tableInfo)
		var ok bool
		tid, ok := tableIDMap[tableInfo.Name.O]
		if !ok {
			panic(fmt.Sprintf("get inspection_schema table id failed, unknown system table `%v`", tableInfo.Name.O))
		}
		// Reuse information_schema table id serial number.
		tableInfo.ID = tid - autoid.InformationSchemaDBID + autoid.InspectionSchemaDBID
		for i, c := range tableInfo.Columns {
			c.ID = int64(i) + 1
		}
	}
	inspectionSchema := &model.DBInfo{
		ID:      dbID,
		Name:    util.InspectionSchemaName,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Tables:  tables,
	}
	builder := func(_ autoid.Allocators, meta *model.TableInfo) (table.Table, error) {
		columns := make([]*table.Column, len(meta.Columns))
		for i, col := range meta.Columns {
			columns[i] = table.ToColumn(col)
		}
		tbl := &inspectionSchemaTable{
			infoschemaTable{
				meta: meta,
				cols: columns,
				tp:   table.VirtualTable,
			},
		}
		return tbl, nil
	}
	RegisterVirtualTable(inspectionSchema, builder)
}
