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

package infoschema

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
)

func init() {
	dbID := autoid.SysSchemaDBID
	sysTables := make([]*model.TableInfo, 0)
	dbInfo := &model.DBInfo{
		ID:      dbID,
		Name:    model.NewCIStr(util.SysSchemaName.O),
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Tables:  sysTables,
	}
	RegisterVirtualTable(dbInfo, sysTableFromMeta)
}

// sysTableFromMeta is also used in test.
func sysTableFromMeta(alloc autoid.Allocators, meta *model.TableInfo) (table.Table, error) {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	t := &sysSchemaTable{
		infoschemaTable: infoschemaTable{
			meta: meta,
			cols: columns,
			tp:   table.VirtualTable,
		},
	}
	return t, nil
}

type sysSchemaTable struct {
	infoschemaTable
}

// IterRecords implements table.Table IterRecords interface.
func (vt *sysSchemaTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column, fn table.RecordIterFunc) error {
	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}
	return nil
}

// MockSysTable is only used for test.
func MockSysTable(testTableName string) {
	dbID := autoid.SysSchemaDBID
	var testTableCols = []columnInfo{
		{"ID", mysql.TypeLong, 20, 0, nil, nil},
	}
	tableInfo := buildTableMeta(testTableName, testTableCols)
	tableInfo.ID = dbID + 1
	for i, c := range tableInfo.Columns {
		c.ID = int64(i) + 1
	}
	dbInfo := &model.DBInfo{
		ID:      dbID,
		Name:    util.SysSchemaName,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Tables:  []*model.TableInfo{tableInfo},
	}
	RegisterVirtualTable(dbInfo, sysTableFromMeta)
}
