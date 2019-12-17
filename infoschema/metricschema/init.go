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
	"sync"

	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
)

var once sync.Once

// Init register the METRIC_DB virtual tables.
func Init() {
	initOnce := func() {
		dbID := autoid.MetricSchemaDBID
		tableID := dbID + 1
		metricTables := make([]*model.TableInfo, 0, len(metricTableMap))
		for name, def := range metricTableMap {
			cols := def.genColumnInfos()
			tableInfo := buildTableMeta(name, cols)
			tableInfo.ID = tableID
			tableID++
			metricTables = append(metricTables, tableInfo)
		}
		dbInfo := &model.DBInfo{
			ID:      dbID,
			Name:    model.NewCIStr(util.MetricSchemaName.O),
			Charset: mysql.DefaultCharset,
			Collate: mysql.DefaultCollationName,
			Tables:  metricTables,
		}
		infoschema.RegisterVirtualTable(dbInfo, tableFromMeta)
	}
	once.Do(initOnce)
}

func buildColumnInfo(col columnInfo) *model.ColumnInfo {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	mFlag := mysql.UnsignedFlag
	if col.tp == mysql.TypeVarchar || col.tp == mysql.TypeBlob {
		mCharset = charset.CharsetUTF8MB4
		mCollation = charset.CollationUTF8MB4
		mFlag = col.flag
	}
	fieldType := types.FieldType{
		Charset: mCharset,
		Collate: mCollation,
		Tp:      col.tp,
		Flen:    col.size,
		Flag:    mFlag,
	}
	return &model.ColumnInfo{
		Name:         model.NewCIStr(col.name),
		FieldType:    fieldType,
		State:        model.StatePublic,
		DefaultValue: col.deflt,
	}
}

func buildTableMeta(tableName string, cs []columnInfo) *model.TableInfo {
	cols := make([]*model.ColumnInfo, 0, len(cs))
	for _, c := range cs {
		cols = append(cols, buildColumnInfo(c))
	}
	for i, col := range cols {
		col.Offset = i
		col.ID = int64(i) + 1
	}
	return &model.TableInfo{
		Name:    model.NewCIStr(tableName),
		Columns: cols,
		State:   model.StatePublic,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
	}
}
