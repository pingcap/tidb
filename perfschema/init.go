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

package perfschema

import (
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

type columnInfo struct {
	tp    byte
	size  int
	flag  uint
	deflt interface{}
	elems []string
}

var globalStatusCols = []columnInfo{
	{mysql.TypeString, 64, mysql.NotNullFlag, `%`, nil},
	{mysql.TypeString, 1024, 0, `%`, nil},
}

var sessionStatusCols = []columnInfo{
	{mysql.TypeString, 64, mysql.NotNullFlag, `%`, nil},
	{mysql.TypeString, 1024, 0, `%`, nil},
}

var setupActorsCols = []columnInfo{
	{mysql.TypeString, 60, mysql.NotNullFlag, `%`, nil},
	{mysql.TypeString, 32, mysql.NotNullFlag, `%`, nil},
	{mysql.TypeString, 16, mysql.NotNullFlag, `%`, nil},
	{mysql.TypeEnum, -1, mysql.NotNullFlag, "YES", []string{"YES", "NO"}},
	{mysql.TypeEnum, -1, mysql.NotNullFlag, "YES", []string{"YES", "NO"}},
}

var setupObjectsCols = []columnInfo{
	{mysql.TypeEnum, -1, mysql.NotNullFlag, "TABLE", []string{"EVENT", "FUNCTION", "TABLE"}},
	{mysql.TypeVarchar, 64, 0, `%`, nil},
	{mysql.TypeVarchar, 64, mysql.NotNullFlag, `%`, nil},
	{mysql.TypeEnum, -1, mysql.NotNullFlag, "YES", []string{"YES", "NO"}},
	{mysql.TypeEnum, -1, mysql.NotNullFlag, "YES", []string{"YES", "NO"}},
}

var setupInstrumentsCols = []columnInfo{
	{mysql.TypeVarchar, 128, mysql.NotNullFlag, nil, nil},
	{mysql.TypeEnum, -1, mysql.NotNullFlag, nil, []string{"YES", "NO"}},
	{mysql.TypeEnum, -1, mysql.NotNullFlag, nil, []string{"YES", "NO"}},
}

var setupConsumersCols = []columnInfo{
	{mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{mysql.TypeEnum, -1, mysql.NotNullFlag, nil, []string{"YES", "NO"}},
}

var setupTimersCols = []columnInfo{
	{mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{mysql.TypeEnum, -1, mysql.NotNullFlag, nil, []string{"NANOSECOND", "MICROSECOND", "MILLISECOND"}},
}

var stmtsCurrentCols = []columnInfo{
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeVarchar, 128, mysql.NotNullFlag, nil, nil},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLongBlob, -1, 0, nil, nil},
	{mysql.TypeVarchar, 32, 0, nil, nil},
	{mysql.TypeLongBlob, -1, 0, nil, nil},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLong, 11, 0, nil, nil},
	{mysql.TypeVarchar, 5, 0, nil, nil},
	{mysql.TypeVarchar, 128, 0, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeEnum, -1, 0, nil, []string{"TRANSACTION", "STATEMENT", "STAGE"}},
	{mysql.TypeLong, 11, 0, nil, nil},
}

var preparedStmtsInstancesCols = []columnInfo{
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeLongBlob, -1, mysql.NotNullFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeEnum, -1, 0, nil, []string{"EVENT", "FUNCTION", "TABLE"}},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
}

var transCurrentCols = []columnInfo{
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeVarchar, 128, mysql.NotNullFlag, nil, nil},
	{mysql.TypeEnum, -1, 0, nil, []string{"ACTIVE", "COMMITTED", "ROLLED BACK"}},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeLong, 11, 0, nil, nil},
	{mysql.TypeVarchar, 130, 0, nil, nil},
	{mysql.TypeVarchar, 130, 0, nil, nil},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeEnum, -1, 0, nil, []string{"READ ONLY", "READ WRITE"}},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeEnum, -1, mysql.NotNullFlag, nil, []string{"YES", "NO"}},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeEnum, -1, 0, nil, []string{"TRANSACTION", "STATEMENT", "STAGE"}},
}

var stagesCurrentCols = []columnInfo{
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.NotNullFlag | mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeVarchar, 128, mysql.NotNullFlag, nil, nil},
	{mysql.TypeVarchar, 64, 0, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{mysql.TypeEnum, -1, 0, nil, []string{"TRANSACTION", "STATEMENT", "STAGE"}},
}

func (ps *perfSchema) buildTables() {
	tbls := make([]*model.TableInfo, 0, len(ps.tables))
	dbID := autoid.GenLocalSchemaID()

	for name, meta := range ps.tables {
		tbls = append(tbls, meta)
		meta.ID = autoid.GenLocalSchemaID()
		for _, c := range meta.Columns {
			c.ID = autoid.GenLocalSchemaID()
		}
		alloc := autoid.NewMemoryAllocator(dbID)
		var tbl table.Table
		switch name {
		//@TODO in the future, we need to add many VirtualTable, we may need to add new type for these tables.
		case TableSessionStatus, TableGlobalStatus:
			tbl = createVirtualTable(meta, name)
		default:
			tbl = tables.MemoryTableFromMeta(alloc, meta)
		}
		ps.mTables[name] = tbl
	}
	ps.dbInfo = &model.DBInfo{
		ID:      dbID,
		Name:    model.NewCIStr(Name),
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Tables:  tbls,
	}
}

func (ps *perfSchema) buildModel(tbName string, colNames []string, cols []columnInfo) {
	rcols := make([]*model.ColumnInfo, len(cols))
	for i, col := range cols {
		var ci *model.ColumnInfo
		if col.elems == nil {
			ci = buildUsualColumnInfo(i, colNames[i], col.tp, col.size, col.flag, col.deflt)
		} else {
			ci = buildEnumColumnInfo(i, colNames[i], col.elems, col.flag, col.deflt)
		}
		rcols[i] = ci
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: rcols,
	}
}

func buildUsualColumnInfo(offset int, name string, tp byte, size int, flag uint, def interface{}) *model.ColumnInfo {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	if tp == mysql.TypeString || tp == mysql.TypeVarchar || tp == mysql.TypeBlob || tp == mysql.TypeLongBlob {
		mCharset = mysql.DefaultCharset
		mCollation = mysql.DefaultCollationName
	}
	if def == nil {
		flag |= mysql.NoDefaultValueFlag
	}
	// TODO: does TypeLongBlob need size?
	fieldType := types.FieldType{
		Charset: mCharset,
		Collate: mCollation,
		Tp:      tp,
		Flen:    size,
		Flag:    flag,
	}
	colInfo := &model.ColumnInfo{
		Name:         model.NewCIStr(name),
		Offset:       offset,
		FieldType:    fieldType,
		DefaultValue: def,
		State:        model.StatePublic,
	}
	return colInfo
}

func buildEnumColumnInfo(offset int, name string, elems []string, flag uint, def interface{}) *model.ColumnInfo {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	if def == nil {
		flag |= mysql.NoDefaultValueFlag
	}
	fieldType := types.FieldType{
		Charset: mCharset,
		Collate: mCollation,
		Tp:      mysql.TypeEnum,
		Flag:    flag,
		Elems:   elems,
	}
	colInfo := &model.ColumnInfo{
		Name:         model.NewCIStr(name),
		Offset:       offset,
		FieldType:    fieldType,
		DefaultValue: def,
		State:        model.StatePublic,
	}
	return colInfo
}

func (ps *perfSchema) initialize() {
	ps.tables = make(map[string]*model.TableInfo)
	ps.mTables = make(map[string]table.Table, len(ps.tables))

	allColDefs := [][]columnInfo{
		globalStatusCols,
		sessionStatusCols,
		setupActorsCols,
		setupObjectsCols,
		setupInstrumentsCols,
		setupConsumersCols,
		setupTimersCols,
		stmtsCurrentCols,
		stmtsCurrentCols, // same as above
		stmtsCurrentCols, // same as above
		preparedStmtsInstancesCols,
		transCurrentCols,
		transCurrentCols, // same as above
		transCurrentCols, // same as above
		stagesCurrentCols,
		stagesCurrentCols, // same as above
		stagesCurrentCols, // same as above
	}

	allColNames := [][]string{
		ColumnGlobalStatus,
		ColumnSessionStatus,
		ColumnSetupActors,
		ColumnSetupObjects,
		ColumnSetupInstruments,
		ColumnSetupConsumers,
		ColumnSetupTimers,
		ColumnStmtsCurrent,
		ColumnStmtsHistory,
		ColumnStmtsHistoryLong,
		ColumnPreparedStmtsInstances,
		ColumnStmtsCurrent,
		ColumnStmtsHistory,
		ColumnStmtsHistoryLong,
		ColumnStagesCurrent,
		ColumnStagesHistory,
		ColumnStagesHistoryLong,
	}

	// initialize all table, column and result field definitions
	for i, def := range allColDefs {
		ps.buildModel(PerfSchemaTables[i], allColNames[i], def)
	}
	ps.buildTables()
}

func (ps *perfSchema) GetDBMeta() *model.DBInfo {
	return ps.dbInfo
}

func (ps *perfSchema) GetTable(name string) (table.Table, bool) {
	tbl, ok := ps.mTables[name]
	return tbl, ok
}
