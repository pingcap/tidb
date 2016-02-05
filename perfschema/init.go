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
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

type columnInfo struct {
	tp    byte
	size  int
	flag  uint
	deflt interface{}
	elems []string
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

func (ps *perfSchema) buildModel(tbName string, colNames []string, cols []columnInfo) {
	rcols := make([]*model.ColumnInfo, len(cols))
	fields := make([]*field.ResultField, len(cols))
	for i, col := range cols {
		var ci *model.ColumnInfo

		if col.elems == nil {
			ci = buildUsualColumnInfo(colNames[i], col.tp, col.size, col.flag, col.deflt)
		} else {
			ci = buildEnumColumnInfo(colNames[i], col.elems, col.flag, col.deflt)
		}

		rcols[i] = ci
		fields[i] = buildResultField(tbName, ci)
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: rcols,
	}
	ps.fields[tbName] = fields
}

func buildUsualColumnInfo(name string, tp byte, size int, flag uint, def interface{}) *model.ColumnInfo {
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
		Flag:    uint(flag),
	}
	colInfo := &model.ColumnInfo{
		Name:         model.NewCIStr(name),
		FieldType:    fieldType,
		DefaultValue: def,
	}
	return colInfo
}

func buildEnumColumnInfo(name string, elems []string, flag uint, def interface{}) *model.ColumnInfo {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	if def == nil {
		flag |= mysql.NoDefaultValueFlag
	}
	fieldType := types.FieldType{
		Charset: mCharset,
		Collate: mCollation,
		Tp:      mysql.TypeEnum,
		Flag:    uint(flag),
		Elems:   elems,
	}
	colInfo := &model.ColumnInfo{
		Name:         model.NewCIStr(name),
		FieldType:    fieldType,
		DefaultValue: def,
	}
	return colInfo
}

func buildResultField(tableName string, colInfo *model.ColumnInfo) *field.ResultField {
	field := &field.ResultField{
		Col:       column.Col{ColumnInfo: *colInfo},
		DBName:    Name,
		TableName: tableName,
		Name:      colInfo.Name.O,
	}
	return field
}

func (ps *perfSchema) initRecords(tbName string, records [][]interface{}) error {
	lastLsn := atomic.AddUint64(ps.lsns[tbName], uint64(len(records)))

	batch := pool.Get().(*leveldb.Batch)
	defer func() {
		batch.Reset()
		pool.Put(batch)
	}()

	for i, rec := range records {
		lsn := lastLsn - uint64(len(records)) + uint64(i)
		rawKey := []interface{}{uint64(lsn)}
		key, err := codec.EncodeKey(nil, rawKey...)
		if err != nil {
			return errors.Trace(err)
		}
		val, err := codec.EncodeValue(nil, rec...)
		if err != nil {
			return errors.Trace(err)
		}
		batch.Put(key, val)
	}

	err := ps.stores[tbName].Write(batch, nil)
	return errors.Trace(err)
}

func (ps *perfSchema) initialize() (err error) {
	ps.tables = make(map[string]*model.TableInfo)
	ps.fields = make(map[string][]*field.ResultField)
	ps.stores = make(map[string]*leveldb.DB)
	ps.lsns = make(map[string]*uint64)

	for _, t := range PerfSchemaTables {
		ps.stores[t], err = leveldb.Open(storage.NewMemStorage(), nil)
		if err != nil {
			return errors.Trace(err)
		}
		ps.lsns[t] = new(uint64)
		*ps.lsns[t] = 0
	}

	allColDefs := [][]columnInfo{
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

	setupActorsRecords := [][]interface{}{
		{`%`, `%`, `%`, mysql.Enum{Name: "YES", Value: 1}, mysql.Enum{Name: "YES", Value: 1}},
	}
	err = ps.initRecords(TableSetupActors, setupActorsRecords)
	if err != nil {
		return errors.Trace(err)
	}

	setupObjectsRecords := [][]interface{}{
		{mysql.Enum{Name: "EVENT", Value: 1}, "mysql", `%`, mysql.Enum{Name: "NO", Value: 2}, mysql.Enum{Name: "NO", Value: 2}},
		{mysql.Enum{Name: "EVENT", Value: 1}, "performance_schema", `%`, mysql.Enum{Name: "NO", Value: 2}, mysql.Enum{Name: "NO", Value: 2}},
		{mysql.Enum{Name: "EVENT", Value: 1}, "information_schema", `%`, mysql.Enum{Name: "NO", Value: 2}, mysql.Enum{Name: "NO", Value: 2}},
		{mysql.Enum{Name: "EVENT", Value: 1}, `%`, `%`, mysql.Enum{Name: "YES", Value: 1}, mysql.Enum{Name: "YES", Value: 1}},
		{mysql.Enum{Name: "FUNCTION", Value: 2}, "mysql", `%`, mysql.Enum{Name: "NO", Value: 2}, mysql.Enum{Name: "NO", Value: 2}},
		{mysql.Enum{Name: "FUNCTION", Value: 2}, "performance_schema", `%`, mysql.Enum{Name: "NO", Value: 2}, mysql.Enum{Name: "NO", Value: 2}},
		{mysql.Enum{Name: "FUNCTION", Value: 2}, "information_schema", `%`, mysql.Enum{Name: "NO", Value: 2}, mysql.Enum{Name: "NO", Value: 2}},
		{mysql.Enum{Name: "FUNCTION", Value: 2}, `%`, `%`, mysql.Enum{Name: "YES", Value: 1}, mysql.Enum{Name: "YES", Value: 1}},
		{mysql.Enum{Name: "TABLE", Value: 3}, "mysql", `%`, mysql.Enum{Name: "NO", Value: 2}, mysql.Enum{Name: "NO", Value: 2}},
		{mysql.Enum{Name: "TABLE", Value: 3}, "performance_schema", `%`, mysql.Enum{Name: "NO", Value: 2}, mysql.Enum{Name: "NO", Value: 2}},
		{mysql.Enum{Name: "TABLE", Value: 3}, "information_schema", `%`, mysql.Enum{Name: "NO", Value: 2}, mysql.Enum{Name: "NO", Value: 2}},
		{mysql.Enum{Name: "TABLE", Value: 3}, `%`, `%`, mysql.Enum{Name: "YES", Value: 1}, mysql.Enum{Name: "YES", Value: 1}},
	}
	err = ps.initRecords(TableSetupObjects, setupObjectsRecords)
	if err != nil {
		return errors.Trace(err)
	}

	setupConsumersRecords := [][]interface{}{
		{"events_stages_current", mysql.Enum{Name: "NO", Value: 2}},
		{"events_stages_history", mysql.Enum{Name: "NO", Value: 2}},
		{"events_stages_history_long", mysql.Enum{Name: "NO", Value: 2}},
		{"events_statements_current", mysql.Enum{Name: "YES", Value: 1}},
		{"events_statements_history", mysql.Enum{Name: "YES", Value: 1}},
		{"events_statements_history_long", mysql.Enum{Name: "NO", Value: 2}},
		{"events_transactions_current", mysql.Enum{Name: "YES", Value: 1}},
		{"events_transactions_history", mysql.Enum{Name: "YES", Value: 1}},
		{"events_transactions_history_long", mysql.Enum{Name: "YES", Value: 1}},
		{"global_instrumentation", mysql.Enum{Name: "YES", Value: 1}},
		{"thread_instrumentation", mysql.Enum{Name: "YES", Value: 1}},
		{"statements_digest", mysql.Enum{Name: "YES", Value: 1}},
	}
	err = ps.initRecords(TableSetupConsumers, setupConsumersRecords)
	if err != nil {
		return errors.Trace(err)
	}

	setupTimersRecords := [][]interface{}{
		{"stage", mysql.Enum{Name: "NANOSECOND", Value: 1}},
		{"statement", mysql.Enum{Name: "NANOSECOND", Value: 1}},
		{"transaction", mysql.Enum{Name: "NANOSECOND", Value: 1}},
	}
	err = ps.initRecords(TableSetupTimers, setupTimersRecords)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
