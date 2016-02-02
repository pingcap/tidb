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

// CREATE TABLE if not exists performance_schema.setup_actors (
// 		HOST			CHAR(60) NOT NULL  DEFAULT '%',
// 		USER			CHAR(32) NOT NULL  DEFAULT '%',
// 		ROLE			CHAR(16) NOT NULL  DEFAULT '%',
// 		ENABLED			ENUM('YES','NO') NOT NULL  DEFAULT 'YES',
// 		HISTORY			ENUM('YES','NO') NOT NULL  DEFAULT 'YES');
func (ps *perfSchema) buildSetupActorsModel() {
	tbName := TableSetupActors
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
		{mysql.TypeString, 60, mysql.NotNullFlag, "%", nil},
		{mysql.TypeString, 32, mysql.NotNullFlag, "%", nil},
		{mysql.TypeString, 16, mysql.NotNullFlag, "%", nil},
		{mysql.TypeEnum, -1, mysql.NotNullFlag, "YES", []string{"YES", "NO"}},
		{mysql.TypeEnum, -1, mysql.NotNullFlag, "YES", []string{"YES", "NO"}},
	}

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnSetupActors[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.setup_objects (
// 		OBJECT_TYPE		ENUM('EVENT','FUNCTION','TABLE') NOT NULL  DEFAULT 'TABLE',
// 		OBJECT_SCHEMA	VARCHAR(64)  DEFAULT '%',
// 		OBJECT_NAME		VARCHAR(64) NOT NULL  DEFAULT '%',
// 		ENABLED			ENUM('YES','NO') NOT NULL  DEFAULT 'YES',
// 		TIMED			ENUM('YES','NO') NOT NULL  DEFAULT 'YES');
func (ps *perfSchema) buildSetupObjectsModel() {
	tbName := TableSetupObjects
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
		{mysql.TypeEnum, -1, mysql.NotNullFlag, "TABLE", []string{"EVENT", "FUNCTION", "TABLE"}},
		{mysql.TypeVarchar, 64, 0, "%", nil},
		{mysql.TypeVarchar, 64, mysql.NotNullFlag, "%", nil},
		{mysql.TypeEnum, -1, mysql.NotNullFlag, "YES", []string{"YES", "NO"}},
		{mysql.TypeEnum, -1, mysql.NotNullFlag, "YES", []string{"YES", "NO"}},
	}

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnSetupObjects[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.setup_instruments (
// 		NAME			VARCHAR(128) NOT NULL,
// 		ENABLED			ENUM('YES','NO') NOT NULL,
// 		TIMED			ENUM('YES','NO') NOT NULL);
func (ps *perfSchema) buildSetupInstrumentsModel() {
	tbName := TableSetupInstruments
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
		{mysql.TypeVarchar, 128, mysql.NotNullFlag, nil, nil},
		{mysql.TypeEnum, -1, mysql.NotNullFlag, nil, []string{"YES", "NO"}},
		{mysql.TypeEnum, -1, mysql.NotNullFlag, nil, []string{"YES", "NO"}},
	}

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnSetupInstruments[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.setup_consumers (
// 		NAME			VARCHAR(64) NOT NULL,
// 		ENABLED			ENUM('YES','NO') NOT NULL);
func (ps *perfSchema) buildSetupConsumersModel() {
	tbName := TableSetupConsumers
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
		{mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
		{mysql.TypeEnum, -1, mysql.NotNullFlag, nil, []string{"YES", "NO"}},
	}

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnSetupConsumers[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.setup_timers (
// 		NAME			VARCHAR(64) NOT NULL,
// 		TIMER_NAME		ENUM('NANOSECOND','MICROSECOND','MILLISECOND') NOT NULL);
func (ps *perfSchema) buildSetupTimersModel() {
	tbName := TableSetupTimers
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
		{mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
		{mysql.TypeEnum, -1, mysql.NotNullFlag, nil, []string{"NANOSECOND", "MICROSECOND", "MILLISECOND"}},
	}

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnSetupTimers[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.events_statements_current (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL,
// 		SQL_TEXT		LONGTEXT,
// 		DIGEST			VARCHAR(32),
// 		DIGEST_TEXT		LONGTEXT,
// 		CURRENT_SCHEMA	VARCHAR(64),
// 		OBJECT_TYPE		VARCHAR(64),
// 		OBJECT_SCHEMA	VARCHAR(64),
// 		OBJECT_NAME		VARCHAR(64),
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
// 		MYSQL_ERRNO		INT(11),
// 		RETURNED_SQLSTATE		VARCHAR(5),
// 		MESSAGE_TEXT	VARCHAR(128),
// 		ERRORS			BIGINT(20) UNSIGNED NOT NULL,
// 		WARNINGS		BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL,
// 		CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL,
// 		CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
// 		NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL,
// 		NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'),
// 		NESTING_EVENT_LEVEL		INT(11));
func (ps *perfSchema) buildStmtsCurrentModel() {
	tbName := TableStmtsCurrent
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
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

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnStmtsCurrent[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.events_statements_history (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL,
// 		SQL_TEXT		LONGTEXT,
// 		DIGEST			VARCHAR(32),
// 		DIGEST_TEXT		LONGTEXT,
// 		CURRENT_SCHEMA	VARCHAR(64),
// 		OBJECT_TYPE		VARCHAR(64),
// 		OBJECT_SCHEMA	VARCHAR(64),
// 		OBJECT_NAME		VARCHAR(64),
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
// 		MYSQL_ERRNO		INT(11),
// 		RETURNED_SQLSTATE		VARCHAR(5),
// 		MESSAGE_TEXT	VARCHAR(128),
// 		ERRORS			BIGINT(20) UNSIGNED NOT NULL,
// 		WARNINGS		BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL,
// 		CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL,
// 		CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
// 		NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL,
// 		NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'),
// 		NESTING_EVENT_LEVEL		INT(11));
func (ps *perfSchema) buildStmtsHistoryModel() {
	tbName := TableStmtsHistory
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
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

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnStmtsHistory[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.events_statements_history_long (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL,
// 		SQL_TEXT		LONGTEXT,
// 		DIGEST			VARCHAR(32),
// 		DIGEST_TEXT		LONGTEXT,
// 		CURRENT_SCHEMA	VARCHAR(64),
// 		OBJECT_TYPE		VARCHAR(64),
// 		OBJECT_SCHEMA	VARCHAR(64),
// 		OBJECT_NAME		VARCHAR(64),
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
// 		MYSQL_ERRNO		INT(11),
// 		RETURNED_SQLSTATE		VARCHAR(5),
// 		MESSAGE_TEXT	VARCHAR(128),
// 		ERRORS			BIGINT(20) UNSIGNED NOT NULL,
// 		WARNINGS		BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL,
// 		CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL,
// 		CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
// 		NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL,
// 		NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'),
// 		NESTING_EVENT_LEVEL		INT(11));
func (ps *perfSchema) buildStmtsHistoryLongModel() {
	tbName := TableStmtsHistoryLong
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
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

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnStmtsHistoryLong[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.prepared_statements_instances (
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED NOT NULL,
// 		STATEMENT_ID	BIGINT(20) UNSIGNED NOT NULL,
// 		STATEMENT_NAME	VARCHAR(64),
// 		SQL_TEXT		LONGTEXT NOT NULL,
// 		OWNER_THREAD_ID	BIGINT(20) UNSIGNED NOT NULL,
// 		OWNER_EVENT_ID	BIGINT(20) UNSIGNED NOT NULL,
// 		OWNER_OBJECT_TYPE		ENUM('EVENT','FUNCTION','TABLE'),
// 		OWNER_OBJECT_SCHEMA		VARCHAR(64),
// 		OWNER_OBJECT_NAME		VARCHAR(64),
// 		TIMER_PREPARE	BIGINT(20) UNSIGNED NOT NULL,
// 		COUNT_REPREPARE	BIGINT(20) UNSIGNED NOT NULL,
// 		COUNT_EXECUTE	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL,
// 		MIN_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL,
// 		AVG_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL,
// 		MAX_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_LOCK_TIME	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_ERRORS		BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_WARNINGS	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_ROWS_AFFECTED		BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_ROWS_SENT	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_ROWS_EXAMINED		BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_CREATED_TMP_TABLES	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SELECT_FULL_JOIN	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SELECT_RANGE		BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SELECT_RANGE_CHECK	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SELECT_SCAN	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SORT_MERGE_PASSES	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SORT_RANGE	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SORT_ROWS	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SORT_SCAN	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_NO_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_NO_GOOD_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL);
func (ps *perfSchema) buildPreparedStmtsInstancesModel() {
	tbName := TablePreparedStmtsInstances
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
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

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnPreparedStmtsInstances[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.events_transactions_current (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		STATE			ENUM('ACTIVE','COMMITTED',"ROLLED BACK"),
// 		TRX_ID			BIGINT(20) UNSIGNED,
// 		GTID			VARCHAR(64),
// 		XID_FORMAT_ID	INT(11),
// 		XID_GTRID		VARCHAR(130),
// 		XID_BQUAL		VARCHAR(130),
// 		XA_STATE		VARCHAR(64),
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		ACCESS_MODE		ENUM('READ ONLY','READ WRITE'),
// 		ISOLATION_LEVEL	VARCHAR(64),
// 		AUTOCOMMIT		ENUM('YES','NO') NOT NULL,
// 		NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED,
// 		NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED,
// 		NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED,
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));
func (ps *perfSchema) buildTransCurrentModel() {
	tbName := TableTransCurrent
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
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

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnTransCurrent[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.events_transactions_history (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		STATE			ENUM('ACTIVE','COMMITTED',"ROLLED BACK"),
// 		TRX_ID			BIGINT(20) UNSIGNED,
// 		GTID			VARCHAR(64),
// 		XID_FORMAT_ID	INT(11),
// 		XID_GTRID		VARCHAR(130),
// 		XID_BQUAL		VARCHAR(130),
// 		XA_STATE		VARCHAR(64),
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		ACCESS_MODE		ENUM('READ ONLY','READ WRITE'),
// 		ISOLATION_LEVEL	VARCHAR(64),
// 		AUTOCOMMIT		ENUM('YES','NO') NOT NULL,
// 		NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED,
// 		NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED,
// 		NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED,
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));
func (ps *perfSchema) buildTransHistoryModel() {
	tbName := TableTransHistory
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
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

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnTransHistory[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.events_transactions_history_long (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		STATE			ENUM('ACTIVE','COMMITTED',"ROLLED BACK"),
// 		TRX_ID			BIGINT(20) UNSIGNED,
// 		GTID			VARCHAR(64),
// 		XID_FORMAT_ID	INT(11),
// 		XID_GTRID		VARCHAR(130),
// 		XID_BQUAL		VARCHAR(130),
// 		XA_STATE		VARCHAR(64),
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		ACCESS_MODE		ENUM('READ ONLY','READ WRITE'),
// 		ISOLATION_LEVEL	VARCHAR(64),
// 		AUTOCOMMIT		ENUM('YES','NO') NOT NULL,
// 		NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED,
// 		NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED,
// 		NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED,
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));
func (ps *perfSchema) buildTransHistoryLongModel() {
	tbName := TableTransHistoryLong
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
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

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnTransHistoryLong[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.events_stages_current (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		WORK_COMPLETED	BIGINT(20) UNSIGNED,
// 		WORK_ESTIMATED	BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));
func (ps *perfSchema) buildStagesCurrentModel() {
	tbName := TableStagesCurrent
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
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

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnStagesCurrent[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.events_stages_history (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		WORK_COMPLETED	BIGINT(20) UNSIGNED,
// 		WORK_ESTIMATED	BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));
func (ps *perfSchema) buildStagesHistoryModel() {
	tbName := TableStagesHistory
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
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

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnStagesHistory[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
}

// CREATE TABLE if not exists performance_schema.events_stages_history_long (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		WORK_COMPLETED	BIGINT(20) UNSIGNED,
// 		WORK_ESTIMATED	BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));
func (ps *perfSchema) buildStagesHistoryLongModel() {
	tbName := TableStagesHistoryLong
	table := []struct {
		tp    byte
		size  int
		flag  uint
		def   interface{}
		elems []string
	}{
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

	cols := make([]*model.ColumnInfo, len(table))
	for i, t := range table {
		var ci *model.ColumnInfo
		var rf *field.ResultField

		colName := ColumnStagesHistoryLong[i]
		if t.elems == nil {
			ci = buildUsualColumnInfo(colName, t.tp, t.size, t.flag, t.def)
		} else {
			ci = buildEnumColumnInfo(colName, t.elems, t.flag, t.def)
		}
		rf = buildResultField(tbName, ci)

		cols[i] = ci
		ps.columns[columnName{tbName, colName}] = ci
		ps.fields[columnName{tbName, colName}] = rf
	}

	ps.tables[tbName] = &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Charset: "utf8",
		Collate: "utf8",
		Columns: cols,
	}
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

func (ps *perfSchema) initSetupActorsRecords() error {
	curNo := atomic.AddUint64(&ps.setupActorsRecordNo, 1)

	table := []struct {
		Value []interface{}
	}{
		{
			[]interface{}{"%", "%", "%", "Yes", "Yes"},
		},
	}

	batch := pool.Get().(*leveldb.Batch)
	for i, t := range table {
		index := curNo + uint64(i-len(table))
		rawKey := []interface{}{uint64(index)}
		key, err := codec.EncodeKey(nil, rawKey...)
		if err != nil {
			batch.Reset()
			pool.Put(batch)
			return errors.Trace(err)
		}
		val, err := codec.EncodeValue(nil, t.Value...)
		if err != nil {
			batch.Reset()
			pool.Put(batch)
			return errors.Trace(err)
		}
		batch.Put(key, val)
	}

	err := ps.setupActorsStore.Write(batch, nil)
	batch.Reset()
	pool.Put(batch)
	return errors.Trace(err)
}

func (ps *perfSchema) initSetupObjectsRecords() error {
	curNo := atomic.AddUint64(&ps.setupObjectsRecordNo, 12)

	table := []struct {
		Value []interface{}
	}{
		{
			[]interface{}{"EVENT", "mysql", "%", "NO", "NO"},
		},

		{
			[]interface{}{"EVENT", "performance_schema", "%", "NO", "NO"},
		},

		{
			[]interface{}{"EVENT", "information_schema", "%", "NO", "NO"},
		},

		{
			[]interface{}{"EVENT", "%", "%", "YES", "YES"},
		},

		{
			[]interface{}{"FUNCTION", "mysql", "%", "NO", "NO"},
		},

		{
			[]interface{}{"FUNCTION", "performance_schema", "%", "NO", "NO"},
		},

		{
			[]interface{}{"FUNCTION", "information_schema", "%", "NO", "NO"},
		},

		{
			[]interface{}{"FUNCTION", "%", "%", "YES", "YES"},
		},

		{
			[]interface{}{"TABLE", "mysql", "%", "NO", "NO"},
		},

		{
			[]interface{}{"TABLE", "performance_schema", "%", "NO", "NO"},
		},

		{
			[]interface{}{"TABLE", "information_schema", "%", "NO", "NO"},
		},

		{
			[]interface{}{"TABLE", "%", "%", "YES", "YES"},
		},
	}

	batch := pool.Get().(*leveldb.Batch)
	for i, t := range table {
		index := curNo + uint64(i-len(table))
		rawKey := []interface{}{uint64(index)}
		key, err := codec.EncodeKey(nil, rawKey...)
		if err != nil {
			batch.Reset()
			pool.Put(batch)
			return errors.Trace(err)
		}
		val, err := codec.EncodeValue(nil, t.Value...)
		if err != nil {
			batch.Reset()
			pool.Put(batch)
			return errors.Trace(err)
		}
		batch.Put(key, val)
	}

	err := ps.setupObjectsStore.Write(batch, nil)
	batch.Reset()
	pool.Put(batch)
	return errors.Trace(err)
}

func (ps *perfSchema) initSetupInstrumentsRecords() error {
	// TODO: add instrumentation points later
	return nil
}

func (ps *perfSchema) initSetupConsumersRecords() error {
	curNo := atomic.AddUint64(&ps.setupConsumersRecordNo, 12)

	table := []struct {
		Value []interface{}
	}{
		{
			[]interface{}{"events_stages_current", "NO"},
		},

		{
			[]interface{}{"events_stages_history", "NO"},
		},

		{
			[]interface{}{"events_stages_history_long", "NO"},
		},

		{
			[]interface{}{"events_statements_current", "YES"},
		},

		{
			[]interface{}{"events_statements_history", "YES"},
		},

		{
			[]interface{}{"events_statements_history_long", "NO"},
		},

		{
			[]interface{}{"events_transactions_current", "YES"},
		},

		{
			[]interface{}{"events_transactions_history", "YES"},
		},

		{
			[]interface{}{"events_transactions_history_long", "YES"},
		},

		{
			[]interface{}{"global_instrumentation", "YES"},
		},

		{
			[]interface{}{"thread_instrumentation", "YES"},
		},

		{
			[]interface{}{"statements_digest", "YES"},
		},
	}

	batch := pool.Get().(*leveldb.Batch)
	for i, t := range table {
		index := curNo + uint64(i-len(table))
		rawKey := []interface{}{uint64(index)}
		key, err := codec.EncodeKey(nil, rawKey...)
		if err != nil {
			batch.Reset()
			pool.Put(batch)
			return errors.Trace(err)
		}
		val, err := codec.EncodeValue(nil, t.Value...)
		if err != nil {
			batch.Reset()
			pool.Put(batch)
			return errors.Trace(err)
		}
		batch.Put(key, val)
	}

	err := ps.setupConsumersStore.Write(batch, nil)
	batch.Reset()
	pool.Put(batch)
	return errors.Trace(err)
}

func (ps *perfSchema) initSetupTimersRecords() error {
	curNo := atomic.AddUint64(&ps.setupTimersRecordNo, 3)

	table := []struct {
		Value []interface{}
	}{
		{
			[]interface{}{"stage", "NANOSECOND"},
		},

		{
			[]interface{}{"statement", "NANOSECOND"},
		},

		{
			[]interface{}{"transaction", "NANOSECOND"},
		},
	}

	batch := pool.Get().(*leveldb.Batch)
	for i, t := range table {
		index := curNo + uint64(i-len(table))
		rawKey := []interface{}{uint64(index)}
		key, err := codec.EncodeKey(nil, rawKey...)
		if err != nil {
			batch.Reset()
			pool.Put(batch)
			return errors.Trace(err)
		}
		val, err := codec.EncodeValue(nil, t.Value...)
		if err != nil {
			batch.Reset()
			pool.Put(batch)
			return errors.Trace(err)
		}
		batch.Put(key, val)
	}

	err := ps.setupTimersStore.Write(batch, nil)
	batch.Reset()
	pool.Put(batch)
	return errors.Trace(err)
}

func (ps *perfSchema) initialize() (err error) {
	ps.tables = make(map[string]*model.TableInfo)
	ps.columns = make(map[columnName]*model.ColumnInfo)
	ps.fields = make(map[columnName]*field.ResultField)

	ps.setupActorsStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.setupObjectsStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.setupInstrumentsStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.setupConsumersStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.setupTimersStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.stmtsCurrentStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.stmtsHistoryStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.stmtsHistoryLongStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.preparedStmtsInstancesStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.transCurrentStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.transHistoryStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.transHistoryLongStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.stagesCurrentStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.stagesHistoryStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}
	ps.stagesHistoryLongStore, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return errors.Trace(err)
	}

	ps.setupActorsRecordNo = 0
	ps.setupObjectsRecordNo = 0
	ps.setupInstrumentsRecordNo = 0
	ps.setupConsumersRecordNo = 0
	ps.setupTimersRecordNo = 0
	ps.stmtsCurrentRecordNo = 0
	ps.stmtsHistoryRecordNo = 0
	ps.stmtsHistoryLongRecordNo = 0
	ps.preparedStmtsInstancesRecordNo = 0
	ps.transCurrentRecordNo = 0
	ps.transHistoryRecordNo = 0
	ps.transHistoryLongRecordNo = 0
	ps.stagesCurrentRecordNo = 0
	ps.stagesHistoryRecordNo = 0
	ps.stagesHistoryLongRecordNo = 0

	ps.buildSetupActorsModel()
	ps.buildSetupObjectsModel()
	ps.buildSetupInstrumentsModel()
	ps.buildSetupConsumersModel()
	ps.buildSetupTimersModel()
	ps.buildStmtsCurrentModel()
	ps.buildStmtsHistoryModel()
	ps.buildStmtsHistoryLongModel()
	ps.buildPreparedStmtsInstancesModel()
	ps.buildTransCurrentModel()
	ps.buildTransHistoryModel()
	ps.buildTransHistoryLongModel()
	ps.buildStagesCurrentModel()
	ps.buildStagesHistoryModel()
	ps.buildStagesHistoryLongModel()

	err = ps.initSetupActorsRecords()
	if err != nil {
		return errors.Trace(err)
	}
	err = ps.initSetupObjectsRecords()
	if err != nil {
		return errors.Trace(err)
	}
	err = ps.initSetupInstrumentsRecords()
	if err != nil {
		return errors.Trace(err)
	}
	err = ps.initSetupConsumersRecords()
	if err != nil {
		return errors.Trace(err)
	}
	err = ps.initSetupTimersRecords()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
