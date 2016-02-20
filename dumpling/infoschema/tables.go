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

package infoschema

import (
	"fmt"
	"sort"

	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

/*
 * 1. Create tables for Information_Schema
 * 2. Fill data for each table.
 */
const (
	tableSchemata      = "SCHEMATA"
	tableTables        = "TABLES"
	tableColumns       = "COLUMNS"
	tableStatistics    = "STATISTICS"
	tableCharacterSets = "CHARACTER_SETS"
	tableCollations    = "COLLATIONS"
	tableFiles         = "FILES"
	catalogVal         = "def"
	tableProfiling     = "PROFILING"
)

func buildColumnInfo(tableName, name string, tp byte, size int) *model.ColumnInfo {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	mFlag := mysql.UnsignedFlag
	if tp == mysql.TypeVarchar || tp == mysql.TypeBlob {
		mCharset = mysql.DefaultCharset
		mCollation = mysql.DefaultCollationName
		mFlag = 0
	}
	fieldType := types.FieldType{
		Charset: mCharset,
		Collate: mCollation,
		Tp:      tp,
		Flen:    size,
		Flag:    uint(mFlag),
	}
	return &model.ColumnInfo{
		Name:      model.NewCIStr(name),
		FieldType: fieldType,
	}
}

func metaForSchemata() *model.TableInfo {
	cols := make([]*model.ColumnInfo, 0, 5)
	tbName := tableSchemata
	cols = append(cols, buildColumnInfo(tbName, "CATALOG_NAME", mysql.TypeVarchar, 512))
	cols = append(cols, buildColumnInfo(tbName, "SCHEMA_NAME", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "DEFAULT_CHARACTER_SET_NAME", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "DEFAULT_COLLATION_NAME", mysql.TypeVarchar, 32))
	cols = append(cols, buildColumnInfo(tbName, "SQL_PATH", mysql.TypeVarchar, 512))
	for i, col := range cols {
		col.Offset = i
	}
	return &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Columns: cols,
		State:   model.StatePublic,
	}
}

func metaForTables() *model.TableInfo {
	tbName := tableTables
	cols := make([]*model.ColumnInfo, 0, 22)
	cols = append(cols, buildColumnInfo(tbName, "TABLE_CATALOG", mysql.TypeVarchar, 512))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_SCHEMA", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_NAME", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_TYPE", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "ENGINE", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "VERSION", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "ROW_FORMAT", mysql.TypeVarchar, 10))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_ROWS", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "AVG_ROW_LENGTH", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "DATA_LENGTH", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "MAX_DATA_LENGTH", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "INDEX_LENGTH", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "DATA_FREE", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "AUTO_INCREMENT", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "CREATE_TIME", mysql.TypeDatetime, 19))
	cols = append(cols, buildColumnInfo(tbName, "UPDATE_TIME", mysql.TypeDatetime, 19))
	cols = append(cols, buildColumnInfo(tbName, "CHECK_TIME", mysql.TypeDatetime, 19))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_COLLATION", mysql.TypeVarchar, 32))
	cols = append(cols, buildColumnInfo(tbName, "CHECK_SUM", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "CREATE_OPTIONS", mysql.TypeVarchar, 255))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_COMMENT", mysql.TypeVarchar, 2048))
	for i, col := range cols {
		col.Offset = i
	}
	return &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Columns: cols,
		State:   model.StatePublic,
	}
}

func metaForColumns() *model.TableInfo {
	tbName := tableColumns
	cols := make([]*model.ColumnInfo, 0, 21)
	cols = append(cols, buildColumnInfo(tbName, "TABLE_CATALOG", mysql.TypeVarchar, 512))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_SCHEMA", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_NAME", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "COLUMN_NAME", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "ORIGINAL_POSITION", mysql.TypeLonglong, 64))
	cols = append(cols, buildColumnInfo(tbName, "COLUMN_DEFAULT", mysql.TypeBlob, 196606))
	cols = append(cols, buildColumnInfo(tbName, "IS_NULLABLE", mysql.TypeVarchar, 3))
	cols = append(cols, buildColumnInfo(tbName, "DATA_TYPE", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "CHARACTER_MAXIMUM_LENGTH", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "CHARACTOR_OCTET_LENGTH", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "NUMERIC_PRECISION", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "NUMERIC_SCALE", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "DATETIME_PRECISION", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "CHARACTER_SET_NAME", mysql.TypeVarchar, 32))
	cols = append(cols, buildColumnInfo(tbName, "COLLATION_NAME", mysql.TypeVarchar, 32))
	cols = append(cols, buildColumnInfo(tbName, "COLUMN_TYPE", mysql.TypeBlob, 196606))
	cols = append(cols, buildColumnInfo(tbName, "COLUMN_KEY", mysql.TypeVarchar, 3))
	cols = append(cols, buildColumnInfo(tbName, "EXTRA", mysql.TypeVarchar, 30))
	cols = append(cols, buildColumnInfo(tbName, "PRIVILEGES", mysql.TypeVarchar, 80))
	cols = append(cols, buildColumnInfo(tbName, "COLUMN_COMMENT", mysql.TypeVarchar, 1024))
	for i, col := range cols {
		col.Offset = i
	}
	return &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Columns: cols,
		State:   model.StatePublic,
	}
}

func metaForStatistics() *model.TableInfo {
	tbName := tableStatistics
	cols := make([]*model.ColumnInfo, 0, 16)
	cols = append(cols, buildColumnInfo(tbName, "TABLE_CATALOG", mysql.TypeVarchar, 512))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_SCHEMA", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_NAME", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "NON_UNIQUE", mysql.TypeVarchar, 1))
	cols = append(cols, buildColumnInfo(tbName, "INDEX_SCHEMA", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "INDEX_NAME", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "SEQ_IN_INDEX", mysql.TypeLonglong, 2))
	cols = append(cols, buildColumnInfo(tbName, "COLUMN_NAME", mysql.TypeVarchar, 21))
	cols = append(cols, buildColumnInfo(tbName, "COLLATION", mysql.TypeVarchar, 1))
	cols = append(cols, buildColumnInfo(tbName, "CARDINALITY", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "SUB_PART", mysql.TypeLonglong, 3))
	cols = append(cols, buildColumnInfo(tbName, "PACKED", mysql.TypeVarchar, 10))
	cols = append(cols, buildColumnInfo(tbName, "NULLABLE", mysql.TypeVarchar, 3))
	cols = append(cols, buildColumnInfo(tbName, "INDEX_TYPE", mysql.TypeVarchar, 16))
	cols = append(cols, buildColumnInfo(tbName, "COMMENT", mysql.TypeVarchar, 16))
	cols = append(cols, buildColumnInfo(tbName, "INDEX_COMMENT", mysql.TypeVarchar, 1024))
	for i, col := range cols {
		col.Offset = i
	}
	return &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Columns: cols,
		State:   model.StatePublic,
	}
}

func metaForProfiling() *model.TableInfo {
	tbName := tableProfiling
	cols := make([]*model.ColumnInfo, 0, 18)
	cols = append(cols, buildColumnInfo(tbName, "QUERY_ID", mysql.TypeLong, 20))
	cols = append(cols, buildColumnInfo(tbName, "SEQ", mysql.TypeLong, 20))
	cols = append(cols, buildColumnInfo(tbName, "STATE", mysql.TypeVarchar, 30))
	cols = append(cols, buildColumnInfo(tbName, "DURATION", mysql.TypeNewDecimal, 9))
	cols = append(cols, buildColumnInfo(tbName, "CPU_USER", mysql.TypeNewDecimal, 9))
	cols = append(cols, buildColumnInfo(tbName, "CPU_SYSTEM", mysql.TypeNewDecimal, 9))
	cols = append(cols, buildColumnInfo(tbName, "CONTEXT_VOLUNTARY", mysql.TypeLong, 20))
	cols = append(cols, buildColumnInfo(tbName, "CONTEXT_INVOLUNTARY", mysql.TypeLong, 20))
	cols = append(cols, buildColumnInfo(tbName, "BLOCK_OPS_IN", mysql.TypeLong, 20))
	cols = append(cols, buildColumnInfo(tbName, "BLOCK_OPS_OUT", mysql.TypeLong, 20))
	cols = append(cols, buildColumnInfo(tbName, "MESSAGES_SENT", mysql.TypeLong, 20))
	cols = append(cols, buildColumnInfo(tbName, "MESSAGES_RECEIVED", mysql.TypeLong, 20))
	cols = append(cols, buildColumnInfo(tbName, "PAGE_FAULTS_MAJOR", mysql.TypeLong, 20))
	cols = append(cols, buildColumnInfo(tbName, "PAGE_FAULTS_MINOR", mysql.TypeLong, 20))
	cols = append(cols, buildColumnInfo(tbName, "SWAPS", mysql.TypeLong, 20))
	cols = append(cols, buildColumnInfo(tbName, "SOURCE_FUNCTION", mysql.TypeVarchar, 30))
	cols = append(cols, buildColumnInfo(tbName, "SOURCE_FILE", mysql.TypeVarchar, 20))
	cols = append(cols, buildColumnInfo(tbName, "SOURCE_LINE", mysql.TypeLong, 20))
	for i, col := range cols {
		col.Offset = i
	}
	return &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Columns: cols,
		State:   model.StatePublic,
	}
}

func metaForCharacterSets() *model.TableInfo {
	tbName := tableCharacterSets
	cols := make([]*model.ColumnInfo, 0, 4)
	cols = append(cols, buildColumnInfo(tbName, "CHARACTER_SET_NAME", mysql.TypeVarchar, 32))
	cols = append(cols, buildColumnInfo(tbName, "DEFAULT_COLLATE_NAME", mysql.TypeVarchar, 32))
	cols = append(cols, buildColumnInfo(tbName, "DESCRIPTION", mysql.TypeVarchar, 60))
	cols = append(cols, buildColumnInfo(tbName, "MAXLEN", mysql.TypeLonglong, 3))
	for i, col := range cols {
		col.Offset = i
	}
	return &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Columns: cols,
		State:   model.StatePublic,
	}
}

func dataForCharacterSets() (records [][]interface{}) {
	records = append(records,
		[]interface{}{"ascii", "ascii_general_ci", "US ASCII", 1},
		[]interface{}{"binary", "binary", "Binary pseudo charset", 1},
		[]interface{}{"latin1", "latin1_swedish_ci", "cp1252 West European", 1},
		[]interface{}{"utf8", "utf8_general_ci", "UTF-8 Unicode", 3},
		[]interface{}{"utf8mb4", "utf8mb4_general_ci", "UTF-8 Unicode", 4},
	)
	return records
}

func metaForCollations() *model.TableInfo {
	tbName := tableCollations
	cols := make([]*model.ColumnInfo, 0, 6)
	cols = append(cols, buildColumnInfo(tbName, "COLLATION_NAME", mysql.TypeVarchar, 32))
	cols = append(cols, buildColumnInfo(tbName, "CHARACTER_SET_NAME", mysql.TypeVarchar, 32))
	cols = append(cols, buildColumnInfo(tbName, "ID", mysql.TypeLonglong, 11))
	cols = append(cols, buildColumnInfo(tbName, "IS_DEFAULT", mysql.TypeVarchar, 3))
	cols = append(cols, buildColumnInfo(tbName, "IS_COMPILED", mysql.TypeVarchar, 3))
	cols = append(cols, buildColumnInfo(tbName, "SORTLEN", mysql.TypeLonglong, 3))
	for i, col := range cols {
		col.Offset = i
	}
	return &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Columns: cols,
		State:   model.StatePublic,
	}
}

func dataForColltions() (records [][]interface{}) {
	records = append(records,
		[]interface{}{"ascii_general_ci", "ascii", 1, "Yes", "Yes", 1},
		[]interface{}{"binary", "binary", 2, "Yes", "Yes", 1},
		[]interface{}{"latin1_swedish_ci", "latin1", 3, "Yes", "Yes", 1},
		[]interface{}{"utf8_general_ci", "utf8", 4, "Yes", "Yes", 1},
		[]interface{}{"utf8mb4_general_ci", "utf8mb4", 5, "Yes", "Yes", 1},
	)
	return records
}

func dataForFiles() (records [][]interface{}) {
	// We do not have files, so we return empty records.
	return records
}

func metaForFiles() *model.TableInfo {
	tbName := tableFiles
	cols := make([]*model.ColumnInfo, 0, 34)
	cols = append(cols, buildColumnInfo(tbName, "FILE_ID", mysql.TypeLonglong, 4))
	cols = append(cols, buildColumnInfo(tbName, "FILE_NAME", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "TABLESPACE_NAME", mysql.TypeVarchar, 20))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_CATALOG", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_SCHEMA", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_NAME", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "LOGFILE_GROUP_NAME", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "LOGFILE_GROUP_NUMBER", mysql.TypeLonglong, 32))
	cols = append(cols, buildColumnInfo(tbName, "ENGINE", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "FULLTEXT_KEYS", mysql.TypeVarchar, 64))
	cols = append(cols, buildColumnInfo(tbName, "DELETED_ROWS", mysql.TypeLonglong, 4))
	cols = append(cols, buildColumnInfo(tbName, "UPDATE_COUNT", mysql.TypeLonglong, 4))
	cols = append(cols, buildColumnInfo(tbName, "FREE_EXTENTS", mysql.TypeLonglong, 4))
	cols = append(cols, buildColumnInfo(tbName, "TOTAL_EXTENTS", mysql.TypeLonglong, 4))
	cols = append(cols, buildColumnInfo(tbName, "EXTENT_SIZE", mysql.TypeLonglong, 4))
	cols = append(cols, buildColumnInfo(tbName, "INITIAL_SIZE", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "MAXIMUM_SIZE", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "AUTOEXTEND_SIZE", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "CREATION_TIME", mysql.TypeDatetime, -1))
	cols = append(cols, buildColumnInfo(tbName, "LAST_UPDATE_TIME", mysql.TypeDatetime, -1))
	cols = append(cols, buildColumnInfo(tbName, "LAST_ACCESS_TIME", mysql.TypeDatetime, -1))
	cols = append(cols, buildColumnInfo(tbName, "RECOVER_TIME", mysql.TypeLonglong, 4))
	cols = append(cols, buildColumnInfo(tbName, "TRANSACTION_COUNTER", mysql.TypeLonglong, 4))
	cols = append(cols, buildColumnInfo(tbName, "VERSION", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "ROW_FORMAT", mysql.TypeVarchar, 21))
	cols = append(cols, buildColumnInfo(tbName, "TABLE_ROWS", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "AVG_ROW_LENGTH", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "DATA_FREE", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "CREATE_TIME", mysql.TypeDatetime, -1))
	cols = append(cols, buildColumnInfo(tbName, "UPDATE_TIME", mysql.TypeDatetime, -1))
	cols = append(cols, buildColumnInfo(tbName, "CHECK_TIME", mysql.TypeDatetime, -1))
	cols = append(cols, buildColumnInfo(tbName, "CHECKSUM", mysql.TypeLonglong, 21))
	cols = append(cols, buildColumnInfo(tbName, "STATUS", mysql.TypeVarchar, 20))
	cols = append(cols, buildColumnInfo(tbName, "EXTRA", mysql.TypeVarchar, 255))
	for i, col := range cols {
		col.Offset = i
	}
	return &model.TableInfo{
		Name:    model.NewCIStr(tbName),
		Columns: cols,
		State:   model.StatePublic,
	}
}

func dataForSchemata(schemas []string) [][]interface{} {
	sort.Strings(schemas)
	rows := [][]interface{}{}
	for _, schema := range schemas {
		record := []interface{}{
			catalogVal,                 // CATALOG_NAME
			schema,                     // SCHEMA_NAME
			mysql.DefaultCharset,       // DEFAULT_CHARACTER_SET_NAME
			mysql.DefaultCollationName, // DEFAULT_COLLATION_NAME
			nil,
		}
		rows = append(rows, record)
	}
	return rows
}

func dataForTables(schemas []*model.DBInfo) [][]interface{} {
	rows := [][]interface{}{}
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			record := []interface{}{
				catalogVal,          // TABLE_CATALOG
				schema.Name.O,       // TABLE_SCHEMA
				table.Name.O,        // TABLE_NAME
				"BASE_TABLE",        // TABLE_TYPE
				"InnoDB",            // ENGINE
				uint64(10),          // VERSION
				"Compact",           // ROW_FORMAT
				uint64(0),           // TABLE_ROWS
				uint64(0),           // AVG_ROW_LENGTH
				uint64(16384),       // DATA_LENGTH
				uint64(0),           // MAX_DATA_LENGTH
				uint64(0),           // INDEX_LENGTH
				uint64(0),           // DATA_FREE
				nil,                 // AUTO_INCREMENT
				nil,                 // CREATE_TIME
				nil,                 // UPDATE_TIME
				nil,                 // CHECK_TIME
				"latin1_swedish_ci", // TABLE_COLLATION
				nil,                 // CHECKSUM
				"",                  // CREATE_OPTIONS
				"",                  // TABLE_COMMENT
			}
			rows = append(rows, record)
		}
	}
	return rows
}

func dataForColumns(schemas []*model.DBInfo) [][]interface{} {
	rows := [][]interface{}{}
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			rs := fetchColumnsInTable(schema, table)
			for _, r := range rs {
				rows = append(rows, r)
			}
		}
	}
	return rows
}

func fetchColumnsInTable(schema *model.DBInfo, table *model.TableInfo) [][]interface{} {
	rows := [][]interface{}{}
	for i, col := range table.Columns {
		colLen := col.Flen
		if colLen == types.UnspecifiedLength {
			colLen = mysql.GetDefaultFieldLength(col.Tp)
		}
		decimal := col.Decimal
		if decimal == types.UnspecifiedLength {
			decimal = 0
		}
		columnType := col.FieldType.CompactStr()
		columnDesc := column.NewColDesc(&column.Col{ColumnInfo: *col})
		var columnDefault interface{}
		if columnDesc.DefaultValue != nil {
			columnDefault = fmt.Sprintf("%v", columnDesc.DefaultValue)
		}
		record := []interface{}{
			catalogVal,                           // TABLE_CATALOG
			schema.Name.O,                        // TABLE_SCHEMA
			table.Name.O,                         // TABLE_NAME
			col.Name.O,                           // COLUMN_NAME
			i + 1,                                // ORIGINAL_POSITION
			columnDefault,                        // COLUMN_DEFAULT
			columnDesc.Null,                      // IS_NULLABLE
			types.TypeToStr(col.Tp, col.Charset), // DATA_TYPE
			colLen,                            // CHARACTER_MAXIMUM_LENGTH
			colLen,                            // CHARACTOR_OCTET_LENGTH
			decimal,                           // NUMERIC_PRECISION
			0,                                 // NUMERIC_SCALE
			0,                                 // DATETIME_PRECISION
			col.Charset,                       // CHARACTER_SET_NAME
			col.Collate,                       // COLLATION_NAME
			columnType,                        // COLUMN_TYPE
			columnDesc.Key,                    // COLUMN_KEY
			columnDesc.Extra,                  // EXTRA
			"select,insert,update,references", // PRIVILEGES
			"", // COLUMN_COMMENT
		}
		rows = append(rows, record)
	}
	return rows
}

func dataForStatistics(schemas []*model.DBInfo) [][]interface{} {
	rows := [][]interface{}{}
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			rs := fetchStatisticsInTable(schema, table)
			for _, r := range rs {
				rows = append(rows, r)
			}
		}
	}
	return rows
}

func fetchStatisticsInTable(schema *model.DBInfo, table *model.TableInfo) [][]interface{} {
	rows := [][]interface{}{}
	if table.PKIsHandle {
		for _, col := range table.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				record := []interface{}{
					catalogVal,    // TABLE_CATALOG
					schema.Name.O, // TABLE_SCHEMA
					table.Name.O,  // TABLE_NAME
					"0",           // NON_UNIQUE
					schema.Name.O, // INDEX_SCHEMA
					"PRIMARY",     // INDEX_NAME
					1,             // SEQ_IN_INDEX
					col.Name.O,    // COLUMN_NAME
					"A",           // COLLATION
					0,             // CARDINALITY
					nil,           // SUB_PART
					nil,           // PACKED
					"",            // NULLABLE
					"BTREE",       // INDEX_TYPE
					"",            // COMMENT
					"",            // INDEX_COMMENT
				}
				rows = append(rows, record)
			}
		}
	}
	for _, index := range table.Indices {
		nonUnique := "1"
		if index.Unique {
			nonUnique = "0"
		}
		for i, key := range index.Columns {
			//col, _ := is.ColumnByName(schema.Name, table.Name, key.Name)
			//var col *model.ColumnInfo
			// TODO: this above code
			nullable := "YES"
			//if mysql.HasNotNullFlag(col.Flag) {
			//	nullable = ""
			//}
			record := []interface{}{
				catalogVal,    // TABLE_CATALOG
				schema.Name.O, // TABLE_SCHEMA
				table.Name.O,  // TABLE_NAME
				nonUnique,     // NON_UNIQUE
				schema.Name.O, // INDEX_SCHEMA
				index.Name.O,  // INDEX_NAME
				i + 1,         // SEQ_IN_INDEX
				key.Name.O,    // COLUMN_NAME
				"A",           // COLLATION
				0,             // CARDINALITY
				nil,           // SUB_PART
				nil,           // PACKED
				nullable,      // NULLABLE
				"BTREE",       // INDEX_TYPE
				"",            // COMMENT
				"",            // INDEX_COMMENT
			}
			rows = append(rows, record)
		}
	}
	return rows
}

func createMemoryTable(meta *model.TableInfo, alloc autoid.Allocator) (table.Table, error) {
	tbl, _ := tables.MemoryTableFromMeta(alloc, meta)
	return tbl, nil
}
