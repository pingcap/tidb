// Copyright 2015 PingCAP, Inc.
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

package plans

import (
	"fmt"
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

var _ = (*InfoSchemaPlan)(nil)

// InfoSchemaPlan handles information_schema query, simulates the behavior of
// MySQL.
type InfoSchemaPlan struct {
	TableName string
	rows      []*plan.Row
	cursor    int
}

var (
	schemataFields       []*field.ResultField
	tablesFields         []*field.ResultField
	columnsFields        []*field.ResultField
	statisticsFields     []*field.ResultField
	characterSetsFields  []*field.ResultField
	collationsFields     []*field.ResultField
	filesFields          []*field.ResultField
	profilingFields      []*field.ResultField
	characterSetsRecords [][]interface{}
	collationsRecords    [][]interface{}
	filesRecords         [][]interface{}
)

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

// NewInfoSchemaPlan returns new InfoSchemaPlan instance, and checks if the
// given table name is valid.
func NewInfoSchemaPlan(tableName string) (isp *InfoSchemaPlan, err error) {
	switch strings.ToUpper(tableName) {
	case tableSchemata:
	case tableTables:
	case tableColumns:
	case tableStatistics:
	case tableCharacterSets:
	case tableCollations:
	case tableFiles:
	case tableProfiling:
	default:
		return nil, errors.Errorf("table INFORMATION_SCHEMA.%s does not exist", tableName)
	}
	isp = &InfoSchemaPlan{
		TableName: strings.ToUpper(tableName),
	}
	return
}

func buildResultFieldsForSchemata() (rfs []*field.ResultField) {
	tbName := tableSchemata
	rfs = append(rfs, buildResultField(tbName, "CATALOG_NAME", mysql.TypeVarchar, 512))
	rfs = append(rfs, buildResultField(tbName, "SCHEMA_NAME", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "DEFAULT_CHARACTER_SET_NAME", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "DEFAULT_COLLATION_NAME", mysql.TypeVarchar, 32))
	rfs = append(rfs, buildResultField(tbName, "SQL_PATH", mysql.TypeVarchar, 512))
	return rfs
}

func buildResultFieldsForTables() (rfs []*field.ResultField) {
	tbName := tableTables
	rfs = append(rfs, buildResultField(tbName, "TABLE_CATALOG", mysql.TypeVarchar, 512))
	rfs = append(rfs, buildResultField(tbName, "TABLE_SCHEMA", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "TABLE_NAME", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "TABLE_TYPE", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "ENGINE", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "VERSION", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "ROW_FORMAT", mysql.TypeVarchar, 10))
	rfs = append(rfs, buildResultField(tbName, "TABLE_ROWS", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "AVG_ROW_LENGTH", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "DATA_LENGTH", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "MAX_DATA_LENGTH", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "INDEX_LENGTH", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "DATA_FREE", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "AUTO_INCREMENT", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "CREATE_TIME", mysql.TypeDatetime, 19))
	rfs = append(rfs, buildResultField(tbName, "UPDATE_TIME", mysql.TypeDatetime, 19))
	rfs = append(rfs, buildResultField(tbName, "CHECK_TIME", mysql.TypeDatetime, 19))
	rfs = append(rfs, buildResultField(tbName, "TABLE_COLLATION", mysql.TypeVarchar, 32))
	rfs = append(rfs, buildResultField(tbName, "CHECK_SUM", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "CREATE_OPTIONS", mysql.TypeVarchar, 255))
	rfs = append(rfs, buildResultField(tbName, "TABLE_COMMENT", mysql.TypeVarchar, 2048))
	for i, f := range rfs {
		f.Offset = i
	}
	return
}

func buildResultFieldsForColumns() (rfs []*field.ResultField) {
	tbName := tableColumns
	rfs = append(rfs, buildResultField(tbName, "TABLE_CATALOG", mysql.TypeVarchar, 512))
	rfs = append(rfs, buildResultField(tbName, "TABLE_SCHEMA", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "TABLE_NAME", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "COLUMN_NAME", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "ORIGINAL_POSITION", mysql.TypeLonglong, 64))
	rfs = append(rfs, buildResultField(tbName, "COLUMN_DEFAULT", mysql.TypeBlob, 196606))
	rfs = append(rfs, buildResultField(tbName, "IS_NULLABLE", mysql.TypeVarchar, 3))
	rfs = append(rfs, buildResultField(tbName, "DATA_TYPE", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "CHARACTER_MAXIMUM_LENGTH", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "CHARACTOR_OCTET_LENGTH", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "NUMERIC_PRECISION", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "NUMERIC_SCALE", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "DATETIME_PRECISION", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "CHARACTER_SET_NAME", mysql.TypeVarchar, 32))
	rfs = append(rfs, buildResultField(tbName, "COLLATION_NAME", mysql.TypeVarchar, 32))
	rfs = append(rfs, buildResultField(tbName, "COLUMN_TYPE", mysql.TypeBlob, 196606))
	rfs = append(rfs, buildResultField(tbName, "COLUMN_KEY", mysql.TypeVarchar, 3))
	rfs = append(rfs, buildResultField(tbName, "EXTRA", mysql.TypeVarchar, 30))
	rfs = append(rfs, buildResultField(tbName, "PRIVILEGES", mysql.TypeVarchar, 80))
	rfs = append(rfs, buildResultField(tbName, "COLUMN_COMMENT", mysql.TypeVarchar, 1024))
	for i, f := range rfs {
		f.Offset = i
	}
	return
}

func buildResultFieldsForStatistics() (rfs []*field.ResultField) {
	tbName := tableStatistics
	rfs = append(rfs, buildResultField(tbName, "TABLE_CATALOG", mysql.TypeVarchar, 512))
	rfs = append(rfs, buildResultField(tbName, "TABLE_SCHEMA", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "TABLE_NAME", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "NON_UNIQUE", mysql.TypeVarchar, 1))
	rfs = append(rfs, buildResultField(tbName, "INDEX_SCHEMA", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "INDEX_NAME", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "SEQ_IN_INDEX", mysql.TypeLonglong, 2))
	rfs = append(rfs, buildResultField(tbName, "COLUMN_NAME", mysql.TypeVarchar, 21))
	rfs = append(rfs, buildResultField(tbName, "COLLATION", mysql.TypeVarchar, 1))
	rfs = append(rfs, buildResultField(tbName, "CARDINALITY", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "SUB_PART", mysql.TypeLonglong, 3))
	rfs = append(rfs, buildResultField(tbName, "PACKED", mysql.TypeVarchar, 10))
	rfs = append(rfs, buildResultField(tbName, "NULLABLE", mysql.TypeVarchar, 3))
	rfs = append(rfs, buildResultField(tbName, "INDEX_TYPE", mysql.TypeVarchar, 16))
	rfs = append(rfs, buildResultField(tbName, "COMMENT", mysql.TypeVarchar, 16))
	rfs = append(rfs, buildResultField(tbName, "INDEX_COMMENT", mysql.TypeVarchar, 1024))
	for i, f := range rfs {
		f.Offset = i
	}
	return
}

func buildResultFieldsForProfiling() (rfs []*field.ResultField) {
	tbName := tableProfiling
	rfs = append(rfs, buildResultField(tbName, "QUERY_ID", mysql.TypeLong, 20))
	rfs = append(rfs, buildResultField(tbName, "SEQ", mysql.TypeLong, 20))
	rfs = append(rfs, buildResultField(tbName, "STATE", mysql.TypeVarchar, 30))
	rfs = append(rfs, buildResultField(tbName, "DURATION", mysql.TypeNewDecimal, 9))
	rfs = append(rfs, buildResultField(tbName, "CPU_USER", mysql.TypeNewDecimal, 9))
	rfs = append(rfs, buildResultField(tbName, "CPU_SYSTEM", mysql.TypeNewDecimal, 9))
	rfs = append(rfs, buildResultField(tbName, "CONTEXT_VOLUNTARY", mysql.TypeLong, 20))
	rfs = append(rfs, buildResultField(tbName, "CONTEXT_INVOLUNTARY", mysql.TypeLong, 20))
	rfs = append(rfs, buildResultField(tbName, "BLOCK_OPS_IN", mysql.TypeLong, 20))
	rfs = append(rfs, buildResultField(tbName, "BLOCK_OPS_OUT", mysql.TypeLong, 20))
	rfs = append(rfs, buildResultField(tbName, "MESSAGES_SENT", mysql.TypeLong, 20))
	rfs = append(rfs, buildResultField(tbName, "MESSAGES_RECEIVED", mysql.TypeLong, 20))
	rfs = append(rfs, buildResultField(tbName, "PAGE_FAULTS_MAJOR", mysql.TypeLong, 20))
	rfs = append(rfs, buildResultField(tbName, "PAGE_FAULTS_MINOR", mysql.TypeLong, 20))
	rfs = append(rfs, buildResultField(tbName, "SWAPS", mysql.TypeLong, 20))
	rfs = append(rfs, buildResultField(tbName, "SOURCE_FUNCTION", mysql.TypeVarchar, 30))
	rfs = append(rfs, buildResultField(tbName, "SOURCE_FILE", mysql.TypeVarchar, 20))
	rfs = append(rfs, buildResultField(tbName, "SOURCE_LINE", mysql.TypeLong, 20))
	for i, f := range rfs {
		f.Offset = i
	}
	return
}

func buildResultFieldsForCharacterSets() (rfs []*field.ResultField) {
	tbName := tableCharacterSets
	rfs = append(rfs, buildResultField(tbName, "CHARACTER_SET_NAME", mysql.TypeVarchar, 32))
	rfs = append(rfs, buildResultField(tbName, "DEFAULT_COLLATE_NAME", mysql.TypeVarchar, 32))
	rfs = append(rfs, buildResultField(tbName, "DESCRIPTION", mysql.TypeVarchar, 60))
	rfs = append(rfs, buildResultField(tbName, "MAXLEN", mysql.TypeLonglong, 3))
	return rfs
}

func buildCharacterSetsRecords() (records [][]interface{}) {
	records = append(records,
		[]interface{}{"ascii", "ascii_general_ci", "US ASCII", 1},
		[]interface{}{"binary", "binary", "Binary pseudo charset", 1},
		[]interface{}{"latin1", "latin1_swedish_ci", "cp1252 West European", 1},
		[]interface{}{"utf8", "utf8_general_ci", "UTF-8 Unicode", 3},
		[]interface{}{"utf8mb4", "utf8mb4_general_ci", "UTF-8 Unicode", 4},
	)
	return records
}

func buildResultFieldsForCollations() (rfs []*field.ResultField) {
	tbName := tableCollations
	rfs = append(rfs, buildResultField(tbName, "COLLATION_NAME", mysql.TypeVarchar, 32))
	rfs = append(rfs, buildResultField(tbName, "CHARACTER_SET_NAME", mysql.TypeVarchar, 32))
	rfs = append(rfs, buildResultField(tbName, "ID", mysql.TypeLonglong, 11))
	rfs = append(rfs, buildResultField(tbName, "IS_DEFAULT", mysql.TypeVarchar, 3))
	rfs = append(rfs, buildResultField(tbName, "IS_COMPILED", mysql.TypeVarchar, 3))
	rfs = append(rfs, buildResultField(tbName, "SORTLEN", mysql.TypeLonglong, 3))
	return rfs
}

func buildColltionsRecords() (records [][]interface{}) {
	records = append(records,
		[]interface{}{"ascii_general_ci", "ascii", 1, "Yes", "Yes", 1},
		[]interface{}{"binary", "binary", 2, "Yes", "Yes", 1},
		[]interface{}{"latin1_swedish_ci", "latin1", 3, "Yes", "Yes", 1},
		[]interface{}{"utf8_general_ci", "utf8", 4, "Yes", "Yes", 1},
		[]interface{}{"utf8mb4_general_ci", "utf8mb4", 5, "Yes", "Yes", 1},
	)
	return records
}

func buildFilesRecords() (records [][]interface{}) {
	// We do not have files, so we return empty records.
	return records
}

func buildFilesFields() (rfs []*field.ResultField) {
	tbName := tableFiles
	rfs = append(rfs, buildResultField(tbName, "FILE_ID", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField(tbName, "FILE_NAME", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "TABLESPACE_NAME", mysql.TypeVarchar, 20))
	rfs = append(rfs, buildResultField(tbName, "TABLE_CATALOG", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "TABLE_SCHEMA", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "TABLE_NAME", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "LOGFILE_GROUP_NAME", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "LOGFILE_GROUP_NUMBER", mysql.TypeLonglong, 32))
	rfs = append(rfs, buildResultField(tbName, "ENGINE", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "FULLTEXT_KEYS", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField(tbName, "DELETED_ROWS", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField(tbName, "UPDATE_COUNT", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField(tbName, "FREE_EXTENTS", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField(tbName, "TOTAL_EXTENTS", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField(tbName, "EXTENT_SIZE", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField(tbName, "INITIAL_SIZE", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "MAXIMUM_SIZE", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "AUTOEXTEND_SIZE", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "CREATION_TIME", mysql.TypeDatetime, -1))
	rfs = append(rfs, buildResultField(tbName, "LAST_UPDATE_TIME", mysql.TypeDatetime, -1))
	rfs = append(rfs, buildResultField(tbName, "LAST_ACCESS_TIME", mysql.TypeDatetime, -1))
	rfs = append(rfs, buildResultField(tbName, "RECOVER_TIME", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField(tbName, "TRANSACTION_COUNTER", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField(tbName, "VERSION", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "ROW_FORMAT", mysql.TypeVarchar, 21))
	rfs = append(rfs, buildResultField(tbName, "TABLE_ROWS", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "AVG_ROW_LENGTH", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "DATA_FREE", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "CREATE_TIME", mysql.TypeDatetime, -1))
	rfs = append(rfs, buildResultField(tbName, "UPDATE_TIME", mysql.TypeDatetime, -1))
	rfs = append(rfs, buildResultField(tbName, "CHECK_TIME", mysql.TypeDatetime, -1))
	rfs = append(rfs, buildResultField(tbName, "CHECKSUM", mysql.TypeLonglong, 21))
	rfs = append(rfs, buildResultField(tbName, "STATUS", mysql.TypeVarchar, 20))
	rfs = append(rfs, buildResultField(tbName, "EXTRA", mysql.TypeVarchar, 255))
	return rfs
}

// Explain implements plan.Plan Explain interface.
func (isp *InfoSchemaPlan) Explain(w format.Formatter) {}

// Filter implements plan.Plan Filter interface.
func (isp *InfoSchemaPlan) Filter(ctx context.Context, expr expression.Expression) (p plan.Plan, filtered bool, err error) {
	return isp, false, nil
}

// GetFields implements plan.Plan GetFields interface, simulates MySQL's output.
func (isp *InfoSchemaPlan) GetFields() []*field.ResultField {
	switch isp.TableName {
	case tableSchemata:
		return schemataFields
	case tableTables:
		return tablesFields
	case tableColumns:
		return columnsFields
	case tableStatistics:
		return statisticsFields
	case tableCharacterSets:
		return characterSetsFields
	case tableCollations:
		return collationsFields
	case tableFiles:
		return filesFields
	case tableProfiling:
		return profilingFields
	}
	return nil
}

func buildResultField(tableName, name string, tp byte, size int) *field.ResultField {
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
	colInfo := model.ColumnInfo{
		Name:      model.NewCIStr(name),
		FieldType: fieldType,
	}
	field := &field.ResultField{
		Col:       column.Col{ColumnInfo: colInfo},
		DBName:    infoschema.Name,
		TableName: tableName,
		Name:      colInfo.Name.O,
	}
	return field
}

// Next implements plan.Plan Next interface.
func (isp *InfoSchemaPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if isp.rows == nil {
		isp.fetchAll(ctx)
	}
	if isp.cursor == len(isp.rows) {
		return
	}
	row = isp.rows[isp.cursor]
	isp.cursor++
	return
}

func (isp *InfoSchemaPlan) fetchAll(ctx context.Context) {
	is := sessionctx.GetDomain(ctx).InfoSchema()
	schemas := is.AllSchemas()
	switch isp.TableName {
	case tableSchemata:
		isp.fetchSchemata(is.AllSchemaNames())
	case tableTables:
		isp.fetchTables(schemas)
	case tableColumns:
		isp.fetchColumns(schemas)
	case tableStatistics:
		isp.fetchStatistics(is, schemas)
	case tableCharacterSets:
		isp.fetchCharacterSets()
	case tableCollations:
		isp.fetchCollations()
	case tableFiles:
		isp.fetchFiles()
	}
}

func (isp *InfoSchemaPlan) fetchSchemata(schemas []string) {
	sort.Strings(schemas)
	for _, schema := range schemas {
		record := []interface{}{
			catalogVal,                 // CATALOG_NAME
			schema,                     // SCHEMA_NAME
			mysql.DefaultCharset,       // DEFAULT_CHARACTER_SET_NAME
			mysql.DefaultCollationName, // DEFAULT_COLLATION_NAME
			nil,
		}
		isp.rows = append(isp.rows, &plan.Row{Data: record})
	}
}

func (isp *InfoSchemaPlan) fetchTables(schemas []*model.DBInfo) {
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
			isp.rows = append(isp.rows, &plan.Row{Data: record})
		}
	}
}

func (isp *InfoSchemaPlan) fetchColumns(schemas []*model.DBInfo) {
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			isp.fetchColumnsInTable(schema, table)
		}
	}
}

func (isp *InfoSchemaPlan) fetchColumnsInTable(schema *model.DBInfo, table *model.TableInfo) {
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
		isp.rows = append(isp.rows, &plan.Row{Data: record})
	}
}

func (isp *InfoSchemaPlan) fetchStatistics(is infoschema.InfoSchema, schemas []*model.DBInfo) {
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			isp.fetchStatisticsInTable(is, schema, table)
		}
	}
}

func (isp *InfoSchemaPlan) fetchStatisticsInTable(is infoschema.InfoSchema, schema *model.DBInfo, table *model.TableInfo) {
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
				isp.rows = append(isp.rows, &plan.Row{Data: record})
			}
		}
	}
	for _, index := range table.Indices {
		nonUnique := "1"
		if index.Unique {
			nonUnique = "0"
		}
		for i, key := range index.Columns {
			col, _ := is.ColumnByName(schema.Name, table.Name, key.Name)
			nullable := "YES"
			if mysql.HasNotNullFlag(col.Flag) {
				nullable = ""
			}
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
			isp.rows = append(isp.rows, &plan.Row{Data: record})
		}
	}
}

func (isp *InfoSchemaPlan) fetchCharacterSets() {
	for _, record := range characterSetsRecords {
		isp.rows = append(isp.rows, &plan.Row{Data: record})
	}
}

func (isp *InfoSchemaPlan) fetchCollations() error {
	for _, record := range collationsRecords {
		isp.rows = append(isp.rows, &plan.Row{Data: record})
	}
	return nil
}

func (isp *InfoSchemaPlan) fetchFiles() error {
	for _, record := range filesRecords {
		isp.rows = append(isp.rows, &plan.Row{Data: record})
	}
	return nil
}

// Close implements plan.Plan Close interface.
func (isp *InfoSchemaPlan) Close() error {
	isp.rows = nil
	isp.cursor = 0
	return nil
}

func init() {
	schemataFields = buildResultFieldsForSchemata()
	tablesFields = buildResultFieldsForTables()
	columnsFields = buildResultFieldsForColumns()
	statisticsFields = buildResultFieldsForStatistics()
	characterSetsFields = buildResultFieldsForCharacterSets()
	collationsFields = buildResultFieldsForCollations()
	filesFields = buildFilesFields()
	characterSetsRecords = buildCharacterSetsRecords()
	collationsRecords = buildColltionsRecords()
	filesRecords = buildFilesRecords()
	profilingFields = buildResultFieldsForProfiling()
}
