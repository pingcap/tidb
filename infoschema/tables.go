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
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	binaryJson "github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	tableSchemata                           = "SCHEMATA"
	tableTables                             = "TABLES"
	tableColumns                            = "COLUMNS"
	tableStatistics                         = "STATISTICS"
	tableCharacterSets                      = "CHARACTER_SETS"
	tableCollations                         = "COLLATIONS"
	tableFiles                              = "FILES"
	catalogVal                              = "def"
	tableProfiling                          = "PROFILING"
	tablePartitions                         = "PARTITIONS"
	tableKeyColumm                          = "KEY_COLUMN_USAGE"
	tableReferConst                         = "REFERENTIAL_CONSTRAINTS"
	tableSessionVar                         = "SESSION_VARIABLES"
	tablePlugins                            = "PLUGINS"
	tableConstraints                        = "TABLE_CONSTRAINTS"
	tableTriggers                           = "TRIGGERS"
	tableUserPrivileges                     = "USER_PRIVILEGES"
	tableSchemaPrivileges                   = "SCHEMA_PRIVILEGES"
	tableTablePrivileges                    = "TABLE_PRIVILEGES"
	tableColumnPrivileges                   = "COLUMN_PRIVILEGES"
	tableEngines                            = "ENGINES"
	tableViews                              = "VIEWS"
	tableRoutines                           = "ROUTINES"
	tableParameters                         = "PARAMETERS"
	tableEvents                             = "EVENTS"
	tableGlobalStatus                       = "GLOBAL_STATUS"
	tableGlobalVariables                    = "GLOBAL_VARIABLES"
	tableSessionStatus                      = "SESSION_STATUS"
	tableOptimizerTrace                     = "OPTIMIZER_TRACE"
	tableTableSpaces                        = "TABLESPACES"
	tableCollationCharacterSetApplicability = "COLLATION_CHARACTER_SET_APPLICABILITY"
	tableProcesslist                        = "PROCESSLIST"
	tableTiDBIndexes                        = "TIDB_INDEXES"
	tableSlowLog                            = "SLOW_QUERY"
	tableTiDBHotRegions                     = "TIDB_HOT_REGIONS"
	tableTiKVStoreStatus                    = "TIKV_STORE_STATUS"
	tableAnalyzeStatus                      = "ANALYZE_STATUS"
	tableTiKVRegionStatus                   = "TIKV_REGION_STATUS"
	tableTiKVRegionPeers                    = "TIKV_REGION_PEERS"
)

type columnInfo struct {
	name  string
	tp    byte
	size  int
	flag  uint
	deflt interface{}
	elems []string
}

func buildColumnInfo(tableName string, col columnInfo) *model.ColumnInfo {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
<<<<<<< HEAD
	mFlag := mysql.UnsignedFlag
	if col.tp == mysql.TypeVarchar || col.tp == mysql.TypeBlob {
=======
	if col.tp == mysql.TypeVarchar || col.tp == mysql.TypeBlob || col.tp == mysql.TypeLongBlob {
>>>>>>> 729fdcb... infoschema: fix query processlist and cluster_log bug in mysql8 client (#19648)
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
		Name:      model.NewCIStr(col.name),
		FieldType: fieldType,
		State:     model.StatePublic,
	}
}

func buildTableMeta(tableName string, cs []columnInfo) *model.TableInfo {
	cols := make([]*model.ColumnInfo, 0, len(cs))
	for _, c := range cs {
		cols = append(cols, buildColumnInfo(tableName, c))
	}
	for i, col := range cols {
		col.Offset = i
	}
	return &model.TableInfo{
		Name:    model.NewCIStr(tableName),
		Columns: cols,
		State:   model.StatePublic,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
	}
}

var schemataCols = []columnInfo{
	{"CATALOG_NAME", mysql.TypeVarchar, 512, 0, nil, nil},
	{"SCHEMA_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DEFAULT_CHARACTER_SET_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DEFAULT_COLLATION_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"SQL_PATH", mysql.TypeVarchar, 512, 0, nil, nil},
}

var tablesCols = []columnInfo{
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ENGINE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"VERSION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"ROW_FORMAT", mysql.TypeVarchar, 10, 0, nil, nil},
	{"TABLE_ROWS", mysql.TypeLonglong, 21, 0, nil, nil},
	{"AVG_ROW_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATA_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"MAX_DATA_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"INDEX_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATA_FREE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"AUTO_INCREMENT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CREATE_TIME", mysql.TypeDatetime, 19, 0, nil, nil},
	{"UPDATE_TIME", mysql.TypeDatetime, 19, 0, nil, nil},
	{"CHECK_TIME", mysql.TypeDatetime, 19, 0, nil, nil},
	{"TABLE_COLLATION", mysql.TypeVarchar, 32, mysql.NotNullFlag, "utf8_bin", nil},
	{"CHECKSUM", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CREATE_OPTIONS", mysql.TypeVarchar, 255, 0, nil, nil},
	{"TABLE_COMMENT", mysql.TypeVarchar, 2048, 0, nil, nil},
	{"TIDB_TABLE_ID", mysql.TypeLonglong, 21, 0, nil, nil},
}

// See: http://dev.mysql.com/doc/refman/5.7/en/columns-table.html
var columnsCols = []columnInfo{
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"COLUMN_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ORDINAL_POSITION", mysql.TypeLonglong, 64, 0, nil, nil},
	{"COLUMN_DEFAULT", mysql.TypeBlob, 196606, 0, nil, nil},
	{"IS_NULLABLE", mysql.TypeVarchar, 3, 0, nil, nil},
	{"DATA_TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"CHARACTER_MAXIMUM_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CHARACTER_OCTET_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"NUMERIC_PRECISION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"NUMERIC_SCALE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATETIME_PRECISION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CHARACTER_SET_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"COLLATION_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"COLUMN_TYPE", mysql.TypeBlob, 196606, 0, nil, nil},
	{"COLUMN_KEY", mysql.TypeVarchar, 3, 0, nil, nil},
	{"EXTRA", mysql.TypeVarchar, 30, 0, nil, nil},
	{"PRIVILEGES", mysql.TypeVarchar, 80, 0, nil, nil},
	{"COLUMN_COMMENT", mysql.TypeVarchar, 1024, 0, nil, nil},
	{"GENERATION_EXPRESSION", mysql.TypeBlob, 589779, mysql.NotNullFlag, nil, nil},
}

var statisticsCols = []columnInfo{
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"NON_UNIQUE", mysql.TypeVarchar, 1, 0, nil, nil},
	{"INDEX_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"INDEX_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"SEQ_IN_INDEX", mysql.TypeLonglong, 2, 0, nil, nil},
	{"COLUMN_NAME", mysql.TypeVarchar, 21, 0, nil, nil},
	{"COLLATION", mysql.TypeVarchar, 1, 0, nil, nil},
	{"CARDINALITY", mysql.TypeLonglong, 21, 0, nil, nil},
	{"SUB_PART", mysql.TypeLonglong, 3, 0, nil, nil},
	{"PACKED", mysql.TypeVarchar, 10, 0, nil, nil},
	{"NULLABLE", mysql.TypeVarchar, 3, 0, nil, nil},
	{"INDEX_TYPE", mysql.TypeVarchar, 16, 0, nil, nil},
	{"COMMENT", mysql.TypeVarchar, 16, 0, nil, nil},
	{"INDEX_COMMENT", mysql.TypeVarchar, 1024, 0, nil, nil},
}

var profilingCols = []columnInfo{
	{"QUERY_ID", mysql.TypeLong, 20, 0, nil, nil},
	{"SEQ", mysql.TypeLong, 20, 0, nil, nil},
	{"STATE", mysql.TypeVarchar, 30, 0, nil, nil},
	{"DURATION", mysql.TypeNewDecimal, 9, 0, nil, nil},
	{"CPU_USER", mysql.TypeNewDecimal, 9, 0, nil, nil},
	{"CPU_SYSTEM", mysql.TypeNewDecimal, 9, 0, nil, nil},
	{"CONTEXT_VOLUNTARY", mysql.TypeLong, 20, 0, nil, nil},
	{"CONTEXT_INVOLUNTARY", mysql.TypeLong, 20, 0, nil, nil},
	{"BLOCK_OPS_IN", mysql.TypeLong, 20, 0, nil, nil},
	{"BLOCK_OPS_OUT", mysql.TypeLong, 20, 0, nil, nil},
	{"MESSAGES_SENT", mysql.TypeLong, 20, 0, nil, nil},
	{"MESSAGES_RECEIVED", mysql.TypeLong, 20, 0, nil, nil},
	{"PAGE_FAULTS_MAJOR", mysql.TypeLong, 20, 0, nil, nil},
	{"PAGE_FAULTS_MINOR", mysql.TypeLong, 20, 0, nil, nil},
	{"SWAPS", mysql.TypeLong, 20, 0, nil, nil},
	{"SOURCE_FUNCTION", mysql.TypeVarchar, 30, 0, nil, nil},
	{"SOURCE_FILE", mysql.TypeVarchar, 20, 0, nil, nil},
	{"SOURCE_LINE", mysql.TypeLong, 20, 0, nil, nil},
}

var charsetCols = []columnInfo{
	{"CHARACTER_SET_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"DEFAULT_COLLATE_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"DESCRIPTION", mysql.TypeVarchar, 60, 0, nil, nil},
	{"MAXLEN", mysql.TypeLonglong, 3, 0, nil, nil},
}

var collationsCols = []columnInfo{
	{"COLLATION_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"CHARACTER_SET_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"ID", mysql.TypeLonglong, 11, 0, nil, nil},
	{"IS_DEFAULT", mysql.TypeVarchar, 3, 0, nil, nil},
	{"IS_COMPILED", mysql.TypeVarchar, 3, 0, nil, nil},
	{"SORTLEN", mysql.TypeLonglong, 3, 0, nil, nil},
}

var keyColumnUsageCols = []columnInfo{
	{"CONSTRAINT_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"CONSTRAINT_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"CONSTRAINT_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"COLUMN_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"ORDINAL_POSITION", mysql.TypeLonglong, 10, mysql.NotNullFlag, nil, nil},
	{"POSITION_IN_UNIQUE_CONSTRAINT", mysql.TypeLonglong, 10, 0, nil, nil},
	{"REFERENCED_TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"REFERENCED_TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"REFERENCED_COLUMN_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
}

// See http://dev.mysql.com/doc/refman/5.7/en/referential-constraints-table.html
var referConstCols = []columnInfo{
	{"CONSTRAINT_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"CONSTRAINT_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"CONSTRAINT_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"UNIQUE_CONSTRAINT_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"UNIQUE_CONSTRAINT_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"UNIQUE_CONSTRAINT_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"MATCH_OPTION", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"UPDATE_RULE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"DELETE_RULE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"REFERENCED_TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
}

// See http://dev.mysql.com/doc/refman/5.7/en/variables-table.html
var sessionVarCols = []columnInfo{
	{"VARIABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"VARIABLE_VALUE", mysql.TypeVarchar, 1024, 0, nil, nil},
}

// See https://dev.mysql.com/doc/refman/5.7/en/plugins-table.html
var pluginsCols = []columnInfo{
	{"PLUGIN_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PLUGIN_VERSION", mysql.TypeVarchar, 20, 0, nil, nil},
	{"PLUGIN_STATUS", mysql.TypeVarchar, 10, 0, nil, nil},
	{"PLUGIN_TYPE", mysql.TypeVarchar, 80, 0, nil, nil},
	{"PLUGIN_TYPE_VERSION", mysql.TypeVarchar, 20, 0, nil, nil},
	{"PLUGIN_LIBRARY", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PLUGIN_LIBRARY_VERSION", mysql.TypeVarchar, 20, 0, nil, nil},
	{"PLUGIN_AUTHOR", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PLUGIN_DESCRIPTION", mysql.TypeLongBlob, types.UnspecifiedLength, 0, nil, nil},
	{"PLUGIN_LICENSE", mysql.TypeVarchar, 80, 0, nil, nil},
	{"LOAD_OPTION", mysql.TypeVarchar, 64, 0, nil, nil},
}

// See https://dev.mysql.com/doc/refman/5.7/en/partitions-table.html
var partitionsCols = []columnInfo{
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PARTITION_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"SUBPARTITION_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PARTITION_ORDINAL_POSITION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"SUBPARTITION_ORDINAL_POSITION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"PARTITION_METHOD", mysql.TypeVarchar, 18, 0, nil, nil},
	{"SUBPARTITION_METHOD", mysql.TypeVarchar, 12, 0, nil, nil},
	{"PARTITION_EXPRESSION", mysql.TypeLongBlob, types.UnspecifiedLength, 0, nil, nil},
	{"SUBPARTITION_EXPRESSION", mysql.TypeLongBlob, types.UnspecifiedLength, 0, nil, nil},
	{"PARTITION_DESCRIPTION", mysql.TypeLongBlob, types.UnspecifiedLength, 0, nil, nil},
	{"TABLE_ROWS", mysql.TypeLonglong, 21, 0, nil, nil},
	{"AVG_ROW_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATA_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"MAX_DATA_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"INDEX_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATA_FREE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CREATE_TIME", mysql.TypeDatetime, 0, 0, nil, nil},
	{"UPDATE_TIME", mysql.TypeDatetime, 0, 0, nil, nil},
	{"CHECK_TIME", mysql.TypeDatetime, 0, 0, nil, nil},
	{"CHECKSUM", mysql.TypeLonglong, 21, 0, nil, nil},
	{"PARTITION_COMMENT", mysql.TypeVarchar, 80, 0, nil, nil},
	{"NODEGROUP", mysql.TypeVarchar, 12, 0, nil, nil},
	{"TABLESPACE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
}

var tableConstraintsCols = []columnInfo{
	{"CONSTRAINT_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"CONSTRAINT_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"CONSTRAINT_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"CONSTRAINT_TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
}

var tableTriggersCols = []columnInfo{
	{"TRIGGER_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"TRIGGER_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TRIGGER_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"EVENT_MANIPULATION", mysql.TypeVarchar, 6, 0, nil, nil},
	{"EVENT_OBJECT_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"EVENT_OBJECT_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"EVENT_OBJECT_TABLE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ACTION_ORDER", mysql.TypeLonglong, 4, 0, nil, nil},
	{"ACTION_CONDITION", mysql.TypeBlob, -1, 0, nil, nil},
	{"ACTION_STATEMENT", mysql.TypeBlob, -1, 0, nil, nil},
	{"ACTION_ORIENTATION", mysql.TypeVarchar, 9, 0, nil, nil},
	{"ACTION_TIMING", mysql.TypeVarchar, 6, 0, nil, nil},
	{"ACTION_REFERENCE_OLD_TABLE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ACTION_REFERENCE_NEW_TABLE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ACTION_REFERENCE_OLD_ROW", mysql.TypeVarchar, 3, 0, nil, nil},
	{"ACTION_REFERENCE_NEW_ROW", mysql.TypeVarchar, 3, 0, nil, nil},
	{"CREATED", mysql.TypeDatetime, 2, 0, nil, nil},
	{"SQL_MODE", mysql.TypeVarchar, 8192, 0, nil, nil},
	{"DEFINER", mysql.TypeVarchar, 77, 0, nil, nil},
	{"CHARACTER_SET_CLIENT", mysql.TypeVarchar, 32, 0, nil, nil},
	{"COLLATION_CONNECTION", mysql.TypeVarchar, 32, 0, nil, nil},
	{"DATABASE_COLLATION", mysql.TypeVarchar, 32, 0, nil, nil},
}

var tableUserPrivilegesCols = []columnInfo{
	{"GRANTEE", mysql.TypeVarchar, 81, 0, nil, nil},
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"PRIVILEGE_TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"IS_GRANTABLE", mysql.TypeVarchar, 3, 0, nil, nil},
}

var tableSchemaPrivilegesCols = []columnInfo{
	{"GRANTEE", mysql.TypeVarchar, 81, mysql.NotNullFlag, nil, nil},
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"PRIVILEGE_TYPE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"IS_GRANTABLE", mysql.TypeVarchar, 3, mysql.NotNullFlag, nil, nil},
}

var tableTablePrivilegesCols = []columnInfo{
	{"GRANTEE", mysql.TypeVarchar, 81, mysql.NotNullFlag, nil, nil},
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"PRIVILEGE_TYPE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"IS_GRANTABLE", mysql.TypeVarchar, 3, mysql.NotNullFlag, nil, nil},
}

var tableColumnPrivilegesCols = []columnInfo{
	{"GRANTEE", mysql.TypeVarchar, 81, mysql.NotNullFlag, nil, nil},
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"COLUMN_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"PRIVILEGE_TYPE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"IS_GRANTABLE", mysql.TypeVarchar, 3, mysql.NotNullFlag, nil, nil},
}

var tableEnginesCols = []columnInfo{
	{"ENGINE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"SUPPORT", mysql.TypeVarchar, 8, 0, nil, nil},
	{"COMMENT", mysql.TypeVarchar, 80, 0, nil, nil},
	{"TRANSACTIONS", mysql.TypeVarchar, 3, 0, nil, nil},
	{"XA", mysql.TypeVarchar, 3, 0, nil, nil},
	{"SAVEPOINTS", mysql.TypeVarchar, 3, 0, nil, nil},
}

var tableViewsCols = []columnInfo{
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"VIEW_DEFINITION", mysql.TypeLongBlob, 0, mysql.NotNullFlag, nil, nil},
	{"CHECK_OPTION", mysql.TypeVarchar, 8, mysql.NotNullFlag, nil, nil},
	{"IS_UPDATABLE", mysql.TypeVarchar, 3, mysql.NotNullFlag, nil, nil},
	{"DEFINER", mysql.TypeVarchar, 77, mysql.NotNullFlag, nil, nil},
	{"SECURITY_TYPE", mysql.TypeVarchar, 7, mysql.NotNullFlag, nil, nil},
	{"CHARACTER_SET_CLIENT", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
	{"COLLATION_CONNECTION", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
}

var tableRoutinesCols = []columnInfo{
	{"SPECIFIC_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_TYPE", mysql.TypeVarchar, 9, mysql.NotNullFlag, nil, nil},
	{"DATA_TYPE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"CHARACTER_MAXIMUM_LENGTH", mysql.TypeLong, 21, 0, nil, nil},
	{"CHARACTER_OCTET_LENGTH", mysql.TypeLong, 21, 0, nil, nil},
	{"NUMERIC_PRECISION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"NUMERIC_SCALE", mysql.TypeLong, 21, 0, nil, nil},
	{"DATETIME_PRECISION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CHARACTER_SET_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"COLLATION_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DTD_IDENTIFIER", mysql.TypeLongBlob, 0, 0, nil, nil},
	{"ROUTINE_BODY", mysql.TypeVarchar, 8, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_DEFINITION", mysql.TypeLongBlob, 0, 0, nil, nil},
	{"EXTERNAL_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"EXTERNAL_LANGUAGE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PARAMETER_STYLE", mysql.TypeVarchar, 8, mysql.NotNullFlag, nil, nil},
	{"IS_DETERMINISTIC", mysql.TypeVarchar, 3, mysql.NotNullFlag, nil, nil},
	{"SQL_DATA_ACCESS", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"SQL_PATH", mysql.TypeVarchar, 64, 0, nil, nil},
	{"SECURITY_TYPE", mysql.TypeVarchar, 7, mysql.NotNullFlag, nil, nil},
	{"CREATED", mysql.TypeDatetime, 0, mysql.NotNullFlag, "0000-00-00 00:00:00", nil},
	{"LAST_ALTERED", mysql.TypeDatetime, 0, mysql.NotNullFlag, "0000-00-00 00:00:00", nil},
	{"SQL_MODE", mysql.TypeVarchar, 8192, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_COMMENT", mysql.TypeLongBlob, 0, 0, nil, nil},
	{"DEFINER", mysql.TypeVarchar, 77, mysql.NotNullFlag, nil, nil},
	{"CHARACTER_SET_CLIENT", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
	{"COLLATION_CONNECTION", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
	{"DATABASE_COLLATION", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
}

var tableParametersCols = []columnInfo{
	{"SPECIFIC_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"SPECIFIC_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"SPECIFIC_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"ORDINAL_POSITION", mysql.TypeVarchar, 21, mysql.NotNullFlag, nil, nil},
	{"PARAMETER_MODE", mysql.TypeVarchar, 5, 0, nil, nil},
	{"PARAMETER_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DATA_TYPE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"CHARACTER_MAXIMUM_LENGTH", mysql.TypeVarchar, 21, 0, nil, nil},
	{"CHARACTER_OCTET_LENGTH", mysql.TypeVarchar, 21, 0, nil, nil},
	{"NUMERIC_PRECISION", mysql.TypeVarchar, 21, 0, nil, nil},
	{"NUMERIC_SCALE", mysql.TypeVarchar, 21, 0, nil, nil},
	{"DATETIME_PRECISION", mysql.TypeVarchar, 21, 0, nil, nil},
	{"CHARACTER_SET_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"COLLATION_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DTD_IDENTIFIER", mysql.TypeLongBlob, 0, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_TYPE", mysql.TypeVarchar, 9, mysql.NotNullFlag, nil, nil},
}

var tableEventsCols = []columnInfo{
	{"EVENT_CATALOG", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"EVENT_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"EVENT_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"DEFINER", mysql.TypeVarchar, 77, mysql.NotNullFlag, nil, nil},
	{"TIME_ZONE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"EVENT_BODY", mysql.TypeVarchar, 8, mysql.NotNullFlag, nil, nil},
	{"EVENT_DEFINITION", mysql.TypeLongBlob, 0, 0, nil, nil},
	{"EVENT_TYPE", mysql.TypeVarchar, 9, mysql.NotNullFlag, nil, nil},
	{"EXECUTE_AT", mysql.TypeDatetime, 0, 0, nil, nil},
	{"INTERVAL_VALUE", mysql.TypeVarchar, 256, 0, nil, nil},
	{"INTERVAL_FIELD", mysql.TypeVarchar, 18, 0, nil, nil},
	{"SQL_MODE", mysql.TypeVarchar, 8192, mysql.NotNullFlag, nil, nil},
	{"STARTS", mysql.TypeDatetime, 0, 0, nil, nil},
	{"ENDS", mysql.TypeDatetime, 0, 0, nil, nil},
	{"STATUS", mysql.TypeVarchar, 18, mysql.NotNullFlag, nil, nil},
	{"ON_COMPLETION", mysql.TypeVarchar, 12, mysql.NotNullFlag, nil, nil},
	{"CREATED", mysql.TypeDatetime, 0, mysql.NotNullFlag, "0000-00-00 00:00:00", nil},
	{"LAST_ALTERED", mysql.TypeDatetime, 0, mysql.NotNullFlag, "0000-00-00 00:00:00", nil},
	{"LAST_EXECUTED", mysql.TypeDatetime, 0, 0, nil, nil},
	{"EVENT_COMMENT", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"ORIGINATOR", mysql.TypeLong, 10, mysql.NotNullFlag, 0, nil},
	{"CHARACTER_SET_CLIENT", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
	{"COLLATION_CONNECTION", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
	{"DATABASE_COLLATION", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
}

var tableGlobalStatusCols = []columnInfo{
	{"VARIABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"VARIABLE_VALUE", mysql.TypeVarchar, 1024, 0, nil, nil},
}

var tableGlobalVariablesCols = []columnInfo{
	{"VARIABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"VARIABLE_VALUE", mysql.TypeVarchar, 1024, 0, nil, nil},
}

var tableSessionStatusCols = []columnInfo{
	{"VARIABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"VARIABLE_VALUE", mysql.TypeVarchar, 1024, 0, nil, nil},
}

var tableOptimizerTraceCols = []columnInfo{
	{"QUERY", mysql.TypeLongBlob, 0, mysql.NotNullFlag, "", nil},
	{"TRACE", mysql.TypeLongBlob, 0, mysql.NotNullFlag, "", nil},
	{"MISSING_BYTES_BEYOND_MAX_MEM_SIZE", mysql.TypeShort, 20, mysql.NotNullFlag, 0, nil},
	{"INSUFFICIENT_PRIVILEGES", mysql.TypeTiny, 1, mysql.NotNullFlag, 0, nil},
}

var tableTableSpacesCols = []columnInfo{
	{"TABLESPACE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, "", nil},
	{"ENGINE", mysql.TypeVarchar, 64, mysql.NotNullFlag, "", nil},
	{"TABLESPACE_TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"LOGFILE_GROUP_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"EXTENT_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"AUTOEXTEND_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"MAXIMUM_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"NODEGROUP_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"TABLESPACE_COMMENT", mysql.TypeVarchar, 2048, 0, nil, nil},
}

var tableCollationCharacterSetApplicabilityCols = []columnInfo{
	{"COLLATION_NAME", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
	{"CHARACTER_SET_NAME", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
}

var tableProcesslistCols = []columnInfo{
<<<<<<< HEAD
	{"ID", mysql.TypeLonglong, 21, mysql.NotNullFlag, 0, nil},
	{"USER", mysql.TypeVarchar, 16, mysql.NotNullFlag, "", nil},
	{"HOST", mysql.TypeVarchar, 64, mysql.NotNullFlag, "", nil},
	{"DB", mysql.TypeVarchar, 64, 0, nil, nil},
	{"COMMAND", mysql.TypeVarchar, 16, mysql.NotNullFlag, "", nil},
	{"TIME", mysql.TypeLong, 7, mysql.NotNullFlag, 0, nil},
	{"STATE", mysql.TypeVarchar, 7, 0, nil, nil},
	{"INFO", mysql.TypeString, 512, 0, nil, nil},
	{"MEM", mysql.TypeLonglong, 21, 0, nil, nil},
	{"TxnStart", mysql.TypeVarchar, 64, mysql.NotNullFlag, "", nil},
=======
	{name: "ID", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag | mysql.UnsignedFlag, deflt: 0},
	{name: "USER", tp: mysql.TypeVarchar, size: 16, flag: mysql.NotNullFlag, deflt: ""},
	{name: "HOST", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag, deflt: ""},
	{name: "DB", tp: mysql.TypeVarchar, size: 64},
	{name: "COMMAND", tp: mysql.TypeVarchar, size: 16, flag: mysql.NotNullFlag, deflt: ""},
	{name: "TIME", tp: mysql.TypeLong, size: 7, flag: mysql.NotNullFlag, deflt: 0},
	{name: "STATE", tp: mysql.TypeVarchar, size: 7},
	{name: "INFO", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "MEM", tp: mysql.TypeLonglong, size: 21, flag: mysql.UnsignedFlag},
	{name: "TxnStart", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag, deflt: ""},
>>>>>>> 729fdcb... infoschema: fix query processlist and cluster_log bug in mysql8 client (#19648)
}

var tableTiDBIndexesCols = []columnInfo{
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"NON_UNIQUE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"KEY_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"SEQ_IN_INDEX", mysql.TypeLonglong, 21, 0, nil, nil},
	{"COLUMN_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"SUB_PART", mysql.TypeLonglong, 21, 0, nil, nil},
	{"INDEX_COMMENT", mysql.TypeVarchar, 2048, 0, nil, nil},
	{"INDEX_ID", mysql.TypeLonglong, 21, 0, nil, nil},
}

var tableTiDBHotRegionsCols = []columnInfo{
	{"TABLE_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"INDEX_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DB_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"INDEX_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"REGION_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"MAX_HOT_DEGREE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"REGION_COUNT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"FLOW_BYTES", mysql.TypeLonglong, 21, 0, nil, nil},
}

var tableTiKVStoreStatusCols = []columnInfo{
	{"STORE_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"ADDRESS", mysql.TypeVarchar, 64, 0, nil, nil},
	{"STORE_STATE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"STORE_STATE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"LABEL", mysql.TypeJSON, 51, 0, nil, nil},
	{"VERSION", mysql.TypeVarchar, 64, 0, nil, nil},
	{"CAPACITY", mysql.TypeVarchar, 64, 0, nil, nil},
	{"AVAILABLE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"LEADER_COUNT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"LEADER_WEIGHT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"LEADER_SCORE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"LEADER_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"REGION_COUNT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"REGION_WEIGHT", mysql.TypeDouble, 22, 0, nil, nil},
	{"REGION_SCORE", mysql.TypeDouble, 22, 0, nil, nil},
	{"REGION_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"START_TS", mysql.TypeDatetime, 0, 0, nil, nil},
	{"LAST_HEARTBEAT_TS", mysql.TypeDatetime, 0, 0, nil, nil},
	{"UPTIME", mysql.TypeVarchar, 64, 0, nil, nil},
}

var tableAnalyzeStatusCols = []columnInfo{
<<<<<<< HEAD
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PARTITION_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"JOB_INFO", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PROCESSED_ROWS", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{"START_TIME", mysql.TypeDatetime, 0, 0, nil, nil},
	{"STATE", mysql.TypeVarchar, 64, 0, nil, nil},
}

var tableTiKVRegionStatusCols = []columnInfo{
	{"REGION_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"START_KEY", mysql.TypeBlob, types.UnspecifiedLength, 0, nil, nil},
	{"END_KEY", mysql.TypeBlob, types.UnspecifiedLength, 0, nil, nil},
	{"EPOCH_CONF_VER", mysql.TypeLonglong, 21, 0, nil, nil},
	{"EPOCH_VERSION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"WRITTEN_BYTES", mysql.TypeLonglong, 21, 0, nil, nil},
	{"READ_BYTES", mysql.TypeLonglong, 21, 0, nil, nil},
	{"APPROXIMATE_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"APPROXIMATE_KEYS", mysql.TypeLonglong, 21, 0, nil, nil},
}

var tableTiKVRegionPeersCols = []columnInfo{
	{"REGION_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"PEER_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"STORE_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"IS_LEARNER", mysql.TypeTiny, 1, mysql.NotNullFlag, 0, nil},
	{"IS_LEADER", mysql.TypeTiny, 1, mysql.NotNullFlag, 0, nil},
	{"STATUS", mysql.TypeVarchar, 10, 0, 0, nil},
	{"DOWN_SECONDS", mysql.TypeLonglong, 21, 0, 0, nil},
}

func dataForTiKVRegionStatus(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	tikvStore, ok := ctx.GetStore().(tikv.Storage)
	if !ok {
		return nil, errors.New("Information about TiKV region status can be gotten only when the storage is TiKV")
=======
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "PARTITION_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "JOB_INFO", tp: mysql.TypeVarchar, size: 64},
	{name: "PROCESSED_ROWS", tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: "START_TIME", tp: mysql.TypeDatetime},
	{name: "STATE", tp: mysql.TypeVarchar, size: 64},
}

// TableTiKVRegionStatusCols is TiKV region status mem table columns.
var TableTiKVRegionStatusCols = []columnInfo{
	{name: "REGION_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "START_KEY", tp: mysql.TypeBlob, size: types.UnspecifiedLength},
	{name: "END_KEY", tp: mysql.TypeBlob, size: types.UnspecifiedLength},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "DB_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "IS_INDEX", tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, deflt: 0},
	{name: "INDEX_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "INDEX_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "EPOCH_CONF_VER", tp: mysql.TypeLonglong, size: 21},
	{name: "EPOCH_VERSION", tp: mysql.TypeLonglong, size: 21},
	{name: "WRITTEN_BYTES", tp: mysql.TypeLonglong, size: 21},
	{name: "READ_BYTES", tp: mysql.TypeLonglong, size: 21},
	{name: "APPROXIMATE_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "APPROXIMATE_KEYS", tp: mysql.TypeLonglong, size: 21},
	{name: "REPLICATIONSTATUS_STATE", tp: mysql.TypeVarchar, size: 64},
	{name: "REPLICATIONSTATUS_STATEID", tp: mysql.TypeLonglong, size: 21},
}

// TableTiKVRegionPeersCols is TiKV region peers mem table columns.
var TableTiKVRegionPeersCols = []columnInfo{
	{name: "REGION_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "PEER_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "STORE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "IS_LEARNER", tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, deflt: 0},
	{name: "IS_LEADER", tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, deflt: 0},
	{name: "STATUS", tp: mysql.TypeVarchar, size: 10, deflt: 0},
	{name: "DOWN_SECONDS", tp: mysql.TypeLonglong, size: 21, deflt: 0},
}

var tableTiDBServersInfoCols = []columnInfo{
	{name: "DDL_ID", tp: mysql.TypeVarchar, size: 64},
	{name: "IP", tp: mysql.TypeVarchar, size: 64},
	{name: "PORT", tp: mysql.TypeLonglong, size: 21},
	{name: "STATUS_PORT", tp: mysql.TypeLonglong, size: 21},
	{name: "LEASE", tp: mysql.TypeVarchar, size: 64},
	{name: "VERSION", tp: mysql.TypeVarchar, size: 64},
	{name: "GIT_HASH", tp: mysql.TypeVarchar, size: 64},
	{name: "BINLOG_STATUS", tp: mysql.TypeVarchar, size: 64},
	{name: "LABELS", tp: mysql.TypeVarchar, size: 128},
}

var tableClusterConfigCols = []columnInfo{
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "KEY", tp: mysql.TypeVarchar, size: 256},
	{name: "VALUE", tp: mysql.TypeVarchar, size: 128},
}

var tableClusterLogCols = []columnInfo{
	{name: "TIME", tp: mysql.TypeVarchar, size: 32},
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "LEVEL", tp: mysql.TypeVarchar, size: 8},
	{name: "MESSAGE", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
}

var tableClusterLoadCols = []columnInfo{
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "DEVICE_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "DEVICE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "NAME", tp: mysql.TypeVarchar, size: 256},
	{name: "VALUE", tp: mysql.TypeVarchar, size: 128},
}

var tableClusterHardwareCols = []columnInfo{
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "DEVICE_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "DEVICE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "NAME", tp: mysql.TypeVarchar, size: 256},
	{name: "VALUE", tp: mysql.TypeVarchar, size: 128},
}

var tableClusterSystemInfoCols = []columnInfo{
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "SYSTEM_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "SYSTEM_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "NAME", tp: mysql.TypeVarchar, size: 256},
	{name: "VALUE", tp: mysql.TypeVarchar, size: 128},
}

var filesCols = []columnInfo{
	{name: "FILE_ID", tp: mysql.TypeLonglong, size: 4},
	{name: "FILE_NAME", tp: mysql.TypeVarchar, size: 4000},
	{name: "FILE_TYPE", tp: mysql.TypeVarchar, size: 20},
	{name: "TABLESPACE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "LOGFILE_GROUP_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "LOGFILE_GROUP_NUMBER", tp: mysql.TypeLonglong, size: 32},
	{name: "ENGINE", tp: mysql.TypeVarchar, size: 64},
	{name: "FULLTEXT_KEYS", tp: mysql.TypeVarchar, size: 64},
	{name: "DELETED_ROWS", tp: mysql.TypeLonglong, size: 4},
	{name: "UPDATE_COUNT", tp: mysql.TypeLonglong, size: 4},
	{name: "FREE_EXTENTS", tp: mysql.TypeLonglong, size: 4},
	{name: "TOTAL_EXTENTS", tp: mysql.TypeLonglong, size: 4},
	{name: "EXTENT_SIZE", tp: mysql.TypeLonglong, size: 4},
	{name: "INITIAL_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "MAXIMUM_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "AUTOEXTEND_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "CREATION_TIME", tp: mysql.TypeDatetime, size: -1},
	{name: "LAST_UPDATE_TIME", tp: mysql.TypeDatetime, size: -1},
	{name: "LAST_ACCESS_TIME", tp: mysql.TypeDatetime, size: -1},
	{name: "RECOVER_TIME", tp: mysql.TypeLonglong, size: 4},
	{name: "TRANSACTION_COUNTER", tp: mysql.TypeLonglong, size: 4},
	{name: "VERSION", tp: mysql.TypeLonglong, size: 21},
	{name: "ROW_FORMAT", tp: mysql.TypeVarchar, size: 10},
	{name: "TABLE_ROWS", tp: mysql.TypeLonglong, size: 21},
	{name: "AVG_ROW_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "DATA_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "MAX_DATA_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "INDEX_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "DATA_FREE", tp: mysql.TypeLonglong, size: 21},
	{name: "CREATE_TIME", tp: mysql.TypeDatetime, size: -1},
	{name: "UPDATE_TIME", tp: mysql.TypeDatetime, size: -1},
	{name: "CHECK_TIME", tp: mysql.TypeDatetime, size: -1},
	{name: "CHECKSUM", tp: mysql.TypeLonglong, size: 21},
	{name: "STATUS", tp: mysql.TypeVarchar, size: 20},
	{name: "EXTRA", tp: mysql.TypeVarchar, size: 255},
}

var tableClusterInfoCols = []columnInfo{
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "STATUS_ADDRESS", tp: mysql.TypeVarchar, size: 64},
	{name: "VERSION", tp: mysql.TypeVarchar, size: 64},
	{name: "GIT_HASH", tp: mysql.TypeVarchar, size: 64},
	{name: "START_TIME", tp: mysql.TypeVarchar, size: 32},
	{name: "UPTIME", tp: mysql.TypeVarchar, size: 32},
}

var tableTableTiFlashReplicaCols = []columnInfo{
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "REPLICA_COUNT", tp: mysql.TypeLonglong, size: 64},
	{name: "LOCATION_LABELS", tp: mysql.TypeVarchar, size: 64},
	{name: "AVAILABLE", tp: mysql.TypeTiny, size: 1},
	{name: "PROGRESS", tp: mysql.TypeDouble, size: 22},
}

var tableInspectionResultCols = []columnInfo{
	{name: "RULE", tp: mysql.TypeVarchar, size: 64},
	{name: "ITEM", tp: mysql.TypeVarchar, size: 64},
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "STATUS_ADDRESS", tp: mysql.TypeVarchar, size: 64},
	{name: "VALUE", tp: mysql.TypeVarchar, size: 64},
	{name: "REFERENCE", tp: mysql.TypeVarchar, size: 64},
	{name: "SEVERITY", tp: mysql.TypeVarchar, size: 64},
	{name: "DETAILS", tp: mysql.TypeVarchar, size: 256},
}

var tableInspectionSummaryCols = []columnInfo{
	{name: "RULE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "METRICS_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "LABEL", tp: mysql.TypeVarchar, size: 64},
	{name: "QUANTILE", tp: mysql.TypeDouble, size: 22},
	{name: "AVG_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "MIN_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "MAX_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 256},
}

var tableInspectionRulesCols = []columnInfo{
	{name: "NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 256},
}

var tableMetricTablesCols = []columnInfo{
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "PROMQL", tp: mysql.TypeVarchar, size: 64},
	{name: "LABELS", tp: mysql.TypeVarchar, size: 64},
	{name: "QUANTILE", tp: mysql.TypeDouble, size: 22},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 256},
}

var tableMetricSummaryCols = []columnInfo{
	{name: "METRICS_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "QUANTILE", tp: mysql.TypeDouble, size: 22},
	{name: "SUM_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "AVG_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "MIN_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "MAX_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 256},
}

var tableMetricSummaryByLabelCols = []columnInfo{
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "METRICS_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "LABEL", tp: mysql.TypeVarchar, size: 64},
	{name: "QUANTILE", tp: mysql.TypeDouble, size: 22},
	{name: "SUM_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "AVG_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "MIN_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "MAX_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 256},
}

var tableDDLJobsCols = []columnInfo{
	{name: "JOB_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "DB_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "JOB_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "SCHEMA_STATE", tp: mysql.TypeVarchar, size: 64},
	{name: "SCHEMA_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "ROW_COUNT", tp: mysql.TypeLonglong, size: 21},
	{name: "START_TIME", tp: mysql.TypeDatetime, size: 19},
	{name: "END_TIME", tp: mysql.TypeDatetime, size: 19},
	{name: "STATE", tp: mysql.TypeVarchar, size: 64},
	{name: "QUERY", tp: mysql.TypeVarchar, size: 64},
}

var tableSequencesCols = []columnInfo{
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "SEQUENCE_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "SEQUENCE_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CACHE", tp: mysql.TypeTiny, flag: mysql.NotNullFlag},
	{name: "CACHE_VALUE", tp: mysql.TypeLonglong, size: 21},
	{name: "CYCLE", tp: mysql.TypeTiny, flag: mysql.NotNullFlag},
	{name: "INCREMENT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "MAX_VALUE", tp: mysql.TypeLonglong, size: 21},
	{name: "MIN_VALUE", tp: mysql.TypeLonglong, size: 21},
	{name: "START", tp: mysql.TypeLonglong, size: 21},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 64},
}

var tableStatementsSummaryCols = []columnInfo{
	{name: "SUMMARY_BEGIN_TIME", tp: mysql.TypeTimestamp, size: 26, flag: mysql.NotNullFlag, comment: "Begin time of this summary"},
	{name: "SUMMARY_END_TIME", tp: mysql.TypeTimestamp, size: 26, flag: mysql.NotNullFlag, comment: "End time of this summary"},
	{name: "STMT_TYPE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag, comment: "Statement type"},
	{name: "SCHEMA_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag, comment: "Current schema"},
	{name: "DIGEST", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "DIGEST_TEXT", tp: mysql.TypeBlob, size: types.UnspecifiedLength, flag: mysql.NotNullFlag, comment: "Normalized statement"},
	{name: "TABLE_NAMES", tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Involved tables"},
	{name: "INDEX_NAMES", tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Used indices"},
	{name: "SAMPLE_USER", tp: mysql.TypeVarchar, size: 64, comment: "Sampled user who executed these statements"},
	{name: "EXEC_COUNT", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Count of executions"},
	{name: "SUM_ERRORS", tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum of errors"},
	{name: "SUM_WARNINGS", tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum of warnings"},
	{name: "SUM_LATENCY", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum latency of these statements"},
	{name: "MAX_LATENCY", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max latency of these statements"},
	{name: "MIN_LATENCY", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Min latency of these statements"},
	{name: "AVG_LATENCY", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average latency of these statements"},
	{name: "AVG_PARSE_LATENCY", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average latency of parsing"},
	{name: "MAX_PARSE_LATENCY", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max latency of parsing"},
	{name: "AVG_COMPILE_LATENCY", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average latency of compiling"},
	{name: "MAX_COMPILE_LATENCY", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max latency of compiling"},
	{name: "SUM_COP_TASK_NUM", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total number of CopTasks"},
	{name: "MAX_COP_PROCESS_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max processing time of CopTasks"},
	{name: "MAX_COP_PROCESS_ADDRESS", tp: mysql.TypeVarchar, size: 256, comment: "Address of the CopTask with max processing time"},
	{name: "MAX_COP_WAIT_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max waiting time of CopTasks"},
	{name: "MAX_COP_WAIT_ADDRESS", tp: mysql.TypeVarchar, size: 256, comment: "Address of the CopTask with max waiting time"},
	{name: "AVG_PROCESS_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average processing time in TiKV"},
	{name: "MAX_PROCESS_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max processing time in TiKV"},
	{name: "AVG_WAIT_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average waiting time in TiKV"},
	{name: "MAX_WAIT_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max waiting time in TiKV"},
	{name: "AVG_BACKOFF_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average waiting time before retry"},
	{name: "MAX_BACKOFF_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max waiting time before retry"},
	{name: "AVG_TOTAL_KEYS", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of scanned keys"},
	{name: "MAX_TOTAL_KEYS", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of scanned keys"},
	{name: "AVG_PROCESSED_KEYS", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of processed keys"},
	{name: "MAX_PROCESSED_KEYS", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of processed keys"},
	{name: "AVG_PREWRITE_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time of prewrite phase"},
	{name: "MAX_PREWRITE_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max time of prewrite phase"},
	{name: "AVG_COMMIT_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time of commit phase"},
	{name: "MAX_COMMIT_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max time of commit phase"},
	{name: "AVG_GET_COMMIT_TS_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time of getting commit_ts"},
	{name: "MAX_GET_COMMIT_TS_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max time of getting commit_ts"},
	{name: "AVG_COMMIT_BACKOFF_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time before retry during commit phase"},
	{name: "MAX_COMMIT_BACKOFF_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max time before retry during commit phase"},
	{name: "AVG_RESOLVE_LOCK_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time for resolving locks"},
	{name: "MAX_RESOLVE_LOCK_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max time for resolving locks"},
	{name: "AVG_LOCAL_LATCH_WAIT_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average waiting time of local transaction"},
	{name: "MAX_LOCAL_LATCH_WAIT_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max waiting time of local transaction"},
	{name: "AVG_WRITE_KEYS", tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average count of written keys"},
	{name: "MAX_WRITE_KEYS", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max count of written keys"},
	{name: "AVG_WRITE_SIZE", tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average amount of written bytes"},
	{name: "MAX_WRITE_SIZE", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max amount of written bytes"},
	{name: "AVG_PREWRITE_REGIONS", tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of involved regions in prewrite phase"},
	{name: "MAX_PREWRITE_REGIONS", tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of involved regions in prewrite phase"},
	{name: "AVG_TXN_RETRY", tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of transaction retries"},
	{name: "MAX_TXN_RETRY", tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of transaction retries"},
	{name: "SUM_EXEC_RETRY", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum number of execution retries in pessimistic transactions"},
	{name: "SUM_EXEC_RETRY_TIME", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum time of execution retries in pessimistic transactions"},
	{name: "SUM_BACKOFF_TIMES", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum of retries"},
	{name: "BACKOFF_TYPES", tp: mysql.TypeVarchar, size: 1024, comment: "Types of errors and the number of retries for each type"},
	{name: "AVG_MEM", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average memory(byte) used"},
	{name: "MAX_MEM", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max memory(byte) used"},
	{name: "AVG_DISK", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average disk space(byte) used"},
	{name: "MAX_DISK", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max disk space(byte) used"},
	{name: "AVG_AFFECTED_ROWS", tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of rows affected"},
	{name: "FIRST_SEEN", tp: mysql.TypeTimestamp, size: 26, flag: mysql.NotNullFlag, comment: "The time these statements are seen for the first time"},
	{name: "LAST_SEEN", tp: mysql.TypeTimestamp, size: 26, flag: mysql.NotNullFlag, comment: "The time these statements are seen for the last time"},
	{name: "PLAN_IN_CACHE", tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, comment: "Whether the last statement hit plan cache"},
	{name: "PLAN_CACHE_HITS", tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag, comment: "The number of times these statements hit plan cache"},
	{name: "QUERY_SAMPLE_TEXT", tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Sampled original statement"},
	{name: "PREV_SAMPLE_TEXT", tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "The previous statement before commit"},
	{name: "PLAN_DIGEST", tp: mysql.TypeVarchar, size: 64, comment: "Digest of its execution plan"},
	{name: "PLAN", tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Sampled execution plan"},
}

var tableStorageStatsCols = []columnInfo{
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "PEER_COUNT", tp: mysql.TypeLonglong, size: 21},
	{name: "REGION_COUNT", tp: mysql.TypeLonglong, size: 21, comment: "The region count of single replica of the table"},
	{name: "EMPTY_REGION_COUNT", tp: mysql.TypeLonglong, size: 21, comment: "The region count of single replica of the table"},
	{name: "TABLE_SIZE", tp: mysql.TypeLonglong, size: 64, comment: "The disk usage(MB) of single replica of the table, if the table size is empty or less than 1MB, it would show 1MB "},
	{name: "TABLE_KEYS", tp: mysql.TypeLonglong, size: 64, comment: "The count of keys of single replica of the table"},
}

var tableTableTiFlashTablesCols = []columnInfo{
	{name: "DATABASE", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_DATABASE", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_TABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 64},
	{name: "IS_TOMBSTONE", tp: mysql.TypeLonglong, size: 64},
	{name: "SEGMENT_COUNT", tp: mysql.TypeLonglong, size: 64},
	{name: "TOTAL_ROWS", tp: mysql.TypeLonglong, size: 64},
	{name: "TOTAL_SIZE", tp: mysql.TypeLonglong, size: 64},
	{name: "TOTAL_DELETE_RANGES", tp: mysql.TypeLonglong, size: 64},
	{name: "DELTA_RATE_ROWS", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_RATE_SEGMENTS", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_PLACED_RATE", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_CACHE_SIZE", tp: mysql.TypeLonglong, size: 64},
	{name: "DELTA_CACHE_RATE", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_CACHE_WASTED_RATE", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_INDEX_SIZE", tp: mysql.TypeLonglong, size: 64},
	{name: "AVG_SEGMENT_ROWS", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_SEGMENT_SIZE", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_COUNT", tp: mysql.TypeLonglong, size: 64},
	{name: "TOTAL_DELTA_ROWS", tp: mysql.TypeLonglong, size: 64},
	{name: "TOTAL_DELTA_SIZE", tp: mysql.TypeLonglong, size: 64},
	{name: "AVG_DELTA_ROWS", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_DELTA_SIZE", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_DELTA_DELETE_RANGES", tp: mysql.TypeDouble, size: 64},
	{name: "STABLE_COUNT", tp: mysql.TypeLonglong, size: 64},
	{name: "TOTAL_STABLE_ROWS", tp: mysql.TypeLonglong, size: 64},
	{name: "TOTAL_STABLE_SIZE", tp: mysql.TypeLonglong, size: 64},
	{name: "TOTAL_STABLE_SIZE_ON_DISK", tp: mysql.TypeLonglong, size: 64},
	{name: "AVG_STABLE_ROWS", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_STABLE_SIZE", tp: mysql.TypeDouble, size: 64},
	{name: "TOTAL_PACK_COUNT_IN_DELTA", tp: mysql.TypeLonglong, size: 64},
	{name: "AVG_PACK_COUNT_IN_DELTA", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_PACK_ROWS_IN_DELTA", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_PACK_SIZE_IN_DELTA", tp: mysql.TypeDouble, size: 64},
	{name: "TOTAL_PACK_COUNT_IN_STABLE", tp: mysql.TypeLonglong, size: 64},
	{name: "AVG_PACK_COUNT_IN_STABLE", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_PACK_ROWS_IN_STABLE", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_PACK_SIZE_IN_STABLE", tp: mysql.TypeDouble, size: 64},
	{name: "STORAGE_STABLE_NUM_SNAPSHOTS", tp: mysql.TypeLonglong, size: 64},
	{name: "STORAGE_STABLE_NUM_PAGES", tp: mysql.TypeLonglong, size: 64},
	{name: "STORAGE_STABLE_NUM_NORMAL_PAGES", tp: mysql.TypeLonglong, size: 64},
	{name: "STORAGE_STABLE_MAX_PAGE_ID", tp: mysql.TypeLonglong, size: 64},
	{name: "STORAGE_DELTA_NUM_SNAPSHOTS", tp: mysql.TypeLonglong, size: 64},
	{name: "STORAGE_DELTA_NUM_PAGES", tp: mysql.TypeLonglong, size: 64},
	{name: "STORAGE_DELTA_NUM_NORMAL_PAGES", tp: mysql.TypeLonglong, size: 64},
	{name: "STORAGE_DELTA_MAX_PAGE_ID", tp: mysql.TypeLonglong, size: 64},
	{name: "STORAGE_META_NUM_SNAPSHOTS", tp: mysql.TypeLonglong, size: 64},
	{name: "STORAGE_META_NUM_PAGES", tp: mysql.TypeLonglong, size: 64},
	{name: "STORAGE_META_NUM_NORMAL_PAGES", tp: mysql.TypeLonglong, size: 64},
	{name: "STORAGE_META_MAX_PAGE_ID", tp: mysql.TypeLonglong, size: 64},
	{name: "BACKGROUND_TASKS_LENGTH", tp: mysql.TypeLonglong, size: 64},
	{name: "TIFLASH_INSTANCE", tp: mysql.TypeVarchar, size: 64},
}

var tableTableTiFlashSegmentsCols = []columnInfo{
	{name: "DATABASE", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_DATABASE", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_TABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 64},
	{name: "IS_TOMBSTONE", tp: mysql.TypeLonglong, size: 64},
	{name: "SEGMENT_ID", tp: mysql.TypeLonglong, size: 64},
	{name: "RANGE", tp: mysql.TypeVarchar, size: 64},
	{name: "ROWS", tp: mysql.TypeLonglong, size: 64},
	{name: "SIZE", tp: mysql.TypeLonglong, size: 64},
	{name: "DELETE_RANGES", tp: mysql.TypeLonglong, size: 64},
	{name: "STABLE_SIZE_ON_DISK", tp: mysql.TypeLonglong, size: 64},
	{name: "DELTA_PACK_COUNT", tp: mysql.TypeLonglong, size: 64},
	{name: "STABLE_PACK_COUNT", tp: mysql.TypeLonglong, size: 64},
	{name: "AVG_DELTA_PACK_ROWS", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_STABLE_PACK_ROWS", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_RATE", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_CACHE_SIZE", tp: mysql.TypeLonglong, size: 64},
	{name: "DELTA_INDEX_SIZE", tp: mysql.TypeLonglong, size: 64},
	{name: "TIFLASH_INSTANCE", tp: mysql.TypeVarchar, size: 64},
}

// GetShardingInfo returns a nil or description string for the sharding information of given TableInfo.
// The returned description string may be:
//  - "NOT_SHARDED": for tables that SHARD_ROW_ID_BITS is not specified.
//  - "NOT_SHARDED(PK_IS_HANDLE)": for tables of which primary key is row id.
//  - "PK_AUTO_RANDOM_BITS={bit_number}": for tables of which primary key is sharded row id.
//  - "SHARD_BITS={bit_number}": for tables that with SHARD_ROW_ID_BITS.
// The returned nil indicates that sharding information is not suitable for the table(for example, when the table is a View).
// This function is exported for unit test.
func GetShardingInfo(dbInfo *model.DBInfo, tableInfo *model.TableInfo) interface{} {
	if dbInfo == nil || tableInfo == nil || tableInfo.IsView() || util.IsMemOrSysDB(dbInfo.Name.L) {
		return nil
>>>>>>> 729fdcb... infoschema: fix query processlist and cluster_log bug in mysql8 client (#19648)
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	regionsStat, err := tikvHelper.GetRegionsInfo()
	if err != nil {
		return nil, err
	}
	for _, regionStat := range regionsStat.Regions {
		row := make([]types.Datum, len(tableTiKVRegionStatusCols))
		row[0].SetInt64(regionStat.ID)
		row[1].SetString(regionStat.StartKey)
		row[2].SetString(regionStat.EndKey)
		row[3].SetInt64(regionStat.Epoch.ConfVer)
		row[4].SetInt64(regionStat.Epoch.Version)
		row[5].SetInt64(regionStat.WrittenBytes)
		row[6].SetInt64(regionStat.ReadBytes)
		row[7].SetInt64(regionStat.ApproximateSize)
		row[8].SetInt64(regionStat.ApproximateKeys)
		records = append(records, row)
	}
	return records, nil
}

const (
	normalPeer  = "NORMAL"
	pendingPeer = "PENDING"
	downPeer    = "DOWN"
)

func dataForTikVRegionPeers(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	tikvStore, ok := ctx.GetStore().(tikv.Storage)
	if !ok {
		return nil, errors.New("Information about TiKV region status can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	regionsStat, err := tikvHelper.GetRegionsInfo()
	if err != nil {
		return nil, err
	}
	for _, regionStat := range regionsStat.Regions {
		pendingPeerIDSet := set.NewInt64Set()
		for _, peer := range regionStat.PendingPeers {
			pendingPeerIDSet.Insert(peer.ID)
		}
		downPeerMap := make(map[int64]int64)
		for _, peerStat := range regionStat.DownPeers {
			downPeerMap[peerStat.ID] = peerStat.DownSec
		}
		for _, peer := range regionStat.Peers {
			row := make([]types.Datum, len(tableTiKVRegionPeersCols))
			row[0].SetInt64(regionStat.ID)
			row[1].SetInt64(peer.ID)
			row[2].SetInt64(peer.StoreID)
			if peer.IsLearner {
				row[3].SetInt64(1)
			} else {
				row[3].SetInt64(0)
			}
			if peer.ID == regionStat.Leader.ID {
				row[4].SetInt64(1)
			} else {
				row[3].SetInt64(0)
			}
			if pendingPeerIDSet.Exist(peer.ID) {
				row[4].SetString(pendingPeer)
			} else if downSec, ok := downPeerMap[peer.ID]; ok {
				row[5].SetString(downPeer)
				row[6].SetInt64(downSec)
			} else {
				row[5].SetString(normalPeer)
			}
			records = append(records, row)
		}
	}
	return records, nil
}

func dataForTiKVStoreStatus(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	tikvStore, ok := ctx.GetStore().(tikv.Storage)
	if !ok {
		return nil, errors.New("Information about TiKV store status can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	storesStat, err := tikvHelper.GetStoresStat()
	if err != nil {
		return nil, err
	}
	for _, storeStat := range storesStat.Stores {
		row := make([]types.Datum, len(tableTiKVStoreStatusCols))
		row[0].SetInt64(storeStat.Store.ID)
		row[1].SetString(storeStat.Store.Address)
		row[2].SetInt64(storeStat.Store.State)
		row[3].SetString(storeStat.Store.StateName)
		data, err := json.Marshal(storeStat.Store.Labels)
		if err != nil {
			return nil, err
		}
		bj := binaryJson.BinaryJSON{}
		if err = bj.UnmarshalJSON(data); err != nil {
			return nil, err
		}
		row[4].SetMysqlJSON(bj)
		row[5].SetString(storeStat.Store.Version)
		row[6].SetString(storeStat.Status.Capacity)
		row[7].SetString(storeStat.Status.Available)
		row[8].SetInt64(storeStat.Status.LeaderCount)
		row[9].SetInt64(storeStat.Status.LeaderWeight)
		row[10].SetInt64(storeStat.Status.LeaderScore)
		row[11].SetInt64(storeStat.Status.LeaderSize)
		row[12].SetInt64(storeStat.Status.RegionCount)
		row[13].SetFloat64(storeStat.Status.RegionWeight)
		row[14].SetFloat64(storeStat.Status.RegionScore)
		row[15].SetInt64(storeStat.Status.RegionSize)
		startTs := types.Time{
			Time: types.FromGoTime(storeStat.Status.StartTs),
			Type: mysql.TypeDatetime,
			Fsp:  types.DefaultFsp,
		}
		row[16].SetMysqlTime(startTs)
		lastHeartbeatTs := types.Time{
			Time: types.FromGoTime(storeStat.Status.LastHeartbeatTs),
			Type: mysql.TypeDatetime,
			Fsp:  types.DefaultFsp,
		}
		row[17].SetMysqlTime(lastHeartbeatTs)
		row[18].SetString(storeStat.Status.Uptime)
		records = append(records, row)
	}
	return records, nil
}

func dataForCharacterSets() (records [][]types.Datum) {

	charsets := charset.GetSupportedCharsets()

	for _, charset := range charsets {

		records = append(records,
			types.MakeDatums(charset.Name, charset.DefaultCollation, charset.Desc, charset.Maxlen),
		)

	}

	return records

}

func dataForCollations() (records [][]types.Datum) {

	collations := charset.GetSupportedCollations()

	for _, collation := range collations {

		isDefault := ""
		if collation.IsDefault {
			isDefault = "Yes"
		}

		records = append(records,
			types.MakeDatums(collation.Name, collation.CharsetName, collation.ID, isDefault, "Yes", 1),
		)

	}

	return records

}

func dataForCollationCharacterSetApplicability() (records [][]types.Datum) {

	collations := charset.GetSupportedCollations()

	for _, collation := range collations {

		records = append(records,
			types.MakeDatums(collation.Name, collation.CharsetName),
		)

	}

	return records

}

func dataForSessionVar(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	sessionVars := ctx.GetSessionVars()
	for _, v := range variable.SysVars {
		var value string
		value, err = variable.GetSessionSystemVar(sessionVars, v.Name)
		if err != nil {
			return nil, err
		}
		row := types.MakeDatums(v.Name, value)
		records = append(records, row)
	}
	return
}

func dataForUserPrivileges(ctx sessionctx.Context) [][]types.Datum {
	pm := privilege.GetPrivilegeManager(ctx)
	return pm.UserPrivilegesTable()
}

func dataForProcesslist(ctx sessionctx.Context) [][]types.Datum {
	sm := ctx.GetSessionManager()
	if sm == nil {
		return nil
	}

	loginUser := ctx.GetSessionVars().User
	var hasProcessPriv bool
	if pm := privilege.GetPrivilegeManager(ctx); pm != nil {
		if pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, "", "", "", mysql.ProcessPriv) {
			hasProcessPriv = true
		}
	}

	pl := sm.ShowProcessList()
	records := make([][]types.Datum, 0, len(pl))
	for _, pi := range pl {
		// If you have the PROCESS privilege, you can see all threads.
		// Otherwise, you can see only your own threads.
		if !hasProcessPriv && pi.User != loginUser.Username {
			continue
		}

		rows := pi.ToRow(ctx.GetSessionVars().StmtCtx.TimeZone)
		record := types.MakeDatums(rows...)
		records = append(records, record)
	}
	return records
}

func dataForEngines() (records [][]types.Datum) {
	records = append(records,
		types.MakeDatums(
			"InnoDB",  // Engine
			"DEFAULT", // Support
			"Supports transactions, row-level locking, and foreign keys", // Comment
			"YES", // Transactions
			"YES", // XA
			"YES", // Savepoints
		),
	)
	return records
}

var filesCols = []columnInfo{
	{"FILE_ID", mysql.TypeLonglong, 4, 0, nil, nil},
	{"FILE_NAME", mysql.TypeVarchar, 4000, 0, nil, nil},
	{"FILE_TYPE", mysql.TypeVarchar, 20, 0, nil, nil},
	{"TABLESPACE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_CATALOG", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"LOGFILE_GROUP_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"LOGFILE_GROUP_NUMBER", mysql.TypeLonglong, 32, 0, nil, nil},
	{"ENGINE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"FULLTEXT_KEYS", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DELETED_ROWS", mysql.TypeLonglong, 4, 0, nil, nil},
	{"UPDATE_COUNT", mysql.TypeLonglong, 4, 0, nil, nil},
	{"FREE_EXTENTS", mysql.TypeLonglong, 4, 0, nil, nil},
	{"TOTAL_EXTENTS", mysql.TypeLonglong, 4, 0, nil, nil},
	{"EXTENT_SIZE", mysql.TypeLonglong, 4, 0, nil, nil},
	{"INITIAL_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"MAXIMUM_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"AUTOEXTEND_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CREATION_TIME", mysql.TypeDatetime, -1, 0, nil, nil},
	{"LAST_UPDATE_TIME", mysql.TypeDatetime, -1, 0, nil, nil},
	{"LAST_ACCESS_TIME", mysql.TypeDatetime, -1, 0, nil, nil},
	{"RECOVER_TIME", mysql.TypeLonglong, 4, 0, nil, nil},
	{"TRANSACTION_COUNTER", mysql.TypeLonglong, 4, 0, nil, nil},
	{"VERSION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"ROW_FORMAT", mysql.TypeVarchar, 10, 0, nil, nil},
	{"TABLE_ROWS", mysql.TypeLonglong, 21, 0, nil, nil},
	{"AVG_ROW_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATA_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"MAX_DATA_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"INDEX_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATA_FREE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CREATE_TIME", mysql.TypeDatetime, -1, 0, nil, nil},
	{"UPDATE_TIME", mysql.TypeDatetime, -1, 0, nil, nil},
	{"CHECK_TIME", mysql.TypeDatetime, -1, 0, nil, nil},
	{"CHECKSUM", mysql.TypeLonglong, 21, 0, nil, nil},
	{"STATUS", mysql.TypeVarchar, 20, 0, nil, nil},
	{"EXTRA", mysql.TypeVarchar, 255, 0, nil, nil},
}

func dataForSchemata(schemas []*model.DBInfo) [][]types.Datum {

	rows := make([][]types.Datum, 0, len(schemas))

	for _, schema := range schemas {

		charset := mysql.DefaultCharset
		collation := mysql.DefaultCollationName

		if len(schema.Charset) > 0 {
			charset = schema.Charset // Overwrite default
		}

		if len(schema.Collate) > 0 {
			collation = schema.Collate // Overwrite default
		}

		record := types.MakeDatums(
			catalogVal,    // CATALOG_NAME
			schema.Name.O, // SCHEMA_NAME
			charset,       // DEFAULT_CHARACTER_SET_NAME
			collation,     // DEFAULT_COLLATION_NAME
			nil,
		)
		rows = append(rows, record)
	}
	return rows
}

func getRowCountAllTable(ctx sessionctx.Context) (map[int64]uint64, error) {
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, "select table_id, count from mysql.stats_meta")
	if err != nil {
		return nil, err
	}
	rowCountMap := make(map[int64]uint64, len(rows))
	for _, row := range rows {
		tableID := row.GetInt64(0)
		rowCnt := row.GetUint64(1)
		rowCountMap[tableID] = rowCnt
	}
	return rowCountMap, nil
}

type tableHistID struct {
	tableID int64
	histID  int64
}

func getColLengthAllTables(ctx sessionctx.Context) (map[tableHistID]uint64, error) {
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, "select table_id, hist_id, tot_col_size from mysql.stats_histograms where is_index = 0")
	if err != nil {
		return nil, err
	}
	colLengthMap := make(map[tableHistID]uint64, len(rows))
	for _, row := range rows {
		tableID := row.GetInt64(0)
		histID := row.GetInt64(1)
		totalSize := row.GetInt64(2)
		if totalSize < 0 {
			totalSize = 0
		}
		colLengthMap[tableHistID{tableID: tableID, histID: histID}] = uint64(totalSize)
	}
	return colLengthMap, nil
}

func getDataAndIndexLength(info *model.TableInfo, physicalID int64, rowCount uint64, columnLengthMap map[tableHistID]uint64) (uint64, uint64) {
	columnLength := make(map[string]uint64)
	for _, col := range info.Columns {
		if col.State != model.StatePublic {
			continue
		}
		length := col.FieldType.StorageLength()
		if length != types.VarStorageLen {
			columnLength[col.Name.L] = rowCount * uint64(length)
		} else {
			length := columnLengthMap[tableHistID{tableID: physicalID, histID: col.ID}]
			columnLength[col.Name.L] = length
		}
	}
	dataLength, indexLength := uint64(0), uint64(0)
	for _, length := range columnLength {
		dataLength += length
	}
	for _, idx := range info.Indices {
		if idx.State != model.StatePublic {
			continue
		}
		for _, col := range idx.Columns {
			if col.Length == types.UnspecifiedLength {
				indexLength += columnLength[col.Name.L]
			} else {
				indexLength += rowCount * uint64(col.Length)
			}
		}
	}
	return dataLength, indexLength
}

type statsCache struct {
	mu         sync.Mutex
	loading    bool
	modifyTime time.Time
	tableRows  map[int64]uint64
	colLength  map[tableHistID]uint64
}

var tableStatsCache = &statsCache{}

// TableStatsCacheExpiry is the expiry time for table stats cache.
var TableStatsCacheExpiry = 3 * time.Second

func (c *statsCache) setLoading(loading bool) {
	c.mu.Lock()
	c.loading = loading
	c.mu.Unlock()
}

func (c *statsCache) get(ctx sessionctx.Context) (map[int64]uint64, map[tableHistID]uint64, error) {
	c.mu.Lock()
	if time.Since(c.modifyTime) < TableStatsCacheExpiry || c.loading {
		tableRows, colLength := c.tableRows, c.colLength
		c.mu.Unlock()
		return tableRows, colLength, nil
	}
	c.loading = true
	c.mu.Unlock()

	tableRows, err := getRowCountAllTable(ctx)
	if err != nil {
		c.setLoading(false)
		return nil, nil, err
	}
	colLength, err := getColLengthAllTables(ctx)
	if err != nil {
		c.setLoading(false)
		return nil, nil, err
	}

	c.mu.Lock()
	c.loading = false
	c.tableRows = tableRows
	c.colLength = colLength
	c.modifyTime = time.Now()
	c.mu.Unlock()
	return tableRows, colLength, nil
}

func getAutoIncrementID(ctx sessionctx.Context, schema *model.DBInfo, tblInfo *model.TableInfo) (int64, error) {
	is := ctx.GetSessionVars().TxnCtx.InfoSchema.(InfoSchema)
	tbl, err := is.TableByName(schema.Name, tblInfo.Name)
	if err != nil {
		return 0, err
	}
	return tbl.Allocator(ctx).Base() + 1, nil
}

func dataForViews(ctx sessionctx.Context, schemas []*model.DBInfo) ([][]types.Datum, error) {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if !table.IsView() {
				continue
			}
			collation := table.Collate
			charset := table.Charset
			if collation == "" {
				collation = mysql.DefaultCollationName
			}
			if charset == "" {
				charset = mysql.DefaultCharset
			}
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}
			record := types.MakeDatums(
				catalogVal,                      // TABLE_CATALOG
				schema.Name.O,                   // TABLE_SCHEMA
				table.Name.O,                    // TABLE_NAME
				table.View.SelectStmt,           // VIEW_DEFINITION
				table.View.CheckOption.String(), // CHECK_OPTION
				"NO",                            // IS_UPDATABLE
				table.View.Definer.String(),     // DEFINER
				table.View.Security.String(),    // SECURITY_TYPE
				charset,                         // CHARACTER_SET_CLIENT
				collation,                       // COLLATION_CONNECTION
			)
			rows = append(rows, record)
		}
	}
	return rows, nil
}

func dataForTables(ctx sessionctx.Context, schemas []*model.DBInfo) ([][]types.Datum, error) {
	tableRowsMap, colLengthMap, err := tableStatsCache.get(ctx)
	if err != nil {
		return nil, err
	}

	checker := privilege.GetPrivilegeManager(ctx)

	var rows [][]types.Datum
	createTimeTp := tablesCols[15].tp
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			collation := table.Collate
			if collation == "" {
				collation = mysql.DefaultCollationName
			}
			createTime := types.Time{
				Time: types.FromGoTime(table.GetUpdateTime()),
				Type: createTimeTp,
			}

			createOptions := ""

			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}

			if !table.IsView() {
				if table.GetPartitionInfo() != nil {
					createOptions = "partitioned"
				}
				var autoIncID interface{}
				hasAutoIncID, _ := HasAutoIncrementColumn(table)
				if hasAutoIncID {
					autoIncID, err = getAutoIncrementID(ctx, schema, table)
					if err != nil {
						return nil, err
					}
				}

				var rowCount, dataLength, indexLength uint64
				if table.GetPartitionInfo() == nil {
					rowCount = tableRowsMap[table.ID]
					dataLength, indexLength = getDataAndIndexLength(table, table.ID, rowCount, colLengthMap)
				} else {
					for _, pi := range table.GetPartitionInfo().Definitions {
						rowCount += tableRowsMap[pi.ID]
						parDataLen, parIndexLen := getDataAndIndexLength(table, pi.ID, tableRowsMap[pi.ID], colLengthMap)
						dataLength += parDataLen
						indexLength += parIndexLen
					}
				}
				avgRowLength := uint64(0)
				if rowCount != 0 {
					avgRowLength = dataLength / rowCount
				}
				record := types.MakeDatums(
					catalogVal,    // TABLE_CATALOG
					schema.Name.O, // TABLE_SCHEMA
					table.Name.O,  // TABLE_NAME
					"BASE TABLE",  // TABLE_TYPE
					"InnoDB",      // ENGINE
					uint64(10),    // VERSION
					"Compact",     // ROW_FORMAT
					rowCount,      // TABLE_ROWS
					avgRowLength,  // AVG_ROW_LENGTH
					dataLength,    // DATA_LENGTH
					uint64(0),     // MAX_DATA_LENGTH
					indexLength,   // INDEX_LENGTH
					uint64(0),     // DATA_FREE
					autoIncID,     // AUTO_INCREMENT
					createTime,    // CREATE_TIME
					nil,           // UPDATE_TIME
					nil,           // CHECK_TIME
					collation,     // TABLE_COLLATION
					nil,           // CHECKSUM
					createOptions, // CREATE_OPTIONS
					table.Comment, // TABLE_COMMENT
					table.ID,      // TIDB_TABLE_ID
				)
				rows = append(rows, record)
			} else {
				record := types.MakeDatums(
					catalogVal,    // TABLE_CATALOG
					schema.Name.O, // TABLE_SCHEMA
					table.Name.O,  // TABLE_NAME
					"VIEW",        // TABLE_TYPE
					nil,           // ENGINE
					nil,           // VERSION
					nil,           // ROW_FORMAT
					nil,           // TABLE_ROWS
					nil,           // AVG_ROW_LENGTH
					nil,           // DATA_LENGTH
					nil,           // MAX_DATA_LENGTH
					nil,           // INDEX_LENGTH
					nil,           // DATA_FREE
					nil,           // AUTO_INCREMENT
					createTime,    // CREATE_TIME
					nil,           // UPDATE_TIME
					nil,           // CHECK_TIME
					nil,           // TABLE_COLLATION
					nil,           // CHECKSUM
					nil,           // CREATE_OPTIONS
					"VIEW",        // TABLE_COMMENT
					table.ID,      // TIDB_TABLE_ID
				)
				rows = append(rows, record)
			}
		}
	}
	return rows, nil
}

func dataForIndexes(ctx sessionctx.Context, schemas []*model.DBInfo) ([][]types.Datum, error) {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, tb := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, tb.Name.L, "", mysql.AllPrivMask) {
				continue
			}

			if tb.PKIsHandle {
				var pkCol *model.ColumnInfo
				for _, col := range tb.Cols() {
					if mysql.HasPriKeyFlag(col.Flag) {
						pkCol = col
						break
					}
				}
				record := types.MakeDatums(
					schema.Name.O, // TABLE_SCHEMA
					tb.Name.O,     // TABLE_NAME
					0,             // NON_UNIQUE
					"PRIMARY",     // KEY_NAME
					1,             // SEQ_IN_INDEX
					pkCol.Name.O,  // COLUMN_NAME
					nil,           // SUB_PART
					"",            // INDEX_COMMENT
					0,             // INDEX_ID
				)
				rows = append(rows, record)
			}
			for _, idxInfo := range tb.Indices {
				if idxInfo.State != model.StatePublic {
					continue
				}
				for i, col := range idxInfo.Columns {
					nonUniq := 1
					if idxInfo.Unique {
						nonUniq = 0
					}
					var subPart interface{}
					if col.Length != types.UnspecifiedLength {
						subPart = col.Length
					}
					record := types.MakeDatums(
						schema.Name.O,   // TABLE_SCHEMA
						tb.Name.O,       // TABLE_NAME
						nonUniq,         // NON_UNIQUE
						idxInfo.Name.O,  // KEY_NAME
						i+1,             // SEQ_IN_INDEX
						col.Name.O,      // COLUMN_NAME
						subPart,         // SUB_PART
						idxInfo.Comment, // INDEX_COMMENT
						idxInfo.ID,      // INDEX_ID
					)
					rows = append(rows, record)
				}
			}
		}
	}
	return rows, nil
}

func dataForColumns(ctx sessionctx.Context, schemas []*model.DBInfo) [][]types.Datum {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}

			rs := dataForColumnsInTable(schema, table)
			rows = append(rows, rs...)
		}
	}
	return rows
}

func dataForColumnsInTable(schema *model.DBInfo, tbl *model.TableInfo) [][]types.Datum {
	rows := make([][]types.Datum, 0, len(tbl.Columns))
	for i, col := range tbl.Columns {
		var charMaxLen, charOctLen, numericPrecision, numericScale, datetimePrecision interface{}
		colLen, decimal := col.Flen, col.Decimal
		defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(col.Tp)
		if decimal == types.UnspecifiedLength {
			decimal = defaultDecimal
		}
		if colLen == types.UnspecifiedLength {
			colLen = defaultFlen
		}
		if col.Tp == mysql.TypeSet {
			// Example: In MySQL set('a','bc','def','ghij') has length 13, because
			// len('a')+len('bc')+len('def')+len('ghij')+len(ThreeComma)=13
			// Reference link: https://bugs.mysql.com/bug.php?id=22613
			colLen = 0
			for _, ele := range col.Elems {
				colLen += len(ele)
			}
			if len(col.Elems) != 0 {
				colLen += (len(col.Elems) - 1)
			}
			charMaxLen = colLen
			charOctLen = colLen
		} else if col.Tp == mysql.TypeEnum {
			// Example: In MySQL enum('a', 'ab', 'cdef') has length 4, because
			// the longest string in the enum is 'cdef'
			// Reference link: https://bugs.mysql.com/bug.php?id=22613
			colLen = 0
			for _, ele := range col.Elems {
				if len(ele) > colLen {
					colLen = len(ele)
				}
			}
			charMaxLen = colLen
			charOctLen = colLen
		} else if types.IsString(col.Tp) {
			charMaxLen = colLen
			charOctLen = colLen
		} else if types.IsTypeFractionable(col.Tp) {
			datetimePrecision = decimal
		} else if types.IsTypeNumeric(col.Tp) {
			numericPrecision = colLen
			if col.Tp != mysql.TypeFloat && col.Tp != mysql.TypeDouble {
				numericScale = decimal
			} else if decimal != -1 {
				numericScale = decimal
			}
		}
		columnType := col.FieldType.InfoSchemaStr()
		columnDesc := table.NewColDesc(table.ToColumn(col))
		var columnDefault interface{}
		if columnDesc.DefaultValue != nil {
			columnDefault = fmt.Sprintf("%v", columnDesc.DefaultValue)
		}
		record := types.MakeDatums(
			catalogVal,                           // TABLE_CATALOG
			schema.Name.O,                        // TABLE_SCHEMA
			tbl.Name.O,                           // TABLE_NAME
			col.Name.O,                           // COLUMN_NAME
			i+1,                                  // ORIGINAL_POSITION
			columnDefault,                        // COLUMN_DEFAULT
			columnDesc.Null,                      // IS_NULLABLE
			types.TypeToStr(col.Tp, col.Charset), // DATA_TYPE
			charMaxLen,                           // CHARACTER_MAXIMUM_LENGTH
			charOctLen,                           // CHARACTER_OCTET_LENGTH
			numericPrecision,                     // NUMERIC_PRECISION
			numericScale,                         // NUMERIC_SCALE
			datetimePrecision,                    // DATETIME_PRECISION
			columnDesc.Charset,                   // CHARACTER_SET_NAME
			columnDesc.Collation,                 // COLLATION_NAME
			columnType,                           // COLUMN_TYPE
			columnDesc.Key,                       // COLUMN_KEY
			columnDesc.Extra,                     // EXTRA
			"select,insert,update,references",    // PRIVILEGES
			columnDesc.Comment,                   // COLUMN_COMMENT
			col.GeneratedExprString,              // GENERATION_EXPRESSION
		)
		rows = append(rows, record)
	}
	return rows
}

func dataForStatistics(schemas []*model.DBInfo) [][]types.Datum {
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			rs := dataForStatisticsInTable(schema, table)
			rows = append(rows, rs...)
		}
	}
	return rows
}

func dataForStatisticsInTable(schema *model.DBInfo, table *model.TableInfo) [][]types.Datum {
	var rows [][]types.Datum
	if table.PKIsHandle {
		for _, col := range table.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				record := types.MakeDatums(
					catalogVal,    // TABLE_CATALOG
					schema.Name.O, // TABLE_SCHEMA
					table.Name.O,  // TABLE_NAME
					"0",           // NON_UNIQUE
					schema.Name.O, // INDEX_SCHEMA
					"PRIMARY",     // INDEX_NAME
					1,             // SEQ_IN_INDEX
					col.Name.O,    // COLUMN_NAME
					"A",           // COLLATION
					nil,           // CARDINALITY
					nil,           // SUB_PART
					nil,           // PACKED
					"",            // NULLABLE
					"BTREE",       // INDEX_TYPE
					"",            // COMMENT
					"",            // INDEX_COMMENT
				)
				rows = append(rows, record)
			}
		}
	}
	nameToCol := make(map[string]*model.ColumnInfo, len(table.Columns))
	for _, c := range table.Columns {
		nameToCol[c.Name.L] = c
	}
	for _, index := range table.Indices {
		nonUnique := "1"
		if index.Unique {
			nonUnique = "0"
		}
		for i, key := range index.Columns {
			col := nameToCol[key.Name.L]
			nullable := "YES"
			if mysql.HasNotNullFlag(col.Flag) {
				nullable = ""
			}
			record := types.MakeDatums(
				catalogVal,    // TABLE_CATALOG
				schema.Name.O, // TABLE_SCHEMA
				table.Name.O,  // TABLE_NAME
				nonUnique,     // NON_UNIQUE
				schema.Name.O, // INDEX_SCHEMA
				index.Name.O,  // INDEX_NAME
				i+1,           // SEQ_IN_INDEX
				key.Name.O,    // COLUMN_NAME
				"A",           // COLLATION
				nil,           // CARDINALITY
				nil,           // SUB_PART
				nil,           // PACKED
				nullable,      // NULLABLE
				"BTREE",       // INDEX_TYPE
				"",            // COMMENT
				"",            // INDEX_COMMENT
			)
			rows = append(rows, record)
		}
	}
	return rows
}

const (
	primaryKeyType    = "PRIMARY KEY"
	primaryConstraint = "PRIMARY"
	uniqueKeyType     = "UNIQUE"
)

// dataForTableConstraints constructs data for table information_schema.constraints.See https://dev.mysql.com/doc/refman/5.7/en/table-constraints-table.html
func dataForTableConstraints(schemas []*model.DBInfo) [][]types.Datum {
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, tbl := range schema.Tables {
			if tbl.PKIsHandle {
				record := types.MakeDatums(
					catalogVal,           // CONSTRAINT_CATALOG
					schema.Name.O,        // CONSTRAINT_SCHEMA
					mysql.PrimaryKeyName, // CONSTRAINT_NAME
					schema.Name.O,        // TABLE_SCHEMA
					tbl.Name.O,           // TABLE_NAME
					primaryKeyType,       // CONSTRAINT_TYPE
				)
				rows = append(rows, record)
			}

			for _, idx := range tbl.Indices {
				var cname, ctype string
				if idx.Primary {
					cname = mysql.PrimaryKeyName
					ctype = primaryKeyType
				} else if idx.Unique {
					cname = idx.Name.O
					ctype = uniqueKeyType
				} else {
					// The index has no constriant.
					continue
				}
				record := types.MakeDatums(
					catalogVal,    // CONSTRAINT_CATALOG
					schema.Name.O, // CONSTRAINT_SCHEMA
					cname,         // CONSTRAINT_NAME
					schema.Name.O, // TABLE_SCHEMA
					tbl.Name.O,    // TABLE_NAME
					ctype,         // CONSTRAINT_TYPE
				)
				rows = append(rows, record)
			}
		}
	}
	return rows
}

// dataForPseudoProfiling returns pseudo data for table profiling when system variable `profiling` is set to `ON`.
func dataForPseudoProfiling() [][]types.Datum {
	var rows [][]types.Datum
	row := types.MakeDatums(
		0,                      // QUERY_ID
		0,                      // SEQ
		"",                     // STATE
		types.NewDecFromInt(0), // DURATION
		types.NewDecFromInt(0), // CPU_USER
		types.NewDecFromInt(0), // CPU_SYSTEM
		0,                      // CONTEXT_VOLUNTARY
		0,                      // CONTEXT_INVOLUNTARY
		0,                      // BLOCK_OPS_IN
		0,                      // BLOCK_OPS_OUT
		0,                      // MESSAGES_SENT
		0,                      // MESSAGES_RECEIVED
		0,                      // PAGE_FAULTS_MAJOR
		0,                      // PAGE_FAULTS_MINOR
		0,                      // SWAPS
		"",                     // SOURCE_FUNCTION
		"",                     // SOURCE_FILE
		0,                      // SOURCE_LINE
	)
	rows = append(rows, row)
	return rows
}

func dataForPartitions(ctx sessionctx.Context, schemas []*model.DBInfo) ([][]types.Datum, error) {
	tableRowsMap, colLengthMap, err := tableStatsCache.get(ctx)
	if err != nil {
		return nil, err
	}
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	// createTimeTp := partitionsCols[18].tp
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.SelectPriv) {
				continue
			}
			createTime := types.Time{Time: types.FromGoTime(table.GetUpdateTime()), Type: mysql.TypeDatetime}

			var rowCount, dataLength, indexLength uint64
			if table.GetPartitionInfo() == nil {
				rowCount = tableRowsMap[table.ID]
				dataLength, indexLength = getDataAndIndexLength(table, table.ID, rowCount, colLengthMap)
				avgRowLength := uint64(0)
				if rowCount != 0 {
					avgRowLength = dataLength / rowCount
				}
				record := types.MakeDatums(
					catalogVal,    // TABLE_CATALOG
					schema.Name.O, // TABLE_SCHEMA
					table.Name.O,  // TABLE_NAME
					nil,           // PARTITION_NAME
					nil,           // SUBPARTITION_NAME
					nil,           // PARTITION_ORDINAL_POSITION
					nil,           // SUBPARTITION_ORDINAL_POSITION
					nil,           // PARTITION_METHOD
					nil,           // SUBPARTITION_METHOD
					nil,           // PARTITION_EXPRESSION
					nil,           // SUBPARTITION_EXPRESSION
					nil,           // PARTITION_DESCRIPTION
					rowCount,      // TABLE_ROWS
					avgRowLength,  // AVG_ROW_LENGTH
					dataLength,    // DATA_LENGTH
					nil,           // MAX_DATA_LENGTH
					indexLength,   // INDEX_LENGTH
					nil,           // DATA_FREE
					createTime,    // CREATE_TIME
					nil,           // UPDATE_TIME
					nil,           // CHECK_TIME
					nil,           // CHECKSUM
					nil,           // PARTITION_COMMENT
					nil,           // NODEGROUP
					nil,           // TABLESPACE_NAME
				)
				rows = append(rows, record)
			} else {
				for i, pi := range table.GetPartitionInfo().Definitions {
					rowCount = tableRowsMap[pi.ID]
					dataLength, indexLength = getDataAndIndexLength(table, pi.ID, tableRowsMap[pi.ID], colLengthMap)

					avgRowLength := uint64(0)
					if rowCount != 0 {
						avgRowLength = dataLength / rowCount
					}

					var partitionDesc string
					if table.Partition.Type == model.PartitionTypeRange {
						partitionDesc = pi.LessThan[0]
					}

					record := types.MakeDatums(
						catalogVal,                    // TABLE_CATALOG
						schema.Name.O,                 // TABLE_SCHEMA
						table.Name.O,                  // TABLE_NAME
						pi.Name.O,                     // PARTITION_NAME
						nil,                           // SUBPARTITION_NAME
						i+1,                           // PARTITION_ORDINAL_POSITION
						nil,                           // SUBPARTITION_ORDINAL_POSITION
						table.Partition.Type.String(), // PARTITION_METHOD
						nil,                           // SUBPARTITION_METHOD
						table.Partition.Expr,          // PARTITION_EXPRESSION
						nil,                           // SUBPARTITION_EXPRESSION
						partitionDesc,                 // PARTITION_DESCRIPTION
						rowCount,                      // TABLE_ROWS
						avgRowLength,                  // AVG_ROW_LENGTH
						dataLength,                    // DATA_LENGTH
						uint64(0),                     // MAX_DATA_LENGTH
						indexLength,                   // INDEX_LENGTH
						uint64(0),                     // DATA_FREE
						createTime,                    // CREATE_TIME
						nil,                           // UPDATE_TIME
						nil,                           // CHECK_TIME
						nil,                           // CHECKSUM
						pi.Comment,                    // PARTITION_COMMENT
						nil,                           // NODEGROUP
						nil,                           // TABLESPACE_NAME
					)
					rows = append(rows, record)
				}
			}
		}
	}
	return rows, nil
}

func dataForKeyColumnUsage(schemas []*model.DBInfo) [][]types.Datum {
	rows := make([][]types.Datum, 0, len(schemas)) // The capacity is not accurate, but it is not a big problem.
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			rs := keyColumnUsageInTable(schema, table)
			rows = append(rows, rs...)
		}
	}
	return rows
}

func keyColumnUsageInTable(schema *model.DBInfo, table *model.TableInfo) [][]types.Datum {
	var rows [][]types.Datum
	if table.PKIsHandle {
		for _, col := range table.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				record := types.MakeDatums(
					catalogVal,        // CONSTRAINT_CATALOG
					schema.Name.O,     // CONSTRAINT_SCHEMA
					primaryConstraint, // CONSTRAINT_NAME
					catalogVal,        // TABLE_CATALOG
					schema.Name.O,     // TABLE_SCHEMA
					table.Name.O,      // TABLE_NAME
					col.Name.O,        // COLUMN_NAME
					1,                 // ORDINAL_POSITION
					1,                 // POSITION_IN_UNIQUE_CONSTRAINT
					nil,               // REFERENCED_TABLE_SCHEMA
					nil,               // REFERENCED_TABLE_NAME
					nil,               // REFERENCED_COLUMN_NAME
				)
				rows = append(rows, record)
				break
			}
		}
	}
	nameToCol := make(map[string]*model.ColumnInfo, len(table.Columns))
	for _, c := range table.Columns {
		nameToCol[c.Name.L] = c
	}
	for _, index := range table.Indices {
		var idxName string
		if index.Primary {
			idxName = primaryConstraint
		} else if index.Unique {
			idxName = index.Name.O
		} else {
			// Only handle unique/primary key
			continue
		}
		for i, key := range index.Columns {
			col := nameToCol[key.Name.L]
			record := types.MakeDatums(
				catalogVal,    // CONSTRAINT_CATALOG
				schema.Name.O, // CONSTRAINT_SCHEMA
				idxName,       // CONSTRAINT_NAME
				catalogVal,    // TABLE_CATALOG
				schema.Name.O, // TABLE_SCHEMA
				table.Name.O,  // TABLE_NAME
				col.Name.O,    // COLUMN_NAME
				i+1,           // ORDINAL_POSITION,
				nil,           // POSITION_IN_UNIQUE_CONSTRAINT
				nil,           // REFERENCED_TABLE_SCHEMA
				nil,           // REFERENCED_TABLE_NAME
				nil,           // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
		}
	}
	for _, fk := range table.ForeignKeys {
		fkRefCol := ""
		if len(fk.RefCols) > 0 {
			fkRefCol = fk.RefCols[0].O
		}
		for i, key := range fk.Cols {
			col := nameToCol[key.L]
			record := types.MakeDatums(
				catalogVal,    // CONSTRAINT_CATALOG
				schema.Name.O, // CONSTRAINT_SCHEMA
				fk.Name.O,     // CONSTRAINT_NAME
				catalogVal,    // TABLE_CATALOG
				schema.Name.O, // TABLE_SCHEMA
				table.Name.O,  // TABLE_NAME
				col.Name.O,    // COLUMN_NAME
				i+1,           // ORDINAL_POSITION,
				1,             // POSITION_IN_UNIQUE_CONSTRAINT
				schema.Name.O, // REFERENCED_TABLE_SCHEMA
				fk.RefTable.O, // REFERENCED_TABLE_NAME
				fkRefCol,      // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
		}
	}
	return rows
}

func dataForTiDBHotRegions(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	tikvStore, ok := ctx.GetStore().(tikv.Storage)
	if !ok {
		return nil, errors.New("Information about hot region can be gotten only when the storage is TiKV")
	}
	allSchemas := ctx.GetSessionVars().TxnCtx.InfoSchema.(InfoSchema).AllSchemas()
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	metrics, err := tikvHelper.ScrapeHotInfo(pdapi.HotRead, allSchemas)
	if err != nil {
		return nil, err
	}
	records = append(records, dataForHotRegionByMetrics(metrics, "read")...)
	metrics, err = tikvHelper.ScrapeHotInfo(pdapi.HotWrite, allSchemas)
	if err != nil {
		return nil, err
	}
	records = append(records, dataForHotRegionByMetrics(metrics, "write")...)
	return records, nil
}

func dataForHotRegionByMetrics(metrics []helper.HotTableIndex, tp string) [][]types.Datum {
	rows := make([][]types.Datum, 0, len(metrics))
	for _, tblIndex := range metrics {
		row := make([]types.Datum, len(tableTiDBHotRegionsCols))
		if tblIndex.IndexName != "" {
			row[1].SetInt64(tblIndex.IndexID)
			row[4].SetString(tblIndex.IndexName)
		} else {
			row[1].SetNull()
			row[4].SetNull()
		}
		row[0].SetInt64(tblIndex.TableID)
		row[2].SetString(tblIndex.DbName)
		row[3].SetString(tblIndex.TableName)
		row[5].SetUint64(tblIndex.RegionID)
		row[6].SetString(tp)
		if tblIndex.RegionMetric == nil {
			row[7].SetNull()
			row[8].SetNull()
		} else {
			row[7].SetInt64(int64(tblIndex.RegionMetric.MaxHotDegree))
			row[8].SetInt64(int64(tblIndex.RegionMetric.Count))
		}
		row[9].SetUint64(tblIndex.RegionMetric.FlowBytes)
		rows = append(rows, row)
	}
	return rows
}

// DataForAnalyzeStatus gets all the analyze jobs.
func DataForAnalyzeStatus() (rows [][]types.Datum) {
	for _, job := range statistics.GetAllAnalyzeJobs() {
		job.Lock()
		var startTime interface{}
		if job.StartTime.IsZero() {
			startTime = nil
		} else {
			startTime = types.Time{Time: types.FromGoTime(job.StartTime), Type: mysql.TypeDatetime}
		}
		rows = append(rows, types.MakeDatums(
			job.DBName,        // TABLE_SCHEMA
			job.TableName,     // TABLE_NAME
			job.PartitionName, // PARTITION_NAME
			job.JobInfo,       // JOB_INFO
			job.RowCount,      // ROW_COUNT
			startTime,         // START_TIME
			job.State,         // STATE
		))
		job.Unlock()
	}
	return
}

var tableNameToColumns = map[string][]columnInfo{
	tableSchemata:                           schemataCols,
	tableTables:                             tablesCols,
	tableColumns:                            columnsCols,
	tableStatistics:                         statisticsCols,
	tableCharacterSets:                      charsetCols,
	tableCollations:                         collationsCols,
	tableFiles:                              filesCols,
	tableProfiling:                          profilingCols,
	tablePartitions:                         partitionsCols,
	tableKeyColumm:                          keyColumnUsageCols,
	tableReferConst:                         referConstCols,
	tableSessionVar:                         sessionVarCols,
	tablePlugins:                            pluginsCols,
	tableConstraints:                        tableConstraintsCols,
	tableTriggers:                           tableTriggersCols,
	tableUserPrivileges:                     tableUserPrivilegesCols,
	tableSchemaPrivileges:                   tableSchemaPrivilegesCols,
	tableTablePrivileges:                    tableTablePrivilegesCols,
	tableColumnPrivileges:                   tableColumnPrivilegesCols,
	tableEngines:                            tableEnginesCols,
	tableViews:                              tableViewsCols,
	tableRoutines:                           tableRoutinesCols,
	tableParameters:                         tableParametersCols,
	tableEvents:                             tableEventsCols,
	tableGlobalStatus:                       tableGlobalStatusCols,
	tableGlobalVariables:                    tableGlobalVariablesCols,
	tableSessionStatus:                      tableSessionStatusCols,
	tableOptimizerTrace:                     tableOptimizerTraceCols,
	tableTableSpaces:                        tableTableSpacesCols,
	tableCollationCharacterSetApplicability: tableCollationCharacterSetApplicabilityCols,
	tableProcesslist:                        tableProcesslistCols,
	tableTiDBIndexes:                        tableTiDBIndexesCols,
	tableSlowLog:                            slowQueryCols,
	tableTiDBHotRegions:                     tableTiDBHotRegionsCols,
	tableTiKVStoreStatus:                    tableTiKVStoreStatusCols,
	tableAnalyzeStatus:                      tableAnalyzeStatusCols,
	tableTiKVRegionStatus:                   tableTiKVRegionStatusCols,
	tableTiKVRegionPeers:                    tableTiKVRegionPeersCols,
}

func createInfoSchemaTable(handle *Handle, meta *model.TableInfo) *infoschemaTable {
	columns := make([]*table.Column, len(meta.Columns))
	for i, col := range meta.Columns {
		columns[i] = table.ToColumn(col)
	}
	return &infoschemaTable{
		handle: handle,
		meta:   meta,
		cols:   columns,
	}
}

type infoschemaTable struct {
	handle *Handle
	meta   *model.TableInfo
	cols   []*table.Column
	rows   [][]types.Datum
}

// schemasSorter implements the sort.Interface interface, sorts DBInfo by name.
type schemasSorter []*model.DBInfo

func (s schemasSorter) Len() int {
	return len(s)
}

func (s schemasSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s schemasSorter) Less(i, j int) bool {
	return s[i].Name.L < s[j].Name.L
}

func (it *infoschemaTable) getRows(ctx sessionctx.Context, cols []*table.Column) (fullRows [][]types.Datum, err error) {
	is := it.handle.Get()
	dbs := is.AllSchemas()
	sort.Sort(schemasSorter(dbs))
	switch it.meta.Name.O {
	case tableSchemata:
		fullRows = dataForSchemata(dbs)
	case tableTables:
		fullRows, err = dataForTables(ctx, dbs)
	case tableTiDBIndexes:
		fullRows, err = dataForIndexes(ctx, dbs)
	case tableColumns:
		fullRows = dataForColumns(ctx, dbs)
	case tableStatistics:
		fullRows = dataForStatistics(dbs)
	case tableCharacterSets:
		fullRows = dataForCharacterSets()
	case tableCollations:
		fullRows = dataForCollations()
	case tableSessionVar:
		fullRows, err = dataForSessionVar(ctx)
	case tableConstraints:
		fullRows = dataForTableConstraints(dbs)
	case tableFiles:
	case tableProfiling:
		if v, ok := ctx.GetSessionVars().GetSystemVar("profiling"); ok && variable.TiDBOptOn(v) {
			fullRows = dataForPseudoProfiling()
		}
	case tablePartitions:
		fullRows, err = dataForPartitions(ctx, dbs)
	case tableKeyColumm:
		fullRows = dataForKeyColumnUsage(dbs)
	case tableReferConst:
	case tablePlugins, tableTriggers:
	case tableUserPrivileges:
		fullRows = dataForUserPrivileges(ctx)
	case tableEngines:
		fullRows = dataForEngines()
	case tableViews:
		fullRows, err = dataForViews(ctx, dbs)
	case tableRoutines:
	// TODO: Fill the following tables.
	case tableSchemaPrivileges:
	case tableTablePrivileges:
	case tableColumnPrivileges:
	case tableParameters:
	case tableEvents:
	case tableGlobalStatus:
	case tableGlobalVariables:
	case tableSessionStatus:
	case tableOptimizerTrace:
	case tableTableSpaces:
	case tableCollationCharacterSetApplicability:
		fullRows = dataForCollationCharacterSetApplicability()
	case tableProcesslist:
		fullRows = dataForProcesslist(ctx)
	case tableSlowLog:
		fullRows, err = dataForSlowLog(ctx)
	case tableTiDBHotRegions:
		fullRows, err = dataForTiDBHotRegions(ctx)
	case tableTiKVStoreStatus:
		fullRows, err = dataForTiKVStoreStatus(ctx)
	case tableAnalyzeStatus:
		fullRows = DataForAnalyzeStatus()
	case tableTiKVRegionStatus:
		fullRows, err = dataForTiKVRegionStatus(ctx)
	case tableTiKVRegionPeers:
		fullRows, err = dataForTikVRegionPeers(ctx)
	}
	if err != nil {
		return nil, err
	}
	if len(cols) == len(it.cols) {
		return
	}
	rows := make([][]types.Datum, len(fullRows))
	for i, fullRow := range fullRows {
		row := make([]types.Datum, len(cols))
		for j, col := range cols {
			row[j] = fullRow[col.Offset]
		}
		rows[i] = row
	}
	return rows, nil
}

// IterRecords implements table.Table IterRecords interface.
func (it *infoschemaTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}
	rows, err := it.getRows(ctx, cols)
	if err != nil {
		return err
	}
	for i, row := range rows {
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

// RowWithCols implements table.Table RowWithCols interface.
func (it *infoschemaTable) RowWithCols(ctx sessionctx.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

// Row implements table.Table Row interface.
func (it *infoschemaTable) Row(ctx sessionctx.Context, h int64) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

// Cols implements table.Table Cols interface.
func (it *infoschemaTable) Cols() []*table.Column {
	return it.cols
}

// WritableCols implements table.Table WritableCols interface.
func (it *infoschemaTable) WritableCols() []*table.Column {
	return it.cols
}

// Indices implements table.Table Indices interface.
func (it *infoschemaTable) Indices() []table.Index {
	return nil
}

// WritableIndices implements table.Table WritableIndices interface.
func (it *infoschemaTable) WritableIndices() []table.Index {
	return nil
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (it *infoschemaTable) DeletableIndices() []table.Index {
	return nil
}

// RecordPrefix implements table.Table RecordPrefix interface.
func (it *infoschemaTable) RecordPrefix() kv.Key {
	return nil
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (it *infoschemaTable) IndexPrefix() kv.Key {
	return nil
}

// FirstKey implements table.Table FirstKey interface.
func (it *infoschemaTable) FirstKey() kv.Key {
	return nil
}

// RecordKey implements table.Table RecordKey interface.
func (it *infoschemaTable) RecordKey(h int64) kv.Key {
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (it *infoschemaTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...*table.AddRecordOpt) (recordID int64, err error) {
	return 0, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (it *infoschemaTable) RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error {
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (it *infoschemaTable) UpdateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	return table.ErrUnsupportedOp
}

// AllocHandle implements table.Table AllocHandle interface.
func (it *infoschemaTable) AllocHandle(ctx sessionctx.Context) (int64, error) {
	return 0, table.ErrUnsupportedOp
}

// Allocator implements table.Table Allocator interface.
func (it *infoschemaTable) Allocator(ctx sessionctx.Context) autoid.Allocator {
	return nil
}

// RebaseAutoID implements table.Table RebaseAutoID interface.
func (it *infoschemaTable) RebaseAutoID(ctx sessionctx.Context, newBase int64, isSetStep bool) error {
	return table.ErrUnsupportedOp
}

// Meta implements table.Table Meta interface.
func (it *infoschemaTable) Meta() *model.TableInfo {
	return it.meta
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (it *infoschemaTable) GetPhysicalID() int64 {
	return it.meta.ID
}

// Seek implements table.Table Seek interface.
func (it *infoschemaTable) Seek(ctx sessionctx.Context, h int64) (int64, bool, error) {
	return 0, false, table.ErrUnsupportedOp
}

// Type implements table.Table Type interface.
func (it *infoschemaTable) Type() table.Type {
	return table.VirtualTable
}

// VirtualTable is a dummy table.Table implementation.
type VirtualTable struct{}

// IterRecords implements table.Table IterRecords interface.
func (vt *VirtualTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}
	return nil
}

// RowWithCols implements table.Table RowWithCols interface.
func (vt *VirtualTable) RowWithCols(ctx sessionctx.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

// Row implements table.Table Row interface.
func (vt *VirtualTable) Row(ctx sessionctx.Context, h int64) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

// Cols implements table.Table Cols interface.
func (vt *VirtualTable) Cols() []*table.Column {
	return nil
}

// WritableCols implements table.Table WritableCols interface.
func (vt *VirtualTable) WritableCols() []*table.Column {
	return nil
}

// Indices implements table.Table Indices interface.
func (vt *VirtualTable) Indices() []table.Index {
	return nil
}

// WritableIndices implements table.Table WritableIndices interface.
func (vt *VirtualTable) WritableIndices() []table.Index {
	return nil
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (vt *VirtualTable) DeletableIndices() []table.Index {
	return nil
}

// RecordPrefix implements table.Table RecordPrefix interface.
func (vt *VirtualTable) RecordPrefix() kv.Key {
	return nil
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (vt *VirtualTable) IndexPrefix() kv.Key {
	return nil
}

// FirstKey implements table.Table FirstKey interface.
func (vt *VirtualTable) FirstKey() kv.Key {
	return nil
}

// RecordKey implements table.Table RecordKey interface.
func (vt *VirtualTable) RecordKey(h int64) kv.Key {
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (vt *VirtualTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...*table.AddRecordOpt) (recordID int64, err error) {
	return 0, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (vt *VirtualTable) RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error {
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (vt *VirtualTable) UpdateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	return table.ErrUnsupportedOp
}

// AllocHandle implements table.Table AllocHandle interface.
func (vt *VirtualTable) AllocHandle(ctx sessionctx.Context) (int64, error) {
	return 0, table.ErrUnsupportedOp
}

// Allocator implements table.Table Allocator interface.
func (vt *VirtualTable) Allocator(ctx sessionctx.Context) autoid.Allocator {
	return nil
}

// RebaseAutoID implements table.Table RebaseAutoID interface.
func (vt *VirtualTable) RebaseAutoID(ctx sessionctx.Context, newBase int64, isSetStep bool) error {
	return table.ErrUnsupportedOp
}

// Meta implements table.Table Meta interface.
func (vt *VirtualTable) Meta() *model.TableInfo {
	return nil
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (vt *VirtualTable) GetPhysicalID() int64 {
	return 0
}

// Seek implements table.Table Seek interface.
func (vt *VirtualTable) Seek(ctx sessionctx.Context, h int64) (int64, bool, error) {
	return 0, false, table.ErrUnsupportedOp
}

// Type implements table.Table Type interface.
func (vt *VirtualTable) Type() table.Type {
	return table.VirtualTable
}
