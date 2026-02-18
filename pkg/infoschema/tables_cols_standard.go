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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package infoschema

import (
	"github.com/pingcap/tidb/pkg/ddl/resourcegroup"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)


var schemataCols = []columnInfo{
	{name: "CATALOG_NAME", tp: mysql.TypeVarchar, size: 512},
	{name: "SCHEMA_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "DEFAULT_CHARACTER_SET_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "DEFAULT_COLLATION_NAME", tp: mysql.TypeVarchar, size: 32},
	{name: "SQL_PATH", tp: mysql.TypeVarchar, size: 512},
	{name: "TIDB_PLACEMENT_POLICY_NAME", tp: mysql.TypeVarchar, size: 64},
}

var tablesCols = []columnInfo{
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 512},
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "ENGINE", tp: mysql.TypeVarchar, size: 64},
	{name: "VERSION", tp: mysql.TypeLonglong, size: 21},
	{name: "ROW_FORMAT", tp: mysql.TypeVarchar, size: 10},
	{name: "TABLE_ROWS", tp: mysql.TypeLonglong, size: 21},
	{name: "AVG_ROW_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "DATA_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "MAX_DATA_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "INDEX_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "DATA_FREE", tp: mysql.TypeLonglong, size: 21},
	{name: "AUTO_INCREMENT", tp: mysql.TypeLonglong, size: 21},
	{name: "CREATE_TIME", tp: mysql.TypeDatetime, size: 19},
	{name: "UPDATE_TIME", tp: mysql.TypeDatetime, size: 19},
	{name: "CHECK_TIME", tp: mysql.TypeDatetime, size: 19},
	{name: "TABLE_COLLATION", tp: mysql.TypeVarchar, size: 32, deflt: mysql.DefaultCollationName},
	{name: "CHECKSUM", tp: mysql.TypeLonglong, size: 21},
	{name: "CREATE_OPTIONS", tp: mysql.TypeVarchar, size: 255},
	{name: "TABLE_COMMENT", tp: mysql.TypeVarchar, size: 2048},
	{name: "TIDB_TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "TIDB_ROW_ID_SHARDING_INFO", tp: mysql.TypeVarchar, size: 255},
	{name: "TIDB_PK_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_PLACEMENT_POLICY_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_TABLE_MODE", tp: mysql.TypeVarchar, size: 16},
	{name: "TIDB_AFFINITY", tp: mysql.TypeVarchar, size: 128},
}

// See: http://dev.mysql.com/doc/refman/5.7/en/information-schema-columns-table.html
var columnsCols = []columnInfo{
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "COLUMN_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "ORDINAL_POSITION", tp: mysql.TypeLong, flag: mysql.UnsignedFlag},
	{name: "COLUMN_DEFAULT", tp: mysql.TypeBlob},
	{name: "IS_NULLABLE", tp: mysql.TypeVarchar, size: 3},
	{name: "DATA_TYPE", tp: mysql.TypeLongBlob},
	{name: "CHARACTER_MAXIMUM_LENGTH", tp: mysql.TypeLonglong},
	{name: "CHARACTER_OCTET_LENGTH", tp: mysql.TypeLonglong},
	{name: "NUMERIC_PRECISION", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag},
	{name: "NUMERIC_SCALE", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag},
	{name: "DATETIME_PRECISION", tp: mysql.TypeLong, flag: mysql.UnsignedFlag},
	{name: "CHARACTER_SET_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "COLLATION_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "COLUMN_TYPE", tp: mysql.TypeMediumBlob},
	{name: "COLUMN_KEY", tp: mysql.TypeVarchar, size: 3},
	{name: "EXTRA", tp: mysql.TypeVarchar, size: 256},
	{name: "PRIVILEGES", tp: mysql.TypeVarchar, size: 154},
	{name: "COLUMN_COMMENT", tp: mysql.TypeBlob},
	{name: "GENERATION_EXPRESSION", tp: mysql.TypeLongBlob, flag: mysql.NotNullFlag},
	{name: "SRS_ID", tp: mysql.TypeLong, flag: mysql.UnsignedFlag},
}

var columnStatisticsCols = []columnInfo{
	{name: "SCHEMA_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "COLUMN_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "HISTOGRAM", tp: mysql.TypeJSON, size: 51},
}

var statisticsCols = []columnInfo{
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 512},
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "NON_UNIQUE", tp: mysql.TypeVarchar, size: 1},
	{name: "INDEX_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "INDEX_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "SEQ_IN_INDEX", tp: mysql.TypeLonglong, size: 2},
	{name: "COLUMN_NAME", tp: mysql.TypeVarchar, size: 21},
	{name: "COLLATION", tp: mysql.TypeVarchar, size: 1},
	{name: "CARDINALITY", tp: mysql.TypeLonglong, size: 21},
	{name: "SUB_PART", tp: mysql.TypeLonglong, size: 3},
	{name: "PACKED", tp: mysql.TypeVarchar, size: 10},
	{name: "NULLABLE", tp: mysql.TypeVarchar, size: 3},
	{name: "INDEX_TYPE", tp: mysql.TypeVarchar, size: 16},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 16},
	{name: "INDEX_COMMENT", tp: mysql.TypeVarchar, size: 1024},
	{name: "IS_VISIBLE", tp: mysql.TypeVarchar, size: 3},
	{name: "Expression", tp: mysql.TypeVarchar, size: 64},
}

var profilingCols = []columnInfo{
	{name: "QUERY_ID", tp: mysql.TypeLong, size: 20},
	{name: "SEQ", tp: mysql.TypeLong, size: 20},
	{name: "STATE", tp: mysql.TypeVarchar, size: 30},
	{name: "DURATION", tp: mysql.TypeNewDecimal, size: 9},
	{name: "CPU_USER", tp: mysql.TypeNewDecimal, size: 9},
	{name: "CPU_SYSTEM", tp: mysql.TypeNewDecimal, size: 9},
	{name: "CONTEXT_VOLUNTARY", tp: mysql.TypeLong, size: 20},
	{name: "CONTEXT_INVOLUNTARY", tp: mysql.TypeLong, size: 20},
	{name: "BLOCK_OPS_IN", tp: mysql.TypeLong, size: 20},
	{name: "BLOCK_OPS_OUT", tp: mysql.TypeLong, size: 20},
	{name: "MESSAGES_SENT", tp: mysql.TypeLong, size: 20},
	{name: "MESSAGES_RECEIVED", tp: mysql.TypeLong, size: 20},
	{name: "PAGE_FAULTS_MAJOR", tp: mysql.TypeLong, size: 20},
	{name: "PAGE_FAULTS_MINOR", tp: mysql.TypeLong, size: 20},
	{name: "SWAPS", tp: mysql.TypeLong, size: 20},
	{name: "SOURCE_FUNCTION", tp: mysql.TypeVarchar, size: 30},
	{name: "SOURCE_FILE", tp: mysql.TypeVarchar, size: 20},
	{name: "SOURCE_LINE", tp: mysql.TypeLong, size: 20},
}

var charsetCols = []columnInfo{
	{name: "CHARACTER_SET_NAME", tp: mysql.TypeVarchar, size: 32},
	{name: "DEFAULT_COLLATE_NAME", tp: mysql.TypeVarchar, size: 32},
	{name: "DESCRIPTION", tp: mysql.TypeVarchar, size: 60},
	{name: "MAXLEN", tp: mysql.TypeLonglong, size: 3},
}

var collationsCols = []columnInfo{
	{name: "COLLATION_NAME", tp: mysql.TypeVarchar, size: 32},
	{name: "CHARACTER_SET_NAME", tp: mysql.TypeVarchar, size: 32},
	{name: "ID", tp: mysql.TypeLonglong, size: 11},
	{name: "IS_DEFAULT", tp: mysql.TypeVarchar, size: 3},
	{name: "IS_COMPILED", tp: mysql.TypeVarchar, size: 3},
	{name: "SORTLEN", tp: mysql.TypeLonglong, size: 3},
	{name: "PAD_ATTRIBUTE", tp: mysql.TypeVarchar, size: 9},
}

var keyColumnUsageCols = []columnInfo{
	{name: "CONSTRAINT_CATALOG", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "CONSTRAINT_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CONSTRAINT_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "COLUMN_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "ORDINAL_POSITION", tp: mysql.TypeLonglong, size: 10, flag: mysql.NotNullFlag},
	{name: "POSITION_IN_UNIQUE_CONSTRAINT", tp: mysql.TypeLonglong, size: 10},
	{name: "REFERENCED_TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "REFERENCED_TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "REFERENCED_COLUMN_NAME", tp: mysql.TypeVarchar, size: 64},
}

// See http://dev.mysql.com/doc/refman/5.7/en/information-schema-referential-constraints-table.html
var referConstCols = []columnInfo{
	{name: "CONSTRAINT_CATALOG", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "CONSTRAINT_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CONSTRAINT_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "UNIQUE_CONSTRAINT_CATALOG", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "UNIQUE_CONSTRAINT_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "UNIQUE_CONSTRAINT_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "MATCH_OPTION", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "UPDATE_RULE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "DELETE_RULE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "REFERENCED_TABLE_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
}

// See https://dev.mysql.com/doc/refman/5.7/en/information-schema-plugins-table.html
var pluginsCols = []columnInfo{
	{name: "PLUGIN_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "PLUGIN_VERSION", tp: mysql.TypeVarchar, size: 20},
	{name: "PLUGIN_STATUS", tp: mysql.TypeVarchar, size: 10},
	{name: "PLUGIN_TYPE", tp: mysql.TypeVarchar, size: 80},
	{name: "PLUGIN_TYPE_VERSION", tp: mysql.TypeVarchar, size: 20},
	{name: "PLUGIN_LIBRARY", tp: mysql.TypeVarchar, size: 64},
	{name: "PLUGIN_LIBRARY_VERSION", tp: mysql.TypeVarchar, size: 20},
	{name: "PLUGIN_AUTHOR", tp: mysql.TypeVarchar, size: 64},
	{name: "PLUGIN_DESCRIPTION", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "PLUGIN_LICENSE", tp: mysql.TypeVarchar, size: 80},
	{name: "LOAD_OPTION", tp: mysql.TypeVarchar, size: 64},
}

// See https://dev.mysql.com/doc/refman/5.7/en/information-schema-partitions-table.html
var partitionsCols = []columnInfo{
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 512},
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "PARTITION_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "SUBPARTITION_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "PARTITION_ORDINAL_POSITION", tp: mysql.TypeLonglong, size: 21},
	{name: "SUBPARTITION_ORDINAL_POSITION", tp: mysql.TypeLonglong, size: 21},
	{name: "PARTITION_METHOD", tp: mysql.TypeVarchar, size: 18},
	{name: "SUBPARTITION_METHOD", tp: mysql.TypeVarchar, size: 12},
	{name: "PARTITION_EXPRESSION", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "SUBPARTITION_EXPRESSION", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "PARTITION_DESCRIPTION", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "TABLE_ROWS", tp: mysql.TypeLonglong, size: 21},
	{name: "AVG_ROW_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "DATA_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "MAX_DATA_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "INDEX_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "DATA_FREE", tp: mysql.TypeLonglong, size: 21},
	{name: "CREATE_TIME", tp: mysql.TypeDatetime},
	{name: "UPDATE_TIME", tp: mysql.TypeDatetime},
	{name: "CHECK_TIME", tp: mysql.TypeDatetime},
	{name: "CHECKSUM", tp: mysql.TypeLonglong, size: 21},
	{name: "PARTITION_COMMENT", tp: mysql.TypeVarchar, size: 80},
	{name: "NODEGROUP", tp: mysql.TypeVarchar, size: 12},
	{name: "TABLESPACE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_PARTITION_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "TIDB_PLACEMENT_POLICY_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_AFFINITY", tp: mysql.TypeVarchar, size: 128},
}

var tableConstraintsCols = []columnInfo{
	{name: "CONSTRAINT_CATALOG", tp: mysql.TypeVarchar, size: 512},
	{name: "CONSTRAINT_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "CONSTRAINT_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "CONSTRAINT_TYPE", tp: mysql.TypeVarchar, size: 64},
}

var tableTriggersCols = []columnInfo{
	{name: "TRIGGER_CATALOG", tp: mysql.TypeVarchar, size: 512},
	{name: "TRIGGER_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TRIGGER_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "EVENT_MANIPULATION", tp: mysql.TypeVarchar, size: 6},
	{name: "EVENT_OBJECT_CATALOG", tp: mysql.TypeVarchar, size: 512},
	{name: "EVENT_OBJECT_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "EVENT_OBJECT_TABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "ACTION_ORDER", tp: mysql.TypeLonglong, size: 4},
	{name: "ACTION_CONDITION", tp: mysql.TypeBlob, size: -1},
	{name: "ACTION_STATEMENT", tp: mysql.TypeBlob, size: -1},
	{name: "ACTION_ORIENTATION", tp: mysql.TypeVarchar, size: 9},
	{name: "ACTION_TIMING", tp: mysql.TypeVarchar, size: 6},
	{name: "ACTION_REFERENCE_OLD_TABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "ACTION_REFERENCE_NEW_TABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "ACTION_REFERENCE_OLD_ROW", tp: mysql.TypeVarchar, size: 3},
	{name: "ACTION_REFERENCE_NEW_ROW", tp: mysql.TypeVarchar, size: 3},
	{name: "CREATED", tp: mysql.TypeDatetime, size: 2},
	{name: "SQL_MODE", tp: mysql.TypeVarchar, size: 8192},
	{name: "DEFINER", tp: mysql.TypeVarchar, size: 77},
	{name: "CHARACTER_SET_CLIENT", tp: mysql.TypeVarchar, size: 32},
	{name: "COLLATION_CONNECTION", tp: mysql.TypeVarchar, size: 32},
	{name: "DATABASE_COLLATION", tp: mysql.TypeVarchar, size: 32},
}

var tableUserPrivilegesCols = []columnInfo{
	{name: "GRANTEE", tp: mysql.TypeVarchar, size: 81},
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 512},
	{name: "PRIVILEGE_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "IS_GRANTABLE", tp: mysql.TypeVarchar, size: 3},
}

var tableSchemaPrivilegesCols = []columnInfo{
	{name: "GRANTEE", tp: mysql.TypeVarchar, size: 81, flag: mysql.NotNullFlag},
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "PRIVILEGE_TYPE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "IS_GRANTABLE", tp: mysql.TypeVarchar, size: 3, flag: mysql.NotNullFlag},
}

var tableTablePrivilegesCols = []columnInfo{
	{name: "GRANTEE", tp: mysql.TypeVarchar, size: 81, flag: mysql.NotNullFlag},
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "PRIVILEGE_TYPE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "IS_GRANTABLE", tp: mysql.TypeVarchar, size: 3, flag: mysql.NotNullFlag},
}

var tableColumnPrivilegesCols = []columnInfo{
	{name: "GRANTEE", tp: mysql.TypeVarchar, size: 81, flag: mysql.NotNullFlag},
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "COLUMN_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "PRIVILEGE_TYPE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "IS_GRANTABLE", tp: mysql.TypeVarchar, size: 3, flag: mysql.NotNullFlag},
}

var tableEnginesCols = []columnInfo{
	{name: "ENGINE", tp: mysql.TypeVarchar, size: 64},
	{name: "SUPPORT", tp: mysql.TypeVarchar, size: 8},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 80},
	{name: "TRANSACTIONS", tp: mysql.TypeVarchar, size: 3},
	{name: "XA", tp: mysql.TypeVarchar, size: 3},
	{name: "SAVEPOINTS", tp: mysql.TypeVarchar, size: 3},
}

var tableViewsCols = []columnInfo{
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "VIEW_DEFINITION", tp: mysql.TypeLongBlob, flag: mysql.NotNullFlag},
	{name: "CHECK_OPTION", tp: mysql.TypeVarchar, size: 8, flag: mysql.NotNullFlag},
	{name: "IS_UPDATABLE", tp: mysql.TypeVarchar, size: 3, flag: mysql.NotNullFlag},
	{name: "DEFINER", tp: mysql.TypeVarchar, size: 77, flag: mysql.NotNullFlag},
	{name: "SECURITY_TYPE", tp: mysql.TypeVarchar, size: 7, flag: mysql.NotNullFlag},
	{name: "CHARACTER_SET_CLIENT", tp: mysql.TypeVarchar, size: 32, flag: mysql.NotNullFlag},
	{name: "COLLATION_CONNECTION", tp: mysql.TypeVarchar, size: 32, flag: mysql.NotNullFlag},
}

var tableRoutinesCols = []columnInfo{
	{name: "SPECIFIC_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "ROUTINE_CATALOG", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "ROUTINE_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "ROUTINE_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "ROUTINE_TYPE", tp: mysql.TypeVarchar, size: 9, flag: mysql.NotNullFlag},
	{name: "DATA_TYPE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CHARACTER_MAXIMUM_LENGTH", tp: mysql.TypeLong, size: 21},
	{name: "CHARACTER_OCTET_LENGTH", tp: mysql.TypeLong, size: 21},
	{name: "NUMERIC_PRECISION", tp: mysql.TypeLonglong, size: 21},
	{name: "NUMERIC_SCALE", tp: mysql.TypeLong, size: 21},
	{name: "DATETIME_PRECISION", tp: mysql.TypeLonglong, size: 21},
	{name: "CHARACTER_SET_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "COLLATION_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "DTD_IDENTIFIER", tp: mysql.TypeLongBlob},
	{name: "ROUTINE_BODY", tp: mysql.TypeVarchar, size: 8, flag: mysql.NotNullFlag},
	{name: "ROUTINE_DEFINITION", tp: mysql.TypeLongBlob},
	{name: "EXTERNAL_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "EXTERNAL_LANGUAGE", tp: mysql.TypeVarchar, size: 64},
	{name: "PARAMETER_STYLE", tp: mysql.TypeVarchar, size: 8, flag: mysql.NotNullFlag},
	{name: "IS_DETERMINISTIC", tp: mysql.TypeVarchar, size: 3, flag: mysql.NotNullFlag},
	{name: "SQL_DATA_ACCESS", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "SQL_PATH", tp: mysql.TypeVarchar, size: 64},
	{name: "SECURITY_TYPE", tp: mysql.TypeVarchar, size: 7, flag: mysql.NotNullFlag},
	{name: "CREATED", tp: mysql.TypeDatetime, flag: mysql.NotNullFlag, deflt: "0000-00-00 00:00:00"},
	{name: "LAST_ALTERED", tp: mysql.TypeDatetime, flag: mysql.NotNullFlag, deflt: "0000-00-00 00:00:00"},
	{name: "SQL_MODE", tp: mysql.TypeVarchar, size: 8192, flag: mysql.NotNullFlag},
	{name: "ROUTINE_COMMENT", tp: mysql.TypeLongBlob},
	{name: "DEFINER", tp: mysql.TypeVarchar, size: 77, flag: mysql.NotNullFlag},
	{name: "CHARACTER_SET_CLIENT", tp: mysql.TypeVarchar, size: 32, flag: mysql.NotNullFlag},
	{name: "COLLATION_CONNECTION", tp: mysql.TypeVarchar, size: 32, flag: mysql.NotNullFlag},
	{name: "DATABASE_COLLATION", tp: mysql.TypeVarchar, size: 32, flag: mysql.NotNullFlag},
}

var tableParametersCols = []columnInfo{
	{name: "SPECIFIC_CATALOG", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "SPECIFIC_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "SPECIFIC_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "ORDINAL_POSITION", tp: mysql.TypeVarchar, size: 21, flag: mysql.NotNullFlag},
	{name: "PARAMETER_MODE", tp: mysql.TypeVarchar, size: 5},
	{name: "PARAMETER_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "DATA_TYPE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CHARACTER_MAXIMUM_LENGTH", tp: mysql.TypeVarchar, size: 21},
	{name: "CHARACTER_OCTET_LENGTH", tp: mysql.TypeVarchar, size: 21},
	{name: "NUMERIC_PRECISION", tp: mysql.TypeVarchar, size: 21},
	{name: "NUMERIC_SCALE", tp: mysql.TypeVarchar, size: 21},
	{name: "DATETIME_PRECISION", tp: mysql.TypeVarchar, size: 21},
	{name: "CHARACTER_SET_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "COLLATION_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "DTD_IDENTIFIER", tp: mysql.TypeLongBlob, flag: mysql.NotNullFlag},
	{name: "ROUTINE_TYPE", tp: mysql.TypeVarchar, size: 9, flag: mysql.NotNullFlag},
}

var tableEventsCols = []columnInfo{
	{name: "EVENT_CATALOG", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "EVENT_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "EVENT_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "DEFINER", tp: mysql.TypeVarchar, size: 77, flag: mysql.NotNullFlag},
	{name: "TIME_ZONE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "EVENT_BODY", tp: mysql.TypeVarchar, size: 8, flag: mysql.NotNullFlag},
	{name: "EVENT_DEFINITION", tp: mysql.TypeLongBlob},
	{name: "EVENT_TYPE", tp: mysql.TypeVarchar, size: 9, flag: mysql.NotNullFlag},
	{name: "EXECUTE_AT", tp: mysql.TypeDatetime},
	{name: "INTERVAL_VALUE", tp: mysql.TypeVarchar, size: 256},
	{name: "INTERVAL_FIELD", tp: mysql.TypeVarchar, size: 18},
	{name: "SQL_MODE", tp: mysql.TypeVarchar, size: 8192, flag: mysql.NotNullFlag},
	{name: "STARTS", tp: mysql.TypeDatetime},
	{name: "ENDS", tp: mysql.TypeDatetime},
	{name: "STATUS", tp: mysql.TypeVarchar, size: 18, flag: mysql.NotNullFlag},
	{name: "ON_COMPLETION", tp: mysql.TypeVarchar, size: 12, flag: mysql.NotNullFlag},
	{name: "CREATED", tp: mysql.TypeDatetime, flag: mysql.NotNullFlag, deflt: "0000-00-00 00:00:00"},
	{name: "LAST_ALTERED", tp: mysql.TypeDatetime, flag: mysql.NotNullFlag, deflt: "0000-00-00 00:00:00"},
	{name: "LAST_EXECUTED", tp: mysql.TypeDatetime},
	{name: "EVENT_COMMENT", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "ORIGINATOR", tp: mysql.TypeLong, size: 10, flag: mysql.NotNullFlag, deflt: 0},
	{name: "CHARACTER_SET_CLIENT", tp: mysql.TypeVarchar, size: 32, flag: mysql.NotNullFlag},
	{name: "COLLATION_CONNECTION", tp: mysql.TypeVarchar, size: 32, flag: mysql.NotNullFlag},
	{name: "DATABASE_COLLATION", tp: mysql.TypeVarchar, size: 32, flag: mysql.NotNullFlag},
}

var tableOptimizerTraceCols = []columnInfo{
	{name: "QUERY", tp: mysql.TypeLongBlob, flag: mysql.NotNullFlag, deflt: ""},
	{name: "TRACE", tp: mysql.TypeLongBlob, flag: mysql.NotNullFlag, deflt: ""},
	{name: "MISSING_BYTES_BEYOND_MAX_MEM_SIZE", tp: mysql.TypeShort, size: 20, flag: mysql.NotNullFlag, deflt: 0},
	{name: "INSUFFICIENT_PRIVILEGES", tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, deflt: 0},
}

var tableTableSpacesCols = []columnInfo{
	{name: "TABLESPACE_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag, deflt: ""},
	{name: "ENGINE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag, deflt: ""},
	{name: "TABLESPACE_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "LOGFILE_GROUP_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "EXTENT_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "AUTOEXTEND_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "MAXIMUM_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "NODEGROUP_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "TABLESPACE_COMMENT", tp: mysql.TypeVarchar, size: 2048},
}

var tableCollationCharacterSetApplicabilityCols = []columnInfo{
	{name: "COLLATION_NAME", tp: mysql.TypeVarchar, size: 32, flag: mysql.NotNullFlag},
	{name: "CHARACTER_SET_NAME", tp: mysql.TypeVarchar, size: 32, flag: mysql.NotNullFlag},
}

var tableProcesslistCols = []columnInfo{
	{name: "ID", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag | mysql.UnsignedFlag, deflt: 0},
	{name: "USER", tp: mysql.TypeVarchar, size: 16, flag: mysql.NotNullFlag, deflt: ""},
	{name: "HOST", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag, deflt: ""},
	{name: "DB", tp: mysql.TypeVarchar, size: 64},
	{name: "COMMAND", tp: mysql.TypeVarchar, size: 16, flag: mysql.NotNullFlag, deflt: ""},
	{name: "TIME", tp: mysql.TypeLong, size: 7, flag: mysql.NotNullFlag, deflt: 0},
	{name: "STATE", tp: mysql.TypeVarchar, size: 7},
	{name: "INFO", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "DIGEST", tp: mysql.TypeVarchar, size: 64, deflt: ""},
	{name: "MEM", tp: mysql.TypeLonglong, size: 21, flag: mysql.UnsignedFlag},
	{name: "MEM_ARBITRATION", tp: mysql.TypeDouble, size: 22},
	{name: "MEM_WAIT_ARBITRATE_START", tp: mysql.TypeVarchar, size: 32},
	{name: "MEM_WAIT_ARBITRATE_BYTES", tp: mysql.TypeLonglong, size: 21},
	{name: "DISK", tp: mysql.TypeLonglong, size: 21, flag: mysql.UnsignedFlag},
	{name: "TxnStart", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag, deflt: ""},
	{name: "RESOURCE_GROUP", tp: mysql.TypeVarchar, size: resourcegroup.MaxGroupNameLength, flag: mysql.NotNullFlag, deflt: ""},
	{name: "SESSION_ALIAS", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag, deflt: ""},
	{name: "ROWS_AFFECTED", tp: mysql.TypeLonglong, size: 21, flag: mysql.UnsignedFlag},
	{name: "TIDB_CPU", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag, deflt: 0},
	{name: "TIKV_CPU", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag, deflt: 0},
}

var tableTiDBIndexesCols = []columnInfo{
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "NON_UNIQUE", tp: mysql.TypeLonglong, size: 21},
	{name: "KEY_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "SEQ_IN_INDEX", tp: mysql.TypeLonglong, size: 21},
	{name: "COLUMN_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "SUB_PART", tp: mysql.TypeLonglong, size: 21},
	{name: "INDEX_COMMENT", tp: mysql.TypeVarchar, size: 1024},
	{name: "Expression", tp: mysql.TypeVarchar, size: 64},
	{name: "INDEX_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "IS_VISIBLE", tp: mysql.TypeVarchar, size: 64},
	{name: "CLUSTERED", tp: mysql.TypeVarchar, size: 64},
	{name: "IS_GLOBAL", tp: mysql.TypeLonglong, size: 21},
	{name: "PREDICATE", tp: mysql.TypeVarchar, size: 1024},
}

var slowQueryCols = []columnInfo{
	{name: variable.SlowLogTimeStr, tp: mysql.TypeTimestamp, size: 26, decimal: 6, flag: mysql.PriKeyFlag | mysql.NotNullFlag | mysql.BinaryFlag},
	{name: variable.SlowLogTxnStartTSStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: variable.SlowLogUserStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.SlowLogHostStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.SlowLogConnIDStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: variable.SlowLogSessAliasStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.SlowLogExecRetryCount, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: variable.SlowLogExecRetryTime, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogQueryTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogParseTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogCompileTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogRewriteTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogPreprocSubQueriesStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: variable.SlowLogPreProcSubQueryTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogOptimizeTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogWaitTSTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.PreWriteTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.WaitPrewriteBinlogTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.CommitTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.GetCommitTSTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.CommitBackoffTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.BackoffTypesStr, tp: mysql.TypeVarchar, size: 64},
	{name: execdetails.ResolveLockTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.LocalLatchWaitTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.WriteKeysStr, tp: mysql.TypeLonglong, size: 22},
	{name: execdetails.WriteSizeStr, tp: mysql.TypeLonglong, size: 22},
	{name: execdetails.PrewriteRegionStr, tp: mysql.TypeLonglong, size: 22},
	{name: execdetails.TxnRetryStr, tp: mysql.TypeLonglong, size: 22},
	{name: execdetails.CopTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.ProcessTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.WaitTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.BackoffTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.LockKeysTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.RequestCountStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: execdetails.TotalKeysStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: execdetails.ProcessKeysStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: execdetails.RocksdbDeleteSkippedCountStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: execdetails.RocksdbKeySkippedCountStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: execdetails.RocksdbBlockCacheHitCountStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: execdetails.RocksdbBlockReadCountStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: execdetails.RocksdbBlockReadByteStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
	{name: variable.SlowLogDBStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.SlowLogIndexNamesStr, tp: mysql.TypeVarchar, size: 100},
	{name: variable.SlowLogIsInternalStr, tp: mysql.TypeTiny, size: 1},
	{name: variable.SlowLogDigestStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.SlowLogStatsInfoStr, tp: mysql.TypeVarchar, size: 512},
	{name: variable.SlowLogCopProcAvg, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogCopProcP90, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogCopProcMax, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogCopProcAddr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.SlowLogCopWaitAvg, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogCopWaitP90, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogCopWaitMax, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogCopWaitAddr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.SlowLogMemMax, tp: mysql.TypeLonglong, size: 20},
	{name: variable.SlowLogMemArbitration, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogDiskMax, tp: mysql.TypeLonglong, size: 20},
	{name: variable.SlowLogKVTotal, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogPDTotal, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogBackoffTotal, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogUnpackedBytesSentTiKVTotal, tp: mysql.TypeLonglong, size: 20},
	{name: variable.SlowLogUnpackedBytesReceivedTiKVTotal, tp: mysql.TypeLonglong, size: 20},
	{name: variable.SlowLogUnpackedBytesSentTiKVCrossZone, tp: mysql.TypeLonglong, size: 20},
	{name: variable.SlowLogUnpackedBytesReceivedTiKVCrossZone, tp: mysql.TypeLonglong, size: 20},
	{name: variable.SlowLogUnpackedBytesSentTiFlashTotal, tp: mysql.TypeLonglong, size: 20},
	{name: variable.SlowLogUnpackedBytesReceivedTiFlashTotal, tp: mysql.TypeLonglong, size: 20},
	{name: variable.SlowLogUnpackedBytesSentTiFlashCrossZone, tp: mysql.TypeLonglong, size: 20},
	{name: variable.SlowLogUnpackedBytesReceivedTiFlashCrossZone, tp: mysql.TypeLonglong, size: 20},
	{name: variable.SlowLogWriteSQLRespTotal, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogResultRows, tp: mysql.TypeLonglong, size: 22},
	{name: variable.SlowLogWarnings, tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: variable.SlowLogBackoffDetail, tp: mysql.TypeVarchar, size: 4096},
	{name: variable.SlowLogPrepared, tp: mysql.TypeTiny, size: 1},
	{name: variable.SlowLogSucc, tp: mysql.TypeTiny, size: 1},
	{name: variable.SlowLogIsExplicitTxn, tp: mysql.TypeTiny, size: 1},
	{name: variable.SlowLogIsWriteCacheTable, tp: mysql.TypeTiny, size: 1},
	{name: variable.SlowLogPlanFromCache, tp: mysql.TypeTiny, size: 1},
	{name: variable.SlowLogPlanFromBinding, tp: mysql.TypeTiny, size: 1},
	{name: variable.SlowLogHasMoreResults, tp: mysql.TypeTiny, size: 1},
	{name: variable.SlowLogResourceGroup, tp: mysql.TypeVarchar, size: 64},
	{name: variable.SlowLogRRU, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogWRU, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogWaitRUDuration, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogTidbCPUUsageDuration, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogTikvCPUUsageDuration, tp: mysql.TypeDouble, size: 22},
	{name: variable.SlowLogStorageFromKV, tp: mysql.TypeTiny, size: 1},
	{name: variable.SlowLogStorageFromMPP, tp: mysql.TypeTiny, size: 1},
	{name: variable.SlowLogPlan, tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: variable.SlowLogPlanDigest, tp: mysql.TypeVarchar, size: 128},
	{name: variable.SlowLogBinaryPlan, tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: variable.SlowLogPrevStmt, tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: variable.SlowLogQuerySQLStr, tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
}

// TableTiDBHotRegionsCols is TiDB hot region mem table columns.
var TableTiDBHotRegionsCols = []columnInfo{
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "INDEX_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "DB_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "INDEX_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "REGION_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "MAX_HOT_DEGREE", tp: mysql.TypeLonglong, size: 21},
	{name: "REGION_COUNT", tp: mysql.TypeLonglong, size: 21},
	{name: "FLOW_BYTES", tp: mysql.TypeLonglong, size: 21},
}

// TableTiDBHotRegionsHistoryCols is TiDB hot region history mem table columns.
var TableTiDBHotRegionsHistoryCols = []columnInfo{
	{name: "UPDATE_TIME", tp: mysql.TypeTimestamp, size: 26, decimal: 6},
	{name: "DB_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "INDEX_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "INDEX_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "REGION_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "STORE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "PEER_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "IS_LEARNER", tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, deflt: 0},
	{name: "IS_LEADER", tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, deflt: 0},
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "HOT_DEGREE", tp: mysql.TypeLonglong, size: 21},
	{name: "FLOW_BYTES", tp: mysql.TypeDouble, size: 22},
	{name: "KEY_RATE", tp: mysql.TypeDouble, size: 22},
	{name: "QUERY_RATE", tp: mysql.TypeDouble, size: 22},
}

// GetTableTiDBHotRegionsHistoryCols is to get TableTiDBHotRegionsHistoryCols.
// It is an optimization because Go does’t support const arrays. The solution  is to use initialization functions.
// It is useful in the BCE optimization.
// https://go101.org/article/bounds-check-elimination.html
func GetTableTiDBHotRegionsHistoryCols() []columnInfo {
	return TableTiDBHotRegionsHistoryCols
}

// TableTiKVStoreStatusCols is TiDB kv store status columns.
var TableTiKVStoreStatusCols = []columnInfo{
	{name: "STORE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "ADDRESS", tp: mysql.TypeVarchar, size: 64},
	{name: "STORE_STATE", tp: mysql.TypeLonglong, size: 21},
	{name: "STORE_STATE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "LABEL", tp: mysql.TypeJSON, size: 51},
	{name: "VERSION", tp: mysql.TypeVarchar, size: 64},
	{name: "CAPACITY", tp: mysql.TypeVarchar, size: 64},
	{name: "AVAILABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "LEADER_COUNT", tp: mysql.TypeLonglong, size: 21},
	{name: "LEADER_WEIGHT", tp: mysql.TypeDouble, size: 22},
	{name: "LEADER_SCORE", tp: mysql.TypeDouble, size: 22},
	{name: "LEADER_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "REGION_COUNT", tp: mysql.TypeLonglong, size: 21},
	{name: "REGION_WEIGHT", tp: mysql.TypeDouble, size: 22},
	{name: "REGION_SCORE", tp: mysql.TypeDouble, size: 22},
	{name: "REGION_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "START_TS", tp: mysql.TypeDatetime},
	{name: "LAST_HEARTBEAT_TS", tp: mysql.TypeDatetime},
	{name: "UPTIME", tp: mysql.TypeVarchar, size: 64},
}

var tableAnalyzeStatusCols = []columnInfo{
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "PARTITION_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "JOB_INFO", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "PROCESSED_ROWS", tp: mysql.TypeLonglong, size: 21, flag: mysql.UnsignedFlag},
	{name: "START_TIME", tp: mysql.TypeDatetime},
	{name: "END_TIME", tp: mysql.TypeDatetime},
	{name: "STATE", tp: mysql.TypeVarchar, size: 64},
	{name: "FAIL_REASON", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 512},
	{name: "PROCESS_ID", tp: mysql.TypeLonglong, size: 21, flag: mysql.UnsignedFlag},
	{name: "REMAINING_SECONDS", tp: mysql.TypeVarchar, size: 512},
	{name: "PROGRESS", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "ESTIMATED_TOTAL_ROWS", tp: mysql.TypeLonglong, size: 21, flag: mysql.UnsignedFlag},
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
	{name: "IS_PARTITION", tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, deflt: 0},
	{name: "PARTITION_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "PARTITION_NAME", tp: mysql.TypeVarchar, size: 64},
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

// GetTableTiKVRegionPeersCols is to get TableTiKVRegionPeersCols.
// It is an optimization because Go does’t support const arrays. The solution  is to use initialization functions.
// It is useful in the BCE optimization.
// https://go101.org/article/bounds-check-elimination.html
func GetTableTiKVRegionPeersCols() []columnInfo {
	return TableTiKVRegionPeersCols
}
