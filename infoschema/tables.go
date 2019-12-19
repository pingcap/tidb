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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain/infosync"
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
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	tableSchemata                           = "SCHEMATA"
	tableTables                             = "TABLES"
	tableColumns                            = "COLUMNS"
	tableColumnStatistics                   = "COLUMN_STATISTICS"
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
	tableTiDBServersInfo                    = "TIDB_SERVERS_INFO"
	tableClusterInfo                        = "CLUSTER_INFO"
	// TableClusterConfig is the string constant of cluster configuration memory table
	TableClusterConfig = "CLUSTER_CONFIG"
	// TableClusterLog is the string constant of cluster log memory table
	TableClusterLog     = "CLUSTER_LOG"
	tableClusterLoad    = "CLUSTER_LOAD"
	tableTiFlashReplica = "TIFLASH_REPLICA"
)

var tableIDMap = map[string]int64{
	tableSchemata:                           autoid.InformationSchemaDBID + 1,
	tableTables:                             autoid.InformationSchemaDBID + 2,
	tableColumns:                            autoid.InformationSchemaDBID + 3,
	tableColumnStatistics:                   autoid.InformationSchemaDBID + 4,
	tableStatistics:                         autoid.InformationSchemaDBID + 5,
	tableCharacterSets:                      autoid.InformationSchemaDBID + 6,
	tableCollations:                         autoid.InformationSchemaDBID + 7,
	tableFiles:                              autoid.InformationSchemaDBID + 8,
	catalogVal:                              autoid.InformationSchemaDBID + 9,
	tableProfiling:                          autoid.InformationSchemaDBID + 10,
	tablePartitions:                         autoid.InformationSchemaDBID + 11,
	tableKeyColumm:                          autoid.InformationSchemaDBID + 12,
	tableReferConst:                         autoid.InformationSchemaDBID + 13,
	tableSessionVar:                         autoid.InformationSchemaDBID + 14,
	tablePlugins:                            autoid.InformationSchemaDBID + 15,
	tableConstraints:                        autoid.InformationSchemaDBID + 16,
	tableTriggers:                           autoid.InformationSchemaDBID + 17,
	tableUserPrivileges:                     autoid.InformationSchemaDBID + 18,
	tableSchemaPrivileges:                   autoid.InformationSchemaDBID + 19,
	tableTablePrivileges:                    autoid.InformationSchemaDBID + 20,
	tableColumnPrivileges:                   autoid.InformationSchemaDBID + 21,
	tableEngines:                            autoid.InformationSchemaDBID + 22,
	tableViews:                              autoid.InformationSchemaDBID + 23,
	tableRoutines:                           autoid.InformationSchemaDBID + 24,
	tableParameters:                         autoid.InformationSchemaDBID + 25,
	tableEvents:                             autoid.InformationSchemaDBID + 26,
	tableGlobalStatus:                       autoid.InformationSchemaDBID + 27,
	tableGlobalVariables:                    autoid.InformationSchemaDBID + 28,
	tableSessionStatus:                      autoid.InformationSchemaDBID + 29,
	tableOptimizerTrace:                     autoid.InformationSchemaDBID + 30,
	tableTableSpaces:                        autoid.InformationSchemaDBID + 31,
	tableCollationCharacterSetApplicability: autoid.InformationSchemaDBID + 32,
	tableProcesslist:                        autoid.InformationSchemaDBID + 33,
	tableTiDBIndexes:                        autoid.InformationSchemaDBID + 34,
	tableSlowLog:                            autoid.InformationSchemaDBID + 35,
	tableTiDBHotRegions:                     autoid.InformationSchemaDBID + 36,
	tableTiKVStoreStatus:                    autoid.InformationSchemaDBID + 37,
	tableAnalyzeStatus:                      autoid.InformationSchemaDBID + 38,
	tableTiKVRegionStatus:                   autoid.InformationSchemaDBID + 39,
	tableTiKVRegionPeers:                    autoid.InformationSchemaDBID + 40,
	tableTiDBServersInfo:                    autoid.InformationSchemaDBID + 41,
	tableClusterInfo:                        autoid.InformationSchemaDBID + 42,
	TableClusterConfig:                      autoid.InformationSchemaDBID + 43,
	tableClusterLoad:                        autoid.InformationSchemaDBID + 44,
	tableTiFlashReplica:                     autoid.InformationSchemaDBID + 45,
	clusterTableSlowLog:                     autoid.InformationSchemaDBID + 46,
	clusterTableProcesslist:                 autoid.InformationSchemaDBID + 47,
	TableClusterLog:                         autoid.InformationSchemaDBID + 48,
}

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
	{"TIDB_ROW_ID_SHARDING_INFO", mysql.TypeVarchar, 255, 0, nil, nil},
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

var columnStatisticsCols = []columnInfo{
	{"SCHEMA_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"COLUMN_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"HISTOGRAM", mysql.TypeJSON, 51, 0, nil, nil},
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
	{"LEADER_WEIGHT", mysql.TypeDouble, 22, 0, nil, nil},
	{"LEADER_SCORE", mysql.TypeDouble, 22, 0, nil, nil},
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
	{"TABLE_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DB_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"IS_INDEX", mysql.TypeTiny, 1, mysql.NotNullFlag, 0, nil},
	{"INDEX_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"INDEX_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"EPOCH_CONF_VER", mysql.TypeLonglong, 21, 0, nil, nil},
	{"EPOCH_VERSION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"WRITTEN_BYTES", mysql.TypeLonglong, 21, 0, nil, nil},
	{"READ_BYTES", mysql.TypeLonglong, 21, 0, nil, nil},
	{"APPROXIMATE_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"APPROXIMATE_KEYS", mysql.TypeLonglong, 21, 0, nil, nil},
}

var tableTiKVRegionPeersCols = []columnInfo{
	{"REGION_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"TABLE_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DB_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"IS_INDEX", mysql.TypeTiny, 1, mysql.NotNullFlag, 0, nil},
	{"INDEX_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"INDEX_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PEER_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"STORE_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"IS_LEARNER", mysql.TypeTiny, 1, mysql.NotNullFlag, 0, nil},
	{"IS_LEADER", mysql.TypeTiny, 1, mysql.NotNullFlag, 0, nil},
	{"STATUS", mysql.TypeVarchar, 10, 0, 0, nil},
	{"DOWN_SECONDS", mysql.TypeLonglong, 21, 0, 0, nil},
}

var tableTiDBServersInfoCols = []columnInfo{
	{"DDL_ID", mysql.TypeVarchar, 64, 0, nil, nil},
	{"IP", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PORT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"STATUS_PORT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"LEASE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"VERSION", mysql.TypeVarchar, 64, 0, nil, nil},
	{"GIT_HASH", mysql.TypeVarchar, 64, 0, nil, nil},
	{"BINLOG_STATUS", mysql.TypeVarchar, 64, 0, nil, nil},
}

var tableClusterConfigCols = []columnInfo{
	{"TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ADDRESS", mysql.TypeVarchar, 64, 0, nil, nil},
	{"KEY", mysql.TypeVarchar, 256, 0, nil, nil},
	{"VALUE", mysql.TypeVarchar, 128, 0, nil, nil},
}

var tableClusterLogCols = []columnInfo{
	{"TIME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ADDRESS", mysql.TypeVarchar, 64, 0, nil, nil},
	{"LEVEL", mysql.TypeVarchar, 8, 0, nil, nil},
	{"MESSAGE", mysql.TypeVarString, 1024, 0, nil, nil},
}

var tableClusterLoadCols = []columnInfo{
	{"TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ADDRESS", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DEVICE_TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DEVICE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"LOAD_NAME", mysql.TypeVarchar, 256, 0, nil, nil},
	{"LOAD_VALUE", mysql.TypeVarchar, 128, 0, nil, nil},
}

func dataForTiKVRegionStatus(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	tikvStore, ok := ctx.GetStore().(tikv.Storage)
	if !ok {
		return nil, errors.New("Information about TiKV region status can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	regionsInfo, err := tikvHelper.GetRegionsInfo()
	if err != nil {
		return nil, err
	}
	allSchemas := ctx.GetSessionVars().TxnCtx.InfoSchema.(InfoSchema).AllSchemas()
	tableInfos := tikvHelper.GetRegionsTableInfo(regionsInfo, allSchemas)
	for _, region := range regionsInfo.Regions {
		tableList := tableInfos[region.ID]
		if len(tableList) == 0 {
			records = append(records, newTiKVRegionStatusCol(&region, nil))
		}
		for _, table := range tableList {
			row := newTiKVRegionStatusCol(&region, &table)
			records = append(records, row)
		}
	}
	return records, nil
}

func newTiKVRegionStatusCol(region *helper.RegionInfo, table *helper.TableInfo) []types.Datum {
	row := make([]types.Datum, len(tableTiKVRegionStatusCols))
	row[0].SetInt64(region.ID)
	row[1].SetString(region.StartKey)
	row[2].SetString(region.EndKey)
	if table != nil {
		row[3].SetInt64(table.Table.ID)
		row[4].SetString(table.DB.Name.O)
		row[5].SetString(table.Table.Name.O)
		if table.IsIndex {
			row[6].SetInt64(1)
			row[7].SetInt64(table.Index.ID)
			row[8].SetString(table.Index.Name.O)
		} else {
			row[6].SetInt64(0)
		}
	}
	row[9].SetInt64(region.Epoch.ConfVer)
	row[10].SetInt64(region.Epoch.Version)
	row[11].SetInt64(region.WrittenBytes)
	row[12].SetInt64(region.ReadBytes)
	row[13].SetInt64(region.ApproximateSize)
	row[14].SetInt64(region.ApproximateKeys)
	return row
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
	regionsInfo, err := tikvHelper.GetRegionsInfo()
	if err != nil {
		return nil, err
	}
	allSchemas := ctx.GetSessionVars().TxnCtx.InfoSchema.(InfoSchema).AllSchemas()
	tableInfos := tikvHelper.GetRegionsTableInfo(regionsInfo, allSchemas)
	for _, region := range regionsInfo.Regions {
		tableList := tableInfos[region.ID]
		if len(tableList) == 0 {
			records = append(records, newTiKVRegionPeersCols(&region, nil)...)
		}
		for _, table := range tableList {
			rows := newTiKVRegionPeersCols(&region, &table)
			records = append(records, rows...)
		}
	}
	return records, nil
}

func newTiKVRegionPeersCols(region *helper.RegionInfo, table *helper.TableInfo) [][]types.Datum {
	records := make([][]types.Datum, 0, len(region.Peers))
	pendingPeerIDSet := set.NewInt64Set()
	for _, peer := range region.PendingPeers {
		pendingPeerIDSet.Insert(peer.ID)
	}
	downPeerMap := make(map[int64]int64)
	for _, peerStat := range region.DownPeers {
		downPeerMap[peerStat.ID] = peerStat.DownSec
	}
	template := make([]types.Datum, 7)
	template[0].SetInt64(region.ID)
	if table != nil {
		template[1].SetInt64(table.Table.ID)
		template[2].SetString(table.DB.Name.O)
		template[3].SetString(table.Table.Name.O)
		if table.IsIndex {
			template[4].SetInt64(1)
			template[5].SetInt64(table.Index.ID)
			template[6].SetString(table.Index.Name.O)
		} else {
			template[4].SetInt64(0)
		}
	}
	for _, peer := range region.Peers {
		row := make([]types.Datum, len(tableTiKVRegionPeersCols))
		copy(row, template)
		row[7].SetInt64(peer.ID)
		row[8].SetInt64(peer.StoreID)
		if peer.IsLearner {
			row[9].SetInt64(1)
		} else {
			row[9].SetInt64(0)
		}
		if peer.ID == region.Leader.ID {
			row[10].SetInt64(1)
		} else {
			row[10].SetInt64(0)
		}
		if pendingPeerIDSet.Exist(peer.ID) {
			row[11].SetString(pendingPeer)
		} else if downSec, ok := downPeerMap[peer.ID]; ok {
			row[11].SetString(downPeer)
			row[12].SetInt64(downSec)
		} else {
			row[11].SetString(normalPeer)
		}
		records = append(records, row)
	}
	return records
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
		row[9].SetFloat64(storeStat.Status.LeaderWeight)
		row[10].SetFloat64(storeStat.Status.LeaderScore)
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
		if !hasProcessPriv && loginUser != nil && pi.User != loginUser.Username {
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

var tableClusterInfoCols = []columnInfo{
	{"TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ADDRESS", mysql.TypeVarchar, 64, 0, nil, nil},
	{"STATUS_ADDRESS", mysql.TypeVarchar, 64, 0, nil, nil},
	{"VERSION", mysql.TypeVarchar, 64, 0, nil, nil},
	{"GIT_HASH", mysql.TypeVarchar, 64, 0, nil, nil},
}

var tableTableTiFlashReplicaCols = []columnInfo{
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"REPLICA_COUNT", mysql.TypeLonglong, 64, 0, nil, nil},
	{"LOCATION_LABELS", mysql.TypeVarchar, 64, 0, nil, nil},
	{"AVAILABLE", mysql.TypeTiny, 1, 0, nil, nil},
}

func dataForSchemata(ctx sessionctx.Context, schemas []*model.DBInfo) [][]types.Datum {
	checker := privilege.GetPrivilegeManager(ctx)
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

		if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, "", "", mysql.AllPrivMask) {
			continue
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
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL("select table_id, count from mysql.stats_meta")
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
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL("select table_id, hist_id, tot_col_size from mysql.stats_histograms where is_index = 0")
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

				shardingInfo := GetShardingInfo(schema, table)
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
					shardingInfo,  // TIDB_ROW_ID_SHARDING_INFO
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
					nil,           // TIDB_ROW_ID_SHARDING_INFO
				)
				rows = append(rows, record)
			}
		}
	}
	return rows, nil
}

// GetShardingInfo returns a nil or description string for the sharding information of given TableInfo.
// The returned description string may be:
//  - "NOT_SHARDED": for tables that SHARD_ROW_ID_BITS is not specified.
//  - "NOT_SHARDED(PK_IS_HANDLE)": for tables that is primary key is row id.
//  - "SHARD_BITS={bit_number}": for tables that with SHARD_ROW_ID_BITS.
// The returned nil indicates that sharding information is not suitable for the table(for example, when the table is a View).
// This function is exported for unit test.
func GetShardingInfo(dbInfo *model.DBInfo, tableInfo *model.TableInfo) interface{} {
	if dbInfo == nil || tableInfo == nil || tableInfo.IsView() || util.IsMemOrSysDB(dbInfo.Name.L) {
		return nil
	}
	shardingInfo := "NOT_SHARDED"
	if tableInfo.PKIsHandle {
		shardingInfo = "NOT_SHARDED(PK_IS_HANDLE)"
	} else {
		if tableInfo.ShardRowIDBits > 0 {
			shardingInfo = "SHARD_BITS=" + strconv.Itoa(int(tableInfo.ShardRowIDBits))
		}
	}
	return shardingInfo
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
		if col.Hidden {
			continue
		}
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

func dataForStatistics(ctx sessionctx.Context, schemas []*model.DBInfo) [][]types.Datum {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}

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
func dataForTableConstraints(ctx sessionctx.Context, schemas []*model.DBInfo) [][]types.Datum {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, tbl := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, tbl.Name.L, "", mysql.AllPrivMask) {
				continue
			}

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

func dataForServersInfo() ([][]types.Datum, error) {
	serversInfo, err := infosync.GetAllServerInfo(context.Background())
	if err != nil {
		return nil, err
	}
	rows := make([][]types.Datum, 0, len(serversInfo))
	for _, info := range serversInfo {
		row := types.MakeDatums(
			info.ID,              // DDL_ID
			info.IP,              // IP
			int(info.Port),       // PORT
			int(info.StatusPort), // STATUS_PORT
			info.Lease,           // LEASE
			info.Version,         // VERSION
			info.GitHash,         // GIT_HASH
			info.BinlogStatus,    // BINLOG_STATUS
		)
		rows = append(rows, row)
	}
	return rows, nil
}

// ServerInfo represents the basic server information of single cluster component
type ServerInfo struct {
	ServerType string
	Address    string
	StatusAddr string
	Version    string
	GitHash    string
}

// GetClusterServerInfo returns all components information of cluster
func GetClusterServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	failpoint.Inject("mockClusterInfo", func(val failpoint.Value) {
		// The cluster topology is injected by `failpoint` expression and
		// there is no extra checks for it. (let the test fail if the expression invalid)
		if s := val.(string); len(s) > 0 {
			var servers []ServerInfo
			for _, server := range strings.Split(s, ";") {
				parts := strings.Split(server, ",")
				servers = append(servers, ServerInfo{
					ServerType: parts[0],
					Address:    parts[1],
					StatusAddr: parts[2],
					Version:    parts[3],
					GitHash:    parts[4],
				})
			}
			failpoint.Return(servers, nil)
		}
	})

	type retriever func(ctx sessionctx.Context) ([]ServerInfo, error)
	var servers []ServerInfo
	for _, r := range []retriever{GetTiDBServerInfo, GetPDServerInfo, GetTiKVServerInfo} {
		nodes, err := r(ctx)
		if err != nil {
			return nil, err
		}
		servers = append(servers, nodes...)
	}
	return servers, nil
}

// GetTiDBServerInfo returns all TiDB nodes information of cluster
func GetTiDBServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	// Get TiDB servers info.
	tidbNodes, err := infosync.GetAllServerInfo(context.Background())
	if err != nil {
		return nil, errors.Trace(err)
	}

	var servers []ServerInfo
	for _, node := range tidbNodes {
		servers = append(servers, ServerInfo{
			ServerType: "tidb",
			Address:    fmt.Sprintf("%s:%d", node.IP, node.Port),
			StatusAddr: fmt.Sprintf("%s:%d", node.IP, node.StatusPort),
			Version:    node.Version,
			GitHash:    node.GitHash,
		})
	}
	return servers, nil
}

// GetPDServerInfo returns all PD nodes information of cluster
func GetPDServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	// Get PD servers info.
	store := ctx.GetStore()
	etcd, ok := store.(tikv.EtcdBackend)
	if !ok {
		return nil, errors.Errorf("%T not an etcd backend", store)
	}
	var servers []ServerInfo
	for _, addr := range etcd.EtcdAddrs() {
		addr = strings.TrimSpace(addr)

		// Get PD version
		url := fmt.Sprintf("http://%s%s", addr, pdapi.ClusterVersion)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		req.Header.Add("PD-Allow-follower-handle", "true")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pdVersion, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.Trace(err)
		}
		terror.Log(resp.Body.Close())
		version := strings.Trim(strings.Trim(string(pdVersion), "\n"), "\"")

		// Get PD git_hash
		url = fmt.Sprintf("http://%s%s", addr, pdapi.Status)
		req, err = http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		req.Header.Add("PD-Allow-follower-handle", "true")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var content = struct {
			GitHash string `json:"git_hash"`
		}{}
		if err := json.NewDecoder(resp.Body).Decode(&content); err != nil {
			return nil, errors.Trace(err)
		}
		terror.Log(resp.Body.Close())

		servers = append(servers, ServerInfo{
			ServerType: "pd",
			Address:    addr,
			StatusAddr: addr,
			Version:    version,
			GitHash:    content.GitHash,
		})
	}
	return servers, nil
}

// GetTiKVServerInfo returns all TiKV nodes information of cluster
func GetTiKVServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	store := ctx.GetStore()
	// Get TiKV servers info.
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return nil, errors.Errorf("%T is not an TiKV store instance", store)
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}

	storesStat, err := tikvHelper.GetStoresStat()
	if err != nil {
		return nil, errors.Trace(err)
	}
	var servers []ServerInfo
	for _, storeStat := range storesStat.Stores {
		servers = append(servers, ServerInfo{
			ServerType: "tikv",
			Address:    storeStat.Store.Address,
			StatusAddr: storeStat.Store.StatusAddress,
			Version:    storeStat.Store.Version,
			GitHash:    storeStat.Store.GitHash,
		})
	}
	return servers, nil
}

func dataForTiDBClusterInfo(ctx sessionctx.Context) ([][]types.Datum, error) {
	servers, err := GetClusterServerInfo(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([][]types.Datum, 0, len(servers))
	for _, server := range servers {
		row := types.MakeDatums(
			server.ServerType,
			server.Address,
			server.StatusAddr,
			server.Version,
			server.GitHash,
		)
		rows = append(rows, row)
	}
	return rows, nil
}

func dataForClusterLoadInfo(ctx sessionctx.Context) ([][]types.Datum, error) {
	serversInfo, err := GetClusterServerInfo(ctx)
	if err != nil {
		return nil, err
	}
	ipMap := make(map[string]struct{}, len(serversInfo))
	rows := make([][]types.Datum, 0, len(serversInfo)*10)
	for _, srv := range serversInfo {
		// TODO: remove this after PD/TiKV support diagnostic grpc service.
		if srv.ServerType == "pd" || srv.ServerType == "tikv" {
			continue
		}
		addr := srv.StatusAddr
		ip := addr
		if idx := strings.Index(addr, ":"); idx != -1 {
			ip = addr[:idx]
		}
		if _, ok := ipMap[ip]; ok {
			continue
		}
		ipMap[ip] = struct{}{}
		items, err := getServerInfoByGRPC(srv.StatusAddr, diagnosticspb.ServerInfoType_LoadInfo)
		if err != nil {
			return nil, err
		}
		partRows := serverInfoItemToRows(items, srv.ServerType, srv.StatusAddr)
		rows = append(rows, partRows...)
	}
	return rows, nil
}

func serverInfoItemToRows(items []*diagnosticspb.ServerInfoItem, tp, addr string) [][]types.Datum {
	rows := make([][]types.Datum, 0, len(items))
	for _, v := range items {
		for _, item := range v.Pairs {
			row := types.MakeDatums(
				tp,
				addr,
				v.Tp,
				v.Name,
				item.Key,
				item.Value,
			)
			rows = append(rows, row)
		}
	}
	return rows
}

func getServerInfoByGRPC(address string, tp diagnosticspb.ServerInfoType) ([]*diagnosticspb.ServerInfoItem, error) {
	opt := grpc.WithInsecure()
	security := config.GetGlobalConfig().Security
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}
	conn, err := grpc.Dial(address, opt)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Error("close grpc connection error", zap.Error(err))
		}
	}()

	cli := diagnosticspb.NewDiagnosticsClient(conn)
	// FIXME: use session context instead of context.Background().
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r, err := cli.ServerInfo(ctx, &diagnosticspb.ServerInfoRequest{Tp: tp})
	if err != nil {
		return nil, err
	}
	return r.Items, nil
}

// dataForTableTiFlashReplica constructs data for table tiflash replica info.
func dataForTableTiFlashReplica(schemas []*model.DBInfo) [][]types.Datum {
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, tbl := range schema.Tables {
			if tbl.TiFlashReplica == nil {
				continue
			}
			record := types.MakeDatums(
				schema.Name.O,                   // TABLE_SCHEMA
				tbl.Name.O,                      // TABLE_NAME
				tbl.ID,                          // TABLE_ID
				int64(tbl.TiFlashReplica.Count), // REPLICA_COUNT
				strings.Join(tbl.TiFlashReplica.LocationLabels, ","), // LOCATION_LABELS
				tbl.TiFlashReplica.Available,                         // AVAILABLE
			)
			rows = append(rows, record)
		}
	}
	return rows
}

var tableNameToColumns = map[string][]columnInfo{
	tableSchemata:                           schemataCols,
	tableTables:                             tablesCols,
	tableColumns:                            columnsCols,
	tableColumnStatistics:                   columnStatisticsCols,
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
	tableTiDBServersInfo:                    tableTiDBServersInfoCols,
	tableClusterInfo:                        tableClusterInfoCols,
	TableClusterConfig:                      tableClusterConfigCols,
	TableClusterLog:                         tableClusterLogCols,
	tableClusterLoad:                        tableClusterLoadCols,
	tableTiFlashReplica:                     tableTableTiFlashReplicaCols,
}

func createInfoSchemaTable(_ autoid.Allocator, meta *model.TableInfo) (table.Table, error) {
	columns := make([]*table.Column, len(meta.Columns))
	for i, col := range meta.Columns {
		columns[i] = table.ToColumn(col)
	}
	tp := table.VirtualTable
	if isClusterTableByName(util.InformationSchemaName, meta.Name.L) {
		tp = table.ClusterTable
	}
	return &infoschemaTable{meta: meta, cols: columns, tp: tp}, nil
}

type infoschemaTable struct {
	meta *model.TableInfo
	cols []*table.Column
	tp   table.Type
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
	is := GetInfoSchema(ctx)
	dbs := is.AllSchemas()
	sort.Sort(schemasSorter(dbs))
	switch it.meta.Name.O {
	case tableSchemata:
		fullRows = dataForSchemata(ctx, dbs)
	case tableTables:
		fullRows, err = dataForTables(ctx, dbs)
	case tableTiDBIndexes:
		fullRows, err = dataForIndexes(ctx, dbs)
	case tableColumns:
		fullRows = dataForColumns(ctx, dbs)
	case tableStatistics:
		fullRows = dataForStatistics(ctx, dbs)
	case tableCharacterSets:
		fullRows = dataForCharacterSets()
	case tableCollations:
		fullRows = dataForCollations()
	case tableSessionVar:
		fullRows, err = dataForSessionVar(ctx)
	case tableConstraints:
		fullRows = dataForTableConstraints(ctx, dbs)
	case tableFiles:
	case tableProfiling:
		if v, ok := ctx.GetSessionVars().GetSystemVar("profiling"); ok && variable.TiDBOptOn(v) {
			fullRows = dataForPseudoProfiling()
		}
	case tablePartitions:
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
	case tableTiDBServersInfo:
		fullRows, err = dataForServersInfo()
	case tableClusterInfo:
		fullRows, err = dataForTiDBClusterInfo(ctx)
	case tableClusterLoad:
		fullRows, err = dataForClusterLoadInfo(ctx)
	case tableTiFlashReplica:
		fullRows = dataForTableTiFlashReplica(dbs)
	// Data for cluster memory table.
	case clusterTableSlowLog, clusterTableProcesslist:
		fullRows, err = getClusterMemTableRows(ctx, it.meta.Name.O)
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

// VisibleCols implements table.Table VisibleCols interface.
func (it *infoschemaTable) VisibleCols() []*table.Column {
	return it.cols
}

// HiddenCols implements table.Table HiddenCols interface.
func (it *infoschemaTable) HiddenCols() []*table.Column {
	return nil
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
func (it *infoschemaTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID int64, err error) {
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

// AllocHandleIDs implements table.Table AllocHandleIDs interface.
func (it *infoschemaTable) AllocHandleIDs(ctx sessionctx.Context, n uint64) (int64, int64, error) {
	return 0, 0, table.ErrUnsupportedOp
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
	return it.tp
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

// VisibleCols implements table.Table VisibleCols interface.
func (vt *VirtualTable) VisibleCols() []*table.Column {
	return nil
}

// HiddenCols implements table.Table HiddenCols interface.
func (vt *VirtualTable) HiddenCols() []*table.Column {
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
func (vt *VirtualTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID int64, err error) {
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

// AllocHandleIDs implements table.Table AllocHandleIDs interface.
func (vt *VirtualTable) AllocHandleIDs(ctx sessionctx.Context, n uint64) (int64, int64, error) {
	return 0, 0, table.ErrUnsupportedOp
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
