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
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// TableSchemata is the string constant of infoschema table.
	TableSchemata = "SCHEMATA"
	// TableTables is the string constant of infoschema table.
	TableTables = "TABLES"
	// TableColumns is the string constant of infoschema table
	TableColumns          = "COLUMNS"
	tableColumnStatistics = "COLUMN_STATISTICS"
	// TableStatistics is the string constant of infoschema table
	TableStatistics = "STATISTICS"
	// TableCharacterSets is the string constant of infoschema charactersets memory table
	TableCharacterSets = "CHARACTER_SETS"
	// TableCollations is the string constant of infoschema collations memory table.
	TableCollations = "COLLATIONS"
	tableFiles      = "FILES"
	// CatalogVal is the string constant of TABLE_CATALOG.
	CatalogVal = "def"
	// TableProfiling is the string constant of infoschema table.
	TableProfiling = "PROFILING"
	// TablePartitions is the string constant of infoschema table.
	TablePartitions = "PARTITIONS"
	// TableKeyColumn is the string constant of KEY_COLUMN_USAGE.
	TableKeyColumn = "KEY_COLUMN_USAGE"
	// TableReferConst is the string constant of REFERENTIAL_CONSTRAINTS.
	TableReferConst = "REFERENTIAL_CONSTRAINTS"
	tablePlugins    = "PLUGINS"
	// TableConstraints is the string constant of TABLE_CONSTRAINTS.
	TableConstraints = "TABLE_CONSTRAINTS"
	tableTriggers    = "TRIGGERS"
	// TableUserPrivileges is the string constant of infoschema user privilege table.
	TableUserPrivileges   = "USER_PRIVILEGES"
	tableSchemaPrivileges = "SCHEMA_PRIVILEGES"
	tableTablePrivileges  = "TABLE_PRIVILEGES"
	tableColumnPrivileges = "COLUMN_PRIVILEGES"
	// TableEngines is the string constant of infoschema table.
	TableEngines = "ENGINES"
	// TableViews is the string constant of infoschema table.
	TableViews          = "VIEWS"
	tableRoutines       = "ROUTINES"
	tableParameters     = "PARAMETERS"
	tableEvents         = "EVENTS"
	tableOptimizerTrace = "OPTIMIZER_TRACE"
	tableTableSpaces    = "TABLESPACES"
	// TableCollationCharacterSetApplicability is the string constant of infoschema memory table.
	TableCollationCharacterSetApplicability = "COLLATION_CHARACTER_SET_APPLICABILITY"
	// TableProcesslist is the string constant of infoschema table.
	TableProcesslist = "PROCESSLIST"
	// TableTiDBIndexes is the string constant of infoschema table
	TableTiDBIndexes = "TIDB_INDEXES"
	// TableTiDBHotRegions is the string constant of infoschema table
	TableTiDBHotRegions = "TIDB_HOT_REGIONS"
	// TableTiDBHotRegionsHistory is the string constant of infoschema table
	TableTiDBHotRegionsHistory = "TIDB_HOT_REGIONS_HISTORY"
	// TableTiKVStoreStatus is the string constant of infoschema table
	TableTiKVStoreStatus = "TIKV_STORE_STATUS"
	// TableAnalyzeStatus is the string constant of Analyze Status
	TableAnalyzeStatus = "ANALYZE_STATUS"
	// TableTiKVRegionStatus is the string constant of infoschema table
	TableTiKVRegionStatus = "TIKV_REGION_STATUS"
	// TableTiKVRegionPeers is the string constant of infoschema table
	TableTiKVRegionPeers = "TIKV_REGION_PEERS"
	// TableTiDBServersInfo is the string constant of TiDB server information table.
	TableTiDBServersInfo = "TIDB_SERVERS_INFO"
	// TableSlowQuery is the string constant of slow query memory table.
	TableSlowQuery = "SLOW_QUERY"
	// TableClusterInfo is the string constant of cluster info memory table.
	TableClusterInfo = "CLUSTER_INFO"
	// TableClusterConfig is the string constant of cluster configuration memory table.
	TableClusterConfig = "CLUSTER_CONFIG"
	// TableClusterLog is the string constant of cluster log memory table.
	TableClusterLog = "CLUSTER_LOG"
	// TableClusterLoad is the string constant of cluster load memory table.
	TableClusterLoad = "CLUSTER_LOAD"
	// TableClusterHardware is the string constant of cluster hardware table.
	TableClusterHardware = "CLUSTER_HARDWARE"
	// TableClusterSystemInfo is the string constant of cluster system info table.
	TableClusterSystemInfo = "CLUSTER_SYSTEMINFO"
	// TableTiFlashReplica is the string constant of tiflash replica table.
	TableTiFlashReplica = "TIFLASH_REPLICA"
	// TableInspectionResult is the string constant of inspection result table.
	TableInspectionResult = "INSPECTION_RESULT"
	// TableMetricTables is a table that contains all metrics table definition.
	TableMetricTables = "METRICS_TABLES"
	// TableMetricSummary is a summary table that contains all metrics.
	TableMetricSummary = "METRICS_SUMMARY"
	// TableMetricSummaryByLabel is a metric table that contains all metrics that group by label info.
	TableMetricSummaryByLabel = "METRICS_SUMMARY_BY_LABEL"
	// TableInspectionSummary is the string constant of inspection summary table.
	TableInspectionSummary = "INSPECTION_SUMMARY"
	// TableInspectionRules is the string constant of currently implemented inspection and summary rules.
	TableInspectionRules = "INSPECTION_RULES"
	// TableDDLJobs is the string constant of DDL job table.
	TableDDLJobs = "DDL_JOBS"
	// TableSequences is the string constant of all sequences created by user.
	TableSequences = "SEQUENCES"
	// TableStatementsSummary is the string constant of statement summary table.
	TableStatementsSummary = "STATEMENTS_SUMMARY"
	// TableStatementsSummaryHistory is the string constant of statements summary history table.
	TableStatementsSummaryHistory = "STATEMENTS_SUMMARY_HISTORY"
	// TableStatementsSummaryEvicted is the string constant of statements summary evicted table.
	TableStatementsSummaryEvicted = "STATEMENTS_SUMMARY_EVICTED"
	// TableTiDBStatementsStats is the string constant of the TiDB statement stats table.
	TableTiDBStatementsStats = "TIDB_STATEMENTS_STATS"
	// TableStorageStats is a table that contains all tables disk usage
	TableStorageStats = "TABLE_STORAGE_STATS"
	// TableTiFlashTables is the string constant of tiflash tables table.
	TableTiFlashTables = "TIFLASH_TABLES"
	// TableTiFlashSegments is the string constant of tiflash segments table.
	TableTiFlashSegments = "TIFLASH_SEGMENTS"
	// TableTiFlashIndexes is the string constant of tiflash indexes table.
	TableTiFlashIndexes = "TIFLASH_INDEXES"
	// TableClientErrorsSummaryGlobal is the string constant of client errors table.
	TableClientErrorsSummaryGlobal = "CLIENT_ERRORS_SUMMARY_GLOBAL"
	// TableClientErrorsSummaryByUser is the string constant of client errors table.
	TableClientErrorsSummaryByUser = "CLIENT_ERRORS_SUMMARY_BY_USER"
	// TableClientErrorsSummaryByHost is the string constant of client errors table.
	TableClientErrorsSummaryByHost = "CLIENT_ERRORS_SUMMARY_BY_HOST"
	// TableTiDBTrx is current running transaction status table.
	TableTiDBTrx = "TIDB_TRX"
	// TableDeadlocks is the string constant of deadlock table.
	TableDeadlocks = "DEADLOCKS"
	// TableDataLockWaits is current lock waiting status table.
	TableDataLockWaits = "DATA_LOCK_WAITS"
	// TableAttributes is the string constant of attributes table.
	TableAttributes = "ATTRIBUTES"
	// TablePlacementPolicies is the string constant of placement policies table.
	TablePlacementPolicies = "PLACEMENT_POLICIES"
	// TableTrxSummary is the string constant of transaction summary table.
	TableTrxSummary = "TRX_SUMMARY"
	// TableVariablesInfo is the string constant of variables_info table.
	TableVariablesInfo = "VARIABLES_INFO"
	// TableUserAttributes is the string constant of user_attributes view.
	TableUserAttributes = "USER_ATTRIBUTES"
	// TableMemoryUsage is the memory usage status of tidb instance.
	TableMemoryUsage = "MEMORY_USAGE"
	// TableMemoryUsageOpsHistory is the memory control operators history.
	TableMemoryUsageOpsHistory = "MEMORY_USAGE_OPS_HISTORY"
	// TableResourceGroups is the metadata of resource groups.
	TableResourceGroups = "RESOURCE_GROUPS"
	// TableRunawayWatches is the query list of runaway watch.
	TableRunawayWatches = "RUNAWAY_WATCHES"
	// TableCheckConstraints is the list of CHECK constraints.
	TableCheckConstraints = "CHECK_CONSTRAINTS"
	// TableTiDBCheckConstraints is the list of CHECK constraints, with non-standard TiDB extensions.
	TableTiDBCheckConstraints = "TIDB_CHECK_CONSTRAINTS"
	// TableKeywords is the list of keywords.
	TableKeywords = "KEYWORDS"
	// TableTiDBIndexUsage is a table to show the usage stats of indexes in the current instance.
	TableTiDBIndexUsage = "TIDB_INDEX_USAGE"
	// TableTiDBPlanCache is the plan cache table.
	TableTiDBPlanCache = "TIDB_PLAN_CACHE"
	// TableKeyspaceMeta is the table to show the keyspace meta.
	TableKeyspaceMeta = "KEYSPACE_META"
	// TableSchemataExtensions is the table to show read only status of database.
	TableSchemataExtensions = "SCHEMATA_EXTENSIONS"
)

const (
	// DataLockWaitsColumnKey is the name of the KEY column of the DATA_LOCK_WAITS table.
	DataLockWaitsColumnKey = "KEY"
	// DataLockWaitsColumnKeyInfo is the name of the KEY_INFO column of the DATA_LOCK_WAITS table.
	DataLockWaitsColumnKeyInfo = "KEY_INFO"
	// DataLockWaitsColumnTrxID is the name of the TRX_ID column of the DATA_LOCK_WAITS table.
	DataLockWaitsColumnTrxID = "TRX_ID"
	// DataLockWaitsColumnCurrentHoldingTrxID is the name of the CURRENT_HOLDING_TRX_ID column of the DATA_LOCK_WAITS table.
	DataLockWaitsColumnCurrentHoldingTrxID = "CURRENT_HOLDING_TRX_ID"
	// DataLockWaitsColumnSQLDigest is the name of the SQL_DIGEST column of the DATA_LOCK_WAITS table.
	DataLockWaitsColumnSQLDigest = "SQL_DIGEST"
	// DataLockWaitsColumnSQLDigestText is the name of the SQL_DIGEST_TEXT column of the DATA_LOCK_WAITS table.
	DataLockWaitsColumnSQLDigestText = "SQL_DIGEST_TEXT"
)

// The following variables will only be used when PD in the microservice mode.
const (
	// tsoServiceName is the name of TSO service.
	tsoServiceName = "tso"
	// schedulingServiceName is the name of scheduling service.
	schedulingServiceName = "scheduling"
)

var tableIDMap = map[string]int64{
	TableSchemata:         autoid.InformationSchemaDBID + 1,
	TableTables:           autoid.InformationSchemaDBID + 2,
	TableColumns:          autoid.InformationSchemaDBID + 3,
	tableColumnStatistics: autoid.InformationSchemaDBID + 4,
	TableStatistics:       autoid.InformationSchemaDBID + 5,
	TableCharacterSets:    autoid.InformationSchemaDBID + 6,
	TableCollations:       autoid.InformationSchemaDBID + 7,
	tableFiles:            autoid.InformationSchemaDBID + 8,
	CatalogVal:            autoid.InformationSchemaDBID + 9,
	TableProfiling:        autoid.InformationSchemaDBID + 10,
	TablePartitions:       autoid.InformationSchemaDBID + 11,
	TableKeyColumn:        autoid.InformationSchemaDBID + 12,
	TableReferConst:       autoid.InformationSchemaDBID + 13,
	// Removed, see https://github.com/pingcap/tidb/issues/9154
	// TableSessionVar:    autoid.InformationSchemaDBID + 14,
	tablePlugins:          autoid.InformationSchemaDBID + 15,
	TableConstraints:      autoid.InformationSchemaDBID + 16,
	tableTriggers:         autoid.InformationSchemaDBID + 17,
	TableUserPrivileges:   autoid.InformationSchemaDBID + 18,
	tableSchemaPrivileges: autoid.InformationSchemaDBID + 19,
	tableTablePrivileges:  autoid.InformationSchemaDBID + 20,
	tableColumnPrivileges: autoid.InformationSchemaDBID + 21,
	TableEngines:          autoid.InformationSchemaDBID + 22,
	TableViews:            autoid.InformationSchemaDBID + 23,
	tableRoutines:         autoid.InformationSchemaDBID + 24,
	tableParameters:       autoid.InformationSchemaDBID + 25,
	tableEvents:           autoid.InformationSchemaDBID + 26,
	// Removed, see https://github.com/pingcap/tidb/issues/9154
	// tableGlobalStatus:                    autoid.InformationSchemaDBID + 27,
	// tableGlobalVariables:                 autoid.InformationSchemaDBID + 28,
	// tableSessionStatus:                   autoid.InformationSchemaDBID + 29,
	tableOptimizerTrace:                     autoid.InformationSchemaDBID + 30,
	tableTableSpaces:                        autoid.InformationSchemaDBID + 31,
	TableCollationCharacterSetApplicability: autoid.InformationSchemaDBID + 32,
	TableProcesslist:                        autoid.InformationSchemaDBID + 33,
	TableTiDBIndexes:                        autoid.InformationSchemaDBID + 34,
	TableSlowQuery:                          autoid.InformationSchemaDBID + 35,
	TableTiDBHotRegions:                     autoid.InformationSchemaDBID + 36,
	TableTiKVStoreStatus:                    autoid.InformationSchemaDBID + 37,
	TableAnalyzeStatus:                      autoid.InformationSchemaDBID + 38,
	TableTiKVRegionStatus:                   autoid.InformationSchemaDBID + 39,
	TableTiKVRegionPeers:                    autoid.InformationSchemaDBID + 40,
	TableTiDBServersInfo:                    autoid.InformationSchemaDBID + 41,
	TableClusterInfo:                        autoid.InformationSchemaDBID + 42,
	TableClusterConfig:                      autoid.InformationSchemaDBID + 43,
	TableClusterLoad:                        autoid.InformationSchemaDBID + 44,
	TableTiFlashReplica:                     autoid.InformationSchemaDBID + 45,
	ClusterTableSlowLog:                     autoid.InformationSchemaDBID + 46,
	ClusterTableProcesslist:                 autoid.InformationSchemaDBID + 47,
	TableClusterLog:                         autoid.InformationSchemaDBID + 48,
	TableClusterHardware:                    autoid.InformationSchemaDBID + 49,
	TableClusterSystemInfo:                  autoid.InformationSchemaDBID + 50,
	TableInspectionResult:                   autoid.InformationSchemaDBID + 51,
	TableMetricSummary:                      autoid.InformationSchemaDBID + 52,
	TableMetricSummaryByLabel:               autoid.InformationSchemaDBID + 53,
	TableMetricTables:                       autoid.InformationSchemaDBID + 54,
	TableInspectionSummary:                  autoid.InformationSchemaDBID + 55,
	TableInspectionRules:                    autoid.InformationSchemaDBID + 56,
	TableDDLJobs:                            autoid.InformationSchemaDBID + 57,
	TableSequences:                          autoid.InformationSchemaDBID + 58,
	TableStatementsSummary:                  autoid.InformationSchemaDBID + 59,
	TableStatementsSummaryHistory:           autoid.InformationSchemaDBID + 60,
	ClusterTableStatementsSummary:           autoid.InformationSchemaDBID + 61,
	ClusterTableStatementsSummaryHistory:    autoid.InformationSchemaDBID + 62,
	TableStorageStats:                       autoid.InformationSchemaDBID + 63,
	TableTiFlashTables:                      autoid.InformationSchemaDBID + 64,
	TableTiFlashSegments:                    autoid.InformationSchemaDBID + 65,
	// Removed, see https://github.com/pingcap/tidb/issues/28890
	//TablePlacementPolicy:                    autoid.InformationSchemaDBID + 66,
	TableClientErrorsSummaryGlobal:       autoid.InformationSchemaDBID + 67,
	TableClientErrorsSummaryByUser:       autoid.InformationSchemaDBID + 68,
	TableClientErrorsSummaryByHost:       autoid.InformationSchemaDBID + 69,
	TableTiDBTrx:                         autoid.InformationSchemaDBID + 70,
	ClusterTableTiDBTrx:                  autoid.InformationSchemaDBID + 71,
	TableDeadlocks:                       autoid.InformationSchemaDBID + 72,
	ClusterTableDeadlocks:                autoid.InformationSchemaDBID + 73,
	TableDataLockWaits:                   autoid.InformationSchemaDBID + 74,
	TableStatementsSummaryEvicted:        autoid.InformationSchemaDBID + 75,
	ClusterTableStatementsSummaryEvicted: autoid.InformationSchemaDBID + 76,
	TableAttributes:                      autoid.InformationSchemaDBID + 77,
	TableTiDBHotRegionsHistory:           autoid.InformationSchemaDBID + 78,
	TablePlacementPolicies:               autoid.InformationSchemaDBID + 79,
	TableTrxSummary:                      autoid.InformationSchemaDBID + 80,
	ClusterTableTrxSummary:               autoid.InformationSchemaDBID + 81,
	TableVariablesInfo:                   autoid.InformationSchemaDBID + 82,
	TableUserAttributes:                  autoid.InformationSchemaDBID + 83,
	TableMemoryUsage:                     autoid.InformationSchemaDBID + 84,
	TableMemoryUsageOpsHistory:           autoid.InformationSchemaDBID + 85,
	ClusterTableMemoryUsage:              autoid.InformationSchemaDBID + 86,
	ClusterTableMemoryUsageOpsHistory:    autoid.InformationSchemaDBID + 87,
	TableResourceGroups:                  autoid.InformationSchemaDBID + 88,
	TableRunawayWatches:                  autoid.InformationSchemaDBID + 89,
	TableCheckConstraints:                autoid.InformationSchemaDBID + 90,
	TableTiDBCheckConstraints:            autoid.InformationSchemaDBID + 91,
	TableKeywords:                        autoid.InformationSchemaDBID + 92,
	TableTiDBIndexUsage:                  autoid.InformationSchemaDBID + 93,
	ClusterTableTiDBIndexUsage:           autoid.InformationSchemaDBID + 94,
	TableTiFlashIndexes:                  autoid.InformationSchemaDBID + 95,
	TableTiDBPlanCache:                   autoid.InformationSchemaDBID + 96,
	ClusterTableTiDBPlanCache:            autoid.InformationSchemaDBID + 97,
	TableTiDBStatementsStats:             autoid.InformationSchemaDBID + 98,
	ClusterTableTiDBStatementsStats:      autoid.InformationSchemaDBID + 99,
	TableKeyspaceMeta:                    autoid.InformationSchemaDBID + 100,
	TableSchemataExtensions:              autoid.InformationSchemaDBID + 101,
}

// columnInfo represents the basic column information of all kinds of INFORMATION_SCHEMA tables
type columnInfo struct {
	// name of column
	name string
	// tp is column type
	tp byte
	// represent size of bytes of the column
	size int
	// represent decimal length of the column
	decimal int
	// flag represent NotNull, Unsigned, PriKey flags etc.
	flag uint
	// deflt is default value
	deflt any
	// comment for the column
	comment string
	// enumElems represent all possible literal string values of an enum column
	enumElems []string
}

func buildColumnInfo(colID int64, col columnInfo) *model.ColumnInfo {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	if col.tp == mysql.TypeVarchar || col.tp == mysql.TypeMediumBlob || col.tp == mysql.TypeBlob || col.tp == mysql.TypeLongBlob || col.tp == mysql.TypeEnum {
		mCharset = charset.CharsetUTF8MB4
		mCollation = charset.CollationUTF8MB4
	}
	fieldType := types.FieldType{}
	fieldType.SetType(col.tp)
	fieldType.SetCharset(mCharset)
	fieldType.SetCollate(mCollation)
	switch col.tp {
	case mysql.TypeBlob:
		fieldType.SetFlen(1 << 16)
	case mysql.TypeMediumBlob:
		fieldType.SetFlen(1 << 24)
	case mysql.TypeLongBlob:
		fieldType.SetFlen(1 << 32)
	default:
		fieldType.SetFlen(col.size)
	}
	fieldType.SetDecimal(col.decimal)
	fieldType.SetFlag(col.flag)
	fieldType.SetElems(col.enumElems)
	return &model.ColumnInfo{
		ID:           colID,
		Name:         ast.NewCIStr(col.name),
		FieldType:    fieldType,
		State:        model.StatePublic,
		DefaultValue: col.deflt,
		Comment:      col.comment,
	}
}

func buildTableMeta(tableName string, cs []columnInfo) *model.TableInfo {
	cols := make([]*model.ColumnInfo, 0, len(cs))
	primaryIndices := make([]*model.IndexInfo, 0, 1)
	tblInfo := &model.TableInfo{
		Name:    ast.NewCIStr(tableName),
		State:   model.StatePublic,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
	}
	for offset, c := range cs {
		if tblInfo.Name.O == ClusterTableSlowLog && mysql.HasPriKeyFlag(c.flag) {
			switch c.tp {
			case mysql.TypeLong, mysql.TypeLonglong,
				mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
				tblInfo.PKIsHandle = true
			default:
				tblInfo.IsCommonHandle = true
				tblInfo.CommonHandleVersion = 1
				index := &model.IndexInfo{
					Name:    ast.NewCIStr("primary"),
					State:   model.StatePublic,
					Primary: true,
					Unique:  true,
					Columns: []*model.IndexColumn{
						{Name: ast.NewCIStr(c.name), Offset: offset, Length: types.UnspecifiedLength}},
				}
				primaryIndices = append(primaryIndices, index)
				tblInfo.Indices = primaryIndices
			}
		}
		cols = append(cols, buildColumnInfo(int64(offset), c))
	}
	for i, col := range cols {
		col.Offset = i
	}
	tblInfo.Columns = cols
	return tblInfo
}

// GetShardingInfo returns a nil or description string for the sharding information of given TableInfo.
// The returned description string may be:
//   - "NOT_SHARDED": for tables that SHARD_ROW_ID_BITS is not specified.
//   - "NOT_SHARDED(PK_IS_HANDLE)": for tables of which primary key is row id.
//   - "PK_AUTO_RANDOM_BITS={bit_number}, RANGE BITS={bit_number}": for tables of which primary key is sharded row id.
//   - "SHARD_BITS={bit_number}": for tables that with SHARD_ROW_ID_BITS.
//
// The returned nil indicates that sharding information is not suitable for the table(for example, when the table is a View).
// This function is exported for unit test.
func GetShardingInfo(dbInfo ast.CIStr, tableInfo *model.TableInfo) any {
	if tableInfo == nil || tableInfo.IsView() || metadef.IsMemOrSysDB(dbInfo.L) {
		return nil
	}
	shardingInfo := "NOT_SHARDED"
	if tableInfo.ContainsAutoRandomBits() {
		shardingInfo = "PK_AUTO_RANDOM_BITS=" + strconv.Itoa(int(tableInfo.AutoRandomBits))
		rangeBits := tableInfo.AutoRandomRangeBits
		if rangeBits != 0 && rangeBits != autoid.AutoRandomRangeBitsDefault {
			shardingInfo = fmt.Sprintf("%s, RANGE BITS=%d", shardingInfo, rangeBits)
		}
	} else if tableInfo.ShardRowIDBits > 0 {
		shardingInfo = "SHARD_BITS=" + strconv.Itoa(int(tableInfo.ShardRowIDBits))
	} else if tableInfo.PKIsHandle {
		shardingInfo = "NOT_SHARDED(PK_IS_HANDLE)"
	}
	return shardingInfo
}

const (
	// PrimaryKeyType is the string constant of PRIMARY KEY.
	PrimaryKeyType = "PRIMARY KEY"
	// PrimaryConstraint is the string constant of PRIMARY.
	PrimaryConstraint = "PRIMARY"
	// UniqueKeyType is the string constant of UNIQUE.
	UniqueKeyType = "UNIQUE"
	// ForeignKeyType is the string constant of Foreign Key.
	ForeignKeyType = "FOREIGN KEY"
)

const (
	// TiFlashWrite is the TiFlash write node in disaggregated mode.
	TiFlashWrite = "tiflash_write"
)

// ServerInfo represents the basic server information of single cluster component
type ServerInfo struct {
	ServerType     string
	Address        string
	StatusAddr     string
	Version        string
	GitHash        string
	StartTimestamp int64
	ServerID       uint64
	EngineRole     string
}

func (s *ServerInfo) isLoopBackOrUnspecifiedAddr(addr string) bool {
	tcpAddr, err := net.ResolveTCPAddr("", addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(tcpAddr.IP.String())
	return ip != nil && (ip.IsUnspecified() || ip.IsLoopback())
}

// ResolveLoopBackAddr exports for testing.
func (s *ServerInfo) ResolveLoopBackAddr() {
	if s.isLoopBackOrUnspecifiedAddr(s.Address) && !s.isLoopBackOrUnspecifiedAddr(s.StatusAddr) {
		addr, err1 := net.ResolveTCPAddr("", s.Address)
		statusAddr, err2 := net.ResolveTCPAddr("", s.StatusAddr)
		if err1 == nil && err2 == nil {
			addr.IP = statusAddr.IP
			s.Address = addr.String()
		}
	} else if !s.isLoopBackOrUnspecifiedAddr(s.Address) && s.isLoopBackOrUnspecifiedAddr(s.StatusAddr) {
		addr, err1 := net.ResolveTCPAddr("", s.Address)
		statusAddr, err2 := net.ResolveTCPAddr("", s.StatusAddr)
		if err1 == nil && err2 == nil {
			statusAddr.IP = addr.IP
			s.StatusAddr = statusAddr.String()
		}
	}
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
				serverID, err := strconv.ParseUint(parts[5], 10, 64)
				if err != nil {
					panic("convert parts[5] to uint64 failed")
				}
				servers = append(servers, ServerInfo{
					ServerType: parts[0],
					Address:    parts[1],
					StatusAddr: parts[2],
					Version:    parts[3],
					GitHash:    parts[4],
					ServerID:   serverID,
				})
			}
			failpoint.Return(servers, nil)
		}
	})

	type retriever func(ctx sessionctx.Context) ([]ServerInfo, error)
	retrievers := []retriever{GetTiDBServerInfo, GetPDServerInfo, func(ctx sessionctx.Context) ([]ServerInfo, error) {
		return GetStoreServerInfo(ctx.GetStore())
	}, GetTiProxyServerInfo, GetTiCDCServerInfo, GetTSOServerInfo, GetSchedulingServerInfo}
	//nolint: prealloc
	var servers []ServerInfo
	for _, r := range retrievers {
		nodes, err := r(ctx)
		if err != nil {
			return nil, err
		}

		// Create an error group with Panic recovery and concurrency limit
		resolveGroup := util.NewErrorGroupWithRecover()
		resolveGroup.SetLimit(runtime.GOMAXPROCS(0)) //Limit concurrency to number of CPU cores

		// Resolve loopback addresses concurrently for each node
		for i := range nodes {
			resolveGroup.Go(func() error {
				nodes[i].ResolveLoopBackAddr()
				return nil
			})
		}

		// Wait for all address resolutions to complete and check for errors
		if err := resolveGroup.Wait(); err != nil {
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
	var isDefaultVersion bool
	if len(config.GetGlobalConfig().ServerVersion) == 0 {
		isDefaultVersion = true
	}
	var servers = make([]ServerInfo, 0, len(tidbNodes))
	for _, node := range tidbNodes {
		servers = append(servers, ServerInfo{
			ServerType:     "tidb",
			Address:        net.JoinHostPort(node.IP, strconv.Itoa(int(node.Port))),
			StatusAddr:     net.JoinHostPort(node.IP, strconv.Itoa(int(node.StatusPort))),
			Version:        FormatTiDBVersion(node.Version, isDefaultVersion),
			GitHash:        node.GitHash,
			StartTimestamp: node.StartTimestamp,
			ServerID:       node.ServerIDGetter(),
		})
	}
	return servers, nil
}

// FormatTiDBVersion make TiDBVersion consistent to TiKV and PD.
// The default TiDBVersion is 5.7.25-TiDB-${TiDBReleaseVersion}.
func FormatTiDBVersion(TiDBVersion string, isDefaultVersion bool) string {
	var version, nodeVersion string

	// The user hasn't set the config 'ServerVersion'.
	if isDefaultVersion {
		nodeVersion = TiDBVersion[strings.Index(TiDBVersion, "TiDB-")+len("TiDB-"):]
		if len(nodeVersion) > 0 && nodeVersion[0] == 'v' {
			nodeVersion = nodeVersion[1:]
		}
		nodeVersions := strings.SplitN(nodeVersion, "-", 2)
		if len(nodeVersions) == 1 {
			version = nodeVersions[0]
		} else if len(nodeVersions) >= 2 {
			version = fmt.Sprintf("%s-%s", nodeVersions[0], nodeVersions[1])
		}
	} else { // The user has already set the config 'ServerVersion',it would be a complex scene, so just use the 'ServerVersion' as version.
		version = TiDBVersion
	}

	return version
}

// GetPDServerInfo returns all PD nodes information of cluster
func GetPDServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	// Get PD servers info.
	members, err := getEtcdMembers(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: maybe we should unify the PD API request interface.
	var (
		memberNum = len(members)
		servers   = make([]ServerInfo, 0, memberNum)
		errs      = make([]error, 0, memberNum)
	)
	if memberNum == 0 {
		return servers, nil
	}
	// Try on each member until one succeeds or all fail.
	for _, addr := range members {
		// Get PD version, git_hash
		url := fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), addr, pd.Status)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			logutil.BgLogger().Warn("create pd server info request error", zap.String("url", url), zap.Error(err))
			errs = append(errs, err)
			continue
		}
		req.Header.Add("PD-Allow-follower-handle", "true")
		resp, err := util.InternalHTTPClient().Do(req)
		if err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			logutil.BgLogger().Warn("request pd server info error", zap.String("url", url), zap.Error(err))
			errs = append(errs, err)
			continue
		}
		var content = struct {
			Version        string `json:"version"`
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{}
		err = json.NewDecoder(resp.Body).Decode(&content)
		terror.Log(resp.Body.Close())
		if err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			logutil.BgLogger().Warn("close pd server info request error", zap.String("url", url), zap.Error(err))
			errs = append(errs, err)
			continue
		}
		if len(content.Version) > 0 && content.Version[0] == 'v' {
			content.Version = content.Version[1:]
		}

		servers = append(servers, ServerInfo{
			ServerType:     "pd",
			Address:        addr,
			StatusAddr:     addr,
			Version:        content.Version,
			GitHash:        content.GitHash,
			StartTimestamp: content.StartTimestamp,
		})
	}
	// Return the errors if all members' requests fail.
	if len(errs) == memberNum {
		errorMsg := ""
		for idx, err := range errs {
			errorMsg += err.Error()
			if idx < memberNum-1 {
				errorMsg += "; "
			}
		}
		return nil, errors.Trace(fmt.Errorf("%s", errorMsg))
	}
	return servers, nil
}

// GetTSOServerInfo returns all TSO nodes information of cluster
func GetTSOServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	return getMicroServiceServerInfo(ctx, tsoServiceName)
}

// GetSchedulingServerInfo returns all scheduling nodes information of cluster
func GetSchedulingServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	return getMicroServiceServerInfo(ctx, schedulingServiceName)
}

func getMicroServiceServerInfo(ctx sessionctx.Context, serviceName string) ([]ServerInfo, error) {
	members, err := getEtcdMembers(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: maybe we should unify the PD API request interface.
	var servers []ServerInfo

	if len(members) == 0 {
		return servers, nil
	}
	// Try on each member until one succeeds or all fail.
	for _, addr := range members {
		// Get members
		url := fmt.Sprintf("%s://%s%s/%s", util.InternalHTTPSchema(), addr, "/pd/api/v2/ms/members", serviceName)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			logutil.BgLogger().Warn("create microservice server info request error", zap.String("service", serviceName), zap.String("url", url), zap.Error(err))
			continue
		}
		req.Header.Add("PD-Allow-follower-handle", "true")
		resp, err := util.InternalHTTPClient().Do(req)
		if err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			logutil.BgLogger().Warn("request microservice server info error", zap.String("service", serviceName), zap.String("url", url), zap.Error(err))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			terror.Log(resp.Body.Close())
			continue
		}
		var content = []struct {
			ServiceAddr    string `json:"service-addr"`
			Version        string `json:"version"`
			GitHash        string `json:"git-hash"`
			DeployPath     string `json:"deploy-path"`
			StartTimestamp int64  `json:"start-timestamp"`
		}{}
		err = json.NewDecoder(resp.Body).Decode(&content)
		terror.Log(resp.Body.Close())
		if err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			logutil.BgLogger().Warn("close microservice server info request error", zap.String("service", serviceName), zap.String("url", url), zap.Error(err))
			continue
		}

		for _, c := range content {
			addr := strings.TrimPrefix(c.ServiceAddr, "http://")
			addr = strings.TrimPrefix(addr, "https://")
			if len(c.Version) > 0 && c.Version[0] == 'v' {
				c.Version = c.Version[1:]
			}
			servers = append(servers, ServerInfo{
				ServerType:     serviceName,
				Address:        addr,
				StatusAddr:     addr,
				Version:        c.Version,
				GitHash:        c.GitHash,
				StartTimestamp: c.StartTimestamp,
			})
		}
		return servers, nil
	}
	return servers, nil
}

func getEtcdMembers(ctx sessionctx.Context) ([]string, error) {
	store := ctx.GetStore()
	etcd, ok := store.(kv.EtcdBackend)
	if !ok {
		return nil, errors.Errorf("%T not an etcd backend", store)
	}
	members, err := etcd.EtcdAddrs()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return members, nil
}

func isTiFlashStore(store *metapb.Store) bool {
	return slices.ContainsFunc(store.Labels, func(label *metapb.StoreLabel) bool {
		return label.GetKey() == placement.EngineLabelKey && label.GetValue() == placement.EngineLabelTiFlash
	})
}

func isTiFlashWriteNode(store *metapb.Store) bool {
	return slices.ContainsFunc(store.Labels, func(label *metapb.StoreLabel) bool {
		return label.GetKey() == placement.EngineRoleLabelKey && label.GetValue() == placement.EngineRoleLabelWrite
	})
}

// GetStoreServerInfo returns all store nodes(TiKV or TiFlash) cluster information
func GetStoreServerInfo(store kv.Storage) ([]ServerInfo, error) {
	failpoint.Inject("mockStoreServerInfo", func(val failpoint.Value) {
		if s := val.(string); len(s) > 0 {
			var servers []ServerInfo
			for _, server := range strings.Split(s, ";") {
				parts := strings.Split(server, ",")
				servers = append(servers, ServerInfo{
					ServerType:     parts[0],
					Address:        parts[1],
					StatusAddr:     parts[2],
					Version:        parts[3],
					GitHash:        parts[4],
					StartTimestamp: 0,
				})
			}
			failpoint.Return(servers, nil)
		}
	})

	// Get TiKV servers info.
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return nil, errors.Errorf("%T is not an TiKV or TiFlash store instance", store)
	}
	pdClient := tikvStore.GetRegionCache().PDClient()
	if pdClient == nil {
		return nil, errors.New("pd unavailable")
	}
	stores, err := pdClient.GetAllStores(context.Background())
	if err != nil {
		return nil, errors.Trace(err)
	}
	servers := make([]ServerInfo, 0, len(stores))
	for _, store := range stores {
		failpoint.Inject("mockStoreTombstone", func(val failpoint.Value) {
			if val.(bool) {
				store.State = metapb.StoreState_Tombstone
			}
		})

		if store.GetState() == metapb.StoreState_Tombstone {
			continue
		}
		var tp string
		if isTiFlashStore(store) {
			tp = kv.TiFlash.Name()
		} else {
			tp = tikv.GetStoreTypeByMeta(store).Name()
		}
		var engineRole string
		if isTiFlashWriteNode(store) {
			engineRole = placement.EngineRoleLabelWrite
		}
		servers = append(servers, ServerInfo{
			ServerType:     tp,
			Address:        store.Address,
			StatusAddr:     store.StatusAddress,
			Version:        FormatStoreServerVersion(store.Version),
			GitHash:        store.GitHash,
			StartTimestamp: store.StartTimestamp,
			EngineRole:     engineRole,
		})
	}
	return servers, nil
}

// FormatStoreServerVersion format version of store servers(Tikv or TiFlash)
func FormatStoreServerVersion(version string) string {
	if len(version) >= 1 && version[0] == 'v' {
		version = version[1:]
	}
	return version
}

// GetTiFlashStoreCount returns the count of tiflash server.
func GetTiFlashStoreCount(store kv.Storage) (cnt uint64, err error) {
	failpoint.Inject("mockTiFlashStoreCount", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(uint64(10), nil)
		}
	})

	stores, err := GetStoreServerInfo(store)
	if err != nil {
		return cnt, err
	}
	for _, store := range stores {
		if store.ServerType == kv.TiFlash.Name() {
			cnt++
		}
	}
	return cnt, nil
}

// GetTiProxyServerInfo gets server info of TiProxy from PD.
func GetTiProxyServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	tiproxyNodes, err := infosync.GetTiProxyServerInfo(context.Background())
	if err != nil {
		return nil, errors.Trace(err)
	}
	var servers = make([]ServerInfo, 0, len(tiproxyNodes))
	for _, node := range tiproxyNodes {
		servers = append(servers, ServerInfo{
			ServerType:     "tiproxy",
			Address:        net.JoinHostPort(node.IP, node.Port),
			StatusAddr:     net.JoinHostPort(node.IP, node.StatusPort),
			Version:        node.Version,
			GitHash:        node.GitHash,
			StartTimestamp: node.StartTimestamp,
		})
	}
	return servers, nil
}

// GetTiCDCServerInfo gets server info of TiCDC from PD.
func GetTiCDCServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	ticdcNodes, err := infosync.GetTiCDCServerInfo(context.Background())
	if err != nil {
		return nil, errors.Trace(err)
	}
	var servers = make([]ServerInfo, 0, len(ticdcNodes))
	for _, node := range ticdcNodes {
		servers = append(servers, ServerInfo{
			ServerType:     "ticdc",
			Address:        node.Address,
			StatusAddr:     node.Address,
			Version:        node.Version,
			GitHash:        node.GitHash,
			StartTimestamp: node.StartTimestamp,
		})
	}
	return servers, nil
}

// SysVarHiddenForSem checks if a given sysvar is hidden according to SEM and privileges.
func SysVarHiddenForSem(ctx sessionctx.Context, sysVarNameInLower string) bool {
	if !sem.IsEnabled() || !sem.IsInvisibleSysVar(sysVarNameInLower) {
		return false
	}
	checker := privilege.GetPrivilegeManager(ctx)
	if checker == nil || checker.RequestDynamicVerification(ctx.GetSessionVars().ActiveRoles, "RESTRICTED_VARIABLES_ADMIN", false) {
		return false
	}
	return true
}

// GetDataFromSessionVariables return the [name, value] of all session variables
func GetDataFromSessionVariables(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	sessionVars := sctx.GetSessionVars()
	sysVars := variable.GetSysVars()
	rows := make([][]types.Datum, 0, len(sysVars))
	for _, v := range sysVars {
		if SysVarHiddenForSem(sctx, v.Name) {
			continue
		}
		var value string
		value, err := sessionVars.GetSessionOrGlobalSystemVar(ctx, v.Name)
		if err != nil {
			return nil, err
		}
		row := types.MakeDatums(v.Name, value)
		rows = append(rows, row)
	}
	return rows, nil
}

// GetDataFromSessionConnectAttrs produces the rows for the session_connect_attrs table.
func GetDataFromSessionConnectAttrs(sctx sessionctx.Context, sameAccount bool) ([][]types.Datum, error) {
	sm := sctx.GetSessionManager()
	if sm == nil {
		return nil, nil
	}
	var user *auth.UserIdentity
	if sameAccount {
		user = sctx.GetSessionVars().User
	}
	allAttrs := sm.GetConAttrs(user)
	rows := make([][]types.Datum, 0, len(allAttrs)*10) // 10 Attributes per connection
	for pid, attrs := range allAttrs {                 // Note: PID is not ordered.
		// Sorts the attributes by key and gives ORDINAL_POSITION based on this. This is needed as we didn't store the
		// ORDINAL_POSITION and a map doesn't have a guaranteed sort order. This is needed to keep the ORDINAL_POSITION
		// stable over multiple queries.
		attrnames := make([]string, 0, len(attrs))
		for attrname := range attrs {
			attrnames = append(attrnames, attrname)
		}
		sort.Strings(attrnames)

		for ord, attrkey := range attrnames {
			row := types.MakeDatums(
				pid,
				attrkey,
				attrs[attrkey],
				ord,
			)
			rows = append(rows, row)
		}
	}
	return rows, nil
}

var tableNameToColumns = map[string][]columnInfo{
	TableSchemata:                           schemataCols,
	TableTables:                             tablesCols,
	TableColumns:                            columnsCols,
	tableColumnStatistics:                   columnStatisticsCols,
	TableStatistics:                         statisticsCols,
	TableCharacterSets:                      charsetCols,
	TableCollations:                         collationsCols,
	tableFiles:                              filesCols,
	TableProfiling:                          profilingCols,
	TablePartitions:                         partitionsCols,
	TableKeyColumn:                          keyColumnUsageCols,
	TableReferConst:                         referConstCols,
	tablePlugins:                            pluginsCols,
	TableConstraints:                        tableConstraintsCols,
	tableTriggers:                           tableTriggersCols,
	TableUserPrivileges:                     tableUserPrivilegesCols,
	tableSchemaPrivileges:                   tableSchemaPrivilegesCols,
	tableTablePrivileges:                    tableTablePrivilegesCols,
	tableColumnPrivileges:                   tableColumnPrivilegesCols,
	TableEngines:                            tableEnginesCols,
	TableViews:                              tableViewsCols,
	tableRoutines:                           tableRoutinesCols,
	tableParameters:                         tableParametersCols,
	tableEvents:                             tableEventsCols,
	tableOptimizerTrace:                     tableOptimizerTraceCols,
	tableTableSpaces:                        tableTableSpacesCols,
	TableCollationCharacterSetApplicability: tableCollationCharacterSetApplicabilityCols,
	TableProcesslist:                        tableProcesslistCols,
	TableTiDBIndexes:                        tableTiDBIndexesCols,
	TableSlowQuery:                          slowQueryCols,
	TableTiDBHotRegions:                     TableTiDBHotRegionsCols,
	TableTiDBHotRegionsHistory:              TableTiDBHotRegionsHistoryCols,
	TableTiKVStoreStatus:                    TableTiKVStoreStatusCols,
	TableAnalyzeStatus:                      tableAnalyzeStatusCols,
	TableTiKVRegionStatus:                   TableTiKVRegionStatusCols,
	TableTiKVRegionPeers:                    TableTiKVRegionPeersCols,
	TableTiDBServersInfo:                    tableTiDBServersInfoCols,
	TableClusterInfo:                        tableClusterInfoCols,
	TableClusterConfig:                      tableClusterConfigCols,
	TableClusterLog:                         tableClusterLogCols,
	TableClusterLoad:                        tableClusterLoadCols,
	TableTiFlashReplica:                     tableTableTiFlashReplicaCols,
	TableClusterHardware:                    tableClusterHardwareCols,
	TableClusterSystemInfo:                  tableClusterSystemInfoCols,
	TableInspectionResult:                   tableInspectionResultCols,
	TableMetricSummary:                      tableMetricSummaryCols,
	TableMetricSummaryByLabel:               tableMetricSummaryByLabelCols,
	TableMetricTables:                       tableMetricTablesCols,
	TableInspectionSummary:                  tableInspectionSummaryCols,
	TableInspectionRules:                    tableInspectionRulesCols,
	TableDDLJobs:                            tableDDLJobsCols,
	TableSequences:                          tableSequencesCols,
	TableStatementsSummary:                  tableStatementsSummaryCols,
	TableStatementsSummaryHistory:           tableStatementsSummaryCols,
	TableStatementsSummaryEvicted:           tableStatementsSummaryEvictedCols,
	TableStorageStats:                       tableStorageStatsCols,
	TableTiDBStatementsStats:                tableTiDBStatementsStatsCols,
	TableTiFlashTables:                      tableTableTiFlashTablesCols,
	TableTiFlashSegments:                    tableTableTiFlashSegmentsCols,
	TableTiFlashIndexes:                     tableTiFlashIndexesCols,
	TableClientErrorsSummaryGlobal:          tableClientErrorsSummaryGlobalCols,
	TableClientErrorsSummaryByUser:          tableClientErrorsSummaryByUserCols,
	TableClientErrorsSummaryByHost:          tableClientErrorsSummaryByHostCols,
	TableTiDBTrx:                            tableTiDBTrxCols,
	TableDeadlocks:                          tableDeadlocksCols,
	TableDataLockWaits:                      tableDataLockWaitsCols,
	TableAttributes:                         tableAttributesCols,
	TablePlacementPolicies:                  tablePlacementPoliciesCols,
	TableTrxSummary:                         tableTrxSummaryCols,
	TableVariablesInfo:                      tableVariablesInfoCols,
	TableUserAttributes:                     tableUserAttributesCols,
	TableMemoryUsage:                        tableMemoryUsageCols,
	TableMemoryUsageOpsHistory:              tableMemoryUsageOpsHistoryCols,
	TableResourceGroups:                     tableResourceGroupsCols,
	TableRunawayWatches:                     tableRunawayWatchListCols,
	TableCheckConstraints:                   tableCheckConstraintsCols,
	TableTiDBCheckConstraints:               tableTiDBCheckConstraintsCols,
	TableKeywords:                           tableKeywords,
	TableTiDBIndexUsage:                     tableTiDBIndexUsage,
	TableTiDBPlanCache:                      tablePlanCache,
	TableKeyspaceMeta:                       tableKeyspaceMetaCols,
}

func createInfoSchemaTable(_ autoid.Allocators, _ func() (pools.Resource, error), meta *model.TableInfo) (table.Table, error) {
	columns := make([]*table.Column, len(meta.Columns))
	for i, col := range meta.Columns {
		columns[i] = table.ToColumn(col)
	}
	tp := table.VirtualTable
	if IsClusterTableByName(metadef.InformationSchemaName.L, meta.Name.L) {
		tp = table.ClusterTable
	}
	return &infoschemaTable{meta: meta, cols: columns, tp: tp}, nil
}

type infoschemaTable struct {
	meta *model.TableInfo
	cols []*table.Column
	tp   table.Type
}

// IterRecords implements table.Table IterRecords interface.
func (*infoschemaTable) IterRecords(ctx context.Context, sctx sessionctx.Context, cols []*table.Column, fn table.RecordIterFunc) error {
	return nil
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

// DeletableCols implements table.Table WritableCols interface.
func (it *infoschemaTable) DeletableCols() []*table.Column {
	return it.cols
}

// FullHiddenColsAndVisibleCols implements table FullHiddenColsAndVisibleCols interface.
func (it *infoschemaTable) FullHiddenColsAndVisibleCols() []*table.Column {
	return it.cols
}

// Indices implements table.Table Indices interface.
func (it *infoschemaTable) Indices() []table.Index {
	return nil
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (it *infoschemaTable) DeletableIndices() []table.Index {
	return nil
}

// WritableConstraint implements table.Table WritableConstraint interface.
func (it *infoschemaTable) WritableConstraint() []*table.Constraint {
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

// AddRecord implements table.Table AddRecord interface.
func (it *infoschemaTable) AddRecord(ctx table.MutateContext, txn kv.Transaction, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return nil, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (it *infoschemaTable) RemoveRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opts ...table.RemoveRecordOption) error {
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (it *infoschemaTable) UpdateRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, oldData, newData []types.Datum, touched []bool, opts ...table.UpdateRecordOption) error {
	return table.ErrUnsupportedOp
}

// Allocators implements table.Table Allocators interface.
func (it *infoschemaTable) Allocators(_ table.AllocatorContext) autoid.Allocators {
	return autoid.Allocators{}
}

// Meta implements table.Table Meta interface.
func (it *infoschemaTable) Meta() *model.TableInfo {
	return it.meta
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (it *infoschemaTable) GetPhysicalID() int64 {
	return it.meta.ID
}

// Type implements table.Table Type interface.
func (it *infoschemaTable) Type() table.Type {
	return it.tp
}

// GetPartitionedTable implements table.Table GetPartitionedTable interface.
func (it *infoschemaTable) GetPartitionedTable() table.PartitionedTable {
	return nil
}

// VirtualTable is a dummy table.Table implementation.
type VirtualTable struct{}

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

// DeletableCols implements table.Table WritableCols interface.
func (vt *VirtualTable) DeletableCols() []*table.Column {
	return nil
}

// FullHiddenColsAndVisibleCols implements table FullHiddenColsAndVisibleCols interface.
func (vt *VirtualTable) FullHiddenColsAndVisibleCols() []*table.Column {
	return nil
}

// Indices implements table.Table Indices interface.
func (vt *VirtualTable) Indices() []table.Index {
	return nil
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (vt *VirtualTable) DeletableIndices() []table.Index {
	return nil
}

// WritableConstraint implements table.Table WritableConstraint interface.
func (vt *VirtualTable) WritableConstraint() []*table.Constraint {
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

// AddRecord implements table.Table AddRecord interface.
func (vt *VirtualTable) AddRecord(ctx table.MutateContext, txn kv.Transaction, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return nil, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (vt *VirtualTable) RemoveRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opts ...table.RemoveRecordOption) error {
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (vt *VirtualTable) UpdateRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, oldData, newData []types.Datum, touched []bool, opts ...table.UpdateRecordOption) error {
	return table.ErrUnsupportedOp
}

// Allocators implements table.Table Allocators interface.
func (vt *VirtualTable) Allocators(_ table.AllocatorContext) autoid.Allocators {
	return autoid.Allocators{}
}

// Meta implements table.Table Meta interface.
func (vt *VirtualTable) Meta() *model.TableInfo {
	return nil
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (vt *VirtualTable) GetPhysicalID() int64 {
	return 0
}

// Type implements table.Table Type interface.
func (vt *VirtualTable) Type() table.Type {
	return table.VirtualTable
}

// GetTiFlashServerInfo returns all TiFlash server infos
func GetTiFlashServerInfo(store kv.Storage) ([]ServerInfo, error) {
	if config.GetGlobalConfig().DisaggregatedTiFlash {
		return nil, table.ErrUnsupportedOp
	}
	serversInfo, err := GetStoreServerInfo(store)
	if err != nil {
		return nil, err
	}
	serversInfo = FilterClusterServerInfo(serversInfo, set.NewStringSet(kv.TiFlash.Name()), set.NewStringSet())
	return serversInfo, nil
}

// FetchClusterServerInfoWithoutPrivilegeCheck fetches cluster server information
func FetchClusterServerInfoWithoutPrivilegeCheck(ctx context.Context, vars *variable.SessionVars, serversInfo []ServerInfo, serverInfoType diagnosticspb.ServerInfoType, recordWarningInStmtCtx bool) ([][]types.Datum, error) {
	type result struct {
		idx  int
		rows [][]types.Datum
		err  error
	}
	wg := sync.WaitGroup{}
	ch := make(chan result, len(serversInfo))
	infoTp := serverInfoType
	finalRows := make([][]types.Datum, 0, len(serversInfo)*10)
	for i, srv := range serversInfo {
		address := srv.Address
		remote := address
		if srv.ServerType == "tidb" || srv.ServerType == "tiproxy" {
			remote = srv.StatusAddr
		}
		wg.Add(1)
		go func(index int, remote, address, serverTP string) {
			util.WithRecovery(func() {
				defer wg.Done()
				items, err := getServerInfoByGRPC(ctx, remote, infoTp)
				if err != nil {
					ch <- result{idx: index, err: err}
					return
				}
				partRows := serverInfoItemToRows(items, serverTP, address)
				ch <- result{idx: index, rows: partRows}
			}, nil)
		}(i, remote, address, srv.ServerType)
	}
	wg.Wait()
	close(ch)
	// Keep the original order to make the result more stable
	var results []result //nolint: prealloc
	for result := range ch {
		if result.err != nil {
			if recordWarningInStmtCtx {
				vars.StmtCtx.AppendWarning(result.err)
			} else {
				log.Warn(result.err.Error())
			}
			continue
		}
		results = append(results, result)
	}
	slices.SortFunc(results, func(i, j result) int { return cmp.Compare(i.idx, j.idx) })
	for _, result := range results {
		finalRows = append(finalRows, result.rows...)
	}
	return finalRows, nil
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

func getServerInfoByGRPC(ctx context.Context, address string, tp diagnosticspb.ServerInfoType) ([]*diagnosticspb.ServerInfoItem, error) {
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	security := config.GetGlobalConfig().Security
	if len(security.ClusterSSLCA) != 0 {
		clusterSecurity := security.ClusterSecurity()
		tlsConfig, err := clusterSecurity.ToTLSConfig()
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
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	r, err := cli.ServerInfo(ctx, &diagnosticspb.ServerInfoRequest{Tp: tp})
	if err != nil {
		return nil, err
	}
	return r.Items, nil
}

// FilterClusterServerInfo filters serversInfo by nodeTypes and addresses
func FilterClusterServerInfo(serversInfo []ServerInfo, nodeTypes, addresses set.StringSet) []ServerInfo {
	if len(nodeTypes) == 0 && len(addresses) == 0 {
		return serversInfo
	}

	filterServers := make([]ServerInfo, 0, len(serversInfo))
	for _, srv := range serversInfo {
		// Skip some node type which has been filtered in WHERE clause
		// e.g: SELECT * FROM cluster_config WHERE type='tikv'
		if len(nodeTypes) > 0 && !nodeTypes.Exist(srv.ServerType) {
			continue
		}
		// Skip some node address which has been filtered in WHERE clause
		// e.g: SELECT * FROM cluster_config WHERE address='192.16.8.12:2379'
		if len(addresses) > 0 && !addresses.Exist(srv.Address) {
			continue
		}
		filterServers = append(filterServers, srv)
	}
	return filterServers
}

// GetDataFromStatusByConn is getting the per-connection status for `performance_schema.status_by_connection`
func GetDataFromStatusByConn(sctx sessionctx.Context) ([][]types.Datum, error) {
	sm := sctx.GetSessionManager()
	if sm == nil {
		return nil, nil
	}
	statusVars := sm.GetStatusVars()
	rows := make([][]types.Datum, 0, 2*len(statusVars))
	for pid, svar := range statusVars {
		for varkey, varval := range svar {
			row := types.MakeDatums(
				pid,
				varkey,
				varval,
			)
			rows = append(rows, row)
		}
	}
	return rows, nil
}
