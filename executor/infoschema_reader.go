// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/tidb/ddl/label"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	binaryJson "github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/deadlockhistory"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/keydecoder"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/resourcegrouptag"
	"github.com/pingcap/tidb/util/sem"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stmtsummary"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

type memtableRetriever struct {
	dummyCloser
	table       *model.TableInfo
	columns     []*model.ColumnInfo
	rows        [][]types.Datum
	rowIdx      int
	retrieved   bool
	initialized bool
	extractor   plannercore.MemTablePredicateExtractor
}

// retrieve implements the infoschemaRetriever interface
func (e *memtableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.table.Name.O == infoschema.TableClusterInfo && !hasPriv(sctx, mysql.ProcessPriv) {
		return nil, plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
	}
	if e.retrieved {
		return nil, nil
	}

	// Cache the ret full rows in schemataRetriever
	if !e.initialized {
		is := sctx.GetInfoSchema().(infoschema.InfoSchema)
		dbs := is.AllSchemas()
		sort.Sort(infoschema.SchemasSorter(dbs))
		var err error
		switch e.table.Name.O {
		case infoschema.TableSchemata:
			e.setDataFromSchemata(sctx, dbs)
		case infoschema.TableStatistics:
			e.setDataForStatistics(sctx, dbs)
		case infoschema.TableTables:
			err = e.setDataFromTables(ctx, sctx, dbs)
		case infoschema.TableReferConst:
			err = e.setDataFromReferConst(ctx, sctx, dbs)
		case infoschema.TableSequences:
			e.setDataFromSequences(sctx, dbs)
		case infoschema.TablePartitions:
			err = e.setDataFromPartitions(ctx, sctx, dbs)
		case infoschema.TableClusterInfo:
			err = e.dataForTiDBClusterInfo(sctx)
		case infoschema.TableAnalyzeStatus:
			err = e.setDataForAnalyzeStatus(sctx)
		case infoschema.TableTiDBIndexes:
			e.setDataFromIndexes(sctx, dbs)
		case infoschema.TableViews:
			e.setDataFromViews(sctx, dbs)
		case infoschema.TableEngines:
			e.setDataFromEngines()
		case infoschema.TableCharacterSets:
			e.setDataFromCharacterSets()
		case infoschema.TableCollations:
			e.setDataFromCollations()
		case infoschema.TableKeyColumn:
			e.setDataFromKeyColumnUsage(sctx, dbs)
		case infoschema.TableMetricTables:
			e.setDataForMetricTables(sctx)
		case infoschema.TableProfiling:
			e.setDataForPseudoProfiling(sctx)
		case infoschema.TableCollationCharacterSetApplicability:
			e.dataForCollationCharacterSetApplicability()
		case infoschema.TableProcesslist:
			e.setDataForProcessList(sctx)
		case infoschema.ClusterTableProcesslist:
			err = e.setDataForClusterProcessList(sctx)
		case infoschema.TableUserPrivileges:
			e.setDataFromUserPrivileges(sctx)
		case infoschema.TableTiKVRegionStatus:
			err = e.setDataForTiKVRegionStatus(sctx)
		case infoschema.TableTiDBHotRegions:
			err = e.setDataForTiDBHotRegions(sctx)
		case infoschema.TableConstraints:
			e.setDataFromTableConstraints(sctx, dbs)
		case infoschema.TableSessionVar:
			err = e.setDataFromSessionVar(sctx)
		case infoschema.TableTiDBServersInfo:
			err = e.setDataForServersInfo(sctx)
		case infoschema.TableTiFlashReplica:
			e.dataForTableTiFlashReplica(sctx, dbs)
		case infoschema.TableTiKVStoreStatus:
			err = e.dataForTiKVStoreStatus(sctx)
		case infoschema.TableStatementsSummaryEvicted,
			infoschema.ClusterTableStatementsSummaryEvicted:
			err = e.setDataForStatementsSummaryEvicted(sctx)
		case infoschema.TableClientErrorsSummaryGlobal,
			infoschema.TableClientErrorsSummaryByUser,
			infoschema.TableClientErrorsSummaryByHost:
			err = e.setDataForClientErrorsSummary(sctx, e.table.Name.O)
		case infoschema.TableAttributes:
			err = e.setDataForAttributes(sctx, is)
		case infoschema.TablePlacementPolicies:
			err = e.setDataFromPlacementPolicies(sctx)
		}
		if err != nil {
			return nil, err
		}
		e.initialized = true
	}

	// Adjust the amount of each return
	maxCount := 1024
	retCount := maxCount
	if e.rowIdx+maxCount > len(e.rows) {
		retCount = len(e.rows) - e.rowIdx
		e.retrieved = true
	}
	ret := make([][]types.Datum, retCount)
	for i := e.rowIdx; i < e.rowIdx+retCount; i++ {
		ret[i-e.rowIdx] = e.rows[i]
	}
	e.rowIdx += retCount
	return adjustColumns(ret, e.columns, e.table), nil
}

func getRowCountAllTable(ctx context.Context, sctx sessionctx.Context) (map[int64]uint64, error) {
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "select table_id, count from mysql.stats_meta")
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

func getColLengthAllTables(ctx context.Context, sctx sessionctx.Context) (map[tableHistID]uint64, error) {
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "select table_id, hist_id, tot_col_size from mysql.stats_histograms where is_index = 0")
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
	columnLength := make(map[string]uint64, len(info.Columns))
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
	mu         sync.RWMutex
	modifyTime time.Time
	tableRows  map[int64]uint64
	colLength  map[tableHistID]uint64
}

var tableStatsCache = &statsCache{}

// TableStatsCacheExpiry is the expiry time for table stats cache.
var TableStatsCacheExpiry = 3 * time.Second

func (c *statsCache) get(ctx context.Context, sctx sessionctx.Context) (map[int64]uint64, map[tableHistID]uint64, error) {
	c.mu.RLock()
	if time.Since(c.modifyTime) < TableStatsCacheExpiry {
		tableRows, colLength := c.tableRows, c.colLength
		c.mu.RUnlock()
		return tableRows, colLength, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.modifyTime) < TableStatsCacheExpiry {
		return c.tableRows, c.colLength, nil
	}
	tableRows, err := getRowCountAllTable(ctx, sctx)
	if err != nil {
		return nil, nil, err
	}
	colLength, err := getColLengthAllTables(ctx, sctx)
	if err != nil {
		return nil, nil, err
	}

	c.tableRows = tableRows
	c.colLength = colLength
	c.modifyTime = time.Now()
	return tableRows, colLength, nil
}

func getAutoIncrementID(ctx sessionctx.Context, schema *model.DBInfo, tblInfo *model.TableInfo) (int64, error) {
	is := ctx.GetInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(schema.Name, tblInfo.Name)
	if err != nil {
		return 0, err
	}
	return tbl.Allocators(ctx).Get(autoid.RowIDAllocType).Base() + 1, nil
}

func hasPriv(ctx sessionctx.Context, priv mysql.PrivilegeType) bool {
	pm := privilege.GetPrivilegeManager(ctx)
	if pm == nil {
		// internal session created with createSession doesn't has the PrivilegeManager. For most experienced cases before,
		// we use it like this:
		// ```
		// checker := privilege.GetPrivilegeManager(ctx)
		// if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
		//	  continue
		// }
		// do something.
		// ```
		// So once the privilege manager is nil, it's a signature of internal sql, so just passing the checker through.
		return true
	}
	return pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, "", "", "", priv)
}

func (e *memtableRetriever) setDataFromSchemata(ctx sessionctx.Context, schemas []*model.DBInfo) {
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
		var policyName interface{}
		if schema.PlacementPolicyRef != nil {
			policyName = schema.PlacementPolicyRef.Name.O
		}

		if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, "", "", mysql.AllPrivMask) {
			continue
		}
		record := types.MakeDatums(
			infoschema.CatalogVal, // CATALOG_NAME
			schema.Name.O,         // SCHEMA_NAME
			charset,               // DEFAULT_CHARACTER_SET_NAME
			collation,             // DEFAULT_COLLATION_NAME
			nil,                   // SQL_PATH
			policyName,            // TIDB_PLACEMENT_POLICY_NAME
		)
		rows = append(rows, record)
	}
	e.rows = rows
}

func (e *memtableRetriever) setDataForStatistics(ctx sessionctx.Context, schemas []*model.DBInfo) {
	checker := privilege.GetPrivilegeManager(ctx)
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}
			e.setDataForStatisticsInTable(schema, table)
		}
	}
}

func (e *memtableRetriever) setDataForStatisticsInTable(schema *model.DBInfo, table *model.TableInfo) {
	var rows [][]types.Datum
	if table.PKIsHandle {
		for _, col := range table.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				record := types.MakeDatums(
					infoschema.CatalogVal, // TABLE_CATALOG
					schema.Name.O,         // TABLE_SCHEMA
					table.Name.O,          // TABLE_NAME
					"0",                   // NON_UNIQUE
					schema.Name.O,         // INDEX_SCHEMA
					"PRIMARY",             // INDEX_NAME
					1,                     // SEQ_IN_INDEX
					col.Name.O,            // COLUMN_NAME
					"A",                   // COLLATION
					0,                     // CARDINALITY
					nil,                   // SUB_PART
					nil,                   // PACKED
					"",                    // NULLABLE
					"BTREE",               // INDEX_TYPE
					"",                    // COMMENT
					"",                    // INDEX_COMMENT
					"YES",                 // IS_VISIBLE
					nil,                   // Expression
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
			if mysql.HasNotNullFlag(col.GetFlag()) {
				nullable = ""
			}

			visible := "YES"
			if index.Invisible {
				visible = "NO"
			}

			colName := col.Name.O
			var expression interface{}
			expression = nil
			tblCol := table.Columns[col.Offset]
			if tblCol.Hidden {
				colName = "NULL"
				expression = tblCol.GeneratedExprString
			}

			record := types.MakeDatums(
				infoschema.CatalogVal, // TABLE_CATALOG
				schema.Name.O,         // TABLE_SCHEMA
				table.Name.O,          // TABLE_NAME
				nonUnique,             // NON_UNIQUE
				schema.Name.O,         // INDEX_SCHEMA
				index.Name.O,          // INDEX_NAME
				i+1,                   // SEQ_IN_INDEX
				colName,               // COLUMN_NAME
				"A",                   // COLLATION
				0,                     // CARDINALITY
				nil,                   // SUB_PART
				nil,                   // PACKED
				nullable,              // NULLABLE
				"BTREE",               // INDEX_TYPE
				"",                    // COMMENT
				index.Comment,         // INDEX_COMMENT
				visible,               // IS_VISIBLE
				expression,            // Expression
			)
			rows = append(rows, record)
		}
	}
	e.rows = append(e.rows, rows...)
}

func (e *memtableRetriever) setDataFromReferConst(ctx context.Context, sctx sessionctx.Context, schemas []*model.DBInfo) error {
	checker := privilege.GetPrivilegeManager(sctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if !table.IsBaseTable() {
				continue
			}
			if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}
			for _, fk := range table.ForeignKeys {
				updateRule, deleteRule := "NO ACTION", "NO ACTION"
				if ast.ReferOptionType(fk.OnUpdate) != 0 {
					updateRule = ast.ReferOptionType(fk.OnUpdate).String()
				}
				if ast.ReferOptionType(fk.OnDelete) != 0 {
					deleteRule = ast.ReferOptionType(fk.OnDelete).String()
				}
				record := types.MakeDatums(
					infoschema.CatalogVal, // CONSTRAINT_CATALOG
					schema.Name.O,         // CONSTRAINT_SCHEMA
					fk.Name.O,             // CONSTRAINT_NAME
					infoschema.CatalogVal, // UNIQUE_CONSTRAINT_CATALOG
					schema.Name.O,         // UNIQUE_CONSTRAINT_SCHEMA
					"PRIMARY",             // UNIQUE_CONSTRAINT_NAME
					"NONE",                // MATCH_OPTION
					updateRule,            // UPDATE_RULE
					deleteRule,            // DELETE_RULE
					table.Name.O,          // TABLE_NAME
					fk.RefTable.O,         // REFERENCED_TABLE_NAME
				)
				rows = append(rows, record)
			}
		}
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromTables(ctx context.Context, sctx sessionctx.Context, schemas []*model.DBInfo) error {
	tableRowsMap, colLengthMap, err := tableStatsCache.get(ctx, sctx)
	if err != nil {
		return err
	}

	checker := privilege.GetPrivilegeManager(sctx)

	var rows [][]types.Datum
	createTimeTp := mysql.TypeDatetime
	loc := sctx.GetSessionVars().TimeZone
	if loc == nil {
		loc = time.Local
	}
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			collation := table.Collate
			if collation == "" {
				collation = mysql.DefaultCollationName
			}
			createTime := types.NewTime(types.FromGoTime(table.GetUpdateTime().In(loc)), createTimeTp, types.DefaultFsp)

			createOptions := ""

			if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}
			pkType := "NONCLUSTERED"
			if !table.IsView() {
				if table.GetPartitionInfo() != nil {
					createOptions = "partitioned"
				} else if table.TableCacheStatusType == model.TableCacheStatusEnable {
					createOptions = "cached=on"
				}
				var autoIncID interface{}
				hasAutoIncID, _ := infoschema.HasAutoIncrementColumn(table)
				if hasAutoIncID {
					autoIncID, err = getAutoIncrementID(sctx, schema, table)
					if err != nil {
						return err
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
				tableType := "BASE TABLE"
				if util.IsSystemView(schema.Name.L) {
					tableType = "SYSTEM VIEW"
				}
				if table.IsSequence() {
					tableType = "SEQUENCE"
					// sequence is always 1 row regardless of stats.
					rowCount = 1
				}
				if table.HasClusteredIndex() {
					pkType = "CLUSTERED"
				}
				shardingInfo := infoschema.GetShardingInfo(schema, table)
				var policyName interface{}
				if table.PlacementPolicyRef != nil {
					policyName = table.PlacementPolicyRef.Name.O
				}
				record := types.MakeDatums(
					infoschema.CatalogVal, // TABLE_CATALOG
					schema.Name.O,         // TABLE_SCHEMA
					table.Name.O,          // TABLE_NAME
					tableType,             // TABLE_TYPE
					"InnoDB",              // ENGINE
					uint64(10),            // VERSION
					"Compact",             // ROW_FORMAT
					rowCount,              // TABLE_ROWS
					avgRowLength,          // AVG_ROW_LENGTH
					dataLength,            // DATA_LENGTH
					uint64(0),             // MAX_DATA_LENGTH
					indexLength,           // INDEX_LENGTH
					uint64(0),             // DATA_FREE
					autoIncID,             // AUTO_INCREMENT
					createTime,            // CREATE_TIME
					nil,                   // UPDATE_TIME
					nil,                   // CHECK_TIME
					collation,             // TABLE_COLLATION
					nil,                   // CHECKSUM
					createOptions,         // CREATE_OPTIONS
					table.Comment,         // TABLE_COMMENT
					table.ID,              // TIDB_TABLE_ID
					shardingInfo,          // TIDB_ROW_ID_SHARDING_INFO
					pkType,                // TIDB_PK_TYPE
					policyName,            // TIDB_PLACEMENT_POLICY_NAME
				)
				rows = append(rows, record)
			} else {
				record := types.MakeDatums(
					infoschema.CatalogVal, // TABLE_CATALOG
					schema.Name.O,         // TABLE_SCHEMA
					table.Name.O,          // TABLE_NAME
					"VIEW",                // TABLE_TYPE
					nil,                   // ENGINE
					nil,                   // VERSION
					nil,                   // ROW_FORMAT
					nil,                   // TABLE_ROWS
					nil,                   // AVG_ROW_LENGTH
					nil,                   // DATA_LENGTH
					nil,                   // MAX_DATA_LENGTH
					nil,                   // INDEX_LENGTH
					nil,                   // DATA_FREE
					nil,                   // AUTO_INCREMENT
					createTime,            // CREATE_TIME
					nil,                   // UPDATE_TIME
					nil,                   // CHECK_TIME
					nil,                   // TABLE_COLLATION
					nil,                   // CHECKSUM
					nil,                   // CREATE_OPTIONS
					"VIEW",                // TABLE_COMMENT
					table.ID,              // TIDB_TABLE_ID
					nil,                   // TIDB_ROW_ID_SHARDING_INFO
					pkType,                // TIDB_PK_TYPE
					nil,                   // TIDB_PLACEMENT_POLICY_NAME
				)
				rows = append(rows, record)
			}
		}
	}
	e.rows = rows
	return nil
}

func (e *hugeMemTableRetriever) setDataForColumns(ctx context.Context, sctx sessionctx.Context, extractor *plannercore.ColumnsTableExtractor) error {
	checker := privilege.GetPrivilegeManager(sctx)
	e.rows = e.rows[:0]
	batch := 1024
	for ; e.dbsIdx < len(e.dbs); e.dbsIdx++ {
		schema := e.dbs[e.dbsIdx]
		for e.tblIdx < len(schema.Tables) {
			table := schema.Tables[e.tblIdx]
			e.tblIdx++
			hasPrivs := false
			var priv mysql.PrivilegeType
			if checker != nil {
				for _, p := range mysql.AllColumnPrivs {
					if checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", p) {
						hasPrivs = true
						priv |= p
					}
				}
				if !hasPrivs {
					continue
				}
			}

			e.dataForColumnsInTable(ctx, sctx, schema, table, priv, extractor)
			if len(e.rows) >= batch {
				return nil
			}
		}
		e.tblIdx = 0
	}
	return nil
}

func (e *hugeMemTableRetriever) dataForColumnsInTable(ctx context.Context, sctx sessionctx.Context, schema *model.DBInfo, tbl *model.TableInfo, priv mysql.PrivilegeType, extractor *plannercore.ColumnsTableExtractor) {
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	if tbl.IsView() {
		e.viewMu.Lock()
		_, ok := e.viewSchemaMap[tbl.ID]
		if !ok {
			var viewLogicalPlan plannercore.Plan
			// Build plan is not thread safe, there will be concurrency on sessionctx.
			if err := runWithSystemSession(sctx, func(s sessionctx.Context) error {
				planBuilder, _ := plannercore.NewPlanBuilder().Init(s, is, &hint.BlockHintProcessor{})
				var err error
				viewLogicalPlan, err = planBuilder.BuildDataSourceFromView(ctx, schema.Name, tbl)
				return errors.Trace(err)
			}); err != nil {
				sctx.GetSessionVars().StmtCtx.AppendWarning(err)
				e.viewMu.Unlock()
				return
			}
			e.viewSchemaMap[tbl.ID] = viewLogicalPlan.Schema()
			e.viewOutputNamesMap[tbl.ID] = viewLogicalPlan.OutputNames()
		}
		e.viewMu.Unlock()
	}

	var tableSchemaRegexp, tableNameRegexp, columnsRegexp []collate.WildcardPattern
	var tableSchemaFilterEnable,
		tableNameFilterEnable, columnsFilterEnable bool
	if !extractor.SkipRequest {
		tableSchemaFilterEnable = extractor.TableSchema.Count() > 0
		tableNameFilterEnable = extractor.TableName.Count() > 0
		columnsFilterEnable = extractor.ColumnName.Count() > 0
		if len(extractor.TableSchemaPatterns) > 0 {
			tableSchemaRegexp = make([]collate.WildcardPattern, len(extractor.TableSchemaPatterns))
			for i, pattern := range extractor.TableSchemaPatterns {
				tableSchemaRegexp[i] = collate.GetCollatorByID(collate.CollationName2ID(mysql.UTF8MB4DefaultCollation)).Pattern()
				tableSchemaRegexp[i].Compile(pattern, byte('\\'))
			}
		}
		if len(extractor.TableNamePatterns) > 0 {
			tableNameRegexp = make([]collate.WildcardPattern, len(extractor.TableNamePatterns))
			for i, pattern := range extractor.TableNamePatterns {
				tableNameRegexp[i] = collate.GetCollatorByID(collate.CollationName2ID(mysql.UTF8MB4DefaultCollation)).Pattern()
				tableNameRegexp[i].Compile(pattern, byte('\\'))
			}
		}
		if len(extractor.ColumnNamePatterns) > 0 {
			columnsRegexp = make([]collate.WildcardPattern, len(extractor.ColumnNamePatterns))
			for i, pattern := range extractor.ColumnNamePatterns {
				columnsRegexp[i] = collate.GetCollatorByID(collate.CollationName2ID(mysql.UTF8MB4DefaultCollation)).Pattern()
				columnsRegexp[i].Compile(pattern, byte('\\'))
			}
		}
	}
ForColumnsTag:
	for i, col := range tbl.Columns {
		if col.Hidden {
			continue
		}

		ft := &col.FieldType
		if tbl.IsView() {
			e.viewMu.RLock()
			if e.viewSchemaMap[tbl.ID] != nil {
				// If this is a view, replace the column with the view column.
				idx := expression.FindFieldNameIdxByColName(e.viewOutputNamesMap[tbl.ID], col.Name.L)
				if idx >= 0 {
					col1 := e.viewSchemaMap[tbl.ID].Columns[idx]
					ft = col1.GetType()
				}
			}
			e.viewMu.RUnlock()
		}
		if !extractor.SkipRequest {
			if tableSchemaFilterEnable && !extractor.TableSchema.Exist(schema.Name.L) {
				continue
			}
			if tableNameFilterEnable && !extractor.TableName.Exist(tbl.Name.L) {
				continue
			}
			if columnsFilterEnable && !extractor.ColumnName.Exist(col.Name.L) {
				continue
			}
			for _, re := range tableSchemaRegexp {
				if !re.DoMatch(schema.Name.L) {
					continue ForColumnsTag
				}
			}
			for _, re := range tableNameRegexp {
				if !re.DoMatch(tbl.Name.L) {
					continue ForColumnsTag
				}
			}
			for _, re := range columnsRegexp {
				if !re.DoMatch(col.Name.L) {
					continue ForColumnsTag
				}
			}
		}

		var charMaxLen, charOctLen, numericPrecision, numericScale, datetimePrecision interface{}
		colLen, decimal := ft.GetFlen(), ft.GetDecimal()
		defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(ft.GetType())
		if decimal == types.UnspecifiedLength {
			decimal = defaultDecimal
		}
		if colLen == types.UnspecifiedLength {
			colLen = defaultFlen
		}
		if ft.GetType() == mysql.TypeSet {
			// Example: In MySQL set('a','bc','def','ghij') has length 13, because
			// len('a')+len('bc')+len('def')+len('ghij')+len(ThreeComma)=13
			// Reference link: https://bugs.mysql.com/bug.php?id=22613
			colLen = 0
			for _, ele := range ft.GetElems() {
				colLen += len(ele)
			}
			if len(ft.GetElems()) != 0 {
				colLen += (len(ft.GetElems()) - 1)
			}
			charMaxLen = colLen
			charOctLen = calcCharOctLength(colLen, ft.GetCharset())
		} else if ft.GetType() == mysql.TypeEnum {
			// Example: In MySQL enum('a', 'ab', 'cdef') has length 4, because
			// the longest string in the enum is 'cdef'
			// Reference link: https://bugs.mysql.com/bug.php?id=22613
			colLen = 0
			for _, ele := range ft.GetElems() {
				if len(ele) > colLen {
					colLen = len(ele)
				}
			}
			charMaxLen = colLen
			charOctLen = calcCharOctLength(colLen, ft.GetCharset())
		} else if types.IsString(ft.GetType()) {
			charMaxLen = colLen
			charOctLen = calcCharOctLength(colLen, ft.GetCharset())
		} else if types.IsTypeFractionable(ft.GetType()) {
			datetimePrecision = decimal
		} else if types.IsTypeNumeric(ft.GetType()) {
			numericPrecision = colLen
			if ft.GetType() != mysql.TypeFloat && ft.GetType() != mysql.TypeDouble {
				numericScale = decimal
			} else if decimal != -1 {
				numericScale = decimal
			}
		}
		columnType := ft.InfoSchemaStr()
		columnDesc := table.NewColDesc(table.ToColumn(col))
		var columnDefault interface{}
		if columnDesc.DefaultValue != nil {
			columnDefault = fmt.Sprintf("%v", columnDesc.DefaultValue)
			if ft.GetType() == mysql.TypeBit {
				defaultStr := fmt.Sprintf("%v", columnDesc.DefaultValue)
				defaultValBinaryLiteral := types.BinaryLiteral(defaultStr)
				columnDefault = defaultValBinaryLiteral.ToBitLiteralString(true)
			}
		}
		record := types.MakeDatums(
			infoschema.CatalogVal, // TABLE_CATALOG
			schema.Name.O,         // TABLE_SCHEMA
			tbl.Name.O,            // TABLE_NAME
			col.Name.O,            // COLUMN_NAME
			i+1,                   // ORIGINAL_POSITION
			columnDefault,         // COLUMN_DEFAULT
			columnDesc.Null,       // IS_NULLABLE
			types.TypeToStr(ft.GetType(), ft.GetCharset()), // DATA_TYPE
			charMaxLen,           // CHARACTER_MAXIMUM_LENGTH
			charOctLen,           // CHARACTER_OCTET_LENGTH
			numericPrecision,     // NUMERIC_PRECISION
			numericScale,         // NUMERIC_SCALE
			datetimePrecision,    // DATETIME_PRECISION
			columnDesc.Charset,   // CHARACTER_SET_NAME
			columnDesc.Collation, // COLLATION_NAME
			columnType,           // COLUMN_TYPE
			columnDesc.Key,       // COLUMN_KEY
			columnDesc.Extra,     // EXTRA
			strings.ToLower(privileges.PrivToString(priv, mysql.AllColumnPrivs, mysql.Priv2Str)), // PRIVILEGES
			columnDesc.Comment,      // COLUMN_COMMENT
			col.GeneratedExprString, // GENERATION_EXPRESSION
		)
		e.rows = append(e.rows, record)
	}
}

func calcCharOctLength(lenInChar int, cs string) int {
	lenInBytes := lenInChar
	if desc, err := charset.GetCharsetInfo(cs); err == nil {
		lenInBytes = desc.Maxlen * lenInChar
	}
	return lenInBytes
}

func (e *memtableRetriever) setDataFromPartitions(ctx context.Context, sctx sessionctx.Context, schemas []*model.DBInfo) error {
	tableRowsMap, colLengthMap, err := tableStatsCache.get(ctx, sctx)
	if err != nil {
		return err
	}
	checker := privilege.GetPrivilegeManager(sctx)
	var rows [][]types.Datum
	createTimeTp := mysql.TypeDatetime
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.SelectPriv) {
				continue
			}
			createTime := types.NewTime(types.FromGoTime(table.GetUpdateTime()), createTimeTp, types.DefaultFsp)

			var rowCount, dataLength, indexLength uint64
			if table.GetPartitionInfo() == nil {
				rowCount = tableRowsMap[table.ID]
				dataLength, indexLength = getDataAndIndexLength(table, table.ID, rowCount, colLengthMap)
				avgRowLength := uint64(0)
				if rowCount != 0 {
					avgRowLength = dataLength / rowCount
				}
				record := types.MakeDatums(
					infoschema.CatalogVal, // TABLE_CATALOG
					schema.Name.O,         // TABLE_SCHEMA
					table.Name.O,          // TABLE_NAME
					nil,                   // PARTITION_NAME
					nil,                   // SUBPARTITION_NAME
					nil,                   // PARTITION_ORDINAL_POSITION
					nil,                   // SUBPARTITION_ORDINAL_POSITION
					nil,                   // PARTITION_METHOD
					nil,                   // SUBPARTITION_METHOD
					nil,                   // PARTITION_EXPRESSION
					nil,                   // SUBPARTITION_EXPRESSION
					nil,                   // PARTITION_DESCRIPTION
					rowCount,              // TABLE_ROWS
					avgRowLength,          // AVG_ROW_LENGTH
					dataLength,            // DATA_LENGTH
					nil,                   // MAX_DATA_LENGTH
					indexLength,           // INDEX_LENGTH
					nil,                   // DATA_FREE
					createTime,            // CREATE_TIME
					nil,                   // UPDATE_TIME
					nil,                   // CHECK_TIME
					nil,                   // CHECKSUM
					nil,                   // PARTITION_COMMENT
					nil,                   // NODEGROUP
					nil,                   // TABLESPACE_NAME
					nil,                   // TIDB_PARTITION_ID
					nil,                   // TIDB_PLACEMENT_POLICY_NAME
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
						partitionDesc = strings.Join(pi.LessThan, ",")
					} else if table.Partition.Type == model.PartitionTypeList {
						if len(pi.InValues) > 0 {
							buf := bytes.NewBuffer(nil)
							if len(pi.InValues[0]) == 1 {
								for i, vs := range pi.InValues {
									if i > 0 {
										buf.WriteString(",")
									}
									buf.WriteString(vs[0])
								}
							} else if len(pi.InValues[0]) > 1 {
								for i, vs := range pi.InValues {
									if i > 0 {
										buf.WriteString(",")
									}
									buf.WriteString("(")
									buf.WriteString(strings.Join(vs, ","))
									buf.WriteString(")")
								}
							}
							partitionDesc = buf.String()
						}
					}

					partitionMethod := table.Partition.Type.String()
					partitionExpr := table.Partition.Expr
					if table.Partition.Type == model.PartitionTypeRange && len(table.Partition.Columns) > 0 {
						partitionMethod = "RANGE COLUMNS"
						partitionExpr = table.Partition.Columns[0].String()
					} else if table.Partition.Type == model.PartitionTypeList && len(table.Partition.Columns) > 0 {
						partitionMethod = "LIST COLUMNS"
						buf := bytes.NewBuffer(nil)
						for i, col := range table.Partition.Columns {
							if i > 0 {
								buf.WriteString(",")
							}
							buf.WriteString(col.String())
						}
						partitionExpr = buf.String()
					}

					var policyName interface{}
					if pi.PlacementPolicyRef != nil {
						policyName = pi.PlacementPolicyRef.Name.O
					}
					record := types.MakeDatums(
						infoschema.CatalogVal, // TABLE_CATALOG
						schema.Name.O,         // TABLE_SCHEMA
						table.Name.O,          // TABLE_NAME
						pi.Name.O,             // PARTITION_NAME
						nil,                   // SUBPARTITION_NAME
						i+1,                   // PARTITION_ORDINAL_POSITION
						nil,                   // SUBPARTITION_ORDINAL_POSITION
						partitionMethod,       // PARTITION_METHOD
						nil,                   // SUBPARTITION_METHOD
						partitionExpr,         // PARTITION_EXPRESSION
						nil,                   // SUBPARTITION_EXPRESSION
						partitionDesc,         // PARTITION_DESCRIPTION
						rowCount,              // TABLE_ROWS
						avgRowLength,          // AVG_ROW_LENGTH
						dataLength,            // DATA_LENGTH
						uint64(0),             // MAX_DATA_LENGTH
						indexLength,           // INDEX_LENGTH
						uint64(0),             // DATA_FREE
						createTime,            // CREATE_TIME
						nil,                   // UPDATE_TIME
						nil,                   // CHECK_TIME
						nil,                   // CHECKSUM
						pi.Comment,            // PARTITION_COMMENT
						nil,                   // NODEGROUP
						nil,                   // TABLESPACE_NAME
						pi.ID,                 // TIDB_PARTITION_ID
						policyName,            // TIDB_PLACEMENT_POLICY_NAME
					)
					rows = append(rows, record)
				}
			}
		}
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromIndexes(ctx sessionctx.Context, schemas []*model.DBInfo) {
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
					if mysql.HasPriKeyFlag(col.GetFlag()) {
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
					nil,           // Expression
					0,             // INDEX_ID
					"YES",         // IS_VISIBLE
					"YES",         // CLUSTERED
				)
				rows = append(rows, record)
			}
			for _, idxInfo := range tb.Indices {
				if idxInfo.State != model.StatePublic {
					continue
				}
				isClustered := "NO"
				if tb.IsCommonHandle && idxInfo.Primary {
					isClustered = "YES"
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
					colName := col.Name.O
					var expression interface{}
					expression = nil
					tblCol := tb.Columns[col.Offset]
					if tblCol.Hidden {
						colName = "NULL"
						expression = tblCol.GeneratedExprString
					}
					visible := "YES"
					if idxInfo.Invisible {
						visible = "NO"
					}
					record := types.MakeDatums(
						schema.Name.O,   // TABLE_SCHEMA
						tb.Name.O,       // TABLE_NAME
						nonUniq,         // NON_UNIQUE
						idxInfo.Name.O,  // KEY_NAME
						i+1,             // SEQ_IN_INDEX
						colName,         // COLUMN_NAME
						subPart,         // SUB_PART
						idxInfo.Comment, // INDEX_COMMENT
						expression,      // Expression
						idxInfo.ID,      // INDEX_ID
						visible,         // IS_VISIBLE
						isClustered,     // CLUSTERED
					)
					rows = append(rows, record)
				}
			}
		}
	}
	e.rows = rows
}

func (e *memtableRetriever) setDataFromViews(ctx sessionctx.Context, schemas []*model.DBInfo) {
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
				infoschema.CatalogVal,           // TABLE_CATALOG
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
	e.rows = rows
}

func (e *memtableRetriever) dataForTiKVStoreStatus(ctx sessionctx.Context) (err error) {
	tikvStore, ok := ctx.GetStore().(helper.Storage)
	if !ok {
		return errors.New("Information about TiKV store status can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	storesStat, err := tikvHelper.GetStoresStat()
	if err != nil {
		return err
	}
	for _, storeStat := range storesStat.Stores {
		row := make([]types.Datum, len(infoschema.TableTiKVStoreStatusCols))
		row[0].SetInt64(storeStat.Store.ID)
		row[1].SetString(storeStat.Store.Address, mysql.DefaultCollationName)
		row[2].SetInt64(storeStat.Store.State)
		row[3].SetString(storeStat.Store.StateName, mysql.DefaultCollationName)
		data, err := json.Marshal(storeStat.Store.Labels)
		if err != nil {
			return err
		}
		bj := binaryJson.BinaryJSON{}
		if err = bj.UnmarshalJSON(data); err != nil {
			return err
		}
		row[4].SetMysqlJSON(bj)
		row[5].SetString(storeStat.Store.Version, mysql.DefaultCollationName)
		row[6].SetString(storeStat.Status.Capacity, mysql.DefaultCollationName)
		row[7].SetString(storeStat.Status.Available, mysql.DefaultCollationName)
		row[8].SetInt64(storeStat.Status.LeaderCount)
		row[9].SetFloat64(storeStat.Status.LeaderWeight)
		row[10].SetFloat64(storeStat.Status.LeaderScore)
		row[11].SetInt64(storeStat.Status.LeaderSize)
		row[12].SetInt64(storeStat.Status.RegionCount)
		row[13].SetFloat64(storeStat.Status.RegionWeight)
		row[14].SetFloat64(storeStat.Status.RegionScore)
		row[15].SetInt64(storeStat.Status.RegionSize)
		startTs := types.NewTime(types.FromGoTime(storeStat.Status.StartTs), mysql.TypeDatetime, types.DefaultFsp)
		row[16].SetMysqlTime(startTs)
		lastHeartbeatTs := types.NewTime(types.FromGoTime(storeStat.Status.LastHeartbeatTs), mysql.TypeDatetime, types.DefaultFsp)
		row[17].SetMysqlTime(lastHeartbeatTs)
		row[18].SetString(storeStat.Status.Uptime, mysql.DefaultCollationName)
		if sem.IsEnabled() {
			// Patch out IP addresses etc if the user does not have the RESTRICTED_TABLES_ADMIN privilege
			checker := privilege.GetPrivilegeManager(ctx)
			if checker == nil || !checker.RequestDynamicVerification(ctx.GetSessionVars().ActiveRoles, "RESTRICTED_TABLES_ADMIN", false) {
				row[1].SetString(strconv.FormatInt(storeStat.Store.ID, 10), mysql.DefaultCollationName)
				row[1].SetNull()
				row[6].SetNull()
				row[7].SetNull()
				row[16].SetNull()
				row[18].SetNull()
			}
		}
		e.rows = append(e.rows, row)
	}
	return nil
}

// DDLJobsReaderExec executes DDLJobs information retrieving.
type DDLJobsReaderExec struct {
	baseExecutor
	DDLJobRetriever

	cacheJobs []*model.Job
	is        infoschema.InfoSchema
}

// Open implements the Executor Next interface.
func (e *DDLJobsReaderExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	e.DDLJobRetriever.is = e.is
	e.activeRoles = e.ctx.GetSessionVars().ActiveRoles
	err = e.DDLJobRetriever.initial(txn)
	if err != nil {
		return err
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *DDLJobsReaderExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	checker := privilege.GetPrivilegeManager(e.ctx)
	count := 0

	// Append running DDL jobs.
	if e.cursor < len(e.runningJobs) {
		num := mathutil.Min(req.Capacity(), len(e.runningJobs)-e.cursor)
		for i := e.cursor; i < e.cursor+num; i++ {
			e.appendJobToChunk(req, e.runningJobs[i], checker)
			req.AppendString(12, e.runningJobs[i].Query)
		}
		e.cursor += num
		count += num
	}
	var err error

	// Append history DDL jobs.
	if count < req.Capacity() {
		e.cacheJobs, err = e.historyJobIter.GetLastJobs(req.Capacity()-count, e.cacheJobs)
		if err != nil {
			return err
		}
		for _, job := range e.cacheJobs {
			e.appendJobToChunk(req, job, checker)
			req.AppendString(12, job.Query)
		}
		e.cursor += len(e.cacheJobs)
	}
	return nil
}

func (e *memtableRetriever) setDataFromEngines() {
	var rows [][]types.Datum
	rows = append(rows,
		types.MakeDatums(
			"InnoDB",  // Engine
			"DEFAULT", // Support
			"Supports transactions, row-level locking, and foreign keys", // Comment
			"YES", // Transactions
			"YES", // XA
			"YES", // Savepoints
		),
	)
	e.rows = rows
}

func (e *memtableRetriever) setDataFromCharacterSets() {
	charsets := charset.GetSupportedCharsets()
	var rows = make([][]types.Datum, 0, len(charsets))
	for _, charset := range charsets {
		rows = append(rows,
			types.MakeDatums(charset.Name, charset.DefaultCollation, charset.Desc, charset.Maxlen),
		)
	}
	e.rows = rows
}

func (e *memtableRetriever) setDataFromCollations() {
	collations := collate.GetSupportedCollations()
	var rows = make([][]types.Datum, 0, len(collations))
	for _, collation := range collations {
		isDefault := ""
		if collation.IsDefault {
			isDefault = "Yes"
		}
		rows = append(rows,
			types.MakeDatums(collation.Name, collation.CharsetName, collation.ID, isDefault, "Yes", 1),
		)
	}
	e.rows = rows
}

func (e *memtableRetriever) dataForCollationCharacterSetApplicability() {
	collations := collate.GetSupportedCollations()
	var rows = make([][]types.Datum, 0, len(collations))
	for _, collation := range collations {
		rows = append(rows,
			types.MakeDatums(collation.Name, collation.CharsetName),
		)
	}
	e.rows = rows
}

func (e *memtableRetriever) dataForTiDBClusterInfo(ctx sessionctx.Context) error {
	servers, err := infoschema.GetClusterServerInfo(ctx)
	if err != nil {
		e.rows = nil
		return err
	}
	rows := make([][]types.Datum, 0, len(servers))
	for _, server := range servers {
		startTimeStr := ""
		upTimeStr := ""
		if server.StartTimestamp > 0 {
			startTime := time.Unix(server.StartTimestamp, 0)
			startTimeStr = startTime.Format(time.RFC3339)
			upTimeStr = time.Since(startTime).String()
		}
		row := types.MakeDatums(
			server.ServerType,
			server.Address,
			server.StatusAddr,
			server.Version,
			server.GitHash,
			startTimeStr,
			upTimeStr,
			server.ServerID,
		)
		if sem.IsEnabled() {
			checker := privilege.GetPrivilegeManager(ctx)
			if checker == nil || !checker.RequestDynamicVerification(ctx.GetSessionVars().ActiveRoles, "RESTRICTED_TABLES_ADMIN", false) {
				row[1].SetString(strconv.FormatUint(server.ServerID, 10), mysql.DefaultCollationName)
				row[2].SetNull()
				row[5].SetNull()
				row[6].SetNull()
			}
		}
		rows = append(rows, row)
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromKeyColumnUsage(ctx sessionctx.Context, schemas []*model.DBInfo) {
	checker := privilege.GetPrivilegeManager(ctx)
	rows := make([][]types.Datum, 0, len(schemas)) // The capacity is not accurate, but it is not a big problem.
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}
			rs := keyColumnUsageInTable(schema, table)
			rows = append(rows, rs...)
		}
	}
	e.rows = rows
}

func (e *memtableRetriever) setDataForClusterProcessList(ctx sessionctx.Context) error {
	e.setDataForProcessList(ctx)
	rows, err := infoschema.AppendHostInfoToRows(ctx, e.rows)
	if err != nil {
		return err
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataForProcessList(ctx sessionctx.Context) {
	sm := ctx.GetSessionManager()
	if sm == nil {
		return
	}

	loginUser := ctx.GetSessionVars().User
	hasProcessPriv := hasPriv(ctx, mysql.ProcessPriv)
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
	e.rows = records
}

func (e *memtableRetriever) setDataFromUserPrivileges(ctx sessionctx.Context) {
	pm := privilege.GetPrivilegeManager(ctx)
	// The results depend on the user querying the information.
	e.rows = pm.UserPrivilegesTable(ctx.GetSessionVars().ActiveRoles, ctx.GetSessionVars().User.Username, ctx.GetSessionVars().User.Hostname)
}

func (e *memtableRetriever) setDataForMetricTables(ctx sessionctx.Context) {
	tables := make([]string, 0, len(infoschema.MetricTableMap))
	for name := range infoschema.MetricTableMap {
		tables = append(tables, name)
	}
	sort.Strings(tables)
	rows := make([][]types.Datum, 0, len(tables))
	for _, name := range tables {
		schema := infoschema.MetricTableMap[name]
		record := types.MakeDatums(
			name,                             // METRICS_NAME
			schema.PromQL,                    // PROMQL
			strings.Join(schema.Labels, ","), // LABELS
			schema.Quantile,                  // QUANTILE
			schema.Comment,                   // COMMENT
		)
		rows = append(rows, record)
	}
	e.rows = rows
}

func keyColumnUsageInTable(schema *model.DBInfo, table *model.TableInfo) [][]types.Datum {
	var rows [][]types.Datum
	if table.PKIsHandle {
		for _, col := range table.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				record := types.MakeDatums(
					infoschema.CatalogVal,        // CONSTRAINT_CATALOG
					schema.Name.O,                // CONSTRAINT_SCHEMA
					infoschema.PrimaryConstraint, // CONSTRAINT_NAME
					infoschema.CatalogVal,        // TABLE_CATALOG
					schema.Name.O,                // TABLE_SCHEMA
					table.Name.O,                 // TABLE_NAME
					col.Name.O,                   // COLUMN_NAME
					1,                            // ORDINAL_POSITION
					1,                            // POSITION_IN_UNIQUE_CONSTRAINT
					nil,                          // REFERENCED_TABLE_SCHEMA
					nil,                          // REFERENCED_TABLE_NAME
					nil,                          // REFERENCED_COLUMN_NAME
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
			idxName = infoschema.PrimaryConstraint
		} else if index.Unique {
			idxName = index.Name.O
		} else {
			// Only handle unique/primary key
			continue
		}
		for i, key := range index.Columns {
			col := nameToCol[key.Name.L]
			if col.Hidden {
				continue
			}
			record := types.MakeDatums(
				infoschema.CatalogVal, // CONSTRAINT_CATALOG
				schema.Name.O,         // CONSTRAINT_SCHEMA
				idxName,               // CONSTRAINT_NAME
				infoschema.CatalogVal, // TABLE_CATALOG
				schema.Name.O,         // TABLE_SCHEMA
				table.Name.O,          // TABLE_NAME
				col.Name.O,            // COLUMN_NAME
				i+1,                   // ORDINAL_POSITION,
				nil,                   // POSITION_IN_UNIQUE_CONSTRAINT
				nil,                   // REFERENCED_TABLE_SCHEMA
				nil,                   // REFERENCED_TABLE_NAME
				nil,                   // REFERENCED_COLUMN_NAME
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
				infoschema.CatalogVal, // CONSTRAINT_CATALOG
				schema.Name.O,         // CONSTRAINT_SCHEMA
				fk.Name.O,             // CONSTRAINT_NAME
				infoschema.CatalogVal, // TABLE_CATALOG
				schema.Name.O,         // TABLE_SCHEMA
				table.Name.O,          // TABLE_NAME
				col.Name.O,            // COLUMN_NAME
				i+1,                   // ORDINAL_POSITION,
				1,                     // POSITION_IN_UNIQUE_CONSTRAINT
				schema.Name.O,         // REFERENCED_TABLE_SCHEMA
				fk.RefTable.O,         // REFERENCED_TABLE_NAME
				fkRefCol,              // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
		}
	}
	return rows
}

func (e *memtableRetriever) setDataForTiKVRegionStatus(ctx sessionctx.Context) (err error) {
	tikvStore, ok := ctx.GetStore().(helper.Storage)
	if !ok {
		return errors.New("Information about TiKV region status can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	requestByTableRange := false
	allRegionsInfo := helper.NewRegionsInfo()
	if e.extractor != nil {
		extractor, ok := e.extractor.(*plannercore.TiKVRegionStatusExtractor)
		if ok && len(extractor.GetTablesID()) > 0 {
			for _, tableID := range extractor.GetTablesID() {
				regionsInfo, err := e.getRegionsInfoForSingleTable(tikvHelper, tableID)
				if err != nil {
					return err
				}
				allRegionsInfo = allRegionsInfo.Merge(regionsInfo)
			}
			requestByTableRange = true
		}
	}
	if !requestByTableRange {
		allRegionsInfo, err = tikvHelper.GetRegionsInfo()
		if err != nil {
			return err
		}
	}
	allSchemas := ctx.GetInfoSchema().(infoschema.InfoSchema).AllSchemas()
	tableInfos := tikvHelper.GetRegionsTableInfo(allRegionsInfo, allSchemas)
	for i := range allRegionsInfo.Regions {
		tableList := tableInfos[allRegionsInfo.Regions[i].ID]
		if len(tableList) == 0 {
			e.setNewTiKVRegionStatusCol(&allRegionsInfo.Regions[i], nil)
		}
		for j := range tableList {
			e.setNewTiKVRegionStatusCol(&allRegionsInfo.Regions[i], &tableList[j])
		}
	}
	return nil
}

func (e *memtableRetriever) getRegionsInfoForSingleTable(helper *helper.Helper, tableID int64) (*helper.RegionsInfo, error) {
	sk, ek := tablecodec.GetTableHandleKeyRange(tableID)
	sRegion, err := helper.GetRegionByKey(codec.EncodeBytes(nil, sk))
	if err != nil {
		return nil, err
	}
	eRegion, err := helper.GetRegionByKey(codec.EncodeBytes(nil, ek))
	if err != nil {
		return nil, err
	}
	sk, err = hex.DecodeString(sRegion.StartKey)
	if err != nil {
		return nil, err
	}
	ek, err = hex.DecodeString(eRegion.EndKey)
	if err != nil {
		return nil, err
	}
	return helper.GetRegionsInfoByRange(sk, ek)
}

func (e *memtableRetriever) setNewTiKVRegionStatusCol(region *helper.RegionInfo, table *helper.TableInfo) {
	row := make([]types.Datum, len(infoschema.TableTiKVRegionStatusCols))
	row[0].SetInt64(region.ID)
	row[1].SetString(region.StartKey, mysql.DefaultCollationName)
	row[2].SetString(region.EndKey, mysql.DefaultCollationName)
	if table != nil {
		row[3].SetInt64(table.Table.ID)
		row[4].SetString(table.DB.Name.O, mysql.DefaultCollationName)
		row[5].SetString(table.Table.Name.O, mysql.DefaultCollationName)
		if table.IsIndex {
			row[6].SetInt64(1)
			row[7].SetInt64(table.Index.ID)
			row[8].SetString(table.Index.Name.O, mysql.DefaultCollationName)
		} else {
			row[6].SetInt64(0)
		}
	} else {
		row[6].SetInt64(0)
	}
	row[9].SetInt64(region.Epoch.ConfVer)
	row[10].SetInt64(region.Epoch.Version)
	row[11].SetUint64(region.WrittenBytes)
	row[12].SetUint64(region.ReadBytes)
	row[13].SetInt64(region.ApproximateSize)
	row[14].SetInt64(region.ApproximateKeys)
	if region.ReplicationStatus != nil {
		row[15].SetString(region.ReplicationStatus.State, mysql.DefaultCollationName)
		row[16].SetInt64(region.ReplicationStatus.StateID)
	}
	e.rows = append(e.rows, row)
}

const (
	normalPeer  = "NORMAL"
	pendingPeer = "PENDING"
	downPeer    = "DOWN"
)

func (e *memtableRetriever) setDataForTiDBHotRegions(ctx sessionctx.Context) error {
	tikvStore, ok := ctx.GetStore().(helper.Storage)
	if !ok {
		return errors.New("Information about hot region can be gotten only when the storage is TiKV")
	}
	allSchemas := ctx.GetInfoSchema().(infoschema.InfoSchema).AllSchemas()
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	metrics, err := tikvHelper.ScrapeHotInfo(pdapi.HotRead, allSchemas)
	if err != nil {
		return err
	}
	e.setDataForHotRegionByMetrics(metrics, "read")
	metrics, err = tikvHelper.ScrapeHotInfo(pdapi.HotWrite, allSchemas)
	if err != nil {
		return err
	}
	e.setDataForHotRegionByMetrics(metrics, "write")
	return nil
}

func (e *memtableRetriever) setDataForHotRegionByMetrics(metrics []helper.HotTableIndex, tp string) {
	rows := make([][]types.Datum, 0, len(metrics))
	for _, tblIndex := range metrics {
		row := make([]types.Datum, len(infoschema.TableTiDBHotRegionsCols))
		if tblIndex.IndexName != "" {
			row[1].SetInt64(tblIndex.IndexID)
			row[4].SetString(tblIndex.IndexName, mysql.DefaultCollationName)
		} else {
			row[1].SetNull()
			row[4].SetNull()
		}
		row[0].SetInt64(tblIndex.TableID)
		row[2].SetString(tblIndex.DbName, mysql.DefaultCollationName)
		row[3].SetString(tblIndex.TableName, mysql.DefaultCollationName)
		row[5].SetUint64(tblIndex.RegionID)
		row[6].SetString(tp, mysql.DefaultCollationName)
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
	e.rows = append(e.rows, rows...)
}

// setDataFromTableConstraints constructs data for table information_schema.constraints.See https://dev.mysql.com/doc/refman/5.7/en/table-constraints-table.html
func (e *memtableRetriever) setDataFromTableConstraints(ctx sessionctx.Context, schemas []*model.DBInfo) {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, tbl := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, tbl.Name.L, "", mysql.AllPrivMask) {
				continue
			}

			if tbl.PKIsHandle {
				record := types.MakeDatums(
					infoschema.CatalogVal,     // CONSTRAINT_CATALOG
					schema.Name.O,             // CONSTRAINT_SCHEMA
					mysql.PrimaryKeyName,      // CONSTRAINT_NAME
					schema.Name.O,             // TABLE_SCHEMA
					tbl.Name.O,                // TABLE_NAME
					infoschema.PrimaryKeyType, // CONSTRAINT_TYPE
				)
				rows = append(rows, record)
			}

			for _, idx := range tbl.Indices {
				var cname, ctype string
				if idx.Primary {
					cname = mysql.PrimaryKeyName
					ctype = infoschema.PrimaryKeyType
				} else if idx.Unique {
					cname = idx.Name.O
					ctype = infoschema.UniqueKeyType
				} else {
					// The index has no constriant.
					continue
				}
				record := types.MakeDatums(
					infoschema.CatalogVal, // CONSTRAINT_CATALOG
					schema.Name.O,         // CONSTRAINT_SCHEMA
					cname,                 // CONSTRAINT_NAME
					schema.Name.O,         // TABLE_SCHEMA
					tbl.Name.O,            // TABLE_NAME
					ctype,                 // CONSTRAINT_TYPE
				)
				rows = append(rows, record)
			}
			//  TiDB includes foreign key information for compatibility but foreign keys are not yet enforced.
			for _, fk := range tbl.ForeignKeys {
				record := types.MakeDatums(
					infoschema.CatalogVal,     // CONSTRAINT_CATALOG
					schema.Name.O,             // CONSTRAINT_SCHEMA
					fk.Name.O,                 // CONSTRAINT_NAME
					schema.Name.O,             // TABLE_SCHEMA
					tbl.Name.O,                // TABLE_NAME
					infoschema.ForeignKeyType, // CONSTRAINT_TYPE
				)
				rows = append(rows, record)
			}
		}
	}
	e.rows = rows
}

// tableStorageStatsRetriever is used to read slow log data.
type tableStorageStatsRetriever struct {
	dummyCloser
	table         *model.TableInfo
	outputCols    []*model.ColumnInfo
	retrieved     bool
	initialized   bool
	extractor     *plannercore.TableStorageStatsExtractor
	initialTables []*initialTable
	curTable      int
	helper        *helper.Helper
	stats         helper.PDRegionStats
}

func (e *tableStorageStatsRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved {
		return nil, nil
	}
	if !e.initialized {
		err := e.initialize(sctx)
		if err != nil {
			return nil, err
		}
	}
	if len(e.initialTables) == 0 || e.curTable >= len(e.initialTables) {
		e.retrieved = true
		return nil, nil
	}

	rows, err := e.setDataForTableStorageStats(sctx)
	if err != nil {
		return nil, err
	}
	if len(e.outputCols) == len(e.table.Columns) {
		return rows, nil
	}
	retRows := make([][]types.Datum, len(rows))
	for i, fullRow := range rows {
		row := make([]types.Datum, len(e.outputCols))
		for j, col := range e.outputCols {
			row[j] = fullRow[col.Offset]
		}
		retRows[i] = row
	}
	return retRows, nil
}

type initialTable struct {
	db string
	*model.TableInfo
}

func (e *tableStorageStatsRetriever) initialize(sctx sessionctx.Context) error {
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	var databases []string
	schemas := e.extractor.TableSchema
	tables := e.extractor.TableName

	// If not specify the table_schema, return an error to avoid traverse all schemas and their tables.
	if len(schemas) == 0 {
		return errors.Errorf("Please specify the 'table_schema'")
	}

	// Filter the sys or memory schema.
	for schema := range schemas {
		if !util.IsMemDB(schema) {
			databases = append(databases, schema)
		}
	}

	// Privilege checker.
	checker := func(db, table string) bool {
		if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
			return pm.RequestVerification(sctx.GetSessionVars().ActiveRoles, db, table, "", mysql.AllPrivMask)
		}
		return true
	}

	// Extract the tables to the initialTable.
	for _, DB := range databases {
		// The user didn't specified the table, extract all tables of this db to initialTable.
		if len(tables) == 0 {
			tbs := is.SchemaTables(model.NewCIStr(DB))
			for _, tb := range tbs {
				// For every db.table, check it's privileges.
				if checker(DB, tb.Meta().Name.L) {
					e.initialTables = append(e.initialTables, &initialTable{DB, tb.Meta()})
				}
			}
		} else {
			// The user specified the table, extract the specified tables of this db to initialTable.
			for tb := range tables {
				if tb, err := is.TableByName(model.NewCIStr(DB), model.NewCIStr(tb)); err == nil {
					// For every db.table, check it's privileges.
					if checker(DB, tb.Meta().Name.L) {
						e.initialTables = append(e.initialTables, &initialTable{DB, tb.Meta()})
					}
				}
			}
		}
	}

	// Cache the helper and return an error if PD unavailable.
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		return errors.Errorf("Information about TiKV region status can be gotten only when the storage is TiKV")
	}
	e.helper = helper.NewHelper(tikvStore)
	_, err := e.helper.GetPDAddr()
	if err != nil {
		return err
	}
	e.initialized = true
	return nil
}

func (e *tableStorageStatsRetriever) setDataForTableStorageStats(ctx sessionctx.Context) ([][]types.Datum, error) {
	rows := make([][]types.Datum, 0, 1024)
	count := 0
	for e.curTable < len(e.initialTables) && count < 1024 {
		table := e.initialTables[e.curTable]
		tableID := table.ID
		err := e.helper.GetPDRegionStats(tableID, &e.stats, false)
		if err != nil {
			return nil, err
		}
		peerCount := len(e.stats.StorePeerCount)

		record := types.MakeDatums(
			table.db,            // TABLE_SCHEMA
			table.Name.O,        // TABLE_NAME
			tableID,             // TABLE_ID
			peerCount,           // TABLE_PEER_COUNT
			e.stats.Count,       // TABLE_REGION_COUNT
			e.stats.EmptyCount,  // TABLE_EMPTY_REGION_COUNT
			e.stats.StorageSize, // TABLE_SIZE
			e.stats.StorageKeys, // TABLE_KEYS
		)
		rows = append(rows, record)
		count++
		e.curTable++
	}
	return rows, nil
}

func (e *memtableRetriever) setDataFromSessionVar(ctx sessionctx.Context) error {
	var err error
	sessionVars := ctx.GetSessionVars()
	sysVars := variable.GetSysVars()
	rows := make([][]types.Datum, 0, len(sysVars))
	for _, v := range sysVars {
		var value string
		value, err = variable.GetSessionOrGlobalSystemVar(sessionVars, v.Name)
		if err != nil {
			return err
		}
		row := types.MakeDatums(v.Name, value)
		rows = append(rows, row)
	}
	e.rows = rows
	return nil
}

// dataForAnalyzeStatusHelper is a helper function which can be used in show_stats.go
func dataForAnalyzeStatusHelper(sctx sessionctx.Context) (rows [][]types.Datum, err error) {
	const maxAnalyzeJobs = 30
	const sql = "SELECT table_schema, table_name, partition_name, job_info, processed_rows, CONVERT_TZ(start_time, @@TIME_ZONE, '+00:00'), CONVERT_TZ(end_time, @@TIME_ZONE, '+00:00'), state, fail_reason, instance, process_id FROM mysql.analyze_jobs ORDER BY update_time DESC LIMIT %?"
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	chunkRows, _, err := exec.ExecRestrictedSQL(context.TODO(), nil, sql, maxAnalyzeJobs)
	if err != nil {
		return nil, err
	}
	checker := privilege.GetPrivilegeManager(sctx)
	for _, chunkRow := range chunkRows {
		dbName := chunkRow.GetString(0)
		tableName := chunkRow.GetString(1)
		if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, dbName, tableName, "", mysql.AllPrivMask) {
			continue
		}
		partitionName := chunkRow.GetString(2)
		jobInfo := chunkRow.GetString(3)
		processedRows := chunkRow.GetInt64(4)
		var startTime, endTime interface{}
		if !chunkRow.IsNull(5) {
			t, err := chunkRow.GetTime(5).GoTime(time.UTC)
			if err != nil {
				return nil, err
			}
			startTime = types.NewTime(types.FromGoTime(t.In(sctx.GetSessionVars().TimeZone)), mysql.TypeDatetime, 0)
		}
		if !chunkRow.IsNull(6) {
			t, err := chunkRow.GetTime(6).GoTime(time.UTC)
			if err != nil {
				return nil, err
			}
			endTime = types.NewTime(types.FromGoTime(t.In(sctx.GetSessionVars().TimeZone)), mysql.TypeDatetime, 0)
		}
		state := chunkRow.GetEnum(7).String()
		var failReason interface{}
		if !chunkRow.IsNull(8) {
			failReason = chunkRow.GetString(8)
		}
		instance := chunkRow.GetString(9)
		var procID interface{}
		if !chunkRow.IsNull(10) {
			procID = chunkRow.GetUint64(10)
		}
		rows = append(rows, types.MakeDatums(
			dbName,        // TABLE_SCHEMA
			tableName,     // TABLE_NAME
			partitionName, // PARTITION_NAME
			jobInfo,       // JOB_INFO
			processedRows, // ROW_COUNT
			startTime,     // START_TIME
			endTime,       // END_TIME
			state,         // STATE
			failReason,    // FAIL_REASON
			instance,      // INSTANCE
			procID,        // PROCESS_ID
		))
	}
	return
}

// setDataForAnalyzeStatus gets all the analyze jobs.
func (e *memtableRetriever) setDataForAnalyzeStatus(sctx sessionctx.Context) (err error) {
	e.rows, err = dataForAnalyzeStatusHelper(sctx)
	return
}

// setDataForPseudoProfiling returns pseudo data for table profiling when system variable `profiling` is set to `ON`.
func (e *memtableRetriever) setDataForPseudoProfiling(sctx sessionctx.Context) {
	if v, ok := sctx.GetSessionVars().GetSystemVar("profiling"); ok && variable.TiDBOptOn(v) {
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
		e.rows = append(e.rows, row)
	}
}

func (e *memtableRetriever) setDataForServersInfo(ctx sessionctx.Context) error {
	serversInfo, err := infosync.GetAllServerInfo(context.Background())
	if err != nil {
		return err
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
			stringutil.BuildStringFromLabels(info.Labels), // LABELS
		)
		if sem.IsEnabled() {
			checker := privilege.GetPrivilegeManager(ctx)
			if checker == nil || !checker.RequestDynamicVerification(ctx.GetSessionVars().ActiveRoles, "RESTRICTED_TABLES_ADMIN", false) {
				row[1].SetNull() // clear IP
			}
		}
		rows = append(rows, row)
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromSequences(ctx sessionctx.Context, schemas []*model.DBInfo) {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if !table.IsSequence() {
				continue
			}
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}
			record := types.MakeDatums(
				infoschema.CatalogVal,     // TABLE_CATALOG
				schema.Name.O,             // TABLE_SCHEMA
				table.Name.O,              // TABLE_NAME
				table.Sequence.Cache,      // Cache
				table.Sequence.CacheValue, // CACHE_VALUE
				table.Sequence.Cycle,      // CYCLE
				table.Sequence.Increment,  // INCREMENT
				table.Sequence.MaxValue,   // MAXVALUE
				table.Sequence.MinValue,   // MINVALUE
				table.Sequence.Start,      // START
				table.Sequence.Comment,    // COMMENT
			)
			rows = append(rows, record)
		}
	}
	e.rows = rows
}

// dataForTableTiFlashReplica constructs data for table tiflash replica info.
func (e *memtableRetriever) dataForTableTiFlashReplica(ctx sessionctx.Context, schemas []*model.DBInfo) {
	var rows [][]types.Datum
	progressMap, err := infosync.GetTiFlashTableSyncProgress(context.Background())
	if err != nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(err)
	}
	for _, schema := range schemas {
		for _, tbl := range schema.Tables {
			if tbl.TiFlashReplica == nil {
				continue
			}
			progress := 1.0
			if !tbl.TiFlashReplica.Available {
				if pi := tbl.GetPartitionInfo(); pi != nil && len(pi.Definitions) > 0 {
					progress = 0
					for _, p := range pi.Definitions {
						if tbl.TiFlashReplica.IsPartitionAvailable(p.ID) {
							progress += 1
						} else {
							progress += progressMap[p.ID]
						}
					}
					progress = progress / float64(len(pi.Definitions))
				} else {
					progress = progressMap[tbl.ID]
				}
			}
			record := types.MakeDatums(
				schema.Name.O,                   // TABLE_SCHEMA
				tbl.Name.O,                      // TABLE_NAME
				tbl.ID,                          // TABLE_ID
				int64(tbl.TiFlashReplica.Count), // REPLICA_COUNT
				strings.Join(tbl.TiFlashReplica.LocationLabels, ","), // LOCATION_LABELS
				tbl.TiFlashReplica.Available,                         // AVAILABLE
				progress,                                             // PROGRESS
			)
			rows = append(rows, record)
		}
	}
	e.rows = rows
}

func (e *memtableRetriever) setDataForStatementsSummaryEvicted(ctx sessionctx.Context) error {
	if !hasPriv(ctx, mysql.ProcessPriv) {
		return plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
	}
	e.rows = stmtsummary.StmtSummaryByDigestMap.ToEvictedCountDatum()
	switch e.table.Name.O {
	case infoschema.ClusterTableStatementsSummaryEvicted:
		rows, err := infoschema.AppendHostInfoToRows(ctx, e.rows)
		if err != nil {
			return err
		}
		e.rows = rows
	}
	return nil
}

func (e *memtableRetriever) setDataForClientErrorsSummary(ctx sessionctx.Context, tableName string) error {
	// Seeing client errors should require the PROCESS privilege, with the exception of errors for your own user.
	// This is similar to information_schema.processlist, which is the closest comparison.
	hasProcessPriv := hasPriv(ctx, mysql.ProcessPriv)
	loginUser := ctx.GetSessionVars().User

	var rows [][]types.Datum
	switch tableName {
	case infoschema.TableClientErrorsSummaryGlobal:
		if !hasProcessPriv {
			return plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
		}
		for code, summary := range errno.GlobalStats() {
			firstSeen := types.NewTime(types.FromGoTime(summary.FirstSeen), mysql.TypeTimestamp, types.DefaultFsp)
			lastSeen := types.NewTime(types.FromGoTime(summary.LastSeen), mysql.TypeTimestamp, types.DefaultFsp)
			row := types.MakeDatums(
				int(code),                    // ERROR_NUMBER
				errno.MySQLErrName[code].Raw, // ERROR_MESSAGE
				summary.ErrorCount,           // ERROR_COUNT
				summary.WarningCount,         // WARNING_COUNT
				firstSeen,                    // FIRST_SEEN
				lastSeen,                     // LAST_SEEN
			)
			rows = append(rows, row)
		}
	case infoschema.TableClientErrorsSummaryByUser:
		for user, agg := range errno.UserStats() {
			for code, summary := range agg {
				// Allow anyone to see their own errors.
				if !hasProcessPriv && loginUser != nil && loginUser.Username != user {
					continue
				}
				firstSeen := types.NewTime(types.FromGoTime(summary.FirstSeen), mysql.TypeTimestamp, types.DefaultFsp)
				lastSeen := types.NewTime(types.FromGoTime(summary.LastSeen), mysql.TypeTimestamp, types.DefaultFsp)
				row := types.MakeDatums(
					user,                         // USER
					int(code),                    // ERROR_NUMBER
					errno.MySQLErrName[code].Raw, // ERROR_MESSAGE
					summary.ErrorCount,           // ERROR_COUNT
					summary.WarningCount,         // WARNING_COUNT
					firstSeen,                    // FIRST_SEEN
					lastSeen,                     // LAST_SEEN
				)
				rows = append(rows, row)
			}
		}
	case infoschema.TableClientErrorsSummaryByHost:
		if !hasProcessPriv {
			return plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
		}
		for host, agg := range errno.HostStats() {
			for code, summary := range agg {
				firstSeen := types.NewTime(types.FromGoTime(summary.FirstSeen), mysql.TypeTimestamp, types.DefaultFsp)
				lastSeen := types.NewTime(types.FromGoTime(summary.LastSeen), mysql.TypeTimestamp, types.DefaultFsp)
				row := types.MakeDatums(
					host,                         // HOST
					int(code),                    // ERROR_NUMBER
					errno.MySQLErrName[code].Raw, // ERROR_MESSAGE
					summary.ErrorCount,           // ERROR_COUNT
					summary.WarningCount,         // WARNING_COUNT
					firstSeen,                    // FIRST_SEEN
					lastSeen,                     // LAST_SEEN
				)
				rows = append(rows, row)
			}
		}
	}
	e.rows = rows
	return nil
}

type stmtSummaryTableRetriever struct {
	dummyCloser
	table     *model.TableInfo
	columns   []*model.ColumnInfo
	retrieved bool
	extractor *plannercore.StatementsSummaryExtractor
}

// retrieve implements the infoschemaRetriever interface
func (e *stmtSummaryTableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}
	e.retrieved = true

	var err error
	var instanceAddr string
	switch e.table.Name.O {
	case infoschema.ClusterTableStatementsSummary,
		infoschema.ClusterTableStatementsSummaryHistory:
		instanceAddr, err = infoschema.GetInstanceAddr(sctx)
		if err != nil {
			return nil, err
		}
	}
	user := sctx.GetSessionVars().User
	reader := stmtsummary.NewStmtSummaryReader(user, hasPriv(sctx, mysql.ProcessPriv), e.columns, instanceAddr, sctx.GetSessionVars().StmtCtx.TimeZone)
	if e.extractor.Enable {
		checker := stmtsummary.NewStmtSummaryChecker(e.extractor.Digests)
		reader.SetChecker(checker)
	}
	var rows [][]types.Datum
	switch e.table.Name.O {
	case infoschema.TableStatementsSummary,
		infoschema.ClusterTableStatementsSummary:
		rows = reader.GetStmtSummaryCurrentRows()
	case infoschema.TableStatementsSummaryHistory,
		infoschema.ClusterTableStatementsSummaryHistory:
		rows = reader.GetStmtSummaryHistoryRows()
	}

	return rows, nil
}

// tidbTrxTableRetriever is the memtable retriever for the TIDB_TRX and CLUSTER_TIDB_TRX table.
type tidbTrxTableRetriever struct {
	dummyCloser
	batchRetrieverHelper
	table       *model.TableInfo
	columns     []*model.ColumnInfo
	txnInfo     []*txninfo.TxnInfo
	initialized bool
}

func (e *tidbTrxTableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved {
		return nil, nil
	}

	if !e.initialized {
		e.initialized = true

		sm := sctx.GetSessionManager()
		if sm == nil {
			e.retrieved = true
			return nil, nil
		}

		loginUser := sctx.GetSessionVars().User
		hasProcessPriv := hasPriv(sctx, mysql.ProcessPriv)
		infoList := sm.ShowTxnList()
		e.txnInfo = make([]*txninfo.TxnInfo, 0, len(infoList))
		for _, info := range infoList {
			// If you have the PROCESS privilege, you can see all running transactions.
			// Otherwise, you can see only your own transactions.
			if !hasProcessPriv && loginUser != nil && info.Username != loginUser.Username {
				continue
			}
			e.txnInfo = append(e.txnInfo, info)
		}

		e.batchRetrieverHelper.totalRows = len(e.txnInfo)
		e.batchRetrieverHelper.batchSize = 1024
	}

	// The current TiDB node's address is needed by the CLUSTER_TIDB_TRX table.
	var err error
	var instanceAddr string
	switch e.table.Name.O {
	case infoschema.ClusterTableTiDBTrx:
		instanceAddr, err = infoschema.GetInstanceAddr(sctx)
		if err != nil {
			return nil, err
		}
	}

	var res [][]types.Datum
	err = e.nextBatch(func(start, end int) error {
		// Before getting rows, collect the SQL digests that needs to be retrieved first.
		var sqlRetriever *expression.SQLDigestTextRetriever
		for _, c := range e.columns {
			if c.Name.O == txninfo.CurrentSQLDigestTextStr {
				if sqlRetriever == nil {
					sqlRetriever = expression.NewSQLDigestTextRetriever()
				}

				for i := start; i < end; i++ {
					sqlRetriever.SQLDigestsMap[e.txnInfo[i].CurrentSQLDigest] = ""
				}
			}
		}
		// Retrieve the SQL texts if necessary.
		if sqlRetriever != nil {
			err1 := sqlRetriever.RetrieveLocal(ctx, sctx)
			if err1 != nil {
				return errors.Trace(err1)
			}
		}

		res = make([][]types.Datum, 0, end-start)

		// Calculate rows.
		for i := start; i < end; i++ {
			row := make([]types.Datum, 0, len(e.columns))
			for _, c := range e.columns {
				if c.Name.O == util.ClusterTableInstanceColumnName {
					row = append(row, types.NewDatum(instanceAddr))
				} else if c.Name.O == txninfo.CurrentSQLDigestTextStr {
					if text, ok := sqlRetriever.SQLDigestsMap[e.txnInfo[i].CurrentSQLDigest]; ok && len(text) != 0 {
						row = append(row, types.NewDatum(text))
					} else {
						row = append(row, types.NewDatum(nil))
					}
				} else {
					row = append(row, e.txnInfo[i].ToDatum(c.Name.O))
				}
			}
			res = append(res, row)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

// dataLockWaitsTableRetriever is the memtable retriever for the DATA_LOCK_WAITS table.
type dataLockWaitsTableRetriever struct {
	dummyCloser
	batchRetrieverHelper
	table       *model.TableInfo
	columns     []*model.ColumnInfo
	lockWaits   []*deadlock.WaitForEntry
	initialized bool
}

func (r *dataLockWaitsTableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if r.retrieved {
		return nil, nil
	}

	if !r.initialized {
		if !hasPriv(sctx, mysql.ProcessPriv) {
			return nil, plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
		}

		r.initialized = true
		var err error
		r.lockWaits, err = sctx.GetStore().GetLockWaits()
		if err != nil {
			r.retrieved = true
			return nil, err
		}

		r.batchRetrieverHelper.totalRows = len(r.lockWaits)
		r.batchRetrieverHelper.batchSize = 1024
	}

	var res [][]types.Datum

	err := r.nextBatch(func(start, end int) error {
		// Before getting rows, collect the SQL digests that needs to be retrieved first.
		var needDigest bool
		var needSQLText bool
		for _, c := range r.columns {
			if c.Name.O == infoschema.DataLockWaitsColumnSQLDigestText {
				needSQLText = true
			} else if c.Name.O == infoschema.DataLockWaitsColumnSQLDigest {
				needDigest = true
			}
		}

		var digests []string
		if needDigest || needSQLText {
			digests = make([]string, end-start)
			for i, lockWait := range r.lockWaits {
				digest, err := resourcegrouptag.DecodeResourceGroupTag(lockWait.ResourceGroupTag)
				if err != nil {
					// Ignore the error if failed to decode the digest from resource_group_tag. We still want to show
					// as much information as possible even we can't retrieve some of them.
					logutil.Logger(ctx).Warn("failed to decode resource group tag", zap.Error(err))
				} else {
					digests[i] = hex.EncodeToString(digest)
				}
			}
		}

		// Fetch the SQL Texts of the digests above if necessary.
		var sqlRetriever *expression.SQLDigestTextRetriever
		if needSQLText {
			sqlRetriever = expression.NewSQLDigestTextRetriever()
			for _, digest := range digests {
				if len(digest) > 0 {
					sqlRetriever.SQLDigestsMap[digest] = ""
				}
			}
			err := sqlRetriever.RetrieveGlobal(ctx, sctx)
			if err != nil {
				return errors.Trace(err)
			}
		}

		// Calculate rows.
		res = make([][]types.Datum, 0, end-start)
		for rowIdx, lockWait := range r.lockWaits[start:end] {
			row := make([]types.Datum, 0, len(r.columns))

			for _, col := range r.columns {
				switch col.Name.O {
				case infoschema.DataLockWaitsColumnKey:
					row = append(row, types.NewDatum(strings.ToUpper(hex.EncodeToString(lockWait.Key))))
				case infoschema.DataLockWaitsColumnKeyInfo:
					infoSchema := sctx.GetInfoSchema().(infoschema.InfoSchema)
					var decodedKeyStr interface{} = nil
					decodedKey, err := keydecoder.DecodeKey(lockWait.Key, infoSchema)
					if err == nil {
						decodedKeyBytes, err := json.Marshal(decodedKey)
						if err != nil {
							logutil.BgLogger().Warn("marshal decoded key info to JSON failed", zap.Error(err))
						} else {
							decodedKeyStr = string(decodedKeyBytes)
						}
					} else {
						logutil.BgLogger().Warn("decode key failed", zap.Error(err))
					}
					row = append(row, types.NewDatum(decodedKeyStr))
				case infoschema.DataLockWaitsColumnTrxID:
					row = append(row, types.NewDatum(lockWait.Txn))
				case infoschema.DataLockWaitsColumnCurrentHoldingTrxID:
					row = append(row, types.NewDatum(lockWait.WaitForTxn))
				case infoschema.DataLockWaitsColumnSQLDigest:
					digest := digests[rowIdx]
					if len(digest) == 0 {
						row = append(row, types.NewDatum(nil))
					} else {
						row = append(row, types.NewDatum(digest))
					}
				case infoschema.DataLockWaitsColumnSQLDigestText:
					text := sqlRetriever.SQLDigestsMap[digests[rowIdx]]
					if len(text) > 0 {
						row = append(row, types.NewDatum(text))
					} else {
						row = append(row, types.NewDatum(nil))
					}
				default:
					row = append(row, types.NewDatum(nil))
				}
			}

			res = append(res, row)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

// deadlocksTableRetriever is the memtable retriever for the DEADLOCKS and CLUSTER_DEADLOCKS table.
type deadlocksTableRetriever struct {
	dummyCloser
	batchRetrieverHelper

	currentIdx          int
	currentWaitChainIdx int

	table       *model.TableInfo
	columns     []*model.ColumnInfo
	deadlocks   []*deadlockhistory.DeadlockRecord
	initialized bool
}

// nextIndexPair advances a index pair (where `idx` is the index of the DeadlockRecord, and `waitChainIdx` is the index
// of the wait chain item in the `idx`-th DeadlockRecord. This function helps iterate over each wait chain item
// in all DeadlockRecords.
func (r *deadlocksTableRetriever) nextIndexPair(idx, waitChainIdx int) (int, int) {
	waitChainIdx++
	if waitChainIdx >= len(r.deadlocks[idx].WaitChain) {
		waitChainIdx = 0
		idx++
		for idx < len(r.deadlocks) && len(r.deadlocks[idx].WaitChain) == 0 {
			idx++
		}
	}
	return idx, waitChainIdx
}

func (r *deadlocksTableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if r.retrieved {
		return nil, nil
	}

	if !r.initialized {
		if !hasPriv(sctx, mysql.ProcessPriv) {
			return nil, plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
		}

		r.initialized = true
		r.deadlocks = deadlockhistory.GlobalDeadlockHistory.GetAll()

		r.batchRetrieverHelper.totalRows = 0
		for _, d := range r.deadlocks {
			r.batchRetrieverHelper.totalRows += len(d.WaitChain)
		}
		r.batchRetrieverHelper.batchSize = 1024
	}

	// The current TiDB node's address is needed by the CLUSTER_DEADLOCKS table.
	var err error
	var instanceAddr string
	switch r.table.Name.O {
	case infoschema.ClusterTableDeadlocks:
		instanceAddr, err = infoschema.GetInstanceAddr(sctx)
		if err != nil {
			return nil, err
		}
	}

	infoSchema := sctx.GetInfoSchema().(infoschema.InfoSchema)

	var res [][]types.Datum

	err = r.nextBatch(func(start, end int) error {
		// Before getting rows, collect the SQL digests that needs to be retrieved first.
		var sqlRetriever *expression.SQLDigestTextRetriever
		for _, c := range r.columns {
			if c.Name.O == deadlockhistory.ColCurrentSQLDigestTextStr {
				if sqlRetriever == nil {
					sqlRetriever = expression.NewSQLDigestTextRetriever()
				}

				idx, waitChainIdx := r.currentIdx, r.currentWaitChainIdx
				for i := start; i < end; i++ {
					if idx >= len(r.deadlocks) {
						return errors.New("reading information_schema.(cluster_)deadlocks table meets corrupted index")
					}

					sqlRetriever.SQLDigestsMap[r.deadlocks[idx].WaitChain[waitChainIdx].SQLDigest] = ""
					// Step to the next entry
					idx, waitChainIdx = r.nextIndexPair(idx, waitChainIdx)
				}
			}
		}
		// Retrieve the SQL texts if necessary.
		if sqlRetriever != nil {
			err1 := sqlRetriever.RetrieveGlobal(ctx, sctx)
			if err1 != nil {
				return errors.Trace(err1)
			}
		}

		res = make([][]types.Datum, 0, end-start)

		for i := start; i < end; i++ {
			if r.currentIdx >= len(r.deadlocks) {
				return errors.New("reading information_schema.(cluster_)deadlocks table meets corrupted index")
			}

			row := make([]types.Datum, 0, len(r.columns))
			deadlock := r.deadlocks[r.currentIdx]
			waitChainItem := deadlock.WaitChain[r.currentWaitChainIdx]

			for _, c := range r.columns {
				if c.Name.O == util.ClusterTableInstanceColumnName {
					row = append(row, types.NewDatum(instanceAddr))
				} else if c.Name.O == deadlockhistory.ColCurrentSQLDigestTextStr {
					if text, ok := sqlRetriever.SQLDigestsMap[waitChainItem.SQLDigest]; ok && len(text) > 0 {
						row = append(row, types.NewDatum(text))
					} else {
						row = append(row, types.NewDatum(nil))
					}
				} else if c.Name.O == deadlockhistory.ColKeyInfoStr {
					value := types.NewDatum(nil)
					if len(waitChainItem.Key) > 0 {
						decodedKey, err := keydecoder.DecodeKey(waitChainItem.Key, infoSchema)
						if err == nil {
							decodedKeyJSON, err := json.Marshal(decodedKey)
							if err != nil {
								logutil.BgLogger().Warn("marshal decoded key info to JSON failed", zap.Error(err))
							} else {
								value = types.NewDatum(string(decodedKeyJSON))
							}
						} else {
							logutil.BgLogger().Warn("decode key failed", zap.Error(err))
						}
					}
					row = append(row, value)
				} else {
					row = append(row, deadlock.ToDatum(r.currentWaitChainIdx, c.Name.O))
				}
			}

			res = append(res, row)
			// Step to the next entry
			r.currentIdx, r.currentWaitChainIdx = r.nextIndexPair(r.currentIdx, r.currentWaitChainIdx)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

type hugeMemTableRetriever struct {
	dummyCloser
	extractor          *plannercore.ColumnsTableExtractor
	table              *model.TableInfo
	columns            []*model.ColumnInfo
	retrieved          bool
	initialized        bool
	rows               [][]types.Datum
	dbs                []*model.DBInfo
	dbsIdx             int
	tblIdx             int
	viewMu             sync.RWMutex
	viewSchemaMap      map[int64]*expression.Schema // table id to view schema
	viewOutputNamesMap map[int64]types.NameSlice    // table id to view output names
}

// retrieve implements the infoschemaRetriever interface
func (e *hugeMemTableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved {
		return nil, nil
	}

	if !e.initialized {
		is := sctx.GetInfoSchema().(infoschema.InfoSchema)
		dbs := is.AllSchemas()
		sort.Sort(infoschema.SchemasSorter(dbs))
		e.dbs = dbs
		e.initialized = true
		e.rows = make([][]types.Datum, 0, 1024)
	}

	var err error
	switch e.table.Name.O {
	case infoschema.TableColumns:
		err = e.setDataForColumns(ctx, sctx, e.extractor)
	}
	if err != nil {
		return nil, err
	}
	e.retrieved = len(e.rows) == 0

	return adjustColumns(e.rows, e.columns, e.table), nil
}

func adjustColumns(input [][]types.Datum, outColumns []*model.ColumnInfo, table *model.TableInfo) [][]types.Datum {
	if table.Name.O == infoschema.TableStatementsSummary {
		return input
	}
	if len(outColumns) == len(table.Columns) {
		return input
	}
	rows := make([][]types.Datum, len(input))
	for i, fullRow := range input {
		row := make([]types.Datum, len(outColumns))
		for j, col := range outColumns {
			row[j] = fullRow[col.Offset]
		}
		rows[i] = row
	}
	return rows
}

// TiFlashSystemTableRetriever is used to read system table from tiflash.
type TiFlashSystemTableRetriever struct {
	dummyCloser
	table         *model.TableInfo
	outputCols    []*model.ColumnInfo
	instanceCount int
	instanceIdx   int
	instanceInfos []tiflashInstanceInfo
	rowIdx        int
	retrieved     bool
	initialized   bool
	extractor     *plannercore.TiFlashSystemTableExtractor
}

func (e *TiFlashSystemTableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}
	if !e.initialized {
		err := e.initialize(sctx, e.extractor.TiFlashInstances)
		if err != nil {
			return nil, err
		}
	}
	if e.instanceCount == 0 || e.instanceIdx >= e.instanceCount {
		e.retrieved = true
		return nil, nil
	}

	for {
		rows, err := e.dataForTiFlashSystemTables(sctx, e.extractor.TiDBDatabases, e.extractor.TiDBTables)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 || e.instanceIdx >= e.instanceCount {
			return rows, nil
		}
	}
}

type tiflashInstanceInfo struct {
	id  string
	url string
}

func (e *TiFlashSystemTableRetriever) initialize(sctx sessionctx.Context, tiflashInstances set.StringSet) error {
	storeInfo, err := infoschema.GetStoreServerInfo(sctx)
	if err != nil {
		return err
	}

	for _, info := range storeInfo {
		if info.ServerType != kv.TiFlash.Name() {
			continue
		}
		info.ResolveLoopBackAddr()
		if len(tiflashInstances) > 0 && !tiflashInstances.Exist(info.Address) {
			continue
		}
		hostAndStatusPort := strings.Split(info.StatusAddr, ":")
		if len(hostAndStatusPort) != 2 {
			return errors.Errorf("node status addr: %s format illegal", info.StatusAddr)
		}
		// fetch tiflash config
		configURL := fmt.Sprintf("%s://%s/config", util.InternalHTTPSchema(), info.StatusAddr)
		resp, err := util.InternalHTTPClient().Get(configURL)
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("request %s failed: %s", configURL, resp.Status)
		}
		// parse http_port or https_port from the fetched config
		var nestedConfig map[string]interface{}
		if err = json.NewDecoder(resp.Body).Decode(&nestedConfig); err != nil {
			return err
		}
		if engineStoreConfig, ok := nestedConfig["engine-store"]; ok {
			foundPort := false
			var port interface{}
			portProtocol := ""
			if httpPort, ok := engineStoreConfig.(map[string]interface{})["http_port"]; ok {
				foundPort = true
				port = httpPort
				portProtocol = "http"
			} else if httpsPort, ok := engineStoreConfig.(map[string]interface{})["https_port"]; ok {
				foundPort = true
				port = httpsPort
				portProtocol = "https"
			}
			if !foundPort {
				return errors.Errorf("engine-store.http_port/https_port not found in server %s", info.Address)
			}
			switch portValue := port.(type) {
			case float64:
				e.instanceInfos = append(e.instanceInfos, tiflashInstanceInfo{
					id:  info.Address,
					url: fmt.Sprintf("%s://%s:%d", portProtocol, hostAndStatusPort[0], int(portValue)),
				})
				e.instanceCount += 1
			default:
				return errors.Errorf("engine-store.http_port value(%p) unexpected in server %s", port, info.Address)
			}
		} else {
			return errors.Errorf("engine-store config not found in server %s", info.Address)
		}
		if err = resp.Body.Close(); err != nil {
			return err
		}
	}
	e.initialized = true
	return nil
}

func (e *TiFlashSystemTableRetriever) dataForTiFlashSystemTables(ctx sessionctx.Context, tidbDatabases string, tidbTables string) ([][]types.Datum, error) {
	var columnNames []string // nolint: prealloc
	for _, c := range e.outputCols {
		if c.Name.O == "TIFLASH_INSTANCE" {
			continue
		}
		columnNames = append(columnNames, c.Name.L)
	}
	maxCount := 1024
	targetTable := strings.ToLower(strings.Replace(e.table.Name.O, "TIFLASH", "DT", 1))
	var filters []string
	if len(tidbDatabases) > 0 {
		filters = append(filters, fmt.Sprintf("tidb_database IN (%s)", strings.ReplaceAll(tidbDatabases, "\"", "'")))
	}
	if len(tidbTables) > 0 {
		filters = append(filters, fmt.Sprintf("tidb_table IN (%s)", strings.ReplaceAll(tidbTables, "\"", "'")))
	}
	sql := fmt.Sprintf("SELECT %s FROM system.%s", strings.Join(columnNames, ","), targetTable)
	if len(filters) > 0 {
		sql = fmt.Sprintf("%s WHERE %s", sql, strings.Join(filters, " AND "))
	}
	sql = fmt.Sprintf("%s LIMIT %d, %d", sql, e.rowIdx, maxCount)
	notNumber := "nan"
	instanceInfo := e.instanceInfos[e.instanceIdx]
	url := instanceInfo.url
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	q := req.URL.Query()
	q.Add("query", sql)
	req.URL.RawQuery = q.Encode()
	resp, err := util.InternalHTTPClient().Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	body, err := io.ReadAll(resp.Body)
	terror.Log(resp.Body.Close())
	if err != nil {
		return nil, errors.Trace(err)
	}
	records := strings.Split(string(body), "\n")
	rows := make([][]types.Datum, 0, len(records))
	for _, record := range records {
		if len(record) == 0 {
			continue
		}
		fields := strings.Split(record, "\t")
		if len(fields) < len(e.outputCols)-1 {
			return nil, errors.Errorf("Record from tiflash doesn't match schema %v", fields)
		}
		row := make([]types.Datum, len(e.outputCols))
		for index, column := range e.outputCols {
			if column.Name.O == "TIFLASH_INSTANCE" {
				continue
			}
			if column.GetType() == mysql.TypeVarchar {
				row[index].SetString(fields[index], mysql.DefaultCollationName)
			} else if column.GetType() == mysql.TypeLonglong {
				if fields[index] == notNumber {
					continue
				}
				value, err := strconv.ParseInt(fields[index], 10, 64)
				if err != nil {
					return nil, errors.Trace(err)
				}
				row[index].SetInt64(value)
			} else if column.GetType() == mysql.TypeDouble {
				if fields[index] == notNumber {
					continue
				}
				value, err := strconv.ParseFloat(fields[index], 64)
				if err != nil {
					return nil, errors.Trace(err)
				}
				row[index].SetFloat64(value)
			} else {
				return nil, errors.Errorf("Meet column of unknown type %v", column)
			}
		}
		row[len(e.outputCols)-1].SetString(instanceInfo.id, mysql.DefaultCollationName)
		rows = append(rows, row)
	}
	e.rowIdx += len(rows)
	if len(rows) < maxCount {
		e.instanceIdx += 1
		e.rowIdx = 0
	}
	return rows, nil
}

func (e *memtableRetriever) setDataForAttributes(ctx sessionctx.Context, is infoschema.InfoSchema) error {
	checker := privilege.GetPrivilegeManager(ctx)
	rules, err := infosync.GetAllLabelRules(context.TODO())
	skipValidateTable := false
	failpoint.Inject("mockOutputOfAttributes", func() {
		convert := func(i interface{}) []interface{} {
			return []interface{}{i}
		}
		rules = []*label.Rule{
			{
				ID:       "schema/test/test_label",
				Labels:   []label.Label{{Key: "merge_option", Value: "allow"}, {Key: "db", Value: "test"}, {Key: "table", Value: "test_label"}},
				RuleType: "key-range",
				Data: convert(map[string]interface{}{
					"start_key": "7480000000000000ff395f720000000000fa",
					"end_key":   "7480000000000000ff3a5f720000000000fa",
				}),
			},
			{
				ID:       "invalidIDtest",
				Labels:   []label.Label{{Key: "merge_option", Value: "allow"}, {Key: "db", Value: "test"}, {Key: "table", Value: "test_label"}},
				RuleType: "key-range",
				Data: convert(map[string]interface{}{
					"start_key": "7480000000000000ff395f720000000000fa",
					"end_key":   "7480000000000000ff3a5f720000000000fa",
				}),
			},
			{
				ID:       "schema/test/test_label",
				Labels:   []label.Label{{Key: "merge_option", Value: "allow"}, {Key: "db", Value: "test"}, {Key: "table", Value: "test_label"}},
				RuleType: "key-range",
				Data: convert(map[string]interface{}{
					"start_key": "aaaaa",
					"end_key":   "bbbbb",
				}),
			},
		}
		err = nil
		skipValidateTable = true
	})

	if err != nil {
		return errors.Wrap(err, "get the label rules failed")
	}

	rows := make([][]types.Datum, 0, len(rules))
	for _, rule := range rules {
		skip := true
		dbName, tableName, partitionName, err := checkRule(rule)
		if err != nil {
			logutil.BgLogger().Warn("check table-rule failed", zap.String("ID", rule.ID), zap.Error(err))
			continue
		}
		tableID, err := decodeTableIDFromRule(rule)
		if err != nil {
			logutil.BgLogger().Warn("decode table ID from rule failed", zap.String("ID", rule.ID), zap.Error(err))
			continue
		}

		if !skipValidateTable && tableOrPartitionNotExist(dbName, tableName, partitionName, is, tableID) {
			continue
		}

		if tableName != "" && dbName != "" && (checker == nil || checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, dbName, tableName, "", mysql.SelectPriv)) {
			skip = false
		}
		if skip {
			continue
		}

		labels := rule.Labels.Restore()
		var ranges []string
		for _, data := range rule.Data {
			if kv, ok := data.(map[string]interface{}); ok {
				startKey := kv["start_key"]
				endKey := kv["end_key"]
				ranges = append(ranges, fmt.Sprintf("[%s, %s]", startKey, endKey))
			}
		}
		kr := strings.Join(ranges, ", ")

		row := types.MakeDatums(
			rule.ID,
			rule.RuleType,
			labels,
			kr,
		)
		rows = append(rows, row)
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromPlacementPolicies(sctx sessionctx.Context) error {
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	placementPolicies := is.AllPlacementPolicies()
	rows := make([][]types.Datum, 0, len(placementPolicies))
	// Get global PLACEMENT POLICIES
	// Currently no privileges needed for seeing global PLACEMENT POLICIES!
	for _, policy := range placementPolicies {
		// Currently we skip converting syntactic sugar. We might revisit this decision still in the future
		// I.e.: if PrimaryRegion or Regions are set,
		// also convert them to LeaderConstraints and FollowerConstraints
		// for better user experience searching for particular constraints

		// Followers == 0 means not set, so the default value 2 will be used
		followerCnt := policy.PlacementSettings.Followers
		if followerCnt == 0 {
			followerCnt = 2
		}

		row := types.MakeDatums(
			policy.ID,
			infoschema.CatalogVal, // CATALOG
			policy.Name.O,         // Policy Name
			policy.PlacementSettings.PrimaryRegion,
			policy.PlacementSettings.Regions,
			policy.PlacementSettings.Constraints,
			policy.PlacementSettings.LeaderConstraints,
			policy.PlacementSettings.FollowerConstraints,
			policy.PlacementSettings.LearnerConstraints,
			policy.PlacementSettings.Schedule,
			followerCnt,
			policy.PlacementSettings.Learners,
		)
		rows = append(rows, row)
	}
	e.rows = rows
	return nil
}

func checkRule(rule *label.Rule) (dbName, tableName string, partitionName string, err error) {
	s := strings.Split(rule.ID, "/")
	if len(s) < 3 {
		err = errors.Errorf("invalid label rule ID: %v", rule.ID)
		return
	}
	if rule.RuleType == "" {
		err = errors.New("empty label rule type")
		return
	}
	if rule.Labels == nil || len(rule.Labels) == 0 {
		err = errors.New("the label rule has no label")
		return
	}
	if rule.Data == nil {
		err = errors.New("the label rule has no data")
		return
	}
	dbName = s[1]
	tableName = s[2]
	if len(s) > 3 {
		partitionName = s[3]
	}
	return
}

func decodeTableIDFromRule(rule *label.Rule) (tableID int64, err error) {
	if len(rule.Data) == 0 {
		err = errors.New(fmt.Sprintf("there is no data in rule %s", rule.ID))
		return
	}
	data := rule.Data[0]
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		err = errors.New(fmt.Sprintf("get the label rules %s failed", rule.ID))
		return
	}
	key, err := hex.DecodeString(fmt.Sprintf("%s", dataMap["start_key"]))
	if err != nil {
		err = errors.New(fmt.Sprintf("decode key from start_key %s in rule %s failed", dataMap["start_key"], rule.ID))
		return
	}
	_, bs, err := codec.DecodeBytes(key, nil)
	if err == nil {
		key = bs
	}
	tableID = tablecodec.DecodeTableID(key)
	if tableID == 0 {
		err = errors.New(fmt.Sprintf("decode tableID from key %s in rule %s failed", key, rule.ID))
		return
	}
	return
}

func tableOrPartitionNotExist(dbName string, tableName string, partitionName string, is infoschema.InfoSchema, tableID int64) (tableNotExist bool) {
	if len(partitionName) == 0 {
		curTable, _ := is.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
		if curTable == nil {
			return true
		}
		curTableID := curTable.Meta().ID
		if curTableID != tableID {
			return true
		}
	} else {
		_, _, partInfo := is.FindTableByPartitionID(tableID)
		if partInfo == nil {
			return true
		}
	}
	return false
}
