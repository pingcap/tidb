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
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/pdhelper"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	"github.com/pingcap/tidb/pkg/util/servermemorylimit"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	pd "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

var lowerPrimaryKeyName = strings.ToLower(mysql.PrimaryKeyName)

type memtableRetriever struct {
	dummyCloser
	table       *model.TableInfo
	columns     []*model.ColumnInfo
	rows        [][]types.Datum
	rowIdx      int
	retrieved   bool
	initialized bool
	extractor   base.MemTablePredicateExtractor
	is          infoschema.InfoSchema

	memTracker      *memory.Tracker
	accMemPerBatch  int64
	accMemRecordCnt int
}

// retrieve implements the infoschemaRetriever interface
func (e *memtableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.table.Name.O == infoschema.TableClusterInfo && !hasPriv(sctx, mysql.ProcessPriv) {
		return nil, plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
	}
	if e.retrieved {
		return nil, nil
	}

	// Cache the ret full rows in schemataRetriever
	if !e.initialized {
		var err error
		// InTxn() should be true in most of the cases.
		// Because the transaction should have been activated in MemTableReaderExec Open().
		// Why not just activate the txn here (sctx.Txn(true)) and do it in Open() instead?
		// Because it could DATA RACE here and in Open() it's safe.
		if sctx.GetSessionVars().InTxn() {
			ts := sctx.GetSessionVars().TxnCtx.StartTS
			if sctx.GetSessionVars().SnapshotTS != 0 {
				ts = sctx.GetSessionVars().SnapshotTS
			}
			e.is, err = domain.GetDomain(sctx).GetSnapshotInfoSchema(ts)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			// When the excutor is built from tidb coprocessor request, the transaction is not valid.
			// Then InTxn() is false.
			//
			// What's the difference between using latest infoschema and using snapshot infoschema?
			// A query *should* use the infoschema of the txn start ts, but it's still safe to use the latest.
			// If now it's 12:00:00, the ts of the latest infoschema might be 11:59:30 or 11:52:12 or anything.
			// Say, default GC interval is 10min, the ts of the latest infoschema is 11:52:12.
			// Then the valid lifetime range on infoschema API become [11:52:12, 12:12:12) using latest infoschema,
			// but it should be [12:00:00, 12:10:00) if using the snapshot infoschema.
			e.is = sctx.GetInfoSchema().(infoschema.InfoSchema)
		}

		switch e.table.Name.O {
		case infoschema.TableSchemata:
			err = e.setDataFromSchemata(sctx)
		case infoschema.TableStatistics:
			err = e.setDataForStatistics(ctx, sctx)
		case infoschema.TableTables:
			err = e.setDataFromTables(ctx, sctx)
		case infoschema.TableReferConst:
			err = e.setDataFromReferConst(ctx, sctx)
		case infoschema.TableSequences:
			err = e.setDataFromSequences(ctx, sctx)
		case infoschema.TablePartitions:
			err = e.setDataFromPartitions(ctx, sctx)
		case infoschema.TableClusterInfo:
			err = e.dataForTiDBClusterInfo(sctx)
		case infoschema.TableAnalyzeStatus:
			err = e.setDataForAnalyzeStatus(ctx, sctx)
		case infoschema.TableTiDBIndexes:
			err = e.setDataFromIndexes(ctx, sctx)
		case infoschema.TableViews:
			err = e.setDataFromViews(ctx, sctx)
		case infoschema.TableEngines:
			e.setDataFromEngines()
		case infoschema.TableCharacterSets:
			e.setDataFromCharacterSets()
		case infoschema.TableCollations:
			e.setDataFromCollations()
		case infoschema.TableKeyColumn:
			err = e.setDataFromKeyColumnUsage(ctx, sctx)
		case infoschema.TableMetricTables:
			e.setDataForMetricTables()
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
			err = e.setDataForTiKVRegionStatus(ctx, sctx)
		case infoschema.TableTiDBHotRegions:
			err = e.setDataForTiDBHotRegions(ctx, sctx)
		case infoschema.TableConstraints:
			err = e.setDataFromTableConstraints(ctx, sctx)
		case infoschema.TableTiDBServersInfo:
			err = e.setDataForServersInfo(sctx)
		case infoschema.TableTiFlashReplica:
			err = e.dataForTableTiFlashReplica(ctx, sctx)
		case infoschema.TableTiKVStoreStatus:
			err = e.dataForTiKVStoreStatus(ctx, sctx)
		case infoschema.TableClientErrorsSummaryGlobal,
			infoschema.TableClientErrorsSummaryByUser,
			infoschema.TableClientErrorsSummaryByHost:
			err = e.setDataForClientErrorsSummary(sctx, e.table.Name.O)
		case infoschema.TableAttributes:
			err = e.setDataForAttributes(ctx, sctx, e.is)
		case infoschema.TablePlacementPolicies:
			err = e.setDataFromPlacementPolicies(sctx)
		case infoschema.TableTrxSummary:
			err = e.setDataForTrxSummary(sctx)
		case infoschema.ClusterTableTrxSummary:
			err = e.setDataForClusterTrxSummary(sctx)
		case infoschema.TableVariablesInfo:
			err = e.setDataForVariablesInfo(sctx)
		case infoschema.TableUserAttributes:
			err = e.setDataForUserAttributes(ctx, sctx)
		case infoschema.TableMemoryUsage:
			err = e.setDataForMemoryUsage()
		case infoschema.ClusterTableMemoryUsage:
			err = e.setDataForClusterMemoryUsage(sctx)
		case infoschema.TableMemoryUsageOpsHistory:
			err = e.setDataForMemoryUsageOpsHistory()
		case infoschema.ClusterTableMemoryUsageOpsHistory:
			err = e.setDataForClusterMemoryUsageOpsHistory(sctx)
		case infoschema.TableResourceGroups:
			err = e.setDataFromResourceGroups()
		case infoschema.TableRunawayWatches:
			err = e.setDataFromRunawayWatches(sctx)
		case infoschema.TableCheckConstraints:
			err = e.setDataFromCheckConstraints(ctx, sctx)
		case infoschema.TableTiDBCheckConstraints:
			err = e.setDataFromTiDBCheckConstraints(ctx, sctx)
		case infoschema.TableKeywords:
			err = e.setDataFromKeywords()
		case infoschema.TableTiDBIndexUsage:
			err = e.setDataFromIndexUsage(ctx, sctx)
		case infoschema.ClusterTableTiDBIndexUsage:
			err = e.setDataFromClusterIndexUsage(ctx, sctx)
		case infoschema.TableTiDBPlanCache:
			err = e.setDataFromPlanCache(ctx, sctx, false)
		case infoschema.ClusterTableTiDBPlanCache:
			err = e.setDataFromPlanCache(ctx, sctx, true)
		case infoschema.TableKeyspaceMeta:
			err = e.setDataForKeyspaceMeta(sctx)
		}
		if err != nil {
			return nil, err
		}
		e.initialized = true
		if e.memTracker != nil && e.accMemRecordCnt > 0 {
			e.memTracker.Consume(e.accMemPerBatch)
			e.accMemRecordCnt = 0
			e.accMemPerBatch = 0
		}
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

func (e *memtableRetriever) recordMemoryConsume(data []types.Datum) {
	if e.memTracker == nil {
		return
	}
	size := types.EstimatedMemUsage(data, 1)
	e.accMemPerBatch += size
	e.accMemRecordCnt++
	if e.accMemRecordCnt >= 1024 {
		e.memTracker.Consume(e.accMemPerBatch)
		e.accMemPerBatch = 0
		e.accMemRecordCnt = 0
	}
}

func getAutoIncrementID(
	is infoschema.InfoSchema,
	sctx sessionctx.Context,
	tblInfo *model.TableInfo,
) int64 {
	if raw, ok := is.(*infoschema.SessionExtendedInfoSchema); ok {
		if ok, v2 := infoschema.IsV2(raw.InfoSchema); ok {
			isCached := v2.TableIsCached(tblInfo.ID)
			if !isCached {
				// Loading table info from kv storage invalidates the cached auto_increment id.
				return 0
			}
		}
	}
	tbl, ok := is.TableByID(context.Background(), tblInfo.ID)
	if !ok {
		return 0
	}
	alloc := tbl.Allocators(sctx.GetTableCtx()).Get(autoid.AutoIncrementType)
	if alloc == nil || alloc.Base() == 0 {
		// It may not be loaded yet.
		// To show global next autoID, one should use `show table x next_row_id`.
		return 0
	}
	return alloc.Base() + 1
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

func (e *memtableRetriever) setDataForVariablesInfo(ctx sessionctx.Context) error {
	sysVars := variable.GetSysVars()
	rows := make([][]types.Datum, 0, len(sysVars))
	for _, sv := range sysVars {
		if infoschema.SysVarHiddenForSem(ctx, sv.Name) {
			continue
		}
		currentVal, err := ctx.GetSessionVars().GetSessionOrGlobalSystemVar(context.Background(), sv.Name)
		if err != nil {
			currentVal = ""
		}
		isNoop := "NO"
		if sv.IsNoop {
			isNoop = "YES"
		}
		defVal := sv.Value
		if sv.HasGlobalScope() {
			defVal = variable.GlobalSystemVariableInitialValue(sv.Name, defVal)
		}
		row := types.MakeDatums(
			sv.Name,           // VARIABLE_NAME
			sv.Scope.String(), // VARIABLE_SCOPE
			defVal,            // DEFAULT_VALUE
			currentVal,        // CURRENT_VALUE
			sv.MinValue,       // MIN_VALUE
			sv.MaxValue,       // MAX_VALUE
			nil,               // POSSIBLE_VALUES
			isNoop,            // IS_NOOP
		)
		// min and max value is only supported for numeric types
		if !(sv.Type == vardef.TypeUnsigned || sv.Type == vardef.TypeInt || sv.Type == vardef.TypeFloat) {
			row[4].SetNull()
			row[5].SetNull()
		}
		if sv.Type == vardef.TypeEnum {
			possibleValues := strings.Join(sv.PossibleValues, ",")
			row[6].SetString(possibleValues, mysql.DefaultCollationName)
		}
		rows = append(rows, row)
		e.recordMemoryConsume(row)
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataForUserAttributes(ctx context.Context, sctx sessionctx.Context) error {
	exec := sctx.GetRestrictedSQLExecutor()
	wrappedCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	chunkRows, _, err := exec.ExecRestrictedSQL(wrappedCtx, nil, `SELECT user, host, JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.metadata')) FROM mysql.user`)
	if err != nil {
		return err
	}
	if len(chunkRows) == 0 {
		return nil
	}
	rows := make([][]types.Datum, 0, len(chunkRows))
	for _, chunkRow := range chunkRows {
		if chunkRow.Len() != 3 {
			continue
		}
		user := chunkRow.GetString(0)
		host := chunkRow.GetString(1)
		// Compatible with results in MySQL
		var attribute any
		if attribute = chunkRow.GetString(2); attribute == "" {
			attribute = nil
		}
		row := types.MakeDatums(user, host, attribute)
		rows = append(rows, row)
		e.recordMemoryConsume(row)
	}

	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromSchemata(ctx sessionctx.Context) error {
	checker := privilege.GetPrivilegeManager(ctx)
	ex, ok := e.extractor.(*plannercore.InfoSchemaSchemataExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaSchemataExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}
	schemas := ex.ListSchemas(e.is)
	rows := make([][]types.Datum, 0, len(schemas))

	for _, schemaName := range schemas {
		schema, _ := e.is.SchemaByName(schemaName)
		charset := mysql.DefaultCharset
		collation := mysql.DefaultCollationName

		if len(schema.Charset) > 0 {
			charset = schema.Charset // Overwrite default
		}

		if len(schema.Collate) > 0 {
			collation = schema.Collate // Overwrite default
		}
		var policyName any
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
		e.recordMemoryConsume(record)
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataForStatistics(ctx context.Context, sctx sessionctx.Context) error {
	checker := privilege.GetPrivilegeManager(sctx)
	ex, ok := e.extractor.(*plannercore.InfoSchemaStatisticsExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaStatisticsExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}
	schemas, tables, err := ex.ListSchemasAndTables(ctx, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	for i, table := range tables {
		schema := schemas[i]
		if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.L, table.Name.L, "", mysql.AllPrivMask) {
			continue
		}
		e.setDataForStatisticsInTable(schema, table, ex)
	}
	return nil
}

func (e *memtableRetriever) setDataForStatisticsInTable(
	schema ast.CIStr,
	table *model.TableInfo,
	ex *plannercore.InfoSchemaStatisticsExtractor,
) {
	var rows [][]types.Datum
	if table.PKIsHandle && ex.HasPrimaryKey() {
		for _, col := range table.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				record := types.MakeDatums(
					infoschema.CatalogVal, // TABLE_CATALOG
					schema.O,              // TABLE_SCHEMA
					table.Name.O,          // TABLE_NAME
					"0",                   // NON_UNIQUE
					schema.O,              // INDEX_SCHEMA
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
				e.recordMemoryConsume(record)
			}
		}
	}
	nameToCol := make(map[string]*model.ColumnInfo, len(table.Columns))
	for _, c := range table.Columns {
		nameToCol[c.Name.L] = c
	}
	for _, index := range table.Indices {
		if !ex.HasIndex(index.Name.L) || index.State != model.StatePublic {
			continue
		}
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
			var expression any
			expression = nil
			tblCol := table.Columns[col.Offset]
			if tblCol.Hidden {
				colName = "NULL"
				expression = tblCol.GeneratedExprString
			}

			var subPart any
			if key.Length != types.UnspecifiedLength {
				subPart = key.Length
			}

			record := types.MakeDatums(
				infoschema.CatalogVal, // TABLE_CATALOG
				schema.O,              // TABLE_SCHEMA
				table.Name.O,          // TABLE_NAME
				nonUnique,             // NON_UNIQUE
				schema.O,              // INDEX_SCHEMA
				index.Name.O,          // INDEX_NAME
				i+1,                   // SEQ_IN_INDEX
				colName,               // COLUMN_NAME
				"A",                   // COLLATION
				0,                     // CARDINALITY
				subPart,               // SUB_PART
				nil,                   // PACKED
				nullable,              // NULLABLE
				"BTREE",               // INDEX_TYPE
				"",                    // COMMENT
				index.Comment,         // INDEX_COMMENT
				visible,               // IS_VISIBLE
				expression,            // Expression
			)
			rows = append(rows, record)
			e.recordMemoryConsume(record)
		}
	}
	e.rows = append(e.rows, rows...)
}

func (e *memtableRetriever) setDataFromReferConst(ctx context.Context, sctx sessionctx.Context) error {
	checker := privilege.GetPrivilegeManager(sctx)
	var rows [][]types.Datum
	ex, ok := e.extractor.(*plannercore.InfoSchemaReferConstExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaReferConstExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}
	schemas, tables, err := ex.ListSchemasAndTables(ctx, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	for i, table := range tables {
		schema := schemas[i]
		if !table.IsBaseTable() {
			continue
		}
		if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.L, table.Name.L, "", mysql.AllPrivMask) {
			continue
		}
		for _, fk := range table.ForeignKeys {
			if ok && !ex.HasConstraint(fk.Name.L) {
				continue
			}
			updateRule, deleteRule := "NO ACTION", "NO ACTION"
			if ast.ReferOptionType(fk.OnUpdate) != 0 {
				updateRule = ast.ReferOptionType(fk.OnUpdate).String()
			}
			if ast.ReferOptionType(fk.OnDelete) != 0 {
				deleteRule = ast.ReferOptionType(fk.OnDelete).String()
			}
			record := types.MakeDatums(
				infoschema.CatalogVal, // CONSTRAINT_CATALOG
				schema.O,              // CONSTRAINT_SCHEMA
				fk.Name.O,             // CONSTRAINT_NAME
				infoschema.CatalogVal, // UNIQUE_CONSTRAINT_CATALOG
				schema.O,              // UNIQUE_CONSTRAINT_SCHEMA
				"PRIMARY",             // UNIQUE_CONSTRAINT_NAME
				"NONE",                // MATCH_OPTION
				updateRule,            // UPDATE_RULE
				deleteRule,            // DELETE_RULE
				table.Name.O,          // TABLE_NAME
				fk.RefTable.O,         // REFERENCED_TABLE_NAME
			)
			rows = append(rows, record)
			e.recordMemoryConsume(record)
		}
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) updateStatsCacheIfNeed(sctx sessionctx.Context, tbls []*model.TableInfo) {
	needUpdate := false
	for _, col := range e.columns {
		// only the following columns need stats cache.
		if col.Name.O == "AVG_ROW_LENGTH" || col.Name.O == "DATA_LENGTH" || col.Name.O == "INDEX_LENGTH" || col.Name.O == "TABLE_ROWS" {
			needUpdate = true
			break
		}
	}
	if !needUpdate {
		return
	}

	tableIDs := make([]int64, 0, len(tbls))
	for _, tbl := range tbls {
		if pi := tbl.GetPartitionInfo(); pi != nil {
			for _, def := range pi.Definitions {
				tableIDs = append(tableIDs, def.ID)
			}
		}
		tableIDs = append(tableIDs, tbl.ID)
	}
	// Even for partitioned tables, we must update the stats cache for the main table itself.
	// This is necessary because the global index length from the table also needs to be included.
	// For further details, see: https://github.com/pingcap/tidb/issues/54173
	err := cache.TableRowStatsCache.UpdateByID(sctx, tableIDs...)
	if err != nil {
		logutil.BgLogger().Warn("cannot update stats cache for tables", zap.Error(err))
	}
	intest.AssertNoError(err)
}

func (e *memtableRetriever) setDataFromOneTable(
	sctx sessionctx.Context,
	loc *time.Location,
	checker privilege.Manager,
	schema ast.CIStr,
	table *model.TableInfo,
	rows [][]types.Datum,
) ([][]types.Datum, error) {
	collation := table.Collate
	if collation == "" {
		collation = mysql.DefaultCollationName
	}
	createTime := types.NewTime(types.FromGoTime(table.GetUpdateTime().In(loc)), mysql.TypeDatetime, types.DefaultFsp)

	createOptions := ""

	if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.L, table.Name.L, "", mysql.AllPrivMask) {
		return rows, nil
	}
	pkType := "NONCLUSTERED"
	if !table.IsView() {
		if table.GetPartitionInfo() != nil {
			createOptions = "partitioned"
		} else if table.TableCacheStatusType == model.TableCacheStatusEnable {
			createOptions = "cached=on"
		}
		var autoIncID any
		hasAutoIncID, _ := infoschema.HasAutoIncrementColumn(table)
		if hasAutoIncID {
			autoIncID = getAutoIncrementID(e.is, sctx, table)
		}
		tableType := "BASE TABLE"
		if metadef.IsMemDB(schema.L) {
			tableType = "SYSTEM VIEW"
		}
		if table.IsSequence() {
			tableType = "SEQUENCE"
		}
		if table.HasClusteredIndex() {
			pkType = "CLUSTERED"
		}
		shardingInfo := infoschema.GetShardingInfo(schema, table)
		var policyName any
		if table.PlacementPolicyRef != nil {
			policyName = table.PlacementPolicyRef.Name.O
		}

		var affinity any
		if info := table.Affinity; info != nil {
			affinity = info.Level
		}

		rowCount, avgRowLength, dataLength, indexLength := cache.TableRowStatsCache.EstimateDataLength(table)

		record := types.MakeDatums(
			infoschema.CatalogVal, // TABLE_CATALOG
			schema.O,              // TABLE_SCHEMA
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
			table.Mode.String(),   // TIDB_TABLE_MODE
			affinity,              // TIDB_AFFINITY
		)
		rows = append(rows, record)
		e.recordMemoryConsume(record)
	} else {
		record := types.MakeDatums(
			infoschema.CatalogVal, // TABLE_CATALOG
			schema.O,              // TABLE_SCHEMA
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
			nil,                   // TIDB_TABLE_MODE
			nil,                   // TIDB_AFFINITY
		)
		rows = append(rows, record)
		e.recordMemoryConsume(record)
	}
	return rows, nil
}

func onlySchemaOrTableColumns(columns []*model.ColumnInfo) bool {
	if len(columns) <= 3 {
		for _, colInfo := range columns {
			switch colInfo.Name.L {
			case "table_schema":
			case "table_name":
			case "table_catalog":
			default:
				return false
			}
		}
		return true
	}
	return false
}

func onlySchemaOrTableColPredicates(predicates map[string]set.StringSet) bool {
	for str := range predicates {
		switch str {
		case "table_name":
		case "table_schema":
		case "table_catalog":
		default:
			return false
		}
	}
	return true
}

func (e *memtableRetriever) setDataFromTables(ctx context.Context, sctx sessionctx.Context) error {
	var rows [][]types.Datum
	checker := privilege.GetPrivilegeManager(sctx)
	ex, ok := e.extractor.(*plannercore.InfoSchemaTablesExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaTablesExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}

	// Special optimize for queries on infoschema v2 like:
	//     select count(table_schema) from INFORMATION_SCHEMA.TABLES
	//     select count(*) from INFORMATION_SCHEMA.TABLES
	//     select table_schema, table_name from INFORMATION_SCHEMA.TABLES
	// column pruning in general is not supported here.
	if onlySchemaOrTableColumns(e.columns) && onlySchemaOrTableColPredicates(ex.ColPredicates) {
		is := e.is
		if raw, ok := is.(*infoschema.SessionExtendedInfoSchema); ok {
			is = raw.InfoSchema
		}
		v2, ok := is.(interface {
			IterateAllTableItems(visit func(infoschema.TableItem) bool)
		})
		if ok {
			if x := ctx.Value("cover-check"); x != nil {
				// The interface assertion is too tricky, so we add test to cover here.
				// To ensure that if implementation changes one day, we can catch it.
				slot := x.(*bool)
				*slot = true
			}
			v2.IterateAllTableItems(func(t infoschema.TableItem) bool {
				if !ex.HasTableName(t.TableName.L) {
					return true
				}
				if !ex.HasTableSchema(t.DBName.L) {
					return true
				}
				if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, t.DBName.L, t.TableName.L, "", mysql.AllPrivMask) {
					return true
				}

				record := types.MakeDatums(
					infoschema.CatalogVal, // TABLE_CATALOG
					t.DBName.O,            // TABLE_SCHEMA
					t.TableName.O,         // TABLE_NAME
					nil,                   // TABLE_TYPE
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
					nil,                   // CREATE_TIME
					nil,                   // UPDATE_TIME
					nil,                   // CHECK_TIME
					nil,                   // TABLE_COLLATION
					nil,                   // CHECKSUM
					nil,                   // CREATE_OPTIONS
					nil,                   // TABLE_COMMENT
					nil,                   // TIDB_TABLE_ID
					nil,                   // TIDB_ROW_ID_SHARDING_INFO
					nil,                   // TIDB_PK_TYPE
					nil,                   // TIDB_PLACEMENT_POLICY_NAME
					nil,                   // TIDB_TABLE_MODE
					nil,                   // TIDB_AFFINITY
				)
				rows = append(rows, record)
				e.recordMemoryConsume(record)
				return true
			})
			e.rows = rows
			return nil
		}
	}

	// Normal code path.
	schemas, tables, err := ex.ListSchemasAndTables(ctx, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	e.updateStatsCacheIfNeed(sctx, tables)
	loc := sctx.GetSessionVars().TimeZone
	if loc == nil {
		loc = time.Local
	}
	for i, table := range tables {
		rows, err = e.setDataFromOneTable(sctx, loc, checker, schemas[i], table, rows)
		if err != nil {
			return errors.Trace(err)
		}
		if ctx.Err() != nil {
			return errors.Trace(ctx.Err())
		}
	}
	e.rows = rows
	return nil
}

// Data for inforation_schema.CHECK_CONSTRAINTS
// This is standards (ISO/IEC 9075-11) compliant and is compatible with the implementation in MySQL as well.
func (e *memtableRetriever) setDataFromCheckConstraints(ctx context.Context, sctx sessionctx.Context) error {
	var rows [][]types.Datum
	checker := privilege.GetPrivilegeManager(sctx)
	ex, ok := e.extractor.(*plannercore.InfoSchemaCheckConstraintsExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaCheckConstraintsExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}
	for _, schema := range ex.ListSchemas(e.is) {
		tables, err := e.is.SchemaTableInfos(ctx, schema)
		if err != nil {
			return errors.Trace(err)
		}
		for _, table := range tables {
			if len(table.Constraints) > 0 {
				if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.L, table.Name.L, "", mysql.SelectPriv) {
					continue
				}
				for _, constraint := range table.Constraints {
					if constraint.State != model.StatePublic {
						continue
					}
					if ok && !ex.HasConstraint(constraint.Name.L) {
						continue
					}
					record := types.MakeDatums(
						infoschema.CatalogVal, // CONSTRAINT_CATALOG
						schema.O,              // CONSTRAINT_SCHEMA
						constraint.Name.O,     // CONSTRAINT_NAME
						fmt.Sprintf("(%s)", constraint.ExprString), // CHECK_CLAUSE
					)
					rows = append(rows, record)
					e.recordMemoryConsume(record)
				}
			}
		}
	}
	e.rows = rows
	return nil
}

// Data for inforation_schema.TIDB_CHECK_CONSTRAINTS
// This has non-standard TiDB specific extensions.
func (e *memtableRetriever) setDataFromTiDBCheckConstraints(ctx context.Context, sctx sessionctx.Context) error {
	var rows [][]types.Datum
	checker := privilege.GetPrivilegeManager(sctx)
	ex, ok := e.extractor.(*plannercore.InfoSchemaTiDBCheckConstraintsExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaTiDBCheckConstraintsExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}
	schemas, tables, err := ex.ListSchemasAndTables(ctx, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	for i, table := range tables {
		schema := schemas[i]
		if len(table.Constraints) > 0 {
			if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.L, table.Name.L, "", mysql.SelectPriv) {
				continue
			}
			for _, constraint := range table.Constraints {
				if constraint.State != model.StatePublic {
					continue
				}
				if ok && !ex.HasConstraint(constraint.Name.L) {
					continue
				}
				record := types.MakeDatums(
					infoschema.CatalogVal, // CONSTRAINT_CATALOG
					schema.O,              // CONSTRAINT_SCHEMA
					constraint.Name.O,     // CONSTRAINT_NAME
					fmt.Sprintf("(%s)", constraint.ExprString), // CHECK_CLAUSE
					table.Name.O, // TABLE_NAME
					table.ID,     // TABLE_ID
				)
				rows = append(rows, record)
				e.recordMemoryConsume(record)
			}
		}
	}
	e.rows = rows
	return nil
}

type hugeMemTableRetriever struct {
	dummyCloser
	extractor          *plannercore.InfoSchemaColumnsExtractor
	table              *model.TableInfo
	columns            []*model.ColumnInfo
	retrieved          bool
	initialized        bool
	rows               [][]types.Datum
	dbs                []ast.CIStr
	curTables          []*model.TableInfo
	dbsIdx             int
	tblIdx             int
	viewMu             syncutil.RWMutex
	viewSchemaMap      map[int64]*expression.Schema // table id to view schema
	viewOutputNamesMap map[int64]types.NameSlice    // table id to view output names
	batch              int
	is                 infoschema.InfoSchema
}

// retrieve implements the infoschemaRetriever interface
func (e *hugeMemTableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}

	if !e.initialized {
		e.is = sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
		e.dbs = e.extractor.ListSchemas(e.is)
		e.initialized = true
		e.rows = make([][]types.Datum, 0, 1024)
		e.batch = 1024
	}

	var err error
	if e.table.Name.O == infoschema.TableColumns {
		err = e.setDataForColumns(ctx, sctx)
	}
	if err != nil {
		return nil, err
	}
	e.retrieved = len(e.rows) == 0

	return adjustColumns(e.rows, e.columns, e.table), nil
}

func (e *hugeMemTableRetriever) setDataForColumns(ctx context.Context, sctx sessionctx.Context) error {
	checker := privilege.GetPrivilegeManager(sctx)
	e.rows = e.rows[:0]
	for ; e.dbsIdx < len(e.dbs); e.dbsIdx++ {
		schema := e.dbs[e.dbsIdx]
		var table *model.TableInfo
		if len(e.curTables) == 0 {
			tables, err := e.extractor.ListTables(ctx, schema, e.is)
			if err != nil {
				return errors.Trace(err)
			}
			e.curTables = tables
		}
		for e.tblIdx < len(e.curTables) {
			table = e.curTables[e.tblIdx]
			e.tblIdx++
			if e.setDataForColumnsWithOneTable(ctx, sctx, schema, table, checker) {
				return nil
			}
		}
		e.tblIdx = 0
		e.curTables = e.curTables[:0]
	}
	return nil
}

func (e *hugeMemTableRetriever) setDataForColumnsWithOneTable(
	ctx context.Context,
	sctx sessionctx.Context,
	schema ast.CIStr,
	table *model.TableInfo,
	checker privilege.Manager,
) bool {
	hasPrivs := false
	var priv mysql.PrivilegeType
	if checker != nil {
		for _, p := range mysql.AllColumnPrivs {
			if checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.L, table.Name.L, "", p) {
				hasPrivs = true
				priv |= p
			}
		}
		if !hasPrivs {
			return false
		}
	}

	e.dataForColumnsInTable(ctx, sctx, schema, table, priv)
	return len(e.rows) >= e.batch
}

// Ref link https://github.com/mysql/mysql-server/blob/6b6d3ed3d5c6591b446276184642d7d0504ecc86/sql/dd/dd_table.cc#L411
func getNumericPrecision(ft *types.FieldType, colLen int) int {
	switch ft.GetType() {
	case mysql.TypeTiny:
		return 3
	case mysql.TypeShort:
		return 5
	case mysql.TypeInt24:
		// It's a MySQL bug, ref link https://bugs.mysql.com/bug.php?id=69042
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			return 8
		}
		return 7
	case mysql.TypeLong:
		return 10
	case mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			return 20
		}
		return 19
	case mysql.TypeBit, mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		return colLen
	}
	return 0
}

func (e *hugeMemTableRetriever) dataForColumnsInTable(
	ctx context.Context,
	sctx sessionctx.Context,
	schema ast.CIStr,
	tbl *model.TableInfo,
	priv mysql.PrivilegeType,
) {
	if tbl.IsView() {
		e.viewMu.Lock()
		_, ok := e.viewSchemaMap[tbl.ID]
		if !ok {
			var viewLogicalPlan base.Plan
			internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
			// Build plan is not thread safe, there will be concurrency on sessionctx.
			if err := runWithSystemSession(internalCtx, sctx, func(s sessionctx.Context) error {
				is := sessiontxn.GetTxnManager(s).GetTxnInfoSchema()
				planBuilder, _ := plannercore.NewPlanBuilder(plannercore.PlanBuilderOptNoExecution{}).Init(s.GetPlanCtx(), is, hint.NewQBHintHandler(nil))
				var err error
				viewLogicalPlan, err = planBuilder.BuildDataSourceFromView(ctx, schema, tbl, nil, nil)
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

	cols, ordinalPos := e.extractor.ListColumns(tbl)
	for i, col := range cols {
		// Skip non-public columns
		if col.State != model.StatePublic {
			continue
		}

		ft := &(col.FieldType)
		if tbl.IsView() {
			e.viewMu.RLock()
			if e.viewSchemaMap[tbl.ID] != nil {
				// If this is a view, replace the column with the view column.
				idx := expression.FindFieldNameIdxByColName(e.viewOutputNamesMap[tbl.ID], col.Name.L)
				if idx >= 0 {
					col1 := e.viewSchemaMap[tbl.ID].Columns[idx]
					ft = col1.GetType(sctx.GetExprCtx().GetEvalCtx())
				}
			}
			e.viewMu.RUnlock()
		}

		var charMaxLen, charOctLen, numericPrecision, numericScale, datetimePrecision any
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
			numericPrecision = getNumericPrecision(ft, colLen)
			if ft.GetType() != mysql.TypeFloat && ft.GetType() != mysql.TypeDouble {
				numericScale = decimal
			} else if decimal != -1 {
				numericScale = decimal
			}
		} else if ft.GetType() == mysql.TypeNull {
			charMaxLen, charOctLen = 0, 0
		}
		columnType := ft.InfoSchemaStr()
		columnDesc := table.NewColDesc(table.ToColumn(col))
		var columnDefault any
		if columnDesc.DefaultValue != nil {
			columnDefault = fmt.Sprintf("%v", columnDesc.DefaultValue)
			switch col.GetDefaultValue() {
			case "CURRENT_TIMESTAMP":
			default:
				if ft.GetType() == mysql.TypeTimestamp && columnDefault != types.ZeroDatetimeStr {
					timeValue, err := table.GetColDefaultValue(sctx.GetExprCtx(), col)
					if err == nil {
						columnDefault = timeValue.GetMysqlTime().String()
					}
				}
				if ft.GetType() == mysql.TypeBit && !col.DefaultIsExpr {
					defaultValBinaryLiteral := types.BinaryLiteral(columnDefault.(string))
					columnDefault = defaultValBinaryLiteral.ToBitLiteralString(true)
				}
			}
		}
		colType := ft.GetType()
		if colType == mysql.TypeVarString {
			colType = mysql.TypeVarchar
		}
		record := types.MakeDatums(
			infoschema.CatalogVal, // TABLE_CATALOG
			schema.O,              // TABLE_SCHEMA
			tbl.Name.O,            // TABLE_NAME
			col.Name.O,            // COLUMN_NAME
			ordinalPos[i],         // ORDINAL_POSITION
			columnDefault,         // COLUMN_DEFAULT
			columnDesc.Null,       // IS_NULLABLE
			types.TypeToStr(colType, ft.GetCharset()), // DATA_TYPE
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
			nil,                     // SRS_ID
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

func (e *memtableRetriever) setDataFromPartitions(ctx context.Context, sctx sessionctx.Context) error {
	checker := privilege.GetPrivilegeManager(sctx)
	var rows [][]types.Datum
	createTimeTp := mysql.TypeDatetime

	ex, ok := e.extractor.(*plannercore.InfoSchemaPartitionsExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaPartitionsExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}
	schemas, tables, err := ex.ListSchemasAndTables(ctx, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	e.updateStatsCacheIfNeed(sctx, tables)
	for i, table := range tables {
		schema := schemas[i]
		if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.L, table.Name.L, "", mysql.SelectPriv) {
			continue
		}
		createTime := types.NewTime(types.FromGoTime(table.GetUpdateTime()), createTimeTp, types.DefaultFsp)

		if ctx.Err() != nil {
			return errors.Trace(ctx.Err())
		}

		var affinity any
		if info := table.Affinity; info != nil {
			affinity = info.Level
		}

		var rowCount, dataLength, indexLength uint64
		if table.GetPartitionInfo() == nil {
			rowCount = cache.TableRowStatsCache.GetTableRows(table.ID)
			dataLength, indexLength = cache.TableRowStatsCache.GetDataAndIndexLength(table, table.ID, rowCount)
			avgRowLength := uint64(0)
			if rowCount != 0 {
				avgRowLength = dataLength / rowCount
			}
			// If there are any condition on the `PARTITION_NAME` in the extractor, this record should be ignored
			if ex.HasPartitionPred() {
				continue
			}
			record := types.MakeDatums(
				infoschema.CatalogVal, // TABLE_CATALOG
				schema.O,              // TABLE_SCHEMA
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
				affinity,              // TIDB_AFFINITY
			)
			rows = append(rows, record)
			e.recordMemoryConsume(record)
		} else {
			for i, pi := range table.GetPartitionInfo().Definitions {
				if !ex.HasPartition(pi.Name.L) {
					continue
				}
				rowCount = cache.TableRowStatsCache.GetTableRows(pi.ID)
				dataLength, indexLength = cache.TableRowStatsCache.GetDataAndIndexLength(table, pi.ID, rowCount)
				avgRowLength := uint64(0)
				if rowCount != 0 {
					avgRowLength = dataLength / rowCount
				}

				var partitionDesc string
				if table.Partition.Type == ast.PartitionTypeRange {
					partitionDesc = strings.Join(pi.LessThan, ",")
				} else if table.Partition.Type == ast.PartitionTypeList {
					if len(pi.InValues) > 0 {
						buf := bytes.NewBuffer(nil)
						for i, vs := range pi.InValues {
							if i > 0 {
								buf.WriteString(",")
							}
							if len(vs) != 1 {
								buf.WriteString("(")
							}
							buf.WriteString(strings.Join(vs, ","))
							if len(vs) != 1 {
								buf.WriteString(")")
							}
						}
						partitionDesc = buf.String()
					}
				}

				partitionMethod := table.Partition.Type.String()
				partitionExpr := table.Partition.Expr
				if len(table.Partition.Columns) > 0 {
					switch table.Partition.Type {
					case ast.PartitionTypeRange:
						partitionMethod = "RANGE COLUMNS"
					case ast.PartitionTypeList:
						partitionMethod = "LIST COLUMNS"
					case ast.PartitionTypeKey:
						partitionMethod = "KEY"
					default:
						return errors.Errorf("Inconsistent partition type, have type %v, but with COLUMNS > 0 (%d)", table.Partition.Type, len(table.Partition.Columns))
					}
					buf := bytes.NewBuffer(nil)
					for i, col := range table.Partition.Columns {
						if i > 0 {
							buf.WriteString(",")
						}
						buf.WriteString("`")
						buf.WriteString(col.String())
						buf.WriteString("`")
					}
					partitionExpr = buf.String()
				}

				var policyName any
				if pi.PlacementPolicyRef != nil {
					policyName = pi.PlacementPolicyRef.Name.O
				}
				record := types.MakeDatums(
					infoschema.CatalogVal, // TABLE_CATALOG
					schema.O,              // TABLE_SCHEMA
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
					affinity,              // TIDB_AFFINITY
				)
				rows = append(rows, record)
				e.recordMemoryConsume(record)
			}
		}
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromIndexes(ctx context.Context, sctx sessionctx.Context) error {
	ex, ok := e.extractor.(*plannercore.InfoSchemaIndexesExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaIndexesExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}

	schemas, tables, err := ex.ListSchemasAndTables(ctx, e.is)
	if err != nil {
		return errors.Trace(err)
	}

	var rows [][]types.Datum
	for i, table := range tables {
		rows, err = e.setDataFromIndex(sctx, schemas[i], table, rows)
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromIndex(
	sctx sessionctx.Context,
	schema ast.CIStr,
	tb *model.TableInfo,
	rows [][]types.Datum,
) ([][]types.Datum, error) {
	checker := privilege.GetPrivilegeManager(sctx)
	if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.L, tb.Name.L, "", mysql.AllPrivMask) {
		return rows, nil
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
			schema.O,     // TABLE_SCHEMA
			tb.Name.O,    // TABLE_NAME
			0,            // NON_UNIQUE
			"PRIMARY",    // KEY_NAME
			1,            // SEQ_IN_INDEX
			pkCol.Name.O, // COLUMN_NAME
			nil,          // SUB_PART
			"",           // INDEX_COMMENT
			nil,          // Expression
			0,            // INDEX_ID
			"YES",        // IS_VISIBLE
			"YES",        // CLUSTERED
			0,            // IS_GLOBAL
			nil,          // PREDICATE
		)
		rows = append(rows, record)
		e.recordMemoryConsume(record)
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
			var subPart any
			if col.Length != types.UnspecifiedLength {
				subPart = col.Length
			}
			colName := col.Name.O
			var expression any
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

			var predicate any
			if idxInfo.ConditionExprString != "" {
				predicate = idxInfo.ConditionExprString
			}

			record := types.MakeDatums(
				schema.O,        // TABLE_SCHEMA
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
				idxInfo.Global,  // IS_GLOBAL
				predicate,       // PREDICATE
			)
			rows = append(rows, record)
			e.recordMemoryConsume(record)
		}
	}
	return rows, nil
}

func (e *memtableRetriever) setDataFromViews(ctx context.Context, sctx sessionctx.Context) error {
	checker := privilege.GetPrivilegeManager(sctx)
	ex, ok := e.extractor.(*plannercore.InfoSchemaViewsExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaIndexesExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}
	schemas, tables, err := ex.ListSchemasAndTables(ctx, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	rows := make([][]types.Datum, 0, len(tables))
	for i, table := range tables {
		schema := schemas[i]
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
		if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.L, table.Name.L, "", mysql.AllPrivMask) {
			continue
		}
		record := types.MakeDatums(
			infoschema.CatalogVal,           // TABLE_CATALOG
			schema.O,                        // TABLE_SCHEMA
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
		e.recordMemoryConsume(record)
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) dataForTiKVStoreStatus(ctx context.Context, sctx sessionctx.Context) (err error) {
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		return errors.New("Information about TiKV store status can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	pdCli, err := tikvHelper.TryGetPDHTTPClient()
	if err != nil {
		return err
	}
	storesStat, err := pdCli.GetStores(ctx)
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
		bj := types.BinaryJSON{}
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
		startTs := types.NewTime(types.FromGoTime(storeStat.Status.StartTS), mysql.TypeDatetime, types.DefaultFsp)
		row[16].SetMysqlTime(startTs)
		lastHeartbeatTs := types.NewTime(types.FromGoTime(storeStat.Status.LastHeartbeatTS), mysql.TypeDatetime, types.DefaultFsp)
		row[17].SetMysqlTime(lastHeartbeatTs)
		row[18].SetString(storeStat.Status.Uptime, mysql.DefaultCollationName)
		if sem.IsEnabled() {
			// Patch out IP addresses etc if the user does not have the RESTRICTED_TABLES_ADMIN privilege
			checker := privilege.GetPrivilegeManager(sctx)
			if checker == nil || !checker.RequestDynamicVerification(sctx.GetSessionVars().ActiveRoles, "RESTRICTED_TABLES_ADMIN", false) {
				row[1].SetString(strconv.FormatInt(storeStat.Store.ID, 10), mysql.DefaultCollationName)
				row[1].SetNull()
				row[6].SetNull()
				row[7].SetNull()
				row[16].SetNull()
				row[18].SetNull()
			}
		}
		e.rows = append(e.rows, row)
		e.recordMemoryConsume(row)
	}
	return nil
}

// DDLJobsReaderExec executes DDLJobs information retrieving.
type DDLJobsReaderExec struct {
	exec.BaseExecutor
	DDLJobRetriever

	cacheJobs []*model.Job
	is        infoschema.InfoSchema
	sess      sessionctx.Context
}

// Open implements the Executor Next interface.
func (e *DDLJobsReaderExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	e.DDLJobRetriever.is = e.is
	e.activeRoles = e.Ctx().GetSessionVars().ActiveRoles
	sess, err := e.GetSysSession()
	if err != nil {
		return err
	}
	e.sess = sess
	err = sessiontxn.NewTxn(context.Background(), sess)
	if err != nil {
		return err
	}
	txn, err := sess.Txn(true)
	if err != nil {
		return err
	}
	sess.GetSessionVars().SetInTxn(true)
	err = e.DDLJobRetriever.initial(txn, sess)
	if err != nil {
		return err
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *DDLJobsReaderExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	checker := privilege.GetPrivilegeManager(e.Ctx())
	count := 0

	// Append running DDL jobs.
	if e.cursor < len(e.runningJobs) {
		num := min(req.Capacity(), len(e.runningJobs)-e.cursor)
		for i := e.cursor; i < e.cursor+num; i++ {
			e.appendJobToChunk(req, e.runningJobs[i], checker, false)
		}
		e.cursor += num
		count += num
	}
	var err error

	// Append history DDL jobs.
	if count < req.Capacity() && e.historyJobIter != nil {
		e.cacheJobs, err = e.historyJobIter.GetLastJobs(req.Capacity()-count, e.cacheJobs)
		if err != nil {
			return err
		}
		for _, job := range e.cacheJobs {
			e.appendJobToChunk(req, job, checker, false)
		}
		e.cursor += len(e.cacheJobs)
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *DDLJobsReaderExec) Close() error {
	e.ReleaseSysSession(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), e.sess)
	return e.BaseExecutor.Close()
}

func (e *memtableRetriever) setDataFromEngines() {
	var rows [][]types.Datum
	record := types.MakeDatums(
		"InnoDB",  // Engine
		"DEFAULT", // Support
		"Supports transactions, row-level locking, and foreign keys", // Comment
		"YES", // Transactions
		"YES", // XA
		"YES", // Savepoints
	)
	rows = append(rows, record)
	e.recordMemoryConsume(record)
	e.rows = rows
}

func (e *memtableRetriever) setDataFromCharacterSets() {
	charsets := charset.GetSupportedCharsets()
	rows := make([][]types.Datum, 0, len(charsets))
	for _, charset := range charsets {
		record := types.MakeDatums(charset.Name, charset.DefaultCollation, charset.Desc, charset.Maxlen)
		rows = append(rows, record)
		e.recordMemoryConsume(record)
	}
	e.rows = rows
}

func (e *memtableRetriever) setDataFromCollations() {
	collations := collate.GetSupportedCollations()
	rows := make([][]types.Datum, 0, len(collations))
	for _, collation := range collations {
		isDefault := ""
		if collation.IsDefault {
			isDefault = "Yes"
		}
		record := types.MakeDatums(collation.Name, collation.CharsetName, collation.ID,
			isDefault, "Yes", collation.Sortlen, collation.PadAttribute)
		rows = append(rows, record)
		e.recordMemoryConsume(record)
	}
	e.rows = rows
}

func (e *memtableRetriever) dataForCollationCharacterSetApplicability() {
	collations := collate.GetSupportedCollations()
	rows := make([][]types.Datum, 0, len(collations))
	for _, collation := range collations {
		record := types.MakeDatums(collation.Name, collation.CharsetName)
		rows = append(rows, record)
		e.recordMemoryConsume(record)
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
		upTimeStr := ""
		startTimeNative := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 0)
		if server.StartTimestamp > 0 {
			startTime := time.Unix(server.StartTimestamp, 0)
			startTimeNative = types.NewTime(types.FromGoTime(startTime), mysql.TypeDatetime, 0)
			upTimeStr = time.Since(startTime).String()
		}
		serverType := server.ServerType
		if server.ServerType == kv.TiFlash.Name() && server.EngineRole == placement.EngineRoleLabelWrite {
			serverType = infoschema.TiFlashWrite
		}
		row := types.MakeDatums(
			serverType,
			server.Address,
			server.StatusAddr,
			server.Version,
			server.GitHash,
			startTimeNative,
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
		e.recordMemoryConsume(row)
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromKeyColumnUsage(ctx context.Context, sctx sessionctx.Context) error {
	checker := privilege.GetPrivilegeManager(sctx)
	ex, ok := e.extractor.(*plannercore.InfoSchemaKeyColumnUsageExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaIndexesExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}
	schemas, tables, err := ex.ListSchemasAndTables(ctx, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	rows := make([][]types.Datum, 0, len(tables))
	for i, table := range tables {
		schema := schemas[i]
		if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.L, table.Name.L, "", mysql.AllPrivMask) {
			continue
		}
		if !ex.HasConstraintSchema(schema.L) {
			continue
		}
		rs := e.keyColumnUsageInTable(schema, table, ex)
		rows = append(rows, rs...)
	}
	e.rows = rows
	return nil
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

		rows := pi.ToRow(ctx.GetSessionVars().StmtCtx.TimeZone())
		record := types.MakeDatums(rows...)
		records = append(records, record)
		e.recordMemoryConsume(record)
	}
	e.rows = records
}

func (e *memtableRetriever) setDataFromUserPrivileges(ctx sessionctx.Context) {
	pm := privilege.GetPrivilegeManager(ctx)
	// The results depend on the user querying the information.
	e.rows = pm.UserPrivilegesTable(ctx.GetSessionVars().ActiveRoles, ctx.GetSessionVars().User.Username, ctx.GetSessionVars().User.Hostname)
}

func (e *memtableRetriever) setDataForMetricTables() {
	tables := make([]string, 0, len(infoschema.MetricTableMap))
	for name := range infoschema.MetricTableMap {
		tables = append(tables, name)
	}
	slices.Sort(tables)
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
		e.recordMemoryConsume(record)
	}
	e.rows = rows
}

func (e *memtableRetriever) keyColumnUsageInTable(schema ast.CIStr, table *model.TableInfo, ex *plannercore.InfoSchemaKeyColumnUsageExtractor) [][]types.Datum {
	var rows [][]types.Datum
	if table.PKIsHandle && ex.HasPrimaryKey() {
		for _, col := range table.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				record := types.MakeDatums(
					infoschema.CatalogVal,        // CONSTRAINT_CATALOG
					schema.O,                     // CONSTRAINT_SCHEMA
					infoschema.PrimaryConstraint, // CONSTRAINT_NAME
					infoschema.CatalogVal,        // TABLE_CATALOG
					schema.O,                     // TABLE_SCHEMA
					table.Name.O,                 // TABLE_NAME
					col.Name.O,                   // COLUMN_NAME
					1,                            // ORDINAL_POSITION
					1,                            // POSITION_IN_UNIQUE_CONSTRAINT
					nil,                          // REFERENCED_TABLE_SCHEMA
					nil,                          // REFERENCED_TABLE_NAME
					nil,                          // REFERENCED_COLUMN_NAME
				)
				rows = append(rows, record)
				e.recordMemoryConsume(record)
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
		var filterIdxName string
		if index.Primary {
			idxName = mysql.PrimaryKeyName
			filterIdxName = lowerPrimaryKeyName
		} else if index.Unique {
			idxName = index.Name.O
			filterIdxName = index.Name.L
		} else {
			// Only handle unique/primary key
			continue
		}

		if !ex.HasConstraint(filterIdxName) {
			continue
		}

		for i, key := range index.Columns {
			col := nameToCol[key.Name.L]
			if col.Hidden {
				continue
			}
			record := types.MakeDatums(
				infoschema.CatalogVal, // CONSTRAINT_CATALOG
				schema.O,              // CONSTRAINT_SCHEMA
				idxName,               // CONSTRAINT_NAME
				infoschema.CatalogVal, // TABLE_CATALOG
				schema.O,              // TABLE_SCHEMA
				table.Name.O,          // TABLE_NAME
				col.Name.O,            // COLUMN_NAME
				i+1,                   // ORDINAL_POSITION,
				nil,                   // POSITION_IN_UNIQUE_CONSTRAINT
				nil,                   // REFERENCED_TABLE_SCHEMA
				nil,                   // REFERENCED_TABLE_NAME
				nil,                   // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
			e.recordMemoryConsume(record)
		}
	}
	for _, fk := range table.ForeignKeys {
		if !ex.HasConstraint(fk.Name.L) {
			continue
		}

		for i, key := range fk.Cols {
			fkRefCol := ""
			if len(fk.RefCols) > i {
				fkRefCol = fk.RefCols[i].O
			}
			col := nameToCol[key.L]
			record := types.MakeDatums(
				infoschema.CatalogVal, // CONSTRAINT_CATALOG
				schema.O,              // CONSTRAINT_SCHEMA
				fk.Name.O,             // CONSTRAINT_NAME
				infoschema.CatalogVal, // TABLE_CATALOG
				schema.O,              // TABLE_SCHEMA
				table.Name.O,          // TABLE_NAME
				col.Name.O,            // COLUMN_NAME
				i+1,                   // ORDINAL_POSITION,
				1,                     // POSITION_IN_UNIQUE_CONSTRAINT
				fk.RefSchema.O,        // REFERENCED_TABLE_SCHEMA
				fk.RefTable.O,         // REFERENCED_TABLE_NAME
				fkRefCol,              // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
			e.recordMemoryConsume(record)
		}
	}
	return rows
}

func (e *memtableRetriever) setDataForTiKVRegionStatus(ctx context.Context, sctx sessionctx.Context) (err error) {
	checker := privilege.GetPrivilegeManager(sctx)
	var extractorTableIDs []int64
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		return errors.New("Information about TiKV region status can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	requestByTableRange := false
	var allRegionsInfo *pd.RegionsInfo
	is := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	if e.extractor != nil {
		extractor, ok := e.extractor.(*plannercore.TiKVRegionStatusExtractor)
		if ok && len(extractor.GetTablesID()) > 0 {
			extractorTableIDs = extractor.GetTablesID()
			for _, tableID := range extractorTableIDs {
				regionsInfo, err := e.getRegionsInfoForTable(ctx, tikvHelper, is, tableID)
				if err != nil {
					if errors.ErrorEqual(err, infoschema.ErrTableNotExists) {
						continue
					}
					return err
				}
				allRegionsInfo = allRegionsInfo.Merge(regionsInfo)
			}
			requestByTableRange = true
		}
	}
	if !requestByTableRange {
		pdCli, err := tikvHelper.TryGetPDHTTPClient()
		if err != nil {
			return err
		}
		allRegionsInfo, err = pdCli.GetRegions(ctx)
		if err != nil {
			return err
		}
	}
	if allRegionsInfo == nil {
		return nil
	}

	tableInfos := tikvHelper.GetRegionsTableInfo(allRegionsInfo, is, nil)
	for i := range allRegionsInfo.Regions {
		regionTableList := tableInfos[allRegionsInfo.Regions[i].ID]
		if len(regionTableList) == 0 {
			e.setNewTiKVRegionStatusCol(&allRegionsInfo.Regions[i], nil)
		}
		for j, regionTable := range regionTableList {
			// Exclude virtual schemas
			if metadef.IsMemDB(regionTable.DB.Name.L) {
				continue
			}
			if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, regionTable.DB.Name.L, regionTable.Table.Name.L, "", mysql.AllPrivMask) {
				continue
			}
			if len(extractorTableIDs) == 0 {
				e.setNewTiKVRegionStatusCol(&allRegionsInfo.Regions[i], &regionTable)
			}
			if slices.Contains(extractorTableIDs, regionTableList[j].Table.ID) {
				e.setNewTiKVRegionStatusCol(&allRegionsInfo.Regions[i], &regionTable)
			}
		}
	}
	return nil
}

func (e *memtableRetriever) getRegionsInfoForTable(ctx context.Context, h *helper.Helper, is infoschema.InfoSchema, tableID int64) (*pd.RegionsInfo, error) {
	tbl, _ := is.TableByID(ctx, tableID)
	if tbl == nil {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tableID)
	}

	pt := tbl.Meta().GetPartitionInfo()
	if pt == nil {
		regionsInfo, err := e.getRegionsInfoForSingleTable(ctx, h, tableID)
		if err != nil {
			return nil, err
		}
		return regionsInfo, nil
	}

	var allRegionsInfo *pd.RegionsInfo
	for _, def := range pt.Definitions {
		regionsInfo, err := e.getRegionsInfoForSingleTable(ctx, h, def.ID)
		if err != nil {
			return nil, err
		}
		allRegionsInfo = allRegionsInfo.Merge(regionsInfo)
	}
	return allRegionsInfo, nil
}

func (*memtableRetriever) getRegionsInfoForSingleTable(ctx context.Context, helper *helper.Helper, tableID int64) (*pd.RegionsInfo, error) {
	pdCli, err := helper.TryGetPDHTTPClient()
	if err != nil {
		return nil, err
	}
	sk, ek := tablecodec.GetTableHandleKeyRange(tableID)
	start, end := helper.Store.GetCodec().EncodeRegionRange(sk, ek)
	return pdCli.GetRegionsByKeyRange(ctx, pd.NewKeyRange(start, end), -1)
}

func (e *memtableRetriever) setNewTiKVRegionStatusCol(region *pd.RegionInfo, table *helper.TableInfo) {
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
		if table.IsPartition {
			row[9].SetInt64(1)
			row[10].SetInt64(table.Partition.ID)
			row[11].SetString(table.Partition.Name.O, mysql.DefaultCollationName)
		} else {
			row[9].SetInt64(0)
		}
	} else {
		row[6].SetInt64(0)
		row[9].SetInt64(0)
	}
	row[12].SetInt64(region.Epoch.ConfVer)
	row[13].SetInt64(region.Epoch.Version)
	row[14].SetUint64(region.WrittenBytes)
	row[15].SetUint64(region.ReadBytes)
	row[16].SetInt64(region.ApproximateSize)
	row[17].SetInt64(region.ApproximateKeys)
	if region.ReplicationStatus != nil {
		row[18].SetString(region.ReplicationStatus.State, mysql.DefaultCollationName)
		row[19].SetInt64(region.ReplicationStatus.StateID)
	}
	e.rows = append(e.rows, row)
	e.recordMemoryConsume(row)
}

const (
	normalPeer  = "NORMAL"
	pendingPeer = "PENDING"
	downPeer    = "DOWN"
)

func (e *memtableRetriever) setDataForTiDBHotRegions(ctx context.Context, sctx sessionctx.Context) error {
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		return errors.New("Information about hot region can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	metrics, err := tikvHelper.ScrapeHotInfo(ctx, helper.HotRead, is, tikvHelper.FilterMemDBs)
	if err != nil {
		return err
	}
	e.setDataForHotRegionByMetrics(metrics, "read")
	metrics, err = tikvHelper.ScrapeHotInfo(ctx, helper.HotWrite, is, nil)
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
		e.recordMemoryConsume(row)
	}
	e.rows = append(e.rows, rows...)
}

// setDataFromTableConstraints constructs data for table information_schema.constraints.See https://dev.mysql.com/doc/refman/5.7/en/table-constraints-table.html
func (e *memtableRetriever) setDataFromTableConstraints(ctx context.Context, sctx sessionctx.Context) error {
	checker := privilege.GetPrivilegeManager(sctx)
	ex, ok := e.extractor.(*plannercore.InfoSchemaTableConstraintsExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaIndexesExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}
	schemas, tables, err := ex.ListSchemasAndTables(ctx, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	rows := make([][]types.Datum, 0, len(tables))
	for i, tbl := range tables {
		schema := schemas[i]
		if !ex.HasConstraintSchema(schema.L) {
			continue
		}
		if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.L, tbl.Name.L, "", mysql.AllPrivMask) {
			continue
		}

		if tbl.PKIsHandle {
			if ex.HasPrimaryKey() {
				record := types.MakeDatums(
					infoschema.CatalogVal,     // CONSTRAINT_CATALOG
					schema.O,                  // CONSTRAINT_SCHEMA
					mysql.PrimaryKeyName,      // CONSTRAINT_NAME
					schema.O,                  // TABLE_SCHEMA
					tbl.Name.O,                // TABLE_NAME
					infoschema.PrimaryKeyType, // CONSTRAINT_TYPE
				)
				rows = append(rows, record)
				e.recordMemoryConsume(record)
			}
		}

		for _, idx := range tbl.Indices {
			var cname, ctype string
			var filterName string
			if idx.Primary {
				cname = mysql.PrimaryKeyName
				filterName = lowerPrimaryKeyName
				ctype = infoschema.PrimaryKeyType
			} else if idx.Unique {
				cname = idx.Name.O
				filterName = idx.Name.L
				ctype = infoschema.UniqueKeyType
			} else {
				// The index has no constriant.
				continue
			}
			if !ex.HasConstraint(filterName) {
				continue
			}
			record := types.MakeDatums(
				infoschema.CatalogVal, // CONSTRAINT_CATALOG
				schema.O,              // CONSTRAINT_SCHEMA
				cname,                 // CONSTRAINT_NAME
				schema.O,              // TABLE_SCHEMA
				tbl.Name.O,            // TABLE_NAME
				ctype,                 // CONSTRAINT_TYPE
			)
			rows = append(rows, record)
			e.recordMemoryConsume(record)
		}
		//  TiDB includes foreign key information for compatibility but foreign keys are not yet enforced.
		for _, fk := range tbl.ForeignKeys {
			if !ex.HasConstraint(fk.Name.L) {
				continue
			}
			record := types.MakeDatums(
				infoschema.CatalogVal,     // CONSTRAINT_CATALOG
				schema.O,                  // CONSTRAINT_SCHEMA
				fk.Name.O,                 // CONSTRAINT_NAME
				schema.O,                  // TABLE_SCHEMA
				tbl.Name.O,                // TABLE_NAME
				infoschema.ForeignKeyType, // CONSTRAINT_TYPE
			)
			rows = append(rows, record)
			e.recordMemoryConsume(record)
		}
	}
	e.rows = rows
	return nil
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
	stats         *pd.RegionStats
}

func (e *tableStorageStatsRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved {
		return nil, nil
	}
	if !e.initialized {
		err := e.initialize(ctx, sctx)
		if err != nil {
			return nil, err
		}
	}
	if len(e.initialTables) == 0 || e.curTable >= len(e.initialTables) {
		e.retrieved = true
		return nil, nil
	}

	rows, err := e.setDataForTableStorageStats(ctx)
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

func (e *tableStorageStatsRetriever) initialize(ctx context.Context, sctx sessionctx.Context) error {
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	var databases []string
	schemas := e.extractor.TableSchema
	tables := e.extractor.TableName

	// If not specify the table_schema, return an error to avoid traverse all schemas and their tables.
	if len(schemas) == 0 {
		return errors.Errorf("Please add where clause to filter the column TABLE_SCHEMA. " +
			"For example, where TABLE_SCHEMA = 'xxx' or where TABLE_SCHEMA in ('xxx', 'yyy')")
	}

	// Filter the sys or memory schema.
	for schema := range schemas {
		if !metadef.IsMemDB(schema) {
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
			tbs, err := is.SchemaTableInfos(ctx, ast.NewCIStr(DB))
			if err != nil {
				return errors.Trace(err)
			}
			for _, tb := range tbs {
				// For every db.table, check it's privileges.
				if checker(DB, tb.Name.L) {
					e.initialTables = append(e.initialTables, &initialTable{DB, tb})
				}
			}
		} else {
			// The user specified the table, extract the specified tables of this db to initialTable.
			for tb := range tables {
				if tb, err := is.TableByName(context.Background(), ast.NewCIStr(DB), ast.NewCIStr(tb)); err == nil {
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

func (e *tableStorageStatsRetriever) setDataForTableStorageStats(ctx context.Context) ([][]types.Datum, error) {
	rows := make([][]types.Datum, 0, 1024)
	count := 0
	for e.curTable < len(e.initialTables) && count < 1024 {
		tbl := e.initialTables[e.curTable]
		tblIDs := make([]int64, 0, 1)
		tblIDs = append(tblIDs, tbl.ID)
		if partInfo := tbl.GetPartitionInfo(); partInfo != nil {
			for _, partDef := range partInfo.Definitions {
				tblIDs = append(tblIDs, partDef.ID)
			}
		}
		var err error
		for _, tableID := range tblIDs {
			e.stats, err = e.helper.GetPDRegionStats(ctx, tableID, false)
			if err != nil {
				return nil, err
			}
			peerCount := 0
			for _, cnt := range e.stats.StorePeerCount {
				peerCount += cnt
			}

			record := types.MakeDatums(
				tbl.db,              // TABLE_SCHEMA
				tbl.Name.O,          // TABLE_NAME
				tableID,             // TABLE_ID
				peerCount,           // TABLE_PEER_COUNT
				e.stats.Count,       // TABLE_REGION_COUNT
				e.stats.EmptyCount,  // TABLE_EMPTY_REGION_COUNT
				e.stats.StorageSize, // TABLE_SIZE
				e.stats.StorageKeys, // TABLE_KEYS
			)
			rows = append(rows, record)
		}
		count++
		e.curTable++
	}
	return rows, nil
}

// dataForAnalyzeStatusHelper is a helper function which can be used in show_stats.go
func dataForAnalyzeStatusHelper(ctx context.Context, e *memtableRetriever, sctx sessionctx.Context) (rows [][]types.Datum, err error) {
	const maxAnalyzeJobs = 30
	const sql = "SELECT table_schema, table_name, partition_name, job_info, processed_rows, CONVERT_TZ(start_time, @@TIME_ZONE, '+00:00'), CONVERT_TZ(end_time, @@TIME_ZONE, '+00:00'), state, fail_reason, instance, process_id FROM mysql.analyze_jobs ORDER BY update_time DESC LIMIT %?"
	exec := sctx.GetRestrictedSQLExecutor()
	kctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	chunkRows, _, err := exec.ExecRestrictedSQL(kctx, nil, sql, maxAnalyzeJobs)
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
		var startTime, endTime any
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
		var failReason any
		if !chunkRow.IsNull(8) {
			failReason = chunkRow.GetString(8)
		}
		instance := chunkRow.GetString(9)
		var procID any
		if !chunkRow.IsNull(10) {
			procID = chunkRow.GetUint64(10)
		}

		var remainDurationStr, progressDouble, estimatedRowCntStr any
		if state == statistics.AnalyzeRunning && !strings.HasPrefix(jobInfo, "merge global stats") {
			startTime, ok := startTime.(types.Time)
			if !ok {
				return nil, errors.New("invalid start time")
			}
			remainingDuration, progress, estimatedRowCnt, remainDurationErr := getRemainDurationForAnalyzeStatusHelper(ctx, sctx, &startTime,
				dbName, tableName, partitionName, processedRows)
			if remainDurationErr != nil {
				logutil.BgLogger().Warn("get remaining duration failed", zap.Error(remainDurationErr))
			}
			if remainingDuration != nil {
				remainDurationStr = execdetails.FormatDuration(*remainingDuration)
			}
			progressDouble = progress
			estimatedRowCntStr = int64(estimatedRowCnt)
		}
		row := types.MakeDatums(
			dbName,             // TABLE_SCHEMA
			tableName,          // TABLE_NAME
			partitionName,      // PARTITION_NAME
			jobInfo,            // JOB_INFO
			processedRows,      // ROW_COUNT
			startTime,          // START_TIME
			endTime,            // END_TIME
			state,              // STATE
			failReason,         // FAIL_REASON
			instance,           // INSTANCE
			procID,             // PROCESS_ID
			remainDurationStr,  // REMAINING_SECONDS
			progressDouble,     // PROGRESS
			estimatedRowCntStr, // ESTIMATED_TOTAL_ROWS
		)
		rows = append(rows, row)
		if e != nil {
			e.recordMemoryConsume(row)
		}
	}
	return
}

func getRemainDurationForAnalyzeStatusHelper(
	ctx context.Context,
	sctx sessionctx.Context, startTime *types.Time,
	dbName, tableName, partitionName string, processedRows int64,
) (_ *time.Duration, percentage, totalCnt float64, err error) {
	remainingDuration := time.Duration(0)
	if startTime != nil {
		start, err := startTime.GoTime(time.UTC)
		if err != nil {
			return nil, percentage, totalCnt, err
		}
		duration := time.Now().UTC().Sub(start)
		if intest.InTest {
			if val := ctx.Value(AnalyzeProgressTest); val != nil {
				remainingDuration, percentage = calRemainInfoForAnalyzeStatus(ctx, int64(totalCnt), processedRows, duration)
				return &remainingDuration, percentage, totalCnt, nil
			}
		}
		var tid int64
		is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
		tb, err := is.TableByName(ctx, ast.NewCIStr(dbName), ast.NewCIStr(tableName))
		if err != nil {
			return nil, percentage, totalCnt, err
		}
		statsHandle := domain.GetDomain(sctx).StatsHandle()
		if statsHandle != nil {
			var statsTbl *statistics.Table
			meta := tb.Meta()
			if partitionName != "" {
				pt := meta.GetPartitionInfo()
				tid = pt.GetPartitionIDByName(partitionName)
				statsTbl = statsHandle.GetPhysicalTableStats(tid, meta)
			} else {
				statsTbl = statsHandle.GetPhysicalTableStats(meta.ID, meta)
				tid = meta.ID
			}
			if statsTbl != nil && statsTbl.RealtimeCount != 0 {
				totalCnt = float64(statsTbl.RealtimeCount)
			}
		}
		if (tid > 0 && totalCnt == 0) || float64(processedRows) > totalCnt {
			totalCnt, _ = pdhelper.GlobalPDHelper.GetApproximateTableCountFromStorage(ctx, sctx, tid, dbName, tableName, partitionName)
		}
		remainingDuration, percentage = calRemainInfoForAnalyzeStatus(ctx, int64(totalCnt), processedRows, duration)
	}
	return &remainingDuration, percentage, totalCnt, nil
}

func calRemainInfoForAnalyzeStatus(ctx context.Context, totalCnt int64, processedRows int64, duration time.Duration) (time.Duration, float64) {
	if intest.InTest {
		if val := ctx.Value(AnalyzeProgressTest); val != nil {
			totalCnt = 100 // But in final result, it is still 0.
			processedRows = 10
			duration = 1 * time.Minute
		}
	}
	if totalCnt == 0 {
		return 0, 100.0
	}
	remainLine := totalCnt - processedRows
	if processedRows == 0 {
		processedRows = 1
	}
	if duration == 0 {
		duration = 1 * time.Second
	}
	i := float64(remainLine) * duration.Seconds() / float64(processedRows)
	persentage := float64(processedRows) / float64(totalCnt)
	return time.Duration(i) * time.Second, persentage
}

// setDataForAnalyzeStatus gets all the analyze jobs.
func (e *memtableRetriever) setDataForAnalyzeStatus(ctx context.Context, sctx sessionctx.Context) (err error) {
	e.rows, err = dataForAnalyzeStatusHelper(ctx, e, sctx)
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
		e.recordMemoryConsume(row)
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
			stringutil.BuildStringFromLabels(info.Labels), // LABELS
		)
		if sem.IsEnabled() {
			checker := privilege.GetPrivilegeManager(ctx)
			if checker == nil || !checker.RequestDynamicVerification(ctx.GetSessionVars().ActiveRoles, "RESTRICTED_TABLES_ADMIN", false) {
				row[1].SetNull() // clear IP
			}
		}
		rows = append(rows, row)
		e.recordMemoryConsume(row)
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromSequences(ctx context.Context, sctx sessionctx.Context) error {
	checker := privilege.GetPrivilegeManager(sctx)
	extractor, ok := e.extractor.(*plannercore.InfoSchemaSequenceExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaSequenceExtractor", e.extractor)
	}
	if extractor.SkipRequest {
		return nil
	}
	schemas, tables, err := extractor.ListSchemasAndTables(ctx, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	rows := make([][]types.Datum, 0, len(tables))
	for i, table := range tables {
		schema := schemas[i]
		if !table.IsSequence() {
			continue
		}
		if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.L, table.Name.L, "", mysql.AllPrivMask) {
			continue
		}
		record := types.MakeDatums(
			infoschema.CatalogVal,     // TABLE_CATALOG
			schema.O,                  // SEQUENCE_SCHEMA
			table.Name.O,              // SEQUENCE_NAME
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
		e.recordMemoryConsume(record)
	}
	e.rows = rows
	return nil
}

// dataForTableTiFlashReplica constructs data for table tiflash replica info.
func (e *memtableRetriever) dataForTableTiFlashReplica(_ context.Context, sctx sessionctx.Context) error {
	var (
		checker       = privilege.GetPrivilegeManager(sctx)
		rows          [][]types.Datum
		tiFlashStores map[int64]pd.StoreInfo
	)
	rs := e.is.ListTablesWithSpecialAttribute(infoschemacontext.TiFlashAttribute)
	for _, schema := range rs {
		for _, tbl := range schema.TableInfos {
			if checker != nil && !checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, schema.DBName.L, tbl.Name.L, "", mysql.AllPrivMask) {
				continue
			}
			var progress float64
			if pi := tbl.GetPartitionInfo(); pi != nil && len(pi.Definitions) > 0 {
				for _, p := range pi.Definitions {
					progressOfPartition, err := infosync.MustGetTiFlashProgress(p.ID, tbl.TiFlashReplica.Count, &tiFlashStores)
					if err != nil {
						logutil.BgLogger().Warn("dataForTableTiFlashReplica error", zap.Int64("tableID", tbl.ID), zap.Int64("partitionID", p.ID), zap.Error(err))
					}
					progress += progressOfPartition
				}
				progress = progress / float64(len(pi.Definitions))
			} else {
				var err error
				progress, err = infosync.MustGetTiFlashProgress(tbl.ID, tbl.TiFlashReplica.Count, &tiFlashStores)
				if err != nil {
					logutil.BgLogger().Warn("dataForTableTiFlashReplica error", zap.Int64("tableID", tbl.ID), zap.Error(err))
				}
			}
			progressString := types.TruncateFloatToString(progress, 2)
			progress, _ = strconv.ParseFloat(progressString, 64)
			record := types.MakeDatums(
				schema.DBName.O,                 // TABLE_SCHEMA
				tbl.Name.O,                      // TABLE_NAME
				tbl.ID,                          // TABLE_ID
				int64(tbl.TiFlashReplica.Count), // REPLICA_COUNT
				strings.Join(tbl.TiFlashReplica.LocationLabels, ","), // LOCATION_LABELS
				tbl.TiFlashReplica.Available,                         // AVAILABLE
				progress,                                             // PROGRESS
			)
			rows = append(rows, record)
			e.recordMemoryConsume(record)
		}
	}
	e.rows = rows
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
			return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
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
			e.recordMemoryConsume(row)
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
				e.recordMemoryConsume(row)
			}
		}
	case infoschema.TableClientErrorsSummaryByHost:
		if !hasProcessPriv {
			return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
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
				e.recordMemoryConsume(row)
			}
		}
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataForTrxSummary(ctx sessionctx.Context) error {
	hasProcessPriv := hasPriv(ctx, mysql.ProcessPriv)
	if !hasProcessPriv {
		return nil
	}
	rows := txninfo.Recorder.DumpTrxSummary()
	e.rows = rows
	for _, row := range rows {
		e.recordMemoryConsume(row)
	}
	return nil
}

func (e *memtableRetriever) setDataForClusterTrxSummary(ctx sessionctx.Context) error {
	err := e.setDataForTrxSummary(ctx)
	if err != nil {
		return err
	}
	rows, err := infoschema.AppendHostInfoToRows(ctx, e.rows)
	if err != nil {
		return err
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataForMemoryUsage() error {
	r := memory.ReadMemStats()
	currentOps, sessionKillLastDatum := types.NewDatum(nil), types.NewDatum(nil)
	if memory.TriggerMemoryLimitGC.Load() || servermemorylimit.IsKilling.Load() {
		currentOps.SetString("shrink", mysql.DefaultCollationName)
	}
	sessionKillLast := servermemorylimit.SessionKillLast.Load()
	if !sessionKillLast.IsZero() {
		sessionKillLastDatum.SetMysqlTime(types.NewTime(types.FromGoTime(sessionKillLast), mysql.TypeDatetime, 0))
	}
	gcLast := types.NewTime(types.FromGoTime(memory.MemoryLimitGCLast.Load()), mysql.TypeDatetime, 0)

	row := []types.Datum{
		types.NewIntDatum(int64(memory.GetMemTotalIgnoreErr())),          // MEMORY_TOTAL
		types.NewIntDatum(int64(memory.ServerMemoryLimit.Load())),        // MEMORY_LIMIT
		types.NewIntDatum(int64(r.HeapInuse)),                            // MEMORY_CURRENT
		types.NewIntDatum(int64(servermemorylimit.MemoryMaxUsed.Load())), // MEMORY_MAX_USED
		currentOps,           // CURRENT_OPS
		sessionKillLastDatum, // SESSION_KILL_LAST
		types.NewIntDatum(servermemorylimit.SessionKillTotal.Load()), // SESSION_KILL_TOTAL
		types.NewTimeDatum(gcLast),                                   // GC_LAST
		types.NewIntDatum(memory.MemoryLimitGCTotal.Load()),          // GC_TOTAL
		types.NewDatum(GlobalDiskUsageTracker.BytesConsumed()),       // DISK_USAGE
		types.NewDatum(memory.QueryForceDisk.Load()),                 // QUERY_FORCE_DISK
	}
	e.rows = append(e.rows, row)
	e.recordMemoryConsume(row)
	return nil
}

func (e *memtableRetriever) setDataForClusterMemoryUsage(ctx sessionctx.Context) error {
	err := e.setDataForMemoryUsage()
	if err != nil {
		return err
	}
	rows, err := infoschema.AppendHostInfoToRows(ctx, e.rows)
	if err != nil {
		return err
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataForMemoryUsageOpsHistory() error {
	e.rows = servermemorylimit.GlobalMemoryOpsHistoryManager.GetRows()
	for _, row := range e.rows {
		e.recordMemoryConsume(row)
	}
	return nil
}

func (e *memtableRetriever) setDataForClusterMemoryUsageOpsHistory(ctx sessionctx.Context) error {
	err := e.setDataForMemoryUsageOpsHistory()
	if err != nil {
		return err
	}
	rows, err := infoschema.AppendHostInfoToRows(ctx, e.rows)
	if err != nil {
		return err
	}
	e.rows = rows
	return nil
}

