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

package executor

import (
	"bytes"
	"context"
	gjson "encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tidb-binlog/node"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/etcd"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/sem"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stringutil"
)

var etcdDialTimeout = 5 * time.Second

// ShowExec represents a show executor.
type ShowExec struct {
	baseExecutor

	Tp        ast.ShowStmtType // Databases/Tables/Columns/....
	DBName    model.CIStr
	Table     *ast.TableName       // Used for showing columns.
	Partition model.CIStr          // Used for showing partition
	Column    *ast.ColumnName      // Used for `desc table column`.
	IndexName model.CIStr          // Used for show table regions.
	Flag      int                  // Some flag parsed from sql, such as FULL.
	Roles     []*auth.RoleIdentity // Used for show grants.
	User      *auth.UserIdentity   // Used by show grants, show create user.
	Extractor plannercore.ShowPredicateExtractor

	is infoschema.InfoSchema

	result *chunk.Chunk
	cursor int

	Full        bool
	IfNotExists bool // Used for `show create database if not exists`
	GlobalScope bool // GlobalScope is used by show variables
	Extended    bool // Used for `show extended columns from ...`
}

// Next implements the Executor Next interface.
func (e *ShowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.result == nil {
		e.result = newFirstChunk(e)
		err := e.fetchAll(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		iter := chunk.NewIterator4Chunk(e.result)
		for colIdx := 0; colIdx < e.Schema().Len(); colIdx++ {
			retType := e.Schema().Columns[colIdx].RetType
			if !types.IsTypeVarchar(retType.GetType()) {
				continue
			}
			for row := iter.Begin(); row != iter.End(); row = iter.Next() {
				if valLen := len(row.GetString(colIdx)); retType.GetFlen() < valLen {
					retType.SetFlen(valLen)
				}
			}
		}
	}
	if e.cursor >= e.result.NumRows() {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), e.result.NumRows()-e.cursor)
	req.Append(e.result, e.cursor, e.cursor+numCurBatch)
	e.cursor += numCurBatch
	return nil
}

func (e *ShowExec) fetchAll(ctx context.Context) error {
	switch e.Tp {
	case ast.ShowCharset:
		return e.fetchShowCharset()
	case ast.ShowCollation:
		return e.fetchShowCollation()
	case ast.ShowColumns:
		return e.fetchShowColumns(ctx)
	case ast.ShowConfig:
		return e.fetchShowClusterConfigs(ctx)
	case ast.ShowCreateTable:
		return e.fetchShowCreateTable()
	case ast.ShowCreateSequence:
		return e.fetchShowCreateSequence()
	case ast.ShowCreateUser:
		return e.fetchShowCreateUser(ctx)
	case ast.ShowCreateView:
		return e.fetchShowCreateView()
	case ast.ShowCreateDatabase:
		return e.fetchShowCreateDatabase()
	case ast.ShowCreatePlacementPolicy:
		return e.fetchShowCreatePlacementPolicy()
	case ast.ShowDatabases:
		return e.fetchShowDatabases()
	case ast.ShowDrainerStatus:
		return e.fetchShowPumpOrDrainerStatus(node.DrainerNode)
	case ast.ShowEngines:
		return e.fetchShowEngines(ctx)
	case ast.ShowGrants:
		return e.fetchShowGrants()
	case ast.ShowIndex:
		return e.fetchShowIndex()
	case ast.ShowProcedureStatus:
		return e.fetchShowProcedureStatus()
	case ast.ShowPumpStatus:
		return e.fetchShowPumpOrDrainerStatus(node.PumpNode)
	case ast.ShowStatus:
		return e.fetchShowStatus()
	case ast.ShowTables:
		return e.fetchShowTables()
	case ast.ShowOpenTables:
		return e.fetchShowOpenTables()
	case ast.ShowTableStatus:
		return e.fetchShowTableStatus(ctx)
	case ast.ShowTriggers:
		return e.fetchShowTriggers()
	case ast.ShowVariables:
		return e.fetchShowVariables()
	case ast.ShowWarnings:
		return e.fetchShowWarnings(false)
	case ast.ShowErrors:
		return e.fetchShowWarnings(true)
	case ast.ShowProcessList:
		return e.fetchShowProcessList()
	case ast.ShowEvents:
		// empty result
	case ast.ShowStatsExtended:
		return e.fetchShowStatsExtended()
	case ast.ShowStatsMeta:
		return e.fetchShowStatsMeta()
	case ast.ShowStatsHistograms:
		return e.fetchShowStatsHistogram()
	case ast.ShowStatsBuckets:
		return e.fetchShowStatsBuckets()
	case ast.ShowStatsTopN:
		return e.fetchShowStatsTopN()
	case ast.ShowStatsHealthy:
		e.fetchShowStatsHealthy()
		return nil
	case ast.ShowHistogramsInFlight:
		e.fetchShowHistogramsInFlight()
		return nil
	case ast.ShowColumnStatsUsage:
		return e.fetchShowColumnStatsUsage()
	case ast.ShowPlugins:
		return e.fetchShowPlugins()
	case ast.ShowProfiles:
		// empty result
	case ast.ShowMasterStatus:
		return e.fetchShowMasterStatus()
	case ast.ShowPrivileges:
		return e.fetchShowPrivileges()
	case ast.ShowBindings:
		return e.fetchShowBind()
	case ast.ShowBindingCacheStatus:
		return e.fetchShowBindingCacheStatus(ctx)
	case ast.ShowAnalyzeStatus:
		return e.fetchShowAnalyzeStatus()
	case ast.ShowRegions:
		return e.fetchShowTableRegions()
	case ast.ShowBuiltins:
		return e.fetchShowBuiltins()
	case ast.ShowBackups:
		return e.fetchShowBRIE(ast.BRIEKindBackup)
	case ast.ShowRestores:
		return e.fetchShowBRIE(ast.BRIEKindRestore)
	case ast.ShowPlacementLabels:
		return e.fetchShowPlacementLabels(ctx)
	case ast.ShowPlacement:
		return e.fetchShowPlacement(ctx)
	case ast.ShowPlacementForDatabase:
		return e.fetchShowPlacementForDB(ctx)
	case ast.ShowPlacementForTable:
		return e.fetchShowPlacementForTable(ctx)
	case ast.ShowPlacementForPartition:
		return e.fetchShowPlacementForPartition(ctx)
	}
	return nil
}

// visibleChecker checks if a stmt is visible for a certain user.
type visibleChecker struct {
	defaultDB string
	ctx       sessionctx.Context
	is        infoschema.InfoSchema
	manager   privilege.Manager
	ok        bool
}

func (v *visibleChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch x := in.(type) {
	case *ast.TableName:
		schema := x.Schema.L
		if schema == "" {
			schema = v.defaultDB
		}
		if !v.is.TableExists(model.NewCIStr(schema), x.Name) {
			return in, true
		}
		activeRoles := v.ctx.GetSessionVars().ActiveRoles
		if v.manager != nil && !v.manager.RequestVerification(activeRoles, schema, x.Name.L, "", mysql.SelectPriv) {
			v.ok = false
		}
		return in, true
	}
	return in, false
}

func (v *visibleChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (e *ShowExec) fetchShowBind() error {
	var bindRecords []*bindinfo.BindRecord
	if !e.GlobalScope {
		handle := e.ctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
		bindRecords = handle.GetAllBindRecord()
	} else {
		bindRecords = domain.GetDomain(e.ctx).BindHandle().GetAllBindRecord()
	}
	// Remove the invalid bindRecord.
	ind := 0
	for _, bindData := range bindRecords {
		if len(bindData.Bindings) > 0 {
			bindRecords[ind] = bindData
			ind++
		}
	}
	bindRecords = bindRecords[:ind]
	parser := parser.New()
	for _, bindData := range bindRecords {
		// For the same origin_sql, sort the bindings according to their update time.
		sort.Slice(bindData.Bindings, func(i int, j int) bool {
			cmpResult := bindData.Bindings[i].UpdateTime.Compare(bindData.Bindings[j].UpdateTime)
			if cmpResult == 0 {
				// Because the create time must be different, the result of sorting is stable.
				cmpResult = bindData.Bindings[i].CreateTime.Compare(bindData.Bindings[j].CreateTime)
			}
			return cmpResult > 0
		})
	}
	// For the different origin_sql, sort the bindRecords according to their max update time.
	sort.Slice(bindRecords, func(i int, j int) bool {
		cmpResult := bindRecords[i].Bindings[0].UpdateTime.Compare(bindRecords[j].Bindings[0].UpdateTime)
		if cmpResult == 0 {
			// Because the create time must be different, the result of sorting is stable.
			cmpResult = bindRecords[i].Bindings[0].CreateTime.Compare(bindRecords[j].Bindings[0].CreateTime)
		}
		return cmpResult > 0
	})
	for _, bindData := range bindRecords {
		for _, hint := range bindData.Bindings {
			stmt, err := parser.ParseOneStmt(hint.BindSQL, hint.Charset, hint.Collation)
			if err != nil {
				return err
			}
			checker := visibleChecker{
				defaultDB: bindData.Db,
				ctx:       e.ctx,
				is:        e.is,
				manager:   privilege.GetPrivilegeManager(e.ctx),
				ok:        true,
			}
			stmt.Accept(&checker)
			if !checker.ok {
				continue
			}
			e.appendRow([]interface{}{
				bindData.OriginalSQL,
				hint.BindSQL,
				bindData.Db,
				hint.Status,
				hint.CreateTime,
				hint.UpdateTime,
				hint.Charset,
				hint.Collation,
				hint.Source,
			})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowBindingCacheStatus(ctx context.Context) error {
	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)

	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, fmt.Sprintf("SELECT count(*) FROM mysql.bind_info where status = '%s' or status = '%s';", bindinfo.Enabled, bindinfo.Using))
	if err != nil {
		return errors.Trace(err)
	}

	handle := domain.GetDomain(e.ctx).BindHandle()

	bindRecords := handle.GetAllBindRecord()
	numBindings := 0
	for _, bindRecord := range bindRecords {
		for _, binding := range bindRecord.Bindings {
			if binding.IsBindingEnabled() {
				numBindings++
			}
		}
	}

	memUsage := handle.GetMemUsage()
	memCapacity := handle.GetMemCapacity()
	e.appendRow([]interface{}{
		numBindings,
		rows[0].GetInt64(0),
		memory.FormatBytes(memUsage),
		memory.FormatBytes(memCapacity),
	})
	return nil
}

func (e *ShowExec) fetchShowEngines(ctx context.Context) error {
	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)

	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, `SELECT * FROM information_schema.engines`)
	if err != nil {
		return errors.Trace(err)
	}

	e.result.AppendRows(rows)
	return nil
}

// moveInfoSchemaToFront moves information_schema to the first, and the others are sorted in the origin ascending order.
func moveInfoSchemaToFront(dbs []string) {
	if len(dbs) > 0 && strings.EqualFold(dbs[0], "INFORMATION_SCHEMA") {
		return
	}

	i := sort.SearchStrings(dbs, "INFORMATION_SCHEMA")
	if i < len(dbs) && strings.EqualFold(dbs[i], "INFORMATION_SCHEMA") {
		copy(dbs[1:i+1], dbs[0:i])
		dbs[0] = "INFORMATION_SCHEMA"
	}
}

func (e *ShowExec) fetchShowDatabases() error {
	dbs := e.is.AllSchemaNames()
	checker := privilege.GetPrivilegeManager(e.ctx)
	sort.Strings(dbs)
	// let information_schema be the first database
	moveInfoSchemaToFront(dbs)
	for _, d := range dbs {
		if checker != nil && !checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, d) {
			continue
		}
		e.appendRow([]interface{}{
			d,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowProcessList() error {
	sm := e.ctx.GetSessionManager()
	if sm == nil {
		return nil
	}

	loginUser, activeRoles := e.ctx.GetSessionVars().User, e.ctx.GetSessionVars().ActiveRoles
	var hasProcessPriv bool
	if pm := privilege.GetPrivilegeManager(e.ctx); pm != nil {
		if pm.RequestVerification(activeRoles, "", "", "", mysql.ProcessPriv) {
			hasProcessPriv = true
		}
	}

	pl := sm.ShowProcessList()
	for _, pi := range pl {
		// If you have the PROCESS privilege, you can see all threads.
		// Otherwise, you can see only your own threads.
		if !hasProcessPriv && pi.User != loginUser.Username {
			continue
		}
		row := pi.ToRowForShow(e.Full)
		e.appendRow(row)
	}
	return nil
}

func (e *ShowExec) fetchShowOpenTables() error {
	// TiDB has no concept like mysql's "table cache" and "open table"
	// For simplicity, we just return an empty result with the same structure as MySQL's SHOW OPEN TABLES
	return nil
}

func (e *ShowExec) fetchShowTables() error {
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, e.DBName.O) {
			return e.dbAccessDenied()
		}
	}
	if !e.is.SchemaExists(e.DBName) {
		return ErrBadDB.GenWithStackByArgs(e.DBName)
	}
	// sort for tables
	schemaTables := e.is.SchemaTables(e.DBName)
	tableNames := make([]string, 0, len(schemaTables))
	activeRoles := e.ctx.GetSessionVars().ActiveRoles
	var (
		tableTypes        = make(map[string]string)
		fieldPatternsLike collate.WildcardPattern
		FieldFilterEnable bool
		fieldFilter       string
	)
	if e.Extractor != nil {
		extractor := (e.Extractor).(*plannercore.ShowTablesTableExtractor)
		if extractor.FieldPatterns != "" {
			fieldPatternsLike = collate.GetCollatorByID(collate.CollationName2ID(mysql.UTF8MB4DefaultCollation)).Pattern()
			fieldPatternsLike.Compile(extractor.FieldPatterns, byte('\\'))
		}
		FieldFilterEnable = extractor.Field != ""
		fieldFilter = extractor.Field
	}
	for _, v := range schemaTables {
		// Test with mysql.AllPrivMask means any privilege would be OK.
		// TODO: Should consider column privileges, which also make a table visible.
		if checker != nil && !checker.RequestVerification(activeRoles, e.DBName.O, v.Meta().Name.O, "", mysql.AllPrivMask) {
			continue
		} else if FieldFilterEnable && v.Meta().Name.L != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(v.Meta().Name.L) {
			continue
		}
		tableNames = append(tableNames, v.Meta().Name.O)
		if v.Meta().IsView() {
			tableTypes[v.Meta().Name.O] = "VIEW"
		} else if v.Meta().IsSequence() {
			tableTypes[v.Meta().Name.O] = "SEQUENCE"
		} else if util.IsSystemView(e.DBName.L) {
			tableTypes[v.Meta().Name.O] = "SYSTEM VIEW"
		} else {
			tableTypes[v.Meta().Name.O] = "BASE TABLE"
		}
	}
	sort.Strings(tableNames)
	for _, v := range tableNames {
		if e.Full {
			e.appendRow([]interface{}{v, tableTypes[v]})
		} else {
			e.appendRow([]interface{}{v})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowTableStatus(ctx context.Context) error {
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, e.DBName.O) {
			return e.dbAccessDenied()
		}
	}
	if !e.is.SchemaExists(e.DBName) {
		return ErrBadDB.GenWithStackByArgs(e.DBName)
	}

	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)

	var snapshot uint64
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return errors.Trace(err)
	}
	if txn.Valid() {
		snapshot = txn.StartTS()
	}
	if e.ctx.GetSessionVars().SnapshotTS != 0 {
		snapshot = e.ctx.GetSessionVars().SnapshotTS
	}

	rows, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionWithSnapshot(snapshot), sqlexec.ExecOptionUseCurSession},
		`SELECT table_name, engine, version, row_format, table_rows,
		avg_row_length, data_length, max_data_length, index_length,
		data_free, auto_increment, create_time, update_time, check_time,
		table_collation, IFNULL(checksum,''), create_options, table_comment
		FROM information_schema.tables
		WHERE lower(table_schema)=%? ORDER BY table_name`, e.DBName.L)
	if err != nil {
		return errors.Trace(err)
	}

	activeRoles := e.ctx.GetSessionVars().ActiveRoles
	for _, row := range rows {
		if checker != nil && !checker.RequestVerification(activeRoles, e.DBName.O, row.GetString(0), "", mysql.AllPrivMask) {
			continue
		}
		e.result.AppendRow(row)

	}
	return nil
}

func (e *ShowExec) fetchShowColumns(ctx context.Context) error {
	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}
	var (
		fieldPatternsLike collate.WildcardPattern
		FieldFilterEnable bool
		fieldFilter       string
	)
	if e.Extractor != nil {
		extractor := (e.Extractor).(*plannercore.ShowColumnsTableExtractor)
		if extractor.FieldPatterns != "" {
			fieldPatternsLike = collate.GetCollatorByID(collate.CollationName2ID(mysql.UTF8MB4DefaultCollation)).Pattern()
			fieldPatternsLike.Compile(extractor.FieldPatterns, byte('\\'))
		}
		FieldFilterEnable = extractor.Field != ""
		fieldFilter = extractor.Field
	}

	checker := privilege.GetPrivilegeManager(e.ctx)
	activeRoles := e.ctx.GetSessionVars().ActiveRoles
	if checker != nil && e.ctx.GetSessionVars().User != nil && !checker.RequestVerification(activeRoles, e.DBName.O, tb.Meta().Name.O, "", mysql.InsertPriv|mysql.SelectPriv|mysql.UpdatePriv|mysql.ReferencesPriv) {
		return e.tableAccessDenied("SELECT", tb.Meta().Name.O)
	}

	var cols []*table.Column
	// The optional EXTENDED keyword causes the output to include information about hidden columns that MySQL uses internally and are not accessible by users.
	// See https://dev.mysql.com/doc/refman/8.0/en/show-columns.html
	if e.Extended {
		cols = tb.Cols()
	} else {
		cols = tb.VisibleCols()
	}
	if err := tryFillViewColumnType(ctx, e.ctx, e.is, e.DBName, tb.Meta()); err != nil {
		return err
	}
	for _, col := range cols {
		if FieldFilterEnable && col.Name.L != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(col.Name.L) {
			continue
		}
		desc := table.NewColDesc(col)
		var columnDefault interface{}
		if desc.DefaultValue != nil {
			// SHOW COLUMNS result expects string value
			defaultValStr := fmt.Sprintf("%v", desc.DefaultValue)
			// If column is timestamp, and default value is not current_timestamp, should convert the default value to the current session time zone.
			if col.GetType() == mysql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr && !strings.HasPrefix(strings.ToUpper(defaultValStr), strings.ToUpper(ast.CurrentTimestamp)) {
				timeValue, err := table.GetColDefaultValue(e.ctx, col.ToInfo())
				if err != nil {
					return errors.Trace(err)
				}
				defaultValStr = timeValue.GetMysqlTime().String()
			}
			if col.GetType() == mysql.TypeBit {
				defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
				columnDefault = defaultValBinaryLiteral.ToBitLiteralString(true)
			} else {
				columnDefault = defaultValStr
			}
		}

		// The FULL keyword causes the output to include the column collation and comments,
		// as well as the privileges you have for each column.
		if e.Full {
			e.appendRow([]interface{}{
				desc.Field,
				desc.Type,
				desc.Collation,
				desc.Null,
				desc.Key,
				columnDefault,
				desc.Extra,
				desc.Privileges,
				desc.Comment,
			})
		} else {
			e.appendRow([]interface{}{
				desc.Field,
				desc.Type,
				desc.Null,
				desc.Key,
				columnDefault,
				desc.Extra,
			})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowIndex() error {
	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	checker := privilege.GetPrivilegeManager(e.ctx)
	activeRoles := e.ctx.GetSessionVars().ActiveRoles
	if checker != nil && e.ctx.GetSessionVars().User != nil && !checker.RequestVerification(activeRoles, e.DBName.O, tb.Meta().Name.O, "", mysql.AllPrivMask) {
		return e.tableAccessDenied("SELECT", tb.Meta().Name.O)
	}

	if tb.Meta().PKIsHandle {
		var pkCol *table.Column
		for _, col := range tb.Cols() {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				pkCol = col
				break
			}
		}
		e.appendRow([]interface{}{
			tb.Meta().Name.O, // Table
			0,                // Non_unique
			"PRIMARY",        // Key_name
			1,                // Seq_in_index
			pkCol.Name.O,     // Column_name
			"A",              // Collation
			0,                // Cardinality
			nil,              // Sub_part
			nil,              // Packed
			"",               // Null
			"BTREE",          // Index_type
			"",               // Comment
			"",               // Index_comment
			"YES",            // Index_visible
			nil,              // Expression
			"YES",            // Clustered
		})
	}
	for _, idx := range tb.Indices() {
		idxInfo := idx.Meta()
		if idxInfo.State != model.StatePublic {
			continue
		}
		isClustered := "NO"
		if tb.Meta().IsCommonHandle && idxInfo.Primary {
			isClustered = "YES"
		}
		for i, col := range idxInfo.Columns {
			nonUniq := 1
			if idx.Meta().Unique {
				nonUniq = 0
			}

			var subPart interface{}
			if col.Length != types.UnspecifiedLength {
				subPart = col.Length
			}

			tblCol := tb.Meta().Columns[col.Offset]
			nullVal := "YES"
			if mysql.HasNotNullFlag(tblCol.GetFlag()) {
				nullVal = ""
			}

			visible := "YES"
			if idx.Meta().Invisible {
				visible = "NO"
			}

			colName := col.Name.O
			var expression interface{}
			if tblCol.Hidden {
				colName = "NULL"
				expression = tblCol.GeneratedExprString
			}

			e.appendRow([]interface{}{
				tb.Meta().Name.O,       // Table
				nonUniq,                // Non_unique
				idx.Meta().Name.O,      // Key_name
				i + 1,                  // Seq_in_index
				colName,                // Column_name
				"A",                    // Collation
				0,                      // Cardinality
				subPart,                // Sub_part
				nil,                    // Packed
				nullVal,                // Null
				idx.Meta().Tp.String(), // Index_type
				"",                     // Comment
				idx.Meta().Comment,     // Index_comment
				visible,                // Index_visible
				expression,             // Expression
				isClustered,            // Clustered
			})
		}
	}
	return nil
}

// fetchShowCharset gets all charset information and fill them into e.rows.
// See http://dev.mysql.com/doc/refman/5.7/en/show-character-set.html
func (e *ShowExec) fetchShowCharset() error {
	descs := charset.GetSupportedCharsets()
	for _, desc := range descs {
		e.appendRow([]interface{}{
			desc.Name,
			desc.Desc,
			desc.DefaultCollation,
			desc.Maxlen,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowMasterStatus() error {
	tso := e.ctx.GetSessionVars().TxnCtx.StartTS
	e.appendRow([]interface{}{"tidb-binlog", tso, "", "", ""})
	return nil
}

func (e *ShowExec) sysVarHiddenForSem(sysVarNameInLower string) bool {
	if !sem.IsEnabled() || !sem.IsInvisibleSysVar(sysVarNameInLower) {
		return false
	}
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil || checker.RequestDynamicVerification(e.ctx.GetSessionVars().ActiveRoles, "RESTRICTED_VARIABLES_ADMIN", false) {
		return false
	}
	return true
}

func (e *ShowExec) fetchShowVariables() (err error) {
	var (
		value       string
		sessionVars = e.ctx.GetSessionVars()
	)
	var (
		fieldPatternsLike collate.WildcardPattern
		FieldFilterEnable bool
		fieldFilter       string
	)
	if e.Extractor != nil {
		extractor := (e.Extractor).(*plannercore.ShowVariablesExtractor)
		if extractor.FieldPatterns != "" {
			fieldPatternsLike = collate.GetCollatorByID(collate.CollationName2ID(mysql.UTF8MB4DefaultCollation)).Pattern()
			fieldPatternsLike.Compile(extractor.FieldPatterns, byte('\\'))
		}
		FieldFilterEnable = extractor.Field != ""
		fieldFilter = extractor.Field
	}
	if e.GlobalScope {
		// Collect global scope variables,
		// 1. Exclude the variables of ScopeSession in variable.SysVars;
		// 2. If the variable is ScopeNone, it's a read-only variable, return the default value of it,
		// 		otherwise, fetch the value from table `mysql.Global_Variables`.
		for _, v := range variable.GetSysVars() {
			if v.Scope != variable.ScopeSession {
				if FieldFilterEnable && v.Name != fieldFilter {
					continue
				} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(v.Name) {
					continue
				}
				if v.Hidden || e.sysVarHiddenForSem(v.Name) {
					continue
				}
				value, err = variable.GetGlobalSystemVar(sessionVars, v.Name)
				if err != nil {
					return errors.Trace(err)
				}
				e.appendRow([]interface{}{v.Name, value})
			}
		}
		return nil
	}

	// Collect session scope variables,
	// If it is a session only variable, use the default value defined in code,
	//   otherwise, fetch the value from table `mysql.Global_Variables`.
	for _, v := range variable.GetSysVars() {
		if FieldFilterEnable && v.Name != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(v.Name) {
			continue
		}
		if v.Hidden || e.sysVarHiddenForSem(v.Name) {
			continue
		}
		value, err = variable.GetSessionOrGlobalSystemVar(sessionVars, v.Name)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendRow([]interface{}{v.Name, value})
	}
	return nil
}

func (e *ShowExec) fetchShowStatus() error {
	sessionVars := e.ctx.GetSessionVars()
	statusVars, err := variable.GetStatusVars(sessionVars)
	if err != nil {
		return errors.Trace(err)
	}
	checker := privilege.GetPrivilegeManager(e.ctx)
	for status, v := range statusVars {
		if e.GlobalScope && v.Scope == variable.ScopeSession {
			continue
		}
		// Skip invisible status vars if permission fails.
		if sem.IsEnabled() && sem.IsInvisibleStatusVar(status) {
			if checker == nil || !checker.RequestDynamicVerification(sessionVars.ActiveRoles, "RESTRICTED_STATUS_ADMIN", false) {
				continue
			}
		}
		switch v.Value.(type) {
		case []interface{}, nil:
			v.Value = fmt.Sprintf("%v", v.Value)
		}
		value, err := types.ToString(v.Value)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendRow([]interface{}{status, value})
	}
	return nil
}

func getDefaultCollate(charsetName string) string {
	ch, err := charset.GetCharsetInfo(charsetName)
	if err != nil {
		// The charset is invalid, return server default.
		return mysql.DefaultCollationName
	}
	return ch.DefaultCollation
}

// ConstructResultOfShowCreateTable constructs the result for show create table.
func ConstructResultOfShowCreateTable(ctx sessionctx.Context, tableInfo *model.TableInfo, allocators autoid.Allocators, buf *bytes.Buffer) (err error) {
	if tableInfo.IsView() {
		fetchShowCreateTable4View(ctx, tableInfo, buf)
		return nil
	}
	if tableInfo.IsSequence() {
		ConstructResultOfShowCreateSequence(ctx, tableInfo, buf)
		return nil
	}

	tblCharset := tableInfo.Charset
	if len(tblCharset) == 0 {
		tblCharset = mysql.DefaultCharset
	}
	tblCollate := tableInfo.Collate
	// Set default collate if collate is not specified.
	if len(tblCollate) == 0 {
		tblCollate = getDefaultCollate(tblCharset)
	}

	sqlMode := ctx.GetSessionVars().SQLMode
	tableName := stringutil.Escape(tableInfo.Name.O, sqlMode)
	switch tableInfo.TempTableType {
	case model.TempTableGlobal:
		fmt.Fprintf(buf, "CREATE GLOBAL TEMPORARY TABLE %s (\n", tableName)
	case model.TempTableLocal:
		fmt.Fprintf(buf, "CREATE TEMPORARY TABLE %s (\n", tableName)
	default:
		fmt.Fprintf(buf, "CREATE TABLE %s (\n", tableName)
	}
	var pkCol *model.ColumnInfo
	var hasAutoIncID bool
	needAddComma := false
	for i, col := range tableInfo.Cols() {
		if col.Hidden {
			continue
		}
		if needAddComma {
			buf.WriteString(",\n")
		}
		fmt.Fprintf(buf, "  %s %s", stringutil.Escape(col.Name.O, sqlMode), col.GetTypeDesc())
		if col.GetCharset() != "binary" {
			if col.GetCharset() != tblCharset {
				fmt.Fprintf(buf, " CHARACTER SET %s", col.GetCharset())
			}
			if col.GetCollate() != tblCollate {
				fmt.Fprintf(buf, " COLLATE %s", col.GetCollate())
			} else {
				defcol, err := charset.GetDefaultCollation(col.GetCharset())
				if err == nil && defcol != col.GetCollate() {
					fmt.Fprintf(buf, " COLLATE %s", col.GetCollate())
				}
			}
		}
		if col.IsGenerated() {
			// It's a generated column.
			fmt.Fprintf(buf, " GENERATED ALWAYS AS (%s)", col.GeneratedExprString)
			if col.GeneratedStored {
				buf.WriteString(" STORED")
			} else {
				buf.WriteString(" VIRTUAL")
			}
		}
		if mysql.HasAutoIncrementFlag(col.GetFlag()) {
			hasAutoIncID = true
			buf.WriteString(" NOT NULL AUTO_INCREMENT")
		} else {
			if mysql.HasNotNullFlag(col.GetFlag()) {
				buf.WriteString(" NOT NULL")
			}
			// default values are not shown for generated columns in MySQL
			if !mysql.HasNoDefaultValueFlag(col.GetFlag()) && !col.IsGenerated() {
				defaultValue := col.GetDefaultValue()
				switch defaultValue {
				case nil:
					if !mysql.HasNotNullFlag(col.GetFlag()) {
						if col.GetType() == mysql.TypeTimestamp {
							buf.WriteString(" NULL")
						}
						buf.WriteString(" DEFAULT NULL")
					}
				case "CURRENT_TIMESTAMP":
					buf.WriteString(" DEFAULT CURRENT_TIMESTAMP")
					if col.GetDecimal() > 0 {
						buf.WriteString(fmt.Sprintf("(%d)", col.GetDecimal()))
					}
				default:
					defaultValStr := fmt.Sprintf("%v", defaultValue)
					// If column is timestamp, and default value is not current_timestamp, should convert the default value to the current session time zone.
					if col.GetType() == mysql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr {
						timeValue, err := table.GetColDefaultValue(ctx, col)
						if err != nil {
							return errors.Trace(err)
						}
						defaultValStr = timeValue.GetMysqlTime().String()
					}

					if col.GetType() == mysql.TypeBit {
						defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
						fmt.Fprintf(buf, " DEFAULT %s", defaultValBinaryLiteral.ToBitLiteralString(true))
					} else if col.DefaultIsExpr {
						fmt.Fprintf(buf, " DEFAULT %s", format.OutputFormat(defaultValStr))
					} else {
						fmt.Fprintf(buf, " DEFAULT '%s'", format.OutputFormat(defaultValStr))
					}
				}
			}
			if mysql.HasOnUpdateNowFlag(col.GetFlag()) {
				buf.WriteString(" ON UPDATE CURRENT_TIMESTAMP")
				buf.WriteString(table.OptionalFsp(&col.FieldType))
			}
		}
		if ddl.IsAutoRandomColumnID(tableInfo, col.ID) {
			buf.WriteString(fmt.Sprintf(" /*T![auto_rand] AUTO_RANDOM(%d) */", tableInfo.AutoRandomBits))
		}
		if len(col.Comment) > 0 {
			buf.WriteString(fmt.Sprintf(" COMMENT '%s'", format.OutputFormat(col.Comment)))
		}
		if i != len(tableInfo.Cols())-1 {
			needAddComma = true
		}
		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()) {
			pkCol = col
		}
	}

	if pkCol != nil {
		// If PKIsHandle, pk info is not in tb.Indices(). We should handle it here.
		buf.WriteString(",\n")
		fmt.Fprintf(buf, "  PRIMARY KEY (%s)", stringutil.Escape(pkCol.Name.O, sqlMode))
		buf.WriteString(" /*T![clustered_index] CLUSTERED */")
	}

	publicIndices := make([]*model.IndexInfo, 0, len(tableInfo.Indices))
	for _, idx := range tableInfo.Indices {
		if idx.State == model.StatePublic {
			publicIndices = append(publicIndices, idx)
		}
	}
	if len(publicIndices) > 0 {
		buf.WriteString(",\n")
	}

	for i, idxInfo := range publicIndices {
		if idxInfo.Primary {
			buf.WriteString("  PRIMARY KEY ")
		} else if idxInfo.Unique {
			fmt.Fprintf(buf, "  UNIQUE KEY %s ", stringutil.Escape(idxInfo.Name.O, sqlMode))
		} else {
			fmt.Fprintf(buf, "  KEY %s ", stringutil.Escape(idxInfo.Name.O, sqlMode))
		}

		cols := make([]string, 0, len(idxInfo.Columns))
		var colInfo string
		for _, c := range idxInfo.Columns {
			if tableInfo.Columns[c.Offset].Hidden {
				colInfo = fmt.Sprintf("(%s)", tableInfo.Columns[c.Offset].GeneratedExprString)
			} else {
				colInfo = stringutil.Escape(c.Name.O, sqlMode)
				if c.Length != types.UnspecifiedLength {
					colInfo = fmt.Sprintf("%s(%s)", colInfo, strconv.Itoa(c.Length))
				}
			}
			cols = append(cols, colInfo)
		}
		fmt.Fprintf(buf, "(%s)", strings.Join(cols, ","))
		if idxInfo.Invisible {
			fmt.Fprintf(buf, ` /*!80000 INVISIBLE */`)
		}
		if idxInfo.Comment != "" {
			fmt.Fprintf(buf, ` COMMENT '%s'`, format.OutputFormat(idxInfo.Comment))
		}
		if idxInfo.Primary {
			if tableInfo.HasClusteredIndex() {
				buf.WriteString(" /*T![clustered_index] CLUSTERED */")
			} else {
				buf.WriteString(" /*T![clustered_index] NONCLUSTERED */")
			}
		}
		if i != len(publicIndices)-1 {
			buf.WriteString(",\n")
		}
	}

	// Foreign Keys are supported by data dictionary even though
	// they are not enforced by DDL. This is still helpful to applications.
	for _, fk := range tableInfo.ForeignKeys {
		buf.WriteString(fmt.Sprintf(",\n  CONSTRAINT %s FOREIGN KEY ", stringutil.Escape(fk.Name.O, sqlMode)))
		colNames := make([]string, 0, len(fk.Cols))
		for _, col := range fk.Cols {
			colNames = append(colNames, stringutil.Escape(col.O, sqlMode))
		}
		buf.WriteString(fmt.Sprintf("(%s)", strings.Join(colNames, ",")))
		buf.WriteString(fmt.Sprintf(" REFERENCES %s ", stringutil.Escape(fk.RefTable.O, sqlMode)))
		refColNames := make([]string, 0, len(fk.Cols))
		for _, refCol := range fk.RefCols {
			refColNames = append(refColNames, stringutil.Escape(refCol.O, sqlMode))
		}
		buf.WriteString(fmt.Sprintf("(%s)", strings.Join(refColNames, ",")))
		if ast.ReferOptionType(fk.OnDelete) != 0 {
			buf.WriteString(fmt.Sprintf(" ON DELETE %s", ast.ReferOptionType(fk.OnDelete).String()))
		}
		if ast.ReferOptionType(fk.OnUpdate) != 0 {
			buf.WriteString(fmt.Sprintf(" ON UPDATE %s", ast.ReferOptionType(fk.OnUpdate).String()))
		}
	}

	buf.WriteString("\n")

	buf.WriteString(") ENGINE=InnoDB")
	// We need to explicitly set the default charset and collation
	// to make it work on MySQL server which has default collate utf8_general_ci.
	if len(tblCollate) == 0 || tblCollate == "binary" {
		// If we can not find default collate for the given charset,
		// or the collate is 'binary'(MySQL-5.7 compatibility, see #15633 for details),
		// do not show the collate part.
		fmt.Fprintf(buf, " DEFAULT CHARSET=%s", tblCharset)
	} else {
		fmt.Fprintf(buf, " DEFAULT CHARSET=%s COLLATE=%s", tblCharset, tblCollate)
	}

	// Displayed if the compression typed is set.
	if len(tableInfo.Compression) != 0 {
		fmt.Fprintf(buf, " COMPRESSION='%s'", tableInfo.Compression)
	}

	incrementAllocator := allocators.Get(autoid.RowIDAllocType)
	if hasAutoIncID && incrementAllocator != nil {
		autoIncID, err := incrementAllocator.NextGlobalAutoID()
		if err != nil {
			return errors.Trace(err)
		}

		// It's compatible with MySQL.
		if autoIncID > 1 {
			fmt.Fprintf(buf, " AUTO_INCREMENT=%d", autoIncID)
		}
	}

	if tableInfo.AutoIdCache != 0 {
		fmt.Fprintf(buf, " /*T![auto_id_cache] AUTO_ID_CACHE=%d */", tableInfo.AutoIdCache)
	}

	randomAllocator := allocators.Get(autoid.AutoRandomType)
	if randomAllocator != nil {
		autoRandID, err := randomAllocator.NextGlobalAutoID()
		if err != nil {
			return errors.Trace(err)
		}

		if autoRandID > 1 {
			fmt.Fprintf(buf, " /*T![auto_rand_base] AUTO_RANDOM_BASE=%d */", autoRandID)
		}
	}

	if tableInfo.ShardRowIDBits > 0 {
		fmt.Fprintf(buf, " /*T! SHARD_ROW_ID_BITS=%d ", tableInfo.ShardRowIDBits)
		if tableInfo.PreSplitRegions > 0 {
			fmt.Fprintf(buf, "PRE_SPLIT_REGIONS=%d ", tableInfo.PreSplitRegions)
		}
		buf.WriteString("*/")
	}

	if len(tableInfo.Comment) > 0 {
		fmt.Fprintf(buf, " COMMENT='%s'", format.OutputFormat(tableInfo.Comment))
	}

	if tableInfo.TempTableType == model.TempTableGlobal {
		fmt.Fprintf(buf, " ON COMMIT DELETE ROWS")
	}

	if tableInfo.PlacementPolicyRef != nil {
		fmt.Fprintf(buf, " /*T![placement] PLACEMENT POLICY=%s */", stringutil.Escape(tableInfo.PlacementPolicyRef.Name.String(), sqlMode))
	}

	if tableInfo.TableCacheStatusType == model.TableCacheStatusEnable {
		// This is not meant to be understand by other components, so it's not written as /*T![cached] */
		// For all external components, cached table is just a normal table.
		fmt.Fprintf(buf, " /* CACHED ON */")
	}

	// add partition info here.
	appendPartitionInfo(tableInfo.Partition, buf, sqlMode)
	return nil
}

// ConstructResultOfShowCreateSequence constructs the result for show create sequence.
func ConstructResultOfShowCreateSequence(ctx sessionctx.Context, tableInfo *model.TableInfo, buf *bytes.Buffer) {
	sqlMode := ctx.GetSessionVars().SQLMode
	fmt.Fprintf(buf, "CREATE SEQUENCE %s ", stringutil.Escape(tableInfo.Name.O, sqlMode))
	sequenceInfo := tableInfo.Sequence
	fmt.Fprintf(buf, "start with %d ", sequenceInfo.Start)
	fmt.Fprintf(buf, "minvalue %d ", sequenceInfo.MinValue)
	fmt.Fprintf(buf, "maxvalue %d ", sequenceInfo.MaxValue)
	fmt.Fprintf(buf, "increment by %d ", sequenceInfo.Increment)
	if sequenceInfo.Cache {
		fmt.Fprintf(buf, "cache %d ", sequenceInfo.CacheValue)
	} else {
		buf.WriteString("nocache ")
	}
	if sequenceInfo.Cycle {
		buf.WriteString("cycle ")
	} else {
		buf.WriteString("nocycle ")
	}
	buf.WriteString("ENGINE=InnoDB")
	if len(sequenceInfo.Comment) > 0 {
		fmt.Fprintf(buf, " COMMENT='%s'", format.OutputFormat(sequenceInfo.Comment))
	}
}

func (e *ShowExec) fetchShowCreateSequence() error {
	tbl, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}
	tableInfo := tbl.Meta()
	if !tableInfo.IsSequence() {
		return ErrWrongObject.GenWithStackByArgs(e.DBName.O, tableInfo.Name.O, "SEQUENCE")
	}
	var buf bytes.Buffer
	ConstructResultOfShowCreateSequence(e.ctx, tableInfo, &buf)
	e.appendRow([]interface{}{tableInfo.Name.O, buf.String()})
	return nil
}

// TestShowClusterConfigKey is the key used to store TestShowClusterConfigFunc.
var TestShowClusterConfigKey stringutil.StringerStr = "TestShowClusterConfigKey"

// TestShowClusterConfigFunc is used to test 'show config ...'.
type TestShowClusterConfigFunc func() ([][]types.Datum, error)

func (e *ShowExec) fetchShowClusterConfigs(ctx context.Context) error {
	emptySet := set.NewStringSet()
	var confItems [][]types.Datum
	var err error
	if f := e.ctx.Value(TestShowClusterConfigKey); f != nil {
		confItems, err = f.(TestShowClusterConfigFunc)()
	} else {
		confItems, err = fetchClusterConfig(e.ctx, emptySet, emptySet)
	}
	if err != nil {
		return err
	}
	for _, items := range confItems {
		row := make([]interface{}, 0, 4)
		for _, item := range items {
			row = append(row, item.GetString())
		}
		e.appendRow(row)
	}
	return nil
}

func (e *ShowExec) fetchShowCreateTable() error {
	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	tableInfo := tb.Meta()
	var buf bytes.Buffer
	// TODO: let the result more like MySQL.
	if err = ConstructResultOfShowCreateTable(e.ctx, tableInfo, tb.Allocators(e.ctx), &buf); err != nil {
		return err
	}
	if tableInfo.IsView() {
		e.appendRow([]interface{}{tableInfo.Name.O, buf.String(), tableInfo.Charset, tableInfo.Collate})
		return nil
	}

	e.appendRow([]interface{}{tableInfo.Name.O, buf.String()})
	return nil
}

func (e *ShowExec) fetchShowCreateView() error {
	db, ok := e.is.SchemaByName(e.DBName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}

	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	if !tb.Meta().IsView() {
		return ErrWrongObject.GenWithStackByArgs(db.Name.O, tb.Meta().Name.O, "VIEW")
	}

	var buf bytes.Buffer
	fetchShowCreateTable4View(e.ctx, tb.Meta(), &buf)
	e.appendRow([]interface{}{tb.Meta().Name.O, buf.String(), tb.Meta().Charset, tb.Meta().Collate})
	return nil
}

func fetchShowCreateTable4View(ctx sessionctx.Context, tb *model.TableInfo, buf *bytes.Buffer) {
	sqlMode := ctx.GetSessionVars().SQLMode
	fmt.Fprintf(buf, "CREATE ALGORITHM=%s ", tb.View.Algorithm.String())
	if tb.View.Definer.AuthUsername == "" || tb.View.Definer.AuthHostname == "" {
		fmt.Fprintf(buf, "DEFINER=%s@%s ", stringutil.Escape(tb.View.Definer.Username, sqlMode), stringutil.Escape(tb.View.Definer.Hostname, sqlMode))
	} else {
		fmt.Fprintf(buf, "DEFINER=%s@%s ", stringutil.Escape(tb.View.Definer.AuthUsername, sqlMode), stringutil.Escape(tb.View.Definer.AuthHostname, sqlMode))
	}
	fmt.Fprintf(buf, "SQL SECURITY %s ", tb.View.Security.String())
	fmt.Fprintf(buf, "VIEW %s (", stringutil.Escape(tb.Name.O, sqlMode))
	for i, col := range tb.Columns {
		fmt.Fprintf(buf, "%s", stringutil.Escape(col.Name.O, sqlMode))
		if i < len(tb.Columns)-1 {
			fmt.Fprintf(buf, ", ")
		}
	}
	fmt.Fprintf(buf, ") AS %s", tb.View.SelectStmt)
}

func appendPartitionInfo(partitionInfo *model.PartitionInfo, buf *bytes.Buffer, sqlMode mysql.SQLMode) {
	if partitionInfo == nil {
		return
	}
	// Since MySQL 5.1/5.5 is very old and TiDB aims for 5.7/8.0 compatibility, we will not
	// include the /*!50100 or /*!50500 comments for TiDB.
	// This also solves the issue with comments within comments that would happen for
	// PLACEMENT POLICY options.
	if partitionInfo.Type == model.PartitionTypeHash {
		defaultPartitionDefinitions := true
		for i, def := range partitionInfo.Definitions {
			if def.Name.O != fmt.Sprintf("p%d", i) {
				defaultPartitionDefinitions = false
				break
			}
			if len(def.Comment) > 0 || def.PlacementPolicyRef != nil {
				defaultPartitionDefinitions = false
				break
			}
		}

		if defaultPartitionDefinitions {
			fmt.Fprintf(buf, "\nPARTITION BY HASH (%s) PARTITIONS %d", partitionInfo.Expr, partitionInfo.Num)
			return
		}
	}
	// this if statement takes care of lists/range columns case
	if partitionInfo.Columns != nil {
		// partitionInfo.Type == model.PartitionTypeRange || partitionInfo.Type == model.PartitionTypeList
		// Notice that MySQL uses two spaces between LIST and COLUMNS...
		fmt.Fprintf(buf, "\nPARTITION BY %s COLUMNS(", partitionInfo.Type.String())
		for i, col := range partitionInfo.Columns {
			buf.WriteString(stringutil.Escape(col.O, sqlMode))
			if i < len(partitionInfo.Columns)-1 {
				buf.WriteString(",")
			}
		}
		buf.WriteString(")\n(")
	} else {
		fmt.Fprintf(buf, "\nPARTITION BY %s (%s)\n(", partitionInfo.Type.String(), partitionInfo.Expr)
	}

	for i, def := range partitionInfo.Definitions {
		if i > 0 {
			fmt.Fprintf(buf, ",\n ")
		}
		fmt.Fprintf(buf, "PARTITION %s", stringutil.Escape(def.Name.O, sqlMode))
		// PartitionTypeHash does not have any VALUES definition
		if partitionInfo.Type == model.PartitionTypeRange {
			lessThans := strings.Join(def.LessThan, ",")
			fmt.Fprintf(buf, " VALUES LESS THAN (%s)", lessThans)
		} else if partitionInfo.Type == model.PartitionTypeList {
			values := bytes.NewBuffer(nil)
			for j, inValues := range def.InValues {
				if j > 0 {
					values.WriteString(",")
				}
				if len(inValues) > 1 {
					values.WriteString("(")
					values.WriteString(strings.Join(inValues, ","))
					values.WriteString(")")
				} else {
					values.WriteString(strings.Join(inValues, ","))
				}
			}
			fmt.Fprintf(buf, " VALUES IN (%s)", values.String())
		}
		if len(def.Comment) > 0 {
			buf.WriteString(fmt.Sprintf(" COMMENT '%s'", format.OutputFormat(def.Comment)))
		}
		if def.PlacementPolicyRef != nil {
			// add placement ref info here
			fmt.Fprintf(buf, " /*T![placement] PLACEMENT POLICY=%s */", stringutil.Escape(def.PlacementPolicyRef.Name.O, sqlMode))
		}
	}
	buf.WriteString(")")
}

// ConstructResultOfShowCreateDatabase constructs the result for show create database.
func ConstructResultOfShowCreateDatabase(ctx sessionctx.Context, dbInfo *model.DBInfo, ifNotExists bool, buf *bytes.Buffer) (err error) {
	sqlMode := ctx.GetSessionVars().SQLMode
	var ifNotExistsStr string
	if ifNotExists {
		ifNotExistsStr = "/*!32312 IF NOT EXISTS*/ "
	}
	fmt.Fprintf(buf, "CREATE DATABASE %s%s", ifNotExistsStr, stringutil.Escape(dbInfo.Name.O, sqlMode))
	if dbInfo.Charset != "" {
		fmt.Fprintf(buf, " /*!40100 DEFAULT CHARACTER SET %s ", dbInfo.Charset)
		defaultCollate, err := charset.GetDefaultCollation(dbInfo.Charset)
		if err != nil {
			return errors.Trace(err)
		}
		if dbInfo.Collate != "" && dbInfo.Collate != defaultCollate {
			fmt.Fprintf(buf, "COLLATE %s ", dbInfo.Collate)
		}
		fmt.Fprint(buf, "*/")
	} else if dbInfo.Collate != "" {
		collInfo, err := collate.GetCollationByName(dbInfo.Collate)
		if err != nil {
			return errors.Trace(err)
		}
		fmt.Fprintf(buf, " /*!40100 DEFAULT CHARACTER SET %s ", collInfo.CharsetName)
		if !collInfo.IsDefault {
			fmt.Fprintf(buf, "COLLATE %s ", dbInfo.Collate)
		}
		fmt.Fprint(buf, "*/")
	}
	// MySQL 5.7 always show the charset info but TiDB may ignore it, which makes a slight difference. We keep this
	// behavior unchanged because it is trivial enough.
	if dbInfo.PlacementPolicyRef != nil {
		// add placement ref info here
		fmt.Fprintf(buf, " /*T![placement] PLACEMENT POLICY=%s */", stringutil.Escape(dbInfo.PlacementPolicyRef.Name.O, sqlMode))
	}
	return nil
}

// ConstructResultOfShowCreatePlacementPolicy constructs the result for show create placement policy.
func ConstructResultOfShowCreatePlacementPolicy(policyInfo *model.PolicyInfo) string {
	return fmt.Sprintf("CREATE PLACEMENT POLICY `%s` %s", policyInfo.Name.O, policyInfo.PlacementSettings.String())
}

// fetchShowCreateDatabase composes show create database result.
func (e *ShowExec) fetchShowCreateDatabase() error {
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, e.DBName.String()) {
			return e.dbAccessDenied()
		}
	}
	dbInfo, ok := e.is.SchemaByName(e.DBName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}

	var buf bytes.Buffer
	err := ConstructResultOfShowCreateDatabase(e.ctx, dbInfo, e.IfNotExists, &buf)
	if err != nil {
		return err
	}
	e.appendRow([]interface{}{dbInfo.Name.O, buf.String()})
	return nil
}

// fetchShowCreatePlacementPolicy composes show create policy result.
func (e *ShowExec) fetchShowCreatePlacementPolicy() error {
	policy, found := e.is.PolicyByName(e.DBName)
	if !found {
		return infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(e.DBName.O)
	}
	showCreate := ConstructResultOfShowCreatePlacementPolicy(policy)
	e.appendRow([]interface{}{e.DBName.O, showCreate})
	return nil
}

func (e *ShowExec) fetchShowCollation() error {
	collations := collate.GetSupportedCollations()
	for _, v := range collations {
		isDefault := ""
		if v.IsDefault {
			isDefault = "Yes"
		}
		e.appendRow([]interface{}{
			v.Name,
			v.CharsetName,
			v.ID,
			isDefault,
			"Yes",
			1,
		})
	}
	return nil
}

// fetchShowCreateUser composes show create create user result.
func (e *ShowExec) fetchShowCreateUser(ctx context.Context) error {
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}

	userName, hostName := e.User.Username, e.User.Hostname
	sessVars := e.ctx.GetSessionVars()
	if e.User.CurrentUser {
		userName = sessVars.User.AuthUsername
		hostName = sessVars.User.AuthHostname
	} else {
		// Show create user requires the SELECT privilege on mysql.user.
		// Ref https://dev.mysql.com/doc/refman/5.7/en/show-create-user.html
		activeRoles := sessVars.ActiveRoles
		if !checker.RequestVerification(activeRoles, mysql.SystemDB, mysql.UserTable, "", mysql.SelectPriv) {
			return e.tableAccessDenied("SELECT", mysql.UserTable)
		}
	}

	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)

	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, `SELECT plugin FROM %n.%n WHERE User=%? AND Host=%?`, mysql.SystemDB, mysql.UserTable, userName, strings.ToLower(hostName))
	if err != nil {
		return errors.Trace(err)
	}

	if len(rows) == 0 {
		// FIXME: the error returned is not escaped safely
		return ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER",
			fmt.Sprintf("'%s'@'%s'", e.User.Username, e.User.Hostname))
	}

	authplugin := mysql.AuthNativePassword
	if len(rows) == 1 && rows[0].GetString(0) != "" {
		authplugin = rows[0].GetString(0)
	}

	rows, _, err = exec.ExecRestrictedSQL(ctx, nil, `SELECT Priv FROM %n.%n WHERE User=%? AND Host=%?`, mysql.SystemDB, mysql.GlobalPrivTable, userName, hostName)
	if err != nil {
		return errors.Trace(err)
	}

	require := "NONE"
	if len(rows) == 1 {
		privData := rows[0].GetString(0)
		var privValue privileges.GlobalPrivValue
		err = gjson.Unmarshal(hack.Slice(privData), &privValue)
		if err != nil {
			return errors.Trace(err)
		}
		require = privValue.RequireStr()
	}

	authData := checker.GetEncodedPassword(e.User.Username, e.User.Hostname)
	authStr := ""
	if !(authplugin == mysql.AuthSocket && authData == "") {
		authStr = fmt.Sprintf(" AS '%s'", authData)
	}

	// FIXME: the returned string is not escaped safely
	showStr := fmt.Sprintf("CREATE USER '%s'@'%s' IDENTIFIED WITH '%s'%s REQUIRE %s PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK",
		e.User.Username, e.User.Hostname, authplugin, authStr, require)
	e.appendRow([]interface{}{showStr})
	return nil
}

func (e *ShowExec) fetchShowGrants() error {
	vars := e.ctx.GetSessionVars()
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	if e.User == nil || e.User.CurrentUser {
		// The input is a "SHOW GRANTS" statement with no users *or* SHOW GRANTS FOR CURRENT_USER()
		// In these cases we include the active roles for showing privileges.
		e.User = &auth.UserIdentity{Username: vars.User.AuthUsername, Hostname: vars.User.AuthHostname}
		if len(e.Roles) == 0 {
			e.Roles = vars.ActiveRoles
		}
	} else {
		userName := vars.User.AuthUsername
		hostName := vars.User.AuthHostname
		// Show grant user requires the SELECT privilege on mysql schema.
		// Ref https://dev.mysql.com/doc/refman/8.0/en/show-grants.html
		if userName != e.User.Username || hostName != e.User.Hostname {
			if !checker.RequestVerification(vars.ActiveRoles, mysql.SystemDB, "", "", mysql.SelectPriv) {
				return ErrDBaccessDenied.GenWithStackByArgs(userName, hostName, mysql.SystemDB)
			}
		}
	}
	// This is for the syntax SHOW GRANTS FOR x USING role
	for _, r := range e.Roles {
		if r.Hostname == "" {
			r.Hostname = "%"
		}
		if !checker.FindEdge(e.ctx, r, e.User) {
			return ErrRoleNotGranted.GenWithStackByArgs(r.String(), e.User.String())
		}
	}
	gs, err := checker.ShowGrants(e.ctx, e.User, e.Roles)
	if err != nil {
		return errors.Trace(err)
	}
	for _, g := range gs {
		e.appendRow([]interface{}{g})
	}
	return nil
}

func (e *ShowExec) fetchShowPrivileges() error {
	e.appendRow([]interface{}{"Alter", "Tables", "To alter the table"})
	e.appendRow([]interface{}{"Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"})
	e.appendRow([]interface{}{"Create", "Databases,Tables,Indexes", "To create new databases and tables"})
	e.appendRow([]interface{}{"Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"})
	e.appendRow([]interface{}{"Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"})
	e.appendRow([]interface{}{"Create view", "Tables", "To create new views"})
	e.appendRow([]interface{}{"Create user", "Server Admin", "To create new users"})
	e.appendRow([]interface{}{"Delete", "Tables", "To delete existing rows"})
	e.appendRow([]interface{}{"Drop", "Databases,Tables", "To drop databases, tables, and views"})
	e.appendRow([]interface{}{"Event", "Server Admin", "To create, alter, drop and execute events"})
	e.appendRow([]interface{}{"Execute", "Functions,Procedures", "To execute stored routines"})
	e.appendRow([]interface{}{"File", "File access on server", "To read and write files on the server"})
	e.appendRow([]interface{}{"Grant option", "Databases,Tables,Functions,Procedures", "To give to other users those privileges you possess"})
	e.appendRow([]interface{}{"Index", "Tables", "To create or drop indexes"})
	e.appendRow([]interface{}{"Insert", "Tables", "To insert data into tables"})
	e.appendRow([]interface{}{"Lock tables", "Databases", "To use LOCK TABLES (together with SELECT privilege)"})
	e.appendRow([]interface{}{"Process", "Server Admin", "To view the plain text of currently executing queries"})
	e.appendRow([]interface{}{"Proxy", "Server Admin", "To make proxy user possible"})
	e.appendRow([]interface{}{"References", "Databases,Tables", "To have references on tables"})
	e.appendRow([]interface{}{"Reload", "Server Admin", "To reload or refresh tables, logs and privileges"})
	e.appendRow([]interface{}{"Replication client", "Server Admin", "To ask where the slave or master servers are"})
	e.appendRow([]interface{}{"Replication slave", "Server Admin", "To read binary log events from the master"})
	e.appendRow([]interface{}{"Select", "Tables", "To retrieve rows from table"})
	e.appendRow([]interface{}{"Show databases", "Server Admin", "To see all databases with SHOW DATABASES"})
	e.appendRow([]interface{}{"Show view", "Tables", "To see views with SHOW CREATE VIEW"})
	e.appendRow([]interface{}{"Shutdown", "Server Admin", "To shut down the server"})
	e.appendRow([]interface{}{"Super", "Server Admin", "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc."})
	e.appendRow([]interface{}{"Trigger", "Tables", "To use triggers"})
	e.appendRow([]interface{}{"Create tablespace", "Server Admin", "To create/alter/drop tablespaces"})
	e.appendRow([]interface{}{"Update", "Tables", "To update existing rows"})
	e.appendRow([]interface{}{"Usage", "Server Admin", "No privileges - allow connect only"})

	for _, priv := range privileges.GetDynamicPrivileges() {
		e.appendRow([]interface{}{priv, "Server Admin", ""})
	}
	return nil
}

func (e *ShowExec) fetchShowTriggers() error {
	return nil
}

func (e *ShowExec) fetchShowProcedureStatus() error {
	return nil
}

func (e *ShowExec) fetchShowPlugins() error {
	tiPlugins := plugin.GetAll()
	for _, ps := range tiPlugins {
		for _, p := range ps {
			e.appendRow([]interface{}{p.Name, p.StateValue(), p.Kind.String(), p.Path, p.License, strconv.Itoa(int(p.Version))})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowWarnings(errOnly bool) error {
	warns := e.ctx.GetSessionVars().StmtCtx.GetWarnings()
	for _, w := range warns {
		if errOnly && w.Level != stmtctx.WarnLevelError {
			continue
		}
		warn := errors.Cause(w.Err)
		switch x := warn.(type) {
		case *terror.Error:
			sqlErr := terror.ToSQLError(x)
			e.appendRow([]interface{}{w.Level, int64(sqlErr.Code), sqlErr.Message})
		default:
			e.appendRow([]interface{}{w.Level, int64(mysql.ErrUnknown), warn.Error()})
		}
	}
	return nil
}

// fetchShowPumpOrDrainerStatus gets status of all pumps or drainers and fill them into e.rows.
func (e *ShowExec) fetchShowPumpOrDrainerStatus(kind string) error {
	registry, err := createRegistry(config.GetGlobalConfig().Path)
	if err != nil {
		return errors.Trace(err)
	}

	nodes, _, err := registry.Nodes(context.Background(), node.NodePrefix[kind])
	if err != nil {
		return errors.Trace(err)
	}
	err = registry.Close()
	if err != nil {
		return errors.Trace(err)
	}

	for _, n := range nodes {
		if n.State == node.Offline {
			continue
		}
		e.appendRow([]interface{}{n.NodeID, n.Addr, n.State, n.MaxCommitTS, util.TSOToRoughTime(n.UpdateTS).Format(types.TimeFormat)})
	}

	return nil
}

// createRegistry returns an ectd registry
func createRegistry(urls string) (*node.EtcdRegistry, error) {
	ectdEndpoints, err := util.ParseHostPortAddr(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli, err := etcd.NewClientFromCfg(ectdEndpoints, etcdDialTimeout, node.DefaultRootPath, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return node.NewEtcdRegistry(cli, etcdDialTimeout), nil
}

func (e *ShowExec) getTable() (table.Table, error) {
	if e.Table == nil {
		return nil, errors.New("table not found")
	}
	tb, ok := e.is.TableByID(e.Table.TableInfo.ID)
	if !ok {
		return nil, errors.Errorf("table %s not found", e.Table.Name)
	}
	return tb, nil
}

func (e *ShowExec) dbAccessDenied() error {
	user := e.ctx.GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return ErrDBaccessDenied.GenWithStackByArgs(u, h, e.DBName)
}

func (e *ShowExec) tableAccessDenied(access string, table string) error {
	user := e.ctx.GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return ErrTableaccessDenied.GenWithStackByArgs(access, u, h, table)
}

func (e *ShowExec) appendRow(row []interface{}) {
	for i, col := range row {
		if col == nil {
			e.result.AppendNull(i)
			continue
		}
		switch x := col.(type) {
		case nil:
			e.result.AppendNull(i)
		case int:
			e.result.AppendInt64(i, int64(x))
		case int64:
			e.result.AppendInt64(i, x)
		case uint64:
			e.result.AppendUint64(i, x)
		case float64:
			e.result.AppendFloat64(i, x)
		case float32:
			e.result.AppendFloat32(i, x)
		case string:
			e.result.AppendString(i, x)
		case []byte:
			e.result.AppendBytes(i, x)
		case types.BinaryLiteral:
			e.result.AppendBytes(i, x)
		case *types.MyDecimal:
			e.result.AppendMyDecimal(i, x)
		case types.Time:
			e.result.AppendTime(i, x)
		case json.BinaryJSON:
			e.result.AppendJSON(i, x)
		case types.Duration:
			e.result.AppendDuration(i, x)
		case types.Enum:
			e.result.AppendEnum(i, x)
		case types.Set:
			e.result.AppendSet(i, x)
		default:
			e.result.AppendNull(i)
		}
	}
}

func (e *ShowExec) fetchShowTableRegions() error {
	store := e.ctx.GetStore()
	tikvStore, ok := store.(helper.Storage)
	if !ok {
		return nil
	}
	splitStore, ok := store.(kv.SplittableStore)
	if !ok {
		return nil
	}

	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	physicalIDs := []int64{}
	if pi := tb.Meta().GetPartitionInfo(); pi != nil {
		for _, name := range e.Table.PartitionNames {
			pid, err := tables.FindPartitionByName(tb.Meta(), name.L)
			if err != nil {
				return err
			}
			physicalIDs = append(physicalIDs, pid)
		}
		if len(physicalIDs) == 0 {
			for _, p := range pi.Definitions {
				physicalIDs = append(physicalIDs, p.ID)
			}
		}
	} else {
		if len(e.Table.PartitionNames) != 0 {
			return plannercore.ErrPartitionClauseOnNonpartitioned
		}
		physicalIDs = append(physicalIDs, tb.Meta().ID)
	}

	// Get table regions from from pd, not from regionCache, because the region cache maybe outdated.
	var regions []regionMeta
	if len(e.IndexName.L) != 0 {
		indexInfo := tb.Meta().FindIndexByName(e.IndexName.L)
		if indexInfo == nil {
			return plannercore.ErrKeyDoesNotExist.GenWithStackByArgs(e.IndexName, tb.Meta().Name)
		}
		regions, err = getTableIndexRegions(indexInfo, physicalIDs, tikvStore, splitStore)
	} else {
		regions, err = getTableRegions(tb, physicalIDs, tikvStore, splitStore)
	}

	if err != nil {
		return err
	}
	e.fillRegionsToChunk(regions)
	return nil
}

func getTableRegions(tb table.Table, physicalIDs []int64, tikvStore helper.Storage, splitStore kv.SplittableStore) ([]regionMeta, error) {
	regions := make([]regionMeta, 0, len(physicalIDs))
	uniqueRegionMap := make(map[uint64]struct{})
	for _, id := range physicalIDs {
		rs, err := getPhysicalTableRegions(id, tb.Meta(), tikvStore, splitStore, uniqueRegionMap)
		if err != nil {
			return nil, err
		}
		regions = append(regions, rs...)
	}
	return regions, nil
}

func getTableIndexRegions(indexInfo *model.IndexInfo, physicalIDs []int64, tikvStore helper.Storage, splitStore kv.SplittableStore) ([]regionMeta, error) {
	regions := make([]regionMeta, 0, len(physicalIDs))
	uniqueRegionMap := make(map[uint64]struct{})
	for _, id := range physicalIDs {
		rs, err := getPhysicalIndexRegions(id, indexInfo, tikvStore, splitStore, uniqueRegionMap)
		if err != nil {
			return nil, err
		}
		regions = append(regions, rs...)
	}
	return regions, nil
}

func (e *ShowExec) fillRegionsToChunk(regions []regionMeta) {
	for i := range regions {
		e.result.AppendUint64(0, regions[i].region.Id)
		e.result.AppendString(1, regions[i].start)
		e.result.AppendString(2, regions[i].end)
		e.result.AppendUint64(3, regions[i].leaderID)
		e.result.AppendUint64(4, regions[i].storeID)

		peers := ""
		for i, peer := range regions[i].region.Peers {
			if i > 0 {
				peers += ", "
			}
			peers += strconv.FormatUint(peer.Id, 10)
		}
		e.result.AppendString(5, peers)
		if regions[i].scattering {
			e.result.AppendInt64(6, 1)
		} else {
			e.result.AppendInt64(6, 0)
		}

		e.result.AppendUint64(7, regions[i].writtenBytes)
		e.result.AppendUint64(8, regions[i].readBytes)
		e.result.AppendInt64(9, regions[i].approximateSize)
		e.result.AppendInt64(10, regions[i].approximateKeys)
	}
}

func (e *ShowExec) fetchShowBuiltins() error {
	for _, f := range expression.GetBuiltinList() {
		e.appendRow([]interface{}{f})
	}
	return nil
}

// tryFillViewColumnType fill the columns type info of a view.
// Because view's underlying table's column could change or recreate, so view's column type may change over time.
// To avoid this situation we need to generate a logical plan and extract current column types from Schema.
func tryFillViewColumnType(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, dbName model.CIStr, tbl *model.TableInfo) error {
	if !tbl.IsView() {
		return nil
	}
	// We need to run the build plan process in another session because there may be
	// multiple goroutines running at the same time while session is not goroutine-safe.
	// Take joining system table as an example, `fetchBuildSideRows` and `fetchProbeSideChunks` can be run concurrently.
	return runWithSystemSession(sctx, func(s sessionctx.Context) error {
		// Retrieve view columns info.
		planBuilder, _ := plannercore.NewPlanBuilder().Init(s, is, &hint.BlockHintProcessor{})
		if viewLogicalPlan, err := planBuilder.BuildDataSourceFromView(ctx, dbName, tbl); err == nil {
			viewSchema := viewLogicalPlan.Schema()
			viewOutputNames := viewLogicalPlan.OutputNames()
			for _, col := range tbl.Columns {
				idx := expression.FindFieldNameIdxByColName(viewOutputNames, col.Name.L)
				if idx >= 0 {
					col.FieldType = *viewSchema.Columns[idx].GetType()
				}
				if col.GetType() == mysql.TypeVarString {
					col.SetType(mysql.TypeVarchar)
				}
			}
		} else {
			return err
		}
		return nil
	})
}

func runWithSystemSession(sctx sessionctx.Context, fn func(sessionctx.Context) error) error {
	b := &baseExecutor{ctx: sctx}
	sysCtx, err := b.getSysSession()
	if err != nil {
		return err
	}
	defer b.releaseSysSession(sysCtx)
	return fn(sysCtx)
}
