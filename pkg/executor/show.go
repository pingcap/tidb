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
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// ShowExec represents a show executor.
type ShowExec struct {
	exec.BaseExecutor

	Tp                ast.ShowStmtType // Databases/Tables/Columns/....
	DBName            ast.CIStr
	Table             *resolve.TableNameW  // Used for showing columns.
	Partition         ast.CIStr            // Used for showing partition
	Column            *ast.ColumnName      // Used for `desc table column`.
	IndexName         ast.CIStr            // Used for show table regions.
	ResourceGroupName ast.CIStr            // Used for showing resource group
	Flag              int                  // Some flag parsed from sql, such as FULL.
	Roles             []*auth.RoleIdentity // Used for show grants.
	User              *auth.UserIdentity   // Used by show grants, show create user.
	Extractor         base.ShowPredicateExtractor

	is infoschema.InfoSchema

	CountWarningsOrErrors bool // Used for showing count(*) warnings | errors

	result *chunk.Chunk
	cursor int

	Full        bool
	IfNotExists bool // Used for `show create database if not exists`
	GlobalScope bool // GlobalScope is used by show variables
	Extended    bool // Used for `show extended columns from ...`

	ImportJobID       *int64
	DistributionJobID *int64
	ImportGroupKey    string // Used for SHOW IMPORT GROUP <GROUP_KEY>
}

type showTableRegionRowItem struct {
	regionMeta
	schedulingConstraints string
	schedulingState       string
}

// Next implements the Executor Next interface.
func (e *ShowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.result == nil {
		e.result = exec.NewFirstChunk(e)
		err := e.fetchAll(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		iter := chunk.NewIterator4Chunk(e.result)
		for colIdx := range e.Schema().Len() {
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
	numCurBatch := min(req.Capacity(), e.result.NumRows()-e.cursor)
	req.Append(e.result, e.cursor, e.cursor+numCurBatch)
	e.cursor += numCurBatch
	return nil
}

func (e *ShowExec) fetchAll(ctx context.Context) error {
	// Temporary disables select limit to avoid miss the result.
	// Because some of below fetch show result stmt functions will generate
	// a SQL stmt and then execute the new SQL stmt to do the fetch result task
	// for the up-level show stmt.
	// Here, the problem is the new SQL stmt will be influenced by SelectLimit value
	// and return a limited result set back to up level show stmt. This, in fact, may
	// cause a filter effect on result set that may exclude the qualified record outside of
	// result set.
	// Finally, when above result set, may not include qualified record, is returned to up
	// level show stmt's selection, which really applies the filter operation on returned
	// result set, it may return empty result to client.
	oldSelectLimit := e.Ctx().GetSessionVars().SelectLimit
	e.Ctx().GetSessionVars().SelectLimit = math.MaxUint64
	defer func() {
		// Restore session Var SelectLimit value.
		e.Ctx().GetSessionVars().SelectLimit = oldSelectLimit
	}()

	switch e.Tp {
	case ast.ShowCharset:
		return e.fetchShowCharset()
	case ast.ShowCollation:
		return e.fetchShowCollation()
	case ast.ShowColumns:
		return e.fetchShowColumns(ctx)
	case ast.ShowConfig:
		return e.fetchShowClusterConfigs()
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
	case ast.ShowCreateResourceGroup:
		return e.fetchShowCreateResourceGroup()
	case ast.ShowDatabases:
		return e.fetchShowDatabases()
	case ast.ShowEngines:
		return e.fetchShowEngines(ctx)
	case ast.ShowGrants:
		return e.fetchShowGrants(ctx)
	case ast.ShowIndex:
		return e.fetchShowIndex()
	case ast.ShowProcedureStatus:
		return e.fetchShowProcedureStatus()
	case ast.ShowStatus:
		return e.fetchShowStatus()
	case ast.ShowTables:
		return e.fetchShowTables(ctx)
	case ast.ShowOpenTables:
		return e.fetchShowOpenTables()
	case ast.ShowTableStatus:
		return e.fetchShowTableStatus(ctx)
	case ast.ShowTriggers:
		return e.fetchShowTriggers()
	case ast.ShowVariables:
		return e.fetchShowVariables(ctx)
	case ast.ShowWarnings:
		return e.fetchShowWarnings(false)
	case ast.ShowErrors:
		return e.fetchShowWarnings(true)
	case ast.ShowProcessList:
		return e.fetchShowProcessList()
	case ast.ShowEvents:
		// empty result
	case ast.ShowStatsExtended:
		return e.fetchShowStatsExtended(ctx)
	case ast.ShowStatsMeta:
		return e.fetchShowStatsMeta(ctx)
	case ast.ShowStatsHistograms:
		return e.fetchShowStatsHistogram(ctx)
	case ast.ShowStatsBuckets:
		return e.fetchShowStatsBuckets(ctx)
	case ast.ShowStatsTopN:
		return e.fetchShowStatsTopN(ctx)
	case ast.ShowStatsHealthy:
		e.fetchShowStatsHealthy(ctx)
		return nil
	case ast.ShowStatsLocked:
		return e.fetchShowStatsLocked(ctx)
	case ast.ShowHistogramsInFlight:
		e.fetchShowHistogramsInFlight()
		return nil
	case ast.ShowColumnStatsUsage:
		return e.fetchShowColumnStatsUsage(ctx)
	case ast.ShowPlugins:
		return e.fetchShowPlugins()
	case ast.ShowProfiles:
		// empty result
	case ast.ShowMasterStatus, ast.ShowBinlogStatus:
		return e.fetchShowMasterStatus()
	case ast.ShowPrivileges:
		return e.fetchShowPrivileges()
	case ast.ShowBindings:
		return e.fetchShowBind()
	case ast.ShowBindingCacheStatus:
		return e.fetchShowBindingCacheStatus(ctx)
	case ast.ShowAnalyzeStatus:
		return e.fetchShowAnalyzeStatus(ctx)
	case ast.ShowRegions:
		return e.fetchShowTableRegions(ctx)
	case ast.ShowDistributions:
		return e.fetchShowDistributions(ctx)
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
	case ast.ShowSessionStates:
		return e.fetchShowSessionStates(ctx)
	case ast.ShowImportJobs:
		return e.fetchShowImportJobs(ctx)
	case ast.ShowImportGroups:
		return e.fetchShowImportGroups(ctx)
	case ast.ShowDistributionJobs:
		return e.fetchShowDistributionJobs(ctx)
	case ast.ShowAffinity:
		return e.fetchShowAffinity(ctx)
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
	if x, ok := in.(*ast.TableName); ok {
		schema := x.Schema.L
		if schema == "" {
			schema = v.defaultDB
		}
		if !v.is.TableExists(ast.NewCIStr(schema), x.Name) {
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

func (*visibleChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (e *ShowExec) fetchShowBind() error {
	var bindings []*bindinfo.Binding
	if !e.GlobalScope {
		handle := e.Ctx().Value(bindinfo.SessionBindInfoKeyType).(bindinfo.SessionBindingHandle)
		bindings = handle.GetAllSessionBindings()
	} else {
		bindings = domain.GetDomain(e.Ctx()).BindingHandle().GetAllBindings()
	}
	// Remove the invalid bindings.
	parser := parser.New()
	// For the different origin_sql, sort the bindings according to their max update time.
	sort.Slice(bindings, func(i int, j int) bool {
		cmpResult := bindings[i].UpdateTime.Compare(bindings[j].UpdateTime)
		if cmpResult == 0 {
			// Because the create time must be different, the result of sorting is stable.
			cmpResult = bindings[i].CreateTime.Compare(bindings[j].CreateTime)
		}
		return cmpResult > 0
	})
	for _, hint := range bindings {
		if hint == nil { // if the binding cache doesn't have enough memory, it might return nil
			continue
		}
		stmt, err := parser.ParseOneStmt(hint.BindSQL, hint.Charset, hint.Collation)
		if err != nil {
			return err
		}
		checker := visibleChecker{
			defaultDB: hint.Db,
			ctx:       e.Ctx(),
			is:        e.is,
			manager:   privilege.GetPrivilegeManager(e.Ctx()),
			ok:        true,
		}
		stmt.Accept(&checker)
		if !checker.ok {
			continue
		}
		e.appendRow([]any{
			hint.OriginalSQL,
			hint.BindSQL,
			hint.Db,
			hint.Status,
			hint.CreateTime,
			hint.UpdateTime,
			hint.Charset,
			hint.Collation,
			hint.Source,
			hint.SQLDigest,
			hint.PlanDigest,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowBindingCacheStatus(ctx context.Context) error {
	exec := e.Ctx().GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBindInfo)

	rows, _, err := exec.ExecRestrictedSQL(ctx, nil,
		fmt.Sprintf("SELECT count(*) FROM mysql.bind_info where status = '%s' or status = '%s';",
			bindinfo.StatusEnabled, bindinfo.StatusUsing))
	if err != nil {
		return errors.Trace(err)
	}

	handle := domain.GetDomain(e.Ctx()).BindingHandle()

	bindings := handle.GetAllBindings()
	numBindings := 0
	for _, binding := range bindings {
		if binding.IsBindingEnabled() {
			numBindings++
		}
	}

	memUsage := handle.GetMemUsage()
	memCapacity := handle.GetMemCapacity()
	e.appendRow([]any{
		numBindings,
		rows[0].GetInt64(0),
		memory.FormatBytes(memUsage),
		memory.FormatBytes(memCapacity),
	})
	return nil
}

func (e *ShowExec) fetchShowEngines(ctx context.Context) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
	exec := e.Ctx().GetRestrictedSQLExecutor()

	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, `SELECT * FROM information_schema.engines`)
	if err != nil {
		return errors.Trace(err)
	}

	e.result.AppendRows(rows)
	return nil
}

// moveInfoSchemaToFront moves information_schema to the first, and the others are sorted in the origin ascending order.
func moveInfoSchemaToFront(dbs []string) {
	if len(dbs) > 0 && strings.EqualFold(dbs[0], metadef.InformationSchemaName.O) {
		return
	}

	i := sort.SearchStrings(dbs, metadef.InformationSchemaName.O)
	if i < len(dbs) && strings.EqualFold(dbs[i], metadef.InformationSchemaName.O) {
		copy(dbs[1:i+1], dbs[0:i])
		dbs[0] = metadef.InformationSchemaName.O
	}
}

func (e *ShowExec) fetchShowDatabases() error {
	dbs := infoschema.AllSchemaNames(e.is)
	checker := privilege.GetPrivilegeManager(e.Ctx())
	slices.Sort(dbs)
	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)

	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}
	// let information_schema be the first database
	moveInfoSchemaToFront(dbs)
	for _, d := range dbs {
		if checker != nil && !checker.DBIsVisible(e.Ctx().GetSessionVars().ActiveRoles, d) {
			continue
		} else if fieldFilter != "" && strings.ToLower(d) != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(strings.ToLower(d)) {
			continue
		}
		e.appendRow([]any{
			d,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowProcessList() error {
	sm := e.Ctx().GetSessionManager()
	if sm == nil {
		return nil
	}

	loginUser, activeRoles := e.Ctx().GetSessionVars().User, e.Ctx().GetSessionVars().ActiveRoles
	var hasProcessPriv bool
	if pm := privilege.GetPrivilegeManager(e.Ctx()); pm != nil {
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

func (*ShowExec) fetchShowOpenTables() error {
	// TiDB has no concept like mysql's "table cache" and "open table"
	// For simplicity, we just return an empty result with the same structure as MySQL's SHOW OPEN TABLES
	return nil
}

// showInfo represents the result of `SHOW TABLES`.
type showInfo struct {
	Name ast.CIStr
	// only used for show full tables
	TableType string
}

// getTableType returns the type of the table.
func (e *ShowExec) getTableType(tb *model.TableInfo) string {
	switch {
	case tb.IsView():
		return "VIEW"
	case tb.IsSequence():
		return "SEQUENCE"
	case metadef.IsMemDB(e.DBName.L):
		return "SYSTEM VIEW"
	default:
		return "BASE TABLE"
	}
}

// fetchShowInfoByName fetches the show info for `SHOW <FULL> TABLES like 'xxx'`
func (e *ShowExec) fetchShowInfoByName(ctx context.Context, name string) ([]*showInfo, error) {
	tb, err := e.is.TableByName(ctx, e.DBName, ast.NewCIStr(name))
	if err != nil {
		// do nothing if table not exists
		if infoschema.ErrTableNotExists.Equal(err) {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	if tb.Meta().TempTableType == model.TempTableLocal {
		return nil, nil
	}
	return []*showInfo{{Name: tb.Meta().Name, TableType: e.getTableType(tb.Meta())}}, nil
}

// fetchShowSimpleTables fetches the table info for `SHOW TABLE`.
func (e *ShowExec) fetchShowSimpleTables(ctx context.Context) ([]*showInfo, error) {
	tb, err := e.is.SchemaSimpleTableInfos(ctx, e.DBName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	showInfos := make([]*showInfo, 0, len(tb))
	for _, v := range tb {
		// TODO: consider add type info to TableNameInfo
		showInfos = append(showInfos, &showInfo{Name: v.Name})
	}
	return showInfos, nil
}

// fetchShowFullTables fetches the table info for `SHOW FULL TABLES`.
func (e *ShowExec) fetchShowFullTables(ctx context.Context) ([]*showInfo, error) {
	tb, err := e.is.SchemaTableInfos(ctx, e.DBName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	showInfos := make([]*showInfo, 0, len(tb))
	for _, v := range tb {
		showInfos = append(showInfos, &showInfo{Name: v.Name, TableType: e.getTableType(v)})
	}
	return showInfos, nil
}

func (e *ShowExec) fetchShowTables(ctx context.Context) error {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker != nil && e.Ctx().GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.Ctx().GetSessionVars().ActiveRoles, e.DBName.O) {
			return e.dbAccessDenied()
		}
	}
	if !e.is.SchemaExists(e.DBName) {
		return exeerrors.ErrBadDB.GenWithStackByArgs(e.DBName)
	}
	var (
		tableNames = make([]string, 0)
		showInfos  []*showInfo
		err        error
	)
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	var (
		tableTypes        = make(map[string]string)
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)

	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}

	if fieldFilter != "" {
		showInfos, err = e.fetchShowInfoByName(ctx, fieldFilter)
	} else if e.Full {
		showInfos, err = e.fetchShowFullTables(ctx)
	} else {
		showInfos, err = e.fetchShowSimpleTables(ctx)
	}
	if err != nil {
		return errors.Trace(err)
	}
	for _, v := range showInfos {
		// Test with mysql.AllPrivMask means any privilege would be OK.
		// TODO: Should consider column privileges, which also make a table visible.
		if checker != nil && !checker.RequestVerification(activeRoles, e.DBName.O, v.Name.O, "", mysql.AllPrivMask&(^mysql.CreateTMPTablePriv)) {
			continue
		} else if fieldFilter != "" && v.Name.L != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(v.Name.L) {
			continue
		}
		tableNames = append(tableNames, v.Name.O)
		if e.Full {
			tableTypes[v.Name.O] = v.TableType
		}
	}
	slices.Sort(tableNames)
	for _, v := range tableNames {
		if e.Full {
			e.appendRow([]any{v, tableTypes[v]})
		} else {
			e.appendRow([]any{v})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowTableStatus(ctx context.Context) error {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker != nil && e.Ctx().GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.Ctx().GetSessionVars().ActiveRoles, e.DBName.O) {
			return e.dbAccessDenied()
		}
	}
	if !e.is.SchemaExists(e.DBName) {
		return exeerrors.ErrBadDB.GenWithStackByArgs(e.DBName)
	}

	exec := e.Ctx().GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStats)

	var snapshot uint64
	txn, err := e.Ctx().Txn(false)
	if err != nil {
		return errors.Trace(err)
	}
	if txn.Valid() {
		snapshot = txn.StartTS()
	}
	if e.Ctx().GetSessionVars().SnapshotTS != 0 {
		snapshot = e.Ctx().GetSessionVars().SnapshotTS
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
	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)

	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	for _, row := range rows {
		tableName := row.GetString(0)
		if checker != nil && !checker.RequestVerification(activeRoles, e.DBName.O, tableName, "", mysql.AllPrivMask) {
			continue
		} else if fieldFilter != "" && strings.ToLower(tableName) != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(strings.ToLower(tableName)) {
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
	// we will fill the column type information later, so clone a new table here.
	tb, err = table.TableFromMeta(tb.Allocators(e.Ctx().GetTableCtx()), tb.Meta().Clone())
	if err != nil {
		return errors.Trace(err)
	}
	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)

	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}

	checker := privilege.GetPrivilegeManager(e.Ctx())
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	if checker != nil && e.Ctx().GetSessionVars().User != nil && !checker.RequestVerification(activeRoles, e.DBName.O, tb.Meta().Name.O, "", mysql.InsertPriv|mysql.SelectPriv|mysql.UpdatePriv|mysql.ReferencesPriv) {
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
	if err := tryFillViewColumnType(ctx, e.Ctx(), e.is, e.DBName, tb.Meta()); err != nil {
		return err
	}
	for _, col := range cols {
		if fieldFilter != "" && col.Name.L != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(col.Name.L) {
			continue
		}
		desc := table.NewColDesc(col)
		var columnDefault any
		if desc.DefaultValue != nil {
			// SHOW COLUMNS result expects string value
			defaultValStr := fmt.Sprintf("%v", desc.DefaultValue)
			// If column is timestamp, and default value is not current_timestamp, should convert the default value to the current session time zone.
			if col.GetType() == mysql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr && !strings.HasPrefix(strings.ToUpper(defaultValStr), strings.ToUpper(ast.CurrentTimestamp)) {
				timeValue, err := table.GetColDefaultValue(e.Ctx().GetExprCtx(), col.ToInfo())
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
			e.appendRow([]any{
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
			e.appendRow([]any{
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

