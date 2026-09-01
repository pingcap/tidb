// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	plannererrors "github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/mviewutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
)

const (
	mviewAttrAlertWarning       = "mview_alert_warning"
	mviewAttrAlertOverdue       = "mview_alert_overdue"
	mviewAttrAlertRefreshFailed = "mview_alert_refresh_failed"
)

// FieldTypeForMaterializedViewLogColumn returns the field type used to copy a
// base table column into a materialized view log table.
func FieldTypeForMaterializedViewLogColumn(baseCol *model.ColumnInfo) types.FieldType {
	ft := *baseCol.FieldType.Clone()
	ft.DelFlag(mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag | mysql.AutoIncrementFlag | mysql.OnUpdateNowFlag)
	normalizeMaterializedViewLogBlobFlen(&ft)
	return ft
}

func normalizeMaterializedViewLogBlobFlen(ft *types.FieldType) {
	if ft.GetType() == mysql.TypeBlob && ft.GetFlen() == blobMaxLength {
		ft.SetFlen(types.UnspecifiedLength)
	}
}

func checkMaterializedViewLogColumnSupportedForOp(operation string, col *model.ColumnInfo) error {
	if col.GetType() == mysql.TypeJSON {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("%s does not support JSON column %s", operation, col.Name.O)
	}
	if types.IsTypeBlob(col.GetType()) && col.GetCharset() == charset.CharsetBin {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("%s does not support BLOB column %s", operation, col.Name.O)
	}
	return nil
}

// CheckMaterializedViewLogColumnSupported checks whether a column can be copied into an MV log.
func CheckMaterializedViewLogColumnSupported(col *model.ColumnInfo) error {
	return checkMaterializedViewLogColumnSupportedForOp("CREATE MATERIALIZED VIEW LOG", col)
}

func errUnsupportedMaterializedViewOnPartitionTable(op string) error {
	return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(op + " on partition table")
}

func isValidMaterializedViewLogBaseTable(schemaLowerName string, tblInfo *model.TableInfo) bool {
	return tblInfo != nil &&
		!metadef.IsMemOrSysDB(schemaLowerName) &&
		!tblInfo.IsView() &&
		!tblInfo.IsSequence() &&
		tblInfo.TempTableType == model.TempTableNone &&
		tblInfo.MaterializedView == nil &&
		tblInfo.MaterializedViewLog == nil
}

// ApplyMViewExecutionSessionVars applies MV execution vars onto a session and returns a restore closure.
func ApplyMViewExecutionSessionVars(sessVars *variable.SessionVars, target variable.MViewExecutionSessionVars) (func(), error) {
	return applyMViewExecutionSessionVars(sessVars, target, false)
}

func applyMViewExecutionSessionVars(
	sessVars *variable.SessionVars,
	target variable.MViewExecutionSessionVars,
	bestEffort bool,
) (func(), error) {
	return variable.ApplyMViewExecutionSessionVarsWithConfig(
		sessVars,
		target,
		variable.MViewExecutionSessionVarsApplyConfig{
			MaintainMemQuotaVarName:             variable.TiDBMemQuotaQuery,
			MaintainIsolationReadEnginesVarName: variable.TiDBIsolationReadEngines,
			CaptureAppliedVars:                  variable.CaptureAppliedMViewExecutionSessionVars,
			BestEffort:                          bestEffort,
			InjectApplyError:                    maybeMockMViewExecutionSessionVarApplyError,
			OnApplyError: func(name, value string, err error) {
				logutil.DDLLogger().Warn(
					"mv execution: failed to apply session var, fallback to current session value",
					zap.String("var", name),
					zap.String("value", value),
					zap.Error(err),
				)
			},
			OnRestoreError: func(name, originValue, currentValue string, err error) {
				logutil.DDLLogger().Warn(
					"mv execution: failed to restore session var",
					zap.String("var", name),
					zap.String("origin", originValue),
					zap.String("current", currentValue),
					zap.Error(err),
				)
			},
		},
	)
}

func maybeMockMViewExecutionSessionVarApplyError(varName string) error {
	var err error
	failpoint.Inject("mockMViewExecutionSessionVarApplyError", func(val failpoint.Value) {
		targetVar, ok := val.(string)
		if ok && targetVar == varName {
			err = errors.Errorf("mock mv execution session var apply error: %s", varName)
		}
	})
	return err
}

// AddMViewExecutionSessionVarsToJob snapshots MV execution vars into the DDL job.
func AddMViewExecutionSessionVarsToJob(job *model.Job, sessVars *variable.SessionVars) {
	if job == nil || sessVars == nil {
		return
	}
	if job.SessionVars == nil {
		job.SessionVars = make(map[string]string)
	}
	target := variable.CaptureMViewExecutionSessionVars(sessVars)
	job.AddSystemVars(variable.TiDBMVMaintainMemQuota, strconv.FormatInt(target.MaintainMemQuota, 10))
	job.AddSystemVars(variable.TiDBMVMaintainIsolationReadEngines, target.IsolationReadEngines)
	job.AddSystemVars(variable.TiDBMaxTiFlashThreads, strconv.FormatInt(target.TiFlashMaxThreads, 10))
	job.AddSystemVars(variable.TiDBMaxBytesBeforeTiFlashExternalJoin, strconv.FormatInt(target.TiFlashMaxBytesBeforeExtJoin, 10))
	job.AddSystemVars(variable.TiDBMaxBytesBeforeTiFlashExternalGroupBy, strconv.FormatInt(target.TiFlashMaxBytesBeforeExtAgg, 10))
	job.AddSystemVars(variable.TiDBMaxBytesBeforeTiFlashExternalSort, strconv.FormatInt(target.TiFlashMaxBytesBeforeExtSort, 10))
	job.AddSystemVars(variable.TiFlashMemQuotaQueryPerNode, strconv.FormatInt(target.TiFlashMemQuotaQueryPerNode, 10))
	job.AddSystemVars(variable.TiFlashQuerySpillRatio, strconv.FormatFloat(target.TiFlashQuerySpillRatio, 'f', -1, 64))
	job.AddSystemVars(variable.TiFlashFineGrainedShuffleStreamCount, strconv.FormatInt(target.FineGrainedStreamCount, 10))
	job.AddSystemVars(variable.TiFlashFineGrainedShuffleBatchSize, strconv.FormatUint(target.FineGrainedBatchSize, 10))
	job.AddSystemVars(variable.TiDBMViewMaintainImportThreads, strconv.Itoa(target.ImportThreads))
	job.AddSystemVars(variable.TiDBMViewMaintainImportDiskQuota, target.ImportDiskQuota)
}

// MViewExecutionSessionVarsFromJob reconstructs MV execution vars from a DDL job.
func MViewExecutionSessionVarsFromJob(job *model.Job, defaultSessVars *variable.SessionVars) (variable.MViewExecutionSessionVars, error) {
	target := variable.CaptureAppliedMViewExecutionSessionVars(defaultSessVars)
	if job == nil {
		return target, nil
	}

	if val, ok := job.GetSystemVars(variable.TiDBMVMaintainMemQuota); ok {
		target.MaintainMemQuota = variable.TidbOptInt64(val, target.MaintainMemQuota)
	}
	if val, ok := job.GetSystemVars(variable.TiDBMVMaintainIsolationReadEngines); ok {
		target.IsolationReadEngines = val
	}
	if val, ok := job.GetSystemVars(variable.TiDBMaxTiFlashThreads); ok {
		target.TiFlashMaxThreads = variable.TidbOptInt64(val, target.TiFlashMaxThreads)
	}
	if val, ok := job.GetSystemVars(variable.TiDBMaxBytesBeforeTiFlashExternalJoin); ok {
		target.TiFlashMaxBytesBeforeExtJoin = variable.TidbOptInt64(val, target.TiFlashMaxBytesBeforeExtJoin)
	}
	if val, ok := job.GetSystemVars(variable.TiDBMaxBytesBeforeTiFlashExternalGroupBy); ok {
		target.TiFlashMaxBytesBeforeExtAgg = variable.TidbOptInt64(val, target.TiFlashMaxBytesBeforeExtAgg)
	}
	if val, ok := job.GetSystemVars(variable.TiDBMaxBytesBeforeTiFlashExternalSort); ok {
		target.TiFlashMaxBytesBeforeExtSort = variable.TidbOptInt64(val, target.TiFlashMaxBytesBeforeExtSort)
	}
	if val, ok := job.GetSystemVars(variable.TiFlashMemQuotaQueryPerNode); ok {
		target.TiFlashMemQuotaQueryPerNode = variable.TidbOptInt64(val, target.TiFlashMemQuotaQueryPerNode)
	}
	if val, ok := job.GetSystemVars(variable.TiFlashQuerySpillRatio); ok {
		ratio, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return variable.MViewExecutionSessionVars{}, errors.Annotatef(err, "invalid %s", variable.TiFlashQuerySpillRatio)
		}
		target.TiFlashQuerySpillRatio = ratio
	}
	if val, ok := job.GetSystemVars(variable.TiFlashFineGrainedShuffleStreamCount); ok {
		target.FineGrainedStreamCount = variable.TidbOptInt64(val, target.FineGrainedStreamCount)
	}
	if val, ok := job.GetSystemVars(variable.TiFlashFineGrainedShuffleBatchSize); ok {
		target.FineGrainedBatchSize = uint64(variable.TidbOptInt64(val, int64(target.FineGrainedBatchSize)))
	}
	if val, ok := job.GetSystemVars(variable.TiDBMViewMaintainImportThreads); ok {
		target.ImportThreads = variable.TidbOptInt(val, target.ImportThreads)
	}
	if val, ok := job.GetSystemVars(variable.TiDBMViewMaintainImportDiskQuota); ok {
		target.ImportDiskQuota = val
	}
	return target, nil
}

// BuildMViewImportIntoOptions builds the WITH options shared by MV IMPORT INTO execution.
func BuildMViewImportIntoOptions(importThreads int, importDiskQuota string) []string {
	options := []string{"disable_precheck"}
	if importThreads > 0 {
		options = append(options, fmt.Sprintf("thread=%d", importThreads))
	}
	if importDiskQuota != "" {
		options = append(options, sqlescape.MustEscapeSQL("disk_quota=%?", importDiskQuota))
	}
	return options
}

func checkMaterializedViewEnabled(ctx sessionctx.Context) error {
	if !ctx.GetSessionVars().EnableMaterializedView {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack(
			"Materialized View is disabled, please set `tidb_materialized_view_enable` to `ON` to enable it",
		)
	}
	return nil
}

func (e *executor) CreateMaterializedViewLog(ctx sessionctx.Context, s *ast.CreateMaterializedViewLogStmt) error {
	if err := checkMaterializedViewEnabled(ctx); err != nil {
		return err
	}
	is := e.infoCache.GetLatest()
	schemaName := s.Table.Schema
	if schemaName.O == "" {
		if ctx.GetSessionVars().CurrentDB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = ast.NewCIStr(ctx.GetSessionVars().CurrentDB)
		s.Table.Schema = schemaName
	}
	schema, ok := is.SchemaByName(schemaName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}

	baseTable, err := is.TableByName(e.ctx, schemaName, s.Table.Name)
	if err != nil {
		return err
	}
	baseTableInfo := baseTable.Meta()
	baseTableID := baseTableInfo.ID
	if !isValidMaterializedViewLogBaseTable(schemaName.L, baseTableInfo) {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schemaName, s.Table.Name, "BASE TABLE")
	}
	if baseTableInfo.GetPartitionInfo() != nil {
		return errUnsupportedMaterializedViewOnPartitionTable("CREATE MATERIALIZED VIEW LOG")
	}

	mlogName := model.MaterializedViewLogTableName(baseTableInfo.Name)
	if err := checkTooLongTable(mlogName); err != nil {
		return err
	}
	if _, err = is.TableByName(e.ctx, schemaName, mlogName); err == nil {
		return infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: schemaName, Name: mlogName})
	} else if !infoschema.ErrTableNotExists.Equal(err) {
		return err
	}

	colMap := make(map[string]*model.ColumnInfo, len(baseTableInfo.Columns))
	for _, col := range baseTableInfo.Columns {
		colMap[col.Name.L] = col
	}
	seenCols := make(map[string]struct{}, len(s.Cols))
	colDefs := make([]*ast.ColumnDef, 0, len(s.Cols)+2)
	for _, c := range s.Cols {
		if _, exists := seenCols[c.L]; exists {
			return infoschema.ErrColumnExists.GenWithStackByArgs(c.O)
		}
		seenCols[c.L] = struct{}{}
		if c.L == strings.ToLower(model.MaterializedViewLogDMLTypeColumnName) || c.L == strings.ToLower(model.MaterializedViewLogOldNewColumnName) {
			return infoschema.ErrColumnExists.GenWithStackByArgs(c.O)
		}
		baseCol := colMap[c.L]
		if baseCol == nil {
			return infoschema.ErrColumnNotExists.GenWithStackByArgs(c.O, s.Table.Name.O)
		}
		if err := CheckMaterializedViewLogColumnSupported(baseCol); err != nil {
			return err
		}
		ft := FieldTypeForMaterializedViewLogColumn(baseCol)
		colDefs = append(colDefs, &ast.ColumnDef{Name: &ast.ColumnName{Name: c}, Tp: &ft})
	}

	metaCols := []struct {
		name string
		tp   byte
		flen int
	}{
		{name: model.MaterializedViewLogDMLTypeColumnName, tp: mysql.TypeVarchar, flen: 1},
		{name: model.MaterializedViewLogOldNewColumnName, tp: mysql.TypeTiny, flen: 4},
	}
	for _, metaCol := range metaCols {
		ft := field_types.NewFieldType(metaCol.tp)
		ft.SetFlen(metaCol.flen)
		ft.SetFlag(mysql.NotNullFlag)
		colDefs = append(colDefs, &ast.ColumnDef{Name: &ast.ColumnName{Name: ast.NewCIStr(metaCol.name)}, Tp: ft})
	}

	createTableStmt := &ast.CreateTableStmt{
		Table:   &ast.TableName{Schema: schemaName, Name: mlogName},
		Cols:    colDefs,
		Options: s.Options,
	}
	mlogTableInfo, err := BuildTableInfoWithStmt(
		NewMetaBuildContextWithSctx(ctx),
		createTableStmt,
		schema.Charset,
		schema.Collate,
		schema.PlacementPolicyRef,
	)
	if err != nil {
		return err
	}

	var purgeMethod, purgeStartWith, purgeNext string
	tzName, tzOffset := ddlutil.GetTimeZone(ctx)
	logAccumulationAlertRows, err := BuildMLogAccumulationAlertRows(s.AccumulationAlert)
	if err != nil {
		return err
	}
	if s.Purge != nil {
		if s.Purge.Immediate {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("PURGE IMMEDIATE is not supported for CREATE MATERIALIZED VIEW LOG")
		}
		purgeMethod = "DEFERRED"
		if s.Purge.StartWith != nil {
			purgeStartWith, err = BuildAndValidateMViewScheduleExpr(ctx, s.Purge.StartWith, "PURGE START WITH")
			if err != nil {
				return err
			}
		}
		if s.Purge.Next == nil {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("PURGE NEXT is required for CREATE MATERIALIZED VIEW LOG")
		}
		purgeNext, err = BuildAndValidateMViewScheduleExpr(ctx, s.Purge.Next, "PURGE NEXT")
		if err != nil {
			return err
		}
	}
	mlogTableInfo.MaterializedViewLog = &model.MaterializedViewLogInfo{
		BaseTableID:              baseTableID,
		Columns:                  s.Cols,
		PurgeMethod:              purgeMethod,
		PurgeStartWith:           purgeStartWith,
		PurgeNext:                purgeNext,
		LogAccumulationAlertRows: logAccumulationAlertRows,
		DefinitionSQLMode:        ctx.GetSessionVars().SQLMode,
		PurgeScheduleTimeZone:    model.TimeZoneLocation{Name: tzName, Offset: tzOffset},
	}

	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   schema.ID,
		SchemaName: schema.Name.L,
		TableName:  mlogTableInfo.Name.L,
		Type:       model.ActionCreateMaterializedViewLog,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: schema.Name.L, Table: mlogTableInfo.Name.L},
			{Database: schema.Name.L, Table: baseTableInfo.Name.L},
		},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
		SessionVars:    make(map[string]string),
	}
	job.AddSystemVars(vardef.TiDBScatterRegion, getScatterScopeFromSessionctx(ctx))
	jobW := NewJobWrapperWithArgs(job, &model.CreateMaterializedViewLogArgs{TableInfo: mlogTableInfo}, false)
	if err := e.DoDDLJobWrapper(ctx, jobW); err != nil {
		return errors.Trace(err)
	}
	var scatterScope string
	if val, ok := jobW.GetSystemVars(vardef.TiDBScatterRegion); ok {
		scatterScope = val
	}
	return errors.Trace(e.createTableWithInfoPost(ctx, mlogTableInfo, jobW.SchemaID, scatterScope))
}

func (e *executor) CreateMaterializedView(ctx sessionctx.Context, s *ast.CreateMaterializedViewStmt) error {
	if err := checkMaterializedViewEnabled(ctx); err != nil {
		return err
	}
	sessionVars := ctx.GetSessionVars() //nolint:forbidigo
	is := e.infoCache.GetLatest()
	schemaName := s.ViewName.Schema
	if schemaName.O == "" {
		if sessionVars.CurrentDB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = ast.NewCIStr(sessionVars.CurrentDB)
		s.ViewName.Schema = schemaName
	}
	schema, ok := is.SchemaByName(schemaName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}
	if _, err := validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, s.ViewName.Name.L, &s.Comment, dbterror.ErrTooLongTableComment); err != nil {
		return errors.Trace(err)
	}

	// Stage-1 only supports a single-table SELECT as MV definition input.
	sel, ok := s.Select.(*ast.SelectStmt)
	if !ok {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports SELECT statement")
	}
	baseTableName, err := extractSingleTableNameFromSelect(sel)
	if err != nil {
		return err
	}
	if baseTableName.Schema.L == "" {
		baseTableName.Schema = schemaName
	}
	if baseTableName.Schema.L != schemaName.L {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports base table in the same schema")
	}

	baseTable, err := is.TableByName(e.ctx, baseTableName.Schema, baseTableName.Name)
	if err != nil {
		return err
	}
	if baseTable.Meta().IsView() || baseTable.Meta().IsSequence() || baseTable.Meta().TempTableType != model.TempTableNone {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schemaName, baseTableName.Name, "BASE TABLE")
	}
	if baseTable.Meta().GetPartitionInfo() != nil {
		return errUnsupportedMaterializedViewOnPartitionTable("CREATE MATERIALIZED VIEW")
	}
	baseTableID := baseTable.Meta().ID

	mlogName := model.MaterializedViewLogTableName(baseTable.Meta().Name)
	mlogTable, err := is.TableByName(e.ctx, baseTableName.Schema, mlogName)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.Errorf("materialized view log does not exist for base table %s.%s", baseTableName.Schema.O, baseTableName.Name.O)
		}
		return err
	}
	if mlogTable.Meta().MaterializedViewLog == nil || mlogTable.Meta().MaterializedViewLog.BaseTableID != baseTableID {
		return errors.Errorf("table %s.%s is not a materialized view log for base table %s.%s", baseTableName.Schema.O, mlogName.O, baseTableName.Schema.O, baseTableName.Name.O)
	}

	// Validate Stage-1 query contract and ensure MV LOG columns cover query references.
	queryAnalysis, err := validateCreateMaterializedViewQuery(
		ctx,
		baseTableName,
		baseTable.Meta(),
		mlogTable.Meta().MaterializedViewLog.Columns,
		s.Select,
	)
	if err != nil {
		return err
	}
	groupByInfos := queryAnalysis.GroupByInfos
	normalizeMVDefinitionHintDBNames(s.Select, schemaName)

	selectSQL, err := restoreNodeToCanonicalSQL(s.Select)
	if err != nil {
		return err
	}

	// Derive MV physical column types from the query output schema.
	exec := ctx.GetRestrictedSQLExecutor()
	kctx := kv.WithInternalSourceType(e.ctx, kv.InternalTxnDDL)
	/* #nosec G202: selectSQL is restored from AST (single statement, no user-provided placeholders). */
	_, resultFields, err := exec.ExecRestrictedSQL(kctx, nil, "SELECT * FROM ("+selectSQL+") AS `tidb_mv_query` LIMIT 0")
	if err != nil {
		return err
	}
	if len(resultFields) != len(s.Cols) {
		return errors.Errorf("materialized view column count %d does not match query output %d", len(s.Cols), len(resultFields))
	}

	colDefs := make([]*ast.ColumnDef, 0, len(resultFields))
	for i, rf := range resultFields {
		ft := rf.Column.FieldType
		ft.DelFlag(mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag | mysql.AutoIncrementFlag | mysql.OnUpdateNowFlag)
		colDefs = append(colDefs, &ast.ColumnDef{
			Name: &ast.ColumnName{Name: s.Cols[i]},
			Tp:   &ft,
		})
	}

	// Build group-key index for one-row-per-group semantics (PK when all keys are NOT NULL, else UNIQUE).
	keys := make([]*ast.IndexPartSpecification, 0, len(groupByInfos))
	allGroupByNotNull := true
	for _, info := range groupByInfos {
		keys = append(keys, &ast.IndexPartSpecification{
			Column: &ast.ColumnName{Name: s.Cols[info.SelectIdx]},
			Length: types.UnspecifiedLength,
		})
		if !info.NotNull {
			allGroupByNotNull = false
		}
	}

	constraintType := ast.ConstraintUniq
	if allGroupByNotNull {
		constraintType = ast.ConstraintPrimaryKey
	}
	constraints := []*ast.Constraint{{Tp: constraintType, Keys: keys}}

	createTableStmt := &ast.CreateTableStmt{
		Table:       s.ViewName,
		Cols:        colDefs,
		Constraints: constraints,
		Options:     s.Options,
	}
	mvTableInfo, err := BuildTableInfoWithStmt(
		NewMetaBuildContextWithSctx(ctx),
		createTableStmt,
		schema.Charset,
		schema.Collate,
		schema.PlacementPolicyRef,
	)
	if err != nil {
		return err
	}
	mvTableInfo.Comment = s.Comment

	refreshMethod, refreshStartWith, refreshNext, err := buildMViewRefreshMeta(ctx, s.Refresh)
	if err != nil {
		return err
	}
	alertWarningSec, alertOverdueSec, alertRefreshFailed, err := parseMViewAttributes(s.Attributes)
	if err != nil {
		return err
	}
	tzName, tzOffset := ddlutil.GetTimeZone(ctx)
	mvTableInfo.MaterializedView = &model.MaterializedViewInfo{
		BaseTableIDs:                    []int64{baseTableID},
		InitBuildState:                  model.MViewInitBuildBuilding,
		SQLContent:                      selectSQL,
		RefreshMethod:                   refreshMethod,
		RefreshStartWith:                refreshStartWith,
		RefreshNext:                     refreshNext,
		AlertWarningSec:                 alertWarningSec,
		AlertOverdueSec:                 alertOverdueSec,
		AlertRefreshFailed:              alertRefreshFailed,
		DefinitionSQLMode:               sessionVars.SQLMode,
		DefinitionDivPrecisionIncrement: sessionVars.DivPrecisionIncrement,
		DefinitionTimeZone: model.TimeZoneLocation{
			Name:   tzName,
			Offset: tzOffset,
		},
		RefreshScheduleTimeZone: model.TimeZoneLocation{
			Name:   tzName,
			Offset: tzOffset,
		},
	}

	// CREATE MATERIALIZED VIEW is submitted as reorg DDL: create table first, then initial build in reorg phase.
	involvingSchemas := []model.InvolvingSchemaInfo{
		{Database: schema.Name.L, Table: mvTableInfo.Name.L},
		{Database: schema.Name.L, Table: baseTable.Meta().Name.L},
		{Database: schema.Name.L, Table: mlogTable.Meta().Name.L},
	}
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schema.ID,
		SchemaName:          schema.Name.L,
		TableName:           mvTableInfo.Name.L,
		Type:                model.ActionCreateMaterializedView,
		BinlogInfo:          &model.HistoryInfo{},
		InvolvingSchemaInfo: involvingSchemas,
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		SQLMode:             ctx.GetSessionVars().SQLMode,
		SessionVars:         make(map[string]string),
	}
	if err := initMaterializedViewReorgMetaFromVariables(job, ctx); err != nil {
		return err
	}
	job.AddSystemVars(variable.TiDBScatterRegion, getScatterScopeFromSessionctx(ctx))
	AddMViewExecutionSessionVarsToJob(job, sessionVars)
	jobW := NewJobWrapperWithArgs(job, &model.CreateMaterializedViewArgs{
		TableInfo:    mvTableInfo,
		MLogTableIDs: []int64{mlogTable.Meta().ID},
	}, false)
	if err := e.DoDDLJobWrapper(ctx, jobW); err != nil {
		return errors.Trace(err)
	}

	var scatterScope string
	if val, ok := jobW.GetSystemVars(variable.TiDBScatterRegion); ok {
		scatterScope = val
	}
	return errors.Trace(e.createTableWithInfoPost(ctx, mvTableInfo, jobW.SchemaID, scatterScope))
}

func initMaterializedViewReorgMetaFromVariables(job *model.Job, sctx sessionctx.Context) error {
	m := NewDDLReorgMeta(sctx)
	sessionVars := sctx.GetSessionVars() //nolint:forbidigo
	if sv, ok := sessionVars.GetSystemVar(vardef.TiDBDDLReorgWorkerCount); ok {
		m.SetConcurrency(variable.TidbOptInt(sv, 0))
	}
	if sv, ok := sessionVars.GetSystemVar(vardef.TiDBDDLReorgBatchSize); ok {
		m.SetBatchSize(variable.TidbOptInt(sv, 0))
	}
	m.SetMaxWriteSpeed(int(vardef.DDLReorgMaxWriteSpeed.Load()))
	job.ReorgMeta = m
	return nil
}

func buildMLogPurgeMeta(sctx sessionctx.Context, purge *ast.MLogPurgeClause) (method, startWith, next string, _ error) {
	if purge == nil {
		return "", "", "", nil
	}
	if purge.Immediate {
		return "", "", "", dbterror.ErrGeneralUnsupportedDDL.GenWithStack("PURGE IMMEDIATE is not supported for ALTER MATERIALIZED VIEW LOG")
	}

	method = "DEFERRED"
	if purge.StartWith != nil {
		s, err := BuildAndValidateMViewScheduleExpr(sctx, purge.StartWith, "PURGE START WITH")
		if err != nil {
			return "", "", "", err
		}
		startWith = s
	}
	if purge.Next != nil {
		s, err := BuildAndValidateMViewScheduleExpr(sctx, purge.Next, "PURGE NEXT")
		if err != nil {
			return "", "", "", err
		}
		next = s
	}
	return method, startWith, next, nil
}

// BuildMLogAccumulationAlertRows validates the ALERT ROWS clause and returns the persisted threshold.
// nil means the CREATE statement did not specify ALERT ROWS.
func BuildMLogAccumulationAlertRows(alert *ast.MLogAccumulationAlertClause) (*uint64, error) {
	if alert == nil {
		return nil, nil
	}
	if alert.Rows < 0 {
		return nil, errors.Errorf("invalid ALERT ROWS value: %d (must be non-negative)", alert.Rows)
	}
	rows := uint64(alert.Rows)
	return &rows, nil
}

func buildMViewRefreshMeta(sctx sessionctx.Context, refresh *ast.MViewRefreshClause) (method, startWith, next string, _ error) {
	if refresh == nil {
		return "FAST", "", "", nil
	}
	switch refresh.Method {
	case ast.MViewRefreshMethodFast:
		method = "FAST"
		if refresh.StartWith != nil {
			s, err := BuildAndValidateMViewScheduleExpr(sctx, refresh.StartWith, "REFRESH START WITH")
			if err != nil {
				return "", "", "", err
			}
			startWith = s
		}
		if refresh.Next != nil {
			s, err := BuildAndValidateMViewScheduleExpr(sctx, refresh.Next, "REFRESH NEXT")
			if err != nil {
				return "", "", "", err
			}
			next = s
		}
		return method, startWith, next, nil
	default:
		return "", "", "", errors.New("unknown refresh method")
	}
}

func parseMViewAttributes(attrs string) (alertWarningSec, alertOverdueSec int64, alertRefreshFailed bool, err error) {
	attrs = strings.TrimSpace(attrs)
	if attrs == "" {
		return 0, 0, false, nil
	}

	seen := make(map[string]struct{}, 3)
	for _, rawKV := range strings.Split(attrs, ",") {
		kv := strings.TrimSpace(rawKV)
		if kv == "" {
			return 0, 0, false, errors.New("invalid ATTRIBUTES format: empty key-value pair")
		}
		pos := strings.Index(kv, "=")
		if pos <= 0 || pos >= len(kv)-1 {
			return 0, 0, false, errors.Errorf("invalid ATTRIBUTES format: %q", kv)
		}
		key := strings.ToLower(strings.TrimSpace(kv[:pos]))
		valStr := strings.TrimSpace(kv[pos+1:])
		if key == "" || valStr == "" {
			return 0, 0, false, errors.Errorf("invalid ATTRIBUTES format: %q", kv)
		}
		if _, ok := seen[key]; ok {
			return 0, 0, false, errors.Errorf("duplicate ATTRIBUTES key: %s", key)
		}
		seen[key] = struct{}{}

		switch key {
		case mviewAttrAlertWarning:
			val, convErr := strconv.ParseInt(valStr, 10, 64)
			if convErr != nil || val < 0 {
				return 0, 0, false, errors.Errorf("invalid ATTRIBUTES value for %s: %s (must be non-negative integer seconds)", key, valStr)
			}
			alertWarningSec = val
		case mviewAttrAlertOverdue:
			val, convErr := strconv.ParseInt(valStr, 10, 64)
			if convErr != nil || val < 0 {
				return 0, 0, false, errors.Errorf("invalid ATTRIBUTES value for %s: %s (must be non-negative integer seconds)", key, valStr)
			}
			alertOverdueSec = val
		case mviewAttrAlertRefreshFailed:
			switch strings.ToLower(valStr) {
			case "yes":
				alertRefreshFailed = true
			case "no":
				alertRefreshFailed = false
			default:
				return 0, 0, false, errors.Errorf("invalid ATTRIBUTES value for %s: %s (must be yes or no)", key, valStr)
			}
		default:
			return 0, 0, false, errors.Errorf("unsupported ATTRIBUTES key: %s", key)
		}
	}

	if alertWarningSec > 0 && alertOverdueSec > 0 && alertWarningSec > alertOverdueSec {
		return 0, 0, false, errors.Errorf("invalid ATTRIBUTES: %s (%d) must be less than or equal to %s (%d)",
			mviewAttrAlertWarning, alertWarningSec, mviewAttrAlertOverdue, alertOverdueSec)
	}
	return alertWarningSec, alertOverdueSec, alertRefreshFailed, nil
}

type mviewGroupByInfo struct {
	SelectIdx int
	NotNull   bool
}

type mviewQueryAnalysis struct {
	GroupByInfos []mviewGroupByInfo
	GroupByCols  []string
	HasMinOrMax  bool
}

func validateCreateMaterializedViewQuery(
	sctx sessionctx.Context,
	baseTableName *ast.TableName,
	baseTableInfo *model.TableInfo,
	mlogColumns []ast.CIStr,
	selectNode ast.ResultSetNode,
) (*mviewQueryAnalysis, error) {
	sel, ok := selectNode.(*ast.SelectStmt)
	if !ok {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports SELECT statement")
	}

	fromTbl, fromAlias, err := extractSingleTableNameAndAliasFromSelect(sel)
	if err != nil {
		return nil, err
	}
	if fromTbl.Schema.L == "" {
		fromTbl.Schema = baseTableName.Schema
	}
	if fromTbl.Schema.L != baseTableName.Schema.L || fromTbl.Name.L != baseTableName.Name.L {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports a single base table")
	}

	if sel.GroupBy == nil || len(sel.GroupBy.Items) == 0 {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW requires GROUP BY clause")
	}
	if sel.GroupBy.Rollup {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support GROUP BY WITH ROLLUP")
	}
	if sel.Having != nil {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support HAVING clause")
	}
	if sel.OrderBy != nil {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support ORDER BY clause")
	}
	if sel.Limit != nil {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support LIMIT clause")
	}
	if sel.Distinct {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support SELECT DISTINCT")
	}

	baseColMap := make(map[string]*model.ColumnInfo, len(baseTableInfo.Columns))
	for _, c := range baseTableInfo.Columns {
		baseColMap[c.Name.L] = c
	}

	mlogColSet := make(map[string]struct{}, len(mlogColumns))
	for _, c := range mlogColumns {
		mlogColSet[c.L] = struct{}{}
	}

	groupBySet := make(map[string]struct{}, len(sel.GroupBy.Items))
	groupByCols := make([]string, 0, len(sel.GroupBy.Items))
	groupByNotNull := make(map[string]bool, len(sel.GroupBy.Items))
	countExprCols := make(map[string]struct{})
	nullableSumCols := make(map[string]struct{})
	usedCols := make(map[string]struct{}, 8)

	for _, item := range sel.GroupBy.Items {
		colExpr, ok := item.Expr.(*ast.ColumnNameExpr)
		if !ok {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("GROUP BY expression is not supported in CREATE MATERIALIZED VIEW")
		}
		colName, err := resolveMViewColumnName(colExpr.Name, baseTableName, fromAlias, baseColMap)
		if err != nil {
			return nil, err
		}
		if _, exists := groupBySet[colName]; exists {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("duplicate GROUP BY column is not supported in CREATE MATERIALIZED VIEW")
		}
		baseCol := baseColMap[colName]
		groupBySet[colName] = struct{}{}
		groupByCols = append(groupByCols, colName)
		groupByNotNull[colName] = mysql.HasNotNullFlag(baseCol.GetFlag())
		usedCols[colName] = struct{}{}
	}

	if sel.Where != nil {
		expr, err := buildMViewSingleTableExpr(sctx, baseTableName, fromAlias, baseTableInfo, sel.Where)
		if err != nil {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW WHERE clause is not supported")
		}
		if expression.CheckNonDeterministic(expr) {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW WHERE clause must be deterministic")
		}
		for _, col := range collectColumnNamesInExpr(sel.Where) {
			colName, err := resolveMViewColumnName(col, baseTableName, fromAlias, baseColMap)
			if err != nil {
				return nil, err
			}
			usedCols[colName] = struct{}{}
		}
	}

	selectColIdx := make(map[string]int, len(sel.Fields.Fields))
	hasCountStarOrOne := false
	hasMinOrMax := false
	for i, f := range sel.Fields.Fields {
		if f.WildCard != nil {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support wildcard select field")
		}
		switch expr := f.Expr.(type) {
		case *ast.ColumnNameExpr:
			colName, err := resolveMViewColumnName(expr.Name, baseTableName, fromAlias, baseColMap)
			if err != nil {
				return nil, err
			}
			if _, ok := groupBySet[colName]; !ok {
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("non-aggregated column must appear in GROUP BY clause")
			}
			if _, exists := selectColIdx[colName]; exists {
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("duplicate GROUP BY column in SELECT list is not supported in CREATE MATERIALIZED VIEW")
			}
			selectColIdx[colName] = i
			usedCols[colName] = struct{}{}
		case *ast.AggregateFuncExpr:
			if expr.Distinct {
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support DISTINCT aggregate function")
			}
			aggFunc := strings.ToLower(expr.F)
			if aggFunc != ast.AggFuncCount && aggFunc != ast.AggFuncSum && aggFunc != ast.AggFuncMin && aggFunc != ast.AggFuncMax {
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("unsupported aggregate function in CREATE MATERIALIZED VIEW" + " agg " + expr.F)
			}
			switch aggFunc {
			case ast.AggFuncCount:
				if len(expr.Args) != 1 {
					return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("count(*)/count(1) must have exactly one argument in CREATE MATERIALIZED VIEW")
				}
				if argCol, ok := expr.Args[0].(*ast.ColumnNameExpr); ok {
					// count(column) is supported.
					colName, err := resolveMViewColumnName(argCol.Name, baseTableName, fromAlias, baseColMap)
					if err != nil {
						return nil, err
					}
					countExprCols[colName] = struct{}{}
					usedCols[colName] = struct{}{}
					continue
				}
				if expr.Args[0] == nil {
					hasCountStarOrOne = true
					continue
				}
				if !isCountStarOrOne(expr.Args[0]) {
					return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports count(*)/count(1)")
				}
				hasCountStarOrOne = true
			case ast.AggFuncSum, ast.AggFuncMin, ast.AggFuncMax:
				if len(expr.Args) != 1 {
					return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("aggregate function must have exactly one argument in CREATE MATERIALIZED VIEW")
				}
				argCol, ok := expr.Args[0].(*ast.ColumnNameExpr)
				if !ok {
					return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("aggregate function only supports column argument in CREATE MATERIALIZED VIEW")
				}
				colName, err := resolveMViewColumnName(argCol.Name, baseTableName, fromAlias, baseColMap)
				if err != nil {
					return nil, err
				}
				if aggFunc == ast.AggFuncSum {
					tp := baseColMap[colName].GetType()
					if types.IsTypeTime(tp) || tp == mysql.TypeDuration {
						return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support SUM on DATE/DATETIME/TIMESTAMP/TIME column")
					}
					if !mysql.HasNotNullFlag(baseColMap[colName].GetFlag()) {
						nullableSumCols[colName] = struct{}{}
					}
				}
				if aggFunc == ast.AggFuncMin || aggFunc == ast.AggFuncMax {
					hasMinOrMax = true
				}
				usedCols[colName] = struct{}{}
			default:
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("unsupported aggregate function in CREATE MATERIALIZED VIEW" + " agg " + expr.F)
			}
		default:
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("unsupported SELECT expression in CREATE MATERIALIZED VIEW")
		}
	}
	if !hasCountStarOrOne {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW must contain count(*)/count(1)")
	}
	for colName := range nullableSumCols {
		if _, ok := countExprCols[colName]; !ok {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack(
				fmt.Sprintf("CREATE MATERIALIZED VIEW SUM on nullable column %s requires matching COUNT(%s) in SELECT list", colName, colName),
			)
		}
	}

	groupByInfos := make([]mviewGroupByInfo, 0, len(sel.GroupBy.Items))
	for _, item := range sel.GroupBy.Items {
		colExpr := item.Expr.(*ast.ColumnNameExpr)
		colName, err := resolveMViewColumnName(colExpr.Name, baseTableName, fromAlias, baseColMap)
		if err != nil {
			return nil, err
		}
		idx, ok := selectColIdx[colName]
		if !ok {
			return nil, errors.Errorf("GROUP BY column %s must appear in SELECT list", colExpr.Name.Name.O)
		}
		groupByInfos = append(groupByInfos, mviewGroupByInfo{SelectIdx: idx, NotNull: groupByNotNull[colName]})
	}

	if hasMinOrMax && !hasVisiblePublicIndexWithPrefixCoveringGroupByColumns(baseTableInfo, groupByCols, "") {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW with MIN/MAX requires base table index whose leading columns cover all GROUP BY columns")
	}

	for colName := range usedCols {
		if _, ok := mlogColSet[colName]; !ok {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack(fmt.Sprintf("materialized view log does not contain column %s", colName))
		}
	}

	return &mviewQueryAnalysis{
		GroupByInfos: groupByInfos,
		GroupByCols:  groupByCols,
		HasMinOrMax:  hasMinOrMax,
	}, nil
}

func extractSingleTableNameFromSelect(sel *ast.SelectStmt) (*ast.TableName, error) {
	tbl, _, err := extractSingleTableNameAndAliasFromSelect(sel)
	return tbl, err
}

func extractSingleTableNameAndAliasFromSelect(sel *ast.SelectStmt) (*ast.TableName, ast.CIStr, error) {
	if sel.From == nil || sel.From.TableRefs == nil || sel.From.TableRefs.Left == nil || sel.From.TableRefs.Right != nil {
		return nil, ast.CIStr{}, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports a single base table")
	}
	ts, ok := sel.From.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, ast.CIStr{}, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports a single base table")
	}
	tbl, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil, ast.CIStr{}, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports a single base table")
	}
	return tbl, ts.AsName, nil
}

func buildMViewSingleTableExpr(sctx sessionctx.Context, baseTableName *ast.TableName, fromAlias ast.CIStr, baseTableInfo *model.TableInfo, expr ast.ExprNode) (expression.Expression, error) {
	resolveTableName := baseTableName.Name
	if fromAlias.L != "" {
		resolveTableName = fromAlias
	}
	cols, names, err := expression.ColumnInfos2ColumnsAndNames(sctx.GetExprCtx(), baseTableName.Schema, resolveTableName, baseTableInfo.Cols(), baseTableInfo)
	if err != nil {
		return nil, err
	}
	return expression.BuildSimpleExpr(
		sctx.GetExprCtx(),
		expr,
		expression.WithInputSchemaAndNames(expression.NewSchema(cols...), names, baseTableInfo),
	)
}

func resolveMViewColumnName(col *ast.ColumnName, baseTableName *ast.TableName, fromAlias ast.CIStr, baseColMap map[string]*model.ColumnInfo) (string, error) {
	if col == nil {
		return "", dbterror.ErrGeneralUnsupportedDDL.GenWithStack("column reference is nil in CREATE MATERIALIZED VIEW")
	}
	if col.Schema.L != "" && col.Schema.L != baseTableName.Schema.L {
		return "", infoschema.ErrColumnNotExists.GenWithStackByArgs(col.Name.O, baseTableName.Name.O)
	}
	if col.Table.L != "" {
		if col.Table.L != baseTableName.Name.L && (fromAlias.L == "" || col.Table.L != fromAlias.L) {
			return "", infoschema.ErrColumnNotExists.GenWithStackByArgs(col.Name.O, baseTableName.Name.O)
		}
	}
	colName := col.Name.L
	if _, ok := baseColMap[colName]; !ok {
		return "", infoschema.ErrColumnNotExists.GenWithStackByArgs(col.Name.O, baseTableName.Name.O)
	}
	return colName, nil
}

func collectColumnNamesInExpr(expr ast.ExprNode) []*ast.ColumnName {
	collector := &columnNameCollector{cols: make([]*ast.ColumnName, 0, 8)}
	expr.Accept(collector)
	return collector.cols
}

type columnNameCollector struct {
	cols []*ast.ColumnName
}

func (c *columnNameCollector) Enter(n ast.Node) (ast.Node, bool) {
	if x, ok := n.(*ast.ColumnNameExpr); ok {
		c.cols = append(c.cols, x.Name)
	}
	return n, false
}

func (*columnNameCollector) Leave(n ast.Node) (ast.Node, bool) { return n, true }

func isCountStarOrOne(arg ast.ExprNode) bool {
	v, ok := arg.(*driver.ValueExpr)
	return ok && v.Kind() == types.KindInt64 && v.GetInt64() == 1
}

func hasIndexWithPrefixCoveringGroupByColumns(baseTableInfo *model.TableInfo, groupByCols []string) bool {
	return mviewutil.HasIndexWithPrefixCoveringColumns(baseTableInfo, groupByCols, "", false)
}

func hasVisiblePublicIndexWithPrefixCoveringGroupByColumns(
	baseTableInfo *model.TableInfo,
	groupByCols []string,
	excludedIndexName string,
) bool {
	return mviewutil.HasIndexWithPrefixCoveringColumns(baseTableInfo, groupByCols, excludedIndexName, true)
}

func buildDeleteMViewRefreshAlertSQL(mviewID int64) string {
	return sqlescape.MustEscapeSQL("DELETE FROM mysql.tidb_mview_refresh_alert WHERE MVIEW_ID = %?", mviewID)
}

func restoreNodeToCanonicalSQL(node ast.Node) (string, error) {
	var sb strings.Builder
	rctx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb)
	if err := node.Restore(rctx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func normalizeMVDefinitionHintDBNames(node ast.Node, defaultDB ast.CIStr) {
	if node == nil || defaultDB.L == "" {
		return
	}
	_, _ = node.Accept(&mvDefinitionHintDBNameNormalizer{defaultDB: defaultDB})
}

type mvDefinitionHintDBNameNormalizer struct {
	defaultDB ast.CIStr
}

func (v *mvDefinitionHintDBNameNormalizer) Enter(node ast.Node) (ast.Node, bool) {
	hint, ok := node.(*ast.TableOptimizerHint)
	if !ok {
		return node, false
	}
	for i := range hint.Tables {
		if hint.Tables[i].DBName.L == "" {
			hint.Tables[i].DBName = v.defaultDB
		}
	}
	return hint, true
}

func (*mvDefinitionHintDBNameNormalizer) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}
