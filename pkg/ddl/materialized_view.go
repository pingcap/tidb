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
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	mvmerge "github.com/pingcap/tidb/pkg/planner/mview"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	exeerrors "github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	plannererrors "github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
)

const (
	materializedViewLogTablePrefix                        = "$mlog$"
	mviewAttrAlertWarning                                 = "mview_alert_warning"
	mviewAttrAlertOverdue                                 = "mview_alert_overdue"
	alterMaterializedScheduleInfoUpdateLockWaitTimeoutSec = int64(10)
)

var (
	// MLogTableNameSeq allocates numeric components for mlog names derived from
	// base tables whose names already start with "$mlog$".
	MLogTableNameSeq atomic.Uint64
	// MLogShortTableNameSeq allocates numeric components for shortened mlog names
	// used when the derived name exceeds TiDB's table-name length limit.
	MLogShortTableNameSeq atomic.Uint64
)

// GenerateMLogTableName generates an available mlog table name for the base table.
//
// TODO(xzx) add ut for this function
func GenerateMLogTableName(
	baseTableName pmodel.CIStr,
	checkTableExistence func(pmodel.CIStr) (bool, error),
) (pmodel.CIStr, error) {
	if checkTableExistence == nil {
		return pmodel.CIStr{}, errors.New("materialized view log table name exists checker is nil")
	}

	var candidate pmodel.CIStr
	if strings.HasPrefix(baseTableName.L, materializedViewLogTablePrefix) {
		suffix := baseTableName.O[len(materializedViewLogTablePrefix):]
		for {
			var err error
			number, err := nextMLogTableNameNumber(
				&MLogTableNameSeq,
				"materialized view log table name number is out of range",
			)
			if err != nil {
				return pmodel.CIStr{}, err
			}
			candidate = pmodel.NewCIStr(materializedViewLogTablePrefix + strconv.FormatUint(number, 10) + suffix)
			exists, err := checkTableExistence(candidate)
			if err != nil {
				return pmodel.CIStr{}, err
			}
			if !exists {
				break
			}
		}
	} else {
		candidate = pmodel.NewCIStr(materializedViewLogTablePrefix + baseTableName.O)
	}

	if utf8.RuneCountInString(candidate.L) <= mysql.MaxTableNameLength {
		return candidate, nil
	}

	// TODO(xzx) more ut for this code
	for {
		var err error
		number, err := nextMLogTableNameNumber(
			&MLogShortTableNameSeq,
			"materialized view log short table name number is out of range",
		)
		if err != nil {
			return pmodel.CIStr{}, err
		}
		candidate = pmodel.NewCIStr(materializedViewLogTablePrefix + strconv.FormatUint(number, 10))
		if utf8.RuneCountInString(candidate.L) > mysql.MaxTableNameLength {
			return pmodel.CIStr{}, dbterror.ErrTooLongIdent.GenWithStackByArgs(candidate)
		}
		exists, err := checkTableExistence(candidate)
		if err != nil {
			return pmodel.CIStr{}, err
		}
		if !exists {
			return candidate, nil
		}
	}
}

func nextMLogTableNameNumber(counter *atomic.Uint64, outOfRangeErr string) (uint64, error) {
	if counter.Load() == math.MaxUint64 {
		return 0, errors.New(outOfRangeErr)
	}
	next := counter.Add(1)
	if next == 0 {
		return 0, errors.New(outOfRangeErr)
	}
	return next, nil
}

func getExistenceOfMLogTableNameChecker(
	ctx context.Context,
	is infoschema.InfoSchema,
	schemaName pmodel.CIStr,
) func(pmodel.CIStr) (bool, error) {
	return func(tableName pmodel.CIStr) (bool, error) {
		_, err := is.TableByName(ctx, schemaName, tableName)
		if err == nil {
			return true, nil
		}
		if infoschema.ErrTableNotExists.Equal(err) {
			return false, nil
		}
		return false, err
	}
}

// GetExistenceOfMLogTableNameChecker returns a checker for whether an mlog table name already exists.
func GetExistenceOfMLogTableNameChecker(
	ctx context.Context,
	is infoschema.InfoSchema,
	schemaName pmodel.CIStr,
) func(pmodel.CIStr) (bool, error) {
	return getExistenceOfMLogTableNameChecker(ctx, is, schemaName)
}

// GetMLogTableByBaseTable returns the mlog table recorded on the base table metadata.
func GetMLogTableByBaseTable(
	ctx context.Context,
	is infoschema.InfoSchema,
	schemaName pmodel.CIStr,
	baseTableMeta *model.TableInfo,
) (table.Table, error) {
	return getMLogTableByBaseTable(ctx, is, schemaName, baseTableMeta)
}

func getMLogTableByBaseTable(
	ctx context.Context,
	is infoschema.InfoSchema,
	schemaName pmodel.CIStr,
	baseTableMeta *model.TableInfo,
) (table.Table, error) {
	if baseTableMeta == nil {
		return nil, errors.New("materialized view log: invalid base table metadata")
	}
	if baseTableMeta.MaterializedViewBase == nil || baseTableMeta.MaterializedViewBase.MLogID == 0 {
		return nil, errors.Errorf(
			"materialized view log does not exist for base table %s.%s",
			schemaName.O,
			baseTableMeta.Name.O,
		)
	}

	mlogID := baseTableMeta.MaterializedViewBase.MLogID
	mlogTable, ok := is.TableByID(ctx, mlogID)
	if !ok {
		return nil, errors.Errorf(
			"materialized view log does not exist for base table %s.%s",
			schemaName.O,
			baseTableMeta.Name.O,
		)
	}

	mlogInfo := mlogTable.Meta().MaterializedViewLog
	if mlogInfo == nil || mlogInfo.BaseTableID != baseTableMeta.ID {
		return nil, errors.Errorf(
			"table %s.%s is not a materialized view log for base table %s.%s",
			schemaName.O,
			mlogTable.Meta().Name.O,
			schemaName.O,
			baseTableMeta.Name.O,
		)
	}
	return mlogTable, nil
}

// ApplyMViewExecutionSessionVars applies MV execution vars onto a session and returns a restore closure.
func ApplyMViewExecutionSessionVars(sessVars *variable.SessionVars, target variable.MViewExecutionSessionVars) (func(), error) {
	return applyMViewExecutionSessionVars(sessVars, target, false)
}

// ApplyMViewExecutionSessionVarsBestEffort applies MV execution vars onto a session and falls back
// to the session's current value for any individual variable that fails to apply.
func ApplyMViewExecutionSessionVarsBestEffort(sessVars *variable.SessionVars, target variable.MViewExecutionSessionVars) (func(), error) {
	return applyMViewExecutionSessionVars(sessVars, target, true)
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
	job.AddSessionVars(variable.TiDBMVMaintainMemQuota, strconv.FormatInt(target.MaintainMemQuota, 10))
	job.AddSessionVars(variable.TiDBMVMaintainIsolationReadEngines, target.IsolationReadEngines)
	job.AddSessionVars(variable.TiDBMaxTiFlashThreads, strconv.FormatInt(target.TiFlashMaxThreads, 10))
	job.AddSessionVars(variable.TiDBMaxBytesBeforeTiFlashExternalJoin, strconv.FormatInt(target.TiFlashMaxBytesBeforeExtJoin, 10))
	job.AddSessionVars(variable.TiDBMaxBytesBeforeTiFlashExternalGroupBy, strconv.FormatInt(target.TiFlashMaxBytesBeforeExtAgg, 10))
	job.AddSessionVars(variable.TiDBMaxBytesBeforeTiFlashExternalSort, strconv.FormatInt(target.TiFlashMaxBytesBeforeExtSort, 10))
	job.AddSessionVars(variable.TiFlashMemQuotaQueryPerNode, strconv.FormatInt(target.TiFlashMemQuotaQueryPerNode, 10))
	job.AddSessionVars(variable.TiFlashQuerySpillRatio, strconv.FormatFloat(target.TiFlashQuerySpillRatio, 'f', -1, 64))
	job.AddSessionVars(variable.TiFlashFineGrainedShuffleStreamCount, strconv.FormatInt(target.FineGrainedStreamCount, 10))
	job.AddSessionVars(variable.TiFlashFineGrainedShuffleBatchSize, strconv.FormatUint(target.FineGrainedBatchSize, 10))
	job.AddSessionVars(variable.TiDBMViewMaintainImportThreads, strconv.Itoa(target.ImportThreads))
	job.AddSessionVars(variable.TiDBMViewMaintainImportDiskQuota, target.ImportDiskQuota)
}

// MViewExecutionSessionVarsFromJob reconstructs MV execution vars from a DDL job.
func MViewExecutionSessionVarsFromJob(job *model.Job, defaultSessVars *variable.SessionVars) (variable.MViewExecutionSessionVars, error) {
	target := variable.CaptureAppliedMViewExecutionSessionVars(defaultSessVars)
	if job == nil {
		return target, nil
	}

	if val, ok := job.GetSessionVars(variable.TiDBMVMaintainMemQuota); ok {
		target.MaintainMemQuota = variable.TidbOptInt64(val, target.MaintainMemQuota)
	}
	if val, ok := job.GetSessionVars(variable.TiDBMVMaintainIsolationReadEngines); ok {
		target.IsolationReadEngines = val
	}
	if val, ok := job.GetSessionVars(variable.TiDBMaxTiFlashThreads); ok {
		target.TiFlashMaxThreads = variable.TidbOptInt64(val, target.TiFlashMaxThreads)
	}
	if val, ok := job.GetSessionVars(variable.TiDBMaxBytesBeforeTiFlashExternalJoin); ok {
		target.TiFlashMaxBytesBeforeExtJoin = variable.TidbOptInt64(val, target.TiFlashMaxBytesBeforeExtJoin)
	}
	if val, ok := job.GetSessionVars(variable.TiDBMaxBytesBeforeTiFlashExternalGroupBy); ok {
		target.TiFlashMaxBytesBeforeExtAgg = variable.TidbOptInt64(val, target.TiFlashMaxBytesBeforeExtAgg)
	}
	if val, ok := job.GetSessionVars(variable.TiDBMaxBytesBeforeTiFlashExternalSort); ok {
		target.TiFlashMaxBytesBeforeExtSort = variable.TidbOptInt64(val, target.TiFlashMaxBytesBeforeExtSort)
	}
	if val, ok := job.GetSessionVars(variable.TiFlashMemQuotaQueryPerNode); ok {
		target.TiFlashMemQuotaQueryPerNode = variable.TidbOptInt64(val, target.TiFlashMemQuotaQueryPerNode)
	}
	if val, ok := job.GetSessionVars(variable.TiFlashQuerySpillRatio); ok {
		ratio, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return variable.MViewExecutionSessionVars{}, errors.Annotatef(err, "invalid %s", variable.TiFlashQuerySpillRatio)
		}
		target.TiFlashQuerySpillRatio = ratio
	}
	if val, ok := job.GetSessionVars(variable.TiFlashFineGrainedShuffleStreamCount); ok {
		target.FineGrainedStreamCount = variable.TidbOptInt64(val, target.FineGrainedStreamCount)
	}
	if val, ok := job.GetSessionVars(variable.TiFlashFineGrainedShuffleBatchSize); ok {
		target.FineGrainedBatchSize = uint64(variable.TidbOptInt64(val, int64(target.FineGrainedBatchSize)))
	}
	if val, ok := job.GetSessionVars(variable.TiDBMViewMaintainImportThreads); ok {
		target.ImportThreads = variable.TidbOptInt(val, target.ImportThreads)
	}
	if val, ok := job.GetSessionVars(variable.TiDBMViewMaintainImportDiskQuota); ok {
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

func (e *executor) CreateMaterializedView(ctx sessionctx.Context, s *ast.CreateMaterializedViewStmt) error {
	is := e.infoCache.GetLatest()
	schemaName := s.ViewName.Schema
	if schemaName.O == "" {
		if ctx.GetSessionVars().CurrentDB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = pmodel.NewCIStr(ctx.GetSessionVars().CurrentDB)
		s.ViewName.Schema = schemaName
	}
	schema, ok := is.SchemaByName(schemaName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
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
	baseTableID := baseTable.Meta().ID

	mlogTable, err := getMLogTableByBaseTable(e.ctx, is, baseTableName.Schema, baseTable.Meta())
	if err != nil {
		return err
	}

	// Validate Stage-1 query contract and ensure MV LOG columns cover query references.
	groupByInfos, err := validateCreateMaterializedViewQuery(
		ctx,
		baseTableName,
		baseTable.Meta(),
		mlogTable.Meta().MaterializedViewLog.Columns,
		s.Select,
	)
	if err != nil {
		return err
	}
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
	alertWarningSec, alertOverdueSec, err := parseMViewAttributes(s.Attributes)
	if err != nil {
		return err
	}
	tzName, tzOffset := ddlutil.GetTimeZone(ctx)
	mvTableInfo.MaterializedView = &model.MaterializedViewInfo{
		BaseTableIDs:      []int64{baseTableID},
		InitBuildState:    model.MVInitBuildBuilding,
		SQLContent:        selectSQL,
		RefreshMethod:     refreshMethod,
		RefreshStartWith:  refreshStartWith,
		RefreshNext:       refreshNext,
		AlertWarningSec:   alertWarningSec,
		AlertOverdueSec:   alertOverdueSec,
		DefinitionSQLMode: ctx.GetSessionVars().SQLMode,
		DefinitionTimeZone: model.TimeZoneLocation{
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
	if err := initJobReorgMetaFromVariables(job, ctx); err != nil {
		return err
	}
	job.AddSessionVars(variable.TiDBScatterRegion, getScatterScopeFromSessionctx(ctx))
	AddMViewExecutionSessionVarsToJob(job, ctx.GetSessionVars())
	jobW := NewJobWrapperWithArgs(job, &model.CreateMaterializedViewArgs{
		TableInfo:    mvTableInfo,
		MLogTableIDs: []int64{mlogTable.Meta().ID},
	}, false)
	if err := e.DoDDLJobWrapper(ctx, jobW); err != nil {
		return errors.Trace(err)
	}

	var scatterScope string
	if val, ok := jobW.GetSessionVars(variable.TiDBScatterRegion); ok {
		scatterScope = val
	}
	return errors.Trace(e.createTableWithInfoPost(ctx, mvTableInfo, jobW.SchemaID, scatterScope))
}

func (e *executor) DropMaterializedView(ctx sessionctx.Context, s *ast.DropMaterializedViewStmt) error {
	is := e.infoCache.GetLatest()
	schemaName := s.ViewName.Schema
	if schemaName.O == "" {
		if ctx.GetSessionVars().CurrentDB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = pmodel.NewCIStr(ctx.GetSessionVars().CurrentDB)
		s.ViewName.Schema = schemaName
	}
	if _, ok := is.SchemaByName(schemaName); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}
	tbl, err := is.TableByName(e.ctx, schemaName, s.ViewName.Name)
	if err != nil {
		return err
	}
	if tbl.Meta().MaterializedView == nil {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schemaName.O, s.ViewName.Name, "MATERIALIZED VIEW")
	}

	dropStmt := &ast.DropTableStmt{Tables: []*ast.TableName{{Schema: schemaName, Name: s.ViewName.Name}}}
	return e.dropTableObject(ctx, dropStmt.Tables, dropStmt.IfExists, tableObject, true)
}

func (e *executor) DropMaterializedViewLog(ctx sessionctx.Context, s *ast.DropMaterializedViewLogStmt) error {
	is := e.infoCache.GetLatest()
	schemaName := s.Table.Schema
	if schemaName.O == "" {
		if ctx.GetSessionVars().CurrentDB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = pmodel.NewCIStr(ctx.GetSessionVars().CurrentDB)
		s.Table.Schema = schemaName
	}
	if _, ok := is.SchemaByName(schemaName); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}
	baseTable, err := is.TableByName(e.ctx, schemaName, s.Table.Name)
	if err != nil {
		return err
	}
	if baseTable.Meta().IsView() || baseTable.Meta().IsSequence() || baseTable.Meta().TempTableType != model.TempTableNone {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schemaName, s.Table.Name, "BASE TABLE")
	}

	mlogTable, err := getMLogTableByBaseTable(e.ctx, is, schemaName, baseTable.Meta())
	if err != nil {
		return err
	}
	mlogName := mlogTable.Meta().Name

	// MV LOG cannot be dropped while any MV still depends on the base table.
	if hasMaterializedViewDependsOnBaseTable(baseTable.Meta()) {
		return errDropMaterializedViewLogDependent(schemaName.O, s.Table.Name.O)
	}

	// Re-checking in DDL worker still exists; this failpoint is used to verify that worker-side
	// validation rejects stale executor pre-check under concurrent CREATE MATERIALIZED VIEW.
	failpoint.Inject("pauseDropMaterializedViewLogAfterCheck", func() {})

	dropStmt := &ast.DropTableStmt{Tables: []*ast.TableName{{Schema: schemaName, Name: mlogName}}}
	return e.dropTableObject(ctx, dropStmt.Tables, dropStmt.IfExists, tableObject, true)
}

func (e *executor) AlterMaterializedView(ctx sessionctx.Context, s *ast.AlterMaterializedViewStmt) error {
	is := e.infoCache.GetLatest()
	schemaName := s.ViewName.Schema
	if schemaName.O == "" {
		if ctx.GetSessionVars().CurrentDB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = pmodel.NewCIStr(ctx.GetSessionVars().CurrentDB)
		s.ViewName.Schema = schemaName
	}
	schema, ok := is.SchemaByName(schemaName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}
	tbl, err := is.TableByName(e.ctx, schemaName, s.ViewName.Name)
	if err != nil {
		return err
	}
	if tbl.Meta().MaterializedView == nil {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schemaName.O, s.ViewName.Name, "MATERIALIZED VIEW")
	}

	for _, action := range s.Actions {
		switch action.Tp {
		case ast.AlterMaterializedViewActionComment:
		case ast.AlterMaterializedViewActionRefresh:
			if _, _, _, err := buildMViewRefreshMeta(ctx, action.Refresh); err != nil {
				return err
			}
		case ast.AlterMaterializedViewActionAttributes:
			if _, _, err := parseMViewAttributes(action.Attributes); err != nil {
				return err
			}
		default:
			return errors.Errorf("unknown alter materialized view action type: %d", action.Tp)
		}
	}

	for _, action := range s.Actions {
		switch action.Tp {
		case ast.AlterMaterializedViewActionComment:
			alterStmt := &ast.AlterTableStmt{
				Table: &ast.TableName{Schema: schemaName, Name: s.ViewName.Name},
				Specs: []*ast.AlterTableSpec{{
					Tp: ast.AlterTableOption,
					Options: []*ast.TableOption{{
						Tp:       ast.TableOptionComment,
						StrValue: action.Comment,
					}},
				}},
			}
			if err := e.alterTable(e.ctx, ctx, alterStmt, true); err != nil {
				return err
			}
		case ast.AlterMaterializedViewActionRefresh:
			if err := e.alterMaterializedViewRefresh(
				ctx,
				schema.ID,
				schemaName,
				s.ViewName.Name,
				tbl.Meta().ID,
				action.Refresh,
			); err != nil {
				return err
			}
		case ast.AlterMaterializedViewActionAttributes:
			if err := e.alterMaterializedViewAttributes(
				ctx,
				schema.ID,
				schemaName,
				s.ViewName.Name,
				tbl.Meta().ID,
				action.Attributes,
			); err != nil {
				return err
			}
		default:
			return errors.Errorf("unknown alter materialized view action type: %d", action.Tp)
		}
	}
	return nil
}

func (e *executor) AlterMaterializedViewLog(ctx sessionctx.Context, s *ast.AlterMaterializedViewLogStmt) error {
	is := e.infoCache.GetLatest()
	schemaName := s.Table.Schema
	if schemaName.O == "" {
		if ctx.GetSessionVars().CurrentDB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = pmodel.NewCIStr(ctx.GetSessionVars().CurrentDB)
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
	if baseTable.Meta().IsView() || baseTable.Meta().IsSequence() || baseTable.Meta().TempTableType != model.TempTableNone {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schemaName, s.Table.Name, "BASE TABLE")
	}

	mlogTable, err := getMLogTableByBaseTable(e.ctx, is, schemaName, baseTable.Meta())
	if err != nil {
		return err
	}
	mlogName := mlogTable.Meta().Name

	for _, action := range s.Actions {
		switch action.Tp {
		case ast.AlterMaterializedViewLogActionPurge:
			if _, _, _, err := buildMLogPurgeMeta(ctx, action.Purge); err != nil {
				return err
			}
		default:
			return errors.Errorf("unknown alter materialized view log action type: %d", action.Tp)
		}
	}

	for _, action := range s.Actions {
		switch action.Tp {
		case ast.AlterMaterializedViewLogActionPurge:
			if err := e.alterMaterializedViewLogPurge(
				ctx,
				schema.ID,
				schemaName,
				mlogName,
				mlogTable.Meta().ID,
				action.Purge,
			); err != nil {
				return err
			}
		default:
			return errors.Errorf("unknown alter materialized view log action type: %d", action.Tp)
		}
	}
	return nil
}

func (e *executor) alterMaterializedViewLogPurge(
	ctx sessionctx.Context,
	schemaID int64,
	schemaName pmodel.CIStr,
	mlogName pmodel.CIStr,
	mlogID int64,
	purge *ast.MLogPurgeClause,
) error {
	purgeMethod, purgeStartWith, purgeNext, err := buildMLogPurgeMeta(ctx, purge)
	if err != nil {
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schemaID,
		TableID:        mlogID,
		SchemaName:     schemaName.L,
		TableName:      mlogName.L,
		Type:           model.ActionAlterMaterializedViewLogPurge,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterMaterializedViewLogPurgeArgs{
		PurgeMethod:    purgeMethod,
		PurgeStartWith: purgeStartWith,
		PurgeNext:      purgeNext,
	}
	if err := e.doDDLJob2(ctx, job, args); err != nil {
		return errors.Trace(err)
	}

	restoreEvalSession := setCreateMaterializedViewScheduleEvalSession(ctx, ctx.GetSessionVars().SQLMode)
	defer restoreEvalSession()

	kctx := kv.WithInternalSourceType(e.ctx, kv.InternalTxnDDL)
	ddlSess := sess.NewSession(ctx)
	nextTime, shouldUpdateNextTime, err := deriveCreateMaterializedScheduleNextTime(
		kctx,
		ddlSess,
		schemaName.O,
		mlogName.O,
		purgeStartWith,
		purgeNext,
		logAlterMaterializedViewLogPurgeNextTimeUpdateNull,
	)
	if err != nil {
		return err
	}
	if !shouldUpdateNextTime {
		return nil
	}
	return errors.Trace(e.updateMaterializedViewLogPurgeInfoNextTime(ctx, mlogID, nextTime))
}

func (e *executor) alterMaterializedViewRefresh(
	ctx sessionctx.Context,
	schemaID int64,
	schemaName pmodel.CIStr,
	viewName pmodel.CIStr,
	mviewID int64,
	refresh *ast.MViewRefreshClause,
) error {
	refreshMethod, refreshStartWith, refreshNext, err := buildMViewRefreshMeta(ctx, refresh)
	if err != nil {
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schemaID,
		TableID:        mviewID,
		SchemaName:     schemaName.L,
		TableName:      viewName.L,
		Type:           model.ActionAlterMaterializedViewRefresh,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterMaterializedViewRefreshArgs{
		RefreshMethod:    refreshMethod,
		RefreshStartWith: refreshStartWith,
		RefreshNext:      refreshNext,
	}
	if err := e.doDDLJob2(ctx, job, args); err != nil {
		return errors.Trace(err)
	}

	restoreEvalSession := setCreateMaterializedViewScheduleEvalSession(ctx, ctx.GetSessionVars().SQLMode)
	defer restoreEvalSession()

	kctx := kv.WithInternalSourceType(e.ctx, kv.InternalTxnDDL)
	ddlSess := sess.NewSession(ctx)
	nextTime, shouldUpdateNextTime, err := deriveCreateMaterializedScheduleNextTime(
		kctx,
		ddlSess,
		schemaName.O,
		viewName.O,
		refreshStartWith,
		refreshNext,
		logAlterMaterializedViewRefreshNextTimeUpdateNull,
	)
	if err != nil {
		return err
	}
	if !shouldUpdateNextTime {
		return nil
	}
	updated, err := e.updateMaterializedViewRefreshInfoNextTime(ctx, mviewID, nextTime)
	if err != nil {
		return err
	}
	if updated && nextTime == nil {
		if err := e.deleteMaterializedViewRefreshAlert(ctx, mviewID); err != nil {
			logutil.DDLLogger().Warn(
				"alter materialized view refresh: failed to delete refresh alert after disabling schedule",
				zap.String("schemaName", schemaName.O),
				zap.String("tableName", viewName.O),
				zap.Int64("mviewID", mviewID),
				zap.Error(err),
			)
		}
	}
	return nil
}

func (e *executor) alterMaterializedViewAttributes(
	ctx sessionctx.Context,
	schemaID int64,
	schemaName pmodel.CIStr,
	viewName pmodel.CIStr,
	mviewID int64,
	attrs string,
) error {
	alertWarningSec, alertOverdueSec, err := parseMViewAttributes(attrs)
	if err != nil {
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schemaID,
		TableID:        mviewID,
		SchemaName:     schemaName.L,
		TableName:      viewName.L,
		Type:           model.ActionAlterMaterializedViewAttributes,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterMaterializedViewAttributesArgs{
		AlertWarningSec: alertWarningSec,
		AlertOverdueSec: alertOverdueSec,
	}
	return errors.Trace(e.doDDLJob2(ctx, job, args))
}

func (e *executor) CreateMaterializedViewShadowTable(
	ctx sessionctx.Context,
	schemaID int64,
	schemaName pmodel.CIStr,
	shadowTableInfo *model.TableInfo,
) error {
	if shadowTableInfo == nil || shadowTableInfo.MaterializedViewShadow == nil {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view shadow table: invalid shadow metadata")
	}
	if shadowTableInfo.MaterializedViewShadow.SourceMViewID == 0 {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view shadow table: invalid source materialized view id")
	}

	involvingSchemas := []model.InvolvingSchemaInfo{{
		Database: schemaName.L,
		Table:    shadowTableInfo.Name.L,
	}}
	refreshTargetName := shadowTableInfo.Name
	if sourceMView, ok := e.infoCache.GetLatest().TableByID(e.ctx, shadowTableInfo.MaterializedViewShadow.SourceMViewID); ok {
		refreshTargetName = sourceMView.Meta().Name
		involvingSchemas = append(involvingSchemas, model.InvolvingSchemaInfo{
			Database: schemaName.L,
			Table:    sourceMView.Meta().Name.L,
			Mode:     model.SharedInvolving,
		})
	}

	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schemaID,
		SchemaName:          schemaName.L,
		TableName:           shadowTableInfo.Name.L,
		Type:                model.ActionCreateMaterializedViewShadow,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involvingSchemas,
		SQLMode:             ctx.GetSessionVars().SQLMode,
		SessionVars:         make(map[string]string),
	}
	job.AddSessionVars(variable.TiDBScatterRegion, getScatterScopeFromSessionctx(ctx))
	jobW := NewJobWrapperWithArgs(job, &model.CreateTableArgs{
		TableInfo: shadowTableInfo,
		FKCheck:   ctx.GetSessionVars().ForeignKeyChecks,
	}, false)
	originQuery := ctx.Value(sessionctx.QueryString)
	ctx.SetValue(
		sessionctx.QueryString,
		sqlescape.MustEscapeSQL("REFRESH MATERIALIZED VIEW %n.%n COMPLETE OUT OF PLACE", schemaName.O, refreshTargetName.O),
	)
	defer ctx.SetValue(sessionctx.QueryString, originQuery)
	if err := e.DoDDLJobWrapper(ctx, jobW); err != nil {
		return errors.Trace(err)
	}

	var scatterScope string
	if val, ok := jobW.GetSessionVars(variable.TiDBScatterRegion); ok {
		scatterScope = val
	}
	return errors.Trace(e.createTableWithInfoPost(ctx, shadowTableInfo, schemaID, scatterScope))
}

func (e *executor) RefreshMaterializedViewCompleteOutOfPlaceCutover(
	ctx sessionctx.Context,
	schemaID int64,
	schemaName pmodel.CIStr,
	viewName pmodel.CIStr,
	oldMViewID int64,
	shadowTableID int64,
	buildReadTSO uint64,
	expectedLastSuccessReadTSO uint64,
	expectedLastSuccessReadTSONull bool,
	nextTime *string,
	shouldUpdateNextTime bool,
) error {
	involvingSchemas, err := buildMViewRefreshOutOfPlaceCutoverInvolvingSchemaInfo(
		context.Background(),
		e.infoCache.GetLatest(),
		schemaName,
		oldMViewID,
		shadowTableID,
	)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schemaID,
		TableID:             oldMViewID,
		SchemaName:          schemaName.L,
		TableName:           viewName.L,
		Type:                model.ActionMViewRefreshOutOfPlaceCutover,
		BinlogInfo:          &model.HistoryInfo{},
		InvolvingSchemaInfo: involvingSchemas,
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	args := &model.RefreshMaterializedViewCompleteOutOfPlaceCutoverArgs{
		OldMViewID:                     oldMViewID,
		ShadowTableID:                  shadowTableID,
		BuildReadTSO:                   buildReadTSO,
		ExpectedLastSuccessReadTSO:     expectedLastSuccessReadTSO,
		ExpectedLastSuccessReadTSONull: expectedLastSuccessReadTSONull,
		NextTime:                       nextTime,
		ShouldUpdateNextTime:           shouldUpdateNextTime,
	}
	return errors.Trace(e.doDDLJob2(ctx, job, args))
}

func buildMViewRefreshOutOfPlaceCutoverInvolvingSchemaInfo(
	ctx context.Context,
	is infoschema.InfoSchema,
	schemaName pmodel.CIStr,
	oldMViewID int64,
	shadowTableID int64,
) ([]model.InvolvingSchemaInfo, error) {
	oldMView, ok := is.TableByID(ctx, oldMViewID)
	if !ok {
		return nil, dbterror.ErrInvalidDDLJob.GenWithStackByArgs(
			"refresh materialized view complete OUT OF PLACE cutover: old materialized view not found",
		)
	}
	oldMViewMeta := oldMView.Meta()
	if oldMViewMeta.MaterializedView == nil {
		return nil, dbterror.ErrInvalidDDLJob.GenWithStackByArgs(
			"refresh materialized view complete OUT OF PLACE cutover: old materialized view metadata missing",
		)
	}
	if len(oldMViewMeta.MaterializedView.BaseTableIDs) != 1 {
		return nil, dbterror.ErrInvalidDDLJob.GenWithStackByArgs(
			"refresh materialized view complete OUT OF PLACE cutover: materialized view must reference exactly one base table in Stage-1",
		)
	}

	baseTable, ok := is.TableByID(ctx, oldMViewMeta.MaterializedView.BaseTableIDs[0])
	if !ok {
		return nil, dbterror.ErrInvalidDDLJob.GenWithStackByArgs(
			"refresh materialized view complete OUT OF PLACE cutover: base table not found",
		)
	}

	shadowTable, ok := is.TableByID(ctx, shadowTableID)
	if !ok {
		return nil, dbterror.ErrInvalidDDLJob.GenWithStackByArgs(
			"refresh materialized view complete OUT OF PLACE cutover: shadow table not found",
		)
	}

	return []model.InvolvingSchemaInfo{
		{
			Database: schemaName.L,
			Table:    oldMViewMeta.Name.L,
			Mode:     model.ExclusiveInvolving,
		},
		{
			Database: schemaName.L,
			Table:    baseTable.Meta().Name.L,
			Mode:     model.ExclusiveInvolving,
		},
		{
			Database: schemaName.L,
			Table:    shadowTable.Meta().Name.L,
			Mode:     model.ExclusiveInvolving,
		},
	}, nil
}

func (e *executor) updateMaterializedViewRefreshInfoNextTime(
	ctx sessionctx.Context,
	mviewID int64,
	nextTime *string,
) (bool, error) {
	return e.bestEffortUpdateMaterializedScheduleInfoNextTime(
		ctx,
		alterMaterializedScheduleInfoNextTimeUpdateSpec{
			operation:                  "alter materialized view refresh",
			infoTable:                  "mysql.tidb_mview_refresh_info",
			idColumn:                   "MVIEW_ID",
			objectID:                   mviewID,
			rowMissingErr:              "alter materialized view refresh: refresh info row missing in mysql.tidb_mview_refresh_info",
			tableNotExistsErrConverter: convertAlterMaterializedViewRefreshInfoTableNotExistsErr,
		},
		nextTime,
	)
}

func (e *executor) deleteMaterializedViewRefreshAlert(ctx sessionctx.Context, mviewID int64) error {
	kctx := kv.WithInternalSourceType(e.ctx, kv.InternalTxnDDL)
	ddlSess, releaseInternalSession, err := e.newInternalMaterializedScheduleInfoUpdateSession(ctx)
	if err != nil {
		return err
	}
	defer releaseInternalSession()

	deleteSQL := sqlescape.MustEscapeSQL("DELETE FROM mysql.tidb_mview_refresh_alert WHERE MVIEW_ID = %?", mviewID)
	_, err = ddlSess.Execute(kctx, deleteSQL, "alter-materialized-view-refresh-delete-alert")
	failpoint.Inject("mockDeleteMaterializedViewRefreshAlertTableNotExists", func(val failpoint.Value) {
		if val.(bool) {
			err = infoschema.ErrTableNotExists.GenWithStackByArgs("mysql", "tidb_mview_refresh_alert")
		}
	})
	if err != nil {
		return errors.Trace(convertAlterMaterializedViewRefreshAlertTableNotExistsErr(err))
	}
	return nil
}

func convertAlterMaterializedViewRefreshInfoTableNotExistsErr(err error) error {
	if infoschema.ErrTableNotExists.Equal(err) {
		return errors.New("alter materialized view refresh: required system table mysql.tidb_mview_refresh_info does not exist")
	}
	return err
}

func convertAlterMaterializedViewRefreshAlertTableNotExistsErr(err error) error {
	if infoschema.ErrTableNotExists.Equal(err) {
		return errors.New("alter materialized view refresh: required system table mysql.tidb_mview_refresh_alert does not exist")
	}
	return err
}

func logAlterMaterializedViewRefreshNextTimeUpdateNull(
	mvSchemaName string,
	mvTableName string,
	nullExprClause string,
	startExpr string,
	nextExpr string,
) {
	if strings.TrimSpace(nextExpr) != "" {
		logutil.DDLLogger().Error(
			"alter materialized view refresh: automatic refresh schedule disabled because schedule expression evaluated to NULL, updating NEXT_TIME to NULL",
			zap.String("schemaName", mvSchemaName),
			zap.String("tableName", mvTableName),
			zap.String("nullExprClause", nullExprClause),
			zap.String("refreshStartWith", startExpr),
			zap.String("refreshNext", nextExpr),
		)
		return
	}
	logutil.DDLLogger().Warn(
		"alter materialized view refresh: schedule expression evaluated to NULL, updating NEXT_TIME to NULL",
		zap.String("schemaName", mvSchemaName),
		zap.String("tableName", mvTableName),
		zap.String("nullExprClause", nullExprClause),
		zap.String("refreshStartWith", startExpr),
		zap.String("refreshNext", nextExpr),
	)
}

func (e *executor) updateMaterializedViewLogPurgeInfoNextTime(
	ctx sessionctx.Context,
	mlogID int64,
	nextTime *string,
) error {
	_, err := e.bestEffortUpdateMaterializedScheduleInfoNextTime(
		ctx,
		alterMaterializedScheduleInfoNextTimeUpdateSpec{
			operation:                  "alter materialized view log purge",
			infoTable:                  "mysql.tidb_mlog_purge_info",
			idColumn:                   "MLOG_ID",
			objectID:                   mlogID,
			rowMissingErr:              "alter materialized view log purge: purge info row missing in mysql.tidb_mlog_purge_info",
			tableNotExistsErrConverter: convertAlterMaterializedViewLogPurgeInfoTableNotExistsErr,
		},
		nextTime,
	)
	return errors.Trace(err)
}

type alterMaterializedScheduleInfoNextTimeUpdateSpec struct {
	operation                  string
	infoTable                  string
	idColumn                   string
	objectID                   int64
	rowMissingErr              string
	tableNotExistsErrConverter func(error) error
}

func (e *executor) newInternalMaterializedScheduleInfoUpdateSession(fallbackCtx sessionctx.Context) (*sess.Session, func(), error) {
	if e.sessPool == nil {
		return sess.NewSession(fallbackCtx), func() {}, nil
	}
	internalCtx, err := e.sessPool.Get()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return sess.NewSession(internalCtx), func() {
		e.sessPool.Put(internalCtx)
	}, nil
}

func (e *executor) bestEffortUpdateMaterializedScheduleInfoNextTime(
	ctx sessionctx.Context,
	spec alterMaterializedScheduleInfoNextTimeUpdateSpec,
	nextTime *string,
) (bool, error) {
	kctx := kv.WithInternalSourceType(e.ctx, kv.InternalTxnDDL)
	// The outer ALTER MATERIALIZED VIEW / LOG statement is privilege-checked on the target
	// MV/base table only. The follow-up NEXT_TIME update touches mysql.* system tables and must
	// therefore run on a true DDL internal session instead of reusing the caller session.
	ddlSess, releaseInternalSession, err := e.newInternalMaterializedScheduleInfoUpdateSession(ctx)
	if err != nil {
		return false, err
	}
	defer releaseInternalSession()
	if err := ddlSess.BeginPessimistic(kctx); err != nil {
		return false, errors.Trace(err)
	}
	committed := false
	defer func() {
		if !committed {
			ddlSess.Rollback()
		}
	}()

	lockSQL := fmt.Sprintf(
		"SELECT 1 FROM %s WHERE %s = %%? LIMIT 1 FOR UPDATE WAIT %d",
		spec.infoTable,
		spec.idColumn,
		alterMaterializedScheduleInfoUpdateLockWaitTimeoutSec,
	)
	rows, err := ddlSess.Execute(kctx, lockSQL, spec.operation+"-lock-info-row", spec.objectID)
	if err != nil {
		if isAlterMaterializedScheduleInfoUpdateLockContentionErr(err) {
			appendAlterMaterializedScheduleInfoUpdateWarning(ctx, spec)
			return false, nil
		}
		return false, errors.Trace(spec.tableNotExistsErrConverter(err))
	}
	if len(rows) == 0 {
		return false, errors.New(spec.rowMissingErr)
	}

	var nextTimeArg any
	if nextTime != nil {
		nextTimeArg = *nextTime
	}
	updateSQL := fmt.Sprintf(
		"UPDATE %s SET NEXT_TIME = %%? WHERE %s = %%?",
		spec.infoTable,
		spec.idColumn,
	)
	if _, err := ddlSess.Execute(kctx, updateSQL, spec.operation+"-update-next-time", nextTimeArg, spec.objectID); err != nil {
		if isAlterMaterializedScheduleInfoUpdateLockContentionErr(err) {
			appendAlterMaterializedScheduleInfoUpdateWarning(ctx, spec)
			return false, nil
		}
		return false, errors.Trace(spec.tableNotExistsErrConverter(err))
	}
	if err := ddlSess.Commit(kctx); err != nil {
		if isAlterMaterializedScheduleInfoUpdateLockContentionErr(err) {
			appendAlterMaterializedScheduleInfoUpdateWarning(ctx, spec)
			return false, nil
		}
		return false, errors.Trace(err)
	}
	committed = true
	return true, nil
}

func isAlterMaterializedScheduleInfoUpdateLockContentionErr(err error) bool {
	return storeerr.ErrLockWaitTimeout.Equal(err) ||
		exeerrors.ErrDeadlock.Equal(err) ||
		kv.ErrWriteConflict.Equal(err)
}

func appendAlterMaterializedScheduleInfoUpdateWarning(
	ctx sessionctx.Context,
	spec alterMaterializedScheduleInfoNextTimeUpdateSpec,
) {
	ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf(
		"%s: metadata updated but failed to update %s.NEXT_TIME within %ds due to row lock contention; please retry later if immediate reschedule is needed",
		spec.operation,
		spec.infoTable,
		alterMaterializedScheduleInfoUpdateLockWaitTimeoutSec,
	))
}

func convertAlterMaterializedViewLogPurgeInfoTableNotExistsErr(err error) error {
	if infoschema.ErrTableNotExists.Equal(err) {
		return errors.New("alter materialized view log purge: required system table mysql.tidb_mlog_purge_info does not exist")
	}
	return err
}

func logAlterMaterializedViewLogPurgeNextTimeUpdateNull(
	mlogSchemaName string,
	mlogTableName string,
	nullExprClause string,
	startExpr string,
	nextExpr string,
) {
	if strings.TrimSpace(nextExpr) != "" {
		logutil.DDLLogger().Error(
			"alter materialized view log purge: automatic purge schedule disabled because schedule expression evaluated to NULL, updating NEXT_TIME to NULL",
			zap.String("schemaName", mlogSchemaName),
			zap.String("tableName", mlogTableName),
			zap.String("nullExprClause", nullExprClause),
			zap.String("purgeStartWith", startExpr),
			zap.String("purgeNext", nextExpr),
		)
		return
	}
	logutil.DDLLogger().Warn(
		"alter materialized view log purge: schedule expression evaluated to NULL, updating NEXT_TIME to NULL",
		zap.String("schemaName", mlogSchemaName),
		zap.String("tableName", mlogTableName),
		zap.String("nullExprClause", nullExprClause),
		zap.String("purgeStartWith", startExpr),
		zap.String("purgeNext", nextExpr),
	)
}

func buildMLogPurgeMeta(sctx sessionctx.Context, purge *ast.MLogPurgeClause) (method, startWith, next string, _ error) {
	if purge == nil {
		return "", "", "", nil
	}
	if purge.Immediate {
		return "IMMEDIATE", "", "", nil
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

func buildMViewRefreshMeta(sctx sessionctx.Context, refresh *ast.MViewRefreshClause) (method, startWith, next string, _ error) {
	if refresh == nil {
		return "FAST", "", "", nil
	}
	switch refresh.Method {
	case ast.MViewRefreshMethodNever:
		return "NEVER", "", "", nil
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

func parseMViewAttributes(attrs string) (alertWarningSec, alertOverdueSec int64, err error) {
	attrs = strings.TrimSpace(attrs)
	if attrs == "" {
		return 0, 0, nil
	}

	seen := make(map[string]struct{}, 2)
	for _, rawKV := range strings.Split(attrs, ",") {
		kv := strings.TrimSpace(rawKV)
		if kv == "" {
			return 0, 0, errors.New("invalid ATTRIBUTES format: empty key-value pair")
		}
		pos := strings.Index(kv, "=")
		if pos <= 0 || pos >= len(kv)-1 {
			return 0, 0, errors.Errorf("invalid ATTRIBUTES format: %q", kv)
		}
		key := strings.ToLower(strings.TrimSpace(kv[:pos]))
		valStr := strings.TrimSpace(kv[pos+1:])
		if key == "" || valStr == "" {
			return 0, 0, errors.Errorf("invalid ATTRIBUTES format: %q", kv)
		}
		if _, ok := seen[key]; ok {
			return 0, 0, errors.Errorf("duplicate ATTRIBUTES key: %s", key)
		}
		seen[key] = struct{}{}

		val, convErr := strconv.ParseInt(valStr, 10, 64)
		if convErr != nil || val < 0 {
			return 0, 0, errors.Errorf("invalid ATTRIBUTES value for %s: %s (must be non-negative integer seconds)", key, valStr)
		}

		switch key {
		case mviewAttrAlertWarning:
			alertWarningSec = val
		case mviewAttrAlertOverdue:
			alertOverdueSec = val
		default:
			return 0, 0, errors.Errorf("unsupported ATTRIBUTES key: %s", key)
		}
	}

	if alertWarningSec > 0 && alertOverdueSec > 0 && alertWarningSec > alertOverdueSec {
		return 0, 0, errors.Errorf("invalid ATTRIBUTES: %s (%d) must be less than or equal to %s (%d)",
			mviewAttrAlertWarning, alertWarningSec, mviewAttrAlertOverdue, alertOverdueSec)
	}
	return alertWarningSec, alertOverdueSec, nil
}

type mviewGroupByInfo struct {
	SelectIdx int
	NotNull   bool
}

func validateCreateMaterializedViewQuery(
	sctx sessionctx.Context,
	baseTableName *ast.TableName,
	baseTableInfo *model.TableInfo,
	mlogColumns []pmodel.CIStr,
	selectNode ast.ResultSetNode,
) (groupByInfos []mviewGroupByInfo, _ error) {
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

	groupByInfos = make([]mviewGroupByInfo, 0, len(sel.GroupBy.Items))
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

	if hasMinOrMax && !mvmerge.HasVisibleIndexWithPrefixCoveringColumns(baseTableInfo, groupByCols) {
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW with MIN/MAX requires base table index whose leading columns cover all GROUP BY columns")
	}

	for colName := range usedCols {
		if _, ok := mlogColSet[colName]; !ok {
			return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack(fmt.Sprintf("materialized view log does not contain column %s", colName))
		}
	}

	return groupByInfos, nil
}

func extractSingleTableNameFromSelect(sel *ast.SelectStmt) (*ast.TableName, error) {
	tbl, _, err := extractSingleTableNameAndAliasFromSelect(sel)
	return tbl, err
}

func extractSingleTableNameAndAliasFromSelect(sel *ast.SelectStmt) (*ast.TableName, pmodel.CIStr, error) {
	if sel.From == nil || sel.From.TableRefs == nil || sel.From.TableRefs.Left == nil || sel.From.TableRefs.Right != nil {
		return nil, pmodel.CIStr{}, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports a single base table")
	}
	ts, ok := sel.From.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, pmodel.CIStr{}, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports a single base table")
	}
	tbl, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil, pmodel.CIStr{}, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW only supports a single base table")
	}
	return tbl, ts.AsName, nil
}

func buildMViewSingleTableExpr(sctx sessionctx.Context, baseTableName *ast.TableName, fromAlias pmodel.CIStr, baseTableInfo *model.TableInfo, expr ast.ExprNode) (expression.Expression, error) {
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

func resolveMViewColumnName(col *ast.ColumnName, baseTableName *ast.TableName, fromAlias pmodel.CIStr, baseColMap map[string]*model.ColumnInfo) (string, error) {
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

func hasMaterializedViewDependsOnBaseTable(baseTableInfo *model.TableInfo) bool {
	return baseTableInfo.MaterializedViewBase != nil && len(baseTableInfo.MaterializedViewBase.MViewIDs) > 0
}

func errDropMaterializedViewLogDependent(schemaName, baseTableName string) error {
	return errors.Errorf("cannot drop materialized view log on %s.%s: dependent materialized views exist", schemaName, baseTableName)
}

func restoreNodeToCanonicalSQL(node ast.Node) (string, error) {
	var sb strings.Builder
	rctx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb)
	if err := node.Restore(rctx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func normalizeMVDefinitionHintDBNames(node ast.Node, defaultDB pmodel.CIStr) {
	if node == nil || defaultDB.L == "" {
		return
	}
	_, _ = node.Accept(&mvDefinitionHintDBNameNormalizer{defaultDB: defaultDB})
}

type mvDefinitionHintDBNameNormalizer struct {
	defaultDB pmodel.CIStr
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
