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

package executor

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	executil "github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	mvmerge "github.com/pingcap/tidb/pkg/planner/mview"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// CompareMaterializedViewExec executes "COMPARE MATERIALIZED VIEW" as a utility-style statement.
type CompareMaterializedViewExec struct {
	exec.BaseExecutor
	stmt *ast.CompareMaterializedViewStmt
	done bool
}

const (
	compareMaterializedViewDifferTypeBaseName = "_Differ_type_"
	compareMaterializedViewDifferType         = "mview_differ"
	compareMaterializedViewVacuumType         = "mview_vacuum"
	compareMaterializedViewExcessiveType      = "mview_excessive"

	compareMaterializedViewOutputWriterBatchSize     = 1024
	compareMaterializedViewOutputWriterBatchBytesCap = 16 * 1024 * 1024
)

type compareMaterializedViewDiffLayout struct {
	groupKeyOffsets        []int
	currentMarkerColIdx    int
	recomputedMarkerColIdx int
	payloadCompareColumns  []mviewCompleteDeltaCompareColumn
}

type compareMaterializedViewRecordSetExec struct {
	exec.BaseExecutor

	recordSet sqlexec.RecordSet
	release   func() error
	closeOnce sync.Once
	closeErr  error
}

type compareMaterializedViewOutputWriter struct {
	sctx          sessionctx.Context
	targetTable   table.Table
	txn           kv.Transaction
	fieldTypes    []*types.FieldType
	row           []types.Datum
	mvColumnCount int
	batchSize     int
	batchBytes    int
	pendingRows   int
	release       func(context.Context)
	dropTable     func(context.Context)
}

// Next implements the Executor Next interface.
func (e *CompareMaterializedViewExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMViewMaintenance)

	compareSnapshotTS, err := e.resolveCompareMaterializedViewSnapshotTS(ctx)
	if err != nil {
		return err
	}
	if err := e.precheckCompareMaterializedViewCurrentState(); err != nil {
		return err
	}
	snapshotIS, err := domain.GetDomain(e.Ctx()).GetSnapshotInfoSchema(compareSnapshotTS)
	if err != nil {
		return err
	}

	schemaName, targetTable, tblInfo, err := e.resolveCompareMaterializedViewTarget(snapshotIS)
	if err != nil {
		return err
	}
	if err := checkRefreshMaterializedViewBaseTableSelect(e.Ctx(), snapshotIS, tblInfo.MaterializedView); err != nil {
		return err
	}
	if err := e.checkCompareMaterializedViewOutputTableNotExists(); err != nil {
		return err
	}

	lastSuccessReadTSO, err := e.readCompareMaterializedViewLastSuccessReadTSO(ctx, tblInfo.ID, compareSnapshotTS)
	if err != nil {
		return err
	}

	diffMeta, err := mvmerge.BuildCompleteDiffSource(e.Ctx().GetPlanCtx(), snapshotIS, tblInfo)
	if err != nil {
		return err
	}
	visibleCols := targetTable.VisibleCols()
	layout, err := buildCompareMaterializedViewDiffLayout(diffMeta, visibleCols)
	if err != nil {
		return err
	}

	var outputWriter *compareMaterializedViewOutputWriter
	if e.stmt.OutputTable != nil {
		outputWriter, err = e.openCompareMaterializedViewOutputWriter(ctx, tblInfo, visibleCols)
		if err != nil {
			return err
		}
		defer func() {
			if outputWriter != nil {
				outputWriter.rollback(context.WithoutCancel(ctx))
			}
		}()
	}

	recomputedQueryExec, err := e.openCompareMaterializedViewQueryExec(ctx, lastSuccessReadTSO, tblInfo.MaterializedView, tblInfo.MaterializedView.SQLContent)
	if err != nil {
		return err
	}
	defer func() { _ = recomputedQueryExec.Close() }()

	currentMVQuerySQL := buildCompareMaterializedViewSelectSQL(schemaName, tblInfo.Name, visibleCols)
	currentQueryExec, err := e.openCompareMaterializedViewQueryExec(ctx, compareSnapshotTS, tblInfo.MaterializedView, currentMVQuerySQL)
	if err != nil {
		return err
	}
	defer func() { _ = currentQueryExec.Close() }()

	hashJoinExec := newCompareMaterializedViewHashJoinExec(e.Ctx(), currentQueryExec, recomputedQueryExec, layout.groupKeyOffsets, visibleCols)
	if err := exec.Open(ctx, hashJoinExec); err != nil {
		_ = hashJoinExec.Close()
		return err
	}
	defer func() { _ = hashJoinExec.Close() }()

	joinResult := exec.NewFirstChunk(hashJoinExec)
	rowIdxes := make([]int, 0, joinResult.Capacity())
	changedRows := make([]uint8, 0, joinResult.Capacity())
	var diffRows int64
	for {
		joinResult.Reset()
		if err := exec.Next(ctx, hashJoinExec, joinResult); err != nil {
			return err
		}
		if joinResult.NumRows() == 0 {
			break
		}

		rowIdxes = prepareCompareMaterializedViewRowIdxes(rowIdxes, joinResult.NumRows())
		if cap(changedRows) < joinResult.NumRows() {
			changedRows = make([]uint8, joinResult.NumRows())
		} else {
			changedRows = changedRows[:joinResult.NumRows()]
			clear(changedRows)
		}
		for _, compareCol := range layout.payloadCompareColumns {
			if err := executil.MarkTouchedRowsByColumn(
				rowIdxes,
				changedRows,
				1,
				0,
				joinResult.Column(compareCol.currentInputColID),
				joinResult.Column(compareCol.recomputedInputColID),
				compareCol.fieldType,
				compareCol.notNull,
				"COMPARE MATERIALIZED VIEW",
			); err != nil {
				return err
			}
		}

		currentMarkerCol := joinResult.Column(layout.currentMarkerColIdx)
		recomputedMarkerCol := joinResult.Column(layout.recomputedMarkerColIdx)
		for rowIdx := range joinResult.NumRows() {
			diffType := classifyCompareMaterializedViewRowDiff(
				currentMarkerCol.IsNull(rowIdx),
				recomputedMarkerCol.IsNull(rowIdx),
				changedRows[rowIdx] != 0,
			)
			if diffType == "" {
				continue
			}
			diffRows++
			if outputWriter != nil {
				if err := outputWriter.writeRow(ctx, joinResult.GetRow(rowIdx), diffType); err != nil {
					return err
				}
			}
		}
	}

	if outputWriter != nil {
		if err := outputWriter.commit(ctx); err != nil {
			return err
		}
		outputWriter = nil
		return nil
	}

	req.AppendString(0, formatCompareMaterializedViewSummary(lastSuccessReadTSO, diffRows, e.Ctx().GetSessionVars().Location()))
	return nil
}

func (e *CompareMaterializedViewExec) precheckCompareMaterializedViewCurrentState() error {
	currentIS := domain.GetDomain(e.Ctx()).InfoSchema()
	_, _, tblInfo, err := e.resolveCompareMaterializedViewTarget(currentIS)
	if err != nil {
		return err
	}
	if err := checkRefreshMaterializedViewBaseTableSelect(e.Ctx(), currentIS, tblInfo.MaterializedView); err != nil {
		return err
	}
	return e.checkCompareMaterializedViewOutputTableNotExists()
}

func (e *CompareMaterializedViewExec) checkCompareMaterializedViewOutputTableNotExists() error {
	if e.stmt.OutputTable == nil {
		return nil
	}

	is := domain.GetDomain(e.Ctx()).InfoSchema()
	schemaName := e.stmt.OutputTable.Schema
	if schemaName.O == "" {
		if e.Ctx().GetSessionVars().CurrentDB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = ast.NewCIStr(e.Ctx().GetSessionVars().CurrentDB)
		e.stmt.OutputTable.Schema = schemaName
	}
	if _, ok := is.SchemaByName(schemaName); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}
	if is.TableExists(schemaName, e.stmt.OutputTable.Name) {
		return infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: schemaName, Name: e.stmt.OutputTable.Name})
	}
	return nil
}

func (e *CompareMaterializedViewExec) resolveCompareMaterializedViewTarget(is infoschema.InfoSchema) (ast.CIStr, table.Table, *model.TableInfo, error) {
	schemaName := e.stmt.ViewName.Schema
	if schemaName.O == "" {
		if e.Ctx().GetSessionVars().CurrentDB == "" {
			return ast.CIStr{}, nil, nil, errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = ast.NewCIStr(e.Ctx().GetSessionVars().CurrentDB)
		e.stmt.ViewName.Schema = schemaName
	}
	if _, ok := is.SchemaByName(schemaName); !ok {
		return ast.CIStr{}, nil, nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}

	tbl, err := is.TableByName(context.Background(), schemaName, e.stmt.ViewName.Name)
	if err != nil {
		return ast.CIStr{}, nil, nil, err
	}
	tblInfo := tbl.Meta()
	if tblInfo.MaterializedView == nil {
		return ast.CIStr{}, nil, nil, dbterror.ErrWrongObject.GenWithStackByArgs(schemaName.O, e.stmt.ViewName.Name.O, "MATERIALIZED VIEW")
	}
	if len(tblInfo.MaterializedView.SQLContent) == 0 {
		return ast.CIStr{}, nil, nil, errors.New("compare materialized view: invalid select sql")
	}
	if err := checkRefreshMaterializedViewReady(schemaName, tblInfo); err != nil {
		return ast.CIStr{}, nil, nil, err
	}
	return schemaName, tbl, tblInfo, nil
}

func (e *CompareMaterializedViewExec) resolveCompareMaterializedViewSnapshotTS(ctx context.Context) (uint64, error) {
	if e.stmt.AsOf == nil {
		return 0, errors.New("compare materialized view: missing AS OF TIMESTAMP")
	}
	return staleread.CalculateAsOfTsExpr(ctx, e.Ctx().GetPlanCtx(), e.stmt.AsOf.TsExpr)
}

func (e *CompareMaterializedViewExec) readCompareMaterializedViewLastSuccessReadTSO(
	ctx context.Context,
	mviewID int64,
	compareSnapshotTS uint64,
) (uint64, error) {
	sctx, cleanup, err := e.openCompareMaterializedViewSnapshotSession(ctx, compareSnapshotTS)
	if err != nil {
		return 0, err
	}
	defer func() { _ = cleanup() }()

	refreshInfo, err := readRefreshInfoSnapshot(ctx, sctx.GetSQLExecutor(), mviewID)
	if err != nil {
		return 0, err
	}
	if refreshInfo.lastSuccessReadTSONull {
		return 0, errors.New("compare materialized view: LAST_SUCCESS_READ_TSO is NULL")
	}
	if refreshInfo.lastSuccessReadTSO > compareSnapshotTS {
		return 0, errors.Errorf(
			"compare materialized view: LAST_SUCCESS_READ_TSO %d is newer than compare snapshot %d",
			refreshInfo.lastSuccessReadTSO,
			compareSnapshotTS,
		)
	}
	return refreshInfo.lastSuccessReadTSO, nil
}

func (e *CompareMaterializedViewExec) openCompareMaterializedViewQueryExec(
	ctx context.Context,
	snapshotTS uint64,
	mviewInfo *model.MaterializedViewInfo,
	sql string,
) (exec.Executor, error) {
	sctx, cleanup, err := e.openCompareMaterializedViewSnapshotSession(ctx, snapshotTS)
	if err != nil {
		return nil, err
	}
	restoreDefinitionSession, err := initRefreshMaterializedViewSession(sctx.GetSessionVars(), mviewInfo)
	if err != nil {
		_ = cleanup()
		return nil, err
	}
	snapshotCleanup := cleanup
	cleanup = func() error {
		restoreDefinitionSession()
		return snapshotCleanup()
	}

	rs, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, sql)
	if err != nil {
		_ = cleanup()
		return nil, err
	}
	if rs == nil {
		_ = cleanup()
		return nil, errors.New("compare materialized view: query returned nil record set")
	}
	schema, err := buildCompareMaterializedViewRecordSetSchema(rs)
	if err != nil {
		_ = rs.Close()
		_ = cleanup()
		return nil, err
	}
	return &compareMaterializedViewRecordSetExec{
		BaseExecutor: exec.NewBaseExecutor(e.Ctx(), schema, 0),
		recordSet:    rs,
		release:      cleanup,
	}, nil
}

func (e *CompareMaterializedViewExec) openCompareMaterializedViewSnapshotSession(
	ctx context.Context,
	snapshotTS uint64,
) (sessionctx.Context, func() error, error) {
	sctx, err := e.GetSysSession()
	if err != nil {
		return nil, nil, err
	}
	restore, err := prepareCompareMaterializedViewSnapshotSession(sctx, snapshotTS)
	if err != nil {
		e.ReleaseSysSession(ctx, sctx)
		return nil, nil, err
	}
	return sctx, func() error {
		restoreErr := restore()
		e.ReleaseSysSession(ctx, sctx)
		return restoreErr
	}, nil
}

func (e *CompareMaterializedViewExec) openCompareMaterializedViewOutputWriter(
	ctx context.Context,
	tblInfo *model.TableInfo,
	visibleCols []*table.Column,
) (*compareMaterializedViewOutputWriter, error) {
	definitionTimeZone := tblInfo.MaterializedView.DefinitionTimeZone.Clone()
	loc, err := definitionTimeZone.GetLocation()
	if err != nil {
		return nil, errors.Annotate(err, "compare materialized view: invalid definition timezone")
	}
	diffTypeColName := chooseCompareMaterializedViewDifferTypeColumnName(visibleCols)

	ddlSctx, err := e.GetSysSession()
	if err != nil {
		return nil, err
	}
	createSQL, err := buildCompareMaterializedViewOutputCreateTableSQL(ddlSctx, e.stmt.OutputTable, tblInfo, visibleCols, diffTypeColName)
	if err != nil {
		e.ReleaseSysSession(ctx, ddlSctx)
		return nil, err
	}
	if _, err := ddlSctx.GetSQLExecutor().ExecuteInternal(ctx, createSQL); err != nil {
		e.ReleaseSysSession(ctx, ddlSctx)
		return nil, err
	}
	e.ReleaseSysSession(ctx, ddlSctx)
	dropOutputTable := func(dropCtx context.Context) {
		e.cleanupCompareMaterializedViewOutputTable(dropCtx, e.stmt.OutputTable)
	}

	insertSctx, err := e.GetSysSession()
	if err != nil {
		dropOutputTable(context.WithoutCancel(ctx))
		return nil, err
	}
	sessVars := insertSctx.GetSessionVars()
	origTimeZone := sessVars.TimeZone
	origStmtCtxTimeZone := sessVars.StmtCtx.TimeZone()
	// The query rows use the MV definition timezone, including TIMESTAMP values.
	sessVars.TimeZone = loc
	sessVars.StmtCtx.SetTimeZone(loc)
	releaseInsertSession := func(releaseCtx context.Context) {
		sessVars.TimeZone = origTimeZone
		sessVars.StmtCtx.SetTimeZone(origStmtCtxTimeZone)
		e.ReleaseSysSession(releaseCtx, insertSctx)
	}

	outputIS := domain.GetDomain(e.Ctx()).InfoSchema()
	targetTable, err := outputIS.TableByName(context.Background(), e.stmt.OutputTable.Schema, e.stmt.OutputTable.Name)
	if err != nil {
		releaseInsertSession(ctx)
		dropOutputTable(context.WithoutCancel(ctx))
		return nil, err
	}
	writableCols := targetTable.WritableCols()
	fieldTypes := make([]*types.FieldType, len(writableCols))
	for i := range writableCols {
		fieldTypes[i] = &writableCols[i].FieldType
	}
	return &compareMaterializedViewOutputWriter{
		sctx:          insertSctx,
		targetTable:   targetTable,
		fieldTypes:    fieldTypes,
		row:           make([]types.Datum, len(writableCols)),
		mvColumnCount: len(visibleCols),
		batchSize:     compareMaterializedViewOutputWriterBatchSize,
		batchBytes:    resolveCompareMaterializedViewOutputWriterBatchBytes(),
		release:       releaseInsertSession,
		dropTable:     dropOutputTable,
	}, nil
}

func (e *CompareMaterializedViewExec) cleanupCompareMaterializedViewOutputTable(ctx context.Context, outputTable *ast.TableName) {
	ddlSctx, err := e.GetSysSession()
	if err != nil {
		logutil.BgLogger().Warn(
			"failed to cleanup compare materialized view output table",
			zap.String("schema", outputTable.Schema.O),
			zap.String("table", outputTable.Name.O),
			zap.Error(err),
		)
		return
	}
	defer e.ReleaseSysSession(ctx, ddlSctx)

	dropSQL := sqlescape.MustEscapeSQL("DROP TABLE IF EXISTS %n.%n", outputTable.Schema.O, outputTable.Name.O)
	if err := executeRefreshMaterializedViewInternalSQL(ctx, ddlSctx.GetSQLExecutor(), dropSQL); err != nil {
		logutil.BgLogger().Warn(
			"failed to cleanup compare materialized view output table",
			zap.String("schema", outputTable.Schema.O),
			zap.String("table", outputTable.Name.O),
			zap.Error(err),
		)
	}
}

func buildCompareMaterializedViewOutputCreateTableSQL(
	sctx sessionctx.Context,
	outputTable *ast.TableName,
	tblInfo *model.TableInfo,
	visibleCols []*table.Column,
	diffTypeColName ast.CIStr,
) (string, error) {
	outputTblInfo := &model.TableInfo{
		Name:    outputTable.Name,
		Charset: tblInfo.Charset,
		Collate: tblInfo.Collate,
		State:   model.StatePublic,
		Columns: make([]*model.ColumnInfo, 0, len(visibleCols)+1),
	}
	const outputColumnExcludedFlags = mysql.PriKeyFlag |
		mysql.UniqueKeyFlag |
		mysql.MultipleKeyFlag |
		mysql.AutoIncrementFlag |
		mysql.NoDefaultValueFlag |
		mysql.OnUpdateNowFlag
	for i, col := range visibleCols {
		colInfo := col.ToInfo().Clone()
		colInfo.Offset = i
		colInfo.State = model.StatePublic
		colInfo.FieldType.SetFlag(colInfo.GetFlag() &^ outputColumnExcludedFlags)
		outputTblInfo.Columns = append(outputTblInfo.Columns, colInfo)
	}
	outputCharset, outputCollate := outputTblInfo.Charset, outputTblInfo.Collate
	if outputCharset == "" {
		outputCharset = mysql.DefaultCharset
	}
	if outputCollate == "" {
		outputCollate = getDefaultCollate(outputCharset)
	}
	diffTypeCol := &model.ColumnInfo{
		ID:     int64(len(outputTblInfo.Columns) + 1),
		Name:   diffTypeColName,
		Offset: len(outputTblInfo.Columns),
		FieldType: *types.NewFieldTypeBuilder().
			SetType(mysql.TypeVarchar).
			SetFlen(32).
			SetFlag(mysql.NotNullFlag).
			SetCharset(outputCharset).
			SetCollate(outputCollate).
			BuildP(),
		State: model.StatePublic,
	}
	outputTblInfo.Columns = append(outputTblInfo.Columns, diffTypeCol)

	var tableDef bytes.Buffer
	if err := ConstructResultOfShowCreateTable(sctx, outputTblInfo, autoid.Allocators{}, &tableDef); err != nil {
		return "", err
	}
	sqlMode := sctx.GetSessionVars().SQLMode
	tablePrefix := "CREATE TABLE " + stringutil.Escape(outputTable.Name.O, sqlMode)
	tableDefSQL := tableDef.String()
	if !strings.HasPrefix(tableDefSQL, tablePrefix) {
		return "", errors.Errorf("compare materialized view: invalid output table DDL %q", tableDefSQL)
	}
	qualifiedName := stringutil.Escape(outputTable.Schema.O, sqlMode) + "." + stringutil.Escape(outputTable.Name.O, sqlMode)
	return "CREATE TABLE " + qualifiedName + strings.TrimPrefix(tableDefSQL, tablePrefix), nil
}

func prepareCompareMaterializedViewSnapshotSession(
	sctx sessionctx.Context,
	snapshotTS uint64,
) (func() error, error) {
	sessVars := sctx.GetSessionVars()
	origSnapshotTS := sessVars.SnapshotTS

	if err := setCompareMaterializedViewSnapshot(sctx, snapshotTS); err != nil {
		return nil, err
	}

	return func() error {
		return setCompareMaterializedViewSnapshot(sctx, origSnapshotTS)
	}, nil
}

func setCompareMaterializedViewSnapshot(sctx sessionctx.Context, snapshotTS uint64) error {
	snapshot := ""
	if snapshotTS != 0 {
		snapshot = strconv.FormatUint(snapshotTS, 10)
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMViewMaintenance)
	rs, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, "SET @@tidb_snapshot = %?", snapshot)
	if err != nil {
		return err
	}
	if rs != nil {
		return rs.Close()
	}
	return nil
}

func buildCompareMaterializedViewDiffLayout(
	diffMeta *mvmerge.CompleteDiffBuildResult,
	visibleCols []*table.Column,
) (*compareMaterializedViewDiffLayout, error) {
	if diffMeta == nil {
		return nil, errors.New("compare materialized view: diff metadata is nil")
	}
	if len(visibleCols) != diffMeta.MVColumnCount {
		return nil, errors.New("compare materialized view: hidden MV columns are unsupported")
	}

	groupKeySet := make(map[int]struct{}, len(diffMeta.GroupKeyMVOffsets))
	for _, offset := range diffMeta.GroupKeyMVOffsets {
		groupKeySet[offset] = struct{}{}
	}
	// Schema: current MV rows (probe/left) + recomputed base-query rows (build/right).
	layout := &compareMaterializedViewDiffLayout{
		groupKeyOffsets:        append([]int(nil), diffMeta.GroupKeyMVOffsets...),
		currentMarkerColIdx:    diffMeta.MarkerMVOffset,
		recomputedMarkerColIdx: len(visibleCols) + diffMeta.MarkerMVOffset,
	}
	layout.payloadCompareColumns = make([]mviewCompleteDeltaCompareColumn, 0, len(visibleCols)-len(groupKeySet))
	for offset, col := range visibleCols {
		if _, ok := groupKeySet[offset]; ok {
			continue
		}
		layout.payloadCompareColumns = append(layout.payloadCompareColumns, mviewCompleteDeltaCompareColumn{
			currentInputColID:    offset,
			recomputedInputColID: len(visibleCols) + offset,
			fieldType:            &col.FieldType,
			// A full outer join can produce NULL on either side of a NOT NULL MV column.
			notNull: false,
		})
	}
	return layout, nil
}

func newCompareMaterializedViewHashJoinExec(
	ctx sessionctx.Context,
	leftExec exec.Executor,
	rightExec exec.Executor,
	groupKeyOffsets []int,
	visibleCols []*table.Column,
) *join.HashJoinV1Exec {
	leftTypes, rightTypes := exec.RetTypes(leftExec), exec.RetTypes(rightExec)
	joinedSchema := buildCompareMaterializedViewJoinSchema(leftTypes, rightTypes)
	concurrency := uint(ctx.GetSessionVars().Concurrency.HashJoinConcurrency())
	if concurrency == 0 {
		concurrency = 1
	}

	hashJoinExec := &join.HashJoinV1Exec{
		BaseExecutor:          exec.NewBaseExecutor(ctx, joinedSchema, 0, leftExec, rightExec),
		ProbeSideTupleFetcher: &join.ProbeSideTupleFetcherV1{},
		ProbeWorkers:          make([]*join.ProbeWorkerV1, concurrency),
		BuildWorker:           &join.BuildWorkerV1{},
		HashJoinCtxV1: &join.HashJoinCtxV1{
			IsOuterJoin:     true,
			UseOuterToBuild: false,
		},
	}
	hashJoinExec.HashJoinCtxV1.SessCtx = ctx
	hashJoinExec.HashJoinCtxV1.JoinType = base.FullOuterJoin
	hashJoinExec.HashJoinCtxV1.Concurrency = concurrency
	hashJoinExec.HashJoinCtxV1.ChunkAllocPool = hashJoinExec.AllocPool
	hashJoinExec.HashJoinCtxV1.IsNullEQ = buildCompareMaterializedViewNullEQFlags(groupKeyOffsets, visibleCols)
	hashJoinExec.FullOuterJoinBuildFilter = nil
	hashJoinExec.FullOuterJoinProbeFilter = nil

	probeKeyColIdx := append([]int(nil), groupKeyOffsets...)
	buildKeyColIdx := append([]int(nil), groupKeyOffsets...)
	hashJoinExec.ProbeSideTupleFetcher.ProbeSideExec = leftExec
	hashJoinExec.BuildWorker.BuildKeyColIdx = buildKeyColIdx
	hashJoinExec.BuildWorker.BuildSideExec = rightExec
	hashJoinExec.BuildWorker.HashJoinCtx = hashJoinExec.HashJoinCtxV1

	childrenUsedSchema := [][]int{
		buildCompareMaterializedViewOrdinalSlice(len(leftTypes)),
		buildCompareMaterializedViewOrdinalSlice(len(rightTypes)),
	}
	// Compare uses current MV rows as the probe/left side and recomputed
	// base-query rows as the build/right side. This matches the full outer hash
	// join builder setup for build=right, probe=left:
	// - unmatched build rows are (NULL-current, recomputed), handled by RightOuterJoin;
	// - unmatched probe rows are (current, NULL-recomputed), handled by LeftOuterJoin.
	fullJoinBuildJoiner := join.NewJoiner(
		ctx,
		base.RightOuterJoin,
		true,
		make([]types.Datum, len(leftTypes)),
		nil,
		leftTypes,
		rightTypes,
		childrenUsedSchema,
		false,
	)
	fullJoinProbeJoiner := join.NewJoiner(
		ctx,
		base.LeftOuterJoin,
		false,
		make([]types.Datum, len(rightTypes)),
		nil,
		leftTypes,
		rightTypes,
		childrenUsedSchema,
		false,
	)
	for i := range concurrency {
		probeJoiner := fullJoinProbeJoiner.Clone()
		hashJoinExec.ProbeWorkers[i] = &join.ProbeWorkerV1{
			HashJoinCtx:         hashJoinExec.HashJoinCtxV1,
			ProbeKeyColIdx:      probeKeyColIdx,
			Joiner:              probeJoiner,
			FullJoinBuildJoiner: fullJoinBuildJoiner.Clone(),
			FullJoinProbeJoiner: probeJoiner,
		}
		hashJoinExec.ProbeWorkers[i].WorkerID = i
	}

	hashJoinExec.BuildTypes = buildCompareMaterializedViewKeyTypes(rightTypes, groupKeyOffsets)
	hashJoinExec.ProbeTypes = buildCompareMaterializedViewKeyTypes(leftTypes, groupKeyOffsets)
	return hashJoinExec
}

func buildCompareMaterializedViewNullEQFlags(groupKeyOffsets []int, visibleCols []*table.Column) []bool {
	flags := make([]bool, len(groupKeyOffsets))
	for i, offset := range groupKeyOffsets {
		flags[i] = !mysql.HasNotNullFlag(visibleCols[offset].GetFlag())
	}
	return flags
}

func buildCompareMaterializedViewKeyTypes(fieldTypes []*types.FieldType, groupKeyOffsets []int) []*types.FieldType {
	keyTypes := make([]*types.FieldType, len(groupKeyOffsets))
	for i, offset := range groupKeyOffsets {
		keyTypes[i] = fieldTypes[offset].Clone()
	}
	return keyTypes
}

func buildCompareMaterializedViewJoinSchema(leftTypes, rightTypes []*types.FieldType) *expression.Schema {
	cols := make([]*expression.Column, 0, len(leftTypes)+len(rightTypes))
	for i, ft := range leftTypes {
		cols = append(cols, &expression.Column{Index: i, RetType: ft})
	}
	offset := len(leftTypes)
	for i, ft := range rightTypes {
		cols = append(cols, &expression.Column{Index: offset + i, RetType: ft})
	}
	return expression.NewSchema(cols...)
}

func buildCompareMaterializedViewRecordSetSchema(rs sqlexec.RecordSet) (*expression.Schema, error) {
	fields := rs.Fields()
	if len(fields) == 0 {
		return nil, errors.New("compare materialized view: query returned empty schema")
	}
	cols := make([]*expression.Column, 0, len(fields))
	for i, field := range fields {
		if field == nil {
			return nil, errors.Errorf("compare materialized view: query returned nil field at offset %d", i)
		}
		if field.Column == nil {
			return nil, errors.Errorf("compare materialized view: query returned nil column at offset %d", i)
		}
		cols = append(cols, &expression.Column{
			ID:      field.Column.ID,
			Index:   i,
			RetType: &field.Column.FieldType,
		})
	}
	return expression.NewSchema(cols...), nil
}

func buildCompareMaterializedViewSelectSQL(
	schemaName ast.CIStr,
	tableName ast.CIStr,
	visibleCols []*table.Column,
) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i, col := range visibleCols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(sqlescape.MustEscapeSQL("%n", col.Name.O))
	}
	sb.WriteString(" FROM ")
	sb.WriteString(sqlescape.MustEscapeSQL("%n.%n", schemaName.O, tableName.O))
	return sb.String()
}

func chooseCompareMaterializedViewDifferTypeColumnName(visibleCols []*table.Column) ast.CIStr {
	used := make(map[string]struct{}, len(visibleCols))
	for _, col := range visibleCols {
		used[col.Name.L] = struct{}{}
	}
	base := compareMaterializedViewDifferTypeBaseName
	name := base
	for suffix := 1; ; suffix++ {
		if _, exists := used[strings.ToLower(name)]; !exists {
			return ast.NewCIStr(name)
		}
		name = fmt.Sprintf("%s%d", base, suffix)
	}
}

func prepareCompareMaterializedViewRowIdxes(rowIdxes []int, rowCnt int) []int {
	if cap(rowIdxes) < rowCnt {
		rowIdxes = make([]int, rowCnt)
	} else {
		rowIdxes = rowIdxes[:rowCnt]
	}
	for i := range rowCnt {
		rowIdxes[i] = i
	}
	return rowIdxes
}

func buildCompareMaterializedViewOrdinalSlice(colCnt int) []int {
	ordinals := make([]int, colCnt)
	for i := range colCnt {
		ordinals[i] = i
	}
	return ordinals
}

func classifyCompareMaterializedViewRowDiff(currentMissing, recomputedMissing, payloadChanged bool) string {
	switch {
	case recomputedMissing:
		return compareMaterializedViewExcessiveType
	case currentMissing:
		return compareMaterializedViewVacuumType
	case payloadChanged:
		return compareMaterializedViewDifferType
	default:
		return ""
	}
}

func formatCompareMaterializedViewSummary(lastSuccessReadTSO uint64, diffRows int64, loc *time.Location) string {
	if loc == nil {
		loc = time.Local
	}
	resultTime := oracle.GetTimeFromTS(lastSuccessReadTSO).In(loc).Format(types.TimeFSPFormat + " -07:00")
	if diffRows == 0 {
		return fmt.Sprintf(
			"There are no differences result in %d rows compared to source base tables at timestamp '%s' (TSO: %d)",
			diffRows,
			resultTime,
			lastSuccessReadTSO,
		)
	}
	return fmt.Sprintf(
		"There are differences result in %d rows compared to source base tables at timestamp '%s' (TSO: %d)",
		diffRows,
		resultTime,
		lastSuccessReadTSO,
	)
}

func (e *compareMaterializedViewRecordSetExec) Open(ctx context.Context) error {
	return e.BaseExecutor.Open(ctx)
}

func (e *compareMaterializedViewRecordSetExec) Next(ctx context.Context, req *chunk.Chunk) error {
	return e.recordSet.Next(ctx, req)
}

func (e *compareMaterializedViewRecordSetExec) Close() error {
	e.closeOnce.Do(func() {
		defer func() {
			if baseErr := e.BaseExecutor.Close(); e.closeErr == nil {
				e.closeErr = baseErr
			}
		}()
		defer func() {
			if e.release != nil {
				if releaseErr := e.release(); e.closeErr == nil {
					e.closeErr = releaseErr
				}
			}
		}()
		if e.recordSet != nil {
			e.closeErr = e.recordSet.Close()
		}
	})
	return e.closeErr
}

func resolveCompareMaterializedViewOutputWriterBatchBytes() int {
	txnLimit := kv.TxnTotalSizeLimit.Load() / 4
	if compareMaterializedViewOutputWriterBatchBytesCap > 0 && txnLimit > uint64(compareMaterializedViewOutputWriterBatchBytesCap) {
		txnLimit = uint64(compareMaterializedViewOutputWriterBatchBytesCap)
	}
	return max(int(txnLimit), 1)
}

func (w *compareMaterializedViewOutputWriter) ensureTxn(ctx context.Context) error {
	if w.txn != nil {
		return nil
	}
	if err := sessiontxn.NewTxnInStmt(ctx, w.sctx); err != nil {
		return err
	}
	txn, err := w.sctx.Txn(true)
	if err != nil {
		return err
	}
	w.txn = txn
	w.pendingRows = 0
	return nil
}

func (w *compareMaterializedViewOutputWriter) flushBatch(ctx context.Context) error {
	if w.txn == nil {
		return nil
	}
	w.sctx.StmtCommit(ctx)
	if err := w.sctx.CommitTxn(ctx); err != nil {
		return err
	}
	w.sctx.GetSessionVars().SetInTxn(false)
	w.txn = nil
	w.pendingRows = 0
	failpoint.Inject("mockCompareMaterializedViewOutputFlushError", func(val failpoint.Value) {
		if msg, ok := val.(string); ok {
			failpoint.Return(errors.New(msg))
		}
		failpoint.Return(errors.New("mock compare materialized view output flush error"))
	})
	return nil
}

func (w *compareMaterializedViewOutputWriter) writeRow(ctx context.Context, row chunk.Row, diffType string) error {
	if err := w.ensureTxn(ctx); err != nil {
		return err
	}
	// Schema: current MV rows (probe/left) + recomputed base-query rows (build/right).
	useRight := diffType == compareMaterializedViewVacuumType
	for colIdx := range w.mvColumnCount {
		sourceColIdx := colIdx
		if useRight {
			sourceColIdx += w.mvColumnCount
		}
		row.DatumWithBuffer(sourceColIdx, w.fieldTypes[colIdx], &w.row[colIdx])
	}
	w.row[w.mvColumnCount].SetString(diffType, mysql.DefaultCollationName)
	if _, err := w.targetTable.AddRecord(w.sctx.GetTableCtx(), w.txn, w.row, table.WithCtx(ctx), table.DupKeyCheckLazy); err != nil {
		return err
	}
	w.pendingRows++
	if w.pendingRows >= w.batchSize || w.txn.Size() >= w.batchBytes {
		return w.flushBatch(ctx)
	}
	return nil
}

func (w *compareMaterializedViewOutputWriter) commit(ctx context.Context) error {
	if w == nil || w.sctx == nil {
		return nil
	}
	if err := w.flushBatch(ctx); err != nil {
		return err
	}
	if w.release != nil {
		w.release(ctx)
	}
	w.sctx = nil
	w.txn = nil
	return nil
}

func (w *compareMaterializedViewOutputWriter) rollback(ctx context.Context) {
	if w == nil || w.sctx == nil {
		return
	}
	if w.txn != nil {
		w.sctx.RollbackTxn(ctx)
	}
	if w.release != nil {
		w.release(ctx)
	}
	w.sctx = nil
	w.txn = nil
	w.pendingRows = 0
	if w.dropTable != nil {
		w.dropTable(ctx)
	}
}
