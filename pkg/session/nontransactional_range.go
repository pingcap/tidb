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

package session

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

const (
	nonTransactionalDMLRangeMaxRetries   = 3
	nonTransactionalDMLRangeRetryBackoff = 50 * time.Millisecond
)

type nonTransactionalDMLRangeContext struct {
	Stmt              *ast.NonTransactionalDMLStmt
	Descriptor        *nonTransactionalDMLHandleDescriptor
	SessionCtx        nonTransactionalDMLSessionContext
	JobID             string
	Mode              string
	DMLType           string
	DBName            string
	CurrentDB         string
	FromSQL           string
	HandleExprSQL     string
	OriginalWhereSQL  string
	OriginalCondition ast.ExprNode
	BatchSize         int
	TableID           int64
	PhysicalTableID   int64
}

type nonTransactionalDMLScannedChunk struct {
	first nonTransactionalDMLBoundary
	last  nonTransactionalDMLBoundary
	size  int
}

func handleNonTransactionalDMLByRange(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se sessiontypes.Session,
	resolveCtx *resolve.Context, tableName *ast.TableName, shardColumnInfo *model.ColumnInfo,
	tableSources []*ast.TableSource) (sqlexec.RecordSet, error) {
	if err := checkNonTransactionalDMLRangeModeStatement(stmt, tableSources); err != nil {
		return nil, err
	}
	if stmt.DryRun != ast.NoDryRun {
		return nil, errors.New("Non-transactional DML range mode doesn't support dry run")
	}
	if stmt.Limit == 0 || stmt.Limit > uint64(math.MaxInt64) {
		return nil, errors.New("Non-transactional DML, batch size should be positive")
	}
	desc, err := buildNonTransactionalDMLHandleDescriptor(se, stmt, tableName, shardColumnInfo, tableSources)
	if err != nil {
		return nil, err
	}
	rangeCtx, err := buildNonTransactionalDMLRangeContext(stmt, se, resolveCtx, tableName, desc, tableSources)
	if err != nil {
		return nil, err
	}
	jobs, err := runNonTransactionalDMLRange(ctx, se, rangeCtx)
	if err != nil {
		return nil, err
	}
	if hasFailedNonTransactionalDMLRangeJob(jobs) {
		return buildExecuteResults(ctx, jobs, se.GetSessionVars().BatchSize.MaxChunkSize, se.GetSessionVars().EnableRedactLog)
	}
	if err := deleteNonTransactionalDMLCheckpoints(ctx, se, rangeCtx.JobID); err != nil {
		return nil, err
	}
	return buildExecuteResults(ctx, jobs, se.GetSessionVars().BatchSize.MaxChunkSize, se.GetSessionVars().EnableRedactLog)
}

func hasFailedNonTransactionalDMLRangeJob(jobs []job) bool {
	for _, job := range jobs {
		if job.err != nil {
			return true
		}
	}
	return false
}

func checkNonTransactionalDMLRangeModeStatement(stmt *ast.NonTransactionalDMLStmt, tableSources []*ast.TableSource) error {
	if err := checkNonTransactionalDMLRangeModeStatementShape(stmt); err != nil {
		return err
	}
	if len(tableSources) != 1 {
		return errors.New("Non-transactional DML range mode supports single-table statements only")
	}
	return nil
}

func checkNonTransactionalDMLRangeModeStatementShape(stmt *ast.NonTransactionalDMLStmt) error {
	switch stmt.DMLStmt.(type) {
	case *ast.DeleteStmt, *ast.UpdateStmt:
	default:
		return errors.New("Non-transactional DML range mode supports DELETE and UPDATE only")
	}
	join, ok := stmt.DMLStmt.TableRefsJoin()
	if !ok {
		return errors.New("Non-transactional DML, table source not found")
	}
	tableSources, err := collectTableSourcesInJoin(join, nil)
	if err != nil {
		return err
	}
	if len(tableSources) != 1 {
		return errors.New("Non-transactional DML range mode supports single-table statements only")
	}
	return nil
}

func buildNonTransactionalDMLRangeContext(stmt *ast.NonTransactionalDMLStmt, se sessiontypes.Session, resolveCtx *resolve.Context,
	tableName *ast.TableName, desc *nonTransactionalDMLHandleDescriptor, tableSources []*ast.TableSource) (*nonTransactionalDMLRangeContext, error) {
	tnW := resolveCtx.GetTableName(tableName)
	if tnW == nil {
		return nil, errors.New("Non-transactional DML range mode, table not found")
	}
	originalWhereSQL, err := restoreNonTransactionalDMLExpr(stmt.DMLStmt.WhereExpr())
	if err != nil {
		return nil, errors.Annotate(err, "Failed to restore where clause in non-transactional DML range mode")
	}
	if originalWhereSQL == "" {
		originalWhereSQL = "TRUE"
	}
	sessionCtx, err := captureNonTransactionalDMLSessionContext(se)
	if err != nil {
		return nil, err
	}

	dbName := tnW.DBInfo.Name.O
	if dbName == "" {
		dbName = se.GetSessionVars().CurrentDB
	}
	tableAlias := ""
	if len(tableSources) > 0 {
		tableAlias = tableSources[0].AsName.O
	}
	qualifier := tableName.Name.O
	if tableAlias != "" {
		qualifier = tableAlias
	}
	fromSQL := fmt.Sprintf("%s.%s", quoteNonTransactionalDMLIdentifier(dbName), quoteNonTransactionalDMLIdentifier(tableName.Name.O))
	if tableAlias != "" {
		fromSQL = fmt.Sprintf("%s AS %s", fromSQL, quoteNonTransactionalDMLIdentifier(tableAlias))
	}
	handleExprSQL := fmt.Sprintf("%s.%s", quoteNonTransactionalDMLIdentifier(qualifier), quoteNonTransactionalDMLIdentifier(desc.columnName.Name.O))
	currentDB := se.GetSessionVars().CurrentDB
	if currentDB == "" {
		currentDB = dbName
	}
	return &nonTransactionalDMLRangeContext{
		Stmt:              stmt,
		Descriptor:        desc,
		SessionCtx:        sessionCtx,
		JobID:             fmt.Sprintf("range-%d-%d", tnW.TableInfo.ID, time.Now().UnixNano()),
		Mode:              "range",
		DMLType:           strings.ToLower(ast.GetStmtLabel(stmt.DMLStmt)),
		DBName:            dbName,
		CurrentDB:         currentDB,
		FromSQL:           fromSQL,
		HandleExprSQL:     handleExprSQL,
		OriginalWhereSQL:  originalWhereSQL,
		OriginalCondition: stmt.DMLStmt.WhereExpr(),
		BatchSize:         int(stmt.Limit),
		TableID:           tnW.TableInfo.ID,
		PhysicalTableID:   tnW.TableInfo.ID,
	}, nil
}

func runNonTransactionalDMLRange(ctx context.Context, se sessiontypes.Session, rangeCtx *nonTransactionalDMLRangeContext) ([]job, error) {
	worker, err := CreateSession(se.GetStore())
	if err != nil {
		return nil, err
	}
	defer worker.Close()
	if err := applyNonTransactionalDMLSessionContext(worker, rangeCtx.SessionCtx); err != nil {
		return nil, err
	}

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	var lower *nonTransactionalDMLBoundary
	jobs := make([]job, 0)
	for {
		scanned, err := scanNonTransactionalDMLRangeChunk(ctx, worker, rangeCtx, lower)
		if err != nil {
			return nil, err
		}
		if scanned == nil {
			break
		}
		chunkID := int64(len(jobs) + 1)
		dmlSQL, err := buildNonTransactionalDMLRangeMutationSQL(rangeCtx, lower, &scanned.last)
		if err != nil {
			return nil, err
		}
		chunkJob := job{
			jobID:   int(chunkID),
			start:   scanned.first.value,
			end:     scanned.last.value,
			jobSize: scanned.size,
			sql:     dmlSQL,
		}
		_, execErr := executeNonTransactionalDMLRangeChunkWithRetry(ctx, worker, rangeCtx, chunkID, lower, scanned, dmlSQL)
		if execErr != nil {
			chunkJob.err = execErr
			jobs = append(jobs, chunkJob)
			if !se.GetSessionVars().NonTransactionalIgnoreError {
				return nil, ErrNonTransactionalJobFailure.GenWithStackByArgs(chunkJob.jobID, len(jobs), chunkJob.start.String(), chunkJob.end.String(), chunkJob.String(se.GetSessionVars().EnableRedactLog), execErr.Error())
			}
		} else {
			jobs = append(jobs, chunkJob)
		}
		nextLower := scanned.last
		nextLower.inclusive = false
		lower = &nextLower
	}
	return jobs, nil
}

func scanNonTransactionalDMLRangeChunk(ctx context.Context, se sessiontypes.Session, rangeCtx *nonTransactionalDMLRangeContext,
	lower *nonTransactionalDMLBoundary) (*nonTransactionalDMLScannedChunk, error) {
	whereSQL, err := buildNonTransactionalDMLRangeSelectWhereSQL(rangeCtx, lower)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d",
		rangeCtx.HandleExprSQL, rangeCtx.FromSQL, whereSQL, rangeCtx.HandleExprSQL, rangeCtx.BatchSize)
	rows, err := sqlexec.ExecSQL(ctx, se, sql)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	first, err := nonTransactionalDMLBoundaryFromRowDatum(se, rangeCtx.Descriptor, rows[0])
	if err != nil {
		return nil, err
	}
	last, err := nonTransactionalDMLBoundaryFromRowDatum(se, rangeCtx.Descriptor, rows[len(rows)-1])
	if err != nil {
		return nil, err
	}
	last.inclusive = true
	return &nonTransactionalDMLScannedChunk{
		first: first,
		last:  last,
		size:  len(rows),
	}, nil
}

func nonTransactionalDMLBoundaryFromRowDatum(se sessiontypes.Session, desc *nonTransactionalDMLHandleDescriptor, row chunk.Row) (nonTransactionalDMLBoundary, error) {
	value := row.GetDatum(0, &desc.fieldType)
	return encodeNonTransactionalDMLBoundary(se.GetSessionVars().StmtCtx, desc, *value.Clone(), true)
}

func buildNonTransactionalDMLRangeSelectWhereSQL(rangeCtx *nonTransactionalDMLRangeContext, lower *nonTransactionalDMLBoundary) (string, error) {
	rangeCondition := buildNonTransactionalDMLRangeCondition(rangeCtx.Descriptor, lower, nil)
	rangeSQL, err := restoreNonTransactionalDMLExpr(rangeCondition)
	if err != nil {
		return "", err
	}
	if rangeSQL == "" {
		return fmt.Sprintf("(%s)", rangeCtx.OriginalWhereSQL), nil
	}
	return fmt.Sprintf("(%s) AND (%s)", rangeCtx.OriginalWhereSQL, rangeSQL), nil
}

func buildNonTransactionalDMLRangeMutationSQL(rangeCtx *nonTransactionalDMLRangeContext, lower *nonTransactionalDMLBoundary,
	upper *nonTransactionalDMLBoundary) (string, error) {
	rangeCondition := buildNonTransactionalDMLRangeCondition(rangeCtx.Descriptor, lower, upper)
	if rangeCondition == nil {
		return "", errors.New("Non-transactional DML range chunk requires an upper boundary")
	}
	condition := rangeCondition
	if rangeCtx.OriginalCondition != nil {
		condition = &ast.BinaryOperationExpr{
			Op: opcode.LogicAnd,
			L:  condition,
			R:  rangeCtx.OriginalCondition,
		}
	}
	originalCondition := rangeCtx.Stmt.DMLStmt.WhereExpr()
	rangeCtx.Stmt.DMLStmt.SetWhereExpr(condition)
	defer rangeCtx.Stmt.DMLStmt.SetWhereExpr(originalCondition)

	var sb strings.Builder
	err := rangeCtx.Stmt.DMLStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
		format.RestoreNameBackQuotes|
		format.RestoreSpacesAroundBinaryOperation|
		format.RestoreBracketAroundBinaryOperation|
		format.RestoreStringWithoutCharset, &sb))
	if err != nil {
		return "", errors.Annotate(err, "Failed to restore non-transactional DML range chunk")
	}
	return sb.String(), nil
}

func executeNonTransactionalDMLRangeChunkWithRetry(ctx context.Context, se sessiontypes.Session, rangeCtx *nonTransactionalDMLRangeContext,
	rangeID int64, lower *nonTransactionalDMLBoundary, scanned *nonTransactionalDMLScannedChunk, dmlSQL string) (uint64, error) {
	var lastErr error
	var attempts uint64
	for attempt := 0; attempt <= nonTransactionalDMLRangeMaxRetries; attempt++ {
		attempts = uint64(attempt + 1)
		affected, err := executeNonTransactionalDMLRangeChunk(ctx, se, rangeCtx, rangeID, lower, scanned, dmlSQL, uint64(attempt))
		if err == nil {
			return affected, nil
		}
		lastErr = err
		if !isNonTransactionalDMLRetryableError(err) || attempt == nonTransactionalDMLRangeMaxRetries {
			break
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(nonTransactionalDMLRangeRetryBackoff):
		}
	}
	failed := nonTransactionalDMLCheckpoint{
		JobID:           rangeCtx.JobID,
		RangeID:         rangeID,
		Mode:            rangeCtx.Mode,
		DMLType:         rangeCtx.DMLType,
		DBName:          rangeCtx.DBName,
		TableID:         rangeCtx.TableID,
		PhysicalTableID: rangeCtx.PhysicalTableID,
		HandleKind:      rangeCtx.Descriptor.kind,
		Lower:           lower,
		Upper:           &scanned.last,
		Checkpoint:      lower,
		Status:          nonTransactionalDMLCheckpointFailed,
		RetryCount:      attempts,
		ErrorClass:      "execution",
		ErrorText:       lastErr.Error(),
		Scanned:         uint64(scanned.size),
	}
	if err := writeNonTransactionalDMLCheckpoint(ctx, se, failed); err != nil {
		logutil.Logger(ctx).Warn("failed to write non-transactional DML failed checkpoint", zap.Error(err))
	}
	return 0, lastErr
}

func executeNonTransactionalDMLRangeChunk(ctx context.Context, se sessiontypes.Session, rangeCtx *nonTransactionalDMLRangeContext,
	rangeID int64, lower *nonTransactionalDMLBoundary, scanned *nonTransactionalDMLScannedChunk, dmlSQL string, retryCount uint64) (uint64, error) {
	if err := executeNonTransactionalDMLInternalNoResult(ctx, se, "BEGIN"); err != nil {
		return 0, err
	}
	committed := false
	defer func() {
		if !committed {
			se.RollbackTxn(ctx)
		}
	}()
	rs, err := se.ExecuteInternal(ctx, dmlSQL)
	if rs != nil {
		_ = rs.Close()
	}
	if err != nil {
		return 0, err
	}
	affected := se.AffectedRows()
	checkpoint := nonTransactionalDMLCheckpoint{
		JobID:           rangeCtx.JobID,
		RangeID:         rangeID,
		Mode:            rangeCtx.Mode,
		DMLType:         rangeCtx.DMLType,
		DBName:          rangeCtx.DBName,
		TableID:         rangeCtx.TableID,
		PhysicalTableID: rangeCtx.PhysicalTableID,
		HandleKind:      rangeCtx.Descriptor.kind,
		Lower:           lower,
		Upper:           &scanned.last,
		Checkpoint:      &scanned.last,
		Status:          nonTransactionalDMLCheckpointDone,
		RetryCount:      retryCount,
		Scanned:         uint64(scanned.size),
		Affected:        affected,
	}
	if err := writeNonTransactionalDMLCheckpoint(ctx, se, checkpoint); err != nil {
		return 0, err
	}
	if err := se.CommitTxn(ctx); err != nil {
		covered, checkErr := nonTransactionalDMLCheckpointCoversBoundary(ctx, se, rangeCtx.JobID, rangeID, rangeCtx.Descriptor, scanned.last)
		if checkErr == nil && covered {
			committed = true
			return affected, nil
		}
		return 0, err
	}
	committed = true
	logutil.Logger(ctx).Info("Non-transactional DML range chunk finished",
		zap.String("job-id", rangeCtx.JobID),
		zap.Int64("range-id", rangeID),
		zap.Int("scanned", scanned.size),
		zap.Uint64("affected", affected),
		zap.String("sql", parser.Normalize(dmlSQL, se.GetSessionVars().EnableRedactLog)))
	return affected, nil
}

func executeNonTransactionalDMLInternalNoResult(ctx context.Context, se sessiontypes.Session, sql string, args ...any) error {
	rs, err := se.ExecuteInternal(ctx, sql, args...)
	if rs != nil {
		_ = rs.Close()
	}
	return err
}

func restoreNonTransactionalDMLExpr(expr ast.ExprNode) (string, error) {
	if expr == nil {
		return "", nil
	}
	var sb strings.Builder
	err := expr.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
		format.RestoreNameBackQuotes|
		format.RestoreSpacesAroundBinaryOperation|
		format.RestoreBracketAroundBinaryOperation|
		format.RestoreStringWithoutCharset, &sb))
	if err != nil {
		return "", err
	}
	return sb.String(), nil
}

func quoteNonTransactionalDMLIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}
