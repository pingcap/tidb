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
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/tikv"
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
	Concurrency       int
	IgnoreError       bool
	TableID           int64
	PhysicalTableID   int64
}

type nonTransactionalDMLScannedChunk struct {
	first nonTransactionalDMLBoundary
	last  nonTransactionalDMLBoundary
	size  int
}

type nonTransactionalDMLRangeSpan struct {
	lower *nonTransactionalDMLBoundary
	upper *nonTransactionalDMLBoundary
}

func handleNonTransactionalDMLByRange(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se sessiontypes.Session,
	resolveCtx *resolve.Context, tableName *ast.TableName, shardColumnInfo *model.ColumnInfo,
	tableSources []*ast.TableSource) (recordSet sqlexec.RecordSet, retErr error) {
	startTime := time.Now()
	dmlType := strings.ToLower(ast.GetStmtLabel(stmt.DMLStmt))
	session_metrics.NonTransactionalDMLTaskInc(session_metrics.NonTransactionalDMLModeRange, dmlType, session_metrics.NonTransactionalDMLResultStart)
	defer func() {
		result := nonTransactionalDMLMetricsResult(retErr)
		session_metrics.NonTransactionalDMLTaskInc(session_metrics.NonTransactionalDMLModeRange, dmlType, result)
		session_metrics.NonTransactionalDMLDurationObserve(session_metrics.NonTransactionalDMLModeRange, dmlType, result, time.Since(startTime).Seconds())
	}()
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
		Concurrency:       se.GetSessionVars().NonTransactionalDMLConcurrency,
		IgnoreError:       se.GetSessionVars().NonTransactionalIgnoreError,
		TableID:           tnW.TableInfo.ID,
		PhysicalTableID:   tnW.TableInfo.ID,
	}, nil
}

func runNonTransactionalDMLRange(ctx context.Context, se sessiontypes.Session, rangeCtx *nonTransactionalDMLRangeContext) ([]job, error) {
	ranges, err := buildNonTransactionalDMLRangeSpans(ctx, se, rangeCtx)
	if err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		ranges = []nonTransactionalDMLRangeSpan{{}}
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerCount := nonTransactionalDMLRangeWorkerCount(rangeCtx.Concurrency, len(ranges))
	rangeCh := make(chan nonTransactionalDMLRangeSpan)
	var nextChunkID int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	jobs := make([]job, 0, len(ranges))
	var firstErr error
	recordResult := func(rangeJobs []job, err error) {
		mu.Lock()
		defer mu.Unlock()
		jobs = append(jobs, rangeJobs...)
		if err != nil && firstErr == nil {
			firstErr = err
			if !rangeCtx.IgnoreError {
				cancel()
			}
		}
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker, err := CreateSession(se.GetStore())
			if err != nil {
				recordResult(nil, err)
				return
			}
			defer worker.Close()
			if err := applyNonTransactionalDMLSessionContext(worker, rangeCtx.SessionCtx); err != nil {
				recordResult(nil, err)
				return
			}
			for rangeSpan := range rangeCh {
				if ctx.Err() != nil {
					return
				}
				rangeJobs, err := runNonTransactionalDMLRangeSpan(ctx, worker, rangeCtx, rangeSpan, &nextChunkID)
				recordResult(rangeJobs, err)
				if err != nil && !rangeCtx.IgnoreError {
					return
				}
			}
		}()
	}

sendLoop:
	for _, rangeSpan := range ranges {
		select {
		case <-ctx.Done():
			break sendLoop
		case rangeCh <- rangeSpan:
		}
	}
	close(rangeCh)
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return jobs, nil
}

func runNonTransactionalDMLRangeSpan(ctx context.Context, worker sessiontypes.Session, rangeCtx *nonTransactionalDMLRangeContext,
	rangeSpan nonTransactionalDMLRangeSpan, nextChunkID *int64) ([]job, error) {
	lower := cloneNonTransactionalDMLBoundaryPtr(rangeSpan.lower)
	jobs := make([]job, 0)
	for {
		scanned, err := scanNonTransactionalDMLRangeChunk(ctx, worker, rangeCtx, lower, rangeSpan.upper)
		if err != nil {
			return nil, err
		}
		if scanned == nil {
			break
		}
		chunkID := atomic.AddInt64(nextChunkID, 1)
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
		_, execErr := executeNonTransactionalDMLRangeChunkWithRetry(ctx, worker, rangeCtx, chunkID, lower, scanned, dmlSQL, 0, 0)
		if execErr != nil {
			chunkJob.err = execErr
			jobs = append(jobs, chunkJob)
			if !rangeCtx.IgnoreError {
				return nil, ErrNonTransactionalJobFailure.GenWithStackByArgs(chunkJob.jobID, len(jobs), chunkJob.start.String(), chunkJob.end.String(), chunkJob.String(worker.GetSessionVars().EnableRedactLog), execErr.Error())
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

func nonTransactionalDMLRangeWorkerCount(concurrency int, rangeCount int) int {
	if concurrency < 1 {
		concurrency = 1
	}
	if rangeCount < 1 {
		return 1
	}
	if concurrency > rangeCount {
		return rangeCount
	}
	return concurrency
}

func buildNonTransactionalDMLRangeSpans(ctx context.Context, se sessiontypes.Session,
	rangeCtx *nonTransactionalDMLRangeContext) ([]nonTransactionalDMLRangeSpan, error) {
	regionRanges, ok, err := loadNonTransactionalDMLRegionKeyRanges(ctx, se.GetStore(), rangeCtx.PhysicalTableID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return []nonTransactionalDMLRangeSpan{{}}, nil
	}
	return buildNonTransactionalDMLRangesFromRegionKeyRanges(se.GetSessionVars().StmtCtx, rangeCtx.Descriptor, rangeCtx.PhysicalTableID, regionRanges)
}

func loadNonTransactionalDMLRegionKeyRanges(ctx context.Context, store kv.Storage, physicalTableID int64) ([]kv.KeyRange, bool, error) {
	tikvStore, ok := store.(helper.Storage)
	if !ok {
		return nil, false, nil
	}
	recordStart := tablecodec.GenTableRecordPrefix(physicalTableID)
	recordEnd := recordStart.PrefixNext()
	regions, err := tikvStore.GetRegionCache().LoadRegionsInKeyRange(tikv.NewBackofferWithVars(ctx, 20000, nil), recordStart, recordEnd)
	if err != nil {
		return nil, true, err
	}
	regionRanges := make([]kv.KeyRange, 0, len(regions))
	for _, region := range regions {
		regionRanges = append(regionRanges, kv.KeyRange{
			StartKey: append(kv.Key(nil), region.StartKey()...),
			EndKey:   append(kv.Key(nil), region.EndKey()...),
		})
	}
	return regionRanges, true, nil
}

func buildNonTransactionalDMLRangesFromRegionKeyRanges(sc *stmtctx.StatementContext, desc *nonTransactionalDMLHandleDescriptor,
	physicalTableID int64, regionRanges []kv.KeyRange) ([]nonTransactionalDMLRangeSpan, error) {
	if len(regionRanges) == 0 {
		return []nonTransactionalDMLRangeSpan{{}}, nil
	}
	boundaries := make([]nonTransactionalDMLBoundary, 0, len(regionRanges))
	seen := make(map[string]struct{}, len(regionRanges))
	appendBoundary := func(key kv.Key) error {
		boundary, ok, err := decodeNonTransactionalDMLRegionSplitBoundary(sc, desc, physicalTableID, key)
		if err != nil || !ok {
			return err
		}
		encoded := string(boundary.encoded)
		if _, exists := seen[encoded]; exists {
			return nil
		}
		seen[encoded] = struct{}{}
		boundaries = append(boundaries, boundary)
		return nil
	}
	for _, regionRange := range regionRanges {
		if err := appendBoundary(regionRange.StartKey); err != nil {
			return nil, err
		}
		if err := appendBoundary(regionRange.EndKey); err != nil {
			return nil, err
		}
	}
	sort.Slice(boundaries, func(i, j int) bool {
		return bytes.Compare(boundaries[i].encoded, boundaries[j].encoded) < 0
	})

	ranges := make([]nonTransactionalDMLRangeSpan, 0, len(boundaries)+1)
	var lower *nonTransactionalDMLBoundary
	for i := range boundaries {
		upper := boundaries[i]
		upper.inclusive = false
		ranges = append(ranges, nonTransactionalDMLRangeSpan{
			lower: cloneNonTransactionalDMLBoundaryPtr(lower),
			upper: &upper,
		})
		nextLower := boundaries[i]
		nextLower.inclusive = true
		lower = &nextLower
	}
	ranges = append(ranges, nonTransactionalDMLRangeSpan{
		lower: cloneNonTransactionalDMLBoundaryPtr(lower),
	})
	return ranges, nil
}

func decodeNonTransactionalDMLRegionSplitBoundary(sc *stmtctx.StatementContext, desc *nonTransactionalDMLHandleDescriptor,
	physicalTableID int64, key kv.Key) (nonTransactionalDMLBoundary, bool, error) {
	if len(key) == 0 {
		return nonTransactionalDMLBoundary{}, false, nil
	}
	recordStart := tablecodec.GenTableRecordPrefix(physicalTableID)
	recordEnd := recordStart.PrefixNext()
	if bytes.Compare(key, recordStart) <= 0 || bytes.Compare(key, recordEnd) >= 0 {
		return nonTransactionalDMLBoundary{}, false, nil
	}
	if !bytes.HasPrefix(key, recordStart) {
		return nonTransactionalDMLBoundary{}, false, nil
	}
	tableID, handle, err := tablecodec.DecodeRecordKey(key)
	if err != nil || tableID != physicalTableID {
		return nonTransactionalDMLBoundary{}, false, nil
	}
	values, err := handle.Data()
	if err != nil || len(values) != 1 {
		return nonTransactionalDMLBoundary{}, false, nil
	}
	boundary, err := encodeNonTransactionalDMLBoundary(sc, desc, values[0], true)
	if err != nil {
		return nonTransactionalDMLBoundary{}, false, err
	}
	return boundary, true, nil
}

func cloneNonTransactionalDMLBoundaryPtr(boundary *nonTransactionalDMLBoundary) *nonTransactionalDMLBoundary {
	if boundary == nil {
		return nil
	}
	cloned := *boundary
	cloned.value = *boundary.value.Clone()
	cloned.encoded = append([]byte(nil), boundary.encoded...)
	return &cloned
}

func scanNonTransactionalDMLRangeChunk(ctx context.Context, se sessiontypes.Session, rangeCtx *nonTransactionalDMLRangeContext,
	lower *nonTransactionalDMLBoundary, upper *nonTransactionalDMLBoundary) (*nonTransactionalDMLScannedChunk, error) {
	whereSQL, err := buildNonTransactionalDMLRangeSelectWhereSQL(rangeCtx, lower, upper)
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

func buildNonTransactionalDMLRangeSelectWhereSQL(rangeCtx *nonTransactionalDMLRangeContext, lower *nonTransactionalDMLBoundary,
	upper *nonTransactionalDMLBoundary) (string, error) {
	rangeCondition := buildNonTransactionalDMLRangeCondition(rangeCtx.Descriptor, lower, upper)
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
	rangeID int64, lower *nonTransactionalDMLBoundary, scanned *nonTransactionalDMLScannedChunk, dmlSQL string,
	scannedBefore uint64, affectedBefore uint64) (uint64, error) {
	var lastErr error
	var attempts uint64
	for attempt := 0; attempt <= nonTransactionalDMLRangeMaxRetries; attempt++ {
		attempts = uint64(attempt + 1)
		affected, err := executeNonTransactionalDMLRangeChunk(ctx, se, rangeCtx, rangeID, lower, scanned, dmlSQL, uint64(attempt), scannedBefore, affectedBefore)
		if err == nil {
			recordNonTransactionalDMLChunkMetrics(rangeCtx, session_metrics.NonTransactionalDMLResultOK, attempts-1, uint64(scanned.size), affected)
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
		ErrorClass:      nonTransactionalDMLCheckpointErrorClass(lastErr),
		ErrorText:       lastErr.Error(),
		Scanned:         scannedBefore + uint64(scanned.size),
		Affected:        affectedBefore,
	}
	if err := writeNonTransactionalDMLCheckpoint(ctx, se, failed); err != nil {
		logutil.Logger(ctx).Warn("failed to write non-transactional DML failed checkpoint", zap.Error(err))
	}
	recordNonTransactionalDMLChunkMetrics(rangeCtx, session_metrics.NonTransactionalDMLResultError, attempts-1, uint64(scanned.size), 0)
	return 0, lastErr
}

func recordNonTransactionalDMLChunkMetrics(rangeCtx *nonTransactionalDMLRangeContext, result string, retries uint64, scanned uint64, affected uint64) {
	mode := rangeCtx.Mode
	if mode == "" {
		mode = session_metrics.NonTransactionalDMLModeRange
	}
	dmlType := rangeCtx.DMLType
	if dmlType == "" {
		dmlType = "unknown"
	}
	session_metrics.NonTransactionalDMLChunkInc(mode, dmlType, result)
	session_metrics.NonTransactionalDMLRowsAdd(mode, dmlType, session_metrics.NonTransactionalDMLRowsScanned, scanned)
	session_metrics.NonTransactionalDMLRowsAdd(mode, dmlType, session_metrics.NonTransactionalDMLRowsAffected, affected)
	session_metrics.NonTransactionalDMLRetryAdd(mode, dmlType, retries)
}

func nonTransactionalDMLMetricsResult(err error) string {
	if err != nil {
		return session_metrics.NonTransactionalDMLResultError
	}
	return session_metrics.NonTransactionalDMLResultOK
}

func nonTransactionalDMLCheckpointErrorClass(err error) string {
	if isNonTransactionalDMLRetryableError(err) {
		return "retryable"
	}
	return "execution"
}

func executeNonTransactionalDMLRangeChunk(ctx context.Context, se sessiontypes.Session, rangeCtx *nonTransactionalDMLRangeContext,
	rangeID int64, lower *nonTransactionalDMLBoundary, scanned *nonTransactionalDMLScannedChunk, dmlSQL string, retryCount uint64,
	scannedBefore uint64, affectedBefore uint64) (uint64, error) {
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
		Scanned:         scannedBefore + uint64(scanned.size),
		Affected:        affectedBefore + affected,
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
