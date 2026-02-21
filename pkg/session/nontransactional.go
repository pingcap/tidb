// Copyright 2022 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// ErrNonTransactionalJobFailure is the error when a non-transactional job fails. The error is returned and following jobs are canceled.
var ErrNonTransactionalJobFailure = dbterror.ClassSession.NewStd(errno.ErrNonTransactionalJobFailure)

// job: handle keys in [start, end]
type job struct {
	start   types.Datum
	end     types.Datum
	err     error
	jobID   int
	jobSize int // it can be inaccurate if there are concurrent writes
	sql     string
}

// statementBuildInfo contains information that is needed to build the split statement in a job
type statementBuildInfo struct {
	stmt              *ast.NonTransactionalDMLStmt
	shardColumnType   types.FieldType
	shardColumnRefer  *resolve.ResultField
	originalCondition ast.ExprNode
}

func (j job) String(redacted string) string {
	return fmt.Sprintf("job id: %d, estimated size: %d, sql: %s", j.jobID, j.jobSize, redact.String(redacted, j.sql))
}

// HandleNonTransactionalDML is the entry point for a non-transactional DML statement
func HandleNonTransactionalDML(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se sessionapi.Session) (sqlexec.RecordSet, error) {
	sessVars := se.GetSessionVars()
	originalReadStaleness := se.GetSessionVars().ReadStaleness
	// NT-DML is a write operation, and should not be affected by read_staleness that is supposed to affect only SELECT.
	sessVars.ReadStaleness = 0
	// NT-DML should not use the bulk DML mode.
	originalBulkDMLEnabled := sessVars.BulkDMLEnabled
	sessVars.BulkDMLEnabled = false
	// NT-DML is used to be large and unusual, so we don't mix it with other DMLs, give it the prefix "NTDML-".
	stmtType := fmt.Sprintf("NTDML-%s", ast.GetStmtLabel(stmt.DMLStmt))
	ctx = stmtctx.WithStmtLabel(ctx, stmtType)
	defer func() {
		sessVars.ReadStaleness = originalReadStaleness
		sessVars.BulkDMLEnabled = originalBulkDMLEnabled
	}()
	nodeW := resolve.NewNodeW(stmt)
	err := core.Preprocess(ctx, se, nodeW)
	if err != nil {
		return nil, err
	}
	if err := checkConstraint(stmt, se); err != nil {
		return nil, err
	}

	tableName, selectSQL, shardColumnInfo, tableSources, err := buildSelectSQL(stmt, nodeW.GetResolveContext(), se)
	if err != nil {
		return nil, err
	}

	if err := checkConstraintWithShardColumn(se, stmt, tableName, shardColumnInfo, tableSources); err != nil {
		return nil, err
	}

	if stmt.DryRun == ast.DryRunQuery {
		return buildDryRunResults(stmt.DryRun, []string{selectSQL}, se.GetSessionVars().BatchSize.MaxChunkSize)
	}

	// TODO: choose an appropriate quota.
	// Use the mem-quota-query as a workaround. As a result, a NT-DML may consume 2x of the memory quota.
	memTracker := memory.NewTracker(memory.LabelForNonTransactionalDML, -1)
	memTracker.AttachTo(se.GetSessionVars().MemTracker)
	se.GetSessionVars().MemTracker.SetBytesLimit(se.GetSessionVars().MemQuotaQuery)
	defer memTracker.Detach()
	jobs, err := buildShardJobs(ctx, stmt, se, selectSQL, shardColumnInfo, memTracker)
	if err != nil {
		return nil, err
	}

	tnW := nodeW.GetResolveContext().GetTableName(tableName)
	splitStmts, err := runJobs(ctx, jobs, stmt, tnW, se, stmt.DMLStmt.WhereExpr())
	if err != nil {
		return nil, err
	}
	if stmt.DryRun == ast.DryRunSplitDml {
		return buildDryRunResults(stmt.DryRun, splitStmts, se.GetSessionVars().BatchSize.MaxChunkSize)
	}
	return buildExecuteResults(ctx, jobs, se.GetSessionVars().BatchSize.MaxChunkSize, se.GetSessionVars().EnableRedactLog)
}

// we require:
// (1) in an update statement, shard column cannot be updated
//
// Note: this is not a comprehensive check.
// We do this to help user prevent some easy mistakes, at an acceptable maintenance cost.
func checkConstraintWithShardColumn(se sessionapi.Session, stmt *ast.NonTransactionalDMLStmt,
	tableName *ast.TableName, shardColumnInfo *model.ColumnInfo, tableSources []*ast.TableSource) error {
	switch s := stmt.DMLStmt.(type) {
	case *ast.UpdateStmt:
		if err := checkUpdateShardColumn(se, s.List, shardColumnInfo, tableName, tableSources, true); err != nil {
			return err
		}
	case *ast.InsertStmt:
		// FIXME: is it possible to happen?
		// `insert into t select * from t on duplicate key update id = id + 1` will return an ambiguous column error?
		if err := checkUpdateShardColumn(se, s.OnDuplicate, shardColumnInfo, tableName, tableSources, false); err != nil {
			return err
		}
	default:
	}
	return nil
}

// shard column should not be updated.
func checkUpdateShardColumn(se sessionapi.Session, assignments []*ast.Assignment, shardColumnInfo *model.ColumnInfo,
	tableName *ast.TableName, tableSources []*ast.TableSource, isUpdate bool) error {
	// if the table has alias, the alias is used in assignments, and we should use aliased name to compare
	aliasedShardColumnTableName := tableName.Name.L
	for _, tableSource := range tableSources {
		if tableSource.Source.(*ast.TableName).Name.L == aliasedShardColumnTableName && tableSource.AsName.L != "" {
			aliasedShardColumnTableName = tableSource.AsName.L
		}
	}

	if shardColumnInfo == nil {
		return nil
	}
	for _, assignment := range assignments {
		sameDB := (assignment.Column.Schema.L == tableName.Schema.L) ||
			(assignment.Column.Schema.L == "" && tableName.Schema.L == se.GetSessionVars().CurrentDB)
		if !sameDB {
			continue
		}
		sameTable := (assignment.Column.Table.L == aliasedShardColumnTableName) || (isUpdate && len(tableSources) == 1)
		if !sameTable {
			continue
		}
		if assignment.Column.Name.L == shardColumnInfo.Name.L {
			return errors.New("Non-transactional DML, shard column cannot be updated")
		}
	}
	return nil
}

func checkConstraint(stmt *ast.NonTransactionalDMLStmt, se sessionapi.Session) error {
	sessVars := se.GetSessionVars()
	if !(sessVars.IsAutocommit() && !sessVars.InTxn()) {
		return errors.Errorf("non-transactional DML can only run in auto-commit mode. auto-commit:%v, inTxn:%v",
			se.GetSessionVars().IsAutocommit(), se.GetSessionVars().InTxn())
	}
	if vardef.EnableBatchDML.Load() && sessVars.DMLBatchSize > 0 && (sessVars.BatchDelete || sessVars.BatchInsert) {
		return errors.Errorf("can't run non-transactional DML with batch-dml")
	}

	if sessVars.ReadConsistency.IsWeak() {
		return errors.New("can't run non-transactional under weak read consistency")
	}
	if sessVars.SnapshotTS != 0 {
		return errors.New("can't do non-transactional DML when tidb_snapshot is set")
	}

	switch s := stmt.DMLStmt.(type) {
	case *ast.DeleteStmt:
		if err := checkTableRef(s.TableRefs, true); err != nil {
			return err
		}
		if err := checkReadClauses(s.Limit, s.Order); err != nil {
			return err
		}
		session_metrics.NonTransactionalDeleteCount.Inc()
	case *ast.UpdateStmt:
		if err := checkTableRef(s.TableRefs, true); err != nil {
			return err
		}
		if err := checkReadClauses(s.Limit, s.Order); err != nil {
			return err
		}
		session_metrics.NonTransactionalUpdateCount.Inc()
	case *ast.InsertStmt:
		if s.Select == nil {
			return errors.New("Non-transactional insert supports insert select stmt only")
		}
		selectStmt, ok := s.Select.(*ast.SelectStmt)
		if !ok {
			return errors.New("Non-transactional insert doesn't support non-select source")
		}
		if err := checkTableRef(selectStmt.From, true); err != nil {
			return err
		}
		if err := checkReadClauses(selectStmt.Limit, selectStmt.OrderBy); err != nil {
			return err
		}
		session_metrics.NonTransactionalInsertCount.Inc()
	default:
		return errors.New("Unsupported DML type for non-transactional DML")
	}

	return nil
}

func checkTableRef(t *ast.TableRefsClause, allowMultipleTables bool) error {
	if t == nil || t.TableRefs == nil || t.TableRefs.Left == nil {
		return errors.New("table reference is nil")
	}
	if !allowMultipleTables && t.TableRefs.Right != nil {
		return errors.New("Non-transactional statements don't support multiple tables")
	}
	return nil
}

func checkReadClauses(limit *ast.Limit, order *ast.OrderByClause) error {
	if limit != nil {
		return errors.New("Non-transactional statements don't support limit")
	}
	if order != nil {
		return errors.New("Non-transactional statements don't support order by")
	}
	return nil
}

// single-threaded worker. work on the key range [start, end]
func runJobs(ctx context.Context, jobs []job, stmt *ast.NonTransactionalDMLStmt,
	tableName *resolve.TableNameW, se sessionapi.Session, originalCondition ast.ExprNode) ([]string, error) {
	// prepare for the construction of statement
	var shardColumnRefer *resolve.ResultField
	var shardColumnType types.FieldType
	for _, col := range tableName.TableInfo.Columns {
		if col.Name.L == stmt.ShardColumn.Name.L {
			shardColumnRefer = &resolve.ResultField{
				Column: col,
				Table:  tableName.TableInfo,
				DBName: tableName.Schema,
			}
			shardColumnType = col.FieldType
		}
	}
	if shardColumnRefer == nil && stmt.ShardColumn.Name.L != model.ExtraHandleName.L {
		return nil, errors.New("Non-transactional DML, shard column not found")
	}

	splitStmts := make([]string, 0, len(jobs))
	for i := range jobs {
		select {
		case <-ctx.Done():
			failedJobs := make([]string, 0)
			for _, job := range jobs {
				if job.err != nil {
					failedJobs = append(failedJobs, fmt.Sprintf("job:%s, error: %s", job.String(se.GetSessionVars().EnableRedactLog), job.err.Error()))
				}
			}
			if len(failedJobs) == 0 {
				logutil.Logger(ctx).Warn("Non-transactional DML worker exit because context canceled. No errors",
					zap.Int("finished", i), zap.Int("total", len(jobs)))
			} else {
				logutil.Logger(ctx).Warn("Non-transactional DML worker exit because context canceled. Errors found",
					zap.Int("finished", i), zap.Int("total", len(jobs)), zap.Strings("errors found", failedJobs))
			}
			return nil, ctx.Err()
		default:
		}

		// _tidb_rowid
		if shardColumnRefer == nil {
			shardColumnType = *types.NewFieldType(mysql.TypeLonglong)
			shardColumnRefer = &resolve.ResultField{
				Column: model.NewExtraHandleColInfo(),
				Table:  tableName.TableInfo,
				DBName: tableName.Schema,
			}
		}
		stmtBuildInfo := statementBuildInfo{
			stmt:              stmt,
			shardColumnType:   shardColumnType,
			shardColumnRefer:  shardColumnRefer,
			originalCondition: originalCondition,
		}
		if stmt.DryRun == ast.DryRunSplitDml {
			if i > 0 && i < len(jobs)-1 {
				continue
			}
			splitStmt := doOneJob(ctx, &jobs[i], len(jobs), stmtBuildInfo, se, true)
			splitStmts = append(splitStmts, splitStmt)
		} else {
			doOneJob(ctx, &jobs[i], len(jobs), stmtBuildInfo, se, false)
		}

		// if the first job failed, there is a large chance that all jobs will fail. So return early.
		if i == 0 && jobs[i].err != nil {
			return nil, errors.Annotate(jobs[i].err, "Early return: error occurred in the first job. All jobs are canceled")
		}
		if jobs[i].err != nil && !se.GetSessionVars().NonTransactionalIgnoreError {
			return nil, ErrNonTransactionalJobFailure.GenWithStackByArgs(jobs[i].jobID, len(jobs), jobs[i].start.String(), jobs[i].end.String(), jobs[i].String(se.GetSessionVars().EnableRedactLog), jobs[i].err.Error())
		}
	}
	return splitStmts, nil
}

func doOneJob(ctx context.Context, job *job, totalJobCount int, options statementBuildInfo, se sessionapi.Session, dryRun bool) string {
	var whereCondition ast.ExprNode

	if job.start.IsNull() {
		isNullCondition := &ast.IsNullExpr{
			Expr: &ast.ColumnNameExpr{
				Name: options.stmt.ShardColumn,
			},
			Not: false,
		}
		if job.end.IsNull() {
			// `where x is null`
			whereCondition = isNullCondition
		} else {
			// `where (x <= job.end) || (x is null)`
			right := &driver.ValueExpr{}
			right.Type = options.shardColumnType
			right.Datum = job.end
			leCondition := &ast.BinaryOperationExpr{
				Op: opcode.LE,
				L: &ast.ColumnNameExpr{
					Name: options.stmt.ShardColumn,
				},
				R: right,
			}
			whereCondition = &ast.BinaryOperationExpr{
				Op: opcode.LogicOr,
				L:  leCondition,
				R:  isNullCondition,
			}
		}
	} else {
		// a normal between condition: `where x between start and end`
		left := &driver.ValueExpr{}
		left.Type = options.shardColumnType
		left.Datum = job.start
		right := &driver.ValueExpr{}
		right.Type = options.shardColumnType
		right.Datum = job.end
		whereCondition = &ast.BetweenExpr{
			Expr: &ast.ColumnNameExpr{
				Name: options.stmt.ShardColumn,
			},
			Left:  left,
			Right: right,
			Not:   false,
		}
	}

	if options.originalCondition == nil {
		options.stmt.DMLStmt.SetWhereExpr(whereCondition)
	} else {
		options.stmt.DMLStmt.SetWhereExpr(&ast.BinaryOperationExpr{
			Op: opcode.LogicAnd,
			L:  whereCondition,
			R:  options.originalCondition,
		})
	}
	var sb strings.Builder
	err := options.stmt.DMLStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
		format.RestoreNameBackQuotes|
		format.RestoreSpacesAroundBinaryOperation|
		format.RestoreBracketAroundBinaryOperation|
		format.RestoreStringWithoutCharset, &sb))
	if err != nil {
		logutil.Logger(ctx).Error("Non-transactional DML, failed to restore the DML statement", zap.Error(err))
		job.err = errors.New("Failed to restore the DML statement, probably because of unsupported type of the shard column")
		return ""
	}
	dmlSQL := sb.String()

	if dryRun {
		return dmlSQL
	}

	job.sql = dmlSQL
	logutil.Logger(ctx).Info("start a Non-transactional DML",
		zap.String("job", job.String(se.GetSessionVars().EnableRedactLog)), zap.Int("totalJobCount", totalJobCount))
	dmlSQLInLog := parser.Normalize(dmlSQL, se.GetSessionVars().EnableRedactLog)

	options.stmt.DMLStmt.SetText(nil, fmt.Sprintf("/* job %v/%v */ %s", job.jobID, totalJobCount, dmlSQL))
	rs, err := se.ExecuteStmt(ctx, options.stmt.DMLStmt)

	// collect errors
	failpoint.Inject("batchDMLError", func(val failpoint.Value) {
		if val.(bool) {
			err = errors.New("injected batch(non-transactional) DML error")
		}
	})
	if err != nil {
		logutil.Logger(ctx).Info("Non-transactional DML SQL failed", zap.String("job", dmlSQLInLog), zap.Error(err), zap.Int("jobID", job.jobID), zap.Int("jobSize", job.jobSize))
		job.err = err
	} else {
		logutil.Logger(ctx).Info("Non-transactional DML SQL finished successfully", zap.Int("jobID", job.jobID),
			zap.Int("jobSize", job.jobSize), zap.String("dmlSQL", dmlSQLInLog))
	}
	if rs != nil {
		_ = rs.Close()
	}
	return ""
}

func buildShardJobs(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se sessionapi.Session,
	selectSQL string, shardColumnInfo *model.ColumnInfo, memTracker *memory.Tracker) ([]job, error) {
	var shardColumnCollate string
	if shardColumnInfo != nil {
		shardColumnCollate = shardColumnInfo.GetCollate()
	} else {
		shardColumnCollate = ""
	}

	// A NT-DML is not a SELECT. We ignore the SelectLimit for selectSQL so that it can read all values.
	originalSelectLimit := se.GetSessionVars().SelectLimit
	se.GetSessionVars().SelectLimit = math.MaxUint64
	// save original max execution time, note it uses MaxExecutionTime instead of GetMaxExecutionTime on purpose
	// because GetMaxExecutionTime may return 0 when current StmtCtx is not in select query, while we need to
	// restore the exact value of MaxExecutionTime.
	originalMaxExecutionTime := se.GetSessionVars().MaxExecutionTime
	// A NT-DML is not read-only, so we disable max execution time for it.
	se.GetSessionVars().MaxExecutionTime = 0
	defer func() {
		se.GetSessionVars().MaxExecutionTime = originalMaxExecutionTime
	}()
	// NT-DML is a write operation, and should not be affected by read_staleness that is supposed to affect only SELECT.
	rss, err := se.Execute(ctx, selectSQL)
	se.GetSessionVars().SelectLimit = originalSelectLimit

	if err != nil {
		return nil, err
	}
	if len(rss) != 1 {
		return nil, errors.Errorf("Non-transactional DML, expecting 1 record set, but got %d", len(rss))
	}
	rs := rss[0]
	defer func() {
		_ = rs.Close()
	}()

	batchSize := int(stmt.Limit)
	if batchSize <= 0 {
		return nil, errors.New("Non-transactional DML, batch size should be positive")
	}
	jobCount := 0
	jobs := make([]job, 0)
	currentSize := 0
	var currentStart, currentEnd types.Datum

	chk := rs.NewChunk(nil)
	for {
		err = rs.Next(ctx, chk)
		if err != nil {
			return nil, err
		}

		// last chunk
		if chk.NumRows() == 0 {
			if currentSize > 0 {
				// there's remaining work
				jobs = appendNewJob(jobs, jobCount+1, currentStart, currentEnd, currentSize, memTracker)
			}
			break
		}

		if len(jobs) > 0 && chk.NumRows()+currentSize < batchSize {
			// not enough data for a batch
			currentSize += chk.NumRows()
			newEnd := chk.GetRow(chk.NumRows()-1).GetDatum(0, &rs.Fields()[0].Column.FieldType)
			currentEnd = *newEnd.Clone()
			continue
		}

		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			if currentSize == 0 {
				newStart := row.GetDatum(0, &rs.Fields()[0].Column.FieldType)
				currentStart = *newStart.Clone()
			}
			newEnd := row.GetDatum(0, &rs.Fields()[0].Column.FieldType)
			if currentSize >= batchSize {
				cmp, err := newEnd.Compare(se.GetSessionVars().StmtCtx.TypeCtx(), &currentEnd, collate.GetCollator(shardColumnCollate))
				if err != nil {
					return nil, err
				}
				if cmp != 0 {
					jobCount++
					jobs = appendNewJob(jobs, jobCount, *currentStart.Clone(), *currentEnd.Clone(), currentSize, memTracker)
					currentSize = 0
					currentStart = newEnd
				}
			}
			currentEnd = newEnd
			currentSize++
		}
		currentEnd = *currentEnd.Clone()
		currentStart = *currentStart.Clone()
	}

	failpoint.Inject("CheckMaxExecutionTime", func(val failpoint.Value) {
		if val.(bool) {
			if se.GetSessionVars().MaxExecutionTime > 0 {
				err = errors.New("injected max execution time exceeded error")
			}
		}
	})

	return jobs, err
}

func appendNewJob(jobs []job, id int, start types.Datum, end types.Datum, size int, tracker *memory.Tracker) []job {
	jobs = append(jobs, job{jobID: id, start: start, end: end, jobSize: size})
	tracker.Consume(start.EstimatedMemUsage() + end.EstimatedMemUsage() + 64)
	return jobs
}

