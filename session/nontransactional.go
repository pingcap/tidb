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
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/sqlexec"
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
	stmt              *ast.NonTransactionalDeleteStmt
	shardColumnType   types.FieldType
	shardColumnRefer  *ast.ResultField
	originalCondition ast.ExprNode
}

func (j job) String(redacted bool) string {
	if redacted {
		return fmt.Sprintf("job id: %d, estimated size: %d", j.jobID, j.jobSize)
	}
	return fmt.Sprintf("job id: %d, estimated size: %d, sql: %s", j.jobID, j.jobSize, j.sql)
}

// HandleNonTransactionalDelete is the entry point for a non-transactional delete
func HandleNonTransactionalDelete(ctx context.Context, stmt *ast.NonTransactionalDeleteStmt, se Session) (sqlexec.RecordSet, error) {
	err := core.Preprocess(se, stmt)
	if err != nil {
		return nil, err
	}
	if err := checkConstraint(stmt, se); err != nil {
		return nil, err
	}
	metrics.NonTransactionalDeleteCount.Inc()
	tableName, selectSQL, shardColumnInfo, err := buildSelectSQL(stmt, se)
	if err != nil {
		return nil, err
	}
	if stmt.DryRun == ast.DryRunQuery {
		return buildDryRunResults(stmt.DryRun, []string{selectSQL}, se.GetSessionVars().BatchSize.MaxChunkSize)
	}

	// TODO: choose an appropriate quota.
	// Use the mem-quota-query as a workaround. As a result, a NT-DML may consume 2x of the memory quota.
	memTracker := setMemTracker(se)
	defer memTracker.DetachFromGlobalTracker()
	jobs, err := buildShardJobs(ctx, stmt, se, selectSQL, shardColumnInfo, memTracker)
	if err != nil {
		return nil, err
	}

	splitStmts, err := splitDeleteWorker(ctx, jobs, stmt, tableName, se, stmt.DeleteStmt.Where)
	if err != nil {
		return nil, err
	}
	if stmt.DryRun == ast.DryRunSplitDml {
		return buildDryRunResults(stmt.DryRun, splitStmts, se.GetSessionVars().BatchSize.MaxChunkSize)
	}
	return buildExecuteResults(ctx, jobs, se.GetSessionVars().BatchSize.MaxChunkSize, se.GetSessionVars().EnableRedactLog)
}

func setMemTracker(se Session) *memory.Tracker {
	memTracker := memory.NewTracker(memory.LabelForNonTransactionalDML, se.GetSessionVars().MemQuotaQuery)
	switch variable.OOMAction.Load() {
	case variable.OOMActionCancel:
		action := &memory.PanicOnExceed{ConnID: se.GetSessionVars().ConnectionID}
		action.SetLogHook(domain.GetDomain(se).ExpensiveQueryHandle().LogOnQueryExceedMemQuota)
		memTracker.SetActionOnExceed(action)
	case variable.OOMActionLog:
		fallthrough
	default:
		action := &memory.LogOnExceed{ConnID: se.GetSessionVars().ConnectionID}
		action.SetLogHook(domain.GetDomain(se).ExpensiveQueryHandle().LogOnQueryExceedMemQuota)
		memTracker.SetActionOnExceed(action)
	}
	memTracker.AttachToGlobalTracker(executor.GlobalMemoryUsageTracker)
	return memTracker
}

func checkConstraint(stmt *ast.NonTransactionalDeleteStmt, se Session) error {
	sessVars := se.GetSessionVars()
	if !(sessVars.IsAutocommit() && !sessVars.InTxn()) {
		return errors.Errorf("non-transactional DML can only run in auto-commit mode. auto-commit:%v, inTxn:%v",
			se.GetSessionVars().IsAutocommit(), se.GetSessionVars().InTxn())
	}
	if variable.EnableBatchDML.Load() && sessVars.DMLBatchSize > 0 && (sessVars.BatchDelete || sessVars.BatchInsert) {
		return errors.Errorf("can't run non-transactional DML with batch-dml")
	}

	if sessVars.ReadConsistency.IsWeak() {
		return errors.New("can't run non-transactional under weak read consistency")
	}
	if sessVars.SnapshotTS != 0 {
		return errors.New("can't do non-transactional DML when tidb_snapshot is set")
	}

	if stmt.DeleteStmt.TableRefs == nil || stmt.DeleteStmt.TableRefs.TableRefs == nil || stmt.DeleteStmt.TableRefs.TableRefs.Left == nil {
		return errors.New("table reference is nil")
	}
	if stmt.DeleteStmt.TableRefs.TableRefs.Right != nil {
		return errors.New("Non-transactional delete doesn't support multiple tables")
	}
	if stmt.DeleteStmt.Limit != nil {
		return errors.New("Non-transactional delete doesn't support limit")
	}
	if stmt.DeleteStmt.Order != nil {
		return errors.New("Non-transactional delete doesn't support order by")
	}
	return nil
}

// single-threaded worker. work on the key range [start, end]
func splitDeleteWorker(ctx context.Context, jobs []job, stmt *ast.NonTransactionalDeleteStmt,
	tableName *ast.TableName, se Session, originalCondition ast.ExprNode) ([]string, error) {

	// prepare for the construction of statement
	var shardColumnRefer *ast.ResultField
	var shardColumnType types.FieldType
	for _, col := range tableName.TableInfo.Columns {
		if col.Name.L == stmt.ShardColumn.Name.L {
			shardColumnRefer = &ast.ResultField{
				Column:    col,
				Table:     tableName.TableInfo,
				DBName:    tableName.Schema,
				TableName: tableName,
			}
			shardColumnType = col.FieldType
		}
	}
	if shardColumnRefer == nil && stmt.ShardColumn.Name.L != model.ExtraHandleName.L {
		return nil, errors.New("Non-transactional delete, column not found")
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
				logutil.Logger(ctx).Warn("Non-transactional delete worker exit because context canceled. No errors",
					zap.Int("finished", i), zap.Int("total", len(jobs)))
			} else {
				logutil.Logger(ctx).Warn("Non-transactional delete worker exit because context canceled. Errors found",
					zap.Int("finished", i), zap.Int("total", len(jobs)), zap.Strings("errors found", failedJobs))
			}
			return nil, ctx.Err()
		default:
		}

		// _tidb_rowid
		if shardColumnRefer == nil {
			shardColumnType = *types.NewFieldType(mysql.TypeLonglong)
			shardColumnRefer = &ast.ResultField{
				Column:    model.NewExtraHandleColInfo(),
				Table:     tableName.TableInfo,
				DBName:    tableName.Schema,
				TableName: tableName,
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

func doOneJob(ctx context.Context, job *job, totalJobCount int, options statementBuildInfo, se Session, dryRun bool) string {
	var whereCondition ast.ExprNode

	if job.start.IsNull() {
		isNullCondition := &ast.IsNullExpr{
			Expr: &ast.ColumnNameExpr{
				Name:  options.stmt.ShardColumn,
				Refer: options.shardColumnRefer,
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
					Name:  options.stmt.ShardColumn,
					Refer: options.shardColumnRefer,
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
				Name:  options.stmt.ShardColumn,
				Refer: options.shardColumnRefer,
			},
			Left:  left,
			Right: right,
			Not:   false,
		}
	}

	if options.originalCondition == nil {
		options.stmt.DeleteStmt.Where = whereCondition
	} else {
		options.stmt.DeleteStmt.Where = &ast.BinaryOperationExpr{
			Op: opcode.LogicAnd,
			L:  whereCondition,
			R:  options.originalCondition,
		}
	}
	var sb strings.Builder
	err := options.stmt.DeleteStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
		format.RestoreNameBackQuotes|
		format.RestoreSpacesAroundBinaryOperation|
		format.RestoreBracketAroundBinaryOperation|
		format.RestoreStringWithoutCharset, &sb))
	if err != nil {
		logutil.Logger(ctx).Error("Non-transactional delete, failed to restore the delete statement", zap.Error(err))
		job.err = errors.New("Failed to restore the delete statement, probably because of unsupported type of the shard column")
		return ""
	}
	deleteSQL := sb.String()

	if dryRun {
		return deleteSQL
	}

	job.sql = deleteSQL
	logutil.Logger(ctx).Info("start a Non-transactional delete",
		zap.String("job", job.String(se.GetSessionVars().EnableRedactLog)), zap.Int("totalJobCount", totalJobCount))
	var deleteSQLInLog string
	if se.GetSessionVars().EnableRedactLog {
		deleteSQLInLog = parser.Normalize(deleteSQL)
	} else {
		deleteSQLInLog = deleteSQL
	}

	options.stmt.DeleteStmt.SetText(nil, fmt.Sprintf("/* job %v/%v */ %s", job.jobID, totalJobCount, deleteSQL))
	rs, err := se.ExecuteStmt(ctx, options.stmt.DeleteStmt)

	// collect errors
	failpoint.Inject("batchDeleteError", func(val failpoint.Value) {
		if val.(bool) {
			err = errors.New("injected batch delete error")
		}
	})
	if err != nil {
		logutil.Logger(ctx).Error("Non-transactional delete SQL failed", zap.String("job", deleteSQLInLog), zap.Error(err), zap.Int("jobID", job.jobID), zap.Int("jobSize", job.jobSize))
		job.err = err
	} else {
		logutil.Logger(ctx).Info("Non-transactional delete SQL finished successfully", zap.Int("jobID", job.jobID),
			zap.Int("jobSize", job.jobSize), zap.String("deleteSQL", deleteSQLInLog))
	}
	if rs != nil {
		rs.Close()
	}
	return ""
}

func buildShardJobs(ctx context.Context, stmt *ast.NonTransactionalDeleteStmt, se Session,
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
	// NT-DML is a write operation, and should not be affected by read_staleness that is supposed to affect only SELECT.
	originalReadStaleness := se.GetSessionVars().ReadStaleness
	se.GetSessionVars().ReadStaleness = 0
	rss, err := se.Execute(ctx, selectSQL)
	se.GetSessionVars().SelectLimit = originalSelectLimit
	se.GetSessionVars().ReadStaleness = originalReadStaleness

	if err != nil {
		return nil, err
	}
	if len(rss) != 1 {
		return nil, errors.Errorf("Non-transactional delete, expecting 1 record set, but got %d", len(rss))
	}
	rs := rss[0]
	defer rs.Close()

	batchSize := int(stmt.Limit)
	if batchSize <= 0 {
		return nil, errors.New("Non-transactional delete, batch size should be positive")
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
				cmp, err := newEnd.Compare(se.GetSessionVars().StmtCtx, &currentEnd, collate.GetCollator(shardColumnCollate))
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

	return jobs, nil
}

func appendNewJob(jobs []job, id int, start types.Datum, end types.Datum, size int, tracker *memory.Tracker) []job {
	jobs = append(jobs, job{jobID: id, start: start, end: end, jobSize: size})
	tracker.Consume(start.EstimatedMemUsage() + end.EstimatedMemUsage() + 64)
	return jobs
}

func buildSelectSQL(stmt *ast.NonTransactionalDeleteStmt, se Session) (*ast.TableName, string, *model.ColumnInfo, error) {
	// only use the first table
	tableSource, ok := stmt.DeleteStmt.TableRefs.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, "", nil, errors.New("Non-transactional delete, table source not found")
	}
	tableName, ok := tableSource.Source.(*ast.TableName)
	if !ok {
		return nil, "", nil, errors.New("Non-transactional delete, table name not found")
	}

	// the shard column must be indexed
	indexed, shardColumnInfo, err := selectShardColumn(stmt, se, tableName, tableSource.AsName)
	if err != nil {
		return nil, "", nil, err
	}
	if !indexed {
		return nil, "", nil, errors.Errorf("Non-transactional delete, shard column %s is not indexed", stmt.ShardColumn.Name.L)
	}

	var sb strings.Builder
	if stmt.DeleteStmt.Where != nil {
		err := stmt.DeleteStmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
			format.RestoreNameBackQuotes|
			format.RestoreSpacesAroundBinaryOperation|
			format.RestoreBracketAroundBinaryOperation|
			format.RestoreStringWithoutCharset, &sb))
		if err != nil {
			return nil, "", nil, errors.Annotate(err, "Failed to restore where clause in non-transactional delete")
		}
	} else {
		sb.WriteString("TRUE")
	}
	// assure NULL values are placed first
	selectSQL := fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` WHERE %s ORDER BY IF(ISNULL(`%s`),0,1),`%s`",
		stmt.ShardColumn.Name.O, tableName.DBInfo.Name.O, tableName.Name.O, sb.String(), stmt.ShardColumn.Name.O, stmt.ShardColumn.Name.O)
	return tableName, selectSQL, shardColumnInfo, nil
}

// it attempts to auto-select a shard column from handle if not specified, and fills back the corresponding info in the stmt,
// making it transparent to following steps
func selectShardColumn(stmt *ast.NonTransactionalDeleteStmt, se Session, tableName *ast.TableName, tableAsName model.CIStr) (indexed bool, shardColumnInfo *model.ColumnInfo, err error) {
	tbl, err := domain.GetDomain(se).InfoSchema().TableByName(tableName.Schema, tableName.Name)
	if err != nil {
		return false, nil, err
	}
	tableInfo := tbl.Meta()

	var shardColumnName string
	if stmt.ShardColumn == nil {
		// auto-detect shard column
		if tbl.Meta().PKIsHandle {
			shardColumnInfo = tableInfo.GetPkColInfo()
		} else if tableInfo.IsCommonHandle {
			for _, index := range tableInfo.Indices {
				if index.Primary {
					if len(index.Columns) == 1 {
						shardColumnInfo = tableInfo.Columns[index.Columns[0].Offset]
						break
					}
					// if the clustered index contains multiple columns, we cannot automatically choose a column as the shard column
					return false, nil, errors.New("Non-transactional delete, the clustered index contains multiple columns. Please specify a shard column")
				}
			}
			if shardColumnInfo == nil {
				return false, nil, errors.New("Non-transactional delete, the clustered index is not found")
			}
		}

		shardColumnName := model.ExtraHandleName.L
		if shardColumnInfo != nil {
			shardColumnName = shardColumnInfo.Name.L
		}

		outputTableName := tableName.Name
		if tableAsName.L != "" {
			outputTableName = tableAsName
		}
		stmt.ShardColumn = &ast.ColumnName{
			Schema: tableName.Schema,
			Table:  outputTableName, // so that table alias works
			Name:   model.NewCIStr(shardColumnName),
		}
		return true, shardColumnInfo, nil
	}
	shardColumnName = stmt.ShardColumn.Name.L

	if shardColumnName == model.ExtraHandleName.L && !tableInfo.HasClusteredIndex() {
		return true, nil, nil
	}

	for _, col := range tbl.Cols() {
		if col.Name.L == shardColumnName {
			shardColumnInfo = col.ColumnInfo
			break
		}
	}
	if shardColumnInfo == nil {
		return false, nil, errors.Errorf("shard column %s not found", shardColumnName)
	}
	// is int handle
	if mysql.HasPriKeyFlag(shardColumnInfo.GetFlag()) && tableInfo.PKIsHandle {
		return true, shardColumnInfo, nil
	}

	for _, index := range tbl.Indices() {
		if index.Meta().State != model.StatePublic || index.Meta().Invisible {
			continue
		}
		indexColumns := index.Meta().Columns
		// check only the first column
		if len(indexColumns) > 0 && indexColumns[0].Name.L == shardColumnName {
			indexed = true
			break
		}
	}
	return indexed, shardColumnInfo, nil
}

func buildDryRunResults(dryRunOption int, results []string, maxChunkSize int) (sqlexec.RecordSet, error) {
	var fieldName string
	if dryRunOption == ast.DryRunSplitDml {
		fieldName = "split statement examples"
	} else {
		fieldName = "query statement"
	}

	resultFields := []*ast.ResultField{{
		Column: &model.ColumnInfo{
			FieldType: *types.NewFieldType(mysql.TypeString),
		},
		ColumnAsName: model.NewCIStr(fieldName),
	}}
	rows := make([][]interface{}, 0, len(results))
	for _, result := range results {
		row := make([]interface{}, 1)
		row[0] = result
		rows = append(rows, row)
	}
	return &sqlexec.SimpleRecordSet{
		ResultFields: resultFields,
		Rows:         rows,
		MaxChunkSize: maxChunkSize,
	}, nil
}

func buildExecuteResults(ctx context.Context, jobs []job, maxChunkSize int, redactLog bool) (sqlexec.RecordSet, error) {
	failedJobs := make([]job, 0)
	for _, job := range jobs {
		if job.err != nil {
			failedJobs = append(failedJobs, job)
		}
	}
	if len(failedJobs) == 0 {
		resultFields := []*ast.ResultField{
			{
				Column: &model.ColumnInfo{
					FieldType: *types.NewFieldType(mysql.TypeLong),
				},
				ColumnAsName: model.NewCIStr("number of jobs"),
			},
			{
				Column: &model.ColumnInfo{
					FieldType: *types.NewFieldType(mysql.TypeString),
				},
				ColumnAsName: model.NewCIStr("job status"),
			},
		}
		rows := make([][]interface{}, 1)
		row := make([]interface{}, 2)
		row[0] = len(jobs)
		row[1] = "all succeeded"
		rows[0] = row
		return &sqlexec.SimpleRecordSet{
			ResultFields: resultFields,
			Rows:         rows,
			MaxChunkSize: maxChunkSize,
		}, nil
	}

	// ignoreError must be set.
	var sb strings.Builder
	for _, job := range failedJobs {
		sb.WriteString(fmt.Sprintf("%s, %s;\n", job.String(redactLog), job.err.Error()))
	}

	errStr := sb.String()
	// log errors here in case the output is too long. There can be thousands of errors.
	logutil.Logger(ctx).Error("Non-transactional delete failed",
		zap.Int("num_failed_jobs", len(failedJobs)), zap.String("failed_jobs", errStr))

	return nil, errors.New(fmt.Sprintf("%d/%d jobs failed in the non-transactional DML: %s, ...(more in logs)",
		len(failedJobs), len(jobs), errStr[:mathutil.Min(500, len(errStr)-1)]))
}
