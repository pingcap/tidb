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
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// ErrNonTransactionalJobFailure is the error when a non-transactional job fails. The error is returned and following jobs are canceled.
var ErrNonTransactionalJobFailure = dbterror.ClassSession.NewStd(errno.ErrNonTransactionalJobFailure)

var (
	nonTransactionalDeleteCount = metrics.NonTransactionalDMLCount.With(prometheus.Labels{metrics.LblType: "delete"})
	nonTransactionalInsertCount = metrics.NonTransactionalDMLCount.With(prometheus.Labels{metrics.LblType: "insert"})
	nonTransactionalUpdateCount = metrics.NonTransactionalDMLCount.With(prometheus.Labels{metrics.LblType: "update"})
)

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
	shardColumnRefer  *ast.ResultField
	originalCondition ast.ExprNode
}

func (j job) String(redacted bool) string {
	if redacted {
		return fmt.Sprintf("job id: %d, estimated size: %d", j.jobID, j.jobSize)
	}
	return fmt.Sprintf("job id: %d, estimated size: %d, sql: %s", j.jobID, j.jobSize, j.sql)
}

// HandleNonTransactionalDML is the entry point for a non-transactional DML statement
func HandleNonTransactionalDML(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se Session) (sqlexec.RecordSet, error) {
	sessVars := se.GetSessionVars()
	originalReadStaleness := se.GetSessionVars().ReadStaleness
	// NT-DML is a write operation, and should not be affected by read_staleness that is supposed to affect only SELECT.
	sessVars.ReadStaleness = 0
	defer func() {
		sessVars.ReadStaleness = originalReadStaleness
	}()
	err := core.Preprocess(ctx, se, stmt)
	if err != nil {
		return nil, err
	}
	if err := checkConstraint(stmt, se); err != nil {
		return nil, err
	}

	tableName, selectSQL, shardColumnInfo, tableSources, err := buildSelectSQL(stmt, se)
	if err != nil {
		return nil, err
	}

	if err := checkConstraintWithShardColumn(se, stmt, tableName, shardColumnInfo, tableSources); err != nil {
		return nil, err
	}

	if stmt.DryRun == ast.DryRunQuery {
		return buildDryRunResults(stmt.DryRun, []string{selectSQL.sql}, se.GetSessionVars().BatchSize.MaxChunkSize)
	}

	// TODO: choose an appropriate quota.
	// Use the mem-quota-query as a workaround. As a result, a NT-DML may consume 2x of the memory quota.
	memTracker := memory.NewTracker(memory.LabelForNonTransactionalDML, -1)
	memTracker.AttachTo(se.GetSessionVars().MemTracker)
	se.GetSessionVars().MemTracker.SetBytesLimit(se.GetSessionVars().MemQuotaQuery)
	defer memTracker.Detach()
	buildJobStart := time.Now()
	jobs, err := buildShardJobs(ctx, stmt, se, selectSQL, shardColumnInfo, memTracker)
	if err != nil {
		return nil, err
	}
	logutil.Logger(ctx).Warn("Non-transactional DML build job",
		zap.Int("count", len(jobs)), zap.Duration("duration", time.Since(buildJobStart)))

	splitStmts, err := runJobs(ctx, jobs, stmt, tableName, se, stmt.DMLStmt.WhereExpr())
	if err != nil {
		return nil, err
	}
	if stmt.DryRun == ast.DryRunSplitDml {
		return buildDryRunResults(stmt.DryRun, splitStmts, se.GetSessionVars().BatchSize.MaxChunkSize)
	}
	if stmt.TransferFromInsert {
		// affected rows will be set in `runJobs`
		return nil, nil
	}
	return buildExecuteResults(ctx, jobs, se.GetSessionVars().BatchSize.MaxChunkSize, se.GetSessionVars().EnableRedactLog)
}

// we require:
// (1) in an update statement, shard column cannot be updated
//
// Note: this is not a comprehensive check.
// We do this to help user prevent some easy mistakes, at an acceptable maintenance cost.
func checkConstraintWithShardColumn(se Session, stmt *ast.NonTransactionalDMLStmt,
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
func checkUpdateShardColumn(se Session, assignments []*ast.Assignment, shardColumnInfo *model.ColumnInfo,
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

func checkConstraint(stmt *ast.NonTransactionalDMLStmt, se Session) error {
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

	switch s := stmt.DMLStmt.(type) {
	case *ast.DeleteStmt:
		if err := checkTableRef(s.TableRefs, true); err != nil {
			return err
		}
		if err := checkReadClauses(s.Limit, s.Order); err != nil {
			return err
		}
		nonTransactionalDeleteCount.Inc()
	case *ast.UpdateStmt:
		if err := checkTableRef(s.TableRefs, true); err != nil {
			return err
		}
		if err := checkReadClauses(s.Limit, s.Order); err != nil {
			return err
		}
		nonTransactionalUpdateCount.Inc()
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
		nonTransactionalInsertCount.Inc()
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
	tableName *ast.TableName, se Session, originalCondition ast.ExprNode) (rs []string, err error) {
	concurrency := se.GetSessionVars().ETLConcurrency
	// no concurrency for dry run.
	if stmt.DryRun != ast.NoDryRun || !stmt.TransferFromInsert {
		concurrency = 0
	}

	var (
		jobCh        chan *job
		errCh        chan error
		wg           sync.WaitGroup
		affectedRows uint64
	)
	if concurrency > 0 {
		// shuffle jobs to avoid hot spot.
		rand.Shuffle(len(jobs), func(i, j int) {
			// do not shuffle first job, because it may be (null, null), a large job, run it at the beginning.
			if i > 0 && j > 0 {
				jobs[i], jobs[j] = jobs[j], jobs[i]
			}
		})
		var sb strings.Builder
		err = stmt.DMLStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
			format.RestoreNameBackQuotes|
			format.RestoreSpacesAroundBinaryOperation|
			format.RestoreBracketAroundBinaryOperation|
			format.RestoreStringWithoutCharset, &sb))
		if err != nil {
			logutil.Logger(ctx).Error("Non-transactional DML, failed to restore the DML statement", zap.Error(err))
			return nil, err
		}
		if concurrency > len(jobs) {
			concurrency = len(jobs)
		}
		dmlSQL := sb.String()
		var originalConditionSQL string
		if originalCondition != nil {
			sb = strings.Builder{}
			originalCondSel := &ast.SelectStmt{
				SelectStmtOpts: &ast.SelectStmtOpts{},
				Where:          originalCondition,
			}
			err = originalCondSel.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
				format.RestoreNameBackQuotes|
				format.RestoreSpacesAroundBinaryOperation|
				format.RestoreBracketAroundBinaryOperation|
				format.RestoreStringWithoutCharset, &sb))
			if err != nil {
				logutil.Logger(ctx).Error("Non-transactional DML, failed to restore the DML statement", zap.Error(err))
				return nil, err
			}
			originalConditionSQL = sb.String()
		}

		logutil.Logger(ctx).Warn("Non-transactional DML execute concurrently", zap.Int("concurrency", concurrency))
		jobCh = make(chan *job, 2*concurrency)
		errCh = make(chan error, 2*concurrency)
		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			if err = jobWorker(ctx, se, jobCh, errCh, len(jobs), stmt, dmlSQL, tableName, originalConditionSQL, &wg, &affectedRows); err != nil {
				return nil, err
			}
		}

		defer func() {
			close(jobCh)
			wg.Wait()
			// in case all worker is exited with error.
			for range jobCh {
			}
			select {
			case e := <-errCh:
				if e != nil {
					err = e
				}
			default:
			}
			close(errCh)
			for range errCh {
			}
			se.GetSessionVars().StmtCtx.AddAffectedRows(affectedRows)
		}()
	}

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
		case e := <-errCh:
			logutil.Logger(ctx).Warn("Non-transactional DML worker exit because error found",
				zap.Int("finished", i), zap.Int("total", len(jobs)), zap.Error(e))
			return nil, e
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
			if concurrency == 0 {
				doOneJob(ctx, &jobs[i], len(jobs), stmtBuildInfo, se, false)
			} else {
				j := jobs[i]
				jobCh <- &j
			}
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

func jobWorker(ctx context.Context, s Session, jobCh <-chan *job, errCh chan<- error, totalJobCount int,
	stmt *ast.NonTransactionalDMLStmt, dmlSQL string, tableName *ast.TableName, originalConditionSQL string, wg *sync.WaitGroup, affectedRows *uint64) error {
	se, err := CreateSession(s.GetStore())
	if err != nil {
		wg.Done()
		return err
	}
	se.GetSessionVars().CurrentDB = s.GetSessionVars().CurrentDB
	se.GetSessionVars().EnableWindowFunction = true
	se.GetSessionVars().ETLConcurrency = 0

	var originalCondition ast.ExprNode
	if originalConditionSQL != "" {
		originalConditionSel, err := se.ParseWithParams(ctx, originalConditionSQL)
		if err != nil {
			se.Close()
			wg.Done()
			return err
		}
		originalCondition = originalConditionSel.(*ast.SelectStmt).Where
	}
	dml, err := se.ParseWithParams(ctx, dmlSQL)
	if err != nil {
		se.Close()
		wg.Done()
		return err
	}
	var dmlStmt ast.ShardableDMLStmt
	switch v := dml.(type) {
	case *ast.InsertStmt:
		dmlStmt = v
	case *ast.DeleteStmt:
		dmlStmt = v
	case *ast.UpdateStmt:
		dmlStmt = v
	default:
		se.Close()
		wg.Done()
		return errors.Errorf("unexpected type %T", v)
	}
	// copy stmt to avoid race.
	stmt = &ast.NonTransactionalDMLStmt{
		DryRun: stmt.DryRun,
		ShardColumn: &ast.ColumnName{
			Schema: stmt.ShardColumn.Schema,
			Table:  stmt.ShardColumn.Table,
			Name:   stmt.ShardColumn.Name,
		},
		Limit:   stmt.Limit,
		DMLStmt: dmlStmt,
	}

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
		wg.Done()
		se.Close()
		return errors.New("Non-transactional DML, shard column not found")
	}

	go func() {
		defer func() {
			se.Close()
			wg.Done()
		}()
		for j := range jobCh {
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
			doOneJob(ctx, j, totalJobCount, stmtBuildInfo, se, false)
			if j.err != nil {
				errCh <- j.err
				return
			}
			subAffectedRows := se.AffectedRows()
			atomic.AddUint64(affectedRows, subAffectedRows)
		}
	}()
	return nil
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
	var dmlSQLInLog string
	if se.GetSessionVars().EnableRedactLog {
		dmlSQLInLog = parser.Normalize(dmlSQL)
	} else {
		dmlSQLInLog = dmlSQL
	}

	options.stmt.DMLStmt.SetText(nil, fmt.Sprintf("/* job %v/%v */ %s", job.jobID, totalJobCount, dmlSQL))
	rs, err := se.ExecuteStmt(ctx, options.stmt.DMLStmt)

	// collect errors
	failpoint.Inject("batchDMLError", func(val failpoint.Value) {
		if val.(bool) {
			err = errors.New("injected batch(non-transactional) DML error")
		}
	})
	if err != nil {
		logutil.Logger(ctx).Error("Non-transactional DML SQL failed", zap.String("job", dmlSQLInLog), zap.Error(err), zap.Int("jobID", job.jobID), zap.Int("jobSize", job.jobSize))
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

func buildShardJobs(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se Session,
	selectSQL *SelectSQL, shardColumnInfo *model.ColumnInfo, memTracker *memory.Tracker) ([]job, error) {
	if selectSQL.order {
		return buildShardJobsOrdered(ctx, stmt, se, selectSQL.sql, shardColumnInfo, memTracker)
	}
	return buildShardJobsUnordered(ctx, stmt, se, selectSQL.sql, memTracker)
}

func buildShardJobsOrdered(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se Session,
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

	// _tidb_rowid may be null in join, handle this case
	if stmt.ShardColumn.Name.L == "_tidb_rowid" && stmt.TransferFromInsert {
		var nullStart, nullEnd types.Datum
		nullStart.SetNull()
		nullEnd.SetNull()
		jobs = appendNewJob(jobs, 0, nullStart, nullEnd, 1, memTracker)
		if len(jobs) > 1 {
			lastIdx := len(jobs) - 1
			jobs[0], jobs[lastIdx] = jobs[lastIdx], jobs[0]
		}
	}

	return jobs, nil
}

// Int64Slice attaches the methods of Interface to []int64, sorting in increasing order.
type Int64Slice []int64

func (x Int64Slice) Len() int           { return len(x) }
func (x Int64Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x Int64Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func buildShardJobsUnordered(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se Session,
	selectSQL string, memTracker *memory.Tracker) ([]job, error) {
	// A NT-DML is not a SELECT. We ignore the SelectLimit for selectSQL so that it can read all values.
	originalSelectLimit := se.GetSessionVars().SelectLimit
	se.GetSessionVars().SelectLimit = math.MaxUint64
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

	rowIDs := make([]int64, 0, 2<<20)

	chk := rs.NewChunk(nil)
	for {
		err = rs.Next(ctx, chk)
		if err != nil {
			return nil, err
		}
		// last chunk
		if chk.NumRows() == 0 {
			break
		}
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			current := row.GetDatum(0, &rs.Fields()[0].Column.FieldType)
			rowIDs = append(rowIDs, current.GetInt64())
		}
	}

	sort.Sort(Int64Slice(rowIDs))

	jobCount := 0
	currentSize := 0
	jobs := make([]job, 0, (len(rowIDs)/batchSize)+2)
	if len(rowIDs) > 0 {
		var start *types.Datum
		for _, i := range rowIDs {
			if start == nil {
				start = &types.Datum{}
				start.SetInt64(i)
			}
			currentSize++
			if currentSize >= batchSize {
				end := types.Datum{}
				end.SetInt64(i)
				jobs = appendNewJob(jobs, jobCount+1, *start, end, currentSize, memTracker)
				start = nil
				jobCount++
				currentSize = 0
			}
		}
		if start != nil {
			end := types.Datum{}
			end.SetInt64(rowIDs[len(rowIDs)-1])
			jobs = appendNewJob(jobs, jobCount+1, *start, end, currentSize, memTracker)
		}
	}

	// _tidb_rowid may be null in join, handle this case
	if stmt.TransferFromInsert {
		var nullStart, nullEnd types.Datum
		nullStart.SetNull()
		nullEnd.SetNull()
		jobs = appendNewJob(jobs, 0, nullStart, nullEnd, 1, memTracker)
		if len(jobs) > 1 {
			lastIdx := len(jobs) - 1
			jobs[0], jobs[lastIdx] = jobs[lastIdx], jobs[0]
		}
	}

	return jobs, nil
}

func appendNewJob(jobs []job, id int, start types.Datum, end types.Datum, size int, tracker *memory.Tracker) []job {
	jobs = append(jobs, job{jobID: id, start: start, end: end, jobSize: size})
	tracker.Consume(start.EstimatedMemUsage() + end.EstimatedMemUsage() + 64)
	return jobs
}

// SelectSQL is used to shard column.
type SelectSQL struct {
	sql   string
	order bool
}

func buildSelectSQL(stmt *ast.NonTransactionalDMLStmt, se Session) (
	*ast.TableName, *SelectSQL, *model.ColumnInfo, []*ast.TableSource, error) {
	// only use the first table
	join, ok := stmt.DMLStmt.TableRefsJoin()
	if !ok {
		return nil, nil, nil, nil, errors.New("Non-transactional DML, table source not found")
	}
	tableSources := make([]*ast.TableSource, 0)
	aliases := make(map[string]*ast.TableName)
	tableSources, aliases, err := collectTableSourcesInJoin(join, tableSources, aliases)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if len(tableSources) == 0 {
		return nil, nil, nil, nil, errors.New("Non-transactional DML, no tables found in table refs")
	}
	leftMostTableSource := tableSources[0]
	leftMostTableName, ok := leftMostTableSource.Source.(*ast.TableName)
	if !ok {
		return nil, nil, nil, nil, errors.New("Non-transactional DML, table name not found")
	}

	shardColumnInfo, tableName, alias, err := selectShardColumn(stmt, se, tableSources, aliases, leftMostTableName, leftMostTableSource)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	var sb strings.Builder
	if stmt.DMLStmt.WhereExpr() != nil {
		err := stmt.DMLStmt.WhereExpr().Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
			format.RestoreNameBackQuotes|
			format.RestoreSpacesAroundBinaryOperation|
			format.RestoreBracketAroundBinaryOperation|
			format.RestoreStringWithoutCharset, &sb),
		)
		if err != nil {
			return nil, nil, nil, nil, errors.Annotate(err, "Failed to restore where clause in non-transactional DML")
		}
	} else {
		sb.WriteString("TRUE")
	}
	var selectSQL SelectSQL
	// assure NULL values are placed first
	if stmt.ShardColumn.Name.L != "_tidb_rowid" {
		selectSQL.sql = fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` WHERE %s ORDER BY IF(ISNULL(`%s`),0,1),`%s`",
			stmt.ShardColumn.Name.O, tableName.DBInfo.Name.O, tableName.Name.O, sb.String(), stmt.ShardColumn.Name.O, stmt.ShardColumn.Name.O)
		selectSQL.order = true
	} else {
		// avoid sort OOM since _tidb_rowid is not indexed.
		selectSQL.sql = fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` WHERE %s",
			stmt.ShardColumn.Name.O, tableName.DBInfo.Name.O, tableName.Name.O, sb.String())
		selectSQL.order = false
	}
	if alias != "" {
		stmt.ShardColumn.Schema = model.NewCIStr("")
		stmt.ShardColumn.Table = model.NewCIStr(alias)
	}
	return tableName, &selectSQL, shardColumnInfo, tableSources, nil
}

func selectShardColumn(stmt *ast.NonTransactionalDMLStmt, se Session, tableSources []*ast.TableSource, aliases map[string]*ast.TableName,
	leftMostTableName *ast.TableName, leftMostTableSource *ast.TableSource) (
	*model.ColumnInfo, *ast.TableName, string, error) {
	var indexed bool
	var shardColumnInfo *model.ColumnInfo
	var selectedTableName *ast.TableName
	var alias string

	if len(tableSources) == 1 {
		// single table
		leftMostTable, err := domain.GetDomain(se).InfoSchema().TableByName(leftMostTableName.Schema, leftMostTableName.Name)
		if err != nil {
			return nil, nil, "", err
		}
		selectedTableName = leftMostTableName
		indexed, shardColumnInfo, err = selectShardColumnFromTheOnlyTable(
			stmt, leftMostTableName, leftMostTableSource.AsName, leftMostTable)
		if err != nil {
			return nil, nil, "", err
		}
	} else {
		// multi table join
		if stmt.ShardColumn == nil {
			leftMostTable, err := domain.GetDomain(se).InfoSchema().TableByName(leftMostTableName.Schema, leftMostTableName.Name)
			if err != nil {
				return nil, nil, "", err
			}
			selectedTableName = leftMostTableName
			indexed, shardColumnInfo, err = selectShardColumnAutomatically(stmt, leftMostTable, leftMostTableName, leftMostTableSource.AsName)
			if err != nil {
				return nil, nil, "", err
			}
		} else if stmt.ShardColumn.Schema.L != "" && stmt.ShardColumn.Table.L != "" && stmt.ShardColumn.Name.L != "" {
			specifiedDbName := stmt.ShardColumn.Schema
			specifiedTableName := stmt.ShardColumn.Table
			specifiedColName := stmt.ShardColumn.Name

			// the specified table must be in the join
			tableInJoin := false
			var chosenTableName model.CIStr
			for _, tableSource := range tableSources {
				tableSourceName := tableSource.Source.(*ast.TableName)
				tableSourceFinalTableName := tableSource.AsName // precedence: alias name, then table name
				if tableSourceFinalTableName.O == "" {
					tableSourceFinalTableName = tableSourceName.Name
				}
				if tableSourceName.Schema.L == specifiedDbName.L && tableSourceFinalTableName.L == specifiedTableName.L {
					tableInJoin = true
					selectedTableName = tableSourceName
					chosenTableName = tableSourceName.Name
					break
				}
				if tableSourceName.Schema.L == specifiedDbName.L && tableSourceName.Name.L == specifiedTableName.L {
					tableInJoin = true
					selectedTableName = tableSourceName
					chosenTableName = tableSourceName.Name
					break
				}
			}
			if !tableInJoin {
				sources := []string{}
				for _, table := range tableSources {
					asName := table.AsName.L
					if tblName, ok := table.Source.(*ast.TableName); ok {
						sources = append(sources, tblName.Schema.L+"."+tblName.Name.L+" as "+asName)
					}
				}
				logutil.BgLogger().Error("Non-transactional DML, table not found",
					zap.String("tbl", specifiedDbName.L+"."+specifiedTableName.L),
					zap.Strings("tbls", sources))
				return nil, nil, "",
					errors.Errorf(
						"Non-transactional DML, shard column %s.%s.%s is not in the tables involved in the join",
						specifiedDbName.L, specifiedTableName.L, specifiedColName.L,
					)
			}

			tbl, err := domain.GetDomain(se).InfoSchema().TableByName(specifiedDbName, chosenTableName)
			if err != nil {
				return nil, nil, "", err
			}
			indexed, shardColumnInfo, err = selectShardColumnByGivenName(specifiedColName.L, tbl)
			if err != nil {
				return nil, nil, "", err
			}
			for a, aliasedTbl := range aliases {
				if aliasedTbl.Name.L == selectedTableName.Name.L {
					alias = a
					break
				}
			}
		} else {
			return nil, nil, "", errors.New(
				"Non-transactional DML, shard column must be fully specified (i.e. `BATCH ON dbname.tablename.colname`) when multiple tables are involved",
			)
		}
	}
	if !indexed {
		return nil, nil, "", errors.Errorf("Non-transactional DML, shard column %s is not indexed", stmt.ShardColumn.Name.L)
	}
	return shardColumnInfo, selectedTableName, alias, nil
}

func collectTableSourcesInJoin(node ast.ResultSetNode, tableSources []*ast.TableSource, aliases map[string]*ast.TableName) ([]*ast.TableSource, map[string]*ast.TableName, error) {
	if node == nil {
		return tableSources, aliases, nil
	}
	switch x := node.(type) {
	case *ast.Join:
		var (
			err error
		)
		tableSources, aliases, err = collectTableSourcesInJoin(x.Left, tableSources, aliases)
		if err != nil {
			return nil, nil, err
		}
		tableSources, aliases, err = collectTableSourcesInJoin(x.Right, tableSources, aliases)
		if err != nil {
			return nil, nil, err
		}
	case *ast.TableSource:
		// assert it's a table name
		switch v := x.Source.(type) {
		case *ast.TableName:
			tableSources = append(tableSources, x)
			if x.AsName.L != "" {
				aliases[x.AsName.L] = v
			}
		case *ast.SelectStmt:
			var (
				subSources = make([]*ast.TableSource, 0)
				err        error
			)
			if v.From == nil {
				return tableSources, aliases, nil
			}
			subSources, aliases, err = collectTableSourcesInJoin(v.From.TableRefs, subSources, aliases)
			if err != nil {
				return nil, nil, err
			}
			tableSources = append(tableSources, subSources...)
			if x.AsName.L != "" {
				if ss, ok := v.From.TableRefs.Left.(*ast.TableSource); ok {
					if st, ok := ss.Source.(*ast.TableName); ok {
						dupAlias := ""
						for alias, tbl := range aliases {
							if tbl == st {
								dupAlias = alias
								break
							}
						}
						if dupAlias != "" {
							delete(aliases, dupAlias)
						}
						aliases[x.AsName.L] = st
					}
				}
			}
		default:
			// ignore the rest.
		}
	default:
		return nil, nil, errors.Errorf("Non-transactional DML, unknown type %T in table refs", node)
	}
	return tableSources, aliases, nil
}

// it attempts to auto-select a shard column from handle if not specified, and fills back the corresponding info in the stmt,
// making it transparent to following steps
func selectShardColumnFromTheOnlyTable(stmt *ast.NonTransactionalDMLStmt, tableName *ast.TableName,
	tableAsName model.CIStr, tbl table.Table) (
	indexed bool, shardColumnInfo *model.ColumnInfo, err error) {
	if stmt.ShardColumn == nil {
		return selectShardColumnAutomatically(stmt, tbl, tableName, tableAsName)
	}

	return selectShardColumnByGivenName(stmt.ShardColumn.Name.L, tbl)
}

func selectShardColumnByGivenName(shardColumnName string, tbl table.Table) (
	indexed bool, shardColumnInfo *model.ColumnInfo, err error) {
	tableInfo := tbl.Meta()
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

func selectShardColumnAutomatically(stmt *ast.NonTransactionalDMLStmt, tbl table.Table,
	tableName *ast.TableName, tableAsName model.CIStr) (bool, *model.ColumnInfo, error) {
	// auto-detect shard column
	var shardColumnInfo *model.ColumnInfo
	tableInfo := tbl.Meta()
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
				return false, nil, errors.New("Non-transactional DML, the clustered index contains multiple columns. Please specify a shard column")
			}
		}
		if shardColumnInfo == nil {
			return false, nil, errors.New("Non-transactional DML, the clustered index is not found")
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
	logutil.Logger(ctx).Error("Non-transactional DML failed",
		zap.Int("num_failed_jobs", len(failedJobs)), zap.String("failed_jobs", errStr))

	return nil, fmt.Errorf("%d/%d jobs failed in the non-transactional DML: %s, ...(more in logs)",
		len(failedJobs), len(jobs), errStr[:mathutil.Min(500, len(errStr)-1)])
}
