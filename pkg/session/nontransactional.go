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
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/planner/core"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
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
func HandleNonTransactionalDML(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se sessiontypes.Session) (sqlexec.RecordSet, error) {
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
		return buildDryRunResults(stmt.DryRun, []string{selectSQL}, se.GetSessionVars().BatchSize.MaxChunkSize)
	}
	if stmt.Sync == ast.Async {
		return registerGlobalJob(ctx, stmt, se)
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

	splitStmts, err := runJobs(ctx, jobs, stmt, tableName, se, stmt.DMLStmt.WhereExpr())
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
func checkConstraintWithShardColumn(se sessiontypes.Session, stmt *ast.NonTransactionalDMLStmt,
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
func checkUpdateShardColumn(se sessiontypes.Session, assignments []*ast.Assignment, shardColumnInfo *model.ColumnInfo,
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

func checkConstraint(stmt *ast.NonTransactionalDMLStmt, se sessiontypes.Session) error {
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
	tableName *ast.TableName, se sessiontypes.Session, originalCondition ast.ExprNode) ([]string, error) {
	// prepare for the construction of statement
	var shardColumnRefer *ast.ResultField
	var shardColumnType types.FieldType
	for _, col := range tableName.TableInfo.Columns {
		if col.Name.L == stmt.ShardColumn.Name.L {
			shardColumnRefer = &ast.ResultField{
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
			shardColumnRefer = &ast.ResultField{
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

func doOneJob(ctx context.Context, job *job, totalJobCount int, options statementBuildInfo, se sessiontypes.Session, dryRun bool) string {
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

func buildShardJobs(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se sessiontypes.Session,
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

	return jobs, nil
}

func appendNewJob(jobs []job, id int, start types.Datum, end types.Datum, size int, tracker *memory.Tracker) []job {
	jobs = append(jobs, job{jobID: id, start: start, end: end, jobSize: size})
	tracker.Consume(start.EstimatedMemUsage() + end.EstimatedMemUsage() + 64)
	return jobs
}

func buildSelectSQL(stmt *ast.NonTransactionalDMLStmt, se sessiontypes.Session) (
	*ast.TableName, string, *model.ColumnInfo, []*ast.TableSource, error) {
	// only use the first table
	join, ok := stmt.DMLStmt.TableRefsJoin()
	if !ok {
		return nil, "", nil, nil, errors.New("Non-transactional DML, table source not found")
	}
	tableSources := make([]*ast.TableSource, 0)
	tableSources, err := collectTableSourcesInJoin(join, tableSources)
	if err != nil {
		return nil, "", nil, nil, err
	}
	if len(tableSources) == 0 {
		return nil, "", nil, nil, errors.New("Non-transactional DML, no tables found in table refs")
	}
	leftMostTableSource := tableSources[0]
	leftMostTableName, ok := leftMostTableSource.Source.(*ast.TableName)
	if !ok {
		return nil, "", nil, nil, errors.New("Non-transactional DML, table name not found")
	}

	shardColumnInfo, tableName, err := selectShardColumn(stmt, se, tableSources, leftMostTableName, leftMostTableSource)
	if err != nil {
		return nil, "", nil, nil, err
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
			return nil, "", nil, nil, errors.Annotate(err, "Failed to restore where clause in non-transactional DML")
		}
	} else {
		sb.WriteString("TRUE")
	}
	// assure NULL values are placed first
	selectSQL := fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` WHERE %s ORDER BY IF(ISNULL(`%s`),0,1),`%s`",
		stmt.ShardColumn.Name.O, tableName.DBInfo.Name.O, tableName.Name.O, sb.String(), stmt.ShardColumn.Name.O, stmt.ShardColumn.Name.O)
	return tableName, selectSQL, shardColumnInfo, tableSources, nil
}

func selectShardColumn(stmt *ast.NonTransactionalDMLStmt, se sessiontypes.Session, tableSources []*ast.TableSource,
	leftMostTableName *ast.TableName, leftMostTableSource *ast.TableSource) (
	*model.ColumnInfo, *ast.TableName, error) {
	var indexed bool
	var shardColumnInfo *model.ColumnInfo
	var selectedTableName *ast.TableName

	if len(tableSources) == 1 {
		// single table
		leftMostTable, err := domain.GetDomain(se).InfoSchema().TableByName(leftMostTableName.Schema, leftMostTableName.Name)
		if err != nil {
			return nil, nil, err
		}
		selectedTableName = leftMostTableName
		indexed, shardColumnInfo, err = selectShardColumnFromTheOnlyTable(
			stmt, leftMostTableName, leftMostTableSource.AsName, leftMostTable)
		if err != nil {
			return nil, nil, err
		}
	} else {
		// multi table join
		if stmt.ShardColumn == nil {
			leftMostTable, err := domain.GetDomain(se).InfoSchema().TableByName(leftMostTableName.Schema, leftMostTableName.Name)
			if err != nil {
				return nil, nil, err
			}
			selectedTableName = leftMostTableName
			indexed, shardColumnInfo, err = selectShardColumnAutomatically(stmt, leftMostTable, leftMostTableName, leftMostTableSource.AsName)
			if err != nil {
				return nil, nil, err
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
			}
			if !tableInJoin {
				return nil, nil,
					errors.Errorf(
						"Non-transactional DML, shard column %s.%s.%s is not in the tables involved in the join",
						specifiedDbName.L, specifiedTableName.L, specifiedColName.L,
					)
			}

			tbl, err := domain.GetDomain(se).InfoSchema().TableByName(specifiedDbName, chosenTableName)
			if err != nil {
				return nil, nil, err
			}
			indexed, shardColumnInfo, err = selectShardColumnByGivenName(specifiedColName.L, tbl)
			if err != nil {
				return nil, nil, err
			}
		} else {
			return nil, nil, errors.New(
				"Non-transactional DML, shard column must be fully specified (i.e. `BATCH ON dbname.tablename.colname`) when multiple tables are involved",
			)
		}
	}
	if !indexed {
		return nil, nil, errors.Errorf("Non-transactional DML, shard column %s is not indexed", stmt.ShardColumn.Name.L)
	}
	return shardColumnInfo, selectedTableName, nil
}

func collectTableSourcesInJoin(node ast.ResultSetNode, tableSources []*ast.TableSource) ([]*ast.TableSource, error) {
	if node == nil {
		return tableSources, nil
	}
	switch x := node.(type) {
	case *ast.Join:
		var err error
		tableSources, err = collectTableSourcesInJoin(x.Left, tableSources)
		if err != nil {
			return nil, err
		}
		tableSources, err = collectTableSourcesInJoin(x.Right, tableSources)
		if err != nil {
			return nil, err
		}
	case *ast.TableSource:
		// assert it's a table name
		if _, ok := x.Source.(*ast.TableName); !ok {
			return nil, errors.New("Non-transactional DML, table name not found in join")
		}
		tableSources = append(tableSources, x)
	default:
		return nil, errors.Errorf("Non-transactional DML, unknown type %T in table refs", node)
	}
	return tableSources, nil
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
		len(failedJobs), len(jobs), errStr[:min(500, len(errStr)-1)])
}

const batchJobType = "batchJob"
const batchJobConcurrency = 16

type batchJobMeta struct {
	DB            string        `json:"db"`
	Stmt          string        `json:"stmt"`
	ReadStaleness time.Duration `json:"read_staleness"`
}

func registerGlobalJob(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se sessiontypes.Session) (sqlexec.RecordSet, error) {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	taskKey := fmt.Sprintf("batch/%d", uuid.New().ID())
	taskMeta := batchJobMeta{
		DB:            se.GetSessionVars().CurrentDB,
		Stmt:          stmt.Text(),
		ReadStaleness: se.GetSessionVars().ReadStaleness,
	}
	metadata, err := json.Marshal(taskMeta)
	if err != nil {
		return nil, err
	}
	taskID, err := globalTaskManager.AddNewGlobalTask(ctx, taskKey, batchJobType, batchJobConcurrency, metadata)
	if err != nil {
		return nil, err
	}
	resultFields := []*ast.ResultField{
		{
			Column: &model.ColumnInfo{
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
			},
			ColumnAsName: model.NewCIStr("task_id"),
		},
		{
			Column: &model.ColumnInfo{
				FieldType: *types.NewFieldType(mysql.TypeString),
			},
			ColumnAsName: model.NewCIStr("task_key"),
		},
	}
	rows := [][]interface{}{
		{taskID, taskKey},
	}
	return &sqlexec.SimpleRecordSet{
		ResultFields: resultFields,
		Rows:         rows,
		MaxChunkSize: se.GetSessionVars().BatchSize.MaxChunkSize,
	}, nil
}

type batchJobSubtask struct {
	DB            string        `json:"db"`
	Stmt          string        `json:"stmt"`
	ReadStaleness time.Duration `json:"read_staleness"`
	JobID         int           `json:"job_id"`
	Start         []byte        `json:"start"`
	End           []byte        `json:"end"`
}

func (batchJobSubtask) IsMinimalTask() {}

var _ dispatcher.Extension = (*batchExtension)(nil)

type batchDispatcher struct {
	*dispatcher.BaseDispatcher
	store kv.Storage
}

func newBatchDispatcher(ctx context.Context, store kv.Storage, taskMgr dispatcher.TaskManager,
	serverID string, task *proto.Task) dispatcher.Dispatcher {
	dsp := batchDispatcher{
		store:          store,
		BaseDispatcher: dispatcher.NewBaseDispatcher(ctx, taskMgr, serverID, task),
	}
	dsp.Extension = &batchExtension{store}
	return &dsp
}

type batchExtension struct {
	store kv.Storage
}

func (batchExtension) OnTick(_ context.Context, _ *proto.Task) {
}

// StepStr convert proto.Step to string.
func StepStr(step proto.Step) string {
	switch step {
	case proto.StepInit:
		return "init"
	case proto.StepOne:
		return "run"
	case proto.StepDone:
		return "done"
	default:
		return "unknown"
	}
}

func (b batchExtension) OnNextSubtasksBatch(ctx context.Context, h dispatcher.TaskHandle, task *proto.Task, serverInfo []*infosync.ServerInfo, step proto.Step) (subtaskMetas [][]byte, err error) {
	logger := logutil.BgLogger().With(
		zap.Stringer("type", task.Type),
		zap.Int64("task-id", task.ID),
		zap.String("curr-step", StepStr(task.Step)),
		zap.String("next-step", StepStr(step)),
	)

	var taskMeta batchJobMeta
	if err = json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return nil, err
	}
	logger.Info("on next subtasks batch")

	se, err := createSession(b.store)
	if err != nil {
		return nil, err
	}
	se.GetSessionVars().CurrentDB = taskMeta.DB
	se.GetSessionVars().ReadStaleness = taskMeta.ReadStaleness
	// if config.DefaultResourceGroup != "" {
	// 	se.GetSessionVars().ResourceGroupName = config.DefaultResourceGroup
	// }

	stmts, _, err := se.ParseSQL(ctx, taskMeta.Stmt)
	if err != nil {
		return nil, err
	}

	if len(stmts) != 1 {
		return nil, errors.New("Non-transactional DML, only one statement is allowed")
	}

	stmt, ok := stmts[0].(*ast.NonTransactionalDMLStmt)
	if !ok {
		return nil, errors.New("Non-transactional DML, only non-transactional DML is allowed")
	}

	err = core.Preprocess(ctx, se, stmt)
	if err != nil {
		return nil, err
	}

	_, selectSQL, shardColumnInfo, _, err := buildSelectSQL(stmt, se)
	if err != nil {
		return nil, err
	}

	memTracker := memory.NewTracker(memory.LabelForNonTransactionalDML, -1)
	memTracker.AttachTo(se.GetSessionVars().MemTracker)
	se.GetSessionVars().MemTracker.SetBytesLimit(se.GetSessionVars().MemQuotaQuery)
	defer memTracker.Detach()

	jobs, err := buildShardJobs(ctx, stmt, se, selectSQL, shardColumnInfo, memTracker)
	if err != nil {
		return nil, err
	}

	for _, job := range jobs {
		start, err := codec.EncodeKey(se.GetSessionVars().StmtCtx.TimeZone(), nil, job.start)
		if err != nil {
			return nil, err
		}
		end, err := codec.EncodeKey(se.GetSessionVars().StmtCtx.TimeZone(), nil, job.end)
		if err != nil {
			return nil, err
		}
		subtask := batchJobSubtask{
			DB:            taskMeta.DB,
			Stmt:          taskMeta.Stmt,
			ReadStaleness: taskMeta.ReadStaleness,
			JobID:         job.jobID,
			Start:         start,
			End:           end,
		}
		meta, err := json.Marshal(subtask)
		if err != nil {
			return nil, err
		}
		subtaskMetas = append(subtaskMetas, meta)
	}
	return
}

func (batchExtension) OnDone(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task) error {
	return nil
}

func (batchExtension) GetEligibleInstances(ctx context.Context, task *proto.Task) ([]*infosync.ServerInfo, bool, error) {
	serverInfos, err := dispatcher.GenerateSchedulerNodes(ctx)
	if err != nil {
		return nil, true, err
	}
	return serverInfos, true, nil
}

func (batchExtension) IsRetryableErr(error) bool {
	return true
}

func (batchExtension) GetNextStep(task *proto.Task) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.StepOne
	case proto.StepOne:
		return proto.StepDone
	default:
		return proto.StepDone
	}
}

var _ scheduler.Scheduler = (*batchDistScheduler)(nil)
var _ scheduler.Extension = (*batchDistScheduler)(nil)
var _ execute.SubtaskExecutor = (*batchSubtaskExecutor)(nil)

func newBatchDistScheduler(ctx context.Context, id string, task *proto.Task, taskTable scheduler.TaskTable, store kv.Storage) scheduler.Scheduler {
	s := &batchDistScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(ctx, id, task.ID, taskTable),
		store:         store,
	}
	s.BaseScheduler.Extension = s
	return s
}

type batchDistScheduler struct {
	*scheduler.BaseScheduler
	store kv.Storage
}

func (s batchDistScheduler) IsIdempotent(_ *proto.Subtask) bool {
	return true
}

func (s batchDistScheduler) GetSubtaskExecutor(ctx context.Context, task *proto.Task, summary *execute.Summary) (execute.SubtaskExecutor, error) {
	return &batchSubtaskExecutor{s.store}, nil
}

type batchSubtaskExecutor struct {
	store kv.Storage
}

// Init is used to initialize the environment for the subtask executor.
func (e *batchSubtaskExecutor) Init(context.Context) error {
	return nil
}

// RunSubtask is used to run the subtask.
func (e *batchSubtaskExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	batchTask := &batchJobSubtask{}
	err := json.Unmarshal(subtask.Meta, batchTask)
	if err != nil {
		logutil.BgLogger().Error("unmarshal error",
			zap.String("category", "ddl"),
			zap.Error(err))
		return err
	}

	se, err := createSession(e.store)
	if err != nil {
		return err
	}
	se.GetSessionVars().CurrentDB = batchTask.DB
	se.GetSessionVars().ReadStaleness = batchTask.ReadStaleness
	// if config.DefaultResourceGroup != "" {
	// 	se.GetSessionVars().ResourceGroupName = config.DefaultResourceGroup
	// }

	stmts, _, err := se.ParseSQL(ctx, batchTask.Stmt)
	if err != nil {
		return err
	}

	if len(stmts) != 1 {
		return errors.New("Non-transactional DML, only one statement is allowed")
	}

	stmt, ok := stmts[0].(*ast.NonTransactionalDMLStmt)
	if !ok {
		return errors.New("Non-transactional DML, only non-transactional DML is allowed")
	}

	err = core.Preprocess(ctx, se, stmt)
	if err != nil {
		return err
	}

	tableName, _, _, _, err := buildSelectSQL(stmt, se)
	if err != nil {
		return err
	}

	_, start, err := codec.DecodeOne(batchTask.Start)
	if err != nil {
		return err
	}
	_, end, err := codec.DecodeOne(batchTask.End)
	if err != nil {
		return err
	}

	j := job{
		jobID: batchTask.JobID,
		start: start,
		end:   end,
	}

	_, err = runJobs(ctx, []job{j}, stmt, tableName, se, stmt.DMLStmt.WhereExpr())
	return err
}

func (e *batchSubtaskExecutor) Cleanup(context.Context) error {
	return nil
}

func (e *batchSubtaskExecutor) OnFinished(ctx context.Context, subtask *proto.Subtask) error {
	return nil
}

func (e *batchSubtaskExecutor) Rollback(context.Context) error {
	return nil
}

// RegisterBatchDisttask registers handlers for async batch non-transactional DML.
func RegisterBatchDisttask(store kv.Storage) {
	dispatcher.RegisterDispatcherFactory(batchJobType,
		func(ctx context.Context, taskMgr dispatcher.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			return newBatchDispatcher(ctx, store, taskMgr, serverID, task)
		})

	scheduler.RegisterTaskType(batchJobType,
		func(ctx context.Context, id string, task *proto.Task, taskTable scheduler.TaskTable) scheduler.Scheduler {
			return newBatchDistScheduler(ctx, id, task, taskTable, store)
		},
	)
}
