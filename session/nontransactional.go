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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// job: handle keys in [start, end]
type job struct {
	start   types.Datum
	end     types.Datum
	err     error
	jobID   int
	jobSize int
	sql     string
}

// statementBuildInfo contains information that is needed to build the split statement in a job
type statementBuildInfo struct {
	stmt              *ast.NonTransactionalDeleteStmt
	shardColumnType   types.FieldType
	shardColumnRefer  *ast.ResultField
	originalCondition ast.ExprNode
}

func (j job) String() string {
	return fmt.Sprintf("job id: %d, job size: %d, range: [%s, %s]", j.jobID, j.jobSize, j.start.String(), j.end.String())
}

// HandleNonTransactionalDelete is the entry point for a non-transactional delete
func HandleNonTransactionalDelete(ctx context.Context, stmt *ast.NonTransactionalDeleteStmt, se Session) (sqlexec.RecordSet, error) {
	err := core.Preprocess(se, stmt)
	if err != nil {
		return nil, err
	}
	if !(se.GetSessionVars().IsAutocommit() && !se.GetSessionVars().InTxn()) {
		return nil, errors.Errorf("non-transactional statement can only run in auto-commit mode. auto-commit:%v, inTxn:%v",
			se.GetSessionVars().IsAutocommit(), se.GetSessionVars().InTxn())
	}
	tableName, selectSQL, shardColumnInfo, err := buildSelectSQL(stmt, se)
	if err != nil {
		return nil, err
	}
	if stmt.DryRun == ast.DryRunQuery {
		return buildDryRunResults(stmt.DryRun, []string{selectSQL}, se.GetSessionVars().BatchSize.MaxChunkSize)
	}
	jobs, err := buildShardJobs(ctx, stmt, se, selectSQL, shardColumnInfo)
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
	return buildExecuteResults(jobs, se.GetSessionVars().BatchSize.MaxChunkSize)
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
				Column: col,
				Table:  tableName.TableInfo,
			}
			shardColumnType = col.FieldType
		}
	}
	if shardColumnRefer == nil && stmt.ShardColumn.Name.O != "_tidb_rowid" {
		return nil, errors.New("Non-transactional delete, column not found")
	}

	splitStmts := make([]string, 0, len(jobs))
	for i := range jobs {
		select {
		case <-ctx.Done():
			failedJobs := make([]string, 0)
			for _, job := range jobs {
				if job.err != nil {
					failedJobs = append(failedJobs, fmt.Sprintf("job:%s, error: %s", job.String(), job.err.Error()))
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
			jobs[i].err = errors.Wrap(jobs[i].err, "Early return: error occurred in the first job. All jobs are canceled")
			logutil.Logger(ctx).Error("Non-transactional delete, early return", zap.Error(jobs[i].err))
			break
		}
	}
	return splitStmts, nil
}

func doOneJob(ctx context.Context, job *job, totalJobCount int, options statementBuildInfo, se Session, dryRun bool) string {
	logutil.Logger(ctx).Info("start a Non-transactional delete", zap.String("job", job.String()), zap.Int("totalJobCount", totalJobCount))

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
		job.err = err
		return ""
	}
	deleteSQL := sb.String()

	if dryRun {
		return deleteSQL
	}

	job.sql = deleteSQL
	options.stmt.DeleteStmt.SetText(nil, fmt.Sprintf("/* job %v/%v */ %s", job.jobID, totalJobCount, deleteSQL))
	rs, err := se.ExecuteStmt(ctx, options.stmt.DeleteStmt)

	// collect errors
	failpoint.Inject("splitDeleteError", func(_ failpoint.Value) {
		err = errors.New("injected split delete error")
	})
	if err != nil {
		errStr := fmt.Sprintf("Non-transactional delete SQL failed, sql: %s, error: %s, jobID: %d, jobSize: %d. ",
			deleteSQL, err.Error(), job.jobID, job.jobSize)
		logutil.Logger(ctx).Error(errStr)
		job.err = err
	} else {
		logutil.Logger(ctx).Info("Non-transactional delete SQL finished successfully", zap.Int("jobID", job.jobID),
			zap.Int("jobSize", job.jobSize), zap.String("deleteSQL", deleteSQL))
	}
	if rs != nil {
		rs.Close()
	}
	return ""
}

func buildShardJobs(ctx context.Context, stmt *ast.NonTransactionalDeleteStmt, se Session,
	selectSQL string, shardColumnInfo *model.ColumnInfo) ([]job, error) {
	logutil.Logger(ctx).Info("Non-transactional delete, select SQL", zap.String("selectSQL", selectSQL))
	var shardColumnCollate string
	if shardColumnInfo != nil {
		shardColumnCollate = shardColumnInfo.Collate
	} else {
		shardColumnCollate = ""
	}

	rss, err := se.Execute(ctx, selectSQL)
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
				jobs = append(jobs, job{jobID: jobCount, start: currentStart, end: currentEnd, jobSize: currentSize})
			}
			break
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
					jobs = append(jobs, job{jobID: jobCount, start: *currentStart.Clone(), end: *currentEnd.Clone(), jobSize: currentSize})
					jobCount++
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

func buildSelectSQL(stmt *ast.NonTransactionalDeleteStmt, se Session) (*ast.TableName, string, *model.ColumnInfo, error) {
	// only use the first table
	// TODO: return error if there are multiple tables
	if stmt.DeleteStmt.TableRefs == nil || stmt.DeleteStmt.TableRefs.TableRefs == nil {
		return nil, "", nil, errors.New("table reference is nil")
	}
	if stmt.DeleteStmt.TableRefs.TableRefs.Right != nil {
		return nil, "", nil, errors.New("Non-transactional delete doesn't support multiple tables")
	}
	tableSource, ok := stmt.DeleteStmt.TableRefs.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, "", nil, errors.New("Non-transactional delete, table source not found")
	}
	tableName, ok := tableSource.Source.(*ast.TableName)
	if !ok {
		return nil, "", nil, errors.New("Non-transactional delete, table name not found")
	}

	// the shard column must be indexed
	indexed, shardColumnInfo, err := selectShardColumn(stmt, se, tableName)
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
			return nil, "", nil, errors.Trace(err)
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
func selectShardColumn(stmt *ast.NonTransactionalDeleteStmt, se Session, tableName *ast.TableName) (indexed bool, shardColumnInfo *model.ColumnInfo, err error) {
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
					} else {
						// if the clustered index contains multiple columns, we cannot automatically choose a column as the shard column
						return false, nil, errors.New("Non-transactional delete, the clustered index contains multiple columns. Please specify a shard column")
					}
				}
			}
			if shardColumnInfo == nil {
				return false, nil, errors.New("Non-transactional delete, the clustered index is not found")
			}
		}

		shardColumnName := "_tidb_rowid"
		if shardColumnInfo != nil {
			shardColumnName = shardColumnInfo.Name.L
		}
		stmt.ShardColumn = &ast.ColumnName{
			Schema: tableName.Schema,
			Table:  tableName.Name,
			Name:   model.NewCIStr(shardColumnName),
		}
		return true, shardColumnInfo, nil
	}
	shardColumnName = stmt.ShardColumn.Name.L

	if shardColumnName == "_tidb_rowid" && !tableInfo.HasClusteredIndex() {
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
	if mysql.HasPriKeyFlag(shardColumnInfo.Flag) && tableInfo.PKIsHandle {
		return true, shardColumnInfo, nil
	}

	for _, index := range tbl.Indices() {
		if index.Meta().State != model.StatePublic {
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

func buildExecuteResults(jobs []job, maxChunkSize int) (sqlexec.RecordSet, error) {
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
	resultFields := []*ast.ResultField{
		{
			Column: &model.ColumnInfo{
				FieldType: *types.NewFieldType(mysql.TypeString),
			},
			ColumnAsName: model.NewCIStr("job"),
		},
		{
			Column: &model.ColumnInfo{
				FieldType: *types.NewFieldType(mysql.TypeString),
			},
			ColumnAsName: model.NewCIStr("sql"),
		},
		{
			Column: &model.ColumnInfo{
				FieldType: *types.NewFieldType(mysql.TypeString),
			},
			ColumnAsName: model.NewCIStr("error"),
		},
	}

	rows := make([][]interface{}, 0, len(failedJobs))
	for _, job := range failedJobs {
		row := make([]interface{}, 3)
		row[0] = job.String()
		row[1] = job.sql
		row[2] = job.err.Error()
		rows = append(rows, row)
	}

	return &sqlexec.SimpleRecordSet{
		ResultFields: resultFields,
		Rows:         rows,
		MaxChunkSize: maxChunkSize,
	}, nil
}
