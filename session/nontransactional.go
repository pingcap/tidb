package session

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
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

func (j job) String() string {
	return fmt.Sprintf("job id: %d, job size: %d, range: [%s, %s]", j.jobID, j.jobSize, j.start.String(), j.end.String())
}

func HandleNonTransactionalDelete(stmt *ast.NonTransactionalDeleteStmt, ctx context.Context, se Session) (sqlexec.RecordSet, error) {
	core.Preprocess(se, stmt)
	if !(se.GetSessionVars().IsAutocommit() && !se.GetSessionVars().InTxn()) {
		return nil, errors.Errorf("non-transactional statement can only run in auto-commit mode. auto=commit:%v, inTxn:%v",
			se.GetSessionVars().IsAutocommit(), se.GetSessionVars().InTxn())
	}
	tableName, selectSQL, err := buildSelectSQL(stmt, se)
	if err != nil {
		return nil, err
	}
	if stmt.DryRun == ast.DryRunQuery {
		return buildDryRunResults(stmt.DryRun, []string{selectSQL}, se.GetSessionVars().BatchSize.MaxChunkSize)
	}
	jobs, err := getShardKeys(ctx, stmt, se, tableName, selectSQL)
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
	tableName *ast.TableName, sqlExecutor sqlexec.SQLExecutor, originalCondition ast.ExprNode) ([]string, error) {

	// prepare for the construction of statement
	var refer *ast.ResultField
	var tp types.FieldType
	for _, col := range tableName.TableInfo.Columns {
		if col.Name.L == stmt.ShardColumn.Name.L {
			refer = &ast.ResultField{
				Column: col,
				Table:  tableName.TableInfo,
			}
			tp = col.FieldType
		}
	}
	if refer == nil && stmt.ShardColumn.Name.O != "_tidb_rowid" {
		return nil, errors.New("Non-transactional delete, column not found")
	}

	splitStmts := make([]string, 0, len(jobs))
	for i := range jobs {
		select {
		case <-ctx.Done():
			logutil.Logger(ctx).Info("Non-transactional delete worker exit because context canceled")
			return nil, ctx.Err()
		default:
		}
		splitStmt := doOneJob(ctx, &jobs[i], len(jobs), stmt, tp, refer, originalCondition, sqlExecutor)
		splitStmts = append(splitStmts, splitStmt)
	}
	return splitStmts, nil
}

func doOneJob(ctx context.Context, job *job, totalJobCount int, stmt *ast.NonTransactionalDeleteStmt, tp types.FieldType, refer *ast.ResultField,
	originalCondition ast.ExprNode, sqlExecutor sqlexec.SQLExecutor) string {
	logutil.Logger(ctx).Info("start a Non-transactional delete", zap.Int("jobID", job.jobID),
		zap.Int("totalJobs", totalJobCount), zap.Int("jobSize", job.jobSize))
	left := &driver.ValueExpr{}
	left.Type = tp
	left.Datum = job.start
	right := &driver.ValueExpr{}
	right.Type = tp
	right.Datum = job.end
	betweenCondition := &ast.BetweenExpr{
		Expr: &ast.ColumnNameExpr{
			Name:  stmt.ShardColumn,
			Refer: refer,
		},
		Left:  left,
		Right: right,
		Not:   false,
	}

	if originalCondition == nil {
		stmt.DeleteStmt.Where = betweenCondition
	} else {
		stmt.DeleteStmt.Where = &ast.BinaryOperationExpr{
			Op: opcode.LogicAnd,
			L:  betweenCondition,
			R:  originalCondition,
		}
	}
	var sb strings.Builder
	err := stmt.DeleteStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
		format.RestoreNameBackQuotes|
		format.RestoreSpacesAroundBinaryOperation|
		format.RestoreBracketAroundBinaryOperation|
		format.RestoreStringWithoutCharset, &sb))
	if err != nil {
		job.err = err
		return ""
	}
	deleteSQL := sb.String()

	if stmt.DryRun == ast.DryRunSplitDml {
		return deleteSQL
	}

	job.sql = deleteSQL
	_, err = sqlExecutor.ExecuteStmt(context.TODO(), stmt.DeleteStmt)
	// collect errors
	failpoint.Inject("splitDeleteError", func(_ failpoint.Value) {
		err = errors.New("injected split delete error")
	})
	if err != nil {
		errStr := fmt.Sprintf("Non-transactional delete SQL failed, sql: %s, error: %s, jobID: %d, jobSize: %d. ",
			deleteSQL, err.Error(), job.jobID, job.jobSize)
		logutil.Logger(ctx).Error(errStr)
		job.err = err
		return ""
	} else {
		logutil.Logger(ctx).Info("Non-transactional delete SQL finished successfully", zap.Int("jobID", job.jobID),
			zap.Int("jobSize", job.jobSize), zap.String("deleteSQL", deleteSQL))
	}

	return ""
}

func getShardKeys(ctx context.Context, stmt *ast.NonTransactionalDeleteStmt, sqlExecutor sqlexec.SQLExecutor, tableName *ast.TableName, selectSQL string) ([]job, error) {
	logutil.Logger(ctx).Info("Non-transactional delete, select SQL", zap.String("selectSQL", selectSQL))
	rss, err := sqlExecutor.Execute(context.TODO(), selectSQL)
	if err != nil {
		return nil, err
	}
	if len(rss) > 1 {
		return nil, errors.New("Non-transactional delete, more than 1 record sets")
	}
	rs := rss[0]

	batchSize := int(stmt.Limit)
	jobCount := 0
	jobs := make([]job, 0)
	currentSize := 0
	var currentStart, currentEnd types.Datum
	newBatchIsComing := true

	chk := rs.NewChunk(nil)
	for {
		err = rs.Next(context.TODO(), chk)
		if err != nil {
			return nil, err
		}

		// last chunk
		if chk.NumRows() == 0 {
			if !newBatchIsComing {
				// there's remaining work
				jobs = append(jobs, job{jobID: jobCount, start: currentStart, end: currentEnd, jobSize: currentSize})
			}
			break
		}

		currentSize += chk.NumRows()
		currentEnd = chk.GetRow(chk.NumRows()-1).GetDatum(0, &rs.Fields()[0].Column.FieldType)

		// a new batch
		if newBatchIsComing {
			currentStart = chk.GetRow(0).GetDatum(0, &rs.Fields()[0].Column.FieldType)
			newBatchIsComing = false
		}

		// end this batch?
		if currentSize > batchSize {
			jobCount++
			jobs = append(jobs, job{
				start:   *currentStart.Clone(),
				end:     *currentEnd.Clone(),
				jobID:   jobCount,
				jobSize: currentSize,
			})

			// get ready for next batch
			newBatchIsComing = true
			currentSize = 0
		}
	}

	return jobs, nil
}

func buildSelectSQL(stmt *ast.NonTransactionalDeleteStmt, se Session) (*ast.TableName, string, error) {
	// only use the first table
	// TODO: return error if there are multiple tables
	if stmt.DeleteStmt.TableRefs == nil || stmt.DeleteStmt.TableRefs.TableRefs == nil {
		return nil, "", errors.New("table reference is nil")
	}
	if stmt.DeleteStmt.TableRefs.TableRefs.Right != nil {
		return nil, "", errors.New("Non-transactional delete doesn't support multiple tables")
	}
	tableSource, ok := stmt.DeleteStmt.TableRefs.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, "", errors.New("Non-transactional delete, table source not found")
	}
	tableName, ok := tableSource.Source.(*ast.TableName)
	if !ok {
		return nil, "", errors.New("Non-transactional delete, table name not found")
	}

	// the shard column must be indexed
	indexed := false
	table, err := sessiontxn.GetTxnManager(se).GetTxnInfoSchema().TableByName(tableName.Schema, tableName.Name)
	if err != nil {
		return nil, "", err
	}
	for _, index := range table.Indices() {
		indexColumns := index.Meta().Columns
		// check only the first column
		if len(indexColumns) > 1 && indexColumns[0].Name.L == stmt.ShardColumn.Name.L {
			indexed = true
			break
		}
	}

	if !indexed {
		return nil, "", errors.Errorf("Non-transactional delete, shard column %s is not indexed", stmt.ShardColumn.Name.L)
	}

	var sb strings.Builder
	if stmt.DeleteStmt.Where != nil {
		err := stmt.DeleteStmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
			format.RestoreNameBackQuotes|
			format.RestoreSpacesAroundBinaryOperation|
			format.RestoreBracketAroundBinaryOperation|
			format.RestoreStringWithoutCharset, &sb))
		if err != nil {
			return nil, "", errors.Trace(err)
		}
	} else {
		sb.WriteString("true")
	}
	selectSQL := fmt.Sprintf("select `%s` from `%s`.`%s` where %s order by `%s`",
		stmt.ShardColumn.Name.O, tableName.DBInfo.Name.O, tableName.Name.O, sb.String(), stmt.ShardColumn.Name.O)
	return tableName, selectSQL, nil
}

func buildDryRunResults(dryRunOption int, results []string, maxChunkSize int) (sqlexec.RecordSet, error) {
	var fieldName string
	if dryRunOption == ast.DryRunSplitDml {
		fieldName = "split statements"
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
