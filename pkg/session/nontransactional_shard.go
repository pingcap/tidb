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
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

func buildSelectSQL(stmt *ast.NonTransactionalDMLStmt, resolveCtx *resolve.Context, se sessionapi.Session) (
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
	tnW := resolveCtx.GetTableName(tableName)
	selectSQL := fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` WHERE %s ORDER BY IF(ISNULL(`%s`),0,1),`%s`",
		stmt.ShardColumn.Name.O, tnW.DBInfo.Name.O, tableName.Name.O, sb.String(), stmt.ShardColumn.Name.O, stmt.ShardColumn.Name.O)
	return tableName, selectSQL, shardColumnInfo, tableSources, nil
}

func selectShardColumn(stmt *ast.NonTransactionalDMLStmt, se sessionapi.Session, tableSources []*ast.TableSource,
	leftMostTableName *ast.TableName, leftMostTableSource *ast.TableSource) (
	*model.ColumnInfo, *ast.TableName, error) {
	var indexed bool
	var shardColumnInfo *model.ColumnInfo
	var selectedTableName *ast.TableName

	if len(tableSources) == 1 {
		// single table
		leftMostTable, err := domain.GetDomain(se).InfoSchema().TableByName(context.Background(), leftMostTableName.Schema, leftMostTableName.Name)
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
			leftMostTable, err := domain.GetDomain(se).InfoSchema().TableByName(context.Background(), leftMostTableName.Schema, leftMostTableName.Name)
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
			var chosenTableName ast.CIStr
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

			tbl, err := domain.GetDomain(se).InfoSchema().TableByName(context.Background(), specifiedDbName, chosenTableName)
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
	tableAsName ast.CIStr, tbl table.Table) (
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
	tableName *ast.TableName, tableAsName ast.CIStr) (bool, *model.ColumnInfo, error) {
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
		Name:   ast.NewCIStr(shardColumnName),
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

	resultFields := []*resolve.ResultField{{
		Column: &model.ColumnInfo{
			FieldType: *types.NewFieldType(mysql.TypeString),
		},
		ColumnAsName: ast.NewCIStr(fieldName),
	}}
	rows := make([][]any, 0, len(results))
	for _, result := range results {
		row := make([]any, 1)
		row[0] = result
		rows = append(rows, row)
	}
	return &sqlexec.SimpleRecordSet{
		ResultFields: resultFields,
		Rows:         rows,
		MaxChunkSize: maxChunkSize,
	}, nil
}

func buildExecuteResults(ctx context.Context, jobs []job, maxChunkSize int, redactLog string) (sqlexec.RecordSet, error) {
	failedJobs := make([]job, 0)
	for _, job := range jobs {
		if job.err != nil {
			failedJobs = append(failedJobs, job)
		}
	}
	if len(failedJobs) == 0 {
		resultFields := []*resolve.ResultField{
			{
				Column: &model.ColumnInfo{
					FieldType: *types.NewFieldType(mysql.TypeLong),
				},
				ColumnAsName: ast.NewCIStr("number of jobs"),
			},
			{
				Column: &model.ColumnInfo{
					FieldType: *types.NewFieldType(mysql.TypeString),
				},
				ColumnAsName: ast.NewCIStr("job status"),
			},
		}
		rows := make([][]any, 1)
		row := make([]any, 2)
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
