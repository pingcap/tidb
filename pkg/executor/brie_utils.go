// Copyright 2023 PingCAP, Inc.
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
	"math/rand"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const (
	defaultCapOfCreateTable    = 512
	defaultCapOfCreateDatabase = 64
)

// SplitBatchCreateTableForTest is only used for test.
var SplitBatchCreateTableForTest = splitBatchCreateTable

// showRestoredCreateDatabase shows the result of SHOW CREATE DATABASE from a dbInfo.
func showRestoredCreateDatabase(sctx sessionctx.Context, db *model.DBInfo, brComment string) (string, error) {
	result := bytes.NewBuffer(make([]byte, 0, defaultCapOfCreateDatabase))
	if len(brComment) > 0 {
		// this can never fail.
		_, _ = result.WriteString(brComment)
	}
	if err := ConstructResultOfShowCreateDatabase(sctx, db, true, result); err != nil {
		return "", errors.Trace(err)
	}
	return result.String(), nil
}

// BRIECreateDatabase creates the database with OnExistIgnore option
func BRIECreateDatabase(sctx sessionctx.Context, schema *model.DBInfo, brComment string) error {
	d := domain.GetDomain(sctx).DDL()
	query, err := showRestoredCreateDatabase(sctx, schema, brComment)
	if err != nil {
		return errors.Trace(err)
	}
	originQuery := sctx.Value(sessionctx.QueryString)
	sctx.SetValue(sessionctx.QueryString, query)
	defer func() {
		sctx.SetValue(sessionctx.QueryString, originQuery)
	}()

	schema = schema.Clone()
	if len(schema.Charset) == 0 {
		schema.Charset = mysql.DefaultCharset
	}
	return d.CreateSchemaWithInfo(sctx, schema, ddl.OnExistIgnore)
}

// showRestoredCreateTable shows the result of SHOW CREATE TABLE from a tableInfo.
func showRestoredCreateTable(sctx sessionctx.Context, tbl *model.TableInfo, brComment string) (string, error) {
	result := bytes.NewBuffer(make([]byte, 0, defaultCapOfCreateTable))
	if len(brComment) > 0 {
		// this can never fail.
		_, _ = result.WriteString(brComment)
	}
	if err := ConstructResultOfShowCreateTable(sctx, tbl, autoid.Allocators{}, result); err != nil {
		return "", err
	}
	return result.String(), nil
}

// BRIECreateTable creates the table with OnExistIgnore option
func BRIECreateTable(
	sctx sessionctx.Context,
	dbName model.CIStr,
	table *model.TableInfo,
	brComment string,
	cs ...ddl.CreateTableOption,
) error {
	d := domain.GetDomain(sctx).DDL()
	query, err := showRestoredCreateTable(sctx, table, brComment)
	if err != nil {
		return err
	}
	originQuery := sctx.Value(sessionctx.QueryString)
	sctx.SetValue(sessionctx.QueryString, query)
	// Disable foreign key check when batch create tables.
	originForeignKeyChecks := sctx.GetSessionVars().ForeignKeyChecks
	sctx.GetSessionVars().ForeignKeyChecks = false
	defer func() {
		sctx.SetValue(sessionctx.QueryString, originQuery)
		sctx.GetSessionVars().ForeignKeyChecks = originForeignKeyChecks
	}()

	table = table.Clone()

	return d.CreateTableWithInfo(sctx, dbName, table, nil, append(cs, ddl.WithOnExist(ddl.OnExistIgnore))...)
}

// BRIECreateTables creates the tables with OnExistIgnore option in batch
func BRIECreateTables(
	sctx sessionctx.Context,
	tables map[string][]*model.TableInfo,
	brComment string,
	cs ...ddl.CreateTableOption,
) error {
	// Disable foreign key check when batch create tables.
	originForeignKeyChecks := sctx.GetSessionVars().ForeignKeyChecks
	sctx.GetSessionVars().ForeignKeyChecks = false
	originQuery := sctx.Value(sessionctx.QueryString)
	defer func() {
		sctx.SetValue(sessionctx.QueryString, originQuery)
		sctx.GetSessionVars().ForeignKeyChecks = originForeignKeyChecks
	}()
	for db, tablesInDB := range tables {
		dbName := model.NewCIStr(db)
		queryBuilder := strings.Builder{}
		cloneTables := make([]*model.TableInfo, 0, len(tablesInDB))
		for _, table := range tablesInDB {
			query, err := showRestoredCreateTable(sctx, table, brComment)
			if err != nil {
				return errors.Trace(err)
			}

			queryBuilder.WriteString(query)
			queryBuilder.WriteString(";")

			cloneTables = append(cloneTables, table.Clone())
		}
		sctx.SetValue(sessionctx.QueryString, queryBuilder.String())
		if err := splitBatchCreateTable(sctx, dbName, cloneTables, cs...); err != nil {
			//It is possible to failure when TiDB does not support model.ActionCreateTables.
			//In this circumstance, BatchCreateTableWithInfo returns errno.ErrInvalidDDLJob,
			//we fall back to old way that creating table one by one
			log.Warn("batch create table from tidb failure", zap.Error(err))
			return err
		}
	}

	return nil
}

// splitBatchCreateTable provide a way to split batch into small batch when batch size is large than 6 MB.
// The raft entry has limit size of 6 MB, a batch of CreateTables may hit this limitation
// TODO: shall query string be set for each split batch create, it looks does not matter if we set once for all.
func splitBatchCreateTable(sctx sessionctx.Context, schema model.CIStr,
	infos []*model.TableInfo, cs ...ddl.CreateTableOption) error {
	var err error
	d := domain.GetDomain(sctx).DDL()
	err = d.BatchCreateTableWithInfo(sctx, schema, infos, append(cs, ddl.WithOnExist(ddl.OnExistIgnore))...)
	if kv.ErrEntryTooLarge.Equal(err) {
		log.Info("entry too large, split batch create table", zap.Int("num table", len(infos)))
		if len(infos) == 1 {
			return err
		}
		mid := len(infos) / 2
		err = splitBatchCreateTable(sctx, schema, infos[:mid], cs...)
		if err != nil {
			return err
		}
		err = splitBatchCreateTable(sctx, schema, infos[mid:], cs...)
		if err != nil {
			return err
		}
		return nil
	}
	return err
}

func addTaskToMetaTable(ctx context.Context, info *brieTaskInfo, e *exec.BaseExecutor) (uint64, error) {
	failpoint.Inject("ignoreMetaTable", func() {
		taskID := rand.Uint64() % 1000
		failpoint.Return(taskID, nil)
	})
	if info.queueTime.IsZero() {
		return 0, errors.New("queueTime is not set")
	}
	if info.storage == "" {
		return 0, errors.New("storage is not set")
	}

	escapedStorage := fmt.Sprintf("'%s'", info.storage)
	escapedQuery := strings.ReplaceAll(info.query, "'", "''")

	// Construct the SQL statement with escaped strings
	insertStmt := fmt.Sprintf(`
    INSERT INTO mysql.tidb_br_jobs (
         query, queueTime, kind, storage, connID, state, progress
    ) VALUES ('%s', '%s', '%s', %s, %d, '%s', %d);
    `, escapedQuery,
		info.queueTime,
		info.kind,
		escapedStorage,
		info.connID,
		"Wait",
		0,
	)
	log.Debug("addTaskToMetaTable", zap.String("query", insertStmt))

	// BEGIN
	stmtCtx := util.WithInternalSourceType(ctx, kv.InternalTxnBR)
	_, err := e.Ctx().GetSQLExecutor().ExecuteInternal(stmtCtx, `BEGIN;`)
	if err != nil {
		return 0, err
	}
	// INSERT INTO mysql.tidb_br_jobs;
	_, err = e.Ctx().GetSQLExecutor().ExecuteInternal(stmtCtx, insertStmt)
	if err != nil {
		log.Error("Failed to insert BRIE task into tidb_br_jobs", zap.Error(err))
		return 0, err
	}
	// SELECT LAST_INSERT_ID();
	rs, err := e.Ctx().GetSQLExecutor().ExecuteInternal(stmtCtx, `SELECT LAST_INSERT_ID();`)
	if err != nil {
		return 0, err
	}
	defer terror.Call(rs.Close)
	// COMMIT
	_, err = e.Ctx().GetSQLExecutor().ExecuteInternal(stmtCtx, `COMMIT;`)
	if err != nil {
		return 0, err
	}

	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, errors.Errorf("unexpected result length: %d", len(rows))
	}

	return rows[0].GetUint64(0), nil
}

// RefineMessage removes duplicate errors in the message field
func RefineMessage(input string) string {
	// Find the position of the first occurrence of ']'
	pos := strings.Index(input, "]")
	if pos == -1 {
		return input
	}

	// Extract the header part [filename:xx]
	header := input[:pos+1]

	// Extract the rest of the string (description and errors)
	rest := input[pos+1:]

	// Find the position of the first occurrence of ':'
	colonPos := strings.Index(rest, ":")
	if colonPos == -1 {
		return input
	}

	// Extract the description and errors
	description := rest[:colonPos+1]
	errors := rest[colonPos+1:]

	// Split the errors by ';'
	errorList := strings.Split(errors, ";")

	// Use a map to track unique errors
	errorMap := make(map[string]struct{})
	var uniqueErrors []string

	for _, err := range errorList {
		trimmedErr := strings.TrimSpace(err)
		if trimmedErr != "" {
			if _, exists := errorMap[trimmedErr]; !exists {
				errorMap[trimmedErr] = struct{}{}
				uniqueErrors = append(uniqueErrors, trimmedErr)
			}
		}
	}

	// Join unique errors with '; '
	uniqueErrorsStr := strings.Join(uniqueErrors, "; ")

	// Reconstruct the final string
	result := header + description + " " + uniqueErrorsStr
	return result
}

func validateFields(updates map[string]any) (map[string]any, error) {
	validFields := map[string]struct{}{
		"connID":      {},
		"queueTime":   {},
		"kind":        {},
		"query":       {},
		"storage":     {},
		"execTime":    {},
		"state":       {},
		"progress":    {},
		"finishTime":  {},
		"backupTS":    {},
		"restoreTS":   {},
		"archiveSize": {},
		"message":     {},
		"lastUpdate":  {},
	}

	for field, value := range updates {
		if _, exists := validFields[field]; !exists {
			return nil, errors.Errorf("field '%s' is not valid in mysql.tidb_br_jobs", field)
		}
		if field == "message" {
			val, ok := value.(string)
			if !ok || val == "" || val == "NULL" {
				continue
			}
		}
	}
	return updates, nil
}

func updateMetaTable(ctx context.Context, e *exec.BaseExecutor, id uint64, updates map[string]any) {
	failpoint.Inject("ignoreMetaTable", func() {
		failpoint.Return()
	})
	// Construct the 'SET' clause dynamically based on the updates map
	updates, err := validateFields(updates)
	if err != nil {
		log.Error("Detected invalid field name:", zap.Error(err))
		return
	}
	setClauses := make([]string, 0, len(updates))
	args := make([]any, 0, len(updates))

	for column, value := range updates {
		setClauses = append(setClauses, fmt.Sprintf("'%s' = %%?", column))
		args = append(args, value)
	}

	// Construct the final SQL query
	query := fmt.Sprintf("UPDATE mysql.tidb_br_jobs SET %s WHERE id = %d", strings.Join(setClauses, ", "), id)
	log.Info("updateMetaTable", zap.String("query", query), zap.Any("args", args))

	stmtCtx := util.WithInternalSourceType(ctx, kv.InternalTxnBR)
	_, err = e.Ctx().GetSQLExecutor().ExecuteInternal(stmtCtx, query, args...)
	if err != nil {
		log.Error("Failed to update BRIE task into tidb_br_jobs", zap.Error(err), zap.String("query", query))
	}
}
