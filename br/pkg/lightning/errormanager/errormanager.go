// Copyright 2021 PingCAP, Inc.
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

package errormanager

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/pingcap/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/utils"
)

const (
	createSchema = `
		CREATE SCHEMA IF NOT EXISTS %s;
	`

	syntaxErrorTableName   = "syntax_error_v1"
	typeErrorTableName     = "type_error_v1"
	conflictErrorTableName = "conflict_error_v1"

	createSyntaxErrorTable = `
		CREATE TABLE IF NOT EXISTS %s.` + syntaxErrorTableName + ` (
			task_id     bigint NOT NULL,
			create_time datetime(6) NOT NULL DEFAULT now(6),
			table_name  varchar(261) NOT NULL,
			path        varchar(2048) NOT NULL,
			offset      bigint NOT NULL,
			error       text NOT NULL,
			context     text
		);
	`

	createTypeErrorTable = `
		CREATE TABLE IF NOT EXISTS %s.` + typeErrorTableName + ` (
			task_id     bigint NOT NULL,
			create_time datetime(6) NOT NULL DEFAULT now(6),
			table_name  varchar(261) NOT NULL,
			path        varchar(2048) NOT NULL,
			offset      bigint NOT NULL,
			error       text NOT NULL,
			row_data    text NOT NULL
		);
	`

	createConflictErrorTable = `
		CREATE TABLE IF NOT EXISTS %s.` + conflictErrorTableName + ` (
			task_id     bigint NOT NULL,
			create_time datetime(6) NOT NULL DEFAULT now(6),
			table_name  varchar(261) NOT NULL,
			index_name  varchar(128) NOT NULL,
			key_data    text NOT NULL COMMENT 'decoded from raw_key, human readable only, not for machine use',
			row_data    text NOT NULL COMMENT 'decoded from raw_row, human readable only, not for machine use',
			raw_key     mediumblob NOT NULL COMMENT 'the conflicted key',
			raw_value   mediumblob NOT NULL COMMENT 'the value of the conflicted key',
			raw_handle  mediumblob NOT NULL COMMENT 'the data handle derived from the conflicted key or value',
			raw_row     mediumblob NOT NULL COMMENT 'the data retrieved from the handle',
			KEY (task_id, table_name)
		);
	`

	insertIntoTypeError = `
		INSERT INTO %s.` + typeErrorTableName + `
		(task_id, table_name, path, offset, error, row_data)
		VALUES (?, ?, ?, ?, ?, ?);
	`

	insertIntoConflictErrorData = `
		INSERT INTO %s.` + conflictErrorTableName + `
		(task_id, table_name, index_name, key_data, row_data, raw_key, raw_value, raw_handle, raw_row)
		VALUES
	`

	sqlValuesConflictErrorData = "(?,?,'PRIMARY',?,?,?,?,raw_key,raw_value)"

	insertIntoConflictErrorIndex = `
		INSERT INTO %s.` + conflictErrorTableName + `
		(task_id, table_name, index_name, key_data, row_data, raw_key, raw_value, raw_handle, raw_row)
		VALUES
	`

	sqlValuesConflictErrorIndex = "(?,?,?,?,?,?,?,?,?)"

	selectConflictKeys = `
		SELECT _tidb_rowid, raw_handle, raw_row
		FROM %s.` + conflictErrorTableName + `
		WHERE table_name = ? AND _tidb_rowid >= ? and _tidb_rowid < ?
		ORDER BY _tidb_rowid LIMIT ?;
	`
)

type ErrorManager struct {
	db             *sql.DB
	taskID         int64
	schemaEscaped  string
	configError    *config.MaxError
	remainingError config.MaxError
	dupResolution  config.DuplicateResolutionAlgorithm
}

func (em *ErrorManager) TypeErrorsRemain() int64 {
	return em.remainingError.Type.Load()
}

// New creates a new error manager.
func New(db *sql.DB, cfg *config.Config) *ErrorManager {
	em := &ErrorManager{
		taskID:         cfg.TaskID,
		configError:    &cfg.App.MaxError,
		remainingError: cfg.App.MaxError,
		dupResolution:  cfg.TikvImporter.DuplicateResolution,
	}
	if len(cfg.App.TaskInfoSchemaName) != 0 {
		em.db = db
		em.schemaEscaped = common.EscapeIdentifier(cfg.App.TaskInfoSchemaName)
	}
	return em
}

// Init creates the schemas and tables to store the task information.
func (em *ErrorManager) Init(ctx context.Context) error {
	if em.db == nil || (em.remainingError.Type.Load() == 0 && em.dupResolution == config.DupeResAlgNone) {
		return nil
	}

	exec := common.SQLWithRetry{
		DB:     em.db,
		Logger: log.L(),
	}

	sqls := make([][2]string, 0)
	sqls = append(sqls, [2]string{"create task info schema", createSchema})
	if em.remainingError.Syntax.Load() > 0 {
		sqls = append(sqls, [2]string{"create syntax error table", createSyntaxErrorTable})
	}
	if em.remainingError.Type.Load() > 0 {
		sqls = append(sqls, [2]string{"create type error table", createTypeErrorTable})
	}
	if em.dupResolution != config.DupeResAlgNone && em.remainingError.Conflict.Load() > 0 {
		sqls = append(sqls, [2]string{"create conflict error table", createConflictErrorTable})
	}

	for _, sql := range sqls {
		// trim spaces for unit test pattern matching
		err := exec.Exec(ctx, sql[0], strings.TrimSpace(fmt.Sprintf(sql[1], em.schemaEscaped)))
		if err != nil {
			return err
		}
	}

	return nil
}

// RecordTypeError records a type error.
// If the number of recorded type errors exceed the max-error count, also returns `err` directly.
func (em *ErrorManager) RecordTypeError(
	ctx context.Context,
	logger log.Logger,
	tableName string,
	path string,
	offset int64,
	rowText string,
	encodeErr error,
) error {
	// elide the encode error if needed.
	if em.remainingError.Type.Dec() < 0 {
		threshold := em.configError.Type.Load()
		if threshold > 0 {
			encodeErr = errors.Annotatef(encodeErr, "meet errors exceed the max-error.type threshold '%d'",
				em.configError.Type.Load())
		}
		return encodeErr
	}

	if em.db != nil {
		errMsg := encodeErr.Error()
		logger = logger.With(
			zap.Int64("offset", offset),
			zap.String("row", redact.String(rowText)),
			zap.String("message", errMsg))

		// put it into the database.
		exec := common.SQLWithRetry{
			DB:           em.db,
			Logger:       logger,
			HideQueryLog: redact.NeedRedact(),
		}
		if err := exec.Exec(ctx, "insert type error record",
			fmt.Sprintf(insertIntoTypeError, em.schemaEscaped),
			em.taskID,
			tableName,
			path,
			offset,
			errMsg,
			rowText,
		); err != nil {
			return multierr.Append(encodeErr, err)
		}
	}
	return nil
}

type DataConflictInfo struct {
	RawKey   []byte
	RawValue []byte
	KeyData  string
	Row      string
}

func (em *ErrorManager) RecordDataConflictError(
	ctx context.Context,
	logger log.Logger,
	tableName string,
	conflictInfos []DataConflictInfo,
) error {
	if len(conflictInfos) == 0 {
		return nil
	}

	if em.remainingError.Conflict.Sub(int64(len(conflictInfos))) < 0 {
		threshold := em.configError.Conflict.Load()
		return errors.Errorf(" meet errors exceed the max-error.conflict threshold '%d'", threshold)
	}

	if em.db == nil {
		return nil
	}

	exec := common.SQLWithRetry{
		DB:           em.db,
		Logger:       logger,
		HideQueryLog: redact.NeedRedact(),
	}
	return exec.Transact(ctx, "insert data conflict error record", func(c context.Context, txn *sql.Tx) error {
		sb := &strings.Builder{}
		fmt.Fprintf(sb, insertIntoConflictErrorData, em.schemaEscaped)
		var sqlArgs []interface{}
		for i, conflictInfo := range conflictInfos {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(sqlValuesConflictErrorData)
			sqlArgs = append(sqlArgs,
				em.taskID,
				tableName,
				conflictInfo.KeyData,
				conflictInfo.Row,
				conflictInfo.RawKey,
				conflictInfo.RawValue,
			)
		}
		_, err := txn.ExecContext(c, sb.String(), sqlArgs...)
		return err
	})
}

func (em *ErrorManager) RecordIndexConflictError(
	ctx context.Context,
	logger log.Logger,
	tableName string,
	indexNames []string,
	conflictInfos []DataConflictInfo,
	rawHandles, rawRows [][]byte,
) error {
	if len(conflictInfos) == 0 {
		return nil
	}

	if em.remainingError.Conflict.Sub(int64(len(conflictInfos))) < 0 {
		threshold := em.configError.Conflict.Load()
		return errors.Errorf(" meet errors exceed the max-error.conflict threshold %d", threshold)
	}

	if em.db == nil {
		return nil
	}

	exec := common.SQLWithRetry{
		DB:           em.db,
		Logger:       logger,
		HideQueryLog: redact.NeedRedact(),
	}
	return exec.Transact(ctx, "insert index conflict error record", func(c context.Context, txn *sql.Tx) error {
		sb := &strings.Builder{}
		fmt.Fprintf(sb, insertIntoConflictErrorIndex, em.schemaEscaped)
		var sqlArgs []interface{}
		for i, conflictInfo := range conflictInfos {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(sqlValuesConflictErrorIndex)
			sqlArgs = append(sqlArgs,
				em.taskID,
				tableName,
				indexNames[i],
				conflictInfo.KeyData,
				conflictInfo.Row,
				conflictInfo.RawKey,
				conflictInfo.RawValue,
				rawHandles[i],
				rawRows[i],
			)
		}
		_, err := txn.ExecContext(c, sb.String(), sqlArgs...)
		return err
	})
}

// ResolveAllConflictKeys query all conflicting rows (handle and their
// values) from the current error report and resolve them concurrently.
func (em *ErrorManager) ResolveAllConflictKeys(
	ctx context.Context,
	tableName string,
	pool *utils.WorkerPool,
	fn func(ctx context.Context, handleRows [][2][]byte) error,
) error {
	if em.db == nil {
		return nil
	}

	const rowLimit = 1000
	taskCh := make(chan [2]int64)
	taskWg := &sync.WaitGroup{}
	g, gCtx := errgroup.WithContext(ctx)

	go func() {
		//nolint:staticcheck
		taskWg.Add(1)
		taskCh <- [2]int64{0, math.MaxInt64}
		taskWg.Wait()
		close(taskCh)
	}()

	for t := range taskCh {
		start, end := t[0], t[1]
		pool.ApplyOnErrorGroup(g, func() error {
			defer taskWg.Done()

			var handleRows [][2][]byte
			for start < end {
				rows, err := em.db.QueryContext(
					gCtx, fmt.Sprintf(selectConflictKeys, em.schemaEscaped),
					tableName, start, end, rowLimit)
				if err != nil {
					return errors.Trace(err)
				}
				var lastRowID int64
				for rows.Next() {
					var handleRow [2][]byte
					if err := rows.Scan(&lastRowID, &handleRow[0], &handleRow[1]); err != nil {
						return errors.Trace(err)
					}
					handleRows = append(handleRows, handleRow)
				}
				if err := rows.Err(); err != nil {
					return errors.Trace(err)
				}
				if err := rows.Close(); err != nil {
					return errors.Trace(err)
				}
				if len(handleRows) == 0 {
					break
				}
				if err := fn(gCtx, handleRows); err != nil {
					return errors.Trace(err)
				}
				start = lastRowID + 1
				// If the remaining tasks cannot be processed at once, split the task
				// into two subtasks and send one of them to the other idle worker if possible.
				if end-start > rowLimit {
					mid := start + (end-start)/2
					taskWg.Add(1)
					select {
					case taskCh <- [2]int64{mid, end}:
						end = mid
					default:
						taskWg.Done()
					}
				}
				handleRows = handleRows[:0]
			}
			return nil
		})
	}
	return errors.Trace(g.Wait())
}

func (em *ErrorManager) errorCount(typeVal func(*config.MaxError) int64) int64 {
	cfgVal := typeVal(em.configError)
	val := typeVal(&em.remainingError)
	if val < 0 {
		val = 0
	}
	return cfgVal - val
}

func (em *ErrorManager) typeErrors() int64 {
	return em.errorCount(func(maxError *config.MaxError) int64 {
		return maxError.Type.Load()
	})
}

func (em *ErrorManager) syntaxError() int64 {
	return em.errorCount(func(maxError *config.MaxError) int64 {
		return maxError.Syntax.Load()
	})
}

func (em *ErrorManager) conflictError() int64 {
	return em.errorCount(func(maxError *config.MaxError) int64 {
		return maxError.Conflict.Load()
	})
}

func (em *ErrorManager) charsetError() int64 {
	return em.errorCount(func(maxError *config.MaxError) int64 {
		return maxError.Charset.Load()
	})
}

func (em *ErrorManager) HasError() bool {
	return em.typeErrors() > 0 || em.syntaxError() > 0 ||
		em.charsetError() > 0 || em.conflictError() > 0
}

// GenErrorLogFields return a slice of zap.Field for each error type
func (em *ErrorManager) LogErrorDetails() {
	fmtErrMsg := func(cnt int64, errType, tblName string) string {
		return fmt.Sprintf("Detect %d %s errors in total, please refer to table %s for more details",
			cnt, errType, em.fmtTableName(tblName))
	}
	if errCnt := em.typeErrors(); errCnt > 0 {
		log.L().Warn(fmtErrMsg(errCnt, "data type", typeErrorTableName))
	}
	if errCnt := em.syntaxError(); errCnt > 0 {
		log.L().Warn(fmtErrMsg(errCnt, "data type", syntaxErrorTableName))
	}
	if errCnt := em.charsetError(); errCnt > 0 {
		// TODO: add charset table name
		log.L().Warn(fmtErrMsg(errCnt, "data type", ""))
	}
	if errCnt := em.conflictError(); errCnt > 0 {
		log.L().Warn(fmtErrMsg(errCnt, "data type", conflictErrorTableName))
	}
}

func (em *ErrorManager) fmtTableName(t string) string {
	return fmt.Sprintf("%s.`%s`", em.schemaEscaped, t)
}

// Output renders a table which contains error summery for each error type.
func (em *ErrorManager) Output() string {
	if !em.HasError() {
		return ""
	}

	t := table.NewWriter()
	t.AppendHeader(table.Row{"#", "Error Type", "Error Count", "Error Data Table"})
	t.SetColumnConfigs([]table.ColumnConfig{
		{Name: "#", WidthMax: 6},
		{Name: "Error Type", WidthMax: 20},
		{Name: "Error Count", WidthMax: 12},
		{Name: "Error Data Table", WidthMax: 42},
	})
	t.SetAllowedRowLength(80)
	t.SetRowPainter(func(row table.Row) text.Colors {
		return text.Colors{text.FgRed}
	})

	count := 0
	if errCnt := em.typeErrors(); errCnt > 0 {
		count++
		t.AppendRow(table.Row{count, "Data Type", errCnt, em.fmtTableName(typeErrorTableName)})
	}
	if errCnt := em.syntaxError(); errCnt > 0 {
		count++
		t.AppendRow(table.Row{count, "Data Syntax", errCnt, em.fmtTableName(syntaxErrorTableName)})
	}
	if errCnt := em.charsetError(); errCnt > 0 {
		count++
		// do not support record charset error now.
		t.AppendRow(table.Row{count, "Charset Error", errCnt, ""})
	}
	if errCnt := em.conflictError(); errCnt > 0 {
		count++
		t.AppendRow(table.Row{count, "Unique Key Conflict", errCnt, em.fmtTableName(conflictErrorTableName)})
	}

	res := "\nImport Data Error Summary: \n"
	res += t.Render()
	res += "\n"

	return res
}
