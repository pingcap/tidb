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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	tidbtbl "github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

const (
	createSchema = `
		CREATE SCHEMA IF NOT EXISTS %s;
	`

	syntaxErrorTableName = "syntax_error_v1"
	typeErrorTableName   = "type_error_v1"
	// ConflictErrorTableName is the table name for duplicate detection.
	ConflictErrorTableName = "conflict_error_v2"
	// DupRecordTable is the table name to record duplicate data that displayed to user.
	DupRecordTable = "conflict_records"

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
		CREATE TABLE IF NOT EXISTS %s.` + ConflictErrorTableName + ` (
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
			is_data_kv  tinyint(1) NOT NULL,
			INDEX (task_id, table_name),
			INDEX (index_name),
			INDEX (table_name, index_name)
		);
	`

	createDupRecordTable = `
		CREATE TABLE IF NOT EXISTS %s.` + DupRecordTable + ` (
			task_id     bigint NOT NULL,
			create_time datetime(6) NOT NULL DEFAULT now(6),
			table_name  varchar(261) NOT NULL,
			path        varchar(2048) NOT NULL,
			offset      bigint NOT NULL,
			error       text NOT NULL,
			row_id 	    bigint NOT NULL COMMENT 'the row id of the conflicted row',
			row_data    text NOT NULL COMMENT 'the row data of the conflicted row',
			KEY (task_id, table_name)
		);
	`

	insertIntoTypeError = `
		INSERT INTO %s.` + typeErrorTableName + `
		(task_id, table_name, path, offset, error, row_data)
		VALUES (?, ?, ?, ?, ?, ?);
	`

	insertIntoConflictErrorData = `
		INSERT INTO %s.` + ConflictErrorTableName + `
		(task_id, table_name, index_name, key_data, row_data, raw_key, raw_value, raw_handle, raw_row, is_data_kv)
		VALUES
	`

	sqlValuesConflictErrorData = "(?,?,'PRIMARY',?,?,?,?,raw_key,raw_value,?)"

	insertIntoConflictErrorIndex = `
		INSERT INTO %s.` + ConflictErrorTableName + `
		(task_id, table_name, index_name, key_data, row_data, raw_key, raw_value, raw_handle, raw_row, is_data_kv)
		VALUES
	`

	sqlValuesConflictErrorIndex = "(?,?,?,?,?,?,?,?,?,?)"

	selectConflictKeysRemove = `
		SELECT _tidb_rowid, raw_handle, raw_row
		FROM %s.` + ConflictErrorTableName + `
		WHERE table_name = ? AND _tidb_rowid >= ? and _tidb_rowid < ?
		ORDER BY _tidb_rowid LIMIT ?;
	`

	selectIndexConflictKeysReplace = `
		SELECT raw_key, index_name, raw_value, raw_handle
		FROM %s.` + ConflictErrorTableName + `
		WHERE table_name = ? AND is_data_kv = 0
		ORDER BY raw_key;
	`

	selectDataConflictKeysReplace = `
		SELECT raw_key, raw_value
		FROM %s.` + ConflictErrorTableName + `
		WHERE table_name = ? AND is_data_kv = 1
		ORDER BY raw_key;
	`

	insertIntoDupRecord = `
		INSERT INTO %s.` + DupRecordTable + `
		(task_id, table_name, path, offset, error, row_id, row_data)
		VALUES (?, ?, ?, ?, ?, ?, ?);
	`
)

// ErrorManager records errors during the import process.
type ErrorManager struct {
	db             *sql.DB
	taskID         int64
	schemaEscaped  string
	configError    *config.MaxError
	remainingError config.MaxError

	configConflict        *config.Conflict
	conflictErrRemain     *atomic.Int64
	conflictRecordsRemain *atomic.Int64
	conflictV1Enabled     bool
	conflictV2Enabled     bool
	logger                log.Logger
	recordErrorOnce       *atomic.Bool
}

// TypeErrorsRemain returns the number of type errors that can be recorded.
func (em *ErrorManager) TypeErrorsRemain() int64 {
	return em.remainingError.Type.Load()
}

// ConflictErrorsRemain returns the number of conflict errors that can be recorded.
func (em *ErrorManager) ConflictErrorsRemain() int64 {
	return em.conflictErrRemain.Load()
}

// ConflictRecordsRemain returns the number of errors that need be recorded.
func (em *ErrorManager) ConflictRecordsRemain() int64 {
	return em.conflictRecordsRemain.Load()
}

// RecordErrorOnce returns if RecordDuplicateOnce has been called. Not that this
// method is not atomic with RecordDuplicateOnce.
func (em *ErrorManager) RecordErrorOnce() bool {
	return em.recordErrorOnce.Load()
}

// New creates a new error manager.
func New(db *sql.DB, cfg *config.Config, logger log.Logger) *ErrorManager {
	conflictErrRemain := atomic.NewInt64(cfg.Conflict.Threshold)
	conflictRecordsRemain := atomic.NewInt64(cfg.Conflict.MaxRecordRows)
	em := &ErrorManager{
		taskID:                cfg.TaskID,
		configError:           &cfg.App.MaxError,
		remainingError:        cfg.App.MaxError,
		conflictV1Enabled:     cfg.TikvImporter.DuplicateResolution != config.DupeResAlgNone,
		configConflict:        &cfg.Conflict,
		conflictErrRemain:     conflictErrRemain,
		conflictRecordsRemain: conflictRecordsRemain,
		logger:                logger,
		recordErrorOnce:       atomic.NewBool(false),
	}
	switch cfg.TikvImporter.Backend {
	case config.BackendLocal:
		if cfg.Conflict.Strategy != "" {
			em.conflictV2Enabled = true
		}
	case config.BackendTiDB:
		em.conflictV2Enabled = true
	}
	if len(cfg.App.TaskInfoSchemaName) != 0 {
		em.db = db
		em.schemaEscaped = common.EscapeIdentifier(cfg.App.TaskInfoSchemaName)
	}
	return em
}

// Init creates the schemas and tables to store the task information.
func (em *ErrorManager) Init(ctx context.Context) error {
	if em.db == nil {
		return nil
	}

	exec := common.SQLWithRetry{
		DB:     em.db,
		Logger: em.logger,
	}

	sqls := make([][2]string, 0)
	sqls = append(sqls, [2]string{"create task info schema", createSchema})
	if em.remainingError.Syntax.Load() > 0 {
		sqls = append(sqls, [2]string{"create syntax error table", createSyntaxErrorTable})
	}
	if em.remainingError.Type.Load() > 0 {
		sqls = append(sqls, [2]string{"create type error table", createTypeErrorTable})
	}
	if em.conflictV1Enabled {
		sqls = append(sqls, [2]string{"create conflict error v2 table", createConflictErrorTable})
	}
	if em.conflictV2Enabled {
		sqls = append(sqls, [2]string{"create duplicate records table", createDupRecordTable})
	}

	// No need to create task info schema if no error is allowed.
	if len(sqls) == 1 {
		return nil
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
			encodeErr = errors.Annotatef(encodeErr,
				"The number of type errors exceeds the threshold configured by `max-error.type`: '%d'",
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

// DataConflictInfo is the information of a data conflict error.
type DataConflictInfo struct {
	RawKey   []byte
	RawValue []byte
	KeyData  string
	Row      string
}

// RecordDataConflictError records a data conflict error.
func (em *ErrorManager) RecordDataConflictError(
	ctx context.Context,
	logger log.Logger,
	tableName string,
	conflictInfos []DataConflictInfo,
) error {
	var gerr error
	if len(conflictInfos) == 0 {
		return nil
	}

	if em.conflictErrRemain.Sub(int64(len(conflictInfos))) < 0 {
		threshold := em.configConflict.Threshold
		// Still need to record this batch of conflict records, and then return this error at last.
		// Otherwise, if the max-error.conflict is set a very small value, none of the conflict errors will be recorded
		gerr = errors.Errorf(
			"The number of conflict errors exceeds the threshold configured by `conflict.threshold`: '%d'",
			threshold)
	}

	if em.db == nil {
		return gerr
	}

	exec := common.SQLWithRetry{
		DB:           em.db,
		Logger:       logger,
		HideQueryLog: redact.NeedRedact(),
	}
	if err := exec.Transact(ctx, "insert data conflict error record", func(c context.Context, txn *sql.Tx) error {
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
				tablecodec.IsRecordKey(conflictInfo.RawKey),
			)
		}
		_, err := txn.ExecContext(c, sb.String(), sqlArgs...)
		return err
	}); err != nil {
		gerr = err
	}
	return gerr
}

// RecordIndexConflictError records a index conflict error.
func (em *ErrorManager) RecordIndexConflictError(
	ctx context.Context,
	logger log.Logger,
	tableName string,
	indexNames []string,
	conflictInfos []DataConflictInfo,
	rawHandles, rawRows [][]byte,
) error {
	var gerr error
	if len(conflictInfos) == 0 {
		return nil
	}

	if em.conflictErrRemain.Sub(int64(len(conflictInfos))) < 0 {
		threshold := em.configConflict.Threshold
		// Still need to record this batch of conflict records, and then return this error at last.
		// Otherwise, if the max-error.conflict is set a very small value, non of the conflict errors will be recorded
		gerr = errors.Errorf(
			"The number of conflict errors exceeds the threshold configured by `conflict.threshold`: '%d'",
			threshold)
	}

	if em.db == nil {
		return gerr
	}

	exec := common.SQLWithRetry{
		DB:           em.db,
		Logger:       logger,
		HideQueryLog: redact.NeedRedact(),
	}
	if err := exec.Transact(ctx, "insert index conflict error record", func(c context.Context, txn *sql.Tx) error {
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
				tablecodec.IsRecordKey(conflictInfo.RawKey),
			)
		}
		_, err := txn.ExecContext(c, sb.String(), sqlArgs...)
		return err
	}); err != nil {
		gerr = err
	}
	return gerr
}

// RemoveAllConflictKeys query all conflicting rows (handle and their
// values) from the current error report and resolve them concurrently by removing all of them.
func (em *ErrorManager) RemoveAllConflictKeys(
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
		//lint:ignore SA2000
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
					gCtx, fmt.Sprintf(selectConflictKeysRemove, em.schemaEscaped),
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

// ReplaceConflictKeys query all conflicting rows (handle and their
// values) from the current error report and resolve them
// by replacing the necessary rows and reserving the others.
func (em *ErrorManager) ReplaceConflictKeys(
	ctx context.Context,
	tbl tidbtbl.Table,
	tableName string,
	pool *utils.WorkerPool,
	fnGetLatest func(ctx context.Context, key []byte) ([]byte, error),
	fnDeleteKey func(ctx context.Context, key []byte) error,
) error {
	if em.db == nil {
		return nil
	}

	sessionOpts := encode.SessionOptions{
		// TODO: need to find the correct value for SQLMode
		SQLMode: mysql.ModeStrictAllTables,
	}
	encoder, err := kv.NewBaseKVEncoder(&encode.EncodingConfig{
		Table:          tbl,
		SessionOptions: sessionOpts,
		Logger:         em.logger,
	})
	if err != nil {
		return errors.Trace(err)
	}

	g, gCtx := errgroup.WithContext(ctx)
	pool.ApplyOnErrorGroup(g, func() error {
		// TODO: provide a detailed document to explain the algorithm and link it here
		// demo for "replace" algorithm: https://github.com/lyzx2001/tidb-conflict-replace
		// check index KV
		indexKvRows, err := em.db.QueryContext(
			gCtx, fmt.Sprintf(selectIndexConflictKeysReplace, em.schemaEscaped),
			tableName)
		if err != nil {
			return errors.Trace(err)
		}
		defer indexKvRows.Close()
		for indexKvRows.Next() {
			var rawKey, rawValue, rawHandle []byte
			var indexName string
			if err := indexKvRows.Scan(&rawKey, &indexName, &rawValue, &rawHandle); err != nil {
				return errors.Trace(err)
			}
			em.logger.Debug("got raw_key, index_name, raw_value, raw_handle from table",
				zap.Binary("raw_key", rawKey),
				zap.String("index_name", indexName),
				zap.Binary("raw_value", rawValue),
				zap.Binary("raw_handle", rawHandle))

			// get the latest value of rawKey from downstream TiDB
			latestValue, err := fnGetLatest(gCtx, rawKey)
			if tikverr.IsErrNotFound(err) {
				continue
			}
			if err != nil {
				return errors.Trace(err)
			}

			// if the latest value of rawKey equals to rawValue, that means this index KV is maintained in downstream TiDB
			// if not, that means this index KV has been overwritten, and its corresponding data KV needs to be deleted
			if bytes.Equal(rawValue, latestValue) {
				continue
			}

			// rawHandle is the row key of the data KV that needs to be deleted
			// get the latest value of the row key of the data KV that needs to be deleted
			overwritten, err := fnGetLatest(gCtx, rawHandle)
			// if the latest value cannot be found, that means the data KV has been deleted
			if tikverr.IsErrNotFound(err) {
				continue
			}
			if err != nil {
				return errors.Trace(err)
			}

			overwrittenHandle, err := tablecodec.DecodeRowKey(rawHandle)
			if err != nil {
				return errors.Trace(err)
			}
			decodedData, _, err := tables.DecodeRawRowData(encoder.SessionCtx,
				tbl.Meta(), overwrittenHandle, tbl.Cols(), overwritten)
			if err != nil {
				return errors.Trace(err)
			}
			if !tbl.Meta().HasClusteredIndex() {
				// for nonclustered PK, need to append handle to decodedData for AddRecord
				decodedData = append(decodedData, types.NewIntDatum(overwrittenHandle.IntValue()))
			}
			_, err = encoder.Table.AddRecord(encoder.SessionCtx, decodedData)
			if err != nil {
				return errors.Trace(err)
			}

			// find out all the KV pairs that are contained in the data KV
			kvPairs := encoder.SessionCtx.TakeKvPairs()

			exec := common.SQLWithRetry{
				DB:           em.db,
				Logger:       em.logger,
				HideQueryLog: redact.NeedRedact(),
			}

			for _, kvPair := range kvPairs.Pairs {
				em.logger.Debug("got encoded KV",
					logutil.Key("key", kvPair.Key),
					zap.Binary("value", kvPair.Val),
					logutil.Key("rawKey", rawKey),
					zap.Binary("rawValue", rawValue))

				// If rawKey equals to KV pair's key and rawValue equals to KV pair's value,
				// this latest data KV of the index KV needs to be deleted;
				// if not, this latest data KV of the index KV was inserted by other rows,
				// so it is unrelated to the index KV that needs to be deleted, we cannot delete it.

				// An example is:
				// (pk, uk)
				// (1, a)
				// (1, b)
				// (2, a)

				// (1, a) is overwritten by (2, a). We found a->1 is an overwritten index KV,
				// and we are considering if its data KV with key "1" can be deleted.
				// We got the latest value of key "1" which is (1, b),
				// and encode it to get all KV pairs which is [1->b, b->1].
				// Only if there is a->1 we dare to delete data KV with key "1".

				if bytes.Equal(kvPair.Key, rawKey) && bytes.Equal(kvPair.Val, rawValue) {
					if err := exec.Transact(ctx, "insert data conflict error record for conflict detection 'replace' mode",
						func(c context.Context, txn *sql.Tx) error {
							sb := &strings.Builder{}
							fmt.Fprintf(sb, insertIntoConflictErrorData, em.schemaEscaped)
							var sqlArgs []interface{}
							sb.WriteString(sqlValuesConflictErrorData)
							sqlArgs = append(sqlArgs,
								em.taskID,
								tableName,
								nil,
								nil,
								rawHandle,
								overwritten,
								1,
							)
							_, err := txn.ExecContext(c, sb.String(), sqlArgs...)
							return err
						}); err != nil {
						return err
					}
					if err := fnDeleteKey(gCtx, rawHandle); err != nil {
						return errors.Trace(err)
					}
					break
				}
			}
		}
		if err := indexKvRows.Err(); err != nil {
			return errors.Trace(err)
		}

		// check data KV
		dataKvRows, err := em.db.QueryContext(
			gCtx, fmt.Sprintf(selectDataConflictKeysReplace, em.schemaEscaped),
			tableName)
		if err != nil {
			return errors.Trace(err)
		}
		defer dataKvRows.Close()

		var previousRawKey, latestValue []byte
		var mustKeepKvPairs *kv.Pairs

		for dataKvRows.Next() {
			var rawKey, rawValue []byte
			if err := dataKvRows.Scan(&rawKey, &rawValue); err != nil {
				return errors.Trace(err)
			}
			em.logger.Debug("got group raw_key, raw_value from table",
				logutil.Key("raw_key", rawKey),
				zap.Binary("raw_value", rawValue))

			if !bytes.Equal(rawKey, previousRawKey) {
				previousRawKey = rawKey
				// get the latest value of rawKey from downstream TiDB
				latestValue, err = fnGetLatest(gCtx, rawKey)
				if err != nil && !tikverr.IsErrNotFound(err) {
					return errors.Trace(err)
				}
				if latestValue != nil {
					handle, err := tablecodec.DecodeRowKey(rawKey)
					if err != nil {
						return errors.Trace(err)
					}
					decodedData, _, err := tables.DecodeRawRowData(encoder.SessionCtx,
						tbl.Meta(), handle, tbl.Cols(), latestValue)
					if err != nil {
						return errors.Trace(err)
					}
					if !tbl.Meta().HasClusteredIndex() {
						// for nonclustered PK, need to append handle to decodedData for AddRecord
						decodedData = append(decodedData, types.NewIntDatum(handle.IntValue()))
					}
					_, err = encoder.Table.AddRecord(encoder.SessionCtx, decodedData)
					if err != nil {
						return errors.Trace(err)
					}
					// calculate the new mustKeepKvPairs corresponding to the new rawKey
					// find out all the KV pairs that are contained in the data KV
					mustKeepKvPairs = encoder.SessionCtx.TakeKvPairs()
				}
			}

			// if the latest value of rawKey equals to rawValue, that means this data KV is maintained in downstream TiDB
			// if not, that means this data KV has been deleted due to overwritten index KV
			if bytes.Equal(rawValue, latestValue) {
				continue
			}

			handle, err := tablecodec.DecodeRowKey(rawKey)
			if err != nil {
				return errors.Trace(err)
			}
			decodedData, _, err := tables.DecodeRawRowData(encoder.SessionCtx,
				tbl.Meta(), handle, tbl.Cols(), rawValue)
			if err != nil {
				return errors.Trace(err)
			}
			if !tbl.Meta().HasClusteredIndex() {
				// for nonclustered PK, need to append handle to decodedData for AddRecord
				decodedData = append(decodedData, types.NewIntDatum(handle.IntValue()))
			}
			_, err = encoder.Table.AddRecord(encoder.SessionCtx, decodedData)
			if err != nil {
				return errors.Trace(err)
			}

			// find out all the KV pairs that are contained in the data KV
			kvPairs := encoder.SessionCtx.TakeKvPairs()
			for _, kvPair := range kvPairs.Pairs {
				em.logger.Debug("got encoded KV",
					logutil.Key("key", kvPair.Key),
					zap.Binary("value", kvPair.Val))
				kvLatestValue, err := fnGetLatest(gCtx, kvPair.Key)
				if tikverr.IsErrNotFound(err) {
					continue
				}
				if err != nil {
					return errors.Trace(err)
				}

				// if the value of the KV pair is not equal to the latest value of the key of the KV pair
				// that means the value of the KV pair has been overwritten, so it needs no extra operation
				if !bytes.Equal(kvLatestValue, kvPair.Val) {
					continue
				}

				// if the KV pair is contained in mustKeepKvPairs, we cannot delete it
				// if not, delete the KV pair
				if mustKeepKvPairs != nil {
					isContained := slices.ContainsFunc(mustKeepKvPairs.Pairs, func(mustKeepKvPair common.KvPair) bool {
						return bytes.Equal(mustKeepKvPair.Key, kvPair.Key) && bytes.Equal(mustKeepKvPair.Val, kvPair.Val)
					})
					if isContained {
						continue
					}
				}

				if err := fnDeleteKey(gCtx, kvPair.Key); err != nil {
					return errors.Trace(err)
				}
			}
		}
		if err := dataKvRows.Err(); err != nil {
			return errors.Trace(err)
		}

		return nil
	})

	return errors.Trace(g.Wait())
}

// RecordDuplicateCount reduce the counter of "duplicate entry" errors.
// Currently, the count will not be shared for multiple lightning instances.
func (em *ErrorManager) RecordDuplicateCount(cnt int64) error {
	if em.conflictErrRemain.Sub(cnt) < 0 {
		threshold := em.configConflict.Threshold
		return errors.Errorf(
			"The number of conflict errors exceeds the threshold configured by `conflict.threshold`: '%d'",
			threshold)
	}
	return nil
}

// RecordDuplicate records a "duplicate entry" error so user can query them later.
// Currently, the error will not be shared for multiple lightning instances.
func (em *ErrorManager) RecordDuplicate(
	ctx context.Context,
	logger log.Logger,
	tableName string,
	path string,
	offset int64,
	errMsg string,
	rowID int64,
	rowData string,
) error {
	if em.conflictErrRemain.Dec() < 0 {
		threshold := em.configConflict.Threshold
		return errors.Errorf(
			"The number of conflict errors exceeds the threshold configured by `conflict.threshold`: '%d'",
			threshold)
	}
	if em.db == nil {
		return nil
	}
	if em.conflictRecordsRemain.Add(-1) < 0 {
		return nil
	}

	return em.recordDuplicate(ctx, logger, tableName, path, offset, errMsg, rowID, rowData)
}

func (em *ErrorManager) recordDuplicate(
	ctx context.Context,
	logger log.Logger,
	tableName string,
	path string,
	offset int64,
	errMsg string,
	rowID int64,
	rowData string,
) error {
	exec := common.SQLWithRetry{
		DB:           em.db,
		Logger:       logger,
		HideQueryLog: redact.NeedRedact(),
	}
	return exec.Exec(ctx, "insert duplicate record",
		fmt.Sprintf(insertIntoDupRecord, em.schemaEscaped),
		em.taskID,
		tableName,
		path,
		offset,
		errMsg,
		rowID,
		rowData,
	)
}

// RecordDuplicateOnce records a "duplicate entry" error so user can query them later.
// Currently the error will not be shared for multiple lightning instances.
// Different from RecordDuplicate, this function is used when conflict.strategy
// is "error" and will only write the first conflict error to the table.
func (em *ErrorManager) RecordDuplicateOnce(
	ctx context.Context,
	logger log.Logger,
	tableName string,
	path string,
	offset int64,
	errMsg string,
	rowID int64,
	rowData string,
) {
	ok := em.recordErrorOnce.CompareAndSwap(false, true)
	if !ok {
		return
	}
	err := em.recordDuplicate(ctx, logger, tableName, path, offset, errMsg, rowID, rowData)
	if err != nil {
		logger.Warn("meet error when record duplicate entry error", zap.Error(err))
	}
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
	val := em.conflictErrRemain.Load()
	if val < 0 {
		val = 0
	}
	return em.configConflict.Threshold - val
}

func (em *ErrorManager) charsetError() int64 {
	return em.errorCount(func(maxError *config.MaxError) int64 {
		return maxError.Charset.Load()
	})
}

// HasError returns true if any error type has reached the limit
func (em *ErrorManager) HasError() bool {
	return em.typeErrors() > 0 || em.syntaxError() > 0 ||
		em.charsetError() > 0 || em.conflictError() > 0
}

// LogErrorDetails return a slice of zap.Field for each error type
func (em *ErrorManager) LogErrorDetails() {
	fmtErrMsg := func(cnt int64, errType, tblName string) string {
		return fmt.Sprintf("Detect %d %s errors in total, please refer to table %s for more details",
			cnt, errType, em.fmtTableName(tblName))
	}
	if errCnt := em.typeErrors(); errCnt > 0 {
		em.logger.Warn(fmtErrMsg(errCnt, "data type", typeErrorTableName))
	}
	if errCnt := em.syntaxError(); errCnt > 0 {
		em.logger.Warn(fmtErrMsg(errCnt, "data syntax", syntaxErrorTableName))
	}
	if errCnt := em.charsetError(); errCnt > 0 {
		// TODO: add charset table name
		em.logger.Warn(fmtErrMsg(errCnt, "data charset", ""))
	}
	if errCnt := em.conflictError(); errCnt > 0 {
		if em.conflictV1Enabled {
			em.logger.Warn(fmtErrMsg(errCnt, "data conflict", ConflictErrorTableName))
		} else {
			em.logger.Warn(fmtErrMsg(errCnt, "data conflict", DupRecordTable))
		}
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
		if em.conflictV1Enabled {
			t.AppendRow(table.Row{count, "Unique Key Conflict", errCnt, em.fmtTableName(ConflictErrorTableName)})
		} else {
			t.AppendRow(table.Row{count, "Unique Key Conflict", errCnt, em.fmtTableName(DupRecordTable)})
		}
	}

	res := "\nImport Data Error Summary: \n"
	res += t.Render()
	res += "\n"

	return res
}
