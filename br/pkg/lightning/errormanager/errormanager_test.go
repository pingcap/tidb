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
	"database/sql/driver"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	tidbkv "github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestInit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.DuplicateResolution = config.DupeResAlgNone
	cfg.Conflict.Strategy = config.ReplaceOnDup
	cfg.App.MaxError.Type.Store(10)
	cfg.Conflict.Threshold = 20
	cfg.App.TaskInfoSchemaName = "lightning_errors"

	em := New(db, cfg, log.L())
	require.False(t, em.conflictV1Enabled)
	require.True(t, em.conflictV2Enabled)
	require.Equal(t, cfg.App.MaxError.Type.Load(), em.remainingError.Type.Load())
	require.Equal(t, cfg.Conflict.Threshold, em.conflictErrRemain.Load())

	em.remainingError.Type.Store(0)
	em.conflictV1Enabled = false
	em.conflictV2Enabled = false
	ctx := context.Background()
	err = em.Init(ctx)
	require.NoError(t, err)

	em.conflictV1Enabled = true
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_errors`;").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.conflict_error_v2.*").
		WillReturnResult(sqlmock.NewResult(2, 1))
	err = em.Init(ctx)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	em.conflictV2Enabled = true
	em.remainingError.Type.Store(1)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_errors`.*").
		WillReturnResult(sqlmock.NewResult(5, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.type_error_v1.*").
		WillReturnResult(sqlmock.NewResult(6, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.conflict_error_v2.*").
		WillReturnResult(sqlmock.NewResult(7, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.conflict_records.*").
		WillReturnResult(sqlmock.NewResult(7, 1))
	err = em.Init(ctx)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

type mockDriver struct {
	driver.Driver
	totalRows int64
}

func (m mockDriver) Open(_ string) (driver.Conn, error) {
	return mockConn{totalRows: m.totalRows}, nil
}

type mockConn struct {
	driver.Conn
	driver.ExecerContext
	driver.QueryerContext
	totalRows int64
}

func (c mockConn) ExecContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	return sqlmock.NewResult(1, 1), nil
}

func (mockConn) Close() error { return nil }

type mockRows struct {
	driver.Rows
	start int64
	end   int64
}

func (r *mockRows) Columns() []string {
	return []string{"_tidb_rowid", "raw_handle", "raw_row"}
}

func (r *mockRows) Close() error { return nil }

func (r *mockRows) Next(dest []driver.Value) error {
	if r.start >= r.end {
		return io.EOF
	}
	dest[0] = r.start  // _tidb_rowid
	dest[1] = []byte{} // raw_handle
	dest[2] = []byte{} // raw_row
	r.start++
	return nil
}

func (c mockConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	expectedQuery := "SELECT _tidb_rowid, raw_handle, raw_row.*"
	if err := sqlmock.QueryMatcherRegexp.Match(expectedQuery, query); err != nil {
		return &mockRows{}, nil
	}
	if len(args) != 4 {
		return &mockRows{}, nil
	}
	// args are tableName, start, end, and limit.
	start := args[1].Value.(int64)
	if start < 1 {
		start = 1
	}
	end := args[2].Value.(int64)
	if end > c.totalRows+1 {
		end = c.totalRows + 1
	}
	limit := args[3].Value.(int64)
	if start+limit < end {
		end = start + limit
	}
	return &mockRows{start: start, end: end}, nil
}

func TestRemoveAllConflictKeys(t *testing.T) {
	const totalRows = int64(1 << 18)
	driverName := "errmgr-mock-" + strconv.Itoa(rand.Int())
	sql.Register(driverName, mockDriver{totalRows: totalRows})
	db, err := sql.Open(driverName, "")
	require.NoError(t, err)
	defer db.Close()

	cfg := config.NewConfig()
	cfg.TikvImporter.DuplicateResolution = config.DupeResAlgRemove
	cfg.App.TaskInfoSchemaName = "lightning_errors"
	em := New(db, cfg, log.L())
	ctx := context.Background()
	err = em.Init(ctx)
	require.NoError(t, err)

	resolved := atomic.NewInt64(0)
	pool := utils.NewWorkerPool(16, "resolve duplicate rows by remove")
	err = em.RemoveAllConflictKeys(
		ctx, "test", pool,
		func(ctx context.Context, handleRows [][2][]byte) error {
			resolved.Add(int64(len(handleRows)))
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, totalRows, resolved.Load())
}

func TestReplaceConflictOneKey(t *testing.T) {
	column1 := &model.ColumnInfo{
		ID:           1,
		Name:         model.NewCIStr("a"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(mysql.TypeLong),
		Hidden:       true,
		State:        model.StatePublic,
	}
	column1.AddFlag(mysql.PriKeyFlag)

	column2 := &model.ColumnInfo{
		ID:           2,
		Name:         model.NewCIStr("b"),
		Offset:       1,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(mysql.TypeLong),
		Hidden:       true,
		State:        model.StatePublic,
	}

	column3 := &model.ColumnInfo{
		ID:           3,
		Name:         model.NewCIStr("c"),
		Offset:       2,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(mysql.TypeBlob),
		Hidden:       true,
		State:        model.StatePublic,
	}

	index := &model.IndexInfo{
		ID:    1,
		Name:  model.NewCIStr("key_b"),
		Table: model.NewCIStr(""),
		Columns: []*model.IndexColumn{
			{
				Name:   model.NewCIStr("b"),
				Offset: 1,
				Length: -1,
			}},
		Unique:  false,
		Primary: false,
		State:   model.StatePublic,
	}

	table := &model.TableInfo{
		ID:         104,
		Name:       model.NewCIStr("a"),
		Charset:    "utf8mb4",
		Collate:    "utf8mb4_bin",
		Columns:    []*model.ColumnInfo{column1, column2, column3},
		Indices:    []*model.IndexInfo{index},
		PKIsHandle: true,
		State:      model.StatePublic,
	}

	tbl, err := tables.TableFromMeta(tidbkv.NewPanickingAllocators(0), table)
	require.NoError(t, err)

	sessionOpts := encode.SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	}

	encoder, err := tidbkv.NewBaseKVEncoder(&encode.EncodingConfig{
		Table:          tbl,
		SessionOptions: sessionOpts,
		Logger:         log.L(),
	})
	require.NoError(t, err)
	encoder.SessionCtx.GetSessionVars().RowEncoder.Enable = true

	data1 := []types.Datum{
		types.NewIntDatum(1),
		types.NewIntDatum(6),
		types.NewStringDatum("1.csv"),
	}
	data2 := []types.Datum{
		types.NewIntDatum(2),
		types.NewIntDatum(6),
		types.NewStringDatum("2.csv"),
	}
	data3 := []types.Datum{
		types.NewIntDatum(3),
		types.NewIntDatum(3),
		types.NewStringDatum("3.csv"),
	}
	data4 := []types.Datum{
		types.NewIntDatum(3),
		types.NewIntDatum(4),
		types.NewStringDatum("4.csv"),
	}
	data5 := []types.Datum{
		types.NewIntDatum(5),
		types.NewIntDatum(4),
		types.NewStringDatum("5.csv"),
	}
	_, err = encoder.Table.AddRecord(encoder.SessionCtx, data1)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(encoder.SessionCtx, data2)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(encoder.SessionCtx, data3)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(encoder.SessionCtx, data4)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(encoder.SessionCtx, data5)
	require.NoError(t, err)
	kvPairs := encoder.SessionCtx.TakeKvPairs()

	data1IndexKey := kvPairs.Pairs[7].Key
	data1IndexValue := kvPairs.Pairs[7].Val
	data1RowKey := kvPairs.Pairs[4].Key
	data1RowValue := kvPairs.Pairs[4].Val
	data2RowValue := kvPairs.Pairs[6].Val

	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_task_info`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_task_info`\\.conflict_error_v2.*").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mockDB.ExpectQuery("\\QSELECT raw_key, index_name, raw_value, raw_handle FROM `lightning_task_info`.conflict_error_v2 WHERE table_name = ? AND is_data_kv = 0 ORDER BY raw_key\\E").
		WillReturnRows(sqlmock.NewRows([]string{"raw_key", "index_name", "raw_value", "raw_handle"}))
	mockDB.ExpectQuery("\\QSELECT raw_key, raw_value FROM `lightning_task_info`.conflict_error_v2 WHERE table_name = ? AND is_data_kv = 1 ORDER BY raw_key\\E").
		WillReturnRows(sqlmock.NewRows([]string{"raw_key", "raw_value"}).
			AddRow(data1RowKey, data1RowValue).
			AddRow(data1RowKey, data2RowValue))

	cfg := config.NewConfig()
	cfg.TikvImporter.DuplicateResolution = config.DupeResAlgReplace
	cfg.App.TaskInfoSchemaName = "lightning_task_info"
	em := New(db, cfg, log.L())
	err = em.Init(ctx)
	require.NoError(t, err)

	fnGetLatestCount := atomic.NewInt32(0)
	fnDeleteKeyCount := atomic.NewInt32(0)
	pool := utils.NewWorkerPool(16, "resolve duplicate rows by replace")
	err = em.ReplaceConflictKeys(
		ctx, tbl, "test", pool,
		func(ctx context.Context, key []byte) ([]byte, error) {
			fnGetLatestCount.Add(1)
			switch {
			case bytes.Equal(key, data1IndexKey):
				return data1IndexValue, nil
			case bytes.Equal(key, data1RowKey):
				return data1RowValue, nil
			default:
				return nil, fmt.Errorf("key %v is not expected", key)
			}
		},
		func(ctx context.Context, key []byte) error {
			fnDeleteKeyCount.Add(1)
			if !bytes.Equal(key, data1IndexKey) {
				return fmt.Errorf("key %v is not expected", key)
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, int32(3), fnGetLatestCount.Load())
	require.Equal(t, int32(1), fnDeleteKeyCount.Load())
	err = mockDB.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestReplaceConflictOneUniqueKey(t *testing.T) {
	column1 := &model.ColumnInfo{
		ID:           1,
		Name:         model.NewCIStr("a"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(mysql.TypeLong),
		Hidden:       true,
		State:        model.StatePublic,
	}
	column1.AddFlag(mysql.PriKeyFlag)

	column2 := &model.ColumnInfo{
		ID:           2,
		Name:         model.NewCIStr("b"),
		Offset:       1,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(mysql.TypeLong),
		Hidden:       true,
		State:        model.StatePublic,
	}
	column2.AddFlag(mysql.UniqueKeyFlag)

	column3 := &model.ColumnInfo{
		ID:           3,
		Name:         model.NewCIStr("c"),
		Offset:       2,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(mysql.TypeBlob),
		Hidden:       true,
		State:        model.StatePublic,
	}

	index := &model.IndexInfo{
		ID:    1,
		Name:  model.NewCIStr("uni_b"),
		Table: model.NewCIStr(""),
		Columns: []*model.IndexColumn{
			{
				Name:   model.NewCIStr("b"),
				Offset: 1,
				Length: -1,
			}},
		Unique:  true,
		Primary: false,
		State:   model.StatePublic,
	}

	table := &model.TableInfo{
		ID:         104,
		Name:       model.NewCIStr("a"),
		Charset:    "utf8mb4",
		Collate:    "utf8mb4_bin",
		Columns:    []*model.ColumnInfo{column1, column2, column3},
		Indices:    []*model.IndexInfo{index},
		PKIsHandle: true,
		State:      model.StatePublic,
	}

	tbl, err := tables.TableFromMeta(tidbkv.NewPanickingAllocators(0), table)
	require.NoError(t, err)

	sessionOpts := encode.SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	}

	encoder, err := tidbkv.NewBaseKVEncoder(&encode.EncodingConfig{
		Table:          tbl,
		SessionOptions: sessionOpts,
		Logger:         log.L(),
	})
	require.NoError(t, err)
	encoder.SessionCtx.GetSessionVars().RowEncoder.Enable = true

	data1 := []types.Datum{
		types.NewIntDatum(1),
		types.NewIntDatum(6),
		types.NewStringDatum("1.csv"),
	}
	data2 := []types.Datum{
		types.NewIntDatum(2),
		types.NewIntDatum(6),
		types.NewStringDatum("2.csv"),
	}
	data3 := []types.Datum{
		types.NewIntDatum(3),
		types.NewIntDatum(3),
		types.NewStringDatum("3.csv"),
	}
	data4 := []types.Datum{
		types.NewIntDatum(3),
		types.NewIntDatum(4),
		types.NewStringDatum("4.csv"),
	}
	data5 := []types.Datum{
		types.NewIntDatum(5),
		types.NewIntDatum(4),
		types.NewStringDatum("5.csv"),
	}
	_, err = encoder.Table.AddRecord(encoder.SessionCtx, data1)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(encoder.SessionCtx, data2)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(encoder.SessionCtx, data3)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(encoder.SessionCtx, data4)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(encoder.SessionCtx, data5)
	require.NoError(t, err)
	kvPairs := encoder.SessionCtx.TakeKvPairs()

	data1IndexKey := kvPairs.Pairs[7].Key
	data3IndexKey := kvPairs.Pairs[1].Key
	data1IndexValue := kvPairs.Pairs[7].Val
	data2IndexValue := kvPairs.Pairs[9].Val
	data3IndexValue := kvPairs.Pairs[1].Val
	data4IndexValue := kvPairs.Pairs[3].Val
	data1RowKey := kvPairs.Pairs[4].Key
	data2RowKey := kvPairs.Pairs[8].Key
	data3RowKey := kvPairs.Pairs[0].Key
	data4RowKey := kvPairs.Pairs[2].Key
	data1RowValue := kvPairs.Pairs[4].Val
	data2RowValue := kvPairs.Pairs[8].Val
	data3RowValue := kvPairs.Pairs[6].Val
	data4RowValue := kvPairs.Pairs[2].Val

	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_task_info`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_task_info`\\.conflict_error_v2.*").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mockDB.ExpectQuery("\\QSELECT raw_key, index_name, raw_value, raw_handle FROM `lightning_task_info`.conflict_error_v2 WHERE table_name = ? AND is_data_kv = 0 ORDER BY raw_key\\E").
		WillReturnRows(sqlmock.NewRows([]string{"raw_key", "index_name", "raw_value", "raw_handle"}).
			AddRow(data1IndexKey, "uni_b", data1IndexValue, data1RowKey).
			AddRow(data1IndexKey, "uni_b", data2IndexValue, data2RowKey).
			AddRow(data3IndexKey, "uni_b", data3IndexValue, data3RowKey).
			AddRow(data3IndexKey, "uni_b", data4IndexValue, data4RowKey))
	mockDB.ExpectBegin()
	mockDB.ExpectExec("INSERT INTO `lightning_task_info`\\.conflict_error_v2.*").
		WithArgs(0, "test", nil, nil, data2RowKey, data2RowValue, 1).
		WillReturnResult(driver.ResultNoRows)
	mockDB.ExpectCommit()
	mockDB.ExpectBegin()
	mockDB.ExpectExec("INSERT INTO `lightning_task_info`\\.conflict_error_v2.*").
		WithArgs(0, "test", nil, nil, data4RowKey, data4RowValue, 1).
		WillReturnResult(driver.ResultNoRows)
	mockDB.ExpectCommit()
	mockDB.ExpectQuery("\\QSELECT raw_key, raw_value FROM `lightning_task_info`.conflict_error_v2 WHERE table_name = ? AND is_data_kv = 1 ORDER BY raw_key\\E").
		WillReturnRows(sqlmock.NewRows([]string{"raw_key", "raw_value"}).
			AddRow(data1RowKey, data1RowValue).
			AddRow(data1RowKey, data3RowValue))

	cfg := config.NewConfig()
	cfg.TikvImporter.DuplicateResolution = config.DupeResAlgReplace
	cfg.App.TaskInfoSchemaName = "lightning_task_info"
	em := New(db, cfg, log.L())
	err = em.Init(ctx)
	require.NoError(t, err)

	fnGetLatestCount := atomic.NewInt32(0)
	fnDeleteKeyCount := atomic.NewInt32(0)
	pool := utils.NewWorkerPool(16, "resolve duplicate rows by replace")
	err = em.ReplaceConflictKeys(
		ctx, tbl, "test", pool,
		func(ctx context.Context, key []byte) ([]byte, error) {
			fnGetLatestCount.Add(1)
			switch {
			case bytes.Equal(key, data1IndexKey):
				return data1IndexValue, nil
			case bytes.Equal(key, data3IndexKey):
				return data3IndexValue, nil
			case bytes.Equal(key, data1RowKey):
				return data1RowValue, nil
			case bytes.Equal(key, data2RowKey):
				return data2RowValue, nil
			case bytes.Equal(key, data4RowKey):
				return data4RowValue, nil
			default:
				return nil, fmt.Errorf("key %v is not expected", key)
			}
		},
		func(ctx context.Context, key []byte) error {
			fnDeleteKeyCount.Add(1)
			if !bytes.Equal(key, data1IndexKey) && !bytes.Equal(key, data2RowKey) && !bytes.Equal(key, data4RowKey) {
				return fmt.Errorf("key %v is not expected", key)
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, int32(9), fnGetLatestCount.Load())
	require.Equal(t, int32(3), fnDeleteKeyCount.Load())
	err = mockDB.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestErrorMgrHasError(t *testing.T) {
	cfg := &config.Config{}
	cfg.App.MaxError = config.MaxError{
		Syntax:  *atomic.NewInt64(100),
		Charset: *atomic.NewInt64(100),
		Type:    *atomic.NewInt64(100),
	}
	cfg.Conflict.Threshold = 100
	em := &ErrorManager{
		configError:       &cfg.App.MaxError,
		remainingError:    cfg.App.MaxError,
		configConflict:    &cfg.Conflict,
		conflictErrRemain: atomic.NewInt64(100),
	}

	// no field changes, should return false
	require.False(t, em.HasError())

	// change single field
	em.remainingError.Syntax.Sub(1)
	require.True(t, em.HasError())

	em.remainingError = cfg.App.MaxError
	em.remainingError.Charset.Sub(1)
	require.True(t, em.HasError())

	em.remainingError = cfg.App.MaxError
	em.remainingError.Type.Sub(1)
	require.True(t, em.HasError())

	em.remainingError = cfg.App.MaxError
	em.conflictErrRemain.Sub(1)
	require.True(t, em.HasError())

	// change multiple keys
	em.remainingError = cfg.App.MaxError
	em.remainingError.Syntax.Store(0)
	em.remainingError.Charset.Store(0)
	em.remainingError.Type.Store(0)
	em.conflictErrRemain.Store(0)
	require.True(t, em.HasError())
}

func TestErrorMgrErrorOutput(t *testing.T) {
	cfg := &config.Config{}
	cfg.App.MaxError = config.MaxError{
		Syntax:  *atomic.NewInt64(100),
		Charset: *atomic.NewInt64(100),
		Type:    *atomic.NewInt64(100),
	}
	cfg.Conflict.Threshold = 100

	em := &ErrorManager{
		configError:       &cfg.App.MaxError,
		remainingError:    cfg.App.MaxError,
		configConflict:    &cfg.Conflict,
		conflictErrRemain: atomic.NewInt64(100),
		schemaEscaped:     "`error_info`",
		conflictV1Enabled: true,
	}

	output := em.Output()
	require.Equal(t, output, "")

	em.remainingError.Syntax.Sub(1)
	output = em.Output()
	expected := "\n" +
		"Import Data Error Summary: \n" +
		"+---+-------------+-------------+--------------------------------+\n" +
		"| # | ERROR TYPE  | ERROR COUNT | ERROR DATA TABLE               |\n" +
		"+---+-------------+-------------+--------------------------------+\n" +
		"|\x1b[31m 1 \x1b[0m|\x1b[31m Data Syntax \x1b[0m|\x1b[31m           1 \x1b[0m|\x1b[31m `error_info`.`syntax_error_v1` \x1b[0m|\n" +
		"+---+-------------+-------------+--------------------------------+\n"
	require.Equal(t, expected, output)

	em.remainingError = cfg.App.MaxError
	em.remainingError.Syntax.Sub(10)
	em.remainingError.Type.Store(10)
	output = em.Output()
	expected = "\n" +
		"Import Data Error Summary: \n" +
		"+---+-------------+-------------+--------------------------------+\n" +
		"| # | ERROR TYPE  | ERROR COUNT | ERROR DATA TABLE               |\n" +
		"+---+-------------+-------------+--------------------------------+\n" +
		"|\x1b[31m 1 \x1b[0m|\x1b[31m Data Type   \x1b[0m|\x1b[31m          90 \x1b[0m|\x1b[31m `error_info`.`type_error_v1`   \x1b[0m|\n" +
		"|\x1b[31m 2 \x1b[0m|\x1b[31m Data Syntax \x1b[0m|\x1b[31m          10 \x1b[0m|\x1b[31m `error_info`.`syntax_error_v1` \x1b[0m|\n" +
		"+---+-------------+-------------+--------------------------------+\n"
	require.Equal(t, expected, output)

	// change multiple keys
	em.remainingError = cfg.App.MaxError
	em.remainingError.Syntax.Store(0)
	em.remainingError.Charset.Store(0)
	em.remainingError.Type.Store(0)
	em.conflictErrRemain.Store(0)
	output = em.Output()
	expected = "\n" +
		"Import Data Error Summary: \n" +
		"+---+---------------------+-------------+----------------------------------+\n" +
		"| # | ERROR TYPE          | ERROR COUNT | ERROR DATA TABLE                 |\n" +
		"+---+---------------------+-------------+----------------------------------+\n" +
		"|\x1b[31m 1 \x1b[0m|\x1b[31m Data Type           \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m `error_info`.`type_error_v1`     \x1b[0m|\n" +
		"|\x1b[31m 2 \x1b[0m|\x1b[31m Data Syntax         \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m `error_info`.`syntax_error_v1`   \x1b[0m|\n" +
		"|\x1b[31m 3 \x1b[0m|\x1b[31m Charset Error       \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m                                  \x1b[0m|\n" +
		"|\x1b[31m 4 \x1b[0m|\x1b[31m Unique Key Conflict \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m `error_info`.`conflict_error_v2` \x1b[0m|\n" +
		"+---+---------------------+-------------+----------------------------------+\n"
	require.Equal(t, expected, output)

	em.conflictV2Enabled = true
	em.conflictV1Enabled = false
	output = em.Output()
	expected = "\n" +
		"Import Data Error Summary: \n" +
		"+---+---------------------+-------------+---------------------------------+\n" +
		"| # | ERROR TYPE          | ERROR COUNT | ERROR DATA TABLE                |\n" +
		"+---+---------------------+-------------+---------------------------------+\n" +
		"|\x1b[31m 1 \x1b[0m|\x1b[31m Data Type           \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m `error_info`.`type_error_v1`    \x1b[0m|\n" +
		"|\x1b[31m 2 \x1b[0m|\x1b[31m Data Syntax         \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m `error_info`.`syntax_error_v1`  \x1b[0m|\n" +
		"|\x1b[31m 3 \x1b[0m|\x1b[31m Charset Error       \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m                                 \x1b[0m|\n" +
		"|\x1b[31m 4 \x1b[0m|\x1b[31m Unique Key Conflict \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m `error_info`.`conflict_records` \x1b[0m|\n" +
		"+---+---------------------+-------------+---------------------------------+\n"
	require.Equal(t, expected, output)
}
