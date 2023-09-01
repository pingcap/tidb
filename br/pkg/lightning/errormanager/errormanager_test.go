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
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	tidbkv "github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
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
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.conflict_error_v1.*").
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
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.conflict_error_v1.*").
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

func TestReplaceConflictKeysIndexKvChecking(t *testing.T) {
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
		ID:         75,
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

	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockDB.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_task_info`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_task_info`\\.conflict_error_v1.*").
		WillReturnResult(sqlmock.NewResult(2, 1))
	rawKeyBase64 := "dIAAAAAAAABLX2mAAAAAAAAAAQOAAAAAAAAABg=="
	rawKey, err := base64.StdEncoding.DecodeString(rawKeyBase64)
	require.NoError(t, err)
	rawValue1Base64 := "AAAAAAAAAAE="
	rawValue1, err := base64.StdEncoding.DecodeString(rawValue1Base64)
	require.NoError(t, err)
	rawValue2Base64 := "AAAAAAAAAAI="
	rawValue2, err := base64.StdEncoding.DecodeString(rawValue2Base64)
	require.NoError(t, err)
	rawHandle1Base64 := "dIAAAAAAAABLX3KAAAAAAAAAAQ=="
	rawHandle1, err := base64.StdEncoding.DecodeString(rawHandle1Base64)
	require.NoError(t, err)
	rawHandle2Base64 := "dIAAAAAAAABLX3KAAAAAAAAAAg=="
	rawHandle2, err := base64.StdEncoding.DecodeString(rawHandle2Base64)
	require.NoError(t, err)
	mockDB.ExpectQuery("\\QSELECT raw_key, index_name, raw_value, raw_handle FROM `lightning_task_info`.conflict_error_v1 WHERE table_name = ? AND index_name <> 'PRIMARY' ORDER BY raw_key\\E").
		WillReturnRows(sqlmock.NewRows([]string{"raw_key", "index_name", "raw_value", "raw_handle"}).
			AddRow(rawKey, "uni_b", rawValue1, rawHandle1).
			AddRow(rawKey, "uni_b", rawValue2, rawHandle2))
	rawRowBase64 := "gAACAAAAAgMBAAYABjIuY3N2"
	rawRow, err := base64.StdEncoding.DecodeString(rawRowBase64)
	require.NoError(t, err)

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
			case bytes.Equal(key, rawKey):
				return rawValue1, nil
			case bytes.Equal(key, rawHandle2):
				return rawRow, nil
			default:
				return nil, fmt.Errorf("key %v is not expected", key)
			}
		},
		func(ctx context.Context, key []byte) error {
			fnDeleteKeyCount.Add(1)
			if !bytes.Equal(key, rawHandle2) {
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
		"|\x1b[31m 4 \x1b[0m|\x1b[31m Unique Key Conflict \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m `error_info`.`conflict_error_v1` \x1b[0m|\n" +
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
