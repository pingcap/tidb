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
	"database/sql/driver"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/utils"
)

func TestInit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	cfg := config.NewConfig()
	cfg.TikvImporter.DuplicateResolution = config.DupeResAlgRecord
	cfg.App.MaxError.Type.Store(10)
	cfg.App.TaskInfoSchemaName = "lightning_errors"

	em := New(db, cfg)
	require.Equal(t, cfg.TikvImporter.DuplicateResolution, em.dupResolution)
	require.Equal(t, cfg.App.MaxError.Type.Load(), em.remainingError.Type.Load())
	require.Equal(t, cfg.App.MaxError.Conflict.Load(), em.remainingError.Conflict.Load())

	em.remainingError.Type.Store(0)
	em.dupResolution = config.DupeResAlgNone
	ctx := context.Background()
	err = em.Init(ctx)
	require.NoError(t, err)

	em.dupResolution = config.DupeResAlgRecord
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_errors`;").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.conflict_error_v1.*").
		WillReturnResult(sqlmock.NewResult(2, 1))
	err = em.Init(ctx)
	require.NoError(t, err)

	em.dupResolution = config.DupeResAlgNone
	em.remainingError.Type.Store(1)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_errors`;").
		WillReturnResult(sqlmock.NewResult(3, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.type_error_v1.*").
		WillReturnResult(sqlmock.NewResult(4, 1))
	err = em.Init(ctx)
	require.NoError(t, err)
	em.dupResolution = config.DupeResAlgRecord
	em.remainingError.Type.Store(1)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_errors`.*").
		WillReturnResult(sqlmock.NewResult(5, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.type_error_v1.*").
		WillReturnResult(sqlmock.NewResult(6, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.conflict_error_v1.*").
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

func TestResolveAllConflictKeys(t *testing.T) {
	const totalRows = int64(1 << 18)
	driverName := "errmgr-mock-" + strconv.Itoa(rand.Int())
	sql.Register(driverName, mockDriver{totalRows: totalRows})
	db, err := sql.Open(driverName, "")
	require.NoError(t, err)
	defer db.Close()

	cfg := config.NewConfig()
	cfg.TikvImporter.DuplicateResolution = config.DupeResAlgRemove
	cfg.App.TaskInfoSchemaName = "lightning_errors"
	em := New(db, cfg)
	ctx := context.Background()
	err = em.Init(ctx)
	require.NoError(t, err)

	resolved := atomic.NewInt64(0)
	pool := utils.NewWorkerPool(16, "resolve duplicate rows")
	err = em.ResolveAllConflictKeys(
		ctx, "test", pool,
		func(ctx context.Context, handleRows [][2][]byte) error {
			resolved.Add(int64(len(handleRows)))
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, totalRows, resolved.Load())
}

func TestErrorMgrHasError(t *testing.T) {
	cfg := &config.Config{}
	cfg.App.MaxError = config.MaxError{
		Syntax:   *atomic.NewInt64(100),
		Charset:  *atomic.NewInt64(100),
		Type:     *atomic.NewInt64(100),
		Conflict: *atomic.NewInt64(100),
	}
	em := &ErrorManager{
		configError:    &cfg.App.MaxError,
		remainingError: cfg.App.MaxError,
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
	em.remainingError.Conflict.Sub(1)
	require.True(t, em.HasError())

	// change multiple keys
	em.remainingError = cfg.App.MaxError
	em.remainingError.Syntax.Store(0)
	em.remainingError.Charset.Store(0)
	em.remainingError.Type.Store(0)
	em.remainingError.Conflict.Store(0)
	require.True(t, em.HasError())
}

func TestErrorMgrErrorOutput(t *testing.T) {
	cfg := &config.Config{}
	cfg.App.MaxError = config.MaxError{
		Syntax:   *atomic.NewInt64(100),
		Charset:  *atomic.NewInt64(100),
		Type:     *atomic.NewInt64(100),
		Conflict: *atomic.NewInt64(100),
	}
	em := &ErrorManager{
		configError:    &cfg.App.MaxError,
		remainingError: cfg.App.MaxError,
		schemaEscaped:  "`error_info`",
	}

	output := em.Output()
	require.Equal(t, output, "")

	em.remainingError.Syntax.Sub(1)
	output = em.Output()
	checkStr := strings.ReplaceAll(output, "\n", "")
	expected := "Import Data Error Summary: +---+-------------+-------------+--------------------------------+| # | ERROR TYPE  | ERROR COUNT | ERROR DATA TABLE               |+---+-------------+-------------+--------------------------------+|\x1b[31m 1 \x1b[0m|\x1b[31m Data Syntax \x1b[0m|\x1b[31m           1 \x1b[0m|\x1b[31m `error_info`.`syntax_error_v1` \x1b[0m|+---+-------------+-------------+--------------------------------+"
	require.Equal(t, expected, checkStr)

	em.remainingError = cfg.App.MaxError
	em.remainingError.Syntax.Sub(10)
	em.remainingError.Type.Store(10)
	output = em.Output()
	checkStr = strings.ReplaceAll(output, "\n", "")
	expected = "Import Data Error Summary: +---+-------------+-------------+--------------------------------+| # | ERROR TYPE  | ERROR COUNT | ERROR DATA TABLE               |+---+-------------+-------------+--------------------------------+|\x1b[31m 1 \x1b[0m|\x1b[31m Data Type   \x1b[0m|\x1b[31m          90 \x1b[0m|\x1b[31m `error_info`.`type_error_v1`   \x1b[0m||\x1b[31m 2 \x1b[0m|\x1b[31m Data Syntax \x1b[0m|\x1b[31m          10 \x1b[0m|\x1b[31m `error_info`.`syntax_error_v1` \x1b[0m|+---+-------------+-------------+--------------------------------+"
	require.Equal(t, expected, checkStr)

	// change multiple keys
	em.remainingError = cfg.App.MaxError
	em.remainingError.Syntax.Store(0)
	em.remainingError.Charset.Store(0)
	em.remainingError.Type.Store(0)
	em.remainingError.Conflict.Store(0)
	output = em.Output()
	checkStr = strings.ReplaceAll(output, "\n", "")
	expected = "Import Data Error Summary: +---+---------------------+-------------+----------------------------------+| # | ERROR TYPE          | ERROR COUNT | ERROR DATA TABLE                 |+---+---------------------+-------------+----------------------------------+|\x1b[31m 1 \x1b[0m|\x1b[31m Data Type           \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m `error_info`.`type_error_v1`     \x1b[0m||\x1b[31m 2 \x1b[0m|\x1b[31m Data Syntax         \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m `error_info`.`syntax_error_v1`   \x1b[0m||\x1b[31m 3 \x1b[0m|\x1b[31m Charset Error       \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m                                  \x1b[0m||\x1b[31m 4 \x1b[0m|\x1b[31m Unique Key Conflict \x1b[0m|\x1b[31m         100 \x1b[0m|\x1b[31m `error_info`.`conflict_error_v1` \x1b[0m|+---+---------------------+-------------+----------------------------------+"
	require.Equal(t, expected, checkStr)
}
