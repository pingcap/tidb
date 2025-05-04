// Copyright 2024 PingCAP, Inc.
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

package cursor

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	mysqlcursor "github.com/YangKeao/go-mysql-driver"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	server2 "github.com/pingcap/tidb/pkg/server"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/server/tests/servertestkit"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCursorFetchErrorInFetch(t *testing.T) {
	tmpStoragePath := t.TempDir()
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = tmpStoragePath
	})

	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := server2.CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := server2.CreateMockConn(t, srv)

	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, payload BLOB)")
	payload := make([]byte, 512)
	for i := range 2048 {
		rand.Read(payload)
		tk.MustExec("insert into t values (?, ?)", i, payload)
	}

	tk.MustExec("set global tidb_enable_tmp_storage_on_oom = ON")
	tk.MustExec("set global tidb_mem_oom_action = 'CANCEL'")
	defer tk.MustExec("set global tidb_mem_oom_action= DEFAULT")
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 1)

	// execute a normal statement, it'll spill to disk
	stmt, _, _, err := c.Context().Prepare("select * from t")
	require.NoError(t, err)

	tk.MustExec(fmt.Sprintf("set tidb_mem_quota_query=%d", 1))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/chunk/get-chunk-error", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/chunk/get-chunk-error"))
	}()

	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{tmysql.ComStmtExecute}, uint32(stmt.ID())),
		tmysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))

	require.ErrorContains(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{tmysql.ComStmtFetch}, uint32(stmt.ID())), 1024)), "fail to get chunk for test")
	// after getting a failed FETCH, the cursor should have been reseted
	require.False(t, stmt.GetCursorActive())
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 0)
	require.Len(t, tk.Session().GetSessionVars().DiskTracker.GetChildrenForTest(), 0)
}

func TestCursorFetchShouldSpill(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})

	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := server2.CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/testCursorFetchSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/testCursorFetchSpill"))
	}()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := server2.CreateMockConn(t, srv)

	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id_1 int, id_2 int)")
	tk.MustExec("insert into t values (1, 1), (1, 2)")
	tk.MustExec("set global tidb_enable_tmp_storage_on_oom = ON")
	tk.MustExec("set global tidb_mem_oom_action = 'CANCEL'")
	defer tk.MustExec("set global tidb_mem_oom_action= DEFAULT")
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 1)

	// execute a normal statement, it'll spill to disk
	stmt, _, _, err := c.Context().Prepare("select * from t")
	require.NoError(t, err)

	tk.MustExec(fmt.Sprintf("set tidb_mem_quota_query=%d", 1))

	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{tmysql.ComStmtExecute}, uint32(stmt.ID())),
		tmysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
}

func TestCursorFetchExecuteCheck(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := server2.CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := server2.CreateMockConn(t, srv)

	stmt, _, _, err := c.Context().Prepare("select 1")
	require.NoError(t, err)

	// execute with wrong ID
	require.Error(t, c.Dispatch(ctx, append(
		appendUint32([]byte{tmysql.ComStmtExecute}, uint32(stmt.ID()+1)),
		tmysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))

	// execute with wrong flag
	require.Error(t, c.Dispatch(ctx, append(
		appendUint32([]byte{tmysql.ComStmtExecute}, uint32(stmt.ID())),
		tmysql.CursorTypeReadOnly|tmysql.CursorTypeForUpdate, 0x1, 0x0, 0x0, 0x0,
	)))

	require.Error(t, c.Dispatch(ctx, append(
		appendUint32([]byte{tmysql.ComStmtExecute}, uint32(stmt.ID())),
		tmysql.CursorTypeReadOnly|tmysql.CursorTypeScrollable, 0x1, 0x0, 0x0, 0x0,
	)))
}

func TestConcurrentExecuteAndFetch(t *testing.T) {
	runTestConcurrentExecuteAndFetch(t, false)
	runTestConcurrentExecuteAndFetch(t, true)
}

func runTestConcurrentExecuteAndFetch(t *testing.T, lazy bool) {
	ts := servertestkit.CreateTidbTestSuite(t)

	mysqldriver := &mysqlcursor.MySQLDriver{}
	rawConn, err := mysqldriver.Open(ts.GetDSNWithCursor(10))
	require.NoError(t, err)
	conn := rawConn.(mysqlcursor.Connection)
	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), "drop table if exists t1", nil)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "create table t1(id int primary key, v int)", nil)
	require.NoError(t, err)
	rowCount := 1000
	for i := range rowCount {
		_, err = conn.ExecContext(context.Background(), fmt.Sprintf("insert into t1 values(%d, %d)", i, i), nil)
		require.NoError(t, err)
	}

	if lazy {
		_, err = conn.ExecContext(context.Background(), "set tidb_enable_lazy_cursor_fetch = 'ON'", nil)
		require.NoError(t, err)

		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/avoidEagerCursorFetch", "return"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/avoidEagerCursorFetch"))
		}()
	}

	// Normal execute. Simple table reader.
	execTimes := 0
outerLoop:
	for execTimes < 50 {
		execTimes++
		rawStmt, err := conn.Prepare("select * from t1 order by id")
		require.NoError(t, err)
		stmt := rawStmt.(mysqlcursor.Statement)

		// This query will return `rowCount` rows and use cursor fetch.
		rows, err := stmt.QueryContext(context.Background(), nil)
		require.NoError(t, err)

		dest := make([]driver.Value, 2)
		fetchRowCount := int64(0)

		for {
			// Now, we'll have two cases: 0. fetch 1. execute another statement
			switch rand.Int() % 2 {
			case 0:
				// it'll send `FETCH` commands for every 10 rows.
				err := rows.Next(dest)
				if err != nil {
					switch err {
					case io.EOF:
						require.Equal(t, int64(rowCount), fetchRowCount)
						rows.Close()
						break outerLoop
					default:
						require.NoError(t, err)
					}
				}
				require.Equal(t, fetchRowCount, dest[0])
				require.Equal(t, fetchRowCount, dest[1])
				fetchRowCount++
			case 1:
				rows, err := conn.QueryContext(context.Background(), "select * from t1 order by id limit 1", nil)
				require.NoError(t, err)
				err = rows.Next(dest)
				require.NoError(t, err)
				require.NoError(t, rows.Close())
			}
		}
	}
}

func TestSerialLazyExecuteAndFetch(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	mysqldriver := &mysqlcursor.MySQLDriver{}
	rawConn, err := mysqldriver.Open(ts.GetDSNWithCursor(10))
	require.NoError(t, err)
	conn := rawConn.(mysqlcursor.Connection)
	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), "drop table if exists t1", nil)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "create table t1(id int primary key, v int)", nil)
	require.NoError(t, err)
	rowCount := 1000
	for i := range rowCount {
		_, err = conn.ExecContext(context.Background(), fmt.Sprintf("insert into t1 values(%d, %d)", i, i), nil)
		require.NoError(t, err)
	}

	_, err = conn.ExecContext(context.Background(), "set tidb_enable_lazy_cursor_fetch = 'ON'", nil)
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/avoidEagerCursorFetch", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/avoidEagerCursorFetch"))
	}()

	// Normal execute. Simple table reader.
	execTimes := 0
outerLoop:
	for execTimes < 50 {
		execTimes++
		rawStmt, err := conn.Prepare("select * from t1 order by id")
		require.NoError(t, err)
		stmt := rawStmt.(mysqlcursor.Statement)

		// This query will return `rowCount` rows and use cursor fetch.
		rows, err := stmt.QueryContext(context.Background(), nil)
		require.NoError(t, err)

		dest := make([]driver.Value, 2)
		fetchRowCount := int64(0)

		for {
			// it'll send `FETCH` commands for every 10 rows.
			err := rows.Next(dest)
			if err != nil {
				switch err {
				case io.EOF:
					require.Equal(t, int64(rowCount), fetchRowCount)
					rows.Close()
					break outerLoop
				default:
					require.NoError(t, err)
				}
			}
			require.Equal(t, fetchRowCount, dest[0])
			require.Equal(t, fetchRowCount, dest[1])
			fetchRowCount++
		}
	}
}

func TestLazyExecuteProjection(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	mysqldriver := &mysqlcursor.MySQLDriver{}
	rawConn, err := mysqldriver.Open(ts.GetDSNWithCursor(10))
	require.NoError(t, err)
	conn := rawConn.(mysqlcursor.Connection)
	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), "drop table if exists t1", nil)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "create table t1(id int primary key, v int)", nil)
	require.NoError(t, err)
	rowCount := 1000
	for i := range rowCount {
		_, err = conn.ExecContext(context.Background(), fmt.Sprintf("insert into t1 values(%d, %d)", i, i), nil)
		require.NoError(t, err)
	}

	_, err = conn.ExecContext(context.Background(), "set tidb_enable_lazy_cursor_fetch = 'ON'", nil)
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/avoidEagerCursorFetch", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/avoidEagerCursorFetch"))
	}()

	// Normal execute. Simple table reader.
	execTimes := 0
outerLoop:
	for execTimes < 50 {
		execTimes++
		rawStmt, err := conn.Prepare("select id + v from t1 order by id")
		require.NoError(t, err)
		stmt := rawStmt.(mysqlcursor.Statement)

		// This query will return `rowCount` rows and use cursor fetch.
		rows, err := stmt.QueryContext(context.Background(), nil)
		require.NoError(t, err)

		dest := make([]driver.Value, 1)
		fetchRowCount := int64(0)

		for {
			// it'll send `FETCH` commands for every 10 rows.
			err := rows.Next(dest)
			if err != nil {
				switch err {
				case io.EOF:
					require.Equal(t, int64(rowCount), fetchRowCount)
					rows.Close()
					break outerLoop
				default:
					require.NoError(t, err)
				}
			}
			require.Equal(t, fetchRowCount*2, dest[0])
			fetchRowCount++
		}
	}
}

func TestLazyExecuteSelection(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	mysqldriver := &mysqlcursor.MySQLDriver{}
	rawConn, err := mysqldriver.Open(ts.GetDSNWithCursor(10))
	require.NoError(t, err)
	conn := rawConn.(mysqlcursor.Connection)
	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), "drop table if exists t1", nil)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "create table t1(id int primary key, v int)", nil)
	require.NoError(t, err)
	rowCount := 1000
	for i := range rowCount {
		_, err = conn.ExecContext(context.Background(), fmt.Sprintf("insert into t1 values(%d, %d)", i, i), nil)
		require.NoError(t, err)
	}

	_, err = conn.ExecContext(context.Background(), "set tidb_enable_lazy_cursor_fetch = 'ON'", nil)
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/avoidEagerCursorFetch", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/avoidEagerCursorFetch"))
	}()

	// Normal execute. Simple table reader.
	execTimes := 0
outerLoop:
	for execTimes < 50 {
		execTimes++
		rawStmt, err := conn.Prepare("select id from t1 where v >= ? order by id")
		require.NoError(t, err)
		stmt := rawStmt.(mysqlcursor.Statement)

		// This query will return `rowCount - 500` rows and use cursor fetch.
		rows, err := stmt.QueryContext(context.Background(), []driver.NamedValue{{Value: int64(500)}})
		require.NoError(t, err)

		dest := make([]driver.Value, 1)
		fetchRowCount := int64(0)

		for {
			// it'll send `FETCH` commands for every 10 rows.
			err := rows.Next(dest)
			if err != nil {
				switch err {
				case io.EOF:
					require.Equal(t, int64(rowCount-500), fetchRowCount)
					rows.Close()
					break outerLoop
				default:
					require.NoError(t, err)
				}
			}
			require.Equal(t, fetchRowCount+500, dest[0])
			fetchRowCount++
		}
	}
}

func TestLazyExecuteWithParam(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	mysqldriver := &mysqlcursor.MySQLDriver{}
	rawConn, err := mysqldriver.Open(ts.GetDSNWithCursor(10))
	require.NoError(t, err)
	conn := rawConn.(mysqlcursor.Connection)
	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), "drop table if exists t1", nil)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "create table t1(id int primary key, v int)", nil)
	require.NoError(t, err)
	rowCount := 1000
	for i := range rowCount {
		_, err = conn.ExecContext(context.Background(), fmt.Sprintf("insert into t1 values(%d, %d)", i, i), nil)
		require.NoError(t, err)
	}

	_, err = conn.ExecContext(context.Background(), "set tidb_enable_lazy_cursor_fetch = 'ON'", nil)
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/avoidEagerCursorFetch", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/avoidEagerCursorFetch"))
	}()

	// Normal execute. Simple table reader.
	execTimes := 0
outerLoop:
	for execTimes < 50 {
		execTimes++
		rawStmt, err := conn.Prepare("select id from t1 where v >= ? and v <= ? order by id")
		require.NoError(t, err)
		stmt := rawStmt.(mysqlcursor.Statement)

		// This query will return `rowCount - 500` rows and use cursor fetch.
		rows, err := stmt.QueryContext(context.Background(), []driver.NamedValue{{Value: int64(500)}, {Value: int64(10000)}})
		require.NoError(t, err)

		dest := make([]driver.Value, 1)
		fetchRowCount := int64(0)

		for {
			// it'll send `FETCH` commands for every 10 rows.
			err := rows.Next(dest)
			if err != nil {
				switch err {
				case io.EOF:
					require.Equal(t, int64(rowCount-500), fetchRowCount)
					rows.Close()
					break outerLoop
				default:
					require.NoError(t, err)
				}
			}
			require.Equal(t, fetchRowCount+500, dest[0])
			fetchRowCount++

			if fetchRowCount%50 == 0 {
				// Run another query with only one parameter.
				rawStmt, err := conn.Prepare("select id from t1 where v = ?")
				require.NoError(t, err)
				stmt := rawStmt.(mysqlcursor.Statement)

				// This query will return `rowCount` rows and use cursor fetch.
				rows, err := stmt.QueryContext(context.Background(), []driver.NamedValue{{Value: int64(500)}})
				require.NoError(t, err)

				dest := make([]driver.Value, 2)
				require.NoError(t, rows.Next(dest))
				require.Equal(t, int64(500), dest[0])
				require.NoError(t, rawStmt.Close())
			}
		}
	}
}

func TestCursorExceedQuota(t *testing.T) {
	cfg := util2.NewTestConfig()
	cfg.TempStoragePath = t.TempDir()

	cfg.Port = 0
	cfg.Status.StatusPort = 0
	cfg.TempStorageQuota = 1000
	executor.GlobalDiskUsageTracker.SetBytesLimit(cfg.TempStorageQuota)
	ts := servertestkit.CreateTidbTestSuiteWithCfg(t, cfg)

	mysqldriver := &mysqlcursor.MySQLDriver{}
	rawConn, err := mysqldriver.Open(ts.GetDSNWithCursor(10))
	require.NoError(t, err)
	conn := rawConn.(mysqlcursor.Connection)

	_, err = conn.ExecContext(context.Background(), "drop table if exists t1", nil)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE `t1` (`c1` varchar(100));", nil)
	require.NoError(t, err)
	rowCount := 1000
	for range rowCount {
		_, err = conn.ExecContext(context.Background(), "insert into t1 (c1) values ('201801');", nil)
		require.NoError(t, err)
	}

	_, err = conn.ExecContext(context.Background(), "set tidb_mem_quota_query = 1;", nil)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "set global tidb_enable_tmp_storage_on_oom = 'ON'", nil)
	require.NoError(t, err)

	rawStmt, err := conn.Prepare("SELECT * FROM test.t1")
	require.NoError(t, err)
	stmt := rawStmt.(mysqlcursor.Statement)

	_, err = stmt.QueryContext(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Out Of Quota For Local Temporary Space!")

	require.NoError(t, conn.Close())

	time.Sleep(time.Second)

	tempStoragePath := cfg.TempStoragePath
	files, err := os.ReadDir(tempStoragePath)
	require.NoError(t, err)
	require.Empty(t, files)
}
