// Copyright 2026 PingCAP, Inc.
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

package pessimistictest

import (
	"context"
	"database/sql"
	"net"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	tidbserver "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestStatementsInterruptedOnDisconnect(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	serverAddr := startTiDBServer(t, store, dom)

	adminDB := openDB(t, serverAddr, "tcp", false)
	adminDB.SetMaxOpenConns(5)

	testCases := []disconnectInterruptTestCase{
		{
			name: "autocommit_update_query",
			buildSQL: func(tableName string) string {
				return "update " + tableName + " set v = 2 where id = 2"
			},
		},
		{
			name:     "autocommit_update_prepared",
			prepared: true,
			buildSQL: func(tableName string) string {
				return "update " + tableName + " set v = 2 where id = 2"
			},
		},
		{
			name:     "explicit_txn_update_query",
			txnSetup: "begin pessimistic",
			buildSQL: func(tableName string) string {
				return "update " + tableName + " set v = 2 where id = 2"
			},
		},
		{
			name:     "explicit_txn_update_prepared",
			txnSetup: "begin pessimistic",
			prepared: true,
			buildSQL: func(tableName string) string {
				return "update " + tableName + " set v = 2 where id = 2"
			},
		},
		{
			name:        "explicit_txn_select_for_update_query",
			txnSetup:    "begin pessimistic",
			returnsRows: true,
			buildSQL: func(tableName string) string {
				return "select * from " + tableName + " where id = 2 for update"
			},
		},
		{
			name:        "explicit_txn_select_for_update_prepared",
			txnSetup:    "begin pessimistic",
			prepared:    true,
			returnsRows: true,
			buildSQL: func(tableName string) string {
				return "select * from " + tableName + " where id = 2 for update"
			},
		},
		{
			name:     "autocommit_off_update_query",
			txnSetup: "set autocommit = 0",
			buildSQL: func(tableName string) string {
				return "update " + tableName + " set v = 2 where id = 2"
			},
		},
		{
			name:            "multi_statement_prefetch",
			txnSetup:        "begin pessimistic",
			multiStatements: true,
			buildSQL: func(tableName string) string {
				return "update " + tableName + " set v = 2 where id = 2; update " + tableName + " set v = 3 where id = 3"
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			runDisconnectInterruptTest(t, adminDB, serverAddr, testCase)
		})
	}
}

type disconnectInterruptTestCase struct {
	name            string
	txnSetup        string
	prepared        bool
	returnsRows     bool
	multiStatements bool
	buildSQL        func(tableName string) string
}

func runDisconnectInterruptTest(t *testing.T, adminDB *sql.DB, serverAddr string, testCase disconnectInterruptTestCase) {
	t.Helper()
	tableName := "issue68682_" + testCase.name
	mustExec(t, adminDB, "drop table if exists "+tableName)
	mustExec(t, adminDB, "create table "+tableName+" (id int primary key, v int)")
	mustExec(t, adminDB, "insert into "+tableName+" values (1, 0), (2, 0), (3, 0)")

	// Capture the victim's TCP connection so the test can simulate the client
	// disappearing while its request is blocked inside TiKV.
	rawConnCh := make(chan net.Conn, 1)
	victimNetwork := "issue68682-" + testCase.name
	mysql.RegisterDialContext(victimNetwork, func(ctx context.Context, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		if err == nil {
			select {
			case rawConnCh <- conn:
			default:
			}
		}
		return conn, err
	})
	victimDB := openDB(t, serverAddr, victimNetwork, testCase.multiStatements)
	victimDB.SetMaxOpenConns(1)
	victim, err := victimDB.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = victim.Close() })

	var rawConn net.Conn
	select {
	case rawConn = <-rawConnCh:
	case <-time.After(time.Second):
		require.FailNow(t, "the victim TCP connection was not captured")
	}
	var blocker *sql.Conn
	t.Cleanup(func() {
		_ = rawConn.Close()
		if blocker != nil {
			_, _ = blocker.ExecContext(context.Background(), "rollback")
			_ = blocker.Close()
		}
	})

	mustExec(t, victim, "set tidb_txn_mode = 'pessimistic'")
	if testCase.txnSetup != "" {
		mustExec(t, victim, testCase.txnSetup)
		mustExec(t, victim, "update "+tableName+" set v = 1 where id = 1")
	}
	var victimID uint64
	require.NoError(t, victim.QueryRowContext(context.Background(), "select connection_id()").Scan(&victimID))

	blocker, err = adminDB.Conn(context.Background())
	require.NoError(t, err)
	mustExec(t, blocker, "begin pessimistic")
	mustExec(t, blocker, "update "+tableName+" set v = 2 where id = 2")

	blockedSQL := testCase.buildSQL(tableName)
	var stmt *sql.Stmt
	if testCase.prepared {
		stmt, err = victim.PrepareContext(context.Background(), blockedSQL)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = rawConn.Close()
			_ = stmt.Close()
		})
	}

	execDone := make(chan error, 1)
	go func() {
		execDone <- executeBlockingSQL(victim, stmt, blockedSQL, testCase.returnsRows)
	}()

	var lockWaitErr error
	require.Eventually(t, func() bool {
		var count int
		lockWaitErr = adminDB.QueryRowContext(context.Background(), `
			select count(*)
			from information_schema.data_lock_waits l
			join information_schema.tidb_trx trx on l.trx_id = trx.id
			where trx.session_id = ?`, victimID).Scan(&count)
		return lockWaitErr == nil && count > 0
	}, 10*time.Second, 100*time.Millisecond, "the statement did not enter a TiKV lock wait: %v", lockWaitErr)

	require.NoError(t, rawConn.Close())

	var execErr error
	require.Eventually(t, func() bool {
		select {
		case execErr = <-execDone:
			return true
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond)
	require.Error(t, execErr)

	require.Eventually(t, func() bool {
		var count int
		err := adminDB.QueryRowContext(
			context.Background(),
			"select count(*) from information_schema.processlist where id = ?",
			victimID,
		).Scan(&count)
		return err == nil && count == 0
	}, 5*time.Second, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = adminDB.ExecContext(ctx, "update "+tableName+" set v = v + 1 where id = 1")
	require.NoError(t, err)

	var row1, row2, row3 int
	require.NoError(t, adminDB.QueryRowContext(context.Background(),
		"select sum(if(id = 1, v, 0)), sum(if(id = 2, v, 0)), sum(if(id = 3, v, 0)) from "+tableName,
	).Scan(&row1, &row2, &row3))
	require.Equal(t, 1, row1)
	require.Equal(t, 0, row2)
	require.Equal(t, 0, row3)
}

func executeBlockingSQL(conn *sql.Conn, stmt *sql.Stmt, query string, returnsRows bool) error {
	if !returnsRows {
		if stmt != nil {
			_, err := stmt.ExecContext(context.Background())
			return err
		}
		_, err := conn.ExecContext(context.Background(), query)
		return err
	}

	var rows *sql.Rows
	var err error
	if stmt != nil {
		rows, err = stmt.QueryContext(context.Background())
	} else {
		rows, err = conn.QueryContext(context.Background(), query)
	}
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}

func startTiDBServer(t *testing.T, store kv.Storage, dom *domain.Domain) string {
	t.Helper()
	originalRunInGoTest := tidbserver.RunInGoTest
	originalRunInGoTestChan := tidbserver.RunInGoTestChan
	tidbserver.RunInGoTest = true
	tidbserver.RunInGoTestChan = make(chan struct{})
	t.Cleanup(func() {
		tidbserver.RunInGoTest = originalRunInGoTest
		tidbserver.RunInGoTestChan = originalRunInGoTestChan
	})

	cfg := config.NewConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.Socket = ""
	cfg.Status.ReportStatus = false
	cfg.Security.AutoTLS = false
	cfg.Store = config.StoreTypeTiKV

	server, err := tidbserver.NewServer(cfg, tidbserver.NewTiDBDriver(store))
	require.NoError(t, err)
	server.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(server)

	runDone := make(chan error, 1)
	go func() {
		runDone <- server.Run(nil)
	}()
	select {
	case <-tidbserver.RunInGoTestChan:
	case err = <-runDone:
		require.NoError(t, err)
		require.FailNow(t, "TiDB server exited before becoming ready")
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timed out waiting for TiDB server startup")
	}

	t.Cleanup(func() {
		server.Close()
		select {
		case err := <-runDone:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			require.Fail(t, "timed out waiting for TiDB server shutdown")
		}
	})
	return server.ListenAddr().String()
}

func openDB(t *testing.T, addr, network string, multiStatements bool) *sql.DB {
	t.Helper()
	cfg := mysql.NewConfig()
	cfg.User = "root"
	cfg.Net = network
	cfg.Addr = addr
	cfg.DBName = "test"
	cfg.MultiStatements = multiStatements
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, db.PingContext(context.Background()))
	return db
}

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func mustExec(t *testing.T, executor sqlExecutor, query string) {
	t.Helper()
	_, err := executor.ExecContext(context.Background(), query)
	require.NoError(t, err)
}
