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

package servertest

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

func TestMain(m *testing.M) {
	realtikvtest.RunTestMain(m)
}

func TestMultiStatementPrefetchInterruptedOnDisconnect(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	serverAddr := startTiDBServer(t, store, dom)

	adminDB := openDB(t, serverAddr, "tcp", false)
	adminDB.SetMaxOpenConns(5)
	mustExec(t, adminDB, "drop table if exists issue68682_multi_prefetch")
	mustExec(t, adminDB, "create table issue68682_multi_prefetch (id int primary key, v int)")
	mustExec(t, adminDB, "insert into issue68682_multi_prefetch values (1, 0), (2, 0), (3, 0)")

	// Capture the victim's TCP connection so the test can simulate the client
	// disappearing while its request is blocked inside TiKV.
	rawConnCh := make(chan net.Conn, 1)
	const victimNetwork = "issue68682-victim"
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
	victimDB := openDB(t, serverAddr, victimNetwork, true)
	victim, err := victimDB.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = victim.Close() })

	mustExec(t, victim, "set tidb_txn_mode = 'pessimistic'")
	mustExec(t, victim, "begin pessimistic")
	mustExec(t, victim, "update issue68682_multi_prefetch set v = 1 where id = 1")
	var victimID uint64
	require.NoError(t, victim.QueryRowContext(context.Background(), "select connection_id()").Scan(&victimID))

	blocker, err := adminDB.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = blocker.ExecContext(context.Background(), "rollback")
		_ = blocker.Close()
	})
	mustExec(t, blocker, "begin pessimistic")
	mustExec(t, blocker, "update issue68682_multi_prefetch set v = 2 where id = 2")

	const multiQuery = "update issue68682_multi_prefetch set v = 2 where id = 2; update issue68682_multi_prefetch set v = 3 where id = 3"
	execDone := make(chan error, 1)
	go func() {
		_, execErr := victim.ExecContext(context.Background(), multiQuery)
		execDone <- execErr
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
	}, 10*time.Second, 100*time.Millisecond, "the multi-statement prefetch did not enter a TiKV lock wait: %v", lockWaitErr)

	var rawConn net.Conn
	select {
	case rawConn = <-rawConnCh:
	case <-time.After(time.Second):
		require.FailNow(t, "the victim TCP connection was not captured")
	}
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
	_, err = adminDB.ExecContext(ctx, "update issue68682_multi_prefetch set v = v + 1 where id = 1")
	require.NoError(t, err)

	var row1, row2, row3 int
	require.NoError(t, adminDB.QueryRowContext(context.Background(), `
		select sum(if(id = 1, v, 0)), sum(if(id = 2, v, 0)), sum(if(id = 3, v, 0))
		from issue68682_multi_prefetch`).Scan(&row1, &row2, &row3))
	require.Equal(t, 1, row1)
	require.Equal(t, 0, row2)
	require.Equal(t, 0, row3)
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
