// Copyright 2015 PingCAP, Inc.
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

package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/cpuprofile"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/resourcegrouptag"
	"github.com/pingcap/tidb/util/topsql"
	"github.com/pingcap/tidb/util/topsql/collector"
	mockTopSQLTraceCPU "github.com/pingcap/tidb/util/topsql/collector/mock"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.opencensus.io/stats/view"
)

type tidbTestSuite struct {
	*testServerClient
	tidbdrv *TiDBDriver
	server  *Server
	domain  *domain.Domain
	store   kv.Storage
}

func createTidbTestSuite(t *testing.T) *tidbTestSuite {
	cfg := newTestConfig()
	cfg.Port = 0
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = 0
	cfg.Status.RecordDBLabel = false
	cfg.Performance.TCPKeepAlive = true
	return createTidbTestSuiteWithCfg(t, cfg)
}

func createTidbTestSuiteWithCfg(t *testing.T, cfg *config.Config) *tidbTestSuite {
	ts := &tidbTestSuite{testServerClient: newTestServerClient()}

	// setup tidbTestSuite
	var err error
	ts.store, err = mockstore.NewMockStore()
	session.DisableStats4Test()
	require.NoError(t, err)
	ts.domain, err = session.BootstrapSession(ts.store)
	require.NoError(t, err)
	ts.tidbdrv = NewTiDBDriver(ts.store)

	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	ts.port = getPortFromTCPAddr(server.listener.Addr())
	ts.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	ts.server = server
	ts.server.SetDomain(ts.domain)
	ts.server.InitGlobalConnID(ts.domain.ServerID)
	ts.domain.InfoSyncer().SetSessionManager(ts.server)
	go func() {
		err := ts.server.Run()
		require.NoError(t, err)
	}()
	ts.waitUntilServerOnline()

	t.Cleanup(func() {
		if ts.domain != nil {
			ts.domain.Close()
		}
		if ts.server != nil {
			ts.server.Close()
		}
		if ts.store != nil {
			require.NoError(t, ts.store.Close())
		}
		view.Stop()
	})
	return ts
}

type tidbTestTopSQLSuite struct {
	*tidbTestSuite
}

func createTidbTestTopSQLSuite(t *testing.T) *tidbTestTopSQLSuite {
	base := createTidbTestSuite(t)

	ts := &tidbTestTopSQLSuite{base}

	// Initialize global variable for top-sql test.
	db, err := sql.Open("mysql", ts.getDSN())
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	dbt := testkit.NewDBTestKit(t, db)
	topsqlstate.GlobalState.PrecisionSeconds.Store(1)
	topsqlstate.GlobalState.ReportIntervalSeconds.Store(2)
	dbt.MustExec("set @@global.tidb_top_sql_max_time_series_count=5;")

	require.NoError(t, cpuprofile.StartCPUProfiler())
	t.Cleanup(func() {
		cpuprofile.StopCPUProfiler()
		topsqlstate.GlobalState.PrecisionSeconds.Store(topsqlstate.DefTiDBTopSQLPrecisionSeconds)
		topsqlstate.GlobalState.ReportIntervalSeconds.Store(topsqlstate.DefTiDBTopSQLReportIntervalSeconds)
		view.Stop()
	})
	return ts
}

func TestRegression(t *testing.T) {
	ts := createTidbTestSuite(t)
	if regression {
		ts.runTestRegression(t, nil, "Regression")
	}
}

func TestUint64(t *testing.T) {
	ts := createTidbTestSuite(t)
	ts.runTestPrepareResultFieldType(t)
}

func TestSpecialType(t *testing.T) {
	ts := createTidbTestSuite(t)
	ts.runTestSpecialType(t)
}

func TestPreparedString(t *testing.T) {
	ts := createTidbTestSuite(t)

	ts.runTestPreparedString(t)
}

func TestPreparedTimestamp(t *testing.T) {
	ts := createTidbTestSuite(t)

	ts.runTestPreparedTimestamp(t)
}

func TestConcurrentUpdate(t *testing.T) {
	ts := createTidbTestSuite(t)

	ts.runTestConcurrentUpdate(t)
}

func TestErrorCode(t *testing.T) {
	ts := createTidbTestSuite(t)

	ts.runTestErrorCode(t)
}

func TestAuth(t *testing.T) {
	ts := createTidbTestSuite(t)

	ts.runTestAuth(t)
	ts.runTestIssue3682(t)
	ts.runTestAccountLock(t)
}

func TestIssues(t *testing.T) {
	ts := createTidbTestSuite(t)

	ts.runTestIssue3662(t)
	ts.runTestIssue3680(t)
	ts.runTestIssue22646(t)
}

func TestDBNameEscape(t *testing.T) {
	ts := createTidbTestSuite(t)
	ts.runTestDBNameEscape(t)
}

func TestResultFieldTableIsNull(t *testing.T) {
	ts := createTidbTestSuite(t)

	ts.runTestResultFieldTableIsNull(t)
}

func TestStatusAPI(t *testing.T) {
	ts := createTidbTestSuite(t)

	ts.runTestStatusAPI(t)
}

func TestStatusPort(t *testing.T) {
	ts := createTidbTestSuite(t)

	cfg := newTestConfig()
	cfg.Port = 0
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = ts.statusPort
	cfg.Performance.TCPKeepAlive = true

	server, err := NewServer(cfg, ts.tidbdrv)
	require.Error(t, err)
	require.Nil(t, server)
}

func TestStatusAPIWithTLS(t *testing.T) {
	ts := createTidbTestSuite(t)

	dir := t.TempDir()

	fileName := func(file string) string {
		return filepath.Join(dir, file)
	}

	caCert, caKey, err := generateCert(0, "TiDB CA 2", nil, nil, fileName("ca-key-2.pem"), fileName("ca-cert-2.pem"))
	require.NoError(t, err)
	_, _, err = generateCert(1, "tidb-server-2", caCert, caKey, fileName("server-key-2.pem"), fileName("server-cert-2.pem"))
	require.NoError(t, err)

	cli := newTestServerClient()
	cli.statusScheme = "https"
	cfg := newTestConfig()
	cfg.Port = cli.port
	cfg.Status.StatusPort = cli.statusPort
	cfg.Security.ClusterSSLCA = fileName("ca-cert-2.pem")
	cfg.Security.ClusterSSLCert = fileName("server-cert-2.pem")
	cfg.Security.ClusterSSLKey = fileName("server-key-2.pem")
	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	cli.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 100)

	// https connection should work.
	ts.runTestStatusAPI(t)

	// but plain http connection should fail.
	cli.statusScheme = "http"
	//nolint:bodyclose
	_, err = cli.fetchStatus("/status")
	require.Error(t, err)

	server.Close()
}

func TestStatusAPIWithTLSCNCheck(t *testing.T) {
	ts := createTidbTestSuite(t)

	dir := t.TempDir()

	caPath := filepath.Join(dir, "ca-cert-cn.pem")
	serverKeyPath := filepath.Join(dir, "server-key-cn.pem")
	serverCertPath := filepath.Join(dir, "server-cert-cn.pem")
	client1KeyPath := filepath.Join(dir, "client-key-cn-check-a.pem")
	client1CertPath := filepath.Join(dir, "client-cert-cn-check-a.pem")
	client2KeyPath := filepath.Join(dir, "client-key-cn-check-b.pem")
	client2CertPath := filepath.Join(dir, "client-cert-cn-check-b.pem")

	caCert, caKey, err := generateCert(0, "TiDB CA CN CHECK", nil, nil, filepath.Join(dir, "ca-key-cn.pem"), caPath)
	require.NoError(t, err)
	_, _, err = generateCert(1, "tidb-server-cn-check", caCert, caKey, serverKeyPath, serverCertPath)
	require.NoError(t, err)
	_, _, err = generateCert(2, "tidb-client-cn-check-a", caCert, caKey, client1KeyPath, client1CertPath, func(c *x509.Certificate) {
		c.Subject.CommonName = "tidb-client-1"
	})
	require.NoError(t, err)
	_, _, err = generateCert(3, "tidb-client-cn-check-b", caCert, caKey, client2KeyPath, client2CertPath, func(c *x509.Certificate) {
		c.Subject.CommonName = "tidb-client-2"
	})
	require.NoError(t, err)

	cli := newTestServerClient()
	cli.statusScheme = "https"
	cfg := newTestConfig()
	cfg.Port = cli.port
	cfg.Status.StatusPort = cli.statusPort
	cfg.Security.ClusterSSLCA = caPath
	cfg.Security.ClusterSSLCert = serverCertPath
	cfg.Security.ClusterSSLKey = serverKeyPath
	cfg.Security.ClusterVerifyCN = []string{"tidb-client-2"}
	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)

	cli.port = getPortFromTCPAddr(server.listener.Addr())
	cli.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	defer server.Close()
	time.Sleep(time.Millisecond * 100)

	hc := newTLSHttpClient(t, caPath,
		client1CertPath,
		client1KeyPath,
	)
	//nolint:bodyclose
	_, err = hc.Get(cli.statusURL("/status"))
	require.Error(t, err)

	hc = newTLSHttpClient(t, caPath,
		client2CertPath,
		client2KeyPath,
	)
	resp, err := hc.Get(cli.statusURL("/status"))
	require.NoError(t, err)
	require.Nil(t, resp.Body.Close())
}

func newTLSHttpClient(t *testing.T, caFile, certFile, keyFile string) *http.Client {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)
	caCert, err := os.ReadFile(caFile)
	require.NoError(t, err)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: true,
	}
	tlsConfig.BuildNameToCertificate()
	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
}

func TestMultiStatements(t *testing.T) {
	ts := createTidbTestSuite(t)

	ts.runFailedTestMultiStatements(t)
	ts.runTestMultiStatements(t)
}

func TestSocketForwarding(t *testing.T) {
	tempDir := t.TempDir()
	socketFile := tempDir + "/tidbtest.sock" // Unix Socket does not work on Windows, so '/' should be OK

	ts := createTidbTestSuite(t)

	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Socket = socketFile
	cfg.Port = cli.port
	os.Remove(cfg.Socket)
	cfg.Status.ReportStatus = false

	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()

	cli.runTestRegression(t, func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = socketFile
		config.DBName = "test"
		config.Params = map[string]string{"sql_mode": "'STRICT_ALL_TABLES'"}
	}, "SocketRegression")
}

func TestSocket(t *testing.T) {
	tempDir := t.TempDir()
	socketFile := tempDir + "/tidbtest.sock" // Unix Socket does not work on Windows, so '/' should be OK

	cfg := newTestConfig()
	cfg.Socket = socketFile
	cfg.Port = 0
	os.Remove(cfg.Socket)
	cfg.Host = ""
	cfg.Status.ReportStatus = false

	ts := createTidbTestSuite(t)

	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()

	confFunc := func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = socketFile
		config.DBName = "test"
		config.Params = map[string]string{"sql_mode": "STRICT_ALL_TABLES"}
	}
	// a fake server client, config is override, just used to run tests
	cli := newTestServerClient()
	cli.waitUntilCustomServerCanConnect(confFunc)
	cli.runTestRegression(t, confFunc, "SocketRegression")
}

func TestSocketAndIp(t *testing.T) {
	tempDir := t.TempDir()
	socketFile := tempDir + "/tidbtest.sock" // Unix Socket does not work on Windows, so '/' should be OK

	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Socket = socketFile
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false

	ts := createTidbTestSuite(t)

	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	cli.waitUntilServerCanConnect()
	defer server.Close()

	// Test with Socket connection + Setup user1@% for all host access
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	defer func() {
		cli.runTests(t, func(config *mysql.Config) {
			config.User = "root"
		},
			func(dbt *testkit.DBTestKit) {
				dbt.MustExec("DROP USER IF EXISTS 'user1'@'%'")
				dbt.MustExec("DROP USER IF EXISTS 'user1'@'localhost'")
				dbt.MustExec("DROP USER IF EXISTS 'user1'@'127.0.0.1'")
			})
	}()
	cli.runTests(t, func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = socketFile
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			cli.checkRows(t, rows, "root@localhost")
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			dbt.MustQuery("CREATE USER user1@'%'")
			dbt.MustQuery("GRANT SELECT ON test.* TO user1@'%'")
		})
	// Test with Network interface connection with all hosts
	cli.runTests(t, func(config *mysql.Config) {
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			// NOTICE: this is not compatible with MySQL! (MySQL would report user1@localhost also for 127.0.0.1)
			cli.checkRows(t, rows, "user1@127.0.0.1")
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT USAGE ON *.* TO 'user1'@'%'\nGRANT SELECT ON test.* TO 'user1'@'%'")
			rows = dbt.MustQuery("select host from information_schema.processlist where user = 'user1'")
			records := cli.Rows(t, rows)
			require.Contains(t, records[0], ":", "Missing :<port> in is.processlist")
		})
	// Test with unix domain socket file connection with all hosts
	cli.runTests(t, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			cli.checkRows(t, rows, "user1@localhost")
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT USAGE ON *.* TO 'user1'@'%'\nGRANT SELECT ON test.* TO 'user1'@'%'")
		})

	// Setup user1@127.0.0.1 for loop back network interface access
	cli.runTests(t, func(config *mysql.Config) {
		config.User = "root"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			// NOTICE: this is not compatible with MySQL! (MySQL would report user1@localhost also for 127.0.0.1)
			cli.checkRows(t, rows, "root@127.0.0.1")
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			dbt.MustQuery("CREATE USER user1@127.0.0.1")
			dbt.MustQuery("GRANT SELECT,INSERT ON test.* TO user1@'127.0.0.1'")
		})
	// Test with Network interface connection with all hosts
	cli.runTests(t, func(config *mysql.Config) {
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			// NOTICE: this is not compatible with MySQL! (MySQL would report user1@localhost also for 127.0.0.1)
			cli.checkRows(t, rows, "user1@127.0.0.1")
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT USAGE ON *.* TO 'user1'@'127.0.0.1'\nGRANT SELECT,INSERT ON test.* TO 'user1'@'127.0.0.1'")
		})
	// Test with unix domain socket file connection with all hosts
	cli.runTests(t, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			cli.checkRows(t, rows, "user1@localhost")
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT USAGE ON *.* TO 'user1'@'%'\nGRANT SELECT ON test.* TO 'user1'@'%'")
		})

	// Setup user1@localhost for socket (and if MySQL compatible; loop back network interface access)
	cli.runTests(t, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "root"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			cli.checkRows(t, rows, "root@localhost")
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			dbt.MustExec("CREATE USER user1@localhost")
			dbt.MustExec("GRANT SELECT,INSERT,UPDATE,DELETE ON test.* TO user1@localhost")
		})
	// Test with Network interface connection with all hosts
	cli.runTests(t, func(config *mysql.Config) {
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			// NOTICE: this is not compatible with MySQL! (MySQL would report user1@localhost also for 127.0.0.1)
			cli.checkRows(t, rows, "user1@127.0.0.1")
			require.NoError(t, rows.Close())
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT USAGE ON *.* TO 'user1'@'127.0.0.1'\nGRANT SELECT,INSERT ON test.* TO 'user1'@'127.0.0.1'")
			require.NoError(t, rows.Close())
		})
	// Test with unix domain socket file connection with all hosts
	cli.runTests(t, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			cli.checkRows(t, rows, "user1@localhost")
			require.NoError(t, rows.Close())
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT USAGE ON *.* TO 'user1'@'localhost'\nGRANT SELECT,INSERT,UPDATE,DELETE ON test.* TO 'user1'@'localhost'")
			require.NoError(t, rows.Close())
		})
}

// TestOnlySocket for server configuration without network interface for mysql clients
func TestOnlySocket(t *testing.T) {
	tempDir := t.TempDir()
	socketFile := tempDir + "/tidbtest.sock" // Unix Socket does not work on Windows, so '/' should be OK

	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Socket = socketFile
	cfg.Host = "" // No network interface listening for mysql traffic
	cfg.Status.ReportStatus = false

	ts := createTidbTestSuite(t)

	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()
	require.Nil(t, server.listener)
	require.NotNil(t, server.socket)

	// Test with Socket connection + Setup user1@% for all host access
	defer func() {
		cli.runTests(t, func(config *mysql.Config) {
			config.User = "root"
			config.Net = "unix"
			config.Addr = socketFile
		},
			func(dbt *testkit.DBTestKit) {
				dbt.MustExec("DROP USER IF EXISTS 'user1'@'%'")
				dbt.MustExec("DROP USER IF EXISTS 'user1'@'localhost'")
				dbt.MustExec("DROP USER IF EXISTS 'user1'@'127.0.0.1'")
			})
	}()
	cli.runTests(t, func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = socketFile
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			cli.checkRows(t, rows, "root@localhost")
			require.NoError(t, rows.Close())
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			require.NoError(t, rows.Close())
			dbt.MustExec("CREATE USER user1@'%'")
			dbt.MustExec("GRANT SELECT ON test.* TO user1@'%'")
		})
	// Test with Network interface connection with all hosts, should fail since server not configured
	db, err := sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "root"
		config.DBName = "test"
	}))
	require.NoErrorf(t, err, "Open failed")
	err = db.Ping()
	require.Errorf(t, err, "Connect succeeded when not configured!?!")
	db.Close()
	db, err = sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "user1"
		config.DBName = "test"
	}))
	require.NoErrorf(t, err, "Open failed")
	err = db.Ping()
	require.Errorf(t, err, "Connect succeeded when not configured!?!")
	db.Close()
	// Test with unix domain socket file connection with all hosts
	cli.runTests(t, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			cli.checkRows(t, rows, "user1@localhost")
			require.NoError(t, rows.Close())
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT USAGE ON *.* TO 'user1'@'%'\nGRANT SELECT ON test.* TO 'user1'@'%'")
			require.NoError(t, rows.Close())
		})

	// Setup user1@127.0.0.1 for loop back network interface access
	cli.runTests(t, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "root"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			// NOTICE: this is not compatible with MySQL! (MySQL would report user1@localhost also for 127.0.0.1)
			cli.checkRows(t, rows, "root@localhost")
			require.NoError(t, rows.Close())
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			require.NoError(t, rows.Close())
			dbt.MustExec("CREATE USER user1@127.0.0.1")
			dbt.MustExec("GRANT SELECT,INSERT ON test.* TO user1@'127.0.0.1'")
		})
	// Test with unix domain socket file connection with all hosts
	cli.runTests(t, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			cli.checkRows(t, rows, "user1@localhost")
			require.NoError(t, rows.Close())
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT USAGE ON *.* TO 'user1'@'%'\nGRANT SELECT ON test.* TO 'user1'@'%'")
			require.NoError(t, rows.Close())
		})

	// Setup user1@localhost for socket (and if MySQL compatible; loop back network interface access)
	cli.runTests(t, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "root"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			cli.checkRows(t, rows, "root@localhost")
			require.NoError(t, rows.Close())
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			require.NoError(t, rows.Close())
			dbt.MustExec("CREATE USER user1@localhost")
			dbt.MustExec("GRANT SELECT,INSERT,UPDATE,DELETE ON test.* TO user1@localhost")
		})
	// Test with unix domain socket file connection with all hosts
	cli.runTests(t, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			cli.checkRows(t, rows, "user1@localhost")
			require.NoError(t, rows.Close())
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT USAGE ON *.* TO 'user1'@'localhost'\nGRANT SELECT,INSERT,UPDATE,DELETE ON test.* TO 'user1'@'localhost'")
			require.NoError(t, rows.Close())
		})
}

// generateCert generates a private key and a certificate in PEM format based on parameters.
// If parentCert and parentCertKey is specified, the new certificate will be signed by the parentCert.
// Otherwise, the new certificate will be self-signed and is a CA.
func generateCert(sn int, commonName string, parentCert *x509.Certificate, parentCertKey *rsa.PrivateKey, outKeyFile string, outCertFile string, opts ...func(c *x509.Certificate)) (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 528)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	notBefore := time.Now().Add(-10 * time.Minute).UTC()
	notAfter := notBefore.Add(1 * time.Hour).UTC()

	template := x509.Certificate{
		SerialNumber:          big.NewInt(int64(sn)),
		Subject:               pkix.Name{CommonName: commonName, Names: []pkix.AttributeTypeAndValue{util.MockPkixAttribute(util.CommonName, commonName)}},
		DNSNames:              []string{commonName},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	for _, opt := range opts {
		opt(&template)
	}

	var parent *x509.Certificate
	var priv *rsa.PrivateKey

	if parentCert == nil || parentCertKey == nil {
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
		parent = &template
		priv = privateKey
	} else {
		parent = parentCert
		priv = parentCertKey
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, parent, &privateKey.PublicKey, priv)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	cert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	certOut, err := os.Create(outCertFile)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = certOut.Close()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	keyOut, err := os.OpenFile(outKeyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = keyOut.Close()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return cert, privateKey, nil
}

// registerTLSConfig registers a mysql client TLS config.
// See https://godoc.org/github.com/go-sql-driver/mysql#RegisterTLSConfig for details.
func registerTLSConfig(configName string, caCertPath string, clientCertPath string, clientKeyPath string, serverName string, verifyServer bool) error {
	rootCertPool := x509.NewCertPool()
	data, err := os.ReadFile(caCertPath)
	if err != nil {
		return err
	}
	if ok := rootCertPool.AppendCertsFromPEM(data); !ok {
		return errors.New("Failed to append PEM")
	}
	clientCert := make([]tls.Certificate, 0, 1)
	certs, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return err
	}
	clientCert = append(clientCert, certs)
	tlsConfig := &tls.Config{
		RootCAs:            rootCertPool,
		Certificates:       clientCert,
		ServerName:         serverName,
		InsecureSkipVerify: !verifyServer,
	}
	return mysql.RegisterTLSConfig(configName, tlsConfig)
}

func TestSystemTimeZone(t *testing.T) {
	ts := createTidbTestSuite(t)

	tk := testkit.NewTestKit(t, ts.store)
	cfg := newTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	defer server.Close()

	tz1 := tk.MustQuery("select variable_value from mysql.tidb where variable_name = 'system_tz'").Rows()
	tk.MustQuery("select @@system_time_zone").Check(tz1)
}

func TestInternalSessionTxnStartTS(t *testing.T) {
	ts := createTidbTestSuite(t)

	se, err := session.CreateSession4Test(ts.store)
	require.NoError(t, err)

	_, err = se.Execute(context.Background(), "set global tidb_enable_metadata_lock=0")
	require.NoError(t, err)

	count := 10
	stmts := make([]ast.StmtNode, count)
	for i := 0; i < count; i++ {
		stmt, err := session.ParseWithParams4Test(context.Background(), se, "select * from mysql.user limit 1")
		require.NoError(t, err)
		stmts[i] = stmt
	}
	// Test an issue that sysSessionPool doesn't call session's Close, cause
	// asyncGetTSWorker goroutine leak.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockDelayInnerSessionExecute", "return"))
	var wg util.WaitGroupWrapper
	for i := 0; i < count; i++ {
		s := stmts[i]
		wg.Run(func() {
			_, _, err := session.ExecRestrictedStmt4Test(context.Background(), se, s)
			require.NoError(t, err)
		})
	}
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockDelayInnerSessionExecute"))

	lst := ts.domain.InfoSyncer().GetSessionManager().GetInternalSessionStartTSList()
	require.Equal(t, len(lst), 10)

	wg.Wait()
}

func TestClientWithCollation(t *testing.T) {
	ts := createTidbTestSuite(t)

	ts.runTestClientWithCollation(t)
}

func TestCreateTableFlen(t *testing.T) {
	ts := createTidbTestSuite(t)

	// issue #4540
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil, nil)
	require.NoError(t, err)
	_, err = Execute(context.Background(), qctx, "use test;")
	require.NoError(t, err)

	ctx := context.Background()
	testSQL := "CREATE TABLE `t1` (" +
		"`a` char(36) NOT NULL," +
		"`b` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`c` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`d` varchar(50) DEFAULT ''," +
		"`e` char(36) NOT NULL DEFAULT ''," +
		"`f` char(36) NOT NULL DEFAULT ''," +
		"`g` char(1) NOT NULL DEFAULT 'N'," +
		"`h` varchar(100) NOT NULL," +
		"`i` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`j` varchar(10) DEFAULT ''," +
		"`k` varchar(10) DEFAULT ''," +
		"`l` varchar(20) DEFAULT ''," +
		"`m` varchar(20) DEFAULT ''," +
		"`n` varchar(30) DEFAULT ''," +
		"`o` varchar(100) DEFAULT ''," +
		"`p` varchar(50) DEFAULT ''," +
		"`q` varchar(50) DEFAULT ''," +
		"`r` varchar(100) DEFAULT ''," +
		"`s` varchar(20) DEFAULT ''," +
		"`t` varchar(50) DEFAULT ''," +
		"`u` varchar(100) DEFAULT ''," +
		"`v` varchar(50) DEFAULT ''," +
		"`w` varchar(300) NOT NULL," +
		"`x` varchar(250) DEFAULT ''," +
		"`y` decimal(20)," +
		"`z` decimal(20, 4)," +
		"PRIMARY KEY (`a`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
	_, err = Execute(ctx, qctx, testSQL)
	require.NoError(t, err)
	rs, err := Execute(ctx, qctx, "show create table t1")
	require.NoError(t, err)
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	require.NoError(t, err)
	cols := rs.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 2)
	require.Equal(t, 5*tmysql.MaxBytesOfCharacter, int(cols[0].ColumnLength))
	require.Equal(t, len(req.GetRow(0).GetString(1))*tmysql.MaxBytesOfCharacter, int(cols[1].ColumnLength))

	// for issue#5246
	rs, err = Execute(ctx, qctx, "select y, z from t1")
	require.NoError(t, err)
	cols = rs.Columns()
	require.Len(t, cols, 2)
	require.Equal(t, 21, int(cols[0].ColumnLength))
	require.Equal(t, 22, int(cols[1].ColumnLength))
	rs.Close()
}

func Execute(ctx context.Context, qc *TiDBContext, sql string) (ResultSet, error) {
	stmts, err := qc.Parse(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		panic("wrong input for Execute: " + sql)
	}
	return qc.ExecuteStmt(ctx, stmts[0])
}

func TestShowTablesFlen(t *testing.T) {
	ts := createTidbTestSuite(t)

	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil, nil)
	require.NoError(t, err)
	ctx := context.Background()
	_, err = Execute(ctx, qctx, "use test;")
	require.NoError(t, err)

	testSQL := "create table abcdefghijklmnopqrstuvwxyz (i int)"
	_, err = Execute(ctx, qctx, testSQL)
	require.NoError(t, err)
	rs, err := Execute(ctx, qctx, "show tables")
	require.NoError(t, err)
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	require.NoError(t, err)
	cols := rs.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 1)
	require.Equal(t, 26*tmysql.MaxBytesOfCharacter, int(cols[0].ColumnLength))
}

func checkColNames(t *testing.T, columns []*ColumnInfo, names ...string) {
	for i, name := range names {
		require.Equal(t, name, columns[i].Name)
		require.Equal(t, name, columns[i].OrgName)
	}
}

func TestFieldList(t *testing.T) {
	ts := createTidbTestSuite(t)

	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil, nil)
	require.NoError(t, err)
	_, err = Execute(context.Background(), qctx, "use test;")
	require.NoError(t, err)

	ctx := context.Background()
	testSQL := `create table t (
		c_bit bit(10),
		c_int_d int,
		c_bigint_d bigint,
		c_float_d float,
		c_double_d double,
		c_decimal decimal(6, 3),
		c_datetime datetime(2),
		c_time time(3),
		c_date date,
		c_timestamp timestamp(4) DEFAULT CURRENT_TIMESTAMP(4),
		c_char char(20),
		c_varchar varchar(20),
		c_text_d text,
		c_binary binary(20),
		c_blob_d blob,
		c_set set('a', 'b', 'c'),
		c_enum enum('a', 'b', 'c'),
		c_json JSON,
		c_year year
	)`
	_, err = Execute(ctx, qctx, testSQL)
	require.NoError(t, err)
	colInfos, err := qctx.FieldList("t")
	require.NoError(t, err)
	require.Len(t, colInfos, 19)

	checkColNames(t, colInfos, "c_bit", "c_int_d", "c_bigint_d", "c_float_d",
		"c_double_d", "c_decimal", "c_datetime", "c_time", "c_date", "c_timestamp",
		"c_char", "c_varchar", "c_text_d", "c_binary", "c_blob_d", "c_set", "c_enum",
		"c_json", "c_year")

	for _, cols := range colInfos {
		require.Equal(t, "test", cols.Schema)
	}

	for _, cols := range colInfos {
		require.Equal(t, "t", cols.Table)
	}

	for i, col := range colInfos {
		switch i {
		case 10, 11, 12, 15, 16:
			// c_char char(20), c_varchar varchar(20), c_text_d text,
			// c_set set('a', 'b', 'c'), c_enum enum('a', 'b', 'c')
			require.Equalf(t, uint16(tmysql.CharsetNameToID(tmysql.DefaultCharset)), col.Charset, "index %d", i)
			continue
		}

		require.Equalf(t, uint16(tmysql.CharsetNameToID("binary")), col.Charset, "index %d", i)
	}

	// c_decimal decimal(6, 3)
	require.Equal(t, uint8(3), colInfos[5].Decimal)

	// for issue#10513
	tooLongColumnAsName := "COALESCE(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)"
	columnAsName := tooLongColumnAsName[:tmysql.MaxAliasIdentifierLen]

	rs, err := Execute(ctx, qctx, "select "+tooLongColumnAsName)
	require.NoError(t, err)
	cols := rs.Columns()
	require.Equal(t, tooLongColumnAsName, cols[0].OrgName)
	require.Equal(t, columnAsName, cols[0].Name)
	rs.Close()

	rs, err = Execute(ctx, qctx, "select c_bit as '"+tooLongColumnAsName+"' from t")
	require.NoError(t, err)
	cols = rs.Columns()
	require.Equal(t, "c_bit", cols[0].OrgName)
	require.Equal(t, columnAsName, cols[0].Name)
	rs.Close()
}

func TestClientErrors(t *testing.T) {
	ts := createTidbTestSuite(t)
	ts.runTestInfoschemaClientErrors(t)
}

func TestInitConnect(t *testing.T) {
	ts := createTidbTestSuite(t)
	ts.runTestInitConnect(t)
}

func TestSumAvg(t *testing.T) {
	ts := createTidbTestSuite(t)
	ts.runTestSumAvg(t)
}

func TestNullFlag(t *testing.T) {
	ts := createTidbTestSuite(t)

	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	{
		// issue #9689
		rs, err := Execute(ctx, qctx, "select 1")
		require.NoError(t, err)
		cols := rs.Columns()
		require.Len(t, cols, 1)
		expectFlag := uint16(tmysql.NotNullFlag | tmysql.BinaryFlag)
		require.Equal(t, expectFlag, dumpFlag(cols[0].Type, cols[0].Flag))
		rs.Close()
	}

	{
		// issue #19025
		rs, err := Execute(ctx, qctx, "select convert('{}', JSON)")
		require.NoError(t, err)
		cols := rs.Columns()
		require.Len(t, cols, 1)
		expectFlag := uint16(tmysql.BinaryFlag)
		require.Equal(t, expectFlag, dumpFlag(cols[0].Type, cols[0].Flag))
		rs.Close()
	}

	{
		// issue #18488
		_, err := Execute(ctx, qctx, "use test")
		require.NoError(t, err)
		_, err = Execute(ctx, qctx, "CREATE TABLE `test` (`iD` bigint(20) NOT NULL, `INT_TEST` int(11) DEFAULT NULL);")
		require.NoError(t, err)
		rs, err := Execute(ctx, qctx, `SELECT id + int_test as res FROM test  GROUP BY res ORDER BY res;`)
		require.NoError(t, err)
		cols := rs.Columns()
		require.Len(t, cols, 1)
		expectFlag := uint16(tmysql.BinaryFlag)
		require.Equal(t, expectFlag, dumpFlag(cols[0].Type, cols[0].Flag))
		rs.Close()
	}

	{
		rs, err := Execute(ctx, qctx, "select if(1, null, 1) ;")
		require.NoError(t, err)
		cols := rs.Columns()
		require.Len(t, cols, 1)
		expectFlag := uint16(tmysql.BinaryFlag)
		require.Equal(t, expectFlag, dumpFlag(cols[0].Type, cols[0].Flag))
		rs.Close()
	}
	{
		rs, err := Execute(ctx, qctx, "select CASE 1 WHEN 2 THEN 1 END ;")
		require.NoError(t, err)
		cols := rs.Columns()
		require.Len(t, cols, 1)
		expectFlag := uint16(tmysql.BinaryFlag)
		require.Equal(t, expectFlag, dumpFlag(cols[0].Type, cols[0].Flag))
		rs.Close()
	}
	{
		rs, err := Execute(ctx, qctx, "select NULL;")
		require.NoError(t, err)
		cols := rs.Columns()
		require.Len(t, cols, 1)
		expectFlag := uint16(tmysql.BinaryFlag)
		require.Equal(t, expectFlag, dumpFlag(cols[0].Type, cols[0].Flag))
		rs.Close()
	}
}

func TestNO_DEFAULT_VALUEFlag(t *testing.T) {
	ts := createTidbTestSuite(t)

	// issue #21465
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = Execute(ctx, qctx, "use test")
	require.NoError(t, err)
	_, err = Execute(ctx, qctx, "drop table if exists t")
	require.NoError(t, err)
	_, err = Execute(ctx, qctx, "create table t(c1 int key, c2 int);")
	require.NoError(t, err)
	rs, err := Execute(ctx, qctx, "select c1 from t;")
	require.NoError(t, err)
	defer rs.Close()
	cols := rs.Columns()
	require.Len(t, cols, 1)
	expectFlag := uint16(tmysql.NotNullFlag | tmysql.PriKeyFlag | tmysql.NoDefaultValueFlag)
	require.Equal(t, expectFlag, dumpFlag(cols[0].Type, cols[0].Flag))
}

func TestGracefulShutdown(t *testing.T) {
	ts := createTidbTestSuite(t)

	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.GracefulWaitBeforeShutdown = 2 // wait before shutdown
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	cfg.Status.ReportStatus = true
	cfg.Performance.TCPKeepAlive = true
	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	require.NotNil(t, server)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	cli.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 100)

	resp, err := cli.fetchStatus("/status") // server is up
	require.NoError(t, err)
	require.Nil(t, resp.Body.Close())

	go server.Close()
	time.Sleep(time.Millisecond * 500)

	resp, _ = cli.fetchStatus("/status") // should return 5xx code
	require.Equal(t, 500, resp.StatusCode)
	require.Nil(t, resp.Body.Close())

	time.Sleep(time.Second * 2)

	//nolint:bodyclose
	_, err = cli.fetchStatus("/status") // status is gone
	require.Error(t, err)
	require.Regexp(t, "connect: connection refused$", err.Error())
}

func TestPessimisticInsertSelectForUpdate(t *testing.T) {
	ts := createTidbTestSuite(t)

	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil, nil)
	require.NoError(t, err)
	defer qctx.Close()
	ctx := context.Background()
	_, err = Execute(ctx, qctx, "use test;")
	require.NoError(t, err)
	_, err = Execute(ctx, qctx, "drop table if exists t1, t2")
	require.NoError(t, err)
	_, err = Execute(ctx, qctx, "create table t1 (id int)")
	require.NoError(t, err)
	_, err = Execute(ctx, qctx, "create table t2 (id int)")
	require.NoError(t, err)
	_, err = Execute(ctx, qctx, "insert into t1 select 1")
	require.NoError(t, err)
	_, err = Execute(ctx, qctx, "begin pessimistic")
	require.NoError(t, err)
	rs, err := Execute(ctx, qctx, "INSERT INTO t2 (id) select id from t1 where id = 1 for update")
	require.NoError(t, err)
	require.Nil(t, rs) // should be no delay
}

func TestTopSQLCatchRunningSQL(t *testing.T) {
	ts := createTidbTestTopSQLSuite(t)

	db, err := sql.Open("mysql", ts.getDSN())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	dbt := testkit.NewDBTestKit(t, db)
	dbt.MustExec("drop database if exists topsql")
	dbt.MustExec("create database topsql")
	dbt.MustExec("use topsql;")
	dbt.MustExec("create table t (a int, b int);")

	for i := 0; i < 5000; i++ {
		dbt.MustExec(fmt.Sprintf("insert into t values (%v, %v)", i, i))
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/util/topsql/mockHighLoadForEachPlan", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/skipLoadSysVarCacheLoop", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/util/topsql/mockHighLoadForEachPlan"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/skipLoadSysVarCacheLoop"))
	}()

	mc := mockTopSQLTraceCPU.NewTopSQLCollector()
	topsql.SetupTopSQLForTest(mc)
	sqlCPUCollector := collector.NewSQLCPUCollector(mc)
	sqlCPUCollector.Start()
	defer sqlCPUCollector.Stop()

	query := "select count(*) from t as t0 join t as t1 on t0.a != t1.a;"
	needEnableTopSQL := int64(0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if atomic.LoadInt64(&needEnableTopSQL) == 1 {
				time.Sleep(2 * time.Millisecond)
				topsqlstate.EnableTopSQL()
				atomic.StoreInt64(&needEnableTopSQL, 0)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	execFn := func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		atomic.StoreInt64(&needEnableTopSQL, 1)
		mustQuery(t, dbt, query)
		topsqlstate.DisableTopSQL()
	}
	check := func() {
		require.NoError(t, ctx.Err())
		stats := mc.GetSQLStatsBySQLWithRetry(query, true)
		require.Greaterf(t, len(stats), 0, query)
	}
	ts.testCase(t, mc, execFn, check)
	cancel()
	wg.Wait()
}

func TestTopSQLCPUProfile(t *testing.T) {
	ts := createTidbTestTopSQLSuite(t)

	db, err := sql.Open("mysql", ts.getDSN())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/util/topsql/mockHighLoadForEachSQL", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/util/topsql/mockHighLoadForEachPlan", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/skipLoadSysVarCacheLoop", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/util/topsql/mockHighLoadForEachSQL"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/util/topsql/mockHighLoadForEachPlan"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/skipLoadSysVarCacheLoop"))
	}()

	topsqlstate.EnableTopSQL()
	defer topsqlstate.DisableTopSQL()

	mc := mockTopSQLTraceCPU.NewTopSQLCollector()
	topsql.SetupTopSQLForTest(mc)
	sqlCPUCollector := collector.NewSQLCPUCollector(mc)
	sqlCPUCollector.Start()
	defer sqlCPUCollector.Stop()

	dbt := testkit.NewDBTestKit(t, db)
	dbt.MustExec("drop database if exists topsql")
	dbt.MustExec("create database topsql")
	dbt.MustExec("use topsql;")
	dbt.MustExec("create table t (a int auto_increment, b int, unique index idx(a));")
	dbt.MustExec("create table t1 (a int auto_increment, b int, unique index idx(a));")
	dbt.MustExec("create table t2 (a int auto_increment, b int, unique index idx(a));")
	dbt.MustExec("set @@global.tidb_txn_mode = 'pessimistic'")

	checkFn := func(sql, planRegexp string) {
		stats := mc.GetSQLStatsBySQLWithRetry(sql, len(planRegexp) > 0)
		// since 1 sql may has many plan, check `len(stats) > 0` instead of `len(stats) == 1`.
		require.Greaterf(t, len(stats), 0, "sql: "+sql)

		for _, s := range stats {
			sqlStr := mc.GetSQL(s.SQLDigest)
			encodedPlan := mc.GetPlan(s.PlanDigest)
			// Normalize the user SQL before check.
			normalizedSQL := parser.Normalize(sql)
			require.Equalf(t, normalizedSQL, sqlStr, "sql: %v", sql)
			// decode plan before check.
			normalizedPlan, err := plancodec.DecodeNormalizedPlan(encodedPlan)
			require.NoError(t, err)
			// remove '\n' '\t' before do regexp match.
			normalizedPlan = strings.Replace(normalizedPlan, "\n", " ", -1)
			normalizedPlan = strings.Replace(normalizedPlan, "\t", " ", -1)
			require.Regexpf(t, planRegexp, normalizedPlan, "sql: %v", sql)
		}
	}

	// Test case 1: DML query: insert/update/replace/delete/select
	cases1 := []struct {
		sql        string
		planRegexp string
	}{
		{sql: "insert into t () values (),(),(),(),(),(),();", planRegexp: ""},
		{sql: "insert into t (b) values (1),(1),(1),(1),(1),(1),(1),(1);", planRegexp: ""},
		{sql: "update t set b=a where b is null limit 1;", planRegexp: ".*Limit.*TableReader.*"},
		{sql: "delete from t where b = a limit 2;", planRegexp: ".*Limit.*TableReader.*"},
		{sql: "replace into t (b) values (1),(1),(1),(1),(1),(1),(1),(1);", planRegexp: ""},
		{sql: "select * from t use index(idx) where a<10;", planRegexp: ".*IndexLookUp.*"},
		{sql: "select * from t ignore index(idx) where a>1000000000;", planRegexp: ".*TableReader.*"},
		{sql: "select /*+ HASH_JOIN(t1, t2) */ * from t t1 join t t2 on t1.a=t2.a where t1.b is not null;", planRegexp: ".*HashJoin.*"},
		{sql: "select /*+ INL_HASH_JOIN(t1, t2) */ * from t t1 join t t2 on t2.a=t1.a where t1.b is not null;", planRegexp: ".*IndexHashJoin.*"},
		{sql: "select * from t where a=1;", planRegexp: ".*Point_Get.*"},
		{sql: "select * from t where a in (1,2,3,4)", planRegexp: ".*Batch_Point_Get.*"},
	}
	execFn := func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		for _, ca := range cases1 {
			sqlStr := ca.sql
			if strings.HasPrefix(sqlStr, "select") {
				mustQuery(t, dbt, sqlStr)
			} else {
				dbt.MustExec(sqlStr)
			}
		}
	}
	check := func() {
		for _, ca := range cases1 {
			checkFn(ca.sql, ca.planRegexp)
		}
	}
	ts.testCase(t, mc, execFn, check)

	// Test case 2: prepare/execute sql
	cases2 := []struct {
		prepare    string
		args       []interface{}
		planRegexp string
	}{
		{prepare: "insert into t1 (b) values (?);", args: []interface{}{1}, planRegexp: ""},
		{prepare: "replace into t1 (b) values (?);", args: []interface{}{1}, planRegexp: ""},
		{prepare: "update t1 set b=a where b is null limit ?;", args: []interface{}{1}, planRegexp: ".*Limit.*TableReader.*"},
		{prepare: "delete from t1 where b = a limit ?;", args: []interface{}{1}, planRegexp: ".*Limit.*TableReader.*"},
		{prepare: "replace into t1 (b) values (?);", args: []interface{}{1}, planRegexp: ""},
		{prepare: "select * from t1 use index(idx) where a<?;", args: []interface{}{10}, planRegexp: ".*IndexLookUp.*"},
		{prepare: "select * from t1 ignore index(idx) where a>?;", args: []interface{}{1000000000}, planRegexp: ".*TableReader.*"},
		{prepare: "select /*+ HASH_JOIN(t1, t2) */ * from t1 t1 join t1 t2 on t1.a=t2.a where t1.b is not null;", args: nil, planRegexp: ".*HashJoin.*"},
		{prepare: "select /*+ INL_HASH_JOIN(t1, t2) */ * from t1 t1 join t1 t2 on t2.a=t1.a where t1.b is not null;", args: nil, planRegexp: ".*IndexHashJoin.*"},
		{prepare: "select * from t1 where a=?;", args: []interface{}{1}, planRegexp: ".*Point_Get.*"},
		{prepare: "select * from t1 where a in (?,?,?,?)", args: []interface{}{1, 2, 3, 4}, planRegexp: ".*Batch_Point_Get.*"},
	}
	execFn = func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		for _, ca := range cases2 {
			prepare, args := ca.prepare, ca.args
			stmt := dbt.MustPrepare(prepare)
			if strings.HasPrefix(prepare, "select") {
				rows, err := stmt.Query(args...)
				require.NoError(t, err)
				for rows.Next() {
				}
				require.NoError(t, rows.Close())
			} else {
				_, err = stmt.Exec(args...)
				require.NoError(t, err)
			}
		}
	}
	check = func() {
		for _, ca := range cases2 {
			checkFn(ca.prepare, ca.planRegexp)
		}
	}
	ts.testCase(t, mc, execFn, check)

	// Test case 3: prepare, execute stmt using @val...
	cases3 := []struct {
		prepare    string
		args       []interface{}
		planRegexp string
	}{
		{prepare: "insert into t2 (b) values (?);", args: []interface{}{1}, planRegexp: ""},
		{prepare: "update t2 set b=a where b is null limit ?;", args: []interface{}{1}, planRegexp: ".*Limit.*TableReader.*"},
		{prepare: "delete from t2 where b = a limit ?;", args: []interface{}{1}, planRegexp: ".*Limit.*TableReader.*"},
		{prepare: "replace into t2 (b) values (?);", args: []interface{}{1}, planRegexp: ""},
		{prepare: "select * from t2 use index(idx) where a<?;", args: []interface{}{10}, planRegexp: ".*IndexLookUp.*"},
		{prepare: "select * from t2 ignore index(idx) where a>?;", args: []interface{}{1000000000}, planRegexp: ".*TableReader.*"},
		{prepare: "select /*+ HASH_JOIN(t1, t2) */ * from t2 t1 join t2 t2 on t1.a=t2.a where t1.b is not null;", args: nil, planRegexp: ".*HashJoin.*"},
		{prepare: "select /*+ INL_HASH_JOIN(t1, t2) */ * from t2 t1 join t2 t2 on t2.a=t1.a where t1.b is not null;", args: nil, planRegexp: ".*IndexHashJoin.*"},
		{prepare: "select * from t2 where a=?;", args: []interface{}{1}, planRegexp: ".*Point_Get.*"},
		{prepare: "select * from t2 where a in (?,?,?,?)", args: []interface{}{1, 2, 3, 4}, planRegexp: ".*Batch_Point_Get.*"},
	}
	execFn = func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		for _, ca := range cases3 {
			prepare, args := ca.prepare, ca.args
			dbt.MustExec(fmt.Sprintf("prepare stmt from '%v'", prepare))

			var params []string
			for i := range args {
				param := 'a' + i
				dbt.MustExec(fmt.Sprintf("set @%c=%v", param, args[i]))
				params = append(params, fmt.Sprintf("@%c", param))
			}

			sqlStr := "execute stmt"
			if len(params) > 0 {
				sqlStr += " using "
				sqlStr += strings.Join(params, ",")
			}
			if strings.HasPrefix(prepare, "select") {
				mustQuery(t, dbt, sqlStr)
			} else {
				dbt.MustExec(sqlStr)
			}
		}
	}
	check = func() {
		for _, ca := range cases3 {
			checkFn(ca.prepare, ca.planRegexp)
		}
	}
	ts.testCase(t, mc, execFn, check)

	// Test case for other statements
	cases4 := []struct {
		sql     string
		plan    string
		isQuery bool
	}{
		{"begin", "", false},
		{"insert into t () values (),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),()", "", false},
		{"commit", "", false},
		{"analyze table t", "", false},
		{"explain analyze select sum(a+b) from t", ".*TableReader.*", true},
		{"trace select sum(b*a), sum(a+b) from t", "", true},
		{"set global tidb_stmt_summary_history_size=5;", "", false},
	}
	execFn = func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		for _, ca := range cases4 {
			if ca.isQuery {
				mustQuery(t, dbt, ca.sql)
			} else {
				dbt.MustExec(ca.sql)
			}
		}
	}
	check = func() {
		for _, ca := range cases4 {
			checkFn(ca.sql, ca.plan)
		}
		// check for internal SQL.
		checkFn("replace into mysql.global_variables (variable_name,variable_value) values ('tidb_stmt_summary_history_size', '5')", "")
	}
	ts.testCase(t, mc, execFn, check)

	// Test case for multi-statement.
	cases5 := []string{
		"delete from t limit 1;",
		"update t set b=1 where b is null limit 1;",
		"select sum(a+b*2) from t;",
	}
	multiStatement5 := strings.Join(cases5, "")
	execFn = func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		dbt.MustExec("SET tidb_multi_statement_mode='ON'")
		dbt.MustExec(multiStatement5)
	}
	check = func() {
		for _, sqlStr := range cases5 {
			checkFn(sqlStr, ".*TableReader.*")
		}
	}
	ts.testCase(t, mc, execFn, check)

	// Test case for multi-statement, but first statements execute failed
	cases6 := []string{
		"delete from t_not_exist;",
		"update t set a=1 where a is null limit 1;",
	}
	multiStatement6 := strings.Join(cases6, "")
	execFn = func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		dbt.MustExec("SET tidb_multi_statement_mode='ON'")
		_, err := db.Exec(multiStatement6)
		require.NotNil(t, err)
		require.Equal(t, "Error 1146: Table 'topsql.t_not_exist' doesn't exist", err.Error())
	}
	check = func() {
		for i := 1; i < len(cases6); i++ {
			sqlStr := cases6[i]
			stats := mc.GetSQLStatsBySQL(sqlStr, false)
			require.Equal(t, 0, len(stats), sqlStr)
		}
	}
	ts.testCase(t, mc, execFn, check)

	// Test case for multi-statement, the first statements execute success but the second statement execute failed.
	cases7 := []string{
		"update t set a=1 where a <0 limit 1;",
		"delete from t_not_exist;",
	}
	multiStatement7 := strings.Join(cases7, "")
	execFn = func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		dbt.MustExec("SET tidb_multi_statement_mode='ON'")
		_, err = db.Exec(multiStatement7)
		require.NotNil(t, err)
		require.Equal(t, "Error 1146 (42S02): Table 'topsql.t_not_exist' doesn't exist", err.Error())
	}
	check = func() {
		checkFn(cases7[0], "") // the first statement execute success, should have topsql data.
	}
	ts.testCase(t, mc, execFn, check)

	// Test case for statement with wrong syntax.
	wrongSyntaxSQL := "select * froms t"
	execFn = func(db *sql.DB) {
		_, err = db.Exec(wrongSyntaxSQL)
		require.NotNil(t, err)
		require.Regexp(t, "Error 1064: You have an error in your SQL syntax...", err.Error())
	}
	check = func() {
		stats := mc.GetSQLStatsBySQL(wrongSyntaxSQL, false)
		require.Equal(t, 0, len(stats), wrongSyntaxSQL)
	}
	ts.testCase(t, mc, execFn, check)

	// Test case for high cost of plan optimize.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/planner/mockHighLoadForOptimize", "return"))
	selectSQL := "select sum(a+b), count(distinct b) from t where a+b >0"
	updateSQL := "update t set a=a+100 where a > 10000000"
	selectInPlanSQL := "select * from t where exists (select 1 from t1 where t1.a = 1);"
	execFn = func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		mustQuery(t, dbt, selectSQL)
		dbt.MustExec(updateSQL)
		mustQuery(t, dbt, selectInPlanSQL)
	}
	check = func() {
		checkFn(selectSQL, "")
		checkFn(updateSQL, "")
		selectCPUTime := mc.GetSQLCPUTimeBySQL(selectSQL)
		updateCPUTime := mc.GetSQLCPUTimeBySQL(updateSQL)
		require.Less(t, updateCPUTime, selectCPUTime)
		checkFn(selectInPlanSQL, "")
	}
	ts.testCase(t, mc, execFn, check)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/planner/mockHighLoadForOptimize"))

	// Test case for DDL execute failed but should still have CPU data.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockHighLoadForAddIndex", "return"))
	dbt.MustExec(fmt.Sprintf("insert into t values (%v,%v), (%v, %v);", 2000, 1, 2001, 1))
	addIndexStr := "alter table t add unique index idx_b (b)"
	execFn = func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		dbt.MustExec("alter table t drop index if exists idx_b")
		_, err := db.Exec(addIndexStr)
		require.NotNil(t, err)
		require.Equal(t, "Error 1062 (23000): Duplicate entry '1' for key 't.idx_b'", err.Error())
	}
	check = func() {
		checkFn(addIndexStr, "")
	}
	ts.testCase(t, mc, execFn, check)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockHighLoadForAddIndex"))

	// Test case for execute failed cause by storage error.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/copr/handleTaskOnceError", `return(true)`))
	execFailedQuery := "select * from t where a*b < 1000"
	execFn = func(db *sql.DB) {
		_, err = db.Query(execFailedQuery)
		require.NotNil(t, err)
		require.Equal(t, "Error 1105 (HY000): mock handleTaskOnce error", err.Error())
	}
	check = func() {
		checkFn(execFailedQuery, "")
	}
	ts.testCase(t, mc, execFn, check)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/copr/handleTaskOnceError"))
}

func (ts *tidbTestTopSQLSuite) testCase(t *testing.T, mc *mockTopSQLTraceCPU.TopSQLCollector, execFn func(db *sql.DB), checkFn func()) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		ts.loopExec(ctx, t, execFn)
	}()

	checkFn()
	cancel()
	wg.Wait()
	mc.Reset()
}

func mustQuery(t *testing.T, dbt *testkit.DBTestKit, query string) {
	rows := dbt.MustQuery(query)
	for rows.Next() {
	}
	err := rows.Close()
	require.NoError(t, err)
}

type mockCollector struct {
	f func(data stmtstats.StatementStatsMap)
}

func newMockCollector(f func(data stmtstats.StatementStatsMap)) stmtstats.Collector {
	return &mockCollector{f: f}
}

func (c *mockCollector) CollectStmtStatsMap(data stmtstats.StatementStatsMap) {
	c.f(data)
}

func waitCollected(ch chan struct{}) {
	select {
	case <-ch:
	case <-time.After(time.Second * 3):
	}
}

func TestTopSQLStatementStats(t *testing.T) {
	ts, total, tagChecker, collectedNotifyCh := setupForTestTopSQLStatementStats(t)

	const ExecCountPerSQL = 2
	// Test for CRUD.
	cases1 := []string{
		"insert into t values (%d, sleep(0.1))",
		"update t set a = %[1]d + 1000 where a = %[1]d and sleep(0.1);",
		"select a from t where b = %d and sleep(0.1);",
		"select a from t where a = %d and sleep(0.1);", // test for point-get
		"delete from t where a = %d and sleep(0.1);",
		"insert into t values (%d, sleep(0.1)) on duplicate key update b = b+1",
	}
	var wg sync.WaitGroup
	sqlDigests := map[stmtstats.BinaryDigest]string{}
	for i, ca := range cases1 {
		sqlStr := fmt.Sprintf(ca, i)
		_, digest := parser.NormalizeDigest(sqlStr)
		sqlDigests[stmtstats.BinaryDigest(digest.Bytes())] = sqlStr
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, ca := range cases1 {
			db, err := sql.Open("mysql", ts.getDSN())
			require.NoError(t, err)
			dbt := testkit.NewDBTestKit(t, db)
			dbt.MustExec("use stmtstats;")
			for n := 0; n < ExecCountPerSQL; n++ {
				sqlStr := fmt.Sprintf(ca, n)
				if strings.HasPrefix(strings.ToLower(sqlStr), "select") {
					mustQuery(t, dbt, sqlStr)
				} else {
					dbt.MustExec(sqlStr)
				}
			}
			err = db.Close()
			require.NoError(t, err)
		}
	}()

	// Test for prepare stmt/execute stmt
	cases2 := []struct {
		prepare    string
		execStmt   string
		setSQLsGen func(idx int) []string
		execSQL    string
	}{
		{
			prepare:  "prepare stmt from 'insert into t2 values (?, sleep(?))';",
			execStmt: "insert into t2 values (1, sleep(0.1))",
			setSQLsGen: func(idx int) []string {
				return []string{fmt.Sprintf("set @a=%v", idx), "set @b=0.1"}
			},
			execSQL: "execute stmt using @a, @b;",
		},
		{
			prepare:  "prepare stmt from 'update t2 set a = a + 1000 where a = ? and sleep(?);';",
			execStmt: "update t2 set a = a + 1000 where a = 1 and sleep(0.1);",
			setSQLsGen: func(idx int) []string {
				return []string{fmt.Sprintf("set @a=%v", idx), "set @b=0.1"}
			},
			execSQL: "execute stmt using @a, @b;",
		},
		{
			// test for point-get
			prepare:  "prepare stmt from 'select a, sleep(?) from t2 where a = ?';",
			execStmt: "select a, sleep(?) from t2 where a = ?",
			setSQLsGen: func(idx int) []string {
				return []string{"set @a=0.1", fmt.Sprintf("set @b=%v", idx)}
			},
			execSQL: "execute stmt using @a, @b;",
		},
		{
			prepare:  "prepare stmt from 'select a, sleep(?) from t2 where b = ?';",
			execStmt: "select a, sleep(?) from t2 where b = ?",
			setSQLsGen: func(idx int) []string {
				return []string{"set @a=0.1", fmt.Sprintf("set @b=%v", idx)}
			},
			execSQL: "execute stmt using @a, @b;",
		},
		{
			prepare:  "prepare stmt from 'delete from t2 where sleep(?) and a = ?';",
			execStmt: "delete from t2 where sleep(0.1) and a = 1",
			setSQLsGen: func(idx int) []string {
				return []string{"set @a=0.1", fmt.Sprintf("set @b=%v", idx)}
			},
			execSQL: "execute stmt using @a, @b;",
		},
		{
			prepare:  "prepare stmt from 'insert into t2 values (?, sleep(?)) on duplicate key update b = b+1';",
			execStmt: "insert into t2 values (1, sleep(0.1)) on duplicate key update b = b+1",
			setSQLsGen: func(idx int) []string {
				return []string{fmt.Sprintf("set @a=%v", idx), "set @b=0.1"}
			},
			execSQL: "execute stmt using @a, @b;",
		},
		{
			prepare:  "prepare stmt from 'set global tidb_enable_top_sql = (? = sleep(?))';",
			execStmt: "set global tidb_enable_top_sql = (0 = sleep(0.1))",
			setSQLsGen: func(idx int) []string {
				return []string{"set @a=0", "set @b=0.1"}
			},
			execSQL: "execute stmt using @a, @b;",
		},
	}
	for _, ca := range cases2 {
		_, digest := parser.NormalizeDigest(ca.execStmt)
		sqlDigests[stmtstats.BinaryDigest(digest.Bytes())] = ca.execStmt
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, ca := range cases2 {
			db, err := sql.Open("mysql", ts.getDSN())
			require.NoError(t, err)
			dbt := testkit.NewDBTestKit(t, db)
			dbt.MustExec("use stmtstats;")
			// prepare stmt
			dbt.MustExec(ca.prepare)
			for n := 0; n < ExecCountPerSQL; n++ {
				setSQLs := ca.setSQLsGen(n)
				for _, setSQL := range setSQLs {
					dbt.MustExec(setSQL)
				}
				if strings.HasPrefix(strings.ToLower(ca.execStmt), "select") {
					mustQuery(t, dbt, ca.execSQL)
				} else {
					dbt.MustExec(ca.execSQL)
				}
			}
			err = db.Close()
			require.NoError(t, err)
		}
	}()

	// Test for prepare by db client prepare/exec interface.
	cases3 := []struct {
		prepare  string
		execStmt string
		argsGen  func(idx int) []interface{}
	}{
		{
			prepare: "insert into t3 values (?, sleep(?))",
			argsGen: func(idx int) []interface{} {
				return []interface{}{idx, 0.1}
			},
		},
		{
			prepare: "update t3 set a = a + 1000 where a = ? and sleep(?)",
			argsGen: func(idx int) []interface{} {
				return []interface{}{idx, 0.1}
			},
		},
		{
			// test for point-get
			prepare: "select a, sleep(?) from t3 where a = ?",
			argsGen: func(idx int) []interface{} {
				return []interface{}{0.1, idx}
			},
		},
		{
			prepare: "select a, sleep(?) from t3 where b = ?",
			argsGen: func(idx int) []interface{} {
				return []interface{}{0.1, idx}
			},
		},
		{
			prepare: "delete from t3 where sleep(?) and a = ?",
			argsGen: func(idx int) []interface{} {
				return []interface{}{0.1, idx}
			},
		},
		{
			prepare: "insert into t3 values (?, sleep(?)) on duplicate key update b = b+1",
			argsGen: func(idx int) []interface{} {
				return []interface{}{idx, 0.1}
			},
		},
		{
			prepare: "set global tidb_enable_1pc = (? = sleep(?))",
			argsGen: func(idx int) []interface{} {
				return []interface{}{0, 0.1}
			},
		},
	}
	for _, ca := range cases3 {
		_, digest := parser.NormalizeDigest(ca.prepare)
		sqlDigests[stmtstats.BinaryDigest(digest.Bytes())] = ca.prepare
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, ca := range cases3 {
			db, err := sql.Open("mysql", ts.getDSN())
			require.NoError(t, err)
			dbt := testkit.NewDBTestKit(t, db)
			dbt.MustExec("use stmtstats;")
			// prepare stmt
			stmt, err := db.Prepare(ca.prepare)
			require.NoError(t, err)
			for n := 0; n < ExecCountPerSQL; n++ {
				args := ca.argsGen(n)
				if strings.HasPrefix(strings.ToLower(ca.prepare), "select") {
					row, err := stmt.Query(args...)
					require.NoError(t, err)
					err = row.Close()
					require.NoError(t, err)
				} else {
					_, err := stmt.Exec(args...)
					require.NoError(t, err)
				}
			}
			err = db.Close()
			require.NoError(t, err)
		}
	}()

	wg.Wait()
	// Wait for collect.
	waitCollected(collectedNotifyCh)

	found := 0
	for digest, item := range total {
		if sqlStr, ok := sqlDigests[digest.SQLDigest]; ok {
			found++
			require.Equal(t, uint64(ExecCountPerSQL), item.ExecCount, sqlStr)
			require.Equal(t, uint64(ExecCountPerSQL), item.DurationCount, sqlStr)
			require.True(t, item.SumDurationNs > uint64(time.Millisecond*100*ExecCountPerSQL), sqlStr)
			require.True(t, item.SumDurationNs < uint64(time.Millisecond*300*ExecCountPerSQL), sqlStr)
			if strings.HasPrefix(sqlStr, "set global") {
				// set global statement use internal SQL to change global variable, so itself doesn't have KV request.
				continue
			}
			var kvSum uint64
			for _, kvCount := range item.KvStatsItem.KvExecCount {
				kvSum += kvCount
			}
			require.Equal(t, uint64(ExecCountPerSQL), kvSum)
			tagChecker.checkExist(t, digest.SQLDigest, sqlStr)
		}
	}
	require.Equal(t, len(sqlDigests), found)
	require.Equal(t, 20, found)
}

type resourceTagChecker struct {
	sync.Mutex
	sqlDigest2Reqs map[stmtstats.BinaryDigest]map[tikvrpc.CmdType]struct{}
}

func (c *resourceTagChecker) checkExist(t *testing.T, digest stmtstats.BinaryDigest, sqlStr string) {
	if strings.HasPrefix(sqlStr, "set global") {
		// `set global` statement will use another internal sql to execute, so `set global` statement won't
		// send RPC request.
		return
	}
	if strings.HasPrefix(sqlStr, "trace") {
		// `trace` statement will use another internal sql to execute, so remove the `trace` prefix before check.
		_, sqlDigest := parser.NormalizeDigest(strings.TrimPrefix(sqlStr, "trace"))
		digest = stmtstats.BinaryDigest(sqlDigest.Bytes())
	}

	c.Lock()
	defer c.Unlock()
	_, ok := c.sqlDigest2Reqs[digest]
	require.True(t, ok, sqlStr)
}

func (c *resourceTagChecker) checkReqExist(t *testing.T, digest stmtstats.BinaryDigest, sqlStr string, reqs ...tikvrpc.CmdType) {
	if len(reqs) == 0 {
		return
	}
	c.Lock()
	defer c.Unlock()
	reqMap, ok := c.sqlDigest2Reqs[digest]
	require.True(t, ok, sqlStr)
	for _, req := range reqs {
		_, ok := reqMap[req]
		require.True(t, ok, fmt.Sprintf("sql: %v, expect: %v, got: %v", sqlStr, reqs, reqMap))
	}
}

func setupForTestTopSQLStatementStats(t *testing.T) (*tidbTestSuite, stmtstats.StatementStatsMap, *resourceTagChecker, chan struct{}) {
	// Prepare stmt stats.
	stmtstats.SetupAggregator()

	// Register stmt stats collector.
	var mu sync.Mutex
	collectedNotifyCh := make(chan struct{})
	total := stmtstats.StatementStatsMap{}
	mockCollector := newMockCollector(func(data stmtstats.StatementStatsMap) {
		mu.Lock()
		defer mu.Unlock()
		total.Merge(data)
		select {
		case collectedNotifyCh <- struct{}{}:
		default:
		}
	})
	stmtstats.RegisterCollector(mockCollector)

	ts := createTidbTestSuite(t)

	db, err := sql.Open("mysql", ts.getDSN())
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/skipLoadSysVarCacheLoop", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/unistoreRPCClientSendHook", `return(true)`))

	dbt := testkit.NewDBTestKit(t, db)
	dbt.MustExec("drop database if exists stmtstats")
	dbt.MustExec("create database stmtstats")
	dbt.MustExec("use stmtstats;")
	dbt.MustExec("create table t (a int, b int, unique index idx(a));")
	dbt.MustExec("create table t2 (a int, b int, unique index idx(a));")
	dbt.MustExec("create table t3 (a int, b int, unique index idx(a));")

	// Enable TopSQL
	topsqlstate.EnableTopSQL()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = "mock-agent"
	})

	tagChecker := &resourceTagChecker{
		sqlDigest2Reqs: make(map[stmtstats.BinaryDigest]map[tikvrpc.CmdType]struct{}),
	}
	unistore.UnistoreRPCClientSendHook = func(req *tikvrpc.Request) {
		tag := req.GetResourceGroupTag()
		if len(tag) == 0 || ddlutil.IsInternalResourceGroupTaggerForTopSQL(tag) {
			// Ignore for internal background request.
			return
		}
		sqlDigest, err := resourcegrouptag.DecodeResourceGroupTag(tag)
		require.NoError(t, err)
		tagChecker.Lock()
		defer tagChecker.Unlock()

		reqMap, ok := tagChecker.sqlDigest2Reqs[stmtstats.BinaryDigest(sqlDigest)]
		if !ok {
			reqMap = make(map[tikvrpc.CmdType]struct{})
		}
		reqMap[req.Type] = struct{}{}
		tagChecker.sqlDigest2Reqs[stmtstats.BinaryDigest(sqlDigest)] = reqMap
	}

	t.Cleanup(func() {
		stmtstats.UnregisterCollector(mockCollector)
		err = failpoint.Disable("github.com/pingcap/tidb/domain/skipLoadSysVarCacheLoop")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/unistoreRPCClientSendHook")
		require.NoError(t, err)
		stmtstats.CloseAggregator()
		view.Stop()
	})

	return ts, total, tagChecker, collectedNotifyCh
}

func TestTopSQLStatementStats2(t *testing.T) {
	ts, total, tagChecker, collectedNotifyCh := setupForTestTopSQLStatementStats(t)

	const ExecCountPerSQL = 3
	sqlDigests := map[stmtstats.BinaryDigest]string{}

	// Test case for other statements
	cases4 := []struct {
		sql     string
		plan    string
		isQuery bool
	}{
		{"insert into t () values (),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),()", "", false},
		{"analyze table t", "", false},
		{"explain analyze select sum(a+b) from t", ".*TableReader.*", true},
		{"trace select sum(b*a), sum(a+b) from t", "", true},
		{"set global tidb_stmt_summary_history_size=5;", "", false},
		{"select * from stmtstats.t where exists (select 1 from stmtstats.t2 where t2.a = 1);", ".*TableReader.*", true},
	}
	executeCaseFn := func(execFn func(db *sql.DB)) {
		db, err := sql.Open("mysql", ts.getDSN())
		require.NoError(t, err)
		dbt := testkit.NewDBTestKit(t, db)
		dbt.MustExec("use stmtstats;")
		require.NoError(t, err)

		for n := 0; n < ExecCountPerSQL; n++ {
			execFn(db)
		}
		err = db.Close()
		require.NoError(t, err)
	}
	execFn := func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		for _, ca := range cases4 {
			if ca.isQuery {
				mustQuery(t, dbt, ca.sql)
			} else {
				dbt.MustExec(ca.sql)
			}
		}
	}
	for _, ca := range cases4 {
		_, digest := parser.NormalizeDigest(ca.sql)
		sqlDigests[stmtstats.BinaryDigest(digest.Bytes())] = ca.sql
	}
	executeCaseFn(execFn)

	// Test case for multi-statement.
	cases5 := []string{
		"delete from t limit 1;",
		"update t set b=1 where b is null limit 1;",
		"select sum(a+b*2) from t;",
	}
	multiStatement5 := strings.Join(cases5, "")
	// Test case for multi-statement, but first statements execute failed
	cases6 := []string{
		"delete from t6_not_exist;",
		"update t set a=1 where a is null limit 1;",
	}
	multiStatement6 := strings.Join(cases6, "")
	// Test case for multi-statement, the first statements execute success but the second statement execute failed.
	cases7 := []string{
		"update t set a=1 where a <0 limit 1;",
		"delete from t7_not_exist;",
	}
	// Test case for DDL.
	cases8 := []string{
		"create table if not exists t10 (a int, b int)",
		"alter table t drop index if exists idx_b",
		"alter table t add index idx_b (b)",
	}
	multiStatement7 := strings.Join(cases7, "")
	execFn = func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		dbt.MustExec("SET tidb_multi_statement_mode='ON'")
		dbt.MustExec(multiStatement5)

		_, err := db.Exec(multiStatement6)
		require.NotNil(t, err)
		require.Equal(t, "Error 1146 (42S02): Table 'stmtstats.t6_not_exist' doesn't exist", err.Error())

		_, err = db.Exec(multiStatement7)
		require.NotNil(t, err)
		require.Equal(t, "Error 1146 (42S02): Table 'stmtstats.t7_not_exist' doesn't exist", err.Error())

		for _, ca := range cases8 {
			dbt.MustExec(ca)
		}
	}
	executeCaseFn(execFn)
	sqlStrs := append([]string{}, cases5...)
	sqlStrs = append(sqlStrs, cases7[0])
	sqlStrs = append(sqlStrs, cases8...)
	for _, sqlStr := range sqlStrs {
		_, digest := parser.NormalizeDigest(sqlStr)
		sqlDigests[stmtstats.BinaryDigest(digest.Bytes())] = sqlStr
	}

	// Wait for collect.
	waitCollected(collectedNotifyCh)

	foundMap := map[stmtstats.BinaryDigest]string{}
	for digest, item := range total {
		if sqlStr, ok := sqlDigests[digest.SQLDigest]; ok {
			require.Equal(t, uint64(ExecCountPerSQL), item.ExecCount, sqlStr)
			require.True(t, item.SumDurationNs > 1, sqlStr)
			foundMap[digest.SQLDigest] = sqlStr
			tagChecker.checkExist(t, digest.SQLDigest, sqlStr)
			// The special check uses to test the issue #33202.
			if strings.Contains(strings.ToLower(sqlStr), "add index") {
				tagChecker.checkReqExist(t, digest.SQLDigest, sqlStr, tikvrpc.CmdScan)
			}
		}
	}
	require.Equal(t, len(sqlDigests), len(foundMap), fmt.Sprintf("%v !=\n %v", sqlDigests, foundMap))
}

func TestTopSQLStatementStats3(t *testing.T) {
	ts, total, tagChecker, collectedNotifyCh := setupForTestTopSQLStatementStats(t)

	err := failpoint.Enable("github.com/pingcap/tidb/executor/mockSleepInTableReaderNext", "return(2000)")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/executor/mockSleepInTableReaderNext")
	}()

	cases := []string{
		"select count(a+b) from stmtstats.t",
		"select * from stmtstats.t where b is null",
		"update stmtstats.t set b = 1 limit 10",
		"delete from stmtstats.t limit 1",
	}
	var wg sync.WaitGroup
	sqlDigests := map[stmtstats.BinaryDigest]string{}
	for _, ca := range cases {
		wg.Add(1)
		go func(sqlStr string) {
			defer wg.Done()
			db, err := sql.Open("mysql", ts.getDSN())
			require.NoError(t, err)
			dbt := testkit.NewDBTestKit(t, db)
			require.NoError(t, err)
			if strings.HasPrefix(sqlStr, "select") {
				mustQuery(t, dbt, sqlStr)
			} else {
				dbt.MustExec(sqlStr)
			}
			err = db.Close()
			require.NoError(t, err)
		}(ca)
		_, digest := parser.NormalizeDigest(ca)
		sqlDigests[stmtstats.BinaryDigest(digest.Bytes())] = ca
	}
	// Wait for collect.
	waitCollected(collectedNotifyCh)

	foundMap := map[stmtstats.BinaryDigest]string{}
	for digest, item := range total {
		if sqlStr, ok := sqlDigests[digest.SQLDigest]; ok {
			// since the SQL doesn't execute finish, the ExecCount should be recorded,
			// but the DurationCount and SumDurationNs should be 0.
			require.Equal(t, uint64(1), item.ExecCount, sqlStr)
			require.Equal(t, uint64(0), item.DurationCount, sqlStr)
			require.Equal(t, uint64(0), item.SumDurationNs, sqlStr)
			foundMap[digest.SQLDigest] = sqlStr
		}
	}

	// wait sql execute finish.
	wg.Wait()
	// Wait for collect.
	waitCollected(collectedNotifyCh)

	for digest, item := range total {
		if sqlStr, ok := sqlDigests[digest.SQLDigest]; ok {
			require.Equal(t, uint64(1), item.ExecCount, sqlStr)
			require.Equal(t, uint64(1), item.DurationCount, sqlStr)
			require.Less(t, uint64(0), item.SumDurationNs, sqlStr)
			foundMap[digest.SQLDigest] = sqlStr
			tagChecker.checkExist(t, digest.SQLDigest, sqlStr)
		}
	}
}

func TestTopSQLStatementStats4(t *testing.T) {
	ts, total, tagChecker, collectedNotifyCh := setupForTestTopSQLStatementStats(t)

	err := failpoint.Enable("github.com/pingcap/tidb/executor/mockSleepInTableReaderNext", "return(2000)")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/executor/mockSleepInTableReaderNext")
	}()

	cases := []struct {
		prepare string
		sql     string
		args    []interface{}
	}{
		{prepare: "select count(a+b) from stmtstats.t", sql: "select count(a+b) from stmtstats.t"},
		{prepare: "select * from stmtstats.t where b is null", sql: "select * from stmtstats.t where b is null"},
		{prepare: "update stmtstats.t set b = ? limit ?", sql: "update stmtstats.t set b = 1 limit 10", args: []interface{}{1, 10}},
		{prepare: "delete from stmtstats.t limit ?", sql: "delete from stmtstats.t limit 1", args: []interface{}{1}},
	}
	var wg sync.WaitGroup
	sqlDigests := map[stmtstats.BinaryDigest]string{}
	for _, ca := range cases {
		wg.Add(1)
		go func(prepare string, args []interface{}) {
			defer wg.Done()
			db, err := sql.Open("mysql", ts.getDSN())
			require.NoError(t, err)
			stmt, err := db.Prepare(prepare)
			require.NoError(t, err)
			if strings.HasPrefix(prepare, "select") {
				rows, err := stmt.Query(args...)
				require.NoError(t, err)
				for rows.Next() {
				}
				err = rows.Close()
				require.NoError(t, err)
			} else {
				_, err := stmt.Exec(args...)
				require.NoError(t, err)
			}
			err = db.Close()
			require.NoError(t, err)
		}(ca.prepare, ca.args)
		_, digest := parser.NormalizeDigest(ca.sql)
		sqlDigests[stmtstats.BinaryDigest(digest.Bytes())] = ca.sql
	}
	// Wait for collect.
	waitCollected(collectedNotifyCh)

	foundMap := map[stmtstats.BinaryDigest]string{}
	for digest, item := range total {
		if sqlStr, ok := sqlDigests[digest.SQLDigest]; ok {
			// since the SQL doesn't execute finish, the ExecCount should be recorded,
			// but the DurationCount and SumDurationNs should be 0.
			require.Equal(t, uint64(1), item.ExecCount, sqlStr)
			require.Equal(t, uint64(0), item.DurationCount, sqlStr)
			require.Equal(t, uint64(0), item.SumDurationNs, sqlStr)
			foundMap[digest.SQLDigest] = sqlStr
		}
	}

	// wait sql execute finish.
	wg.Wait()
	// Wait for collect.
	waitCollected(collectedNotifyCh)

	for digest, item := range total {
		if sqlStr, ok := sqlDigests[digest.SQLDigest]; ok {
			require.Equal(t, uint64(1), item.ExecCount, sqlStr)
			require.Equal(t, uint64(1), item.DurationCount, sqlStr)
			require.Less(t, uint64(0), item.SumDurationNs, sqlStr)
			foundMap[digest.SQLDigest] = sqlStr
			tagChecker.checkExist(t, digest.SQLDigest, sqlStr)
		}
	}
}

func TestTopSQLResourceTag(t *testing.T) {
	ts, _, tagChecker, _ := setupForTestTopSQLStatementStats(t)
	defer func() {
		topsqlstate.DisableTopSQL()
	}()

	loadDataFile, err := os.CreateTemp("", "load_data_test0.csv")
	require.NoError(t, err)
	defer func() {
		path := loadDataFile.Name()
		err = loadDataFile.Close()
		require.NoError(t, err)
		err = os.Remove(path)
		require.NoError(t, err)
	}()
	_, err = loadDataFile.WriteString(
		"31	31\n" +
			"32	32\n" +
			"33	33\n")
	require.NoError(t, err)

	// Test case for other statements
	cases := []struct {
		sql     string
		isQuery bool
		reqs    []tikvrpc.CmdType
	}{
		// Test for curd.
		{"insert into t values (1,1), (3,3)", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit}},
		{"insert into t values (1,2) on duplicate key update a = 2", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit, tikvrpc.CmdBatchGet}},
		{"update t set b=b+1 where a=3", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit, tikvrpc.CmdGet}},
		{"update t set b=b+1 where a>1", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit, tikvrpc.CmdCop}},
		{"delete from t where a=3", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit, tikvrpc.CmdGet}},
		{"delete from t where a>1", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit, tikvrpc.CmdCop}},
		{"insert ignore into t values (2,2), (3,3)", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit, tikvrpc.CmdBatchGet}},
		{"select * from t where a in (1,2,3,4)", true, []tikvrpc.CmdType{tikvrpc.CmdBatchGet}},
		{"select * from t where a = 1", true, []tikvrpc.CmdType{tikvrpc.CmdGet}},
		{"select * from t where b > 0", true, []tikvrpc.CmdType{tikvrpc.CmdCop}},
		{"replace into t values (2,2), (4,4)", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit, tikvrpc.CmdBatchGet}},

		// Test for DDL
		{"create database test_db0", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit}},
		{"create table test_db0.test_t0 (a int, b int, index idx(a))", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit}},
		{"create table test_db0.test_t1 (a int, b int, index idx(a))", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit}},
		{"alter  table test_db0.test_t0 add column c int", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit}},
		{"drop   table test_db0.test_t0", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit}},
		{"drop   database test_db0", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit}},
		{"alter  table t modify column b double", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit, tikvrpc.CmdScan, tikvrpc.CmdCop}},
		{"alter  table t add index idx2 (b,a)", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit, tikvrpc.CmdScan, tikvrpc.CmdCop}},
		{"alter  table t drop index idx2", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit}},

		// Test for transaction
		{"begin", false, nil},
		{"insert into t2 values (10,10), (11,11)", false, nil},
		{"insert ignore into t2 values (20,20), (21,21)", false, []tikvrpc.CmdType{tikvrpc.CmdBatchGet}},
		{"commit", false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit}},

		// Test for other statements.
		{"set @@global.tidb_enable_1pc = 1", false, nil},
		{fmt.Sprintf("load data local infile %q into table t2", loadDataFile.Name()), false, []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit, tikvrpc.CmdBatchGet}},
		{"admin check table t", false, []tikvrpc.CmdType{tikvrpc.CmdCop}},
		{"admin check index t idx", false, []tikvrpc.CmdType{tikvrpc.CmdCop}},
		{"admin recover index t idx", false, []tikvrpc.CmdType{tikvrpc.CmdBatchGet}},
		{"admin cleanup index t idx", false, []tikvrpc.CmdType{tikvrpc.CmdBatchGet}},
	}

	internalCases := []struct {
		sql  string
		reqs []tikvrpc.CmdType
	}{
		{"replace into mysql.global_variables (variable_name,variable_value) values ('tidb_enable_1pc', '1')", []tikvrpc.CmdType{tikvrpc.CmdPrewrite, tikvrpc.CmdCommit, tikvrpc.CmdBatchGet}},
	}
	executeCaseFn := func(execFn func(db *sql.DB)) {
		dsn := ts.getDSN(func(config *mysql.Config) {
			config.AllowAllFiles = true
			config.Params["sql_mode"] = "''"
		})
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		dbt := testkit.NewDBTestKit(t, db)
		dbt.MustExec("use stmtstats;")
		require.NoError(t, err)

		execFn(db)
		err = db.Close()
		require.NoError(t, err)
	}
	execFn := func(db *sql.DB) {
		dbt := testkit.NewDBTestKit(t, db)
		for _, ca := range cases {
			if ca.isQuery {
				mustQuery(t, dbt, ca.sql)
			} else {
				dbt.MustExec(ca.sql)
			}
		}
	}
	executeCaseFn(execFn)

	for _, ca := range cases {
		_, digest := parser.NormalizeDigest(ca.sql)
		tagChecker.checkReqExist(t, stmtstats.BinaryDigest(digest.Bytes()), ca.sql, ca.reqs...)
	}
	for _, ca := range internalCases {
		_, digest := parser.NormalizeDigest(ca.sql)
		tagChecker.checkReqExist(t, stmtstats.BinaryDigest(digest.Bytes()), ca.sql, ca.reqs...)
	}
}

func (ts *tidbTestTopSQLSuite) loopExec(ctx context.Context, t *testing.T, fn func(db *sql.DB)) {
	db, err := sql.Open("mysql", ts.getDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	dbt := testkit.NewDBTestKit(t, db)
	dbt.MustExec("use topsql;")
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		fn(db)
	}
}

func TestLocalhostClientMapping(t *testing.T) {
	tempDir := t.TempDir()
	socketFile := tempDir + "/tidbtest.sock" // Unix Socket does not work on Windows, so '/' should be OK

	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Socket = socketFile
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false

	ts := createTidbTestSuite(t)

	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	defer server.Close()
	cli.waitUntilServerCanConnect()

	cli.port = getPortFromTCPAddr(server.listener.Addr())
	// Create a db connection for root
	db, err := sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.DBName = "test"
		config.Addr = socketFile
	}))
	require.NoErrorf(t, err, "Open failed")
	err = db.Ping()
	require.NoErrorf(t, err, "Ping failed")
	defer db.Close()
	dbt := testkit.NewDBTestKit(t, db)
	rows := dbt.MustQuery("select user()")
	cli.checkRows(t, rows, "root@localhost")
	require.NoError(t, rows.Close())
	rows = dbt.MustQuery("show grants")
	cli.checkRows(t, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
	require.NoError(t, rows.Close())

	dbt.MustExec("CREATE USER 'localhostuser'@'localhost'")
	dbt.MustExec("CREATE USER 'localhostuser'@'%'")
	defer func() {
		dbt.MustExec("DROP USER IF EXISTS 'localhostuser'@'%'")
		dbt.MustExec("DROP USER IF EXISTS 'localhostuser'@'localhost'")
		dbt.MustExec("DROP USER IF EXISTS 'localhostuser'@'127.0.0.1'")
	}()

	dbt.MustExec("GRANT SELECT ON test.* TO 'localhostuser'@'%'")
	dbt.MustExec("GRANT SELECT,UPDATE ON test.* TO 'localhostuser'@'localhost'")

	// Test with loopback interface - Should get access to localhostuser@localhost!
	cli.runTests(t, func(config *mysql.Config) {
		config.User = "localhostuser"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			// NOTICE: this is not compatible with MySQL! (MySQL would report localhostuser@localhost also for 127.0.0.1)
			cli.checkRows(t, rows, "localhostuser@127.0.0.1")
			require.NoError(t, rows.Close())
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT USAGE ON *.* TO 'localhostuser'@'localhost'\nGRANT SELECT,UPDATE ON test.* TO 'localhostuser'@'localhost'")
			require.NoError(t, rows.Close())
		})

	dbt.MustExec("DROP USER IF EXISTS 'localhostuser'@'localhost'")
	dbt.MustExec("CREATE USER 'localhostuser'@'127.0.0.1'")
	dbt.MustExec("GRANT SELECT,UPDATE ON test.* TO 'localhostuser'@'127.0.0.1'")
	// Test with unix domain socket file connection - Should get access to '%'
	cli.runTests(t, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "localhostuser"
		config.DBName = "test"
	},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("select user()")
			cli.checkRows(t, rows, "localhostuser@localhost")
			require.NoError(t, rows.Close())
			rows = dbt.MustQuery("show grants")
			cli.checkRows(t, rows, "GRANT USAGE ON *.* TO 'localhostuser'@'%'\nGRANT SELECT ON test.* TO 'localhostuser'@'%'")
			require.NoError(t, rows.Close())
		})

	// Test if only localhost exists
	dbt.MustExec("DROP USER 'localhostuser'@'%'")
	dbSocket, err := sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "localhostuser"
		config.Net = "unix"
		config.DBName = "test"
		config.Addr = socketFile
	}))
	require.NoErrorf(t, err, "Open failed")
	defer dbSocket.Close()
	err = dbSocket.Ping()
	require.Errorf(t, err, "Connection successful without matching host for unix domain socket!")
}

func TestRcReadCheckTSConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)

	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
	}
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_rc_read_check_ts = ON")
	tk.RefreshSession()
	cc.setCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})

	tk.MustExec("use test")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	dml := "insert into t values"
	for i := 0; i < 50; i++ {
		dml += fmt.Sprintf("(%v, 0)", i)
		if i != 49 {
			dml += ","
		}
	}
	tk.MustExec(dml)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("50"))
	require.Equal(t, "ON", tk.MustQuery("show variables like 'tidb_rc_read_check_ts'").Rows()[0][1])

	ctx := context.Background()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/fetchNextErr", "return(\"secondNextAndRetConflict\")"))
	err := cc.handleQuery(ctx, "select * from t limit 20")
	require.NoError(t, err)

	err = cc.handleQuery(ctx, "select * from t t1 join t t2")
	require.Equal(t, kv.ErrWriteConflict, err)

	tk.MustExec("set session tidb_max_chunk_size = 4096")
	require.Equal(t, "4096", tk.MustQuery("show variables like 'tidb_max_chunk_size'").Rows()[0][1])
	err = cc.handleQuery(ctx, "select * from t t1 join t t2")
	require.NoError(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/fetchNextErr"))

	tk.MustExec("drop table t")
}

func TestRcReadCheckTSConflictExtra(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/sessiontxn/isolation/CallOnStmtRetry", "return"))
	defer func() {
		defer require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/sessiontxn/isolation/CallOnStmtRetry"))
	}()
	store := testkit.CreateMockStore(t)

	ctx := context.Background()
	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
	}
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_rc_read_check_ts = ON")

	se := tk.Session()
	cc.setCtx(&TiDBContext{Session: se, stmts: make(map[int]*TiDBStatement)})

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (10, 10, 10)")
	require.Equal(t, "ON", tk.MustQuery("show variables like 'tidb_rc_read_check_ts'").Rows()[0][1])

	tk.MustExec("set transaction_isolation = 'READ-COMMITTED'")
	tk2.MustExec("set transaction_isolation = 'READ-COMMITTED'")

	// Execute in text protocol
	se.SetValue(sessiontxn.CallOnStmtRetryCount, 0)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t1 set id3 = id3 + 1 where id1 = 1")
	err := cc.handleQuery(ctx, "select * from t1 where id1 = 1")
	require.NoError(t, err)
	tk.MustExec("commit")
	count, ok := se.Value(sessiontxn.CallOnStmtRetryCount).(int)
	require.Equal(t, true, ok)
	require.Equal(t, 1, count)

	// Execute in prepare binary protocol
	se.SetValue(sessiontxn.CallOnStmtRetryCount, 0)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t1 set id3 = id3 + 1 where id1 = 1")
	require.NoError(t, cc.handleStmtPrepare(ctx, "select * from t1 where id1 = 1"))
	require.NoError(t, cc.handleStmtExecute(ctx, []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0}))
	tk.MustExec("commit")
	count, ok = se.Value(sessiontxn.CallOnStmtRetryCount).(int)
	require.Equal(t, true, ok)
	require.Equal(t, 1, count)

	tk.MustExec("drop table t1")
}

func TestRcReadCheckTS(t *testing.T) {
	ts := createTidbTestSuite(t)

	db, err := sql.Open("mysql", ts.getDSN())
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	db2, err := sql.Open("mysql", ts.getDSN())
	require.NoError(t, err)
	defer func() {
		err := db2.Close()
		require.NoError(t, err)
	}()
	tk2 := testkit.NewDBTestKit(t, db2)
	tk2.MustExec("set @@tidb_enable_async_commit = 0")
	tk2.MustExec("set @@tidb_enable_1pc = 0")

	cli := newTestServerClient()

	tk := testkit.NewDBTestKit(t, db)
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int key, c2 int)")
	tk.MustExec("insert into t1 values(1, 10), (2, 20), (3, 30)")

	tk.MustExec(`set tidb_rc_read_check_ts = 'on';`)
	tk.MustExec(`set tx_isolation = 'READ-COMMITTED';`)
	tk.MustExec("begin pessimistic")
	// Test point get retry.
	rows := tk.MustQuery("select * from t1 where c1 = 1")
	cli.checkRows(t, rows, "1 10")
	tk2.MustExec("update t1 set c2 = c2 + 1")
	rows = tk.MustQuery("select * from t1 where c1 = 1")
	cli.checkRows(t, rows, "1 11")
	// Test batch point get retry.
	rows = tk.MustQuery("select * from t1 where c1 in (1, 3)")
	cli.checkRows(t, rows, "1 11", "3 31")
	tk2.MustExec("update t1 set c2 = c2 + 1")
	rows = tk.MustQuery("select * from t1 where c1 in (1, 3)")
	cli.checkRows(t, rows, "1 12", "3 32")
	// Test scan retry.
	rows = tk.MustQuery("select * from t1")
	cli.checkRows(t, rows, "1 12", "2 22", "3 32")
	tk2.MustExec("update t1 set c2 = c2 + 1")
	rows = tk.MustQuery("select * from t1")
	cli.checkRows(t, rows, "1 13", "2 23", "3 33")
	// Test reverse scan retry.
	rows = tk.MustQuery("select * from t1 order by c1 desc")
	cli.checkRows(t, rows, "3 33", "2 23", "1 13")
	tk2.MustExec("update t1 set c2 = c2 + 1")
	rows = tk.MustQuery("select * from t1 order by c1 desc")
	cli.checkRows(t, rows, "3 34", "2 24", "1 14")

	// Test retry caused by ongoing prewrite lock.
	// As the `defaultLockTTL` is 3s and it's difficult to change it here, the lock
	// test is implemented in the uft test cases.
}

type connEventLogs struct {
	sync.Mutex
	types []extension.ConnEventTp
	infos []extension.ConnEventInfo
}

func (l *connEventLogs) add(tp extension.ConnEventTp, info *extension.ConnEventInfo) {
	l.Lock()
	defer l.Unlock()
	l.types = append(l.types, tp)
	l.infos = append(l.infos, *info)
}

func (l *connEventLogs) reset() {
	l.Lock()
	defer l.Unlock()
	l.types = l.types[:0]
	l.infos = l.infos[:0]
}

func (l *connEventLogs) check(fn func()) {
	l.Lock()
	defer l.Unlock()
	fn()
}

func (l *connEventLogs) waitEvent(tp extension.ConnEventTp) error {
	totalSleep := 0
	for {
		l.Lock()
		if l.types[len(l.types)-1] == tp {
			l.Unlock()
			return nil
		}
		l.Unlock()
		if totalSleep >= 10000 {
			break
		}
		time.Sleep(time.Millisecond * 100)
		totalSleep += 100
	}
	return errors.New("timeout")
}

func TestExtensionConnEvent(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	logs := &connEventLogs{}
	require.NoError(t, extension.Register("test", extension.WithSessionHandlerFactory(func() *extension.SessionHandler {
		return &extension.SessionHandler{
			OnConnectionEvent: logs.add,
		}
	})))
	require.NoError(t, extension.Setup())

	ts := createTidbTestSuite(t)
	// createTidbTestSuite create an inner connection, so wait the previous connection closed
	require.NoError(t, logs.waitEvent(extension.ConnDisconnected))

	// test for login success
	logs.reset()
	db, err := sql.Open("mysql", ts.getDSN())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	var expectedConn2 variable.ConnectionInfo
	require.NoError(t, logs.waitEvent(extension.ConnHandshakeAccepted))
	logs.check(func() {
		require.Equal(t, []extension.ConnEventTp{
			extension.ConnConnected,
			extension.ConnHandshakeAccepted,
		}, logs.types)
		conn1 := logs.infos[0]
		require.Equal(t, "127.0.0.1", conn1.ClientIP)
		require.Equal(t, "127.0.0.1", conn1.ServerIP)
		require.Empty(t, conn1.User)
		require.Empty(t, conn1.DB)
		require.Equal(t, int(ts.port), conn1.ServerPort)
		require.NotEqual(t, conn1.ServerPort, conn1.ClientPort)
		require.NotEmpty(t, conn1.ConnectionID)
		require.Nil(t, conn1.ActiveRoles)
		require.NoError(t, conn1.Error)

		expectedConn2 = *(conn1.ConnectionInfo)
		expectedConn2.User = "root"
		expectedConn2.DB = "test"
		require.Equal(t, []*auth.RoleIdentity{}, logs.infos[1].ActiveRoles)
		require.Nil(t, logs.infos[1].Error)
		require.Equal(t, expectedConn2, *(logs.infos[1].ConnectionInfo))
	})

	_, err = conn.ExecContext(context.TODO(), "create role r1@'%'")
	require.NoError(t, err)
	_, err = conn.ExecContext(context.TODO(), "grant r1 TO root")
	require.NoError(t, err)
	_, err = conn.ExecContext(context.TODO(), "set role all")
	require.NoError(t, err)

	require.NoError(t, conn.Close())
	require.NoError(t, db.Close())
	require.NoError(t, logs.waitEvent(extension.ConnDisconnected))
	logs.check(func() {
		require.Equal(t, 3, len(logs.infos))
		require.Equal(t, 1, len(logs.infos[2].ActiveRoles))
		require.Equal(t, auth.RoleIdentity{
			Username: "r1",
			Hostname: "%",
		}, *logs.infos[2].ActiveRoles[0])
		require.Nil(t, logs.infos[2].Error)
		require.Equal(t, expectedConn2, *(logs.infos[2].ConnectionInfo))
	})

	// test for login failed
	logs.reset()
	cfg := mysql.NewConfig()
	cfg.User = "noexist"
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("127.0.0.1:%d", ts.port)
	cfg.DBName = "test"

	db, err = sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	_, err = db.Conn(context.Background())
	require.Error(t, err)
	require.NoError(t, logs.waitEvent(extension.ConnDisconnected))
	logs.check(func() {
		require.Equal(t, []extension.ConnEventTp{
			extension.ConnConnected,
			extension.ConnHandshakeRejected,
			extension.ConnDisconnected,
		}, logs.types)
		conn1 := logs.infos[0]
		require.Equal(t, "127.0.0.1", conn1.ClientIP)
		require.Equal(t, "127.0.0.1", conn1.ServerIP)
		require.Empty(t, conn1.User)
		require.Empty(t, conn1.DB)
		require.Equal(t, int(ts.port), conn1.ServerPort)
		require.NotEqual(t, conn1.ServerPort, conn1.ClientPort)
		require.NotEmpty(t, conn1.ConnectionID)
		require.Nil(t, conn1.ActiveRoles)
		require.NoError(t, conn1.Error)

		expectedConn2 = *(conn1.ConnectionInfo)
		expectedConn2.User = "noexist"
		expectedConn2.DB = "test"
		require.Equal(t, []*auth.RoleIdentity{}, logs.infos[1].ActiveRoles)
		require.EqualError(t, logs.infos[1].Error, "[server:1045]Access denied for user 'noexist'@'127.0.0.1' (using password: NO)")
		require.Equal(t, expectedConn2, *(logs.infos[1].ConnectionInfo))
	})
}

func TestSandBoxMode(t *testing.T) {
	ts := createTidbTestSuite(t)
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil, nil)
	require.NoError(t, err)
	_, err = Execute(context.Background(), qctx, "create user testuser;")
	require.NoError(t, err)
	qctx.Session.GetSessionVars().User = &auth.UserIdentity{Username: "testuser", AuthUsername: "testuser", AuthHostname: "%"}

	alterPwdStmts := []string{
		"set password = '1234';",
		"alter user testuser identified by '1234';",
		"alter user current_user() identified by '1234';",
	}

	for _, alterPwdStmt := range alterPwdStmts {
		require.False(t, qctx.Session.InSandBoxMode())
		_, err = Execute(context.Background(), qctx, "select 1;")
		require.NoError(t, err)

		qctx.Session.EnableSandBoxMode()
		require.True(t, qctx.Session.InSandBoxMode())
		_, err = Execute(context.Background(), qctx, "select 1;")
		require.Error(t, err)
		_, err = Execute(context.Background(), qctx, "alter user testuser identified with 'mysql_native_password';")
		require.Error(t, err)
		_, err = Execute(context.Background(), qctx, alterPwdStmt)
		require.NoError(t, err)
		_, err = Execute(context.Background(), qctx, "select 1;")
		require.NoError(t, err)
	}
}

// See: https://github.com/pingcap/tidb/issues/40979
// Reusing memory of `chunk.Chunk` may cause some systems variable's memory value to be modified unexpectedly.
func TestChunkReuseCorruptSysVarString(t *testing.T) {
	ts := createTidbTestSuite(t)

	db, err := sql.Open("mysql", ts.getDSN())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	rs, err := conn.QueryContext(context.Background(), "show tables in test")
	ts.Rows(t, rs)
	require.NoError(t, err)

	_, err = conn.ExecContext(context.Background(), "set @@time_zone=(select 'Asia/Shanghai')")
	require.NoError(t, err)

	rs, err = conn.QueryContext(context.Background(), "select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='aaaa'")
	ts.Rows(t, rs)
	require.NoError(t, err)

	rs, err = conn.QueryContext(context.Background(), "select @@time_zone")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rs.Close())
	}()

	rows := ts.Rows(t, rs)
	require.Equal(t, 1, len(rows))
	require.Equal(t, "Asia/Shanghai", rows[0])
}

type mockProxyProtocolProxy struct {
	frontend      string
	backend       string
	clientAddr    string
	backendIsSock bool
	ln            net.Listener
	run           bool
}

func newMockProxyProtocolProxy(frontend, backend, clientAddr string, backendIsSock bool) *mockProxyProtocolProxy {
	return &mockProxyProtocolProxy{
		frontend:      frontend,
		backend:       backend,
		clientAddr:    clientAddr,
		backendIsSock: backendIsSock,
		ln:            nil,
		run:           false,
	}
}

func (p *mockProxyProtocolProxy) ListenAddr() net.Addr {
	return p.ln.Addr()
}

func (p *mockProxyProtocolProxy) Run() (err error) {
	p.run = true
	p.ln, err = net.Listen("tcp", p.frontend)
	if err != nil {
		return err
	}
	for p.run {
		conn, err := p.ln.Accept()
		if err != nil {
			break
		}
		go p.onConn(conn)
	}
	return nil
}

func (p *mockProxyProtocolProxy) Close() error {
	p.run = false
	if p.ln != nil {
		return p.ln.Close()
	}
	return nil
}

func (p *mockProxyProtocolProxy) connectToBackend() (net.Conn, error) {
	if p.backendIsSock {
		return net.Dial("unix", p.backend)
	}
	return net.Dial("tcp", p.backend)
}

func (p *mockProxyProtocolProxy) onConn(conn net.Conn) {
	bconn, err := p.connectToBackend()
	if err != nil {
		conn.Close()
		fmt.Println(err)
	}
	defer bconn.Close()
	ppHeader := p.generateProxyProtocolHeaderV2("tcp4", p.clientAddr, p.frontend)
	bconn.Write(ppHeader)
	p.proxyPipe(conn, bconn)
}

func (p *mockProxyProtocolProxy) proxyPipe(p1, p2 io.ReadWriteCloser) {
	defer p1.Close()
	defer p2.Close()

	// start proxy
	p1die := make(chan struct{})
	go func() { io.Copy(p1, p2); close(p1die) }()

	p2die := make(chan struct{})
	go func() { io.Copy(p2, p1); close(p2die) }()

	// wait for proxy termination
	select {
	case <-p1die:
	case <-p2die:
	}
}

func (p *mockProxyProtocolProxy) generateProxyProtocolHeaderV2(network, srcAddr, dstAddr string) []byte {
	var (
		proxyProtocolV2Sig = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}
		v2CmdPos           = 12
		v2FamlyPos         = 13
	)
	saddr, _ := net.ResolveTCPAddr(network, srcAddr)
	daddr, _ := net.ResolveTCPAddr(network, dstAddr)
	buffer := make([]byte, 1024)
	copy(buffer, proxyProtocolV2Sig)
	// Command
	buffer[v2CmdPos] = 0x21
	// Famly
	if network == "tcp4" {
		buffer[v2FamlyPos] = 0x11
		binary.BigEndian.PutUint16(buffer[14:14+2], 12)
		copy(buffer[16:16+4], []byte(saddr.IP.To4()))
		copy(buffer[20:20+4], []byte(daddr.IP.To4()))
		binary.BigEndian.PutUint16(buffer[24:24+2], uint16(saddr.Port))
		binary.BigEndian.PutUint16(buffer[26:26+2], uint16(saddr.Port))
		return buffer[0:28]
	} else if network == "tcp6" {
		buffer[v2FamlyPos] = 0x21
		binary.BigEndian.PutUint16(buffer[14:14+2], 36)
		copy(buffer[16:16+16], []byte(saddr.IP.To16()))
		copy(buffer[32:32+16], []byte(daddr.IP.To16()))
		binary.BigEndian.PutUint16(buffer[48:48+2], uint16(saddr.Port))
		binary.BigEndian.PutUint16(buffer[50:50+2], uint16(saddr.Port))
		return buffer[0:52]
	}
	return buffer
}

func TestProxyProtocolWithIpFallbackable(t *testing.T) {
	cfg := newTestConfig()
	cfg.Port = 4999
	cfg.Status.ReportStatus = false
	// Setup proxy protocol config
	cfg.ProxyProtocol.Networks = "*"
	cfg.ProxyProtocol.Fallbackable = true

	ts := createTidbTestSuite(t)

	// Prepare Server
	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 100)
	defer func() {
		server.Close()
	}()

	require.NotNil(t, server.listener)
	require.Nil(t, server.socket)

	// Prepare Proxy
	ppProxy := newMockProxyProtocolProxy("127.0.0.1:5000", "127.0.0.1:4999", "192.168.1.2:60055", false)
	go func() {
		ppProxy.Run()
	}()
	time.Sleep(time.Millisecond * 100)
	defer func() {
		ppProxy.Close()
	}()

	cli := newTestServerClient()
	cli.port = getPortFromTCPAddr(ppProxy.ListenAddr())
	cli.waitUntilServerCanConnect()

	cli.runTests(t,
		func(config *mysql.Config) {
			config.User = "root"
		},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("SHOW PROCESSLIST;")
			records := cli.Rows(t, rows)
			require.Contains(t, records[0], "192.168.1.2:60055")
		},
	)

	cli2 := newTestServerClient()
	cli2.port = 4999
	cli2.runTests(t,
		func(config *mysql.Config) {
			config.User = "root"
		},
		func(dbt *testkit.DBTestKit) {
			rows := dbt.MustQuery("SHOW PROCESSLIST;")
			records := cli.Rows(t, rows)
			require.Contains(t, records[0], "127.0.0.1:")
		},
	)
}

func TestProxyProtocolWithIpNoFallbackable(t *testing.T) {
	cfg := newTestConfig()
	cfg.Port = 4000
	cfg.Status.ReportStatus = false
	// Setup proxy protocol config
	cfg.ProxyProtocol.Networks = "*"
	cfg.ProxyProtocol.Fallbackable = false

	ts := createTidbTestSuite(t)

	// Prepare Server
	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 1000)
	defer func() {
		server.Close()
	}()

	require.NotNil(t, server.listener)
	require.Nil(t, server.socket)

	cli := newTestServerClient()
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	dsn := cli.getDSN(func(config *mysql.Config) {
		config.User = "root"
		config.DBName = "test"
	})
	db, err := sql.Open("mysql", dsn)
	require.Nil(t, err)
	err = db.Ping()
	require.NotNil(t, err)
	db.Close()
}
