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
// See the License for the specific language governing permissions and
// limitations under the License.
// +build !race

package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/topsql/reporter"
	mockTopSQLReporter "github.com/pingcap/tidb/util/topsql/reporter/mock"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	mockTopSQLTraceCPU "github.com/pingcap/tidb/util/topsql/tracecpu/mock"
)

type tidbTestSuite struct {
	*tidbTestSuiteBase
}

type tidbTestSerialSuite struct {
	*tidbTestSuiteBase
}

type tidbTestTopSQLSuite struct {
	*tidbTestSuiteBase
}

type tidbTestSuiteBase struct {
	*testServerClient
	tidbdrv *TiDBDriver
	server  *Server
	domain  *domain.Domain
	store   kv.Storage
}

func newTiDBTestSuiteBase() *tidbTestSuiteBase {
	return &tidbTestSuiteBase{
		testServerClient: newTestServerClient(),
	}
}

var _ = Suite(&tidbTestSuite{newTiDBTestSuiteBase()})
var _ = SerialSuites(&tidbTestSerialSuite{newTiDBTestSuiteBase()})
var _ = SerialSuites(&tidbTestTopSQLSuite{newTiDBTestSuiteBase()})

func (ts *tidbTestSuite) SetUpSuite(c *C) {
	metrics.RegisterMetrics()
	ts.tidbTestSuiteBase.SetUpSuite(c)
}

func (ts *tidbTestSuite) TearDownSuite(c *C) {
	ts.tidbTestSuiteBase.TearDownSuite(c)
}

func (ts *tidbTestTopSQLSuite) SetUpSuite(c *C) {
	ts.tidbTestSuiteBase.SetUpSuite(c)

	// Initialize global variable for top-sql test.
	db, err := sql.Open("mysql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer func() {
		err := db.Close()
		c.Assert(err, IsNil)
	}()

	dbt := &DBTest{c, db}
	dbt.mustExec("set @@global.tidb_top_sql_precision_seconds=1;")
	dbt.mustExec("set @@global.tidb_top_sql_report_interval_seconds=2;")
	dbt.mustExec("set @@global.tidb_top_sql_max_statement_count=5;")

	tracecpu.GlobalSQLCPUProfiler.Run()
}

func (ts *tidbTestTopSQLSuite) TearDownSuite(c *C) {
	ts.tidbTestSuiteBase.TearDownSuite(c)
}

func (ts *tidbTestSuiteBase) SetUpSuite(c *C) {
	var err error
	ts.store, err = mockstore.NewMockStore()
	session.DisableStats4Test()
	c.Assert(err, IsNil)
	ts.domain, err = session.BootstrapSession(ts.store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(ts.store)
	cfg := newTestConfig()
	cfg.Port = ts.port
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = ts.statusPort
	cfg.Performance.TCPKeepAlive = true
	err = logutil.InitLogger(cfg.Log.ToLogConfig())
	c.Assert(err, IsNil)

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.port = getPortFromTCPAddr(server.listener.Addr())
	ts.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	ts.server = server
	go func() {
		err := ts.server.Run()
		c.Assert(err, IsNil)
	}()
	ts.waitUntilServerOnline()
}

func (ts *tidbTestSuiteBase) TearDownSuite(c *C) {
	if ts.store != nil {
		ts.store.Close()
	}
	if ts.domain != nil {
		ts.domain.Close()
	}
	if ts.server != nil {
		ts.server.Close()
	}
}

func (ts *tidbTestSuite) TestRegression(c *C) {
	if regression {
		c.Parallel()
		ts.runTestRegression(c, nil, "Regression")
	}
}

func (ts *tidbTestSuite) TestUint64(c *C) {
	ts.runTestPrepareResultFieldType(c)
}

func (ts *tidbTestSuite) TestSpecialType(c *C) {
	c.Parallel()
	ts.runTestSpecialType(c)
}

func (ts *tidbTestSuite) TestPreparedString(c *C) {
	c.Parallel()
	ts.runTestPreparedString(c)
}

func (ts *tidbTestSuite) TestPreparedTimestamp(c *C) {
	c.Parallel()
	ts.runTestPreparedTimestamp(c)
}

func (ts *tidbTestSerialSuite) TestConfigDefaultValue(c *C) {
	ts.runTestsOnNewDB(c, nil, "config", func(dbt *DBTest) {
		rows := dbt.mustQuery("select @@tidb_slow_log_threshold;")
		ts.checkRows(c, rows, "300")
	})
}

// this test will change `kv.TxnTotalSizeLimit` which may affect other test suites,
// so we must make it running in serial.
func (ts *tidbTestSerialSuite) TestLoadData(c *C) {
	ts.runTestLoadData(c, ts.server)
	ts.runTestLoadDataWithSelectIntoOutfile(c, ts.server)
	ts.runTestLoadDataForSlowLog(c, ts.server)
}

func (ts *tidbTestSerialSuite) TestLoadDataListPartition(c *C) {
	ts.runTestLoadDataForListPartition(c)
	ts.runTestLoadDataForListPartition2(c)
	ts.runTestLoadDataForListColumnPartition(c)
	ts.runTestLoadDataForListColumnPartition2(c)
}

// Fix issue#22540. Change tidb_dml_batch_size,
// then check if load data into table with auto random column works properly.
func (ts *tidbTestSerialSuite) TestLoadDataAutoRandom(c *C) {
	ts.runTestLoadDataAutoRandom(c)
}

func (ts *tidbTestSerialSuite) TestLoadDataAutoRandomWithSpecialTerm(c *C) {
	ts.runTestLoadDataAutoRandomWithSpecialTerm(c)
}

func (ts *tidbTestSerialSuite) TestExplainFor(c *C) {
	ts.runTestExplainForConn(c)
}

func (ts *tidbTestSerialSuite) TestStmtCount(c *C) {
	ts.runTestStmtCount(c)
}

func (ts *tidbTestSuite) TestConcurrentUpdate(c *C) {
	c.Parallel()
	ts.runTestConcurrentUpdate(c)
}

func (ts *tidbTestSuite) TestErrorCode(c *C) {
	c.Parallel()
	ts.runTestErrorCode(c)
}

func (ts *tidbTestSuite) TestAuth(c *C) {
	c.Parallel()
	ts.runTestAuth(c)
	ts.runTestIssue3682(c)
}

func (ts *tidbTestSuite) TestIssues(c *C) {
	c.Parallel()
	ts.runTestIssue3662(c)
	ts.runTestIssue3680(c)
	ts.runTestIssue22646(c)
}

func (ts *tidbTestSuite) TestDBNameEscape(c *C) {
	c.Parallel()
	ts.runTestDBNameEscape(c)
}

func (ts *tidbTestSuite) TestResultFieldTableIsNull(c *C) {
	c.Parallel()
	ts.runTestResultFieldTableIsNull(c)
}

func (ts *tidbTestSuite) TestStatusAPI(c *C) {
	c.Parallel()
	ts.runTestStatusAPI(c)
}

func (ts *tidbTestSuite) TestStatusPort(c *C) {
	var err error
	ts.store, err = mockstore.NewMockStore()
	session.DisableStats4Test()
	c.Assert(err, IsNil)
	ts.domain, err = session.BootstrapSession(ts.store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(ts.store)
	cfg := newTestConfig()
	cfg.Port = 0
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = ts.statusPort
	cfg.Performance.TCPKeepAlive = true

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, NotNil)
	c.Assert(server, IsNil)
}

func (ts *tidbTestSuite) TestStatusAPIWithTLS(c *C) {
	caCert, caKey, err := generateCert(0, "TiDB CA 2", nil, nil, "/tmp/ca-key-2.pem", "/tmp/ca-cert-2.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(1, "tidb-server-2", caCert, caKey, "/tmp/server-key-2.pem", "/tmp/server-cert-2.pem")
	c.Assert(err, IsNil)

	defer func() {
		os.Remove("/tmp/ca-key-2.pem")
		os.Remove("/tmp/ca-cert-2.pem")
		os.Remove("/tmp/server-key-2.pem")
		os.Remove("/tmp/server-cert-2.pem")
	}()

	cli := newTestServerClient()
	cli.statusScheme = "https"
	cfg := newTestConfig()
	cfg.Port = cli.port
	cfg.Status.StatusPort = cli.statusPort
	cfg.Security.ClusterSSLCA = "/tmp/ca-cert-2.pem"
	cfg.Security.ClusterSSLCert = "/tmp/server-cert-2.pem"
	cfg.Security.ClusterSSLKey = "/tmp/server-key-2.pem"
	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	cli.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)

	// https connection should work.
	ts.runTestStatusAPI(c)

	// but plain http connection should fail.
	cli.statusScheme = "http"
	_, err = cli.fetchStatus("/status")
	c.Assert(err, NotNil)

	server.Close()
}

func (ts *tidbTestSuite) TestStatusAPIWithTLSCNCheck(c *C) {
	caPath := filepath.Join(os.TempDir(), "ca-cert-cn.pem")
	serverKeyPath := filepath.Join(os.TempDir(), "server-key-cn.pem")
	serverCertPath := filepath.Join(os.TempDir(), "server-cert-cn.pem")
	client1KeyPath := filepath.Join(os.TempDir(), "client-key-cn-check-a.pem")
	client1CertPath := filepath.Join(os.TempDir(), "client-cert-cn-check-a.pem")
	client2KeyPath := filepath.Join(os.TempDir(), "client-key-cn-check-b.pem")
	client2CertPath := filepath.Join(os.TempDir(), "client-cert-cn-check-b.pem")

	caCert, caKey, err := generateCert(0, "TiDB CA CN CHECK", nil, nil, filepath.Join(os.TempDir(), "ca-key-cn.pem"), caPath)
	c.Assert(err, IsNil)
	_, _, err = generateCert(1, "tidb-server-cn-check", caCert, caKey, serverKeyPath, serverCertPath)
	c.Assert(err, IsNil)
	_, _, err = generateCert(2, "tidb-client-cn-check-a", caCert, caKey, client1KeyPath, client1CertPath, func(c *x509.Certificate) {
		c.Subject.CommonName = "tidb-client-1"
	})
	c.Assert(err, IsNil)
	_, _, err = generateCert(3, "tidb-client-cn-check-b", caCert, caKey, client2KeyPath, client2CertPath, func(c *x509.Certificate) {
		c.Subject.CommonName = "tidb-client-2"
	})
	c.Assert(err, IsNil)

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
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	cli.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)

	hc := newTLSHttpClient(c, caPath,
		client1CertPath,
		client1KeyPath,
	)
	_, err = hc.Get(cli.statusURL("/status"))
	c.Assert(err, NotNil)

	hc = newTLSHttpClient(c, caPath,
		client2CertPath,
		client2KeyPath,
	)
	_, err = hc.Get(cli.statusURL("/status"))
	c.Assert(err, IsNil)
}

func newTLSHttpClient(c *C, caFile, certFile, keyFile string) *http.Client {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	c.Assert(err, IsNil)
	caCert, err := os.ReadFile(caFile)
	c.Assert(err, IsNil)
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

func (ts *tidbTestSuite) TestMultiStatements(c *C) {
	c.Parallel()
	ts.runFailedTestMultiStatements(c)
	ts.runTestMultiStatements(c)
}

func (ts *tidbTestSuite) TestSocketForwarding(c *C) {
	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Socket = "/tmp/tidbtest.sock"
	cfg.Port = cli.port
	os.Remove(cfg.Socket)
	cfg.Status.ReportStatus = false

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()

	cli.runTestRegression(c, func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = "/tmp/tidbtest.sock"
		config.DBName = "test"
		config.Params = map[string]string{"sql_mode": "'STRICT_ALL_TABLES'"}
	}, "SocketRegression")
}

func (ts *tidbTestSuite) TestSocket(c *C) {
	cfg := newTestConfig()
	cfg.Socket = "/tmp/tidbtest.sock"
	cfg.Port = 0
	os.Remove(cfg.Socket)
	cfg.Host = ""
	cfg.Status.ReportStatus = false

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()

	// a fake server client, config is override, just used to run tests
	cli := newTestServerClient()
	cli.runTestRegression(c, func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = "/tmp/tidbtest.sock"
		config.DBName = "test"
		config.Params = map[string]string{"sql_mode": "STRICT_ALL_TABLES"}
	}, "SocketRegression")

}

func (ts *tidbTestSuite) TestSocketAndIp(c *C) {
	osTempDir := os.TempDir()
	tempDir, err := ioutil.TempDir(osTempDir, "tidb-test.*.socket")
	c.Assert(err, IsNil)
	socketFile := tempDir + "/tidbtest.sock" // Unix Socket does not work on Windows, so '/' should be OK
	defer os.RemoveAll(tempDir)
	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Socket = socketFile
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()

	// Test with Socket connection + Setup user1@% for all host access
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	defer func() {
		cli.runTests(c, func(config *mysql.Config) {
			config.User = "root"
		},
			func(dbt *DBTest) {
				dbt.mustQuery("DROP USER IF EXISTS 'user1'@'%'")
				dbt.mustQuery("DROP USER IF EXISTS 'user1'@'localhost'")
				dbt.mustQuery("DROP USER IF EXISTS 'user1'@'127.0.0.1'")
			})
	}()
	cli.runTests(c, func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = socketFile
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "root@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			dbt.mustQuery("CREATE USER user1@'%'")
			dbt.mustQuery("GRANT SELECT ON test.* TO user1@'%'")
		})
	// Test with Network interface connection with all hosts
	cli.runTests(c, func(config *mysql.Config) {
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			// NOTICE: this is not compatible with MySQL! (MySQL would report user1@localhost also for 127.0.0.1)
			cli.checkRows(c, rows, "user1@127.0.0.1")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT USAGE ON *.* TO 'user1'@'%'\nGRANT SELECT ON test.* TO 'user1'@'%'")
		})
	// Test with unix domain socket file connection with all hosts
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "user1@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT USAGE ON *.* TO 'user1'@'%'\nGRANT SELECT ON test.* TO 'user1'@'%'")
		})

	// Setup user1@127.0.0.1 for loop back network interface access
	cli.runTests(c, func(config *mysql.Config) {
		config.User = "root"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			// NOTICE: this is not compatible with MySQL! (MySQL would report user1@localhost also for 127.0.0.1)
			cli.checkRows(c, rows, "root@127.0.0.1")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			dbt.mustQuery("CREATE USER user1@127.0.0.1")
			dbt.mustQuery("GRANT SELECT,INSERT ON test.* TO user1@'127.0.0.1'")
		})
	// Test with Network interface connection with all hosts
	cli.runTests(c, func(config *mysql.Config) {
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			// NOTICE: this is not compatible with MySQL! (MySQL would report user1@localhost also for 127.0.0.1)
			cli.checkRows(c, rows, "user1@127.0.0.1")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT USAGE ON *.* TO 'user1'@'127.0.0.1'\nGRANT SELECT,INSERT ON test.* TO 'user1'@'127.0.0.1'")
		})
	// Test with unix domain socket file connection with all hosts
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "user1@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT USAGE ON *.* TO 'user1'@'%'\nGRANT SELECT ON test.* TO 'user1'@'%'")
		})

	// Setup user1@localhost for socket (and if MySQL compatible; loop back network interface access)
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "root"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "root@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			dbt.mustQuery("CREATE USER user1@localhost")
			dbt.mustQuery("GRANT SELECT,INSERT,UPDATE,DELETE ON test.* TO user1@localhost")
		})
	// Test with Network interface connection with all hosts
	cli.runTests(c, func(config *mysql.Config) {
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			// NOTICE: this is not compatible with MySQL! (MySQL would report user1@localhost also for 127.0.0.1)
			cli.checkRows(c, rows, "user1@127.0.0.1")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT USAGE ON *.* TO 'user1'@'127.0.0.1'\nGRANT SELECT,INSERT ON test.* TO 'user1'@'127.0.0.1'")
		})
	// Test with unix domain socket file connection with all hosts
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "user1@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT USAGE ON *.* TO 'user1'@'localhost'\nGRANT SELECT,INSERT,UPDATE,DELETE ON test.* TO 'user1'@'localhost'")
		})

}

// TestOnlySocket for server configuration without network interface for mysql clients
func (ts *tidbTestSuite) TestOnlySocket(c *C) {
	osTempDir := os.TempDir()
	tempDir, err := ioutil.TempDir(osTempDir, "tidb-test.*.socket")
	c.Assert(err, IsNil)
	socketFile := tempDir + "/tidbtest.sock" // Unix Socket does not work on Windows, so '/' should be OK
	defer os.RemoveAll(tempDir)
	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Socket = socketFile
	cfg.Host = "" // No network interface listening for mysql traffic
	cfg.Status.ReportStatus = false

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()
	c.Assert(server.listener, IsNil)
	c.Assert(server.socket, NotNil)

	// Test with Socket connection + Setup user1@% for all host access
	defer func() {
		cli.runTests(c, func(config *mysql.Config) {
			config.User = "root"
			config.Net = "unix"
			config.Addr = socketFile
		},
			func(dbt *DBTest) {
				dbt.mustQuery("DROP USER IF EXISTS 'user1'@'%'")
				dbt.mustQuery("DROP USER IF EXISTS 'user1'@'localhost'")
				dbt.mustQuery("DROP USER IF EXISTS 'user1'@'127.0.0.1'")
			})
	}()
	cli.runTests(c, func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = socketFile
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "root@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			dbt.mustQuery("CREATE USER user1@'%'")
			dbt.mustQuery("GRANT SELECT ON test.* TO user1@'%'")
		})
	// Test with Network interface connection with all hosts, should fail since server not configured
	_, err = sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "root"
		config.DBName = "test"
		config.Addr = "127.0.0.1"
	}))
	c.Assert(err, IsNil, Commentf("Connect succeeded when not configured!?!"))
	_, err = sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "user1"
		config.DBName = "test"
		config.Addr = "127.0.0.1"
	}))
	c.Assert(err, IsNil, Commentf("Connect succeeded when not configured!?!"))
	// Test with unix domain socket file connection with all hosts
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "user1@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT USAGE ON *.* TO 'user1'@'%'\nGRANT SELECT ON test.* TO 'user1'@'%'")
		})

	// Setup user1@127.0.0.1 for loop back network interface access
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "root"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			// NOTICE: this is not compatible with MySQL! (MySQL would report user1@localhost also for 127.0.0.1)
			cli.checkRows(c, rows, "root@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			dbt.mustQuery("CREATE USER user1@127.0.0.1")
			dbt.mustQuery("GRANT SELECT,INSERT ON test.* TO user1@'127.0.0.1'")
		})
	// Test with unix domain socket file connection with all hosts
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "user1@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT USAGE ON *.* TO 'user1'@'%'\nGRANT SELECT ON test.* TO 'user1'@'%'")
		})

	// Setup user1@localhost for socket (and if MySQL compatible; loop back network interface access)
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "root"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "root@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			dbt.mustQuery("CREATE USER user1@localhost")
			dbt.mustQuery("GRANT SELECT,INSERT,UPDATE,DELETE ON test.* TO user1@localhost")
		})
	// Test with unix domain socket file connection with all hosts
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "user1@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT USAGE ON *.* TO 'user1'@'localhost'\nGRANT SELECT,INSERT,UPDATE,DELETE ON test.* TO 'user1'@'localhost'")
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

func (ts *tidbTestSuite) TestSystemTimeZone(c *C) {
	tk := testkit.NewTestKit(c, ts.store)
	cfg := newTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	defer server.Close()

	tz1 := tk.MustQuery("select variable_value from mysql.tidb where variable_name = 'system_tz'").Rows()
	tk.MustQuery("select @@system_time_zone").Check(tz1)
}

func (ts *tidbTestSerialSuite) TestTLS(c *C) {
	// Generate valid TLS certificates.
	caCert, caKey, err := generateCert(0, "TiDB CA", nil, nil, "/tmp/ca-key.pem", "/tmp/ca-cert.pem")
	c.Assert(err, IsNil)
	serverCert, _, err := generateCert(1, "tidb-server", caCert, caKey, "/tmp/server-key.pem", "/tmp/server-cert.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(2, "SQL Client Certificate", caCert, caKey, "/tmp/client-key.pem", "/tmp/client-cert.pem")
	c.Assert(err, IsNil)
	err = registerTLSConfig("client-certificate", "/tmp/ca-cert.pem", "/tmp/client-cert.pem", "/tmp/client-key.pem", "tidb-server", true)
	c.Assert(err, IsNil)

	defer func() {
		err := os.Remove("/tmp/ca-key.pem")
		c.Assert(err, IsNil)
		err = os.Remove("/tmp/ca-cert.pem")
		c.Assert(err, IsNil)
		err = os.Remove("/tmp/server-key.pem")
		c.Assert(err, IsNil)
		err = os.Remove("/tmp/server-cert.pem")
		c.Assert(err, IsNil)
		err = os.Remove("/tmp/client-key.pem")
		c.Assert(err, IsNil)
		err = os.Remove("/tmp/client-cert.pem")
		c.Assert(err, IsNil)
	}()

	// Start the server without TLS.
	connOverrider := func(config *mysql.Config) {
		config.TLSConfig = "skip-verify"
	}
	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false
	cfg.Security.AutoTLS = true
	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)
	err = cli.runTestTLSConnection(c, connOverrider) // Relying on automatically created TLS certificates
	c.Assert(err, IsNil)

	server.Close()

	// Start the server with TLS but without CA, in this case the server will not verify client's certificate.
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "skip-verify"
	}
	cli = newTestServerClient()
	cfg = newTestConfig()
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false
	cfg.Security = config.Security{
		SSLCert: "/tmp/server-cert.pem",
		SSLKey:  "/tmp/server-key.pem",
	}
	server, err = NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)
	err = cli.runTestTLSConnection(c, connOverrider) // We should establish connection successfully.
	c.Assert(err, IsNil)
	cli.runTestRegression(c, connOverrider, "TLSRegression")
	// Perform server verification.
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "client-certificate"
	}
	err = cli.runTestTLSConnection(c, connOverrider) // We should establish connection successfully.
	c.Assert(err, IsNil, Commentf("%v", errors.ErrorStack(err)))
	cli.runTestRegression(c, connOverrider, "TLSRegression")

	// Test SSL/TLS session vars
	var v *variable.SessionVars
	stats, err := server.Stats(v)
	c.Assert(err, IsNil)
	c.Assert(stats, HasKey, "Ssl_server_not_after")
	c.Assert(stats, HasKey, "Ssl_server_not_before")
	c.Assert(stats["Ssl_server_not_after"], Equals, serverCert.NotAfter.Format("Jan _2 15:04:05 2006 MST"))
	c.Assert(stats["Ssl_server_not_before"], Equals, serverCert.NotBefore.Format("Jan _2 15:04:05 2006 MST"))

	server.Close()

	// Start the server with TLS & CA, if the client presents its certificate, the certificate will be verified.
	cli = newTestServerClient()
	cfg = newTestConfig()
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false
	cfg.Security = config.Security{
		SSLCA:   "/tmp/ca-cert.pem",
		SSLCert: "/tmp/server-cert.pem",
		SSLKey:  "/tmp/server-key.pem",
	}
	server, err = NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)
	// The client does not provide a certificate, the connection should succeed.
	err = cli.runTestTLSConnection(c, nil)
	c.Assert(err, IsNil)
	cli.runTestRegression(c, connOverrider, "TLSRegression")
	// The client provides a valid certificate.
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "client-certificate"
	}
	err = cli.runTestTLSConnection(c, connOverrider)
	c.Assert(err, IsNil)
	cli.runTestRegression(c, connOverrider, "TLSRegression")
	server.Close()

	c.Assert(util.IsTLSExpiredError(errors.New("unknown test")), IsFalse)
	c.Assert(util.IsTLSExpiredError(x509.CertificateInvalidError{Reason: x509.CANotAuthorizedForThisName}), IsFalse)
	c.Assert(util.IsTLSExpiredError(x509.CertificateInvalidError{Reason: x509.Expired}), IsTrue)

	_, _, err = util.LoadTLSCertificates("", "wrong key", "wrong cert", true)
	c.Assert(err, NotNil)
	_, _, err = util.LoadTLSCertificates("wrong ca", "/tmp/server-key.pem", "/tmp/server-cert.pem", true)
	c.Assert(err, NotNil)
}

func (ts *tidbTestSerialSuite) TestReloadTLS(c *C) {
	// Generate valid TLS certificates.
	caCert, caKey, err := generateCert(0, "TiDB CA", nil, nil, "/tmp/ca-key-reload.pem", "/tmp/ca-cert-reload.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(1, "tidb-server", caCert, caKey, "/tmp/server-key-reload.pem", "/tmp/server-cert-reload.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(2, "SQL Client Certificate", caCert, caKey, "/tmp/client-key-reload.pem", "/tmp/client-cert-reload.pem")
	c.Assert(err, IsNil)
	err = registerTLSConfig("client-certificate-reload", "/tmp/ca-cert-reload.pem", "/tmp/client-cert-reload.pem", "/tmp/client-key-reload.pem", "tidb-server", true)
	c.Assert(err, IsNil)

	defer func() {
		os.Remove("/tmp/ca-key-reload.pem")
		os.Remove("/tmp/ca-cert-reload.pem")

		os.Remove("/tmp/server-key-reload.pem")
		os.Remove("/tmp/server-cert-reload.pem")
		os.Remove("/tmp/client-key-reload.pem")
		os.Remove("/tmp/client-cert-reload.pem")
	}()

	// try old cert used in startup configuration.
	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false
	cfg.Security = config.Security{
		SSLCA:   "/tmp/ca-cert-reload.pem",
		SSLCert: "/tmp/server-cert-reload.pem",
		SSLKey:  "/tmp/server-key-reload.pem",
	}
	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)
	// The client provides a valid certificate.
	connOverrider := func(config *mysql.Config) {
		config.TLSConfig = "client-certificate-reload"
	}
	err = cli.runTestTLSConnection(c, connOverrider)
	c.Assert(err, IsNil)

	// try reload a valid cert.
	tlsCfg := server.getTLSConfig()
	cert, err := x509.ParseCertificate(tlsCfg.Certificates[0].Certificate[0])
	c.Assert(err, IsNil)
	oldExpireTime := cert.NotAfter
	_, _, err = generateCert(1, "tidb-server", caCert, caKey, "/tmp/server-key-reload2.pem", "/tmp/server-cert-reload2.pem", func(c *x509.Certificate) {
		c.NotBefore = time.Now().Add(-24 * time.Hour).UTC()
		c.NotAfter = time.Now().Add(1 * time.Hour).UTC()
	})
	c.Assert(err, IsNil)
	err = os.Rename("/tmp/server-key-reload2.pem", "/tmp/server-key-reload.pem")
	c.Assert(err, IsNil)
	err = os.Rename("/tmp/server-cert-reload2.pem", "/tmp/server-cert-reload.pem")
	c.Assert(err, IsNil)
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "skip-verify"
	}
	err = cli.runReloadTLS(c, connOverrider, false)
	c.Assert(err, IsNil)
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "client-certificate-reload"
	}
	err = cli.runTestTLSConnection(c, connOverrider)
	c.Assert(err, IsNil)

	tlsCfg = server.getTLSConfig()
	cert, err = x509.ParseCertificate(tlsCfg.Certificates[0].Certificate[0])
	c.Assert(err, IsNil)
	newExpireTime := cert.NotAfter
	c.Assert(newExpireTime.After(oldExpireTime), IsTrue)

	// try reload a expired cert.
	_, _, err = generateCert(1, "tidb-server", caCert, caKey, "/tmp/server-key-reload3.pem", "/tmp/server-cert-reload3.pem", func(c *x509.Certificate) {
		c.NotBefore = time.Now().Add(-24 * time.Hour).UTC()
		c.NotAfter = c.NotBefore.Add(1 * time.Hour).UTC()
	})
	c.Assert(err, IsNil)
	err = os.Rename("/tmp/server-key-reload3.pem", "/tmp/server-key-reload.pem")
	c.Assert(err, IsNil)
	err = os.Rename("/tmp/server-cert-reload3.pem", "/tmp/server-cert-reload.pem")
	c.Assert(err, IsNil)
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "skip-verify"
	}
	err = cli.runReloadTLS(c, connOverrider, false)
	c.Assert(err, IsNil)
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "client-certificate-reload"
	}
	err = cli.runTestTLSConnection(c, connOverrider)
	c.Assert(err, NotNil)
	c.Assert(util.IsTLSExpiredError(err), IsTrue, Commentf("real error is %+v", err))
	server.Close()
}

func (ts *tidbTestSerialSuite) TestErrorNoRollback(c *C) {
	// Generate valid TLS certificates.
	caCert, caKey, err := generateCert(0, "TiDB CA", nil, nil, "/tmp/ca-key-rollback.pem", "/tmp/ca-cert-rollback.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(1, "tidb-server", caCert, caKey, "/tmp/server-key-rollback.pem", "/tmp/server-cert-rollback.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(2, "SQL Client Certificate", caCert, caKey, "/tmp/client-key-rollback.pem", "/tmp/client-cert-rollback.pem")
	c.Assert(err, IsNil)
	err = registerTLSConfig("client-cert-rollback-test", "/tmp/ca-cert-rollback.pem", "/tmp/client-cert-rollback.pem", "/tmp/client-key-rollback.pem", "tidb-server", true)
	c.Assert(err, IsNil)

	defer func() {
		os.Remove("/tmp/ca-key-rollback.pem")
		os.Remove("/tmp/ca-cert-rollback.pem")

		os.Remove("/tmp/server-key-rollback.pem")
		os.Remove("/tmp/server-cert-rollback.pem")
		os.Remove("/tmp/client-key-rollback.pem")
		os.Remove("/tmp/client-cert-rollback.pem")
	}()

	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false

	cfg.Security = config.Security{
		RequireSecureTransport: true,
		SSLCA:                  "wrong path",
		SSLCert:                "wrong path",
		SSLKey:                 "wrong path",
	}
	_, err = NewServer(cfg, ts.tidbdrv)
	c.Assert(err, NotNil)

	// test reload tls fail with/without "error no rollback option"
	cfg.Security = config.Security{
		SSLCA:   "/tmp/ca-cert-rollback.pem",
		SSLCert: "/tmp/server-cert-rollback.pem",
		SSLKey:  "/tmp/server-key-rollback.pem",
	}
	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)
	connOverrider := func(config *mysql.Config) {
		config.TLSConfig = "client-cert-rollback-test"
	}
	err = cli.runTestTLSConnection(c, connOverrider)
	c.Assert(err, IsNil)
	os.Remove("/tmp/server-key-rollback.pem")
	err = cli.runReloadTLS(c, connOverrider, false)
	c.Assert(err, NotNil)
	tlsCfg := server.getTLSConfig()
	c.Assert(tlsCfg, NotNil)
	err = cli.runReloadTLS(c, connOverrider, true)
	c.Assert(err, IsNil)
	tlsCfg = server.getTLSConfig()
	c.Assert(tlsCfg, IsNil)
}

func (ts *tidbTestSuite) TestClientWithCollation(c *C) {
	c.Parallel()
	ts.runTestClientWithCollation(c)
}

func (ts *tidbTestSuite) TestCreateTableFlen(c *C) {
	// issue #4540
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	_, err = Execute(context.Background(), qctx, "use test;")
	c.Assert(err, IsNil)

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
	c.Assert(err, IsNil)
	rs, err := Execute(ctx, qctx, "show create table t1")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, IsNil)
	cols := rs.Columns()
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 2)
	c.Assert(int(cols[0].ColumnLength), Equals, 5*tmysql.MaxBytesOfCharacter)
	c.Assert(int(cols[1].ColumnLength), Equals, len(req.GetRow(0).GetString(1))*tmysql.MaxBytesOfCharacter)

	// for issue#5246
	rs, err = Execute(ctx, qctx, "select y, z from t1")
	c.Assert(err, IsNil)
	cols = rs.Columns()
	c.Assert(len(cols), Equals, 2)
	c.Assert(int(cols[0].ColumnLength), Equals, 21)
	c.Assert(int(cols[1].ColumnLength), Equals, 22)
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

func (ts *tidbTestSuite) TestShowTablesFlen(c *C) {
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = Execute(ctx, qctx, "use test;")
	c.Assert(err, IsNil)

	testSQL := "create table abcdefghijklmnopqrstuvwxyz (i int)"
	_, err = Execute(ctx, qctx, testSQL)
	c.Assert(err, IsNil)
	rs, err := Execute(ctx, qctx, "show tables")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, IsNil)
	cols := rs.Columns()
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 1)
	c.Assert(int(cols[0].ColumnLength), Equals, 26*tmysql.MaxBytesOfCharacter)
}

func checkColNames(c *C, columns []*ColumnInfo, names ...string) {
	for i, name := range names {
		c.Assert(columns[i].Name, Equals, name)
		c.Assert(columns[i].OrgName, Equals, name)
	}
}

func (ts *tidbTestSuite) TestFieldList(c *C) {
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	_, err = Execute(context.Background(), qctx, "use test;")
	c.Assert(err, IsNil)

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
	c.Assert(err, IsNil)
	colInfos, err := qctx.FieldList("t")
	c.Assert(err, IsNil)
	c.Assert(len(colInfos), Equals, 19)

	checkColNames(c, colInfos, "c_bit", "c_int_d", "c_bigint_d", "c_float_d",
		"c_double_d", "c_decimal", "c_datetime", "c_time", "c_date", "c_timestamp",
		"c_char", "c_varchar", "c_text_d", "c_binary", "c_blob_d", "c_set", "c_enum",
		"c_json", "c_year")

	for _, cols := range colInfos {
		c.Assert(cols.Schema, Equals, "test")
	}

	for _, cols := range colInfos {
		c.Assert(cols.Table, Equals, "t")
	}

	for i, col := range colInfos {
		switch i {
		case 10, 11, 12, 15, 16:
			// c_char char(20), c_varchar varchar(20), c_text_d text,
			// c_set set('a', 'b', 'c'), c_enum enum('a', 'b', 'c')
			c.Assert(col.Charset, Equals, uint16(tmysql.CharsetNameToID(tmysql.DefaultCharset)), Commentf("index %d", i))
			continue
		}

		c.Assert(col.Charset, Equals, uint16(tmysql.CharsetNameToID("binary")), Commentf("index %d", i))
	}

	// c_decimal decimal(6, 3)
	c.Assert(colInfos[5].Decimal, Equals, uint8(3))

	// for issue#10513
	tooLongColumnAsName := "COALESCE(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)"
	columnAsName := tooLongColumnAsName[:tmysql.MaxAliasIdentifierLen]

	rs, err := Execute(ctx, qctx, "select "+tooLongColumnAsName)
	c.Assert(err, IsNil)
	cols := rs.Columns()
	c.Assert(cols[0].OrgName, Equals, tooLongColumnAsName)
	c.Assert(cols[0].Name, Equals, columnAsName)

	rs, err = Execute(ctx, qctx, "select c_bit as '"+tooLongColumnAsName+"' from t")
	c.Assert(err, IsNil)
	cols = rs.Columns()
	c.Assert(cols[0].OrgName, Equals, "c_bit")
	c.Assert(cols[0].Name, Equals, columnAsName)
}

func (ts *tidbTestSuite) TestClientErrors(c *C) {
	ts.runTestInfoschemaClientErrors(c)
}

func (ts *tidbTestSuite) TestInitConnect(c *C) {
	ts.runTestInitConnect(c)
}

func (ts *tidbTestSuite) TestSumAvg(c *C) {
	c.Parallel()
	ts.runTestSumAvg(c)
}

func (ts *tidbTestSuite) TestNullFlag(c *C) {
	// issue #9689
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)

	ctx := context.Background()
	rs, err := Execute(ctx, qctx, "select 1")
	c.Assert(err, IsNil)
	cols := rs.Columns()
	c.Assert(len(cols), Equals, 1)
	expectFlag := uint16(tmysql.NotNullFlag | tmysql.BinaryFlag)
	c.Assert(dumpFlag(cols[0].Type, cols[0].Flag), Equals, expectFlag)
}

func (ts *tidbTestSuite) TestNO_DEFAULT_VALUEFlag(c *C) {
	// issue #21465
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)

	ctx := context.Background()
	_, err = Execute(ctx, qctx, "use test")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "drop table if exists t")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "create table t(c1 int key, c2 int);")
	c.Assert(err, IsNil)
	rs, err := Execute(ctx, qctx, "select c1 from t;")
	c.Assert(err, IsNil)
	cols := rs.Columns()
	c.Assert(len(cols), Equals, 1)
	expectFlag := uint16(tmysql.NotNullFlag | tmysql.PriKeyFlag | tmysql.NoDefaultValueFlag)
	c.Assert(dumpFlag(cols[0].Type, cols[0].Flag), Equals, expectFlag)
}

func (ts *tidbTestSuite) TestGracefulShutdown(c *C) {
	var err error
	ts.store, err = mockstore.NewMockStore()
	session.DisableStats4Test()
	c.Assert(err, IsNil)
	ts.domain, err = session.BootstrapSession(ts.store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(ts.store)
	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.GracefulWaitBeforeShutdown = 2 // wait before shutdown
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	cfg.Status.ReportStatus = true
	cfg.Performance.TCPKeepAlive = true
	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	c.Assert(server, NotNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	cli.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)

	_, err = cli.fetchStatus("/status") // server is up
	c.Assert(err, IsNil)

	go server.Close()
	time.Sleep(time.Millisecond * 500)

	resp, _ := cli.fetchStatus("/status") // should return 5xx code
	c.Assert(resp.StatusCode, Equals, 500)

	time.Sleep(time.Second * 2)

	_, err = cli.fetchStatus("/status") // status is gone
	c.Assert(err, ErrorMatches, ".*connect: connection refused")
}

func (ts *tidbTestSerialSuite) TestDefaultCharacterAndCollation(c *C) {
	// issue #21194
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	// 255 is the collation id of mysql client 8 default collation_connection
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(255), "test", nil)
	c.Assert(err, IsNil)
	testCase := []struct {
		variable string
		except   string
	}{
		{"collation_connection", "utf8mb4_bin"},
		{"character_set_connection", "utf8mb4"},
		{"character_set_client", "utf8mb4"},
	}

	for _, t := range testCase {
		sVars, b := qctx.GetSessionVars().GetSystemVar(t.variable)
		c.Assert(b, IsTrue)
		c.Assert(sVars, Equals, t.except)
	}
}

func (ts *tidbTestSuite) TestPessimisticInsertSelectForUpdate(c *C) {
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = Execute(ctx, qctx, "use test;")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "drop table if exists t1, t2")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "create table t1 (id int)")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "create table t2 (id int)")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "insert into t1 select 1")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "begin pessimistic")
	c.Assert(err, IsNil)
	rs, err := Execute(ctx, qctx, "INSERT INTO t2 (id) select id from t1 where id = 1 for update")
	c.Assert(err, IsNil)
	c.Assert(rs, IsNil) // should be no delay
}

func (ts *tidbTestSerialSuite) TestPrepareCount(c *C) {
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	prepareCnt := atomic.LoadInt64(&variable.PreparedStmtCount)
	ctx := context.Background()
	_, err = Execute(ctx, qctx, "use test;")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "drop table if exists t1")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "create table t1 (id int)")
	c.Assert(err, IsNil)
	stmt, _, _, err := qctx.Prepare("insert into t1 values (?)")
	c.Assert(err, IsNil)
	c.Assert(atomic.LoadInt64(&variable.PreparedStmtCount), Equals, prepareCnt+1)
	c.Assert(err, IsNil)
	err = qctx.GetStatement(stmt.ID()).Close()
	c.Assert(err, IsNil)
	c.Assert(atomic.LoadInt64(&variable.PreparedStmtCount), Equals, prepareCnt)
}

type collectorWrapper struct {
	reporter.TopSQLReporter
}

func (ts *tidbTestTopSQLSuite) TestTopSQLCPUProfile(c *C) {
	db, err := sql.Open("mysql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer func() {
		err := db.Close()
		c.Assert(err, IsNil)
	}()

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/domain/skipLoadSysVarCacheLoop", `return(true)`), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/util/topsql/mockHighLoadForEachSQL", `return(true)`), IsNil)
	defer func() {
		err = failpoint.Disable("github.com/pingcap/tidb/domain/skipLoadSysVarCacheLoop")
		c.Assert(err, IsNil)
		err = failpoint.Disable("github.com/pingcap/tidb/util/topsql/mockHighLoadForEachSQL")
		c.Assert(err, IsNil)
	}()

	collector := mockTopSQLTraceCPU.NewTopSQLCollector()
	tracecpu.GlobalSQLCPUProfiler.SetCollector(&collectorWrapper{collector})

	dbt := &DBTest{c, db}
	dbt.mustExec("drop database if exists topsql")
	dbt.mustExec("create database topsql")
	dbt.mustExec("use topsql;")
	dbt.mustExec("create table t (a int auto_increment, b int, unique index idx(a));")
	dbt.mustExec("create table t1 (a int auto_increment, b int, unique index idx(a));")
	dbt.mustExec("create table t2 (a int auto_increment, b int, unique index idx(a));")
	dbt.mustExec("set @@global.tidb_enable_top_sql='On';")
	dbt.mustExec("set @@tidb_top_sql_agent_address='127.0.0.1:4001';")
	dbt.mustExec("set @@global.tidb_top_sql_precision_seconds=1;")
	dbt.mustExec("set @@global.tidb_txn_mode = 'pessimistic'")

	// Test case 1: DML query: insert/update/replace/delete/select
	cases1 := []struct {
		sql        string
		planRegexp string
		cancel     func()
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
	for i, ca := range cases1 {
		ctx, cancel := context.WithCancel(context.Background())
		cases1[i].cancel = cancel
		sqlStr := ca.sql
		go ts.loopExec(ctx, c, func(db *sql.DB) {
			dbt := &DBTest{c, db}
			if strings.HasPrefix(sqlStr, "select") {
				rows := dbt.mustQuery(sqlStr)
				for rows.Next() {
				}
			} else {
				// Ignore error here since the error may be write conflict.
				db.Exec(sqlStr)
			}
		})
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	checkFn := func(sql, planRegexp string) {
		c.Assert(timeoutCtx.Err(), IsNil)
		commentf := Commentf("sql: %v", sql)
		stats := collector.GetSQLStatsBySQLWithRetry(sql, len(planRegexp) > 0)
		// since 1 sql may has many plan, check `len(stats) > 0` instead of `len(stats) == 1`.
		c.Assert(len(stats) > 0, IsTrue, commentf)

		for _, s := range stats {
			sqlStr := collector.GetSQL(s.SQLDigest)
			encodedPlan := collector.GetPlan(s.PlanDigest)
			// Normalize the user SQL before check.
			normalizedSQL := parser.Normalize(sql)
			c.Assert(sqlStr, Equals, normalizedSQL, commentf)
			// decode plan before check.
			normalizedPlan, err := plancodec.DecodeNormalizedPlan(encodedPlan)
			c.Assert(err, IsNil)
			// remove '\n' '\t' before do regexp match.
			normalizedPlan = strings.Replace(normalizedPlan, "\n", " ", -1)
			normalizedPlan = strings.Replace(normalizedPlan, "\t", " ", -1)
			c.Assert(normalizedPlan, Matches, planRegexp, commentf)
		}
	}
	// Wait the top sql collector to collect profile data.
	collector.WaitCollectCnt(1)
	// Check result of test case 1.
	for _, ca := range cases1 {
		checkFn(ca.sql, ca.planRegexp)
		ca.cancel()
	}

	// Test case 2: prepare/execute sql
	cases2 := []struct {
		prepare    string
		args       []interface{}
		planRegexp string
		cancel     func()
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
	for i, ca := range cases2 {
		ctx, cancel := context.WithCancel(context.Background())
		cases2[i].cancel = cancel
		prepare, args := ca.prepare, ca.args
		var stmt *sql.Stmt
		go ts.loopExec(ctx, c, func(db *sql.DB) {
			if stmt == nil {
				stmt, err = db.Prepare(prepare)
				c.Assert(err, IsNil)
			}
			if strings.HasPrefix(prepare, "select") {
				rows, err := stmt.Query(args...)
				c.Assert(err, IsNil)
				for rows.Next() {
				}
			} else {
				// Ignore error here since the error may be write conflict.
				stmt.Exec(args...)
			}
		})
	}
	// Wait the top sql collector to collect profile data.
	collector.WaitCollectCnt(1)
	// Check result of test case 2.
	for _, ca := range cases2 {
		checkFn(ca.prepare, ca.planRegexp)
		ca.cancel()
	}

	// Test case 3: prepare, execute stmt using @val...
	cases3 := []struct {
		prepare    string
		args       []interface{}
		planRegexp string
		cancel     func()
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
	for i, ca := range cases3 {
		ctx, cancel := context.WithCancel(context.Background())
		cases3[i].cancel = cancel
		prepare, args := ca.prepare, ca.args
		doPrepare := true
		go ts.loopExec(ctx, c, func(db *sql.DB) {
			if doPrepare {
				doPrepare = false
				_, err := db.Exec(fmt.Sprintf("prepare stmt from '%v'", prepare))
				c.Assert(err, IsNil)
			}
			sqlBuf := bytes.NewBuffer(nil)
			sqlBuf.WriteString("execute stmt ")
			for i := range args {
				_, err = db.Exec(fmt.Sprintf("set @%c=%v", 'a'+i, args[i]))
				c.Assert(err, IsNil)
				if i == 0 {
					sqlBuf.WriteString("using ")
				} else {
					sqlBuf.WriteByte(',')
				}
				sqlBuf.WriteByte('@')
				sqlBuf.WriteByte('a' + byte(i))
			}
			if strings.HasPrefix(prepare, "select") {
				rows, err := db.Query(sqlBuf.String())
				c.Assert(err, IsNil, Commentf("%v", sqlBuf.String()))
				for rows.Next() {
				}
			} else {
				// Ignore error here since the error may be write conflict.
				db.Exec(sqlBuf.String())
			}
		})
	}

	// Wait the top sql collector to collect profile data.
	collector.WaitCollectCnt(1)
	// Check result of test case 3.
	for _, ca := range cases3 {
		checkFn(ca.prepare, ca.planRegexp)
		ca.cancel()
	}

	// Test case 4: transaction commit
	ctx4, cancel4 := context.WithCancel(context.Background())
	defer cancel4()
	go ts.loopExec(ctx4, c, func(db *sql.DB) {
		db.Exec("begin")
		db.Exec("insert into t () values (),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),(),()")
		db.Exec("commit")
	})
	// Check result of test case 4.
	checkFn("commit", "")
}

func (ts *tidbTestTopSQLSuite) TestTopSQLAgent(c *C) {
	c.Skip("unstable, skip it and fix it before 20210702")
	db, err := sql.Open("mysql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer func() {
		err := db.Close()
		c.Assert(err, IsNil)
	}()
	agentServer, err := mockTopSQLReporter.StartMockAgentServer()
	c.Assert(err, IsNil)
	defer func() {
		agentServer.Stop()
	}()

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/util/topsql/reporter/resetTimeoutForTest", `return(true)`), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/domain/skipLoadSysVarCacheLoop", `return(true)`), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/util/topsql/mockHighLoadForEachSQL", `return(true)`), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/util/topsql/reporter/resetTimeoutForTest")
		c.Assert(err, IsNil)
		err = failpoint.Disable("github.com/pingcap/tidb/domain/skipLoadSysVarCacheLoop")
		c.Assert(err, IsNil)
		err = failpoint.Disable("github.com/pingcap/tidb/util/topsql/mockHighLoadForEachSQL")
		c.Assert(err, IsNil)
	}()

	dbt := &DBTest{c, db}
	dbt.mustExec("drop database if exists topsql")
	dbt.mustExec("create database topsql")
	dbt.mustExec("use topsql;")
	for i := 0; i < 20; i++ {
		dbt.mustExec(fmt.Sprintf("create table t%v (a int auto_increment, b int, unique index idx(a));", i))
		for j := 0; j < 100; j++ {
			dbt.mustExec(fmt.Sprintf("insert into t%v (b) values (%v);", i, j))
		}
	}
	dbt.mustExec("set @@global.tidb_enable_top_sql='On';")
	dbt.mustExec("set @@tidb_top_sql_agent_address='';")
	dbt.mustExec("set @@global.tidb_top_sql_precision_seconds=1;")
	dbt.mustExec("set @@global.tidb_top_sql_report_interval_seconds=2;")
	dbt.mustExec("set @@global.tidb_top_sql_max_statement_count=5;")

	r := reporter.NewRemoteTopSQLReporter(reporter.NewGRPCReportClient(plancodec.DecodeNormalizedPlan))
	tracecpu.GlobalSQLCPUProfiler.SetCollector(&collectorWrapper{r})

	// TODO: change to ensure that the right sql statements are reported, not just counts
	checkFn := func(n int) {
		records := agentServer.GetLatestRecords()
		c.Assert(len(records), Equals, n)
		for _, r := range records {
			sqlMeta, exist := agentServer.GetSQLMetaByDigestBlocking(r.SqlDigest, time.Second)
			c.Assert(exist, IsTrue)
			c.Check(sqlMeta.NormalizedSql, Matches, "select.*from.*join.*")
			if len(r.PlanDigest) == 0 {
				continue
			}
			plan, exist := agentServer.GetPlanMetaByDigestBlocking(r.PlanDigest, time.Second)
			c.Assert(exist, IsTrue)
			plan = strings.Replace(plan, "\n", " ", -1)
			plan = strings.Replace(plan, "\t", " ", -1)
			c.Assert(plan, Matches, ".*Join.*Select.*")
		}
	}
	runWorkload := func(start, end int) context.CancelFunc {
		ctx, cancel := context.WithCancel(context.Background())
		for i := start; i < end; i++ {
			query := fmt.Sprintf("select /*+ HASH_JOIN(ta, tb) */ * from t%[1]v ta join t%[1]v tb on ta.a=tb.a where ta.b is not null;", i)
			go ts.loopExec(ctx, c, func(db *sql.DB) {
				dbt := &DBTest{c, db}
				rows := dbt.mustQuery(query)
				for rows.Next() {
				}
			})
		}
		return cancel
	}

	// case 1: dynamically change agent endpoint
	cancel := runWorkload(0, 10)
	// Test with null agent address, the agent server can't receive any record.
	dbt.mustExec("set @@tidb_top_sql_agent_address='';")
	agentServer.WaitCollectCnt(1, time.Second*4)
	checkFn(0)
	// Test after set agent address and the evict take effect.
	dbt.mustExec("set @@global.tidb_top_sql_max_statement_count=5;")
	dbt.mustExec(fmt.Sprintf("set @@tidb_top_sql_agent_address='%v';", agentServer.Address()))
	agentServer.WaitCollectCnt(1, time.Second*4)
	checkFn(5)
	// Test with wrong agent address, the agent server can't receive any record.
	dbt.mustExec("set @@global.tidb_top_sql_max_statement_count=8;")
	dbt.mustExec("set @@tidb_top_sql_agent_address='127.0.0.1:65530';")
	agentServer.WaitCollectCnt(1, time.Second*4)
	checkFn(0)
	// Test after set agent address and the evict take effect.
	dbt.mustExec(fmt.Sprintf("set @@tidb_top_sql_agent_address='%v';", agentServer.Address()))
	agentServer.WaitCollectCnt(1, time.Second*4)
	checkFn(8)
	cancel() // cancel case 1

	// case 2: agent hangs for a while
	cancel2 := runWorkload(0, 10)
	// empty agent address, should not collect records
	dbt.mustExec("set @@global.tidb_top_sql_max_statement_count=5;")
	dbt.mustExec("set @@tidb_top_sql_agent_address='';")
	agentServer.WaitCollectCnt(1, time.Second*4)
	checkFn(0)
	// set correct address, should collect records
	dbt.mustExec(fmt.Sprintf("set @@tidb_top_sql_agent_address='%v';", agentServer.Address()))
	agentServer.WaitCollectCnt(1, time.Second*4)
	checkFn(5)
	// agent server hangs for a while
	agentServer.HangFromNow(time.Second * 6)
	// run another set of SQL queries
	cancel2()

	cancel3 := runWorkload(11, 20)
	agentServer.WaitCollectCnt(1, time.Second*8)
	checkFn(5)
	cancel3()

	// case 3: agent restart
	cancel4 := runWorkload(0, 10)
	// empty agent address, should not collect records
	dbt.mustExec("set @@tidb_top_sql_agent_address='';")
	agentServer.WaitCollectCnt(1, time.Second*4)
	checkFn(0)
	// set correct address, should collect records
	dbt.mustExec(fmt.Sprintf("set @@tidb_top_sql_agent_address='%v';", agentServer.Address()))
	agentServer.WaitCollectCnt(1, time.Second*8)
	checkFn(5)
	// run another set of SQL queries
	cancel4()

	cancel5 := runWorkload(11, 20)
	// agent server shutdown
	agentServer.Stop()
	// agent server restart
	agentServer, err = mockTopSQLReporter.StartMockAgentServer()
	c.Assert(err, IsNil)
	dbt.mustExec(fmt.Sprintf("set @@tidb_top_sql_agent_address='%v';", agentServer.Address()))
	// check result
	agentServer.WaitCollectCnt(2, time.Second*8)
	checkFn(5)
	cancel5()
}

func (ts *tidbTestTopSQLSuite) loopExec(ctx context.Context, c *C, fn func(db *sql.DB)) {
	db, err := sql.Open("mysql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer func() {
		err := db.Close()
		c.Assert(err, IsNil)
	}()
	dbt := &DBTest{c, db}
	dbt.mustExec("use topsql;")
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		fn(db)
	}
}
