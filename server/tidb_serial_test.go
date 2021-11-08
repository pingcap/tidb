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

package server

import (
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

// this test will change `kv.TxnTotalSizeLimit` which may affect other test suites,
// so we must make it running in serial.
func TestLoadData(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestLoadData(t, ts.server)
	ts.runTestLoadDataWithSelectIntoOutfile(t, ts.server)
	ts.runTestLoadDataForSlowLog(t, ts.server)
}

func TestConfigDefaultValue(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestsOnNewDB(t, nil, "config", func(dbt *testkit.DBTestKit) {
		rows := dbt.MustQuery("select @@tidb_slow_log_threshold;")
		ts.checkRows(t, rows, "300")
	})
}

// Fix issue#22540. Change tidb_dml_batch_size,
// then check if load data into table with auto random column works properly.
func TestLoadDataAutoRandom(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestLoadDataAutoRandom(t)
}

func TestLoadDataAutoRandomWithSpecialTerm(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestLoadDataAutoRandomWithSpecialTerm(t)
}

func TestExplainFor(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestExplainForConn(t)
}

func TestStmtCount(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestStmtCount(t)
}

func TestLoadDataListPartition(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestLoadDataForListPartition(t)
	ts.runTestLoadDataForListPartition2(t)
	ts.runTestLoadDataForListColumnPartition(t)
	ts.runTestLoadDataForListColumnPartition2(t)
}

func TestTLSAuto(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	// Start the server without TLS configure, letting the server create these as AutoTLS is enabled
	connOverrider := func(config *mysql.Config) {
		config.TLSConfig = "skip-verify"
	}
	cli := newTestingServerClient()
	cfg := newTestConfig()
	cfg.Socket = ""
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false
	cfg.Security.AutoTLS = true
	cfg.Security.RSAKeySize = 528 // Reduces unittest runtime
	err := os.MkdirAll(cfg.TempStoragePath, 0700)
	require.NoError(t, err)
	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 100)
	err = cli.runTestTLSConnection(t, connOverrider) // Relying on automatically created TLS certificates
	require.NoError(t, err)

	server.Close()
}

func TestTLSBasic(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	// Generate valid TLS certificates.
	caCert, caKey, err := generateCert(0, "TiDB CA", nil, nil, "/tmp/ca-key.pem", "/tmp/ca-cert.pem")
	require.NoError(t, err)
	serverCert, _, err := generateCert(1, "tidb-server", caCert, caKey, "/tmp/server-key.pem", "/tmp/server-cert.pem")
	require.NoError(t, err)
	_, _, err = generateCert(2, "SQL Client Certificate", caCert, caKey, "/tmp/client-key.pem", "/tmp/client-cert.pem")
	require.NoError(t, err)
	err = registerTLSConfig("client-certificate", "/tmp/ca-cert.pem", "/tmp/client-cert.pem", "/tmp/client-key.pem", "tidb-server", true)
	require.NoError(t, err)

	defer func() {
		err := os.Remove("/tmp/ca-key.pem")
		require.NoError(t, err)
		err = os.Remove("/tmp/ca-cert.pem")
		require.NoError(t, err)
		err = os.Remove("/tmp/server-key.pem")
		require.NoError(t, err)
		err = os.Remove("/tmp/server-cert.pem")
		require.NoError(t, err)
		err = os.Remove("/tmp/client-key.pem")
		require.NoError(t, err)
		err = os.Remove("/tmp/client-cert.pem")
		require.NoError(t, err)
	}()

	// Start the server with TLS but without CA, in this case the server will not verify client's certificate.
	connOverrider := func(config *mysql.Config) {
		config.TLSConfig = "skip-verify"
	}
	cli := newTestingServerClient()
	cfg := newTestConfig()
	cfg.Socket = ""
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false
	cfg.Security = config.Security{
		SSLCert: "/tmp/server-cert.pem",
		SSLKey:  "/tmp/server-key.pem",
	}
	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 100)
	err = cli.runTestTLSConnection(t, connOverrider) // We should establish connection successfully.
	require.NoError(t, err)
	cli.runTestRegression(t, connOverrider, "TLSRegression")
	// Perform server verification.
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "client-certificate"
	}
	err = cli.runTestTLSConnection(t, connOverrider) // We should establish connection successfully.
	require.NoError(t, err, "%v", errors.ErrorStack(err))
	cli.runTestRegression(t, connOverrider, "TLSRegression")

	// Test SSL/TLS session vars
	var v *variable.SessionVars
	stats, err := server.Stats(v)
	require.NoError(t, err)
	_, hasKey := stats["Ssl_server_not_after"]
	require.True(t, hasKey)
	_, hasKey = stats["Ssl_server_not_before"]
	require.True(t, hasKey)
	require.Equal(t, serverCert.NotAfter.Format("Jan _2 15:04:05 2006 MST"), stats["Ssl_server_not_after"])
	require.Equal(t, serverCert.NotBefore.Format("Jan _2 15:04:05 2006 MST"), stats["Ssl_server_not_before"])

	server.Close()
}
