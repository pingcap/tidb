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
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testkit"
)

type tidbTestSuite struct {
	*tidbTestSuiteBase
}

type tidbTestSerialSuite struct {
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

func (ts *tidbTestSuite) SetUpSuite(c *C) {
	metrics.RegisterMetrics()
	ts.tidbTestSuiteBase.SetUpSuite(c)
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

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go ts.server.Run()
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

// this test will change `kv.TxnTotalSizeLimit` which may affect other test suites,
// so we must make it running in serial.
func (ts *tidbTestSerialSuite) TestLoadData(c *C) {
	ts.runTestLoadData(c, ts.server)
	ts.runTestLoadDataWithSelectIntoOutfile(c, ts.server)
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
	cfg.Port = genPort()
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
	go server.Run()
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
	go server.Run()
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
	caCert, err := ioutil.ReadFile(caFile)
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
	go server.Run()
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
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()

	//a fake server client, config is override, just used to run tests
	cli := newTestServerClient()
	cli.runTestRegression(c, func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = "/tmp/tidbtest.sock"
		config.DBName = "test"
		config.Params = map[string]string{"sql_mode": "STRICT_ALL_TABLES"}
	}, "SocketRegression")

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
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certOut.Close()

	keyOut, err := os.OpenFile(outKeyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	keyOut.Close()

	return cert, privateKey, nil
}

// registerTLSConfig registers a mysql client TLS config.
// See https://godoc.org/github.com/go-sql-driver/mysql#RegisterTLSConfig for details.
func registerTLSConfig(configName string, caCertPath string, clientCertPath string, clientKeyPath string, serverName string, verifyServer bool) error {
	rootCertPool := x509.NewCertPool()
	data, err := ioutil.ReadFile(caCertPath)
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
	mysql.RegisterTLSConfig(configName, tlsConfig)
	return nil
}

func (ts *tidbTestSuite) TestSystemTimeZone(c *C) {
	tk := testkit.NewTestKit(c, ts.store)
	cfg := newTestConfig()
	cfg.Port, cfg.Status.StatusPort = genPorts()
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
	_, _, err = generateCert(1, "tidb-server", caCert, caKey, "/tmp/server-key.pem", "/tmp/server-cert.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(2, "SQL Client Certificate", caCert, caKey, "/tmp/client-key.pem", "/tmp/client-cert.pem")
	c.Assert(err, IsNil)
	err = registerTLSConfig("client-certificate", "/tmp/ca-cert.pem", "/tmp/client-cert.pem", "/tmp/client-key.pem", "tidb-server", true)
	c.Assert(err, IsNil)

	defer func() {
		os.Remove("/tmp/ca-key.pem")
		os.Remove("/tmp/ca-cert.pem")
		os.Remove("/tmp/server-key.pem")
		os.Remove("/tmp/server-cert.pem")
		os.Remove("/tmp/client-key.pem")
		os.Remove("/tmp/client-cert.pem")
	}()

	// Start the server without TLS.
	connOverrider := func(config *mysql.Config) {
		config.TLSConfig = "skip-verify"
	}
	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	err = cli.runTestTLSConnection(c, connOverrider) // We should get ErrNoTLS.
	c.Assert(err, NotNil)
	c.Assert(errors.Cause(err).Error(), Equals, mysql.ErrNoTLS.Error())
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
	go server.Run()
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
	go server.Run()
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

	_, err = util.LoadTLSCertificates("", "wrong key", "wrong cert")
	c.Assert(err, NotNil)
	_, err = util.LoadTLSCertificates("wrong ca", "/tmp/server-key.pem", "/tmp/server-cert.pem")
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
	go server.Run()
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
	os.Rename("/tmp/server-key-reload2.pem", "/tmp/server-key-reload.pem")
	os.Rename("/tmp/server-cert-reload2.pem", "/tmp/server-cert-reload.pem")
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
	os.Rename("/tmp/server-key-reload3.pem", "/tmp/server-key-reload.pem")
	os.Rename("/tmp/server-cert-reload3.pem", "/tmp/server-cert-reload.pem")
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
	go server.Run()
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
