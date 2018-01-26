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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io/ioutil"
	"math/big"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/config"
	tmysql "github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/store/tikv"
	goctx "golang.org/x/net/context"
)

type TidbTestSuite struct {
	tidbdrv *TiDBDriver
	server  *Server
}

var suite = new(TidbTestSuite)
var _ = Suite(suite)

func (ts *TidbTestSuite) SetUpSuite(c *C) {
	store, err := tikv.NewMockTikvStore()
	tidb.SetStatsLease(0)
	c.Assert(err, IsNil)
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(store)
	cfg := config.NewConfig()
	cfg.Port = 4001
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = 10090
	cfg.Performance.TCPKeepAlive = true

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go ts.server.Run()
	waitUntilServerOnline(cfg.Status.StatusPort)

	// Run this test here because parallel would affect the result of it.
	runTestStmtCount(c)
	defaultLoadDataBatchCnt = 3
}

func (ts *TidbTestSuite) TearDownSuite(c *C) {
	if ts.server != nil {
		ts.server.Close()
	}
}

func (ts *TidbTestSuite) TestRegression(c *C) {
	if regression {
		c.Parallel()
		runTestRegression(c, nil, "Regression")
	}
}

func (ts *TidbTestSuite) TestUint64(c *C) {
	runTestPrepareResultFieldType(c)
}

func (ts *TidbTestSuite) TestSpecialType(c *C) {
	c.Parallel()
	runTestSpecialType(c)
}

func (ts *TidbTestSuite) TestPreparedString(c *C) {
	c.Parallel()
	runTestPreparedString(c)
}

func (ts *TidbTestSuite) TestLoadData(c *C) {
	c.Parallel()
	runTestLoadData(c, suite.server)
}

func (ts *TidbTestSuite) TestConcurrentUpdate(c *C) {
	c.Parallel()
	runTestConcurrentUpdate(c)
}

func (ts *TidbTestSuite) TestErrorCode(c *C) {
	c.Parallel()
	runTestErrorCode(c)
}

func (ts *TidbTestSuite) TestAuth(c *C) {
	c.Parallel()
	runTestAuth(c)
	runTestIssue3682(c)
}

func (ts *TidbTestSuite) TestIssues(c *C) {
	c.Parallel()
	runTestIssue3662(c)
	runTestIssue3680(c)
}

func (ts *TidbTestSuite) TestDBNameEscape(c *C) {
	c.Parallel()
	runTestDBNameEscape(c)
}

func (ts *TidbTestSuite) TestResultFieldTableIsNull(c *C) {
	c.Parallel()
	runTestResultFieldTableIsNull(c)
}

func (ts *TidbTestSuite) TestStatusAPI(c *C) {
	c.Parallel()
	runTestStatusAPI(c)
}

func (ts *TidbTestSuite) TestMultiStatements(c *C) {
	c.Parallel()
	runTestMultiStatements(c)
}

func (ts *TidbTestSuite) TestSocket(c *C) {
	cfg := config.NewConfig()
	cfg.Socket = "/tmp/tidbtest.sock"
	cfg.Status.StatusPort = 10091

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()

	runTestRegression(c, func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = "/tmp/tidbtest.sock"
		config.DBName = "test"
		config.Strict = true
	}, "SocketRegression")
}

// generateCert generates a private key and a certificate in PEM format based on parameters.
// If parentCert and parentCertKey is specified, the new certificate will be signed by the parentCert.
// Otherwise, the new certificate will be self-signed and is a CA.
func generateCert(sn int, commonName string, parentCert *x509.Certificate, parentCertKey *rsa.PrivateKey, outKeyFile string, outCertFile string) (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 512)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	notBefore := time.Now().Add(-10 * time.Minute).UTC()
	notAfter := notBefore.Add(1 * time.Hour).UTC()

	template := x509.Certificate{
		SerialNumber:          big.NewInt(int64(sn)),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
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

func (ts *TidbTestSuite) TestTLS(c *C) {
	// Generate valid TLS certificates.
	caCert, caKey, err := generateCert(0, "TiDB CA", nil, nil, "/tmp/ca-key.pem", "/tmp/ca-cert.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(1, "TiDB Server Certificate", caCert, caKey, "/tmp/server-key.pem", "/tmp/server-cert.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(2, "SQL Client Certificate", caCert, caKey, "/tmp/client-key.pem", "/tmp/client-cert.pem")
	c.Assert(err, IsNil)
	err = registerTLSConfig("client-certificate", "/tmp/ca-cert.pem", "/tmp/client-cert.pem", "/tmp/client-key.pem", "TiDB Server Certificate", true)
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
		config.Addr = "localhost:4002"
	}
	cfg := config.NewConfig()
	cfg.Port = 4002
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = 10091
	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	err = runTestTLSConnection(c, connOverrider) // We should get ErrNoTLS.
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, mysql.ErrNoTLS.Error())
	server.Close()

	// Start the server with TLS but without CA, in this case the server will not verify client's certificate.
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "skip-verify"
		config.Addr = "localhost:4003"
	}
	cfg = config.NewConfig()
	cfg.Port = 4003
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = 10091
	cfg.Security = config.Security{
		SSLCert: "/tmp/server-cert.pem",
		SSLKey:  "/tmp/server-key.pem",
	}
	server, err = NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	err = runTestTLSConnection(c, connOverrider) // We should establish connection successfully.
	c.Assert(err, IsNil)
	runTestRegression(c, connOverrider, "TLSRegression")
	// Perform server verification.
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "client-certificate"
		config.Addr = "localhost:4003"
	}
	err = runTestTLSConnection(c, connOverrider) // We should establish connection successfully.
	c.Assert(err, IsNil)
	runTestRegression(c, connOverrider, "TLSRegression")
	server.Close()

	// Start the server with TLS & CA, if the client presents its certificate, the certificate will be verified.
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "client-certificate"
		config.Addr = "localhost:4004"
	}
	cfg = config.NewConfig()
	cfg.Port = 4004
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = 10091
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
	connOverrider = func(config *mysql.Config) {
		config.Addr = "localhost:4004"
	}
	err = runTestTLSConnection(c, connOverrider)
	c.Assert(err, IsNil)
	runTestRegression(c, connOverrider, "TLSRegression")
	// The client provides a valid certificate.
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "client-certificate"
		config.Addr = "localhost:4004"
	}
	err = runTestTLSConnection(c, connOverrider)
	c.Assert(err, IsNil)
	runTestRegression(c, connOverrider, "TLSRegression")
	server.Close()
}

func (ts *TidbTestSuite) TestClientWithCollation(c *C) {
	c.Parallel()
	runTestClientWithCollation(c)
}

func (ts *TidbTestSuite) TestCreateTableFlen(c *C) {
	// issue #4540
	ctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	_, err = ctx.Execute(goctx.Background(), "use test;")
	c.Assert(err, IsNil)

	goCtx := goctx.Background()
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
	_, err = ctx.Execute(goCtx, testSQL)
	c.Assert(err, IsNil)
	rs, err := ctx.Execute(goCtx, "show create table t1")
	row, err := rs[0].Next(goCtx)
	c.Assert(err, IsNil)
	cols := rs[0].Columns()
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 2)
	c.Assert(int(cols[0].ColumnLength), Equals, 5*tmysql.MaxBytesOfCharacter)
	c.Assert(int(cols[1].ColumnLength), Equals, len(row.GetString(1))*tmysql.MaxBytesOfCharacter)

	// for issue#5246
	rs, err = ctx.Execute(goCtx, "select y, z from t1")
	c.Assert(err, IsNil)
	cols = rs[0].Columns()
	c.Assert(len(cols), Equals, 2)
	c.Assert(int(cols[0].ColumnLength), Equals, 21)
	c.Assert(int(cols[1].ColumnLength), Equals, 22)
}

func checkColNames(c *C, columns []*ColumnInfo, names ...string) {
	for i, name := range names {
		c.Assert(columns[i].Name, Equals, name)
		c.Assert(columns[i].OrgName, Equals, name)
	}
}

func (ts *TidbTestSuite) TestFieldList(c *C) {
	ctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	_, err = ctx.Execute(goctx.Background(), "use test;")
	c.Assert(err, IsNil)

	goCtx := goctx.Background()
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
	_, err = ctx.Execute(goCtx, testSQL)
	c.Assert(err, IsNil)
	colInfos, err := ctx.FieldList("t")
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
			c.Assert(col.Charset, Equals, uint16(tmysql.CharsetIDs["utf8"]), Commentf("index %d", i))
			continue
		}

		c.Assert(col.Charset, Equals, uint16(tmysql.CharsetIDs["binary"]), Commentf("index %d", i))
	}

	// c_decimal decimal(6, 3)
	c.Assert(colInfos[5].Decimal, Equals, uint8(3))
}

func (ts *TidbTestSuite) TestSumAvg(c *C) {
	c.Parallel()
	runTestSumAvg(c)
}
