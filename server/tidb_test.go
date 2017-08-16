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
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/config"
)

type TidbTestSuite struct {
	tidbdrv *TiDBDriver
	server  *Server
}

var suite = new(TidbTestSuite)
var _ = Suite(suite)

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
		SerialNumber: big.NewInt(int64(sn)),
		Subject: pkix.Name{
			Organization: []string{"PingCAP"},
			CommonName:   commonName,
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
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

func (ts *TidbTestSuite) SetUpSuite(c *C) {
	log.SetLevelByString("error")
	store, err := tidb.NewStore("memory:///tmp/tidb")
	c.Assert(err, IsNil)
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(store)
	cfg := &config.Config{
		Addr:         ":4001",
		LogLevel:     "debug",
		StatusAddr:   ":10090",
		ReportStatus: true,
		TCPKeepAlive: true,
	}

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go ts.server.Run()
	waitUntilServerOnline(cfg.StatusAddr)

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
		runTestRegression(c, "Regression")
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
	runTestLoadData(c)
}

func (ts *TidbTestSuite) TestConcurrentUpdate(c *C) {
	runTestConcurrentUpdate(c)
}

func (ts *TidbTestSuite) TestErrorCode(c *C) {
	runTestErrorCode(c)
}

func (ts *TidbTestSuite) TestAuth(c *C) {
	runTestAuth(c)
}

func (ts *TidbTestSuite) TestIssues(c *C) {
	runTestIssue3662(c)
	runTestIssue3680(c)
	runTestIssue3682(c)
	// runTestIssue3713(c)
}

func (ts *TidbTestSuite) TestResultFieldTableIsNull(c *C) {
	c.Parallel()
	runTestResultFieldTableIsNull(c)
}

func (ts *TidbTestSuite) TestStatusAPI(c *C) {
	runTestStatusAPI(c)
}

func (ts *TidbTestSuite) TestMultiStatements(c *C) {
	c.Parallel()
	runTestMultiStatements(c)
}

func (ts *TidbTestSuite) TestSocket(c *C) {
	cfg := &config.Config{
		LogLevel:   "debug",
		StatusAddr: ":10091",
		Socket:     "/tmp/tidbtest.sock",
	}

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()

	originalDsnConfig := defaultDSNConfig
	defaultDSNConfig = map[string]interface{}{
		"User":   "root",
		"Net":    "unix",
		"Addr":   "/tmp/tidbtest.sock",
		"DBName": "test",
		"Strict": true,
	}
	runTestRegression(c, "SocketRegression")
	defaultDSNConfig = originalDsnConfig
}

// func (ts *TidbTestSuite) TestTLS(c *C) {
// 	caCert, caKey, err := generateCert(0, "TiDB CA", nil, nil, "/tmp/ca-key.pem", "/tmp/ca-cert.pem")
// 	if err != nil {
// 		log.Fatal(errors.ErrorStack(errors.Trace(err)))
// 	}
// 	_, _, err = generateCert(1, "TiDB Server Certificate", caCert, caKey, "/tmp/server-key.pem", "/tmp/server-cert.pem")
// 	if err != nil {
// 		log.Fatal(errors.ErrorStack(errors.Trace(err)))
// 	}

// 	cfg := &config.Config{
// 		Addr:         ":4002",
// 		LogLevel:     "debug",
// 		StatusAddr:   ":10091",
// 		ReportStatus: true,
// 		TCPKeepAlive: true,
// 		//SSLCertPath:  "/tmp/server-cert.pem",
// 		//SSLKeyPath:   "/tmp/server-key.pem",
// 	}

// 	server, err := NewServer(cfg, ts.tidbdrv)
// 	c.Assert(err, IsNil)
// 	ts.server = server
// 	go ts.server.Run()
// 	time.Sleep(time.Millisecond * 100)
// 	defer server.Close()

// 	originalDsnConfig := defaultDSNConfig
// 	defaultDSNConfig = map[string]interface{}{
// 		"User":      "root",
// 		"Net":       "tcp",
// 		"Addr":      "localhost:4002",
// 		"DBName":    "test",
// 		"Strict":    true,
// 		"TLSConfig": "true",
// 	}
// 	runTestRegression(c, "TLSRegression")
// 	defaultDSNConfig = originalDsnConfig
// }
