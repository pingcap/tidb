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

package tls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	tidbserver "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/internal/testserverclient"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/server/tests/servertestkit"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

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

// isTLSExpiredError checks error is caused by TLS expired.
func isTLSExpiredError(err error) bool {
	err = errors.Cause(err)
	switch inval := err.(type) {
	case x509.CertificateInvalidError:
		if inval.Reason != x509.Expired {
			return false
		}
	case *tls.CertificateVerificationError:
		invalid, ok := inval.Err.(x509.CertificateInvalidError)
		if !ok || invalid.Reason != x509.Expired {
			return false
		}
		return true
	default:
		return false
	}
	return true
}

func TestTLSVerify(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	dir := t.TempDir()

	fileName := func(file string) string {
		return filepath.Join(dir, file)
	}

	// Generate valid TLS certificates.
	caCert, caKey, err := generateCert(0, "TiDB CA", nil, nil, fileName("ca-key.pem"), fileName("ca-cert.pem"))
	require.NoError(t, err)
	_, _, err = generateCert(1, "tidb-server", caCert, caKey, fileName("server-key.pem"), fileName("server-cert.pem"))
	require.NoError(t, err)
	_, _, err = generateCert(2, "SQL Client Certificate", caCert, caKey, fileName("client-key.pem"), fileName("client-cert.pem"))
	require.NoError(t, err)
	err = registerTLSConfig("client-certificate", fileName("ca-cert.pem"), fileName("client-cert.pem"), fileName("client-key.pem"), "tidb-server", true)
	require.NoError(t, err)

	// Start the server with TLS & CA, if the client presents its certificate, the certificate will be verified.
	cli := testserverclient.NewTestServerClient()
	cfg := util2.NewTestConfig()
	cfg.Port = cli.Port
	cfg.Socket = dir + "/tidbtest.sock"
	cfg.Status.ReportStatus = false
	cfg.Security = config.Security{
		SSLCA:   fileName("ca-cert.pem"),
		SSLCert: fileName("server-cert.pem"),
		SSLKey:  fileName("server-key.pem"),
	}
	tidbserver.RunInGoTestChan = make(chan struct{})
	server, err := tidbserver.NewServer(cfg, ts.Tidbdrv)
	require.NoError(t, err)
	server.SetDomain(ts.Domain)
	defer server.Close()

	go func() {
		err := server.Run(nil)
		require.NoError(t, err)
	}()
	<-tidbserver.RunInGoTestChan
	cli.Port = testutil.GetPortFromTCPAddr(server.ListenAddr())
	// The client does not provide a certificate, the connection should succeed.
	err = cli.RunTestTLSConnection(t, nil)
	require.NoError(t, err)
	connOverrider := func(config *mysql.Config) {
		config.TLSConfig = "client-certificate"
	}
	cli.RunTestRegression(t, connOverrider, "TLSRegression")
	// The client provides a valid certificate.
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "client-certificate"
	}
	err = cli.RunTestTLSConnection(t, connOverrider)
	require.NoError(t, err)
	cli.RunTestRegression(t, connOverrider, "TLSRegression")

	require.False(t, isTLSExpiredError(errors.New("unknown test")))
	require.False(t, isTLSExpiredError(x509.CertificateInvalidError{Reason: x509.CANotAuthorizedForThisName}))
	require.True(t, isTLSExpiredError(x509.CertificateInvalidError{Reason: x509.Expired}))

	_, _, err = util.LoadTLSCertificates("", "wrong key", "wrong cert", true, 528)
	require.Error(t, err)
	_, _, err = util.LoadTLSCertificates("wrong ca", fileName("server-key.pem"), fileName("server-cert.pem"), true, 528)
	require.Error(t, err)

	// Test connecting with a client that does not have TLS configured.
	// It can still connect, but it should not be able to change "require_secure_transport" to "ON"
	// because that is a lock-out risk.
	err = cli.RunTestEnableSecureTransport(t, nil)
	require.ErrorContains(t, err, "require_secure_transport can only be set to ON if the connection issuing the change is secure")

	// Success: when using a secure connection, the value of "require_secure_transport" can change to "ON"
	err = cli.RunTestEnableSecureTransport(t, connOverrider)
	require.NoError(t, err)

	// This connection will now fail since the client is not configured to use TLS.
	err = cli.RunTestTLSConnection(t, nil)
	require.ErrorContains(t, err, "Connections using insecure transport are prohibited while --require_secure_transport=ON")

	// However, this connection is successful
	err = cli.RunTestTLSConnection(t, connOverrider)
	require.NoError(t, err)

	// Test socketFile does not require TLS enabled in require-secure-transport.
	// Since this restriction should only apply to TCP connections.
	err = cli.RunTestTLSConnection(t, func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = cfg.Socket
		config.DBName = "test"
	})
	require.NoError(t, err)
}
func TestTLSBasic(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	dir := t.TempDir()

	fileName := func(file string) string {
		return filepath.Join(dir, file)
	}

	// Generate valid TLS certificates.
	caCert, caKey, err := generateCert(0, "TiDB CA", nil, nil, fileName("ca-key.pem"), fileName("ca-cert.pem"))
	require.NoError(t, err)
	serverCert, _, err := generateCert(1, "tidb-server", caCert, caKey, fileName("server-key.pem"), fileName("server-cert.pem"))
	require.NoError(t, err)
	_, _, err = generateCert(2, "SQL Client Certificate", caCert, caKey, fileName("client-key.pem"), fileName("client-cert.pem"))
	require.NoError(t, err)
	err = registerTLSConfig("client-certificate", fileName("ca-cert.pem"), fileName("client-cert.pem"), fileName("client-key.pem"), "tidb-server", true)
	require.NoError(t, err)

	// Start the server with TLS but without CA, in this case the server will not verify client's certificate.
	connOverrider := func(config *mysql.Config) {
		config.TLSConfig = "skip-verify"
	}
	cli := testserverclient.NewTestServerClient()
	cfg := util2.NewTestConfig()
	cfg.Port = cli.Port
	cfg.Status.ReportStatus = false
	cfg.Security = config.Security{
		SSLCert: fileName("server-cert.pem"),
		SSLKey:  fileName("server-key.pem"),
	}
	tidbserver.RunInGoTestChan = make(chan struct{})
	server, err := tidbserver.NewServer(cfg, ts.Tidbdrv)
	require.NoError(t, err)
	server.SetDomain(ts.Domain)
	go func() {
		err := server.Run(nil)
		require.NoError(t, err)
	}()
	<-tidbserver.RunInGoTestChan
	cli.Port = testutil.GetPortFromTCPAddr(server.ListenAddr())
	err = cli.RunTestTLSConnection(t, connOverrider) // We should establish connection successfully.
	require.NoError(t, err)
	cli.RunTestRegression(t, connOverrider, "TLSRegression")
	// Perform server verification.
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "client-certificate"
	}
	err = cli.RunTestTLSConnection(t, connOverrider) // We should establish connection successfully.
	require.NoError(t, err, "%v", errors.ErrorStack(err))
	cli.RunTestRegression(t, connOverrider, "TLSRegression")

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

func TestErrorNoRollback(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	// Generate valid TLS certificates.
	caCert, caKey, err := generateCert(0, "TiDB CA", nil, nil, "/tmp/ca-key-rollback.pem", "/tmp/ca-cert-rollback.pem")
	require.NoError(t, err)
	_, _, err = generateCert(1, "tidb-server", caCert, caKey, "/tmp/server-key-rollback.pem", "/tmp/server-cert-rollback.pem")
	require.NoError(t, err)
	_, _, err = generateCert(2, "SQL Client Certificate", caCert, caKey, "/tmp/client-key-rollback.pem", "/tmp/client-cert-rollback.pem")
	require.NoError(t, err)
	err = registerTLSConfig("client-cert-rollback-test", "/tmp/ca-cert-rollback.pem", "/tmp/client-cert-rollback.pem", "/tmp/client-key-rollback.pem", "tidb-server", true)
	require.NoError(t, err)

	defer func() {
		os.Remove("/tmp/ca-key-rollback.pem")
		os.Remove("/tmp/ca-cert-rollback.pem")

		os.Remove("/tmp/server-key-rollback.pem")
		os.Remove("/tmp/server-cert-rollback.pem")
		os.Remove("/tmp/client-key-rollback.pem")
		os.Remove("/tmp/client-cert-rollback.pem")
	}()

	cli := testserverclient.NewTestServerClient()
	cfg := util2.NewTestConfig()
	cfg.Port = cli.Port
	cfg.Status.ReportStatus = false

	cfg.Security = config.Security{
		SSLCA:   "wrong path",
		SSLCert: "wrong path",
		SSLKey:  "wrong path",
	}
	_, err = tidbserver.NewServer(cfg, ts.Tidbdrv)
	require.Error(t, err)

	// test reload tls fail with/without "error no rollback option"
	cfg.Security = config.Security{
		SSLCA:   "/tmp/ca-cert-rollback.pem",
		SSLCert: "/tmp/server-cert-rollback.pem",
		SSLKey:  "/tmp/server-key-rollback.pem",
	}
	tidbserver.RunInGoTestChan = make(chan struct{})
	server, err := tidbserver.NewServer(cfg, ts.Tidbdrv)
	require.NoError(t, err)
	server.SetDomain(ts.Domain)
	go func() {
		err := server.Run(nil)
		require.NoError(t, err)
	}()
	defer server.Close()
	<-tidbserver.RunInGoTestChan
	cli.Port = testutil.GetPortFromTCPAddr(server.ListenAddr())
	connOverrider := func(config *mysql.Config) {
		config.TLSConfig = "client-cert-rollback-test"
	}
	err = cli.RunTestTLSConnection(t, connOverrider)
	require.NoError(t, err)
	os.Remove("/tmp/server-key-rollback.pem")
	err = cli.RunReloadTLS(t, connOverrider, false)
	require.Error(t, err)
	tlsCfg := server.GetTLSConfig()
	require.NotNil(t, tlsCfg)
	err = cli.RunReloadTLS(t, connOverrider, true)
	require.NoError(t, err)
	tlsCfg = server.GetTLSConfig()
	require.Nil(t, tlsCfg)
}

func TestReloadTLS(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	// Generate valid TLS certificates.
	caCert, caKey, err := generateCert(0, "TiDB CA", nil, nil, "/tmp/ca-key-reload.pem", "/tmp/ca-cert-reload.pem")
	require.NoError(t, err)
	_, _, err = generateCert(1, "tidb-server", caCert, caKey, "/tmp/server-key-reload.pem", "/tmp/server-cert-reload.pem")
	require.NoError(t, err)
	_, _, err = generateCert(2, "SQL Client Certificate", caCert, caKey, "/tmp/client-key-reload.pem", "/tmp/client-cert-reload.pem")
	require.NoError(t, err)
	err = registerTLSConfig("client-certificate-reload", "/tmp/ca-cert-reload.pem", "/tmp/client-cert-reload.pem", "/tmp/client-key-reload.pem", "tidb-server", true)
	require.NoError(t, err)

	defer func() {
		os.Remove("/tmp/ca-key-reload.pem")
		os.Remove("/tmp/ca-cert-reload.pem")

		os.Remove("/tmp/server-key-reload.pem")
		os.Remove("/tmp/server-cert-reload.pem")
		os.Remove("/tmp/client-key-reload.pem")
		os.Remove("/tmp/client-cert-reload.pem")
	}()

	// try old cert used in startup configuration.
	cli := testserverclient.NewTestServerClient()
	cfg := util2.NewTestConfig()
	cfg.Port = cli.Port
	cfg.Status.ReportStatus = false
	cfg.Security = config.Security{
		SSLCA:   "/tmp/ca-cert-reload.pem",
		SSLCert: "/tmp/server-cert-reload.pem",
		SSLKey:  "/tmp/server-key-reload.pem",
	}
	tidbserver.RunInGoTestChan = make(chan struct{})
	server, err := tidbserver.NewServer(cfg, ts.Tidbdrv)
	require.NoError(t, err)
	server.SetDomain(ts.Domain)
	go func() {
		err := server.Run(nil)
		require.NoError(t, err)
	}()
	<-tidbserver.RunInGoTestChan
	cli.Port = testutil.GetPortFromTCPAddr(server.ListenAddr())
	// The client provides a valid certificate.
	connOverrider := func(config *mysql.Config) {
		config.TLSConfig = "client-certificate-reload"
	}
	err = cli.RunTestTLSConnection(t, connOverrider)
	require.NoError(t, err)

	// try reload a valid cert.
	tlsCfg := server.GetTLSConfig()
	cert, err := x509.ParseCertificate(tlsCfg.Certificates[0].Certificate[0])
	require.NoError(t, err)
	oldExpireTime := cert.NotAfter
	_, _, err = generateCert(1, "tidb-server", caCert, caKey, "/tmp/server-key-reload2.pem", "/tmp/server-cert-reload2.pem", func(c *x509.Certificate) {
		c.NotBefore = time.Now().Add(-24 * time.Hour).UTC()
		c.NotAfter = time.Now().Add(1 * time.Hour).UTC()
	})
	require.NoError(t, err)
	err = os.Rename("/tmp/server-key-reload2.pem", "/tmp/server-key-reload.pem")
	require.NoError(t, err)
	err = os.Rename("/tmp/server-cert-reload2.pem", "/tmp/server-cert-reload.pem")
	require.NoError(t, err)
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "skip-verify"
	}
	err = cli.RunReloadTLS(t, connOverrider, false)
	require.NoError(t, err)
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "client-certificate-reload"
	}
	err = cli.RunTestTLSConnection(t, connOverrider)
	require.NoError(t, err)

	tlsCfg = server.GetTLSConfig()
	cert, err = x509.ParseCertificate(tlsCfg.Certificates[0].Certificate[0])
	require.NoError(t, err)
	newExpireTime := cert.NotAfter
	require.True(t, newExpireTime.After(oldExpireTime))

	// try reload a expired cert.
	_, _, err = generateCert(1, "tidb-server", caCert, caKey, "/tmp/server-key-reload3.pem", "/tmp/server-cert-reload3.pem", func(c *x509.Certificate) {
		c.NotBefore = time.Now().Add(-24 * time.Hour).UTC()
		c.NotAfter = c.NotBefore.Add(1 * time.Hour).UTC()
	})
	require.NoError(t, err)
	err = os.Rename("/tmp/server-key-reload3.pem", "/tmp/server-key-reload.pem")
	require.NoError(t, err)
	err = os.Rename("/tmp/server-cert-reload3.pem", "/tmp/server-cert-reload.pem")
	require.NoError(t, err)
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "skip-verify"
	}
	err = cli.RunReloadTLS(t, connOverrider, false)
	require.NoError(t, err)
	connOverrider = func(config *mysql.Config) {
		config.TLSConfig = "client-certificate-reload"
	}
	err = cli.RunTestTLSConnection(t, connOverrider)
	require.NotNil(t, err)
	require.Truef(t, isTLSExpiredError(err), "real error is %+v", err)
	server.Close()
}

func TestStatusAPIWithTLS(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	dir := t.TempDir()

	fileName := func(file string) string {
		return filepath.Join(dir, file)
	}

	caCert, caKey, err := generateCert(0, "TiDB CA 2", nil, nil, fileName("ca-key-2.pem"), fileName("ca-cert-2.pem"))
	require.NoError(t, err)
	_, _, err = generateCert(1, "tidb-server-2", caCert, caKey, fileName("server-key-2.pem"), fileName("server-cert-2.pem"))
	require.NoError(t, err)

	cli := testserverclient.NewTestServerClient()
	cli.StatusScheme = "https"
	cfg := util2.NewTestConfig()
	cfg.Port = cli.Port
	cfg.Status.StatusPort = cli.StatusPort
	cfg.Security.ClusterSSLCA = fileName("ca-cert-2.pem")
	cfg.Security.ClusterSSLCert = fileName("server-cert-2.pem")
	cfg.Security.ClusterSSLKey = fileName("server-key-2.pem")
	tidbserver.RunInGoTestChan = make(chan struct{})
	server, err := tidbserver.NewServer(cfg, ts.Tidbdrv)
	require.NoError(t, err)

	go func() {
		err := server.Run(nil)
		require.NoError(t, err)
	}()
	<-tidbserver.RunInGoTestChan
	cli.Port = testutil.GetPortFromTCPAddr(server.ListenAddr())
	cli.StatusPort = testutil.GetPortFromTCPAddr(server.StatusListenerAddr())
	// https connection should work.
	ts.RunTestStatusAPI(t)

	// but plain http connection should fail.
	cli.StatusScheme = "http"
	//nolint:bodyclose
	_, err = cli.FetchStatus("/status")
	require.Error(t, err)

	server.Close()
}

func TestStatusAPIWithTLSCNCheck(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

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

	cli := testserverclient.NewTestServerClient()
	cli.StatusScheme = "https"
	cfg := util2.NewTestConfig()
	cfg.Port = cli.Port
	cfg.Status.StatusPort = cli.StatusPort
	cfg.Security.ClusterSSLCA = caPath
	cfg.Security.ClusterSSLCert = serverCertPath
	cfg.Security.ClusterSSLKey = serverKeyPath
	cfg.Security.ClusterVerifyCN = []string{"tidb-client-2"}
	tidbserver.RunInGoTestChan = make(chan struct{})
	server, err := tidbserver.NewServer(cfg, ts.Tidbdrv)
	require.NoError(t, err)

	go func() {
		err := server.Run(nil)
		require.NoError(t, err)
	}()
	<-tidbserver.RunInGoTestChan
	cli.Port = testutil.GetPortFromTCPAddr(server.ListenAddr())
	cli.StatusPort = testutil.GetPortFromTCPAddr(server.StatusListenerAddr())
	defer server.Close()
	time.Sleep(time.Millisecond * 100)

	hc := newTLSHttpClient(t, caPath,
		client1CertPath,
		client1KeyPath,
	)
	//nolint:bodyclose
	_, err = hc.Get(cli.StatusURL("/status"))
	require.Error(t, err)

	hc = newTLSHttpClient(t, caPath,
		client2CertPath,
		client2KeyPath,
	)
	resp, err := hc.Get(cli.StatusURL("/status"))
	require.NoError(t, err)
	require.Nil(t, resp.Body.Close())
}

func TestInvalidTLS(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	cfg := util2.NewTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	cfg.Security = config.Security{
		SSLCA:   "bogus-ca-cert.pem",
		SSLCert: "bogus-server-cert.pem",
		SSLKey:  "bogus-server-key.pem",
	}
	_, err := tidbserver.NewServer(cfg, ts.Tidbdrv)
	require.Error(t, err)
}

func TestTLSAuto(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)

	// Start the server without TLS configure, letting the server create these as AutoTLS is enabled
	connOverrider := func(config *mysql.Config) {
		config.TLSConfig = "skip-verify"
	}
	cli := testserverclient.NewTestServerClient()
	cfg := util2.NewTestConfig()
	cfg.Port = cli.Port
	cfg.Status.ReportStatus = false
	cfg.Security.AutoTLS = true
	cfg.Security.RSAKeySize = 528 // Reduces unittest runtime
	tidbserver.RunInGoTestChan = make(chan struct{})
	err := os.MkdirAll(cfg.TempStoragePath, 0700)
	require.NoError(t, err)
	server, err := tidbserver.NewServer(cfg, ts.Tidbdrv)
	require.NoError(t, err)
	server.SetDomain(ts.Domain)
	go func() {
		err := server.Run(nil)
		require.NoError(t, err)
	}()
	<-tidbserver.RunInGoTestChan
	cli.Port = testutil.GetPortFromTCPAddr(server.ListenAddr())
	err = cli.RunTestTLSConnection(t, connOverrider) // Relying on automatically created TLS certificates
	require.NoError(t, err)

	server.Close()
}
