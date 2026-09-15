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

package tls

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	stderrors "errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/config"
	tidbserver "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/internal/testserverclient"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/server/tests/servertestkit"
	tidbtls "github.com/pingcap/tidb/pkg/util/tls"
	"github.com/spiffe/go-spiffe/v2/proto/spiffe/workload"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const (
	spiffeTestServerID       = "spiffe://example.org/tidb/server"
	spiffeTestClientID       = "spiffe://example.org/client/allowed"
	spiffeTestWrongClientID  = "spiffe://example.org/client/wrong"
	spiffeTestSQLUser        = "spiffe_tls_user"
	spiffeTestSQLAccessError = uint16(1045)
)

var spiffeTestSocketSequence atomic.Uint64

type spiffeTestCertificate struct {
	certificate *x509.Certificate
	privateKey  *ecdsa.PrivateKey
}

type spiffeTestCA struct {
	certificate *x509.Certificate
	privateKey  *ecdsa.PrivateKey
}

func newSPIFFETestCA(t *testing.T, serial int64, commonName string) *spiffeTestCA {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		SubjectKeyId:          []byte(commonName),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &spiffeTestCA{certificate: certificate, privateKey: privateKey}
}

func (ca *spiffeTestCA) issueCertificate(t *testing.T, serial int64, spiffeID string, notAfter time.Time, usages ...x509.ExtKeyUsage) *spiffeTestCertificate {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:   big.NewInt(serial),
		Subject:        pkix.Name{CommonName: fmt.Sprintf("SPIFFE test certificate %d", serial)},
		NotBefore:      time.Now().Add(-time.Minute).UTC(),
		NotAfter:       notAfter.UTC(),
		KeyUsage:       x509.KeyUsageDigitalSignature,
		ExtKeyUsage:    usages,
		AuthorityKeyId: ca.certificate.SubjectKeyId,
	}
	if spiffeID != "" {
		uri, err := url.Parse(spiffeID)
		require.NoError(t, err)
		template.URIs = []*url.URL{uri}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, &privateKey.PublicKey, ca.privateKey)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &spiffeTestCertificate{certificate: certificate, privateKey: privateKey}
}

func (c *spiffeTestCertificate) tlsCertificate() tls.Certificate {
	return tls.Certificate{
		Certificate: [][]byte{c.certificate.Raw},
		PrivateKey:  c.privateKey,
		Leaf:        c.certificate,
	}
}

func newSPIFFEX509Context(t *testing.T, server *spiffeTestCertificate, bundle *spiffeTestCA) *workload.X509SVIDResponse {
	t.Helper()

	privateKey, err := x509.MarshalPKCS8PrivateKey(server.privateKey)
	require.NoError(t, err)
	return &workload.X509SVIDResponse{
		Svids: []*workload.X509SVID{{
			SpiffeId:    spiffeTestServerID,
			X509Svid:    server.certificate.Raw,
			X509SvidKey: privateKey,
			Bundle:      bundle.certificate.Raw,
		}},
		FederatedBundles: map[string][]byte{},
	}
}

type spiffeTestWorkloadAPI struct {
	workload.UnimplementedSpiffeWorkloadAPIServer

	mu       sync.Mutex
	response *workload.X509SVIDResponse
	watchers map[chan *workload.X509SVIDResponse]struct{}

	server     *grpc.Server
	listener   net.Listener
	socketPath string
	done       chan struct{}
	stopOnce   sync.Once
}

func newSPIFFETestWorkloadAPI(t *testing.T, response *workload.X509SVIDResponse) *spiffeTestWorkloadAPI {
	t.Helper()

	socketPath := filepath.Join("/tmp", fmt.Sprintf("tidb-spiffe-%d-%d.sock", os.Getpid(), spiffeTestSocketSequence.Add(1)))
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	api := &spiffeTestWorkloadAPI{
		response:   response,
		watchers:   make(map[chan *workload.X509SVIDResponse]struct{}),
		server:     grpc.NewServer(),
		listener:   listener,
		socketPath: socketPath,
		done:       make(chan struct{}),
	}
	workload.RegisterSpiffeWorkloadAPIServer(api.server, api)
	go func() {
		defer close(api.done)
		_ = api.server.Serve(listener)
	}()
	t.Cleanup(api.Stop)
	return api
}

func (api *spiffeTestWorkloadAPI) Addr() string {
	return (&url.URL{Scheme: "unix", Path: api.socketPath}).String()
}

func (api *spiffeTestWorkloadAPI) SetX509Context(response *workload.X509SVIDResponse) {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.response = response
	for watcher := range api.watchers {
		select {
		case watcher <- response:
		default:
			<-watcher
			watcher <- response
		}
	}
}

func (api *spiffeTestWorkloadAPI) FetchX509SVID(_ *workload.X509SVIDRequest, stream workload.SpiffeWorkloadAPI_FetchX509SVIDServer) error {
	updates := make(chan *workload.X509SVIDResponse, 1)
	api.mu.Lock()
	api.watchers[updates] = struct{}{}
	response := api.response
	api.mu.Unlock()
	defer func() {
		api.mu.Lock()
		delete(api.watchers, updates)
		api.mu.Unlock()
	}()

	if response != nil {
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	for {
		select {
		case response := <-updates:
			if err := stream.Send(response); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (api *spiffeTestWorkloadAPI) Stop() {
	api.stopOnce.Do(func() {
		api.server.Stop()
		<-api.done
		_ = os.Remove(api.socketPath)
	})
}

func registerSPIFFETestTLSConfig(t *testing.T, name string, certificate *spiffeTestCertificate, observedServerCertificates chan<- *x509.Certificate) {
	t.Helper()

	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
	}
	if certificate != nil {
		clientCertificate := certificate.tlsCertificate()
		tlsConfig.Certificates = []tls.Certificate{clientCertificate}
		// Go normally withholds a client certificate whose issuer is not in the
		// server's acceptable-CA list. Force it onto the wire so the untrusted
		// certificate case exercises the server's TLS rejection path.
		tlsConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &clientCertificate, nil
		}
	}
	if observedServerCertificates != nil {
		tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) > 0 {
				select {
				case observedServerCertificates <- state.PeerCertificates[0]:
				default:
				}
			}
			return nil
		}
	}
	require.NoError(t, mysql.RegisterTLSConfig(name, tlsConfig))
	t.Cleanup(func() { mysql.DeregisterTLSConfig(name) })
}

func openSPIFFETestDB(cli *testserverclient.TestServerClient, user, tlsConfigName string) (*sql.DB, error) {
	dsn := cli.GetDSN(func(cfg *mysql.Config) {
		cfg.User = user
		cfg.TLSConfig = tlsConfigName
	})
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func requireSPIFFETestTLSFailure(t *testing.T, cli *testserverclient.TestServerClient, tlsConfigName string) {
	t.Helper()

	_, err := openSPIFFETestDB(cli, spiffeTestSQLUser, tlsConfigName)
	require.Error(t, err)
	var mysqlError *mysql.MySQLError
	if stderrors.As(err, &mysqlError) {
		require.NotEqual(t, spiffeTestSQLAccessError, mysqlError.Number, "certificate unexpectedly reached SQL authorization: %v", err)
	}
}

func TestSPIFFEWorkloadAPITLS(t *testing.T) {
	originalConfig := *config.GetGlobalConfig()
	originalRequireSecureTransport := tidbtls.RequireSecureTransport.Load()
	t.Cleanup(func() {
		config.StoreGlobalConfig(&originalConfig)
		tidbtls.RequireSecureTransport.Store(originalRequireSecureTransport)
	})
	tidbtls.RequireSecureTransport.Store(false)

	now := time.Now().UTC()
	firstCA := newSPIFFETestCA(t, 1, "SPIFFE test CA 1")
	secondCA := newSPIFFETestCA(t, 2, "SPIFFE test CA 2")
	untrustedCA := newSPIFFETestCA(t, 3, "SPIFFE untrusted CA")
	firstServer := firstCA.issueCertificate(t, 11, spiffeTestServerID, now.Add(45*time.Minute), x509.ExtKeyUsageServerAuth)
	secondServer := secondCA.issueCertificate(t, 12, spiffeTestServerID, now.Add(2*time.Hour), x509.ExtKeyUsageServerAuth)
	matchingClient := firstCA.issueCertificate(t, 21, spiffeTestClientID, now.Add(time.Hour), x509.ExtKeyUsageClientAuth)
	wrongClient := firstCA.issueCertificate(t, 22, spiffeTestWrongClientID, now.Add(time.Hour), x509.ExtKeyUsageClientAuth)
	nonSPIFFEClient := firstCA.issueCertificate(t, 23, "", now.Add(time.Hour), x509.ExtKeyUsageClientAuth)
	untrustedClient := untrustedCA.issueCertificate(t, 24, spiffeTestClientID, now.Add(time.Hour), x509.ExtKeyUsageClientAuth)
	rotatedClient := secondCA.issueCertificate(t, 25, spiffeTestClientID, now.Add(time.Hour), x509.ExtKeyUsageClientAuth)

	workloadAPI := newSPIFFETestWorkloadAPI(t, newSPIFFEX509Context(t, firstServer, firstCA))

	testSuite := servertestkit.CreateTidbTestSuite(t)
	cli := testserverclient.NewTestServerClient()
	cfg := util2.NewTestConfig()
	cfg.Port = 0
	cfg.Status.ReportStatus = false
	cfg.Security.SPIFFEWorkloadAPIAddr = workloadAPI.Addr()
	cfg.Security.SPIFFEWorkloadAPITimeout = "5s"
	config.StoreGlobalConfig(cfg)

	tidbserver.RunInGoTestChan = make(chan struct{})
	server, err := tidbserver.NewServer(cfg, testSuite.Tidbdrv)
	require.NoError(t, err)
	server.SetDomain(testSuite.Domain)
	runError := make(chan error, 1)
	go func() { runError <- server.Run(nil) }()
	defer func() {
		server.Close()
		require.NoError(t, <-runError)
	}()
	<-tidbserver.RunInGoTestChan
	cli.Port = testutil.GetPortFromTCPAddr(server.ListenAddr())

	observedServerCertificates := make(chan *x509.Certificate, 8)
	registerSPIFFETestTLSConfig(t, "spiffe-test-root", nil, observedServerCertificates)
	registerSPIFFETestTLSConfig(t, "spiffe-test-matching", matchingClient, observedServerCertificates)
	registerSPIFFETestTLSConfig(t, "spiffe-test-wrong", wrongClient, nil)
	registerSPIFFETestTLSConfig(t, "spiffe-test-non-spiffe", nonSPIFFEClient, nil)
	registerSPIFFETestTLSConfig(t, "spiffe-test-untrusted", untrustedClient, nil)
	registerSPIFFETestTLSConfig(t, "spiffe-test-rotated", rotatedClient, observedServerCertificates)

	rootDB, err := openSPIFFETestDB(cli, "root", "spiffe-test-root")
	require.NoError(t, err)
	defer func() { require.NoError(t, rootDB.Close()) }()
	getVariable := func(name string) string {
		var returnedName, value string
		require.NoError(t, rootDB.QueryRow(fmt.Sprintf("SHOW VARIABLES LIKE '%s'", name)).Scan(&returnedName, &value))
		require.Equal(t, name, returnedName)
		return value
	}
	require.Equal(t, "YES", getVariable("have_ssl"))
	require.Equal(t, "YES", getVariable("have_openssl"))
	require.Empty(t, getVariable("ssl_ca"))
	require.Empty(t, getVariable("ssl_cert"))
	require.Empty(t, getVariable("ssl_key"))
	_, err = rootDB.Exec(fmt.Sprintf("CREATE USER '%s'@'%%' REQUIRE SAN 'URI:%s'", spiffeTestSQLUser, spiffeTestClientID))
	require.NoError(t, err)
	_, err = rootDB.Exec(fmt.Sprintf("GRANT ALL ON test.* TO '%s'@'%%'", spiffeTestSQLUser))
	require.NoError(t, err)

	// REQUIRE SAN authorization can only succeed when the TLS stack populated
	// ConnectionState.VerifiedChains with the verified client X.509-SVID.
	existingDB, err := openSPIFFETestDB(cli, spiffeTestSQLUser, "spiffe-test-matching")
	require.NoError(t, err)
	defer func() { require.NoError(t, existingDB.Close()) }()
	var one int
	require.NoError(t, existingDB.QueryRow("SELECT 1").Scan(&one))
	require.Equal(t, 1, one)

	var initialObserved *x509.Certificate
	require.Eventually(t, func() bool {
		select {
		case initialObserved = <-observedServerCertificates:
			return initialObserved.SerialNumber.Cmp(firstServer.certificate.SerialNumber) == 0
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	_, err = openSPIFFETestDB(cli, spiffeTestSQLUser, "spiffe-test-wrong")
	require.Error(t, err)
	var mysqlError *mysql.MySQLError
	require.ErrorAs(t, err, &mysqlError)
	require.Equal(t, spiffeTestSQLAccessError, mysqlError.Number)

	requireSPIFFETestTLSFailure(t, cli, "spiffe-test-untrusted")
	requireSPIFFETestTLSFailure(t, cli, "spiffe-test-non-spiffe")

	workloadAPI.SetX509Context(newSPIFFEX509Context(t, secondServer, secondCA))
	expectedExpiry := secondServer.certificate.NotAfter.Format("Jan _2 15:04:05 2006 MST")
	require.Eventually(t, func() bool {
		stats, err := server.Stats(nil)
		return err == nil && stats["Ssl_server_not_after"] == expectedExpiry
	}, 5*time.Second, 20*time.Millisecond)

	// An established TLS session keeps working after rotation.
	require.NoError(t, existingDB.QueryRow("SELECT 1").Scan(&one))
	require.Equal(t, 1, one)

	// Fresh handshakes atomically use the new SVID and matching trust bundle.
	rotatedDB, err := openSPIFFETestDB(cli, spiffeTestSQLUser, "spiffe-test-rotated")
	require.NoError(t, err)
	require.NoError(t, rotatedDB.Close())
	var rotatedObserved *x509.Certificate
	require.Eventually(t, func() bool {
		select {
		case rotatedObserved = <-observedServerCertificates:
			return rotatedObserved.SerialNumber.Cmp(secondServer.certificate.SerialNumber) == 0
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	requireSPIFFETestTLSFailure(t, cli, "spiffe-test-matching")

	// Losing the socket retains the last-good snapshot, and file-oriented TLS
	// reload statements are successful no-ops while SPIFFE mode is active.
	workloadAPI.Stop()
	for _, statement := range []string{
		"ALTER INSTANCE RELOAD TLS",
		"ALTER INSTANCE RELOAD TLS NO ROLLBACK ON ERROR",
	} {
		_, err = rootDB.Exec(statement)
		require.NoError(t, err)
		freshDB, err := openSPIFFETestDB(cli, spiffeTestSQLUser, "spiffe-test-rotated")
		require.NoError(t, err)
		require.NoError(t, freshDB.Close())
	}
}
