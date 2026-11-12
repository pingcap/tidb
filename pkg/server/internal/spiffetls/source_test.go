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

package spiffetls

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"github.com/stretchr/testify/require"
)

func TestSPIFFETLSConfigClientVerification(t *testing.T) {
	now := time.Now()
	trustDomainA := spiffeid.RequireTrustDomainFromString("example.org")
	trustDomainB := spiffeid.RequireTrustDomainFromString("federated.example")
	authorityA := newTestAuthority(t, "authority-a", 1, now)
	authorityB := newTestAuthority(t, "authority-b", 2, now)
	serverCert, serverKey := newTestLeaf(t, authorityA, testLeafOptions{
		serialNumber: 10,
		spiffeID:     "spiffe://example.org/tidb/server",
		dnsNames:     []string{"server.example.org"},
		extKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		notBefore:    now.Add(-time.Minute),
		notAfter:     now.Add(time.Hour),
	})
	serverSVID := newTestSVID(t, serverCert, serverKey)
	bundleA := x509bundle.FromX509Authorities(trustDomainA, []*x509.Certificate{authorityA.certificate})
	bundleB := x509bundle.FromX509Authorities(trustDomainB, []*x509.Certificate{authorityB.certificate})
	x509Context := &workloadapi.X509Context{
		SVIDs:   []*x509svid.SVID{serverSVID},
		Bundles: x509bundle.NewSet(bundleA, bundleB),
	}

	optionalConfig, err := newTLSConfig(newTestBaseTLSConfig(false), x509Context, false)
	require.NoError(t, err)
	require.Equal(t, tls.VerifyClientCertIfGiven, optionalConfig.ClientAuth)
	require.Len(t, optionalConfig.Certificates, 1)
	require.NotNil(t, optionalConfig.VerifyConnection)

	clientCert, clientKey := newTestLeaf(t, authorityA, testLeafOptions{
		serialNumber: 20,
		spiffeID:     "spiffe://example.org/client",
		extKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		notBefore:    now.Add(-time.Minute),
		notAfter:     now.Add(time.Hour),
	})
	serverState, serverErr, clientErr := runTLSHandshake(optionalConfig, newTestClientTLSConfig(
		authorityA.certificate,
		tlsCertificate(clientCert, clientKey),
	))
	require.NoError(t, serverErr)
	require.NoError(t, clientErr)
	require.NotEmpty(t, serverState.VerifiedChains, "native Go verification must populate VerifiedChains")
	federatedClientCert, federatedClientKey := newTestLeaf(t, authorityB, testLeafOptions{
		serialNumber: 23,
		spiffeID:     "spiffe://federated.example/client",
		extKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		notBefore:    now.Add(-time.Minute),
		notAfter:     now.Add(time.Hour),
	})
	serverState, serverErr, clientErr = runTLSHandshake(optionalConfig, newTestClientTLSConfig(
		authorityA.certificate,
		tlsCertificate(federatedClientCert, federatedClientKey),
	))
	require.NoError(t, serverErr)
	require.NoError(t, clientErr)
	require.NotEmpty(t, serverState.VerifiedChains, "federated client verification must populate VerifiedChains")

	_, serverErr, clientErr = runTLSHandshake(optionalConfig, newTestClientTLSConfig(authorityA.certificate))
	require.NoError(t, serverErr)
	require.NoError(t, clientErr)

	requiredConfig, err := newTLSConfig(newTestBaseTLSConfig(true), x509Context, true)
	require.NoError(t, err)
	require.Equal(t, tls.RequireAndVerifyClientCert, requiredConfig.ClientAuth)
	require.ErrorContains(t, requiredConfig.VerifyConnection(tls.ConnectionState{}), "did not provide")
	_, serverErr, _ = runTLSHandshakeAndCloseClient(requiredConfig, newTestClientTLSConfig(authorityA.certificate))
	require.Error(t, serverErr, "required client-certificate policy must reject a client without a certificate")

	regularClientCert, _ := newTestLeaf(t, authorityA, testLeafOptions{
		serialNumber: 21,
		extKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		notBefore:    now.Add(-time.Minute),
		notAfter:     now.Add(time.Hour),
	})
	_, err = regularClientCert.Verify(x509.VerifyOptions{
		Roots:     optionalConfig.ClientCAs,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	require.NoError(t, err, "the conventional certificate must pass native verification")
	require.ErrorContains(t, optionalConfig.VerifyConnection(tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{regularClientCert},
	}), "verify client X.509-SVID")

	crossDomainCert, _ := newTestLeaf(t, authorityA, testLeafOptions{
		serialNumber: 22,
		spiffeID:     "spiffe://federated.example/client",
		extKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		notBefore:    now.Add(-time.Minute),
		notAfter:     now.Add(time.Hour),
	})
	_, err = crossDomainCert.Verify(x509.VerifyOptions{
		Roots:     optionalConfig.ClientCAs,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	require.NoError(t, err, "aggregated roots alone accept the cross-domain certificate")
	require.ErrorContains(t, optionalConfig.VerifyConnection(tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{crossDomainCert},
	}), "verify client X.509-SVID")

	// The published callback must retain its cloned bundle snapshot even if the
	// Workload API objects are subsequently mutated.
	bundleA.SetX509Authorities([]*x509.Certificate{authorityB.certificate})
	require.NoError(t, optionalConfig.VerifyConnection(tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{clientCert},
	}))
}

func TestSPIFFETLSConfigRejectsInvalidServerSVID(t *testing.T) {
	now := time.Now()
	trustDomain := spiffeid.RequireTrustDomainFromString("example.org")
	authority := newTestAuthority(t, "authority", 1, now)
	bundle := x509bundle.FromX509Authorities(trustDomain, []*x509.Certificate{authority.certificate})
	validCert, validKey := newTestLeaf(t, authority, testLeafOptions{
		serialNumber: 10,
		spiffeID:     "spiffe://example.org/tidb/server",
		extKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		notBefore:    now.Add(-time.Minute),
		notAfter:     now.Add(time.Hour),
	})
	validSVID := newTestSVID(t, validCert, validKey)
	validContext := func(svid *x509svid.SVID) *workloadapi.X509Context {
		return &workloadapi.X509Context{
			SVIDs:   []*x509svid.SVID{svid},
			Bundles: x509bundle.NewSet(bundle),
		}
	}

	_, err := newTLSConfig(newTestBaseTLSConfig(false), validContext(validSVID), false)
	require.NoError(t, err)

	clientOnlyCert, clientOnlyKey := newTestLeaf(t, authority, testLeafOptions{
		serialNumber: 11,
		spiffeID:     "spiffe://example.org/tidb/server",
		extKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		notBefore:    now.Add(-time.Minute),
		notAfter:     now.Add(time.Hour),
	})
	_, err = newTLSConfig(newTestBaseTLSConfig(false), validContext(newTestSVID(t, clientOnlyCert, clientOnlyKey)), false)
	require.ErrorContains(t, err, "server authentication")

	expiredCert, expiredKey := newTestLeaf(t, authority, testLeafOptions{
		serialNumber: 12,
		spiffeID:     "spiffe://example.org/tidb/server",
		extKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		notBefore:    now.Add(-2 * time.Hour),
		notAfter:     now.Add(-time.Hour),
	})
	_, err = newTLSConfig(newTestBaseTLSConfig(false), validContext(newTestSVID(t, expiredCert, expiredKey)), false)
	require.ErrorContains(t, err, "verify default X.509-SVID")

	wrongKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	wrongKeySVID := *validSVID
	wrongKeySVID.PrivateKey = wrongKey
	_, err = newTLSConfig(newTestBaseTLSConfig(false), validContext(&wrongKeySVID), false)
	require.ErrorContains(t, err, "does not match")

	wrongIDSVID := *validSVID
	wrongIDSVID.ID = spiffeid.RequireFromString("spiffe://example.org/other")
	_, err = newTLSConfig(newTestBaseTLSConfig(false), validContext(&wrongIDSVID), false)
	require.ErrorContains(t, err, "does not match certificate ID")

	_, err = newTLSConfig(newTestBaseTLSConfig(false), &workloadapi.X509Context{
		SVIDs:   []*x509svid.SVID{validSVID},
		Bundles: x509bundle.NewSet(),
	}, false)
	require.ErrorContains(t, err, "verify default X.509-SVID")
}

func TestSPIFFESourceRotationLastGoodAndClose(t *testing.T) {
	now := time.Now()
	trustDomain := spiffeid.RequireTrustDomainFromString("example.org")
	authority := newTestAuthority(t, "authority", 1, now)
	bundle := x509bundle.FromX509Authorities(trustDomain, []*x509.Certificate{authority.certificate})
	rotatedAuthority := newTestAuthority(t, "rotated-authority", 2, now)
	rotatedBundle := x509bundle.FromX509Authorities(trustDomain, []*x509.Certificate{rotatedAuthority.certificate})
	contextForSerial := func(signingAuthority testAuthority, trustBundle *x509bundle.Bundle, serial int64) *workloadapi.X509Context {
		certificate, privateKey := newTestLeaf(t, signingAuthority, testLeafOptions{
			serialNumber: serial,
			spiffeID:     "spiffe://example.org/tidb/server",
			extKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			notBefore:    now.Add(-time.Minute),
			notAfter:     now.Add(time.Hour),
		})
		return &workloadapi.X509Context{
			SVIDs:   []*x509svid.SVID{newTestSVID(t, certificate, privateKey)},
			Bundles: x509bundle.NewSet(trustBundle),
		}
	}

	client := newFakeWorkloadAPIClient()
	type sourceResult struct {
		source *Source
		err    error
	}
	resultCh := make(chan sourceResult, 1)
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), time.Second)
	defer cancelStartup()
	go func() {
		source, err := newSource(startupCtx, client, newTestBaseTLSConfig(false), false)
		resultCh <- sourceResult{source: source, err: err}
	}()
	<-client.started
	client.updates <- contextForSerial(authority, bundle, 10)
	<-client.delivered
	result := <-resultCh
	require.NoError(t, result.err)
	source := result.source
	dispatcher := source.TLSConfig()
	require.NotNil(t, dispatcher)
	require.Equal(t, "10", currentSerial(t, dispatcher))

	invalidContext := contextForSerial(authority, bundle, 11)
	wrongKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	invalidContext.SVIDs[0].PrivateKey = wrongKey
	client.updates <- invalidContext
	<-client.delivered
	require.Equal(t, "10", currentSerial(t, dispatcher), "invalid updates must retain the last good snapshot")

	client.watchErrors <- errors.New("socket unavailable")
	<-client.delivered
	require.Equal(t, "10", currentSerial(t, dispatcher), "watch errors must retain the last good snapshot")

	client.updates <- contextForSerial(rotatedAuthority, rotatedBundle, 12)
	<-client.delivered
	require.Same(t, dispatcher, source.TLSConfig(), "the dispatcher must remain stable across rotations")
	require.Equal(t, "12", currentSerial(t, dispatcher))
	certificate, err := dispatcher.GetCertificate(nil)
	require.NoError(t, err)
	require.Equal(t, "12", certificate.Leaf.SerialNumber.String(), "status lookup must see the rotated certificate")
	rotatedClientCert, _ := newTestLeaf(t, rotatedAuthority, testLeafOptions{
		serialNumber: 20,
		spiffeID:     "spiffe://example.org/client",
		extKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		notBefore:    now.Add(-time.Minute),
		notAfter:     now.Add(time.Hour),
	})
	current, err := dispatcher.GetConfigForClient(nil)
	require.NoError(t, err)
	_, err = rotatedClientCert.Verify(x509.VerifyOptions{
		Roots:     current.ClientCAs,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	require.NoError(t, err, "the rotated client CA must be published with the rotated SVID")
	require.NoError(t, current.VerifyConnection(tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{rotatedClientCert},
	}))

	require.NoError(t, source.Close())
	require.NoError(t, source.Close())
	require.Equal(t, int32(1), client.closeCount.Load())
	select {
	case <-client.watchExited:
	default:
		require.Fail(t, "watch goroutine did not exit before Close returned")
	}
}

func TestSPIFFESourceStartupTimeout(t *testing.T) {
	client := newFakeWorkloadAPIClient()
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelStartup()
	source, err := newSource(startupCtx, client, newTestBaseTLSConfig(false), false)
	require.Nil(t, source)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, int32(1), client.closeCount.Load())
	select {
	case <-client.watchExited:
	default:
		require.Fail(t, "watch goroutine did not exit after startup timeout")
	}
}

func TestSPIFFESourceAcceptsInitialUpdateBeforeWatchStops(t *testing.T) {
	now := time.Now()
	authority := newTestAuthority(t, "authority", 1, now)
	certificate, privateKey := newTestLeaf(t, authority, testLeafOptions{
		serialNumber: 10,
		spiffeID:     "spiffe://example.org/tidb/server",
		extKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		notBefore:    now.Add(-time.Minute),
		notAfter:     now.Add(time.Hour),
	})
	client := &updateThenReturnClient{x509Context: &workloadapi.X509Context{
		SVIDs: []*x509svid.SVID{newTestSVID(t, certificate, privateKey)},
		Bundles: x509bundle.NewSet(x509bundle.FromX509Authorities(
			spiffeid.RequireTrustDomainFromString("example.org"),
			[]*x509.Certificate{authority.certificate},
		)),
	}}
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), time.Second)
	defer cancelStartup()
	source, err := newSource(startupCtx, client, newTestBaseTLSConfig(false), false)
	require.NoError(t, err)
	require.Equal(t, "10", currentSerial(t, source.TLSConfig()))
	require.NoError(t, source.Close())
	require.Equal(t, int32(1), client.closeCount.Load())
}

func TestSPIFFESourceAcceptsInitialUpdateAsStartupExpires(t *testing.T) {
	now := time.Now()
	authority := newTestAuthority(t, "authority", 1, now)
	certificate, privateKey := newTestLeaf(t, authority, testLeafOptions{
		serialNumber: 10,
		spiffeID:     "spiffe://example.org/tidb/server",
		extKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		notBefore:    now.Add(-time.Minute),
		notAfter:     now.Add(time.Hour),
	})
	startupCtx, cancelStartup := context.WithCancel(context.Background())
	client := &updateThenCancelClient{
		x509Context: &workloadapi.X509Context{
			SVIDs: []*x509svid.SVID{newTestSVID(t, certificate, privateKey)},
			Bundles: x509bundle.NewSet(x509bundle.FromX509Authorities(
				spiffeid.RequireTrustDomainFromString("example.org"),
				[]*x509.Certificate{authority.certificate},
			)),
		},
		cancelStartup: cancelStartup,
	}
	source, err := newSource(startupCtx, client, newTestBaseTLSConfig(false), false)
	require.NoError(t, err)
	require.Equal(t, "10", currentSerial(t, source.TLSConfig()))
	require.NoError(t, source.Close())
}

func TestSPIFFENewValidatesInputs(t *testing.T) {
	_, err := New("", time.Second, "", false)
	require.ErrorContains(t, err, "address is required")
	_, err = New("tcp://127.0.0.1:1234", time.Second, "", false)
	require.ErrorContains(t, err, "absolute unix:///")
	_, err = New("unix:///tmp/spire-agent.sock", 0, "", false)
	require.ErrorContains(t, err, "timeout must be positive")
}

type testAuthority struct {
	certificate *x509.Certificate
	privateKey  *ecdsa.PrivateKey
}

type testLeafOptions struct {
	serialNumber int64
	spiffeID     string
	dnsNames     []string
	extKeyUsage  []x509.ExtKeyUsage
	notBefore    time.Time
	notAfter     time.Time
}

func newTestAuthority(t *testing.T, commonName string, serialNumber int64, now time.Time) testAuthority {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serialNumber),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return testAuthority{certificate: certificate, privateKey: privateKey}
}

func newTestLeaf(t *testing.T, authority testAuthority, options testLeafOptions) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(options.serialNumber),
		Subject:      pkix.Name{CommonName: options.spiffeID},
		NotBefore:    options.notBefore,
		NotAfter:     options.notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  options.extKeyUsage,
		DNSNames:     options.dnsNames,
	}
	if options.spiffeID != "" {
		spiffeURL, err := url.Parse(options.spiffeID)
		require.NoError(t, err)
		template.URIs = []*url.URL{spiffeURL}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, authority.certificate, &privateKey.PublicKey, authority.privateKey)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return certificate, privateKey
}

func newTestSVID(t *testing.T, certificate *x509.Certificate, privateKey *ecdsa.PrivateKey) *x509svid.SVID {
	t.Helper()
	id, err := x509svid.IDFromCert(certificate)
	require.NoError(t, err)
	return &x509svid.SVID{
		ID:           id,
		Certificates: []*x509.Certificate{certificate},
		PrivateKey:   privateKey,
	}
}

func newTestBaseTLSConfig(requireClientCert bool) *tls.Config {
	clientAuth := tls.VerifyClientCertIfGiven
	if requireClientCert {
		clientAuth = tls.RequireAndVerifyClientCert
	}
	/* #nosec G402 -- TLS 1.2 is the minimum supported TiDB server version. */
	return &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: clientAuth}
}

func tlsCertificate(certificate *x509.Certificate, privateKey *ecdsa.PrivateKey) tls.Certificate {
	return tls.Certificate{
		Certificate: [][]byte{certificate.Raw},
		PrivateKey:  privateKey,
		Leaf:        certificate,
	}
}

func newTestClientTLSConfig(authority *x509.Certificate, certificates ...tls.Certificate) *tls.Config {
	roots := x509.NewCertPool()
	roots.AddCert(authority)
	/* #nosec G402 -- TLS 1.2 is the minimum supported TiDB server version. */
	return &tls.Config{
		Certificates: certificates,
		RootCAs:      roots,
		ServerName:   "server.example.org",
		MinVersion:   tls.VersionTLS12,
	}
}

type tlsHandshakeResult struct {
	state tls.ConnectionState
	err   error
}

func runTLSHandshake(serverConfig, clientConfig *tls.Config) (tls.ConnectionState, error, error) {
	return runTLSHandshakeWithClientClose(serverConfig, clientConfig, false)
}

func runTLSHandshakeAndCloseClient(serverConfig, clientConfig *tls.Config) (tls.ConnectionState, error, error) {
	return runTLSHandshakeWithClientClose(serverConfig, clientConfig, true)
}

func runTLSHandshakeWithClientClose(serverConfig, clientConfig *tls.Config, closeClientAfterHandshake bool) (tls.ConnectionState, error, error) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	deadline := time.Now().Add(2 * time.Second)
	_ = serverConn.SetDeadline(deadline)
	_ = clientConn.SetDeadline(deadline)

	serverTLS := tls.Server(serverConn, serverConfig)
	clientTLS := tls.Client(clientConn, clientConfig)
	serverResultCh := make(chan tlsHandshakeResult, 1)
	clientErrCh := make(chan error, 1)
	go func() {
		err := serverTLS.Handshake()
		serverResultCh <- tlsHandshakeResult{state: serverTLS.ConnectionState(), err: err}
	}()
	go func() {
		clientErrCh <- clientTLS.Handshake()
		if closeClientAfterHandshake {
			_ = clientConn.Close()
		}
	}()
	serverResult := <-serverResultCh
	if serverResult.err != nil {
		_ = serverConn.Close()
	}
	return serverResult.state, serverResult.err, <-clientErrCh
}

func currentSerial(t *testing.T, dispatcher *tls.Config) string {
	t.Helper()
	current, err := dispatcher.GetConfigForClient(nil)
	require.NoError(t, err)
	require.Len(t, current.Certificates, 1)
	require.NotNil(t, current.Certificates[0].Leaf)
	return current.Certificates[0].Leaf.SerialNumber.String()
}

type fakeWorkloadAPIClient struct {
	updates     chan *workloadapi.X509Context
	watchErrors chan error
	delivered   chan struct{}
	started     chan struct{}
	watchExited chan struct{}
	startOnce   sync.Once
	closeCount  atomic.Int32
}

func newFakeWorkloadAPIClient() *fakeWorkloadAPIClient {
	return &fakeWorkloadAPIClient{
		updates:     make(chan *workloadapi.X509Context),
		watchErrors: make(chan error),
		delivered:   make(chan struct{}, 1),
		started:     make(chan struct{}),
		watchExited: make(chan struct{}),
	}
}

func (c *fakeWorkloadAPIClient) WatchX509Context(ctx context.Context, watcher workloadapi.X509ContextWatcher) error {
	c.startOnce.Do(func() {
		close(c.started)
	})
	defer close(c.watchExited)
	for {
		select {
		case x509Context := <-c.updates:
			watcher.OnX509ContextUpdate(x509Context)
			c.delivered <- struct{}{}
		case err := <-c.watchErrors:
			watcher.OnX509ContextWatchError(err)
			c.delivered <- struct{}{}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *fakeWorkloadAPIClient) Close() error {
	c.closeCount.Add(1)
	return nil
}

type updateThenReturnClient struct {
	x509Context *workloadapi.X509Context
	closeCount  atomic.Int32
}

func (c *updateThenReturnClient) WatchX509Context(_ context.Context, watcher workloadapi.X509ContextWatcher) error {
	watcher.OnX509ContextUpdate(c.x509Context)
	return errors.New("watch stopped")
}

func (c *updateThenReturnClient) Close() error {
	c.closeCount.Add(1)
	return nil
}

type updateThenCancelClient struct {
	x509Context   *workloadapi.X509Context
	cancelStartup context.CancelFunc
}

func (c *updateThenCancelClient) WatchX509Context(ctx context.Context, watcher workloadapi.X509ContextWatcher) error {
	watcher.OnX509ContextUpdate(c.x509Context)
	c.cancelStartup()
	<-ctx.Done()
	return ctx.Err()
}

func (*updateThenCancelClient) Close() error {
	return nil
}
