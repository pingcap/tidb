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

// Package spiffetls provides a rotating TLS configuration backed by the
// SPIFFE Workload API.
package spiffetls

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"go.uber.org/zap"
)

type workloadAPIClient interface {
	WatchX509Context(context.Context, workloadapi.X509ContextWatcher) error
	Close() error
}

// Source owns a Workload API watch and publishes its latest valid TLS
// configuration through a stable dispatcher.
type Source struct {
	closeErr          error
	watchCtx          context.Context
	client            workloadAPIClient
	dispatcher        *tls.Config
	baseTLSConfig     *tls.Config
	cancelWatch       context.CancelFunc
	watchDone         chan error
	current           atomic.Pointer[tls.Config]
	ready             chan struct{}
	watchWG           sync.WaitGroup
	readyOnce         sync.Once
	closeOnce         sync.Once
	requireClientCert bool
}

// New starts watching addr and waits up to timeout for the first valid X.509
// context. The first SVID returned by the Workload API is used, as prescribed
// for the default Workload API identity.
func New(addr string, timeout time.Duration, minTLSVersion string, requireClientCert bool) (*Source, error) {
	if addr == "" {
		return nil, errors.New("SPIFFE Workload API address is required")
	}
	if err := workloadapi.ValidateAddress(addr); err != nil {
		return nil, fmt.Errorf("invalid SPIFFE Workload API address: %w", err)
	}
	workloadAPIURL, _ := url.Parse(addr)
	if !strings.HasPrefix(addr, "unix:///") || workloadAPIURL.Scheme != "unix" || workloadAPIURL.Host != "" ||
		workloadAPIURL.Path == "" || !filepath.IsAbs(workloadAPIURL.Path) {
		return nil, errors.New("SPIFFE Workload API address must be an absolute unix:/// URI")
	}
	if timeout <= 0 {
		return nil, errors.New("SPIFFE Workload API startup timeout must be positive")
	}

	clientAuth := tls.VerifyClientCertIfGiven
	if requireClientCert {
		clientAuth = tls.RequireAndVerifyClientCert
	}
	baseTLSConfig, err := util.NewServerTLSConfig(minTLSVersion, clientAuth, nil)
	if err != nil {
		return nil, fmt.Errorf("build SPIFFE server TLS policy: %w", err)
	}

	startupCtx, cancelStartup := context.WithTimeout(context.Background(), timeout)
	defer cancelStartup()
	client, err := workloadapi.New(
		startupCtx,
		workloadapi.WithAddr(addr),
		workloadapi.WithLogger(workloadAPILogger{logger: logutil.BgLogger().Sugar()}),
	)
	if err != nil {
		return nil, fmt.Errorf("create SPIFFE Workload API client: %w", err)
	}

	return newSource(startupCtx, client, baseTLSConfig, requireClientCert)
}

func newSource(
	startupCtx context.Context,
	client workloadAPIClient,
	baseTLSConfig *tls.Config,
	requireClientCert bool,
) (_ *Source, err error) {
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	source := &Source{
		client:            client,
		baseTLSConfig:     baseTLSConfig,
		requireClientCert: requireClientCert,
		watchCtx:          watchCtx,
		cancelWatch:       cancelWatch,
		watchDone:         make(chan error, 1),
		ready:             make(chan struct{}),
	}

	defer func() {
		if err != nil {
			err = errors.Join(err, source.Close())
		}
	}()

	source.watchWG.Add(1)
	go func() {
		defer source.watchWG.Done()
		watchErr := source.client.WatchX509Context(source.watchCtx, source)
		if source.watchCtx.Err() == nil {
			logutil.BgLogger().Error("SPIFFE Workload API watch stopped; keeping the last valid TLS configuration", zap.Error(watchErr))
		}
		source.watchDone <- watchErr
	}()

	select {
	case <-source.ready:
		current := source.current.Load()
		if current == nil {
			return nil, errors.New("SPIFFE Workload API reported readiness without a TLS configuration")
		}
		source.dispatcher = source.newDispatcher(current)
		return source, nil
	case watchErr := <-source.watchDone:
		if current := source.current.Load(); current != nil {
			source.dispatcher = source.newDispatcher(current)
			return source, nil
		}
		if watchErr == nil {
			watchErr = errors.New("watch stopped without an error")
		}
		return nil, fmt.Errorf("SPIFFE Workload API watch stopped before the first valid X.509 context: %w", watchErr)
	case <-startupCtx.Done():
		if current := source.current.Load(); current != nil {
			source.dispatcher = source.newDispatcher(current)
			return source, nil
		}
		return nil, fmt.Errorf("waiting for the first valid SPIFFE X.509 context: %w", startupCtx.Err())
	}
}

// TLSConfig returns a stable dispatcher that selects the latest valid
// Workload API snapshot for each new TLS handshake.
func (s *Source) TLSConfig() *tls.Config {
	return s.dispatcher
}

// Close stops the Workload API watch and releases its connection. It is safe
// to call Close more than once.
func (s *Source) Close() error {
	s.closeOnce.Do(func() {
		s.cancelWatch()
		s.closeErr = s.client.Close()
		s.watchWG.Wait()
	})
	return s.closeErr
}

// OnX509ContextUpdate implements workloadapi.X509ContextWatcher.
func (s *Source) OnX509ContextUpdate(x509Context *workloadapi.X509Context) {
	tlsConfig, err := newTLSConfig(s.baseTLSConfig, x509Context, s.requireClientCert)
	if err != nil {
		logutil.BgLogger().Warn("Ignoring invalid SPIFFE X.509 context; keeping the last valid TLS configuration", zap.Error(err))
		return
	}

	previous := s.current.Swap(tlsConfig)
	oldID, oldSerial, oldNotAfter := tlsConfigIdentity(previous)
	newID, newSerial, newNotAfter := tlsConfigIdentity(tlsConfig)
	logutil.BgLogger().Info("accepted SPIFFE X.509 context",
		zap.String("old-spiffe-id", oldID),
		zap.String("old-serial-number", oldSerial),
		zap.String("old-not-after", oldNotAfter),
		zap.String("new-spiffe-id", newID),
		zap.String("new-serial-number", newSerial),
		zap.String("new-not-after", newNotAfter),
	)
	s.readyOnce.Do(func() {
		close(s.ready)
	})
}

// OnX509ContextWatchError implements workloadapi.X509ContextWatcher.
func (s *Source) OnX509ContextWatchError(err error) {
	if err == nil || s.watchCtx.Err() != nil || errors.Is(err, context.Canceled) {
		return
	}
	logutil.BgLogger().Warn("SPIFFE Workload API watch error; keeping the last valid TLS configuration", zap.Error(err))
}

func (s *Source) newDispatcher(initial *tls.Config) *tls.Config {
	dispatcher := initial.Clone()
	dispatcher.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		current := s.current.Load()
		if current == nil {
			return nil, errors.New("no valid SPIFFE TLS configuration is available")
		}
		return current, nil
	}
	dispatcher.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		current := s.current.Load()
		if current == nil || len(current.Certificates) != 1 {
			return nil, errors.New("no valid SPIFFE server certificate is available")
		}
		return &current.Certificates[0], nil
	}
	return dispatcher
}

func tlsConfigIdentity(tlsConfig *tls.Config) (id, serialNumber, notAfter string) {
	if tlsConfig == nil || len(tlsConfig.Certificates) != 1 || tlsConfig.Certificates[0].Leaf == nil {
		return "", "", ""
	}
	leaf := tlsConfig.Certificates[0].Leaf
	spiffeID, err := x509svid.IDFromCert(leaf)
	if err == nil {
		id = spiffeID.String()
	}
	if leaf.SerialNumber != nil {
		serialNumber = leaf.SerialNumber.String()
	}
	return id, serialNumber, leaf.NotAfter.UTC().Format(time.RFC3339)
}

func newTLSConfig(baseTLSConfig *tls.Config, x509Context *workloadapi.X509Context, requireClientCert bool) (*tls.Config, error) {
	if x509Context == nil {
		return nil, errors.New("X.509 context is nil")
	}
	if len(x509Context.SVIDs) == 0 || x509Context.SVIDs[0] == nil {
		return nil, errors.New("X.509 context contains no default SVID")
	}
	if x509Context.Bundles == nil {
		return nil, errors.New("X.509 context contains no bundles")
	}

	bundles, clientCAs, err := cloneBundles(x509Context.Bundles)
	if err != nil {
		return nil, err
	}
	tlsCertificate, err := validateServerSVID(x509Context.SVIDs[0], bundles)
	if err != nil {
		return nil, err
	}

	tlsConfig := baseTLSConfig.Clone()
	tlsConfig.Certificates = []tls.Certificate{tlsCertificate}
	tlsConfig.ClientCAs = clientCAs
	tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			if requireClientCert {
				return errors.New("client did not provide an X.509-SVID")
			}
			return nil
		}
		if _, _, err := x509svid.Verify(state.PeerCertificates, bundles); err != nil {
			return fmt.Errorf("verify client X.509-SVID: %w", err)
		}
		return nil
	}
	return tlsConfig, nil
}

func cloneBundles(bundles *x509bundle.Set) (*x509bundle.Set, *x509.CertPool, error) {
	clonedBundles := make([]*x509bundle.Bundle, 0, bundles.Len())
	clientCAs := x509.NewCertPool()
	for _, bundle := range bundles.Bundles() {
		authorities, err := cloneCertificates(bundle.X509Authorities())
		if err != nil {
			return nil, nil, fmt.Errorf("clone SPIFFE bundle %q: %w", bundle.TrustDomain(), err)
		}
		clonedBundles = append(clonedBundles, x509bundle.FromX509Authorities(bundle.TrustDomain(), authorities))
		for _, authority := range authorities {
			clientCAs.AddCert(authority)
		}
	}
	return x509bundle.NewSet(clonedBundles...), clientCAs, nil
}

func validateServerSVID(svid *x509svid.SVID, bundles *x509bundle.Set) (tls.Certificate, error) {
	if svid == nil {
		return tls.Certificate{}, errors.New("default X.509-SVID is nil")
	}
	certificates, err := cloneCertificates(svid.Certificates)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("clone default X.509-SVID certificate chain: %w", err)
	}
	if len(certificates) == 0 {
		return tls.Certificate{}, errors.New("default X.509-SVID contains no certificates")
	}
	if svid.PrivateKey == nil {
		return tls.Certificate{}, errors.New("default X.509-SVID contains no private key")
	}

	privatePublicKey, err := x509.MarshalPKIXPublicKey(svid.PrivateKey.Public())
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal default X.509-SVID private key public component: %w", err)
	}
	certificatePublicKey, err := x509.MarshalPKIXPublicKey(certificates[0].PublicKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal default X.509-SVID certificate public key: %w", err)
	}
	if !bytes.Equal(privatePublicKey, certificatePublicKey) {
		return tls.Certificate{}, errors.New("default X.509-SVID private key does not match its leaf certificate")
	}

	verifiedID, _, err := x509svid.Verify(certificates, bundles)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("verify default X.509-SVID: %w", err)
	}
	if verifiedID != svid.ID {
		return tls.Certificate{}, fmt.Errorf("default X.509-SVID ID %q does not match certificate ID %q", svid.ID, verifiedID)
	}
	if err := verifyServerAuth(certificates, bundles, verifiedID.TrustDomain()); err != nil {
		return tls.Certificate{}, err
	}

	certificateDER := make([][]byte, 0, len(certificates))
	for _, certificate := range certificates {
		certificateDER = append(certificateDER, append([]byte(nil), certificate.Raw...))
	}
	return tls.Certificate{
		Certificate: certificateDER,
		PrivateKey:  svid.PrivateKey,
		Leaf:        certificates[0],
	}, nil
}

func verifyServerAuth(certificates []*x509.Certificate, bundles *x509bundle.Set, trustDomain spiffeid.TrustDomain) error {
	bundle, err := bundles.GetX509BundleForTrustDomain(trustDomain)
	if err != nil {
		return fmt.Errorf("get bundle for default X.509-SVID trust domain: %w", err)
	}
	roots := x509.NewCertPool()
	for _, authority := range bundle.X509Authorities() {
		roots.AddCert(authority)
	}
	intermediates := x509.NewCertPool()
	for _, certificate := range certificates[1:] {
		intermediates.AddCert(certificate)
	}
	if _, err := certificates[0].Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return fmt.Errorf("verify default X.509-SVID for server authentication: %w", err)
	}
	return nil
}

func cloneCertificates(certificates []*x509.Certificate) ([]*x509.Certificate, error) {
	cloned := make([]*x509.Certificate, 0, len(certificates))
	for index, certificate := range certificates {
		if certificate == nil {
			return nil, fmt.Errorf("certificate %d is nil", index)
		}
		parsed, err := x509.ParseCertificate(certificate.Raw)
		if err != nil {
			return nil, fmt.Errorf("parse certificate %d: %w", index, err)
		}
		cloned = append(cloned, parsed)
	}
	return cloned, nil
}

type workloadAPILogger struct {
	logger *zap.SugaredLogger
}

func (l workloadAPILogger) Debugf(format string, args ...any) {
	l.logger.Debugf("SPIFFE Workload API: "+format, args...)
}

func (l workloadAPILogger) Infof(format string, args ...any) {
	l.logger.Infof("SPIFFE Workload API: "+format, args...)
}

func (l workloadAPILogger) Warnf(format string, args ...any) {
	l.logger.Warnf("SPIFFE Workload API: "+format, args...)
}

func (l workloadAPILogger) Errorf(format string, args ...any) {
	l.logger.Errorf("SPIFFE Workload API: "+format, args...)
}
