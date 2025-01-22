// Copyright 2022 PingCAP, Inc.
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

package util

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/pingcap/errors"
)

// TLS saves some information about tls
type TLS struct {
	inner  *tls.Config
	client *http.Client
	url    string
}

// ToTLSConfig generates tls's config.
func ToTLSConfig(caPath, certPath, keyPath string) (*tls.Config, error) {
	return ToTLSConfigWithVerify(caPath, certPath, keyPath, nil)
}

func addVerifyPeerCertificate(tlsCfg *tls.Config, verifyCN []string) {
	if len(verifyCN) != 0 {
		checkCN := make(map[string]struct{})
		for _, cn := range verifyCN {
			cn = strings.TrimSpace(cn)
			checkCN[cn] = struct{}{}
		}
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		tlsCfg.VerifyPeerCertificate = func(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
			cns := make([]string, 0, len(verifiedChains))
			for _, chains := range verifiedChains {
				for _, chain := range chains {
					cns = append(cns, chain.Subject.CommonName)
					if _, match := checkCN[chain.Subject.CommonName]; match {
						return nil
					}
				}
			}
			return errors.Errorf("client certificate authentication failed. The Common Name from the client certificate %v was not found in the configuration cluster-verify-cn with value: %s", cns, verifyCN)
		}
	}
}

// ToTLSConfigWithVerify constructs a `*tls.Config` from the CA, certification and key
// paths, and add verify for CN.
//
// If the CA path is empty, returns nil.
func ToTLSConfigWithVerify(caPath, certPath, keyPath string, verifyCN []string) (*tls.Config, error) {
	if len(caPath) == 0 {
		return nil, nil
	}

	// Load the client certificates from disk
	var certificates []tls.Certificate
	if len(certPath) != 0 && len(keyPath) != 0 {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, errors.Annotate(err, "could not load client key pair")
		}
		certificates = []tls.Certificate{cert}
	}

	// Create a certificate pool from CA
	certPool := x509.NewCertPool()
	//nolint: gosec
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, errors.Annotate(err, "could not read ca certificate")
	}

	// Append the certificates from the CA
	if !certPool.AppendCertsFromPEM(ca) {
		return nil, errors.New("failed to append ca certs")
	}
	/* #nosec G402 */
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: certificates,
		RootCAs:      certPool,
		ClientCAs:    certPool,
		NextProtos:   []string{"h2", "http/1.1"}, // specify `h2` to let Go use HTTP/2.
	}

	addVerifyPeerCertificate(tlsCfg, verifyCN)
	return tlsCfg, nil
}

type tlsConfigBuilder struct {
	caPath, certPath, keyPath          string
	caContent, certContent, keyContent []byte
	verifyCN                           []string
	minTLSVersion                      uint16
}

// TLSConfigOption is used to build a tls.Config in NewTLSConfig.
type TLSConfigOption func(*tlsConfigBuilder)

// WithCAPath sets the CA path to build a tls.Config, and the peer should use the certificate which can be verified by
// this CA. It has higher priority than WithCAContent.
// empty `caPath` is no-op.
func WithCAPath(caPath string) TLSConfigOption {
	return func(builder *tlsConfigBuilder) {
		builder.caPath = caPath
	}
}

// WithCertAndKeyPath sets the client certificate and primary key path to build a tls.Config. It has higher priority
// than WithCertAndKeyContent.
// empty `certPath`/`keyPath` is no-op.
// WithCertAndKeyPath also support rotation, which means if the client certificate or primary key file is changed, the
// new content will be used.
func WithCertAndKeyPath(certPath, keyPath string) TLSConfigOption {
	return func(builder *tlsConfigBuilder) {
		builder.certPath = certPath
		builder.keyPath = keyPath
	}
}

// WithVerifyCommonName sets the Common Name the peer must provide before starting a TLS connection.
// empty `verifyCN` is no-op.
func WithVerifyCommonName(verifyCN []string) TLSConfigOption {
	return func(builder *tlsConfigBuilder) {
		builder.verifyCN = verifyCN
	}
}

// WithCAContent sets the CA content to build a tls.Config, and the peer should use the certificate which can be
// verified by this CA. It has lower priority than WithCAPath.
// empty `caContent` is no-op.
func WithCAContent(caContent []byte) TLSConfigOption {
	return func(builder *tlsConfigBuilder) {
		builder.caContent = caContent
	}
}

// WithCertAndKeyContent sets the client certificate and primary key content to build a tls.Config. It has lower
// priority than WithCertAndKeyPath.
// empty `certContent`/`keyContent` is no-op.
func WithCertAndKeyContent(certContent, keyContent []byte) TLSConfigOption {
	return func(builder *tlsConfigBuilder) {
		builder.certContent = certContent
		builder.keyContent = keyContent
	}
}

// WithMinTLSVersion sets the min tls version to build a tls.Config.
func WithMinTLSVersion(minTLSVersion uint16) TLSConfigOption {
	return func(builder *tlsConfigBuilder) {
		builder.minTLSVersion = minTLSVersion
	}
}

// NewTLSConfig creates a tls.Config from the given options. If no certificate is provided, it will return (nil, nil).
func NewTLSConfig(opts ...TLSConfigOption) (*tls.Config, error) {
	builder := &tlsConfigBuilder{}
	for _, opt := range opts {
		opt(builder)
	}

	if builder.caPath == "" && len(builder.caContent) == 0 &&
		builder.certPath == "" && len(builder.certContent) == 0 &&
		builder.keyPath == "" && len(builder.keyContent) == 0 {
		return nil, nil
	}

	var (
		certPool    *x509.CertPool
		certPoolMu  sync.RWMutex
		verifyFuncs []func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error
	)

	/* #nosec G402 */
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2", "http/1.2"}, // specify `h2` to let Go use HTTP/2.
	}

	if builder.minTLSVersion != 0 {
		tlsCfg.MinVersion = builder.minTLSVersion
	}

	// 1. handle client certificates

	if builder.certPath != "" && builder.keyPath != "" {
		// clear the content if path is provided
		builder.certContent = nil
		builder.keyContent = nil
		loadCert := func() (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(builder.certPath, builder.keyPath)
			if err != nil {
				return nil, errors.Annotate(err, "could not load client key pair")
			}
			return &cert, nil
		}
		tlsCfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return loadCert()
		}
		tlsCfg.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return loadCert()
		}
	}
	if len(builder.certContent) > 0 && len(builder.keyContent) > 0 {
		cert, err := tls.X509KeyPair(builder.certContent, builder.keyContent)
		if err != nil {
			return nil, errors.Annotate(err, "could not load client key pair")
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	// 2. handle CA

	// need this closure to access certPool
	// thanks to https://cloud.google.com/sql/docs/mysql/samples/cloud-sql-mysql-databasesql-sslcerts
	verifyCA := func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("no certificates available to verify")
		}

		cert, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}

		intermediates := x509.NewCertPool()
		for _, certBytes := range rawCerts[1:] {
			c, err2 := x509.ParseCertificate(certBytes)
			if err2 != nil {
				return err2
			}
			intermediates.AddCert(c)
		}

		certPoolMu.RLock()
		defer certPoolMu.RUnlock()
		if _, err = cert.Verify(x509.VerifyOptions{
			Roots:         certPool,
			Intermediates: intermediates,
		}); err != nil {
			return errors.Wrap(err, "can't verify certificate, maybe different CA is used")
		}
		return nil
	}

	var (
		caContent []byte
		err       error
	)
	if builder.caPath != "" {
		caContent, err = os.ReadFile(builder.caPath)
		if err != nil {
			return nil, errors.Annotate(err, "could not read ca certificate")
		}
	} else {
		caContent = builder.caContent
	}

	if len(caContent) > 0 {
		certPool = x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caContent) {
			return nil, errors.New("failed to append ca certs")
		}
		tlsCfg.RootCAs = certPool
		tlsCfg.ClientCAs = certPool

		verifyFuncs = append(verifyFuncs, verifyCA)
	}

	// 3. handle verify Common Name

	if len(builder.verifyCN) > 0 {
		// set RequireAndVerifyClientCert so server can verify the Common Name of client
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		// set InsecureSkipVerify to false so client can verify the Common Name of server
		tlsCfg.InsecureSkipVerify = false
		verifyFuncs = append(verifyFuncs, verifyCommonName(builder.verifyCN))
	}

	tlsCfg.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		for _, f := range verifyFuncs {
			if err := f(rawCerts, verifiedChains); err != nil {
				return err
			}
		}
		return nil
	}
	return tlsCfg, nil
}

func verifyCommonName(verifyCN []string) func([][]byte, [][]*x509.Certificate) error {
	checkCN := make(map[string]struct{})
	for _, cn := range verifyCN {
		cn = strings.TrimSpace(cn)
		checkCN[cn] = struct{}{}
	}

	return func(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
		cns := make([]string, 0, len(verifiedChains))
		for _, chains := range verifiedChains {
			for _, chain := range chains {
				cns = append(cns, chain.Subject.CommonName)
				if _, match := checkCN[chain.Subject.CommonName]; match {
					return nil
				}
			}
		}
		return errors.Errorf("client certificate authentication failed. The Common Name from the client certificate %v was not found in the configuration cluster-verify-cn with value: %s", cns, verifyCN)
	}
}

// NewTLS constructs a new HTTP client with TLS configured with the CA,
// certificate and key paths.
//
// If the CA path is empty, returns an instance where TLS is disabled.
func NewTLS(caPath, certPath, keyPath, host string, verifyCN []string) (*TLS, error) {
	if len(caPath) == 0 {
		return &TLS{
			inner:  nil,
			client: &http.Client{},
			url:    "http://" + host,
		}, nil
	}
	inner, err := ToTLSConfigWithVerify(caPath, certPath, keyPath, verifyCN)
	if err != nil {
		return nil, err
	}

	client := ClientWithTLS(inner)

	return &TLS{
		inner:  inner,
		client: client,
		url:    "https://" + host,
	}, nil
}

// ClientWithTLS creates a http client wit tls
func ClientWithTLS(tlsCfg *tls.Config) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsCfg
	return &http.Client{Transport: transport}
}

// WrapListener places a TLS layer on top of the existing listener.
func (tc *TLS) WrapListener(l net.Listener) net.Listener {
	if tc.inner == nil {
		return l
	}
	return tls.NewListener(l, tc.inner)
}
