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
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/pingcap/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
		tlsCfg.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
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
	ca, err := ioutil.ReadFile(caPath)
	if err != nil {
		return nil, errors.Annotate(err, "could not read ca certificate")
	}

	// Append the certificates from the CA
	if !certPool.AppendCertsFromPEM(ca) {
		return nil, errors.New("failed to append ca certs")
	}
	/* #nosec G402 */
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS10,
		Certificates: certificates,
		RootCAs:      certPool,
		ClientCAs:    certPool,
		NextProtos:   []string{"h2", "http/1.1"}, // specify `h2` to let Go use HTTP/2.
	}

	addVerifyPeerCertificate(tlsCfg, verifyCN)
	return tlsCfg, nil
}

// NewTLSConfigWithVerifyCN constructs a `*tls.Config` from given data:
//   - caData: represents the MySQL server's CA certificate. If not empty, the TLS connection only allows this CA.
//   - certData, keyData: represents the client's certificate and key. If not empty, the TLS connection will use this
//     certificate. Otherwise, it will use self-generated certificate.
//   - verifyCN: represents the Common Name that the MySQL server's certificate is expected to have. If not empty, the TLS
//     connection only allows the certificates with these CN.
func NewTLSConfigWithVerifyCN(caData, certData, keyData []byte, verifyCN []string) (*tls.Config, error) {
	var (
		certificates []tls.Certificate
		certPool     *x509.CertPool
		verifyFuncs  []func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error
	)

	if len(certData) != 0 && len(keyData) != 0 {
		// Generate a key pair from your pem-encoded cert and key ([]byte).
		cert, err := tls.X509KeyPair(certData, keyData)
		if err != nil {
			return nil, errors.Wrap(err, "failed to generate cert")
		}
		certificates = []tls.Certificate{cert}
	}

	/* #nosec G402 */
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS10,
		Certificates:       certificates,
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2", "http/1.2"}, // specify `h2` to let Go use HTTP/2.
	}

	if len(caData) != 0 {
		certPool = x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caData) {
			return nil, errors.New("failed to append ca certs")
		}
		tlsCfg.RootCAs = certPool
		tlsCfg.ClientCAs = certPool

		// check the received MySQL server certificate during TLS handshake
		// thanks to https://cloud.google.com/sql/docs/mysql/samples/cloud-sql-mysql-databasesql-sslcerts
		verifyFuncs = append(verifyFuncs, func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("no certificates available to verify")
			}

			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return err
			}

			opts := x509.VerifyOptions{Roots: certPool}
			if _, err = cert.Verify(opts); err != nil {
				return errors.Wrap(err, "can't verify certificate, maybe different CA is used")
			}
			return nil
		})
	}

	if len(verifyCN) != 0 {
		checkCN := make(map[string]struct{})
		for _, cn := range verifyCN {
			cn = strings.TrimSpace(cn)
			checkCN[cn] = struct{}{}
		}
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		// check the received CommonName of MySQL server certificate's verify chain during TLS handshake
		verifyFuncs = append(verifyFuncs, func(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
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
		})
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

// NewTLSFromMockServer constructs a new TLS instance from the certificates of
// an *httptest.Server.
func NewTLSFromMockServer(server *httptest.Server) *TLS {
	return &TLS{
		inner:  server.TLS,
		client: server.Client(),
		url:    server.URL,
	}
}

// WithHost creates a new TLS instance with the host replaced.
func (tc *TLS) WithHost(host string) *TLS {
	var url string
	if tc.inner != nil {
		url = "https://" + host
	} else {
		url = "http://" + host
	}
	return &TLS{
		inner:  tc.inner,
		client: tc.client,
		url:    url,
	}
}

// TLSConfig returns tls config
func (tc *TLS) TLSConfig() *tls.Config {
	return tc.inner
}

// ToGRPCDialOption constructs a gRPC dial option.
func (tc *TLS) ToGRPCDialOption() grpc.DialOption {
	if tc.inner != nil {
		return grpc.WithTransportCredentials(credentials.NewTLS(tc.inner))
	}
	return grpc.WithInsecure()
}

// ToGRPCServerOption constructs a gRPC server option.
func (tc *TLS) ToGRPCServerOption() grpc.ServerOption {
	if tc.inner != nil {
		return grpc.Creds(credentials.NewTLS(tc.inner))
	}

	return grpc.Creds(nil)
}

// WrapListener places a TLS layer on top of the existing listener.
func (tc *TLS) WrapListener(l net.Listener) net.Listener {
	if tc.inner == nil {
		return l
	}
	return tls.NewListener(l, tc.inner)
}

// GetJSON obtains JSON result with the HTTP GET method.
func (tc *TLS) GetJSON(path string, v interface{}) error {
	return GetJSON(tc.client, tc.url+path, v)
}
