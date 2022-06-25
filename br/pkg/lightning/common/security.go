// Copyright 2020 PingCAP, Inc.
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

package common

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/tikv/client-go/v2/config"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type TLS struct {
	caPath   string
	certPath string
	keyPath  string
	inner    *tls.Config
	client   *http.Client
	url      string
}

// ToTLSConfig constructs a `*tls.Config` from the CA, certification and key
// paths.
//
// If the CA path is empty, returns nil.
func ToTLSConfig(caPath, certPath, keyPath string) (*tls.Config, error) {
	if len(caPath) == 0 {
		return nil, nil
	}

	// Create a certificate pool from CA
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, errors.Annotate(err, "could not read ca certificate")
	}

	// Append the certificates from the CA
	if !certPool.AppendCertsFromPEM(ca) {
		return nil, errors.New("failed to append ca certs")
	}

	tlsConfig := &tls.Config{
		RootCAs:    certPool,
		NextProtos: []string{"h2", "http/1.1"}, // specify `h2` to let Go use HTTP/2.
		MinVersion: tls.VersionTLS12,
	}

	if len(certPath) != 0 && len(keyPath) != 0 {
		loadCert := func() (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(certPath, keyPath)
			if err != nil {
				return nil, errors.Annotate(err, "could not load client key pair")
			}
			return &cert, nil
		}
		tlsConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return loadCert()
		}
		tlsConfig.GetCertificate = func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
			return loadCert()
		}
	}
	return tlsConfig, nil
}

// NewTLS constructs a new HTTP client with TLS configured with the CA,
// certificate and key paths.
//
// If the CA path is empty, returns an instance where TLS is disabled.
func NewTLS(caPath, certPath, keyPath, host string) (*TLS, error) {
	if len(caPath) == 0 {
		return &TLS{
			inner:  nil,
			client: &http.Client{},
			url:    "http://" + host,
		}, nil
	}
	inner, err := ToTLSConfig(caPath, certPath, keyPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &TLS{
		caPath:   caPath,
		certPath: certPath,
		keyPath:  keyPath,
		inner:    inner,
		client:   httputil.NewClient(inner),
		url:      "https://" + host,
	}, nil
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

// ToGRPCDialOption constructs a gRPC dial option.
func (tc *TLS) ToGRPCDialOption() grpc.DialOption {
	if tc.inner != nil {
		return grpc.WithTransportCredentials(credentials.NewTLS(tc.inner))
	}
	return grpc.WithInsecure()
}

// WrapListener places a TLS layer on top of the existing listener.
func (tc *TLS) WrapListener(l net.Listener) net.Listener {
	if tc.inner == nil {
		return l
	}
	return tls.NewListener(l, tc.inner)
}

func (tc *TLS) GetJSON(ctx context.Context, path string, v interface{}) error {
	return GetJSON(ctx, tc.client, tc.url+path, v)
}

func (tc *TLS) ToPDSecurityOption() pd.SecurityOption {
	return pd.SecurityOption{
		CAPath:   tc.caPath,
		CertPath: tc.certPath,
		KeyPath:  tc.keyPath,
	}
}

func (tc *TLS) ToTiKVSecurityConfig() config.Security {
	return config.Security{
		ClusterSSLCA:    tc.caPath,
		ClusterSSLCert:  tc.certPath,
		ClusterSSLKey:   tc.keyPath,
		ClusterVerifyCN: nil, // FIXME should fill this in?
	}
}

func (tc *TLS) TLSConfig() *tls.Config {
	return tc.inner
}
