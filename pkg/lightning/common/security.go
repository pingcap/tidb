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
	"net"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/tikv/client-go/v2/config"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TLS is a wrapper around a TLS configuration.
type TLS struct {
	caPath    string
	certPath  string
	keyPath   string
	caBytes   []byte
	certBytes []byte
	keyBytes  []byte
	inner     *tls.Config
	client    *http.Client
	url       string
}

// NewTLS constructs a new HTTP client with TLS configured with the CA,
// certificate and key paths.
func NewTLS(caPath, certPath, keyPath, host string, caBytes, certBytes, keyBytes []byte) (*TLS, error) {
	inner, err := util.NewTLSConfig(
		util.WithCAPath(caPath),
		util.WithCertAndKeyPath(certPath, keyPath),
		util.WithCAContent(caBytes),
		util.WithCertAndKeyContent(certBytes, keyBytes),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if inner == nil {
		return &TLS{
			inner:  nil,
			client: &http.Client{},
			url:    "http://" + host,
		}, nil
	}

	return &TLS{
		caPath:    caPath,
		certPath:  certPath,
		keyPath:   keyPath,
		caBytes:   caBytes,
		certBytes: certBytes,
		keyBytes:  keyBytes,
		inner:     inner,
		client:    httputil.NewClient(inner),
		url:       "https://" + host,
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

// GetMockTLSUrl returns tls's host for mock test
func GetMockTLSUrl(tls *TLS) string {
	return tls.url
}

// WithHost creates a new TLS instance with the host replaced.
func (tc *TLS) WithHost(host string) *TLS {
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "https://")
	var url string
	if tc.inner != nil {
		url = "https://" + host
	} else {
		url = "http://" + host
	}
	shallowClone := *tc
	shallowClone.url = url
	return &shallowClone
}

// ToGRPCDialOption constructs a gRPC dial option.
func (tc *TLS) ToGRPCDialOption() grpc.DialOption {
	return ToGRPCDialOption(tc.inner)
}

// ToGRPCDialOption constructs a gRPC dial option from tls.Config.
func ToGRPCDialOption(tls *tls.Config) grpc.DialOption {
	if tls != nil {
		return grpc.WithTransportCredentials(credentials.NewTLS(tls))
	}
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// WrapListener places a TLS layer on top of the existing listener.
func (tc *TLS) WrapListener(l net.Listener) net.Listener {
	if tc.inner == nil {
		return l
	}
	return tls.NewListener(l, tc.inner)
}

// GetJSON performs a GET request to the given path and unmarshals the response
func (tc *TLS) GetJSON(ctx context.Context, path string, v any) error {
	return GetJSON(ctx, tc.client, tc.url+path, v)
}

// ToPDSecurityOption converts the TLS configuration to a PD security option.
func (tc *TLS) ToPDSecurityOption() pd.SecurityOption {
	return pd.SecurityOption{
		CAPath:       tc.caPath,
		CertPath:     tc.certPath,
		KeyPath:      tc.keyPath,
		SSLCABytes:   tc.caBytes,
		SSLCertBytes: tc.certBytes,
		SSLKEYBytes:  tc.keyBytes,
	}
}

// ToTiKVSecurityConfig converts the TLS configuration to a TiKV security config.
// TODO: TiKV does not support pass in content.
func (tc *TLS) ToTiKVSecurityConfig() config.Security {
	return config.Security{
		ClusterSSLCA:    tc.caPath,
		ClusterSSLCert:  tc.certPath,
		ClusterSSLKey:   tc.keyPath,
		ClusterVerifyCN: nil, // FIXME should fill this in?
	}
}

// TLSConfig returns the underlying TLS configuration.
func (tc *TLS) TLSConfig() *tls.Config {
	return tc.inner
}
