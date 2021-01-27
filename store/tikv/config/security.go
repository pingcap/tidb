// Copyright 2021 PingCAP, Inc.
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

package config

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/pingcap/errors"
)

// Security is the security section of the config.
type Security struct {
	ClusterSSLCA    string   `toml:"cluster-ssl-ca" json:"cluster-ssl-ca"`
	ClusterSSLCert  string   `toml:"cluster-ssl-cert" json:"cluster-ssl-cert"`
	ClusterSSLKey   string   `toml:"cluster-ssl-key" json:"cluster-ssl-key"`
	ClusterVerifyCN []string `toml:"cluster-verify-cn" json:"cluster-verify-cn"`
}

func NewSecurity(sslCA, sslCert, sslKey string, verityCN []string) Security {
	return Security{
		ClusterSSLCA:    sslCA,
		ClusterSSLCert:  sslCert,
		ClusterSSLKey:   sslKey,
		ClusterVerifyCN: verityCN,
	}
}

// ToTLSConfig generates tls's config based on security section of the config.
func (s *Security) ToTLSConfig() (tlsConfig *tls.Config, err error) {
	if len(s.ClusterSSLCA) != 0 {
		certPool := x509.NewCertPool()
		// Create a certificate pool from the certificate authority
		var ca []byte
		ca, err = ioutil.ReadFile(s.ClusterSSLCA)
		if err != nil {
			err = errors.Errorf("could not read ca certificate: %s", err)
			return
		}
		// Append the certificates from the CA
		if !certPool.AppendCertsFromPEM(ca) {
			err = errors.New("failed to append ca certs")
			return
		}
		tlsConfig = &tls.Config{
			RootCAs:   certPool,
			ClientCAs: certPool,
		}

		if len(s.ClusterSSLCert) != 0 && len(s.ClusterSSLKey) != 0 {
			getCert := func() (*tls.Certificate, error) {
				// Load the client certificates from disk
				cert, err := tls.LoadX509KeyPair(s.ClusterSSLCert, s.ClusterSSLKey)
				if err != nil {
					return nil, errors.Errorf("could not load client key pair: %s", err)
				}
				return &cert, nil
			}
			// pre-test cert's loading.
			if _, err = getCert(); err != nil {
				return
			}
			tlsConfig.GetClientCertificate = func(info *tls.CertificateRequestInfo) (certificate *tls.Certificate, err error) {
				return getCert()
			}
			tlsConfig.GetCertificate = func(info *tls.ClientHelloInfo) (certificate *tls.Certificate, err error) {
				return getCert()
			}
		}
	}
	return
}
