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

package export

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

func (d *Dumper) initTLS() error {
	conf := d.conf
	security := &conf.Security
	addr := security.SPIFFEWorkloadAPIAddr
	if addr == "" {
		if security.SPIFFEServerID != "" {
			return errors.New("--spiffe-server-id requires --spiffe-workload-api-addr")
		}
		return buildTLSConfig(conf)
	}
	if conf.DumpService != "" {
		return errors.New("--spiffe-workload-api-addr cannot be combined with --dump-service")
	}
	if security.CAPath != "" || security.CertPath != "" || security.KeyPath != "" ||
		len(security.SSLCABytes) != 0 || len(security.SSLCertBytes) != 0 || len(security.SSLKeyBytes) != 0 {
		return errors.New("--spiffe-workload-api-addr cannot be combined with file or byte TLS credentials")
	}
	endpoint, err := url.Parse(addr)
	if err != nil || !strings.HasPrefix(addr, "unix:///") || strings.ContainsAny(addr, "?#") ||
		endpoint.Path == "/" || strings.ContainsRune(endpoint.Path, '\x00') {
		return errors.New("--spiffe-workload-api-addr requires a Unix socket URL such as unix:///absolute/path.sock")
	}
	matchServer, err := parseSPIFFEServerMatcher(security.SPIFFEServerID)
	if err != nil {
		return errors.Annotate(err, "invalid --spiffe-server-id")
	}
	ctx, cancel := context.WithTimeout(d.tctx.Context, 30*time.Second)
	defer cancel()
	source, err := workloadapi.NewX509Source(ctx, workloadapi.WithClientOptions(workloadapi.WithAddr(addr)))
	if err != nil {
		return errors.Annotate(err, "load X.509 credentials from the SPIFFE Workload API")
	}
	tlsConfig := tlsconfig.MTLSClientConfig(source, source, tlsconfig.AdaptMatcher(matchServer))
	tlsConfig.MinVersion = max(tlsConfig.MinVersion, conf.MinTLSVersion)
	d.spiffeSource = source
	security.TLS = tlsConfig
	return nil
}

func parseSPIFFEServerMatcher(pattern string) (spiffeid.Matcher, error) {
	authorityAndPath, hasScheme := strings.CutPrefix(pattern, "spiffe://")
	domain, path, hasPath := strings.Cut(authorityAndPath, "/")
	if !hasScheme || !hasPath || strings.Contains(domain, "*") {
		return nil, errors.New("expected a SPIFFE URI with a fixed trust domain and a workload path")
	}
	trustDomain, err := spiffeid.TrustDomainFromString(domain)
	if err != nil {
		return nil, err
	}
	segments := strings.Split(path, "/")
	for _, segment := range segments {
		if segment == "*" {
			continue
		}
		if strings.Contains(segment, "*") {
			return nil, errors.New("a wildcard must occupy a complete SPIFFE path segment")
		}
		if err := spiffeid.ValidatePathSegment(segment); err != nil {
			return nil, err
		}
	}

	return func(peer spiffeid.ID) error {
		// TLS verification parses and validates the peer SPIFFE ID before matching it.
		peerPath := peer.Path()
		peerSegments := strings.Split(strings.TrimPrefix(peerPath, "/"), "/")
		if !peer.MemberOf(trustDomain) || peerPath == "" || len(peerSegments) != len(segments) {
			return errors.Errorf("unexpected ID %q", peer)
		}
		for i, segment := range segments {
			if segment != "*" && segment != peerSegments[i] {
				return errors.Errorf("unexpected ID %q", peer)
			}
		}
		return nil
	}, nil
}
