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
	serverID, err := spiffeid.FromString(security.SPIFFEServerID)
	if err != nil || serverID.Path() == "" {
		return errors.New("--spiffe-workload-api-addr requires a valid --spiffe-server-id")
	}
	ctx, cancel := context.WithTimeout(d.tctx.Context, 30*time.Second)
	defer cancel()
	source, err := workloadapi.NewX509Source(ctx, workloadapi.WithClientOptions(workloadapi.WithAddr(addr)))
	if err != nil {
		return errors.Annotate(err, "load X.509 credentials from the SPIFFE Workload API")
	}
	tlsConfig := tlsconfig.MTLSClientConfig(source, source, tlsconfig.AuthorizeID(serverID))
	tlsConfig.MinVersion = max(tlsConfig.MinVersion, conf.MinTLSVersion)
	d.spiffeSource = source
	security.TLS = tlsConfig
	return nil
}
