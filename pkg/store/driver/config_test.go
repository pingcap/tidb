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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/pd/client/opt"
)

func TestSetDefaultAndOptions(t *testing.T) {
	globalConfig := config.GetGlobalConfig()
	origSecurity := globalConfig.Security

	d := TiKVDriver{}
	security := config.Security{ClusterSSLCA: "test"}
	d.setDefaultAndOptions(WithSecurity(security))

	require.Equal(t, security, d.security)
	require.Equal(t, globalConfig.TiKVClient, d.tikvConfig)
	require.Equal(t, globalConfig.TxnLocalLatches, d.txnLocalLatches)
	require.Equal(t, globalConfig.PDClient, d.pdConfig)
	require.Equal(t, origSecurity, config.GetGlobalConfig().Security)

	metricscommon.SetConstLabels("keyspace_id", "42", "keyspace_name", "ks")
	t.Cleanup(func() {
		metricscommon.SetConstLabels()
	})
	pdOpt := opt.NewOption()
	for _, apply := range d.pdClientOptions() {
		apply(pdOpt)
	}
	require.Equal(t, metricscommon.GetConstLabels(), pdOpt.MetricsLabels)
}

func TestOpenKeyspaceRequirement(t *testing.T) {
	// Fail PD initialization locally so accepted inputs need no running cluster.
	missingCert := filepath.Join(t.TempDir(), "missing-cert.pem")
	security := config.Security{ClusterSSLCert: missingCert, ClusterSSLKey: missingCert}
	t.Cleanup(config.UpdateGlobal(func(cfg *config.Config) { cfg.Security = security }))
	d := &TiKVDriver{}
	for _, entry := range []struct {
		name string
		open func(string) (kv.Storage, error)
	}{
		{name: "Open", open: d.Open},
		{name: "OpenWithOptions", open: func(path string) (kv.Storage, error) {
			return d.OpenWithOptions(path, WithSecurity(security))
		}},
	} {
		t.Run(entry.name, func(t *testing.T) {
			for _, tc := range []struct {
				name, path  string
				hasKeyspace bool
			}{
				{name: "omitted", path: "tikv://127.0.0.1:1"},
				{name: "empty", path: "tikv://127.0.0.1:1?keyspaceName="},
				{name: "named", path: "tikv://127.0.0.1:1?keyspaceName=ks42", hasKeyspace: true},
				{name: "default", path: "tikv://127.0.0.1:1?keyspaceName=DEFAULT", hasKeyspace: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					store, err := entry.open(tc.path)
					require.Nil(t, store)
					if kerneltype.IsNextGen() && !tc.hasKeyspace {
						require.EqualError(t, err, "nextgen TiKV storage requires a keyspace name")
					} else {
						require.ErrorContains(t, err, missingCert, "accepted inputs must reach PD initialization")
					}
				})
			}
		})
	}
}
