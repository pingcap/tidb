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

package session

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/stretchr/testify/require"
)

func TestStarterBootstrapManifestValidationAndRendering(t *testing.T) {
	originConfig := config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(originConfig)
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = `ks'name`
	})

	manifest, err := parseStarterBootstrapManifest([]byte(`{
		"version": 3,
		"bootstrap": ["INSERT INTO mysql.tidb VALUES ('starter_manifest_test', '<keyspace>.root', 'test')"],
		"upgrades": [
			{"version": 3, "sql": ["INSERT INTO mysql.tidb VALUES ('starter_manifest_v3', '<keyspace>.v3', 'test')"]},
			{"version": 2, "sql": []}
		]
	}`))
	require.NoError(t, err)
	require.Equal(t, int64(3), manifest.Version)
	require.Len(t, manifest.Upgrades, 2)
	require.Equal(t, int64(2), manifest.Upgrades[0].Version)
	require.Equal(t, int64(3), manifest.Upgrades[1].Version)
	require.Equal(t, `SELECT 'ks\'name.root'`, renderStarterManifestSQL(`SELECT '<keyspace>.root'`))
}

func TestStarterBootstrapManifestValidationErrors(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
		err      string
	}{
		{
			name:     "unknown field",
			manifest: `{"version": 1, "bootstrap": [], "extra": []}`,
			err:      `unknown field "extra"`,
		},
		{
			name:     "invalid version",
			manifest: `{"version": 0}`,
			err:      "manifest version must be greater than 0",
		},
		{
			name:     "duplicate upgrade",
			manifest: `{"version": 2, "upgrades": [{"version": 2}, {"version": 2}]}`,
			err:      "duplicated upgrade version 2",
		},
		{
			name:     "upgrade past manifest version",
			manifest: `{"version": 2, "upgrades": [{"version": 3}]}`,
			err:      "upgrades[0].version 3 is greater than manifest version 2",
		},
		{
			name:     "unknown placeholder",
			manifest: `{"version": 1, "bootstrap": ["SELECT '<tenant>'"]}`,
			err:      `bootstrap[0] uses unsupported placeholder "<tenant>"`,
		},
		{
			name:     "empty sql block",
			manifest: `{"version": 1, "bootstrap": [" "]}`,
			err:      "bootstrap[0] must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseStarterBootstrapManifest([]byte(tt.manifest))
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestStarterBootstrapManifestLoadNoopOutsideStarter(t *testing.T) {
	if kerneltype.IsNextGen() {
		originMode := deploymode.Get()
		t.Cleanup(func() {
			require.NoError(t, deploymode.Set(originMode))
		})
		require.NoError(t, deploymode.Set(deploymode.Premium))
	}

	originConfig := config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(originConfig)
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.StarterParams.BootstrapManifestFile = filepath.Join(t.TempDir(), "missing.json")
	})

	manifest, err := loadStarterBootstrapManifest()
	require.NoError(t, err)
	require.Nil(t, manifest)
}

func TestStarterBootstrapManifestLoadInStarter(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("starter deploy mode is only available in nextgen")
	}

	manifestFile := filepath.Join(t.TempDir(), "starter-bootstrap.json")
	require.NoError(t, os.WriteFile(manifestFile, []byte(`{"version": 1, "bootstrap": ["SELECT 1"]}`), 0644))

	originMode := deploymode.Get()
	originConfig := config.GetGlobalConfig()
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originMode))
		config.StoreGlobalConfig(originConfig)
	})
	require.NoError(t, deploymode.Set(deploymode.Starter))
	config.UpdateGlobal(func(conf *config.Config) {
		conf.StarterParams.BootstrapManifestFile = manifestFile
	})

	manifest, err := loadStarterBootstrapManifest()
	require.NoError(t, err)
	require.NotNil(t, manifest)
	require.Equal(t, int64(1), manifest.Version)
	require.Equal(t, []string{"SELECT 1"}, manifest.Bootstrap)
}

func TestStarterBootstrapManifestBootstrapBlocks(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("classic mock store is sufficient for manifest SQL execution")
	}
	originConfig := config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(originConfig)
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "test_keyspace"
	})

	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})

	se := CreateSessionAndSetID(t, store)
	t.Cleanup(func() {
		se.Close()
	})
	err := executeStarterManifestSQLBlocks(se, []string{
		"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('starter_manifest_bootstrap_test', '<keyspace>.boot', 'test')",
	})
	require.NoError(t, err)
	require.NoError(t, updateStarterBootstrapVersion(se, 2))
	MustExec(t, se, "COMMIT")

	require.Equal(t, "2", mustGetTiDBVarForStarterManifest(t, se, starterBootstrapVersionVar))
	require.Equal(t, "test_keyspace.boot", mustGetTiDBVarForStarterManifest(t, se, "starter_manifest_bootstrap_test"))
}

func TestStarterBootstrapManifestUpgrade(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("classic mock store is sufficient for manifest upgrade execution")
	}
	originConfig := config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(originConfig)
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "test_keyspace"
	})

	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	t.Cleanup(func() {
		se.Close()
	})
	require.NoError(t, updateStarterBootstrapVersion(se, 1))
	MustExec(t, se, "COMMIT")

	manifest, err := parseStarterBootstrapManifest([]byte(`{
		"version": 3,
		"upgrades": [
			{"version": 2, "sql": [
				"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('starter_manifest_upgrade_v2', '<keyspace>.v2', 'test')"
			]},
			{"version": 3, "sql": [
				"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('starter_manifest_upgrade_v3', '<keyspace>.v3', 'test')"
			]}
		]
	}`))
	require.NoError(t, err)
	storedVersion, err := getStarterBootstrapVersion(se)
	require.NoError(t, err)
	require.NoError(t, upgradeStarterBootstrapManifestFromVersion(se, manifest, storedVersion))

	require.Equal(t, "3", mustGetTiDBVarForStarterManifest(t, se, starterBootstrapVersionVar))
	require.Equal(t, "test_keyspace.v2", mustGetTiDBVarForStarterManifest(t, se, "starter_manifest_upgrade_v2"))
	require.Equal(t, "test_keyspace.v3", mustGetTiDBVarForStarterManifest(t, se, "starter_manifest_upgrade_v3"))
}

func TestStarterBootstrapManifestUpgradeSkipsOlderManifest(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("classic mock store is sufficient for manifest upgrade execution")
	}

	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	t.Cleanup(func() {
		se.Close()
	})
	require.NoError(t, updateStarterBootstrapVersion(se, 5))
	MustExec(t, se, "COMMIT")

	manifest, err := parseStarterBootstrapManifest([]byte(`{"version": 3}`))
	require.NoError(t, err)
	require.NoError(t, upgradeStarterBootstrapManifestFromVersion(se, manifest, 5))
	require.Equal(t, "5", mustGetTiDBVarForStarterManifest(t, se, starterBootstrapVersionVar))
}

func mustGetTiDBVarForStarterManifest(t *testing.T, se sessionapi.Session, name string) string {
	t.Helper()
	rs := MustExecToRecodeSet(t, se, "SELECT variable_value FROM mysql.tidb WHERE variable_name = ?", name)
	t.Cleanup(func() {
		require.NoError(t, rs.Close())
	})
	req := rs.NewChunk(nil)
	err := rs.Next(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap), req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	return req.GetRow(0).GetString(0)
}
