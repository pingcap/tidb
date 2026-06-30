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

func TestStarterBootstrapFileValidationAndRendering(t *testing.T) {
	originConfig := config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(originConfig)
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = `ks'name`
	})

	bootstrapFile, err := parseStarterBootstrapFile([]byte(`{
		"version": 3,
		"bootstrap": ["INSERT INTO mysql.tidb VALUES ('starter_file_test', '<keyspace>.root', 'test')"],
		"upgrades": [
			{"version": 3, "sql": ["INSERT INTO mysql.tidb VALUES ('starter_file_v3', '<keyspace>.v3', 'test')"]},
			{"version": 2, "sql": []}
		]
	}`))
	require.NoError(t, err)
	require.Equal(t, int64(3), bootstrapFile.Version)
	require.Len(t, bootstrapFile.Upgrades, 2)
	require.Equal(t, int64(2), bootstrapFile.Upgrades[0].Version)
	require.Equal(t, int64(3), bootstrapFile.Upgrades[1].Version)
	require.Equal(t, `SELECT 'ks\'name.root'`, renderStarterBootstrapSQL(`SELECT '<keyspace>.root'`))
}

func TestStarterBootstrapFileValidationErrors(t *testing.T) {
	tests := []struct {
		name          string
		bootstrapFile string
		err           string
	}{
		{
			name:          "unknown field",
			bootstrapFile: `{"version": 1, "bootstrap": [], "extra": []}`,
			err:           `unknown field "extra"`,
		},
		{
			name:          "invalid version",
			bootstrapFile: `{"version": 0}`,
			err:           "bootstrap file version must be greater than 0",
		},
		{
			name:          "duplicate upgrade",
			bootstrapFile: `{"version": 2, "upgrades": [{"version": 2}, {"version": 2}]}`,
			err:           "duplicated upgrade version 2",
		},
		{
			name:          "upgrade past bootstrap file version",
			bootstrapFile: `{"version": 2, "upgrades": [{"version": 3}]}`,
			err:           "upgrades[0].version 3 is greater than bootstrap file version 2",
		},
		{
			name:          "unknown placeholder",
			bootstrapFile: `{"version": 1, "bootstrap": ["SELECT '<tenant>'"]}`,
			err:           `bootstrap[0] uses unsupported placeholder "<tenant>"`,
		},
		{
			name:          "empty sql block",
			bootstrapFile: `{"version": 1, "bootstrap": [" "]}`,
			err:           "bootstrap[0] must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseStarterBootstrapFile([]byte(tt.bootstrapFile))
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestStarterBootstrapFileLoadNoopOutsideStarter(t *testing.T) {
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
		conf.StarterParams.BootstrapFile = filepath.Join(t.TempDir(), "missing.json")
	})

	bootstrapFile, err := loadStarterBootstrapFile()
	require.NoError(t, err)
	require.Nil(t, bootstrapFile)
}

func TestStarterBootstrapFileLoadInStarter(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("starter deploy mode is only available in nextgen")
	}

	bootstrapFilePath := filepath.Join(t.TempDir(), "starter-bootstrap.json")
	require.NoError(t, os.WriteFile(bootstrapFilePath, []byte(`{"version": 1, "bootstrap": ["SELECT 1"]}`), 0644))

	originMode := deploymode.Get()
	originConfig := config.GetGlobalConfig()
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originMode))
		config.StoreGlobalConfig(originConfig)
	})
	require.NoError(t, deploymode.Set(deploymode.Starter))
	config.UpdateGlobal(func(conf *config.Config) {
		conf.StarterParams.BootstrapFile = bootstrapFilePath
	})

	bootstrapFile, err := loadStarterBootstrapFile()
	require.NoError(t, err)
	require.NotNil(t, bootstrapFile)
	require.Equal(t, int64(1), bootstrapFile.Version)
	require.Equal(t, []string{"SELECT 1"}, bootstrapFile.Bootstrap)
}

func TestStarterBootstrapFileBootstrapBlocks(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("classic mock store is sufficient for bootstrap file SQL execution")
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
	err := executeStarterBootstrapSQLBlocks(se, []string{
		"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('starter_file_bootstrap_test', '<keyspace>.boot', 'test')",
	})
	require.NoError(t, err)
	require.NoError(t, updateStarterBootstrapVersion(se, 2))
	MustExec(t, se, "COMMIT")

	require.Equal(t, "2", mustGetTiDBVarForStarterFile(t, se, starterBootstrapVersionVar))
	require.Equal(t, "test_keyspace.boot", mustGetTiDBVarForStarterFile(t, se, "starter_file_bootstrap_test"))
}

func TestStarterBootstrapFileUpgrade(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("classic mock store is sufficient for bootstrap file upgrade execution")
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

	bootstrapFile, err := parseStarterBootstrapFile([]byte(`{
		"version": 3,
		"upgrades": [
			{"version": 2, "sql": [
				"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('starter_file_upgrade_v2', '<keyspace>.v2', 'test')"
			]},
			{"version": 3, "sql": [
				"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('starter_file_upgrade_v3', '<keyspace>.v3', 'test')"
			]}
		]
	}`))
	require.NoError(t, err)
	storedVersion, err := getStarterBootstrapVersion(se)
	require.NoError(t, err)
	require.NoError(t, upgradeStarterBootstrapFromVersion(se, bootstrapFile, storedVersion))

	require.Equal(t, "3", mustGetTiDBVarForStarterFile(t, se, starterBootstrapVersionVar))
	require.Equal(t, "test_keyspace.v2", mustGetTiDBVarForStarterFile(t, se, "starter_file_upgrade_v2"))
	require.Equal(t, "test_keyspace.v3", mustGetTiDBVarForStarterFile(t, se, "starter_file_upgrade_v3"))
}

func TestStarterBootstrapFileUpgradeSkipsOlderFile(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("classic mock store is sufficient for bootstrap file upgrade execution")
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

	bootstrapFile, err := parseStarterBootstrapFile([]byte(`{"version": 3}`))
	require.NoError(t, err)
	require.NoError(t, upgradeStarterBootstrapFromVersion(se, bootstrapFile, 5))
	require.Equal(t, "5", mustGetTiDBVarForStarterFile(t, se, starterBootstrapVersionVar))
}

func mustGetTiDBVarForStarterFile(t *testing.T, se sessionapi.Session, name string) string {
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
