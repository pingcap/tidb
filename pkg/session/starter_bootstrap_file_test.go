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
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
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
	require.Len(t, bootstrapFile.BootstrapSQLBlocks, 1)
	require.Len(t, bootstrapFile.Upgrades[1].SQLBlocks, 1)
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
	require.Equal(t, []string{"SELECT 1"}, bootstrapFile.BootstrapSQLBlocks)
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
	err = executeStarterBootstrapSQLBlocks(se, []string{"SELECT 1; SELECT 2"})
	require.ErrorContains(t, err, "SQL block 0 must contain exactly one statement")
	require.NoError(t, updateStarterBootstrapVersion(se, 2))
	MustExec(t, se, "COMMIT")

	require.Equal(t, "2", mustGetTiDBVarForStarterFile(t, se, starterBootstrapVersionVar))
	require.Equal(t, "test_keyspace.boot", mustGetTiDBVarForStarterFile(t, se, "starter_file_bootstrap_test"))
}

func TestStarterBootstrapFileInitialBootstrap(t *testing.T) {
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
	missingRootFile, err := parseStarterBootstrapFile([]byte(`{
		"version": 3,
		"bootstrap": [
			"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('starter_file_initial_missing_root', '<keyspace>.boot', 'test')"
		]
	}`))
	require.NoError(t, err)
	require.ErrorContains(t, runStarterBootstrapLocked(se, missingRootFile),
		"must create 'test_keyspace.root'@'%'")
	_, isNull, err := getTiDBVar(se, "starter_file_initial_missing_root")
	require.NoError(t, err)
	require.True(t, isNull)

	bootstrapFile, err := parseStarterBootstrapFile([]byte(`{
		"version": 3,
		"bootstrap": [
			"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('starter_file_initial_bootstrap', '<keyspace>.boot', 'test')",
			"INSERT INTO mysql.user (Host, User, authentication_string, plugin) VALUES ('%', '<keyspace>.root', '', 'mysql_native_password')"
		],
		"upgrades": [
			{"version": 3, "sql": [
				"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('starter_file_initial_upgrade', '<keyspace>.upgrade', 'test')"
			]}
		]
	}`))
	require.NoError(t, err)

	require.NoError(t, runStarterBootstrapLocked(se, bootstrapFile))
	require.Equal(t, "3", mustGetTiDBVarForStarterFile(t, se, starterBootstrapVersionVar))
	require.Equal(t, "test_keyspace.boot", mustGetTiDBVarForStarterFile(t, se, "starter_file_initial_bootstrap"))
	require.Equal(t, int64(1), mustCountStarterPrivilegeRows(t, se,
		"SELECT COUNT(*) FROM mysql.user WHERE Host = '%' AND User = ?", "test_keyspace.root"))
	_, isNull, err = getTiDBVar(se, "starter_file_initial_upgrade")
	require.NoError(t, err)
	require.True(t, isNull)
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
				"SET SESSION sql_mode = 'ANSI_QUOTES'",
				"CREATE TABLE test.\"starter_file_ansi_quotes\" (\"id\" INT)",
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
	require.Equal(t, int64(1), mustCountStarterPrivilegeRows(t, se,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'test' AND table_name = 'starter_file_ansi_quotes'"))
}

func TestStarterBootstrapFileUpgradePartialFailure(t *testing.T) {
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
	require.NoError(t, updateStarterBootstrapVersion(se, 1))
	MustExec(t, se, "COMMIT")

	bootstrapFile, err := parseStarterBootstrapFile([]byte(`{
		"version": 2,
		"upgrades": [{
			"version": 2,
			"sql": [
				"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('starter_file_upgrade_partial_failure', 'first', 'test')",
				"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('starter_file_upgrade_partial_failure', 'second', 'test')"
			]
		}]
	}`))
	require.NoError(t, err)
	err = upgradeStarterBootstrapFromVersion(se, bootstrapFile, 1)
	require.Error(t, err)

	checkSe := CreateSessionAndSetID(t, store)
	t.Cleanup(func() {
		checkSe.Close()
	})
	require.Equal(t, "1", mustGetTiDBVarForStarterFile(t, checkSe, starterBootstrapVersionVar))
	require.Equal(t, "first", mustGetTiDBVarForStarterFile(t, checkSe, "starter_file_upgrade_partial_failure"))
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

func TestStarterBootstrapStoreVersionGate(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("classic mock store is sufficient for starter bootstrap reconciliation")
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
		if dom != nil {
			dom.Close()
		}
		require.NoError(t, store.Close())
	})
	bootstrapFile, err := parseStarterBootstrapFile([]byte(`{
		"version": 3,
		"bootstrap": [
			"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('starter_file_store_version', 'initialized', 'test')",
			"INSERT INTO mysql.user (Host, User, authentication_string, plugin) VALUES ('%', '<keyspace>.root', '', 'mysql_native_password')"
		]
	}`))
	require.NoError(t, err)

	dom.Close()
	dom = nil
	require.NoError(t, upgradeStarterBootstrapWithFile(store, bootstrapFile))
	completedVersion, err := getStoreStarterBootstrapVersion(store)
	require.NoError(t, err)
	require.Equal(t, int64(3), completedVersion)

	dom, err = BootstrapSession(store)
	require.NoError(t, err)
	se := CreateSessionAndSetID(t, store)
	t.Cleanup(func() {
		se.Close()
	})
	require.Equal(t, "3", mustGetTiDBVarForStarterFile(t, se, starterBootstrapVersionVar))
	require.Equal(t, "initialized", mustGetTiDBVarForStarterFile(t, se, "starter_file_store_version"))

	mappedDomain, err := domap.Get(store)
	require.NoError(t, err)
	require.Same(t, dom, mappedDomain)
	currentBootstrapFile := *bootstrapFile
	currentBootstrapFile.BootstrapSQLBlocks = []string{"CREATE TABLE mysql.starter_file_noop (id INT)"}
	require.NoError(t, upgradeStarterBootstrapWithFile(store, &currentBootstrapFile))
	mappedDomain, err = domap.Get(store)
	require.NoError(t, err)
	require.Same(t, dom, mappedDomain)

	require.NoError(t, finishStarterBootstrap(store, 0))
	dom.Close()
	dom = nil
	require.NoError(t, upgradeStarterBootstrapWithFile(store, bootstrapFile))
	completedVersion, err = getStoreStarterBootstrapVersion(store)
	require.NoError(t, err)
	require.Equal(t, int64(3), completedVersion)
}

func TestStarterPrivilegeResetMetadataState(t *testing.T) {
	tests := []struct {
		name           string
		config         map[string]string
		pendingMarkers map[string]string
		err            string
	}{
		{
			name: "ordinary keyspace",
		},
		{
			name: "restore pending",
			config: map[string]string{
				restoreResetDoneKey: "False",
			},
			pendingMarkers: map[string]string{
				restoreResetDoneKey: "False",
			},
		},
		{
			name: "restore complete",
			config: map[string]string{
				restoreResetDoneKey: "true",
			},
		},
		{
			name: "branch pending",
			config: map[string]string{
				branchResetDoneKey: "False",
			},
			pendingMarkers: map[string]string{
				branchResetDoneKey: "False",
			},
		},
		{
			name: "branch complete",
			config: map[string]string{
				branchResetDoneKey: "true",
			},
		},
		{
			name: "branch complete and restore pending",
			config: map[string]string{
				branchResetDoneKey:  "true",
				restoreResetDoneKey: "False",
			},
			pendingMarkers: map[string]string{
				restoreResetDoneKey: "False",
			},
		},
		{
			name: "branch and restore pending",
			config: map[string]string{
				branchResetDoneKey:  "False",
				restoreResetDoneKey: "false",
			},
			pendingMarkers: map[string]string{
				branchResetDoneKey:  "False",
				restoreResetDoneKey: "false",
			},
		},
		{
			name: "invalid marker",
			config: map[string]string{
				restoreResetDoneKey: "invalid",
			},
			err: "invalid starter privilege reset marker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, pending, err := parsePrivilegeReset(tt.config)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				require.False(t, pending)
				require.Empty(t, state.pendingMarkers)
				return
			}
			require.NoError(t, err)
			require.Equal(t, len(tt.pendingMarkers) > 0, pending)
			require.Equal(t, tt.pendingMarkers, state.pendingMarkers)
		})
	}
}

func TestStarterPrivilegeResetWorkflow(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("classic mock store is sufficient for starter privilege reset orchestration")
	}

	originConfig := config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(originConfig)
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "restored_keyspace"
	})

	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Id:   42,
		Name: "restored_keyspace",
		Config: map[string]string{
			branchResetDoneKey:  "False",
			restoreResetDoneKey: "False",
		},
	}
	underlyingStore, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	pdHTTPClient := &starterResetPDHTTPClient{
		keyspaceMeta:      keyspaceMeta,
		remainingFailures: 1,
	}
	pdClient := &starterResetPDClient{
		Client:       underlyingStore.(kv.StorageWithPD).GetPDClient(),
		keyspaceMeta: keyspaceMeta,
	}
	codec := &starterResetCodec{
		Codec:        underlyingStore.GetCodec(),
		keyspaceMeta: keyspaceMeta,
	}
	store := &starterResetStorage{
		Storage:      underlyingStore,
		codec:        codec,
		pdClient:     pdClient,
		pdHTTPClient: pdHTTPClient,
	}
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	_, _, err = loadPrivilegeResetFromPD(&storageWithoutPD{Storage: store})
	require.ErrorContains(t, err, "PD client is required")

	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	se := CreateSessionAndSetID(t, store)
	seedStarterPrivilegeRows(t, se, "source_keyspace.user")
	require.NoError(t, updateStarterBootstrapVersion(se, 2))
	MustExec(t, se, "COMMIT")
	require.NoError(t, finishStarterBootstrap(store, 2))
	se.Close()
	dom.Close()

	bootstrapFile := validStarterPrivilegeBootstrapFile(t)
	err = upgradeStarterBootstrapWithFile(store, bootstrapFile)
	require.ErrorContains(t, err, "transient keyspace config update")
	require.Equal(t, 1, pdHTTPClient.updateCalls)
	completedVersion, err := getStoreStarterBootstrapVersion(store)
	require.NoError(t, err)
	require.Equal(t, int64(3), completedVersion)
	require.Equal(t, "False", keyspaceMeta.Config[branchResetDoneKey])
	require.Equal(t, "False", keyspaceMeta.Config[restoreResetDoneKey])

	dom, err = BootstrapSession(store)
	require.NoError(t, err)
	se = CreateSessionAndSetID(t, store)
	requireStarterPrivilegeRows(t, se, "source_keyspace.user", 0)
	requireStarterRootUser(t, se)
	se.Close()
	dom.Close()

	require.NoError(t, upgradeStarterBootstrapWithFile(store, bootstrapFile))
	require.Equal(t, 2, pdHTTPClient.updateCalls)
	require.Equal(t, "True", keyspaceMeta.Config[branchResetDoneKey])
	require.Equal(t, "True", keyspaceMeta.Config[restoreResetDoneKey])
	require.NoError(t, upgradeStarterBootstrapWithFile(store, bootstrapFile))
	require.Equal(t, 2, pdHTTPClient.updateCalls)

	codec.keyspaceMeta = &keyspacepb.KeyspaceMeta{
		Id:   keyspaceMeta.Id,
		Name: keyspaceMeta.Name,
		Config: map[string]string{
			restoreResetDoneKey: "False",
		},
	}
	require.NoError(t, upgradeStarterBootstrapWithFile(store, bootstrapFile))
	require.Equal(t, 3, pdClient.loadCalls)
	require.Equal(t, 2, pdHTTPClient.updateCalls)
	codec.keyspaceMeta = keyspaceMeta

	dom, err = BootstrapSession(store)
	require.NoError(t, err)
	t.Cleanup(dom.Close)
	se = CreateSessionAndSetID(t, store)
	t.Cleanup(se.Close)
	requireStarterPrivilegeRows(t, se, "source_keyspace.user", 0)
	requireStarterRootUser(t, se)

	keyspaceMeta.Config[restoreResetDoneKey] = "invalid"
	seedStarterPrivilegeRows(t, se, "invalid_marker.user")
	err = upgradeStarterBootstrapWithFile(store, validStarterPrivilegeBootstrapFile(t))
	require.ErrorContains(t, err, "invalid starter privilege reset marker")
	requireStarterPrivilegeRows(t, se, "invalid_marker.user", 1)
}

func TestStarterPrivilegeReset(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("classic mock store is sufficient for starter privilege reset")
	}

	t.Run("validation does not mutate privileges", func(t *testing.T) {
		_, se := newStarterPrivilegeResetSession(t)
		require.ErrorContains(t, resetPrivilegesLocked(se, &starterBootstrapFileSpec{Version: 3}),
			"must contain bootstrap SQL")

		nonTransactionalFile, err := parseStarterBootstrapFile([]byte(`{
			"version": 3,
			"bootstrap": ["CREATE TABLE mysql.starter_reset_ddl (id INT)"]
		}`))
		require.NoError(t, err)
		require.ErrorContains(t, resetPrivilegesLocked(se, nonTransactionalFile),
			"must be INSERT, REPLACE, UPDATE, or DELETE")
		requireStarterPrivilegeRows(t, se, "source_keyspace.user", 1)
	})

	t.Run("missing root converges on retry", func(t *testing.T) {
		_, se := newStarterPrivilegeResetSession(t)
		missingRootFile, err := parseStarterBootstrapFile([]byte(`{
			"version": 3,
			"bootstrap": [
				"INSERT INTO mysql.user (Host, User) VALUES ('%', '<keyspace>.not_root')"
			]
		}`))
		require.NoError(t, err)
		require.ErrorContains(t, resetPrivilegesLocked(se, missingRootFile),
			"must create 'restored_keyspace.root'@'%'")
		requireStarterPrivilegeRows(t, se, "source_keyspace.user", 0)
		require.Equal(t, int64(0), mustCountStarterPrivilegeRows(t, se,
			"SELECT COUNT(*) FROM mysql.user WHERE User = ?", "restored_keyspace.not_root"))

		require.NoError(t, resetPrivilegesLocked(se, validStarterPrivilegeBootstrapFile(t)))
		requireStarterRootUser(t, se)
	})

	t.Run("execution failure converges on retry", func(t *testing.T) {
		_, se := newStarterPrivilegeResetSession(t)
		failingBootstrapFile, err := parseStarterBootstrapFile([]byte(`{
			"version": 3,
			"bootstrap": [
				"INSERT INTO mysql.user (Host, User) VALUES ('%', '<keyspace>.failed')",
				"INSERT INTO mysql.user (Host, User) VALUES ('%', '<keyspace>.failed')"
			]
		}`))
		require.NoError(t, err)
		require.Error(t, resetPrivilegesLocked(se, failingBootstrapFile))
		requireStarterPrivilegeRows(t, se, "source_keyspace.user", 0)
		require.Equal(t, int64(0), mustCountStarterPrivilegeRows(t, se,
			"SELECT COUNT(*) FROM mysql.user WHERE Host = '%' AND User = ?", "restored_keyspace.failed"))

		require.NoError(t, resetPrivilegesLocked(se, validStarterPrivilegeBootstrapFile(t)))
		requireStarterRootUser(t, se)
	})

	t.Run("bounded reset clears all grants", func(t *testing.T) {
		store, se := newStarterPrivilegeResetSession(t)
		seedManyStarterUsers(t, se, 1024)

		originalLimit := kv.TxnTotalSizeLimit.Load()
		kv.TxnTotalSizeLimit.Store(32 * 1024)
		t.Cleanup(func() {
			kv.TxnTotalSizeLimit.Store(originalLimit)
		})
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
		_, err := se.ExecuteInternal(ctx, "DELETE FROM mysql.user")
		require.ErrorContains(t, err, "txn too large")
		_, rollbackErr := se.ExecuteInternal(ctx, "ROLLBACK")
		require.NoError(t, rollbackErr)

		require.NoError(t, resetPrivilegesLocked(se, validStarterPrivilegeBootstrapFile(t)))
		requireStarterPrivilegeRows(t, se, "source_keyspace.user", 0)
		require.Equal(t, int64(0), mustCountStarterPrivilegeRows(t, se,
			"SELECT COUNT(*) FROM mysql.user WHERE User LIKE 'source_keyspace.user%'"))
		requireStarterRootUser(t, se)
		require.Equal(t, int64(1), mustCountStarterPrivilegeRows(t, se,
			"SELECT COUNT(*) FROM mysql.password_history WHERE User = ?", "source_keyspace.user"))

		MustExec(t, se, "CREATE TABLE test.t (c INT)")
		MustExec(t, se, "CREATE USER 'source_keyspace.user'@'%'")
		userSe := CreateSessionAndSetID(t, store)
		t.Cleanup(userSe.Close)
		require.NoError(t, userSe.Auth(&auth.UserIdentity{Username: "source_keyspace.user", Hostname: "localhost"}, nil, nil, nil))
		rs, err := exec(userSe, "SELECT c FROM test.t")
		if rs != nil {
			require.NoError(t, rs.Close())
		}
		require.ErrorContains(t, err, "SELECT command denied")
	})
}

func newStarterPrivilegeResetSession(t *testing.T) (kv.Storage, sessionapi.Session) {
	t.Helper()
	originConfig := config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(originConfig)
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "restored_keyspace"
	})

	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})
	se := CreateSessionAndSetID(t, store)
	t.Cleanup(se.Close)
	seedStarterPrivilegeRows(t, se, "source_keyspace.user")
	require.NoError(t, updateStarterBootstrapVersion(se, 3))
	MustExec(t, se, "COMMIT")
	require.NoError(t, finishStarterBootstrap(store, 3))
	return store, se
}

func validStarterPrivilegeBootstrapFile(t *testing.T) *starterBootstrapFileSpec {
	t.Helper()
	bootstrapFile, err := parseStarterBootstrapFile([]byte(`{
		"version": 3,
		"bootstrap": [
			"INSERT INTO mysql.user (Host, User, authentication_string, plugin) VALUES ('%', '<keyspace>.root', '', 'mysql_native_password')"
		]
	}`))
	require.NoError(t, err)
	return bootstrapFile
}

func requireStarterPrivilegeRows(t *testing.T, se sessionapi.Session, user string, expected int64) {
	t.Helper()
	for _, table := range privilegeResetTables {
		userColumn := "User"
		if table == "role_edges" {
			userColumn = "TO_USER"
		}
		require.Equal(t, expected, mustCountStarterPrivilegeRows(t, se,
			"SELECT COUNT(*) FROM mysql."+table+" WHERE "+userColumn+" = ?", user), table)
	}
}

func requireStarterRootUser(t *testing.T, se sessionapi.Session) {
	t.Helper()
	require.Equal(t, int64(1), mustCountStarterPrivilegeRows(t, se,
		"SELECT COUNT(*) FROM mysql.user WHERE Host = '%' AND User = ? AND authentication_string = ''",
		"restored_keyspace.root"))
	require.Equal(t, "3", mustGetTiDBVarForStarterFile(t, se, starterBootstrapVersionVar))
}

func seedManyStarterUsers(t *testing.T, se sessionapi.Session, count int) {
	t.Helper()
	var sql strings.Builder
	sql.WriteString("INSERT INTO mysql.user (Host, User) VALUES ")
	for i := range count {
		if i > 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "('%%','source_keyspace.user%04d')", i)
	}
	MustExec(t, se, sql.String())
}

type storageWithoutPD struct {
	kv.Storage
}

type starterResetStorage struct {
	kv.Storage
	codec        tikv.Codec
	pdClient     pd.Client
	pdHTTPClient pdhttp.Client
}

func (s *starterResetStorage) GetCodec() tikv.Codec {
	return s.codec
}

func (s *starterResetStorage) GetPDClient() pd.Client {
	return s.pdClient
}

func (s *starterResetStorage) GetPDHTTPClient() pdhttp.Client {
	return s.pdHTTPClient
}

type starterResetCodec struct {
	tikv.Codec
	keyspaceMeta *keyspacepb.KeyspaceMeta
}

func (c *starterResetCodec) GetKeyspaceMeta() *keyspacepb.KeyspaceMeta {
	return c.keyspaceMeta
}

type starterResetPDClient struct {
	pd.Client
	keyspaceMeta *keyspacepb.KeyspaceMeta
	loadCalls    int
}

func (c *starterResetPDClient) LoadKeyspace(_ context.Context, name string) (*keyspacepb.KeyspaceMeta, error) {
	c.loadCalls++
	if name != c.keyspaceMeta.Name {
		return nil, fmt.Errorf("unexpected keyspace %q", name)
	}
	return c.keyspaceMeta, nil
}

type starterResetPDHTTPClient struct {
	pdhttp.Client
	keyspaceMeta      *keyspacepb.KeyspaceMeta
	remainingFailures int
	updateCalls       int
}

func (c *starterResetPDHTTPClient) WithCallerID(string) pdhttp.Client {
	return c
}

func (c *starterResetPDHTTPClient) WithRespHandler(func(*http.Response, any) error) pdhttp.Client {
	return c
}

func (*starterResetPDHTTPClient) GetPlacementRuleGroupByID(context.Context, string) (*pdhttp.RuleGroup, error) {
	return nil, errors.New("placement rules are unavailable in this test")
}

func (*starterResetPDHTTPClient) GetStores(context.Context) (*pdhttp.StoresInfo, error) {
	return &pdhttp.StoresInfo{}, nil
}

func (c *starterResetPDHTTPClient) UpdateKeyspaceConfig(
	_ context.Context,
	keyspaceName string,
	params *pdhttp.UpdateKeyspaceConfigParams,
) (*keyspacepb.KeyspaceMeta, error) {
	c.updateCalls++
	if keyspaceName != c.keyspaceMeta.Name {
		return nil, fmt.Errorf("unexpected keyspace %q", keyspaceName)
	}
	for key, expected := range params.Preconditions {
		actual, ok := c.keyspaceMeta.Config[key]
		if expected == nil {
			if ok {
				return nil, fmt.Errorf("keyspace config precondition failed for %s", key)
			}
			continue
		}
		if !ok || actual != *expected {
			return nil, fmt.Errorf("keyspace config precondition failed for %s", key)
		}
	}
	if c.remainingFailures > 0 {
		c.remainingFailures--
		return nil, errors.New("transient keyspace config update")
	}
	for key, value := range params.Config {
		if value == nil {
			delete(c.keyspaceMeta.Config, key)
			continue
		}
		c.keyspaceMeta.Config[key] = *value
	}
	return c.keyspaceMeta, nil
}

func seedStarterPrivilegeRows(t *testing.T, se sessionapi.Session, user string) {
	t.Helper()
	MustExec(t, se, "INSERT INTO mysql.columns_priv (Host, DB, User, Table_name, Column_name, Column_priv) VALUES ('%', 'test', ?, 't', 'c', 'Select')", user)
	MustExec(t, se, "INSERT INTO mysql.db (Host, DB, User) VALUES ('%', 'test', ?)", user)
	MustExec(t, se, "INSERT INTO mysql.default_roles (Host, User, DEFAULT_ROLE_HOST, DEFAULT_ROLE_USER) VALUES ('%', ?, '%', 'source_role')", user)
	MustExec(t, se, "INSERT INTO mysql.global_grants (User, Host, Priv) VALUES (?, '%', 'BACKUP_ADMIN')", user)
	MustExec(t, se, "INSERT INTO mysql.global_priv (Host, User, Priv) VALUES ('%', ?, '{}')", user)
	MustExec(t, se, "INSERT INTO mysql.password_history (Host, User, Password) VALUES ('%', ?, 'hash')", user)
	MustExec(t, se, "INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ('%', 'source_role', '%', ?)", user)
	MustExec(t, se, "INSERT INTO mysql.user (Host, User) VALUES ('%', ?)", user)
	MustExec(t, se, "INSERT INTO mysql.tables_priv (Host, DB, User, Table_name, Table_priv) VALUES ('%', 'test', ?, 't', 'Select')", user)
}

func mustCountStarterPrivilegeRows(t *testing.T, se sessionapi.Session, sql string, args ...any) int64 {
	t.Helper()
	rs := MustExecToRecodeSet(t, se, sql, args...)
	t.Cleanup(func() {
		require.NoError(t, rs.Close())
	})
	req := rs.NewChunk(nil)
	err := rs.Next(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap), req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	return req.GetRow(0).GetInt64(0)
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
