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

package keyspace

import (
	"os"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestSetKeyspaceNameInConf(t *testing.T) {
	expectedKeyspaceName := "test_keyspace_cfg"
	// Reset keyspace name in TiDB config.
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = expectedKeyspaceName
	})

	// Set KeyspaceName in conf
	getKeyspaceName := GetKeyspaceNameBySettings()

	// Check the keyspaceName which get from GetKeyspaceNameBySettings, equals expectedKeyspaceName which is in conf.
	// The cfg.keyspaceName get higher weights than KEYSPACE_NAME in system env.
	require.Equal(t, expectedKeyspaceName, getKeyspaceName)
	require.Equal(t, expectedKeyspaceName, config.GetGlobalKeyspaceName())
	require.Equal(t, false, IsKeyspaceNameEmpty(getKeyspaceName))
}

func TestSetKeyspaceNameByEnv(t *testing.T) {
	// Reset keyspace name in TiDB config.
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = ""
	})

	// Set keyspace name by system env.
	expectedKeyspaceName := "test_keyspace_02"
	os.Setenv(config.EnvVarKeyspaceName, expectedKeyspaceName)

	getKeyspaceName := GetKeyspaceNameBySettings()

	// Check the keyspaceName which get from GetKeyspaceNameBySettings, equals expectedKeyspaceName which is in conf.
	// The cfg.keyspaceName get higher weights than KEYSPACE_NAME in system env.
	require.Equal(t, expectedKeyspaceName, getKeyspaceName)
	require.Equal(t, expectedKeyspaceName, config.GetGlobalKeyspaceName())
	require.Equal(t, false, IsKeyspaceNameEmpty(getKeyspaceName))

	os.Unsetenv(config.EnvVarKeyspaceName)
}

func TestNoKeyspaceNameSet(t *testing.T) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = ""
	})

	getKeyspaceName := GetKeyspaceNameBySettings()

	require.Equal(t, "", getKeyspaceName)
	require.Equal(t, true, IsKeyspaceNameEmpty(getKeyspaceName))
}
