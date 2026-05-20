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
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
)

func TestSetKeyspaceNameInConf(t *testing.T) {
	savedConfig := *config.GetGlobalConfig()
	t.Cleanup(func() {
		config.UpdateGlobal(func(conf *config.Config) {
			*conf = savedConfig
		})
		keyspaceNameBytes = nil
		genKeyspaceNameOnce = sync.Once{}
	})

	keyspaceNameInCfg := "test_keyspace_cfg"

	// Set KeyspaceName in conf
	c1 := config.GetGlobalConfig()
	c1.KeyspaceName = keyspaceNameInCfg

	getKeyspaceName := GetKeyspaceNameBySettings()
	// Check the keyspaceName which get from GetKeyspaceNameBytesBySettings, equals keyspaceNameInCfg which is in conf.
	// The cfg.keyspaceName get higher weights than KEYSPACE_NAME in system env.
	require.Equal(t, keyspaceNameInCfg, getKeyspaceName)
	require.Equal(t, false, IsKeyspaceNameEmpty(getKeyspaceName))

	// Make sure genKeyspaceNameOnce is called only once in this test.
	keyspaceNameBytes = nil
	genKeyspaceNameOnce = sync.Once{}
	getKeyspaceNameByte := GetKeyspaceNameBytesBySettings()
	if kerneltype.IsNextGen() {
		require.Equal(t, []byte(keyspaceNameInCfg), getKeyspaceNameByte)
	} else {
		require.Nil(t, getKeyspaceNameByte)
	}
}

func TestNoKeyspaceNameSet(t *testing.T) {
	resetKeyspaceNameCache := func() {
		keyspaceNameBytes = nil
		genKeyspaceNameOnce = sync.Once{}
	}
	restoreGlobalConfigAndCache := func(t *testing.T) {
		t.Helper()

		savedConfig := *config.GetGlobalConfig()
		t.Cleanup(func() {
			config.UpdateGlobal(func(conf *config.Config) {
				*conf = savedConfig
			})
			resetKeyspaceNameCache()
		})
	}

	t.Run("empty config and env", func(t *testing.T) {
		restoreGlobalConfigAndCache(t)
		t.Setenv(config.EnvVarKeyspaceName, "")
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = ""
		})

		getKeyspaceName := GetKeyspaceNameBySettings()
		require.Equal(t, "", getKeyspaceName)
		require.Equal(t, true, IsKeyspaceNameEmpty(getKeyspaceName))

		resetKeyspaceNameCache()
		getKeyspaceNameByte := GetKeyspaceNameBytesBySettings()
		if kerneltype.IsNextGen() {
			require.Equal(t, []byte(""), getKeyspaceNameByte)
		} else {
			require.Nil(t, getKeyspaceNameByte)
		}
	})

	t.Run("fallback to env", func(t *testing.T) {
		const keyspaceNameInEnv = "test_keyspace_env"

		restoreGlobalConfigAndCache(t)
		t.Setenv(config.EnvVarKeyspaceName, keyspaceNameInEnv)
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = ""
		})

		getKeyspaceName := GetKeyspaceNameBySettings()
		require.Empty(t, getKeyspaceName)
		require.True(t, IsKeyspaceNameEmpty(getKeyspaceName))
		require.Empty(t, config.GetGlobalKeyspaceName())

		resetKeyspaceNameCache()
		getKeyspaceNameByte := GetKeyspaceNameBytesBySettings()
		if kerneltype.IsNextGen() {
			require.Equal(t, []byte(""), getKeyspaceNameByte)
		} else {
			require.Nil(t, getKeyspaceNameByte)
		}
	})

	t.Run("config overrides env", func(t *testing.T) {
		const keyspaceNameInEnv = "test_keyspace_env_conflict"
		const keyspaceNameInCfg = "test_keyspace_cfg_priority"

		restoreGlobalConfigAndCache(t)
		t.Setenv(config.EnvVarKeyspaceName, keyspaceNameInEnv)
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = keyspaceNameInCfg
		})

		getKeyspaceName := GetKeyspaceNameBySettings()
		require.Equal(t, keyspaceNameInCfg, getKeyspaceName)
		require.Equal(t, false, IsKeyspaceNameEmpty(getKeyspaceName))
		require.Equal(t, keyspaceNameInCfg, config.GetGlobalKeyspaceName())

		resetKeyspaceNameCache()
		getKeyspaceNameByte := GetKeyspaceNameBytesBySettings()
		if kerneltype.IsNextGen() {
			require.Equal(t, []byte(keyspaceNameInCfg), getKeyspaceNameByte)
		} else {
			require.Nil(t, getKeyspaceNameByte)
		}
	})

	t.Run("bytes getter does not fall back to env before string getter", func(t *testing.T) {
		const keyspaceNameInEnv = "test_keyspace_env_first"

		restoreGlobalConfigAndCache(t)
		t.Setenv(config.EnvVarKeyspaceName, keyspaceNameInEnv)
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = ""
		})

		resetKeyspaceNameCache()
		getKeyspaceNameByte := GetKeyspaceNameBytesBySettings()
		if kerneltype.IsNextGen() {
			require.Equal(t, []byte(""), getKeyspaceNameByte)
			require.Empty(t, config.GetGlobalKeyspaceName())
		} else {
			require.Nil(t, getKeyspaceNameByte)
		}
		require.Empty(t, GetKeyspaceNameBySettings())
	})
}

func BenchmarkGetKeyspaceNameBytesBySettings(b *testing.B) {
	if !kerneltype.IsNextGen() {
		b.Skip("NextGen is not enabled, skipping benchmark")
		return
	}

	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "benchmark_keyspace"
	})
	b.Cleanup(func() {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = ""
		})
		keyspaceNameBytes = nil
		genKeyspaceNameOnce = sync.Once{}
	})

	var result []byte
	keyspaceNameBytes = nil
	genKeyspaceNameOnce = sync.Once{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = GetKeyspaceNameBytesBySettings()
	}
	_ = result
}
