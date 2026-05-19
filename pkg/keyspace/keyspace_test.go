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

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestSetKeyspaceNameInConf(t *testing.T) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = ""
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
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = ""
	})

	getKeyspaceName := GetKeyspaceNameBySettings()
	require.Equal(t, "", getKeyspaceName)
	require.Equal(t, true, IsKeyspaceNameEmpty(getKeyspaceName))

	// Make sure genKeyspaceNameOnce is called only once in this test.
	keyspaceNameBytes = nil
	genKeyspaceNameOnce = sync.Once{}
	getKeyspaceNameByte := GetKeyspaceNameBytesBySettings()
	if kerneltype.IsNextGen() {
		require.Equal(t, []byte(""), getKeyspaceNameByte)
	} else {
		require.Nil(t, getKeyspaceNameByte)
	}
}

func TestSetKeyspaceMeta(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	t.Cleanup(func() {
		keyspaceMeta.Delete("test_keyspace")
	})

	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = ""
	})

	meta := &keyspacepb.KeyspaceMeta{Id: 42, Name: "test_keyspace"}
	SetKeyspaceMeta(meta)

	got, ok := GetKeyspaceMeta("test_keyspace")
	require.True(t, ok)
	require.Equal(t, uint32(42), got.GetId())
	require.Equal(t, "test_keyspace", got.GetName())

	SetKeyspaceMeta(nil)
	_, ok = GetKeyspaceMeta("")
	require.False(t, ok)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "test_keyspace"
	})
	core, logs := observer.New(zap.InfoLevel)
	logger := zap.New(core, WrapZapcoreWithKeyspace())
	logger.Info("test")
	entries := logs.All()
	require.Len(t, entries, 1)
	require.Equal(t, "test_keyspace", entries[0].ContextMap()["keyspaceName"])
	require.Equal(t, uint32(42), entries[0].ContextMap()["keyspaceID"])
}

func BenchmarkGetKeyspaceNameBytesBySettings(b *testing.B) {
	if !kerneltype.IsNextGen() {
		b.Skip("NextGen is not enabled, skipping benchmark")
		return
	}

	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "benchmark_keyspace"
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
