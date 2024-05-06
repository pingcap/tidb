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
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
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

	// Check the keyspaceName which get from GetKeyspaceNameBySettings, equals keyspaceNameInCfg which is in conf.
	// The cfg.keyspaceName get higher weights than KEYSPACE_NAME in system env.
	require.Equal(t, keyspaceNameInCfg, getKeyspaceName)
	require.Equal(t, false, IsKeyspaceNameEmpty(getKeyspaceName))
}

func TestNoKeyspaceNameSet(t *testing.T) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = ""
	})

	getKeyspaceName := GetKeyspaceNameBySettings()

	require.Equal(t, "", getKeyspaceName)
	require.Equal(t, true, IsKeyspaceNameEmpty(getKeyspaceName))
}

func TestGetKeyspaceRange(t *testing.T) {
	keyspaceID := uint32(1)
	leftBound, rightBound := GetKeyspaceTxnRange(keyspaceID)
	expectLeftBound := codec.EncodeBytes(nil, []byte{'x', 0, 0, 1})
	expectRightBound := codec.EncodeBytes(nil, []byte{'x', 0, 0, 2})
	require.Equal(t, expectLeftBound, leftBound)
	require.Equal(t, expectRightBound, rightBound)

	// Check the max keyspace ID txn left boundary and txn right boundary.
	maxKeyspaceIDLeftBound, maxKeyspaceIDRightBound := GetKeyspaceTxnRange(maxKeyspaceID)
	expectMaxKeyspaceIDLeftBound := codec.EncodeBytes(nil, []byte{'x', 0xff, 0xff, 0xff})
	maxKeyspaceIDexpectRightBound := codec.EncodeBytes(nil, []byte{MaxKeyspaceRightBoundaryPrefix})
	require.Equal(t, expectMaxKeyspaceIDLeftBound, maxKeyspaceIDLeftBound)
	require.Equal(t, maxKeyspaceIDexpectRightBound, maxKeyspaceIDRightBound)
}

func TestGetKeyspaceGCManagementType(t *testing.T) {
	// Case 1: The keyspace use global GC by default.
	keyspaceMeta := keyspacepb.KeyspaceMeta{
		Id: 1,
	}

	require.Equal(t, false, IsKeyspaceUseKeyspaceLevelGC(&keyspaceMeta))
	require.Equal(t, true, IsKeyspaceMetaExistsAndUseGlobalGC(&keyspaceMeta))

	// Case 2: The keyspace is set to use global GC.
	keyspaceConfig := map[string]string{
		KeyspaceMetaConfigGCManagementType: KeyspaceMetaConfigGCManagementTypeGlobalGC,
	}
	keyspaceMeta = keyspacepb.KeyspaceMeta{
		Id:     1,
		Config: keyspaceConfig,
	}

	require.Equal(t, false, IsKeyspaceUseKeyspaceLevelGC(&keyspaceMeta))
	require.Equal(t, true, IsKeyspaceMetaExistsAndUseGlobalGC(&keyspaceMeta))

	// Case 3: The keyspace is set to use keyspace level GC.
	keyspaceConfig = map[string]string{
		KeyspaceMetaConfigGCManagementType: KeyspaceMetaConfigGCManagementTypeKeyspaceLevelGC,
	}
	keyspaceMeta = keyspacepb.KeyspaceMeta{
		Id:     1,
		Config: keyspaceConfig,
	}
	require.Equal(t, true, IsKeyspaceUseKeyspaceLevelGC(&keyspaceMeta))
	require.Equal(t, false, IsKeyspaceMetaExistsAndUseGlobalGC(&keyspaceMeta))

	// Case 4: The keyspace meta is nil, it means use global GC.
	require.Equal(t, false, IsKeyspaceUseKeyspaceLevelGC(nil))

	// Case 5: The keyspace meta is nil.
	require.Equal(t, false, IsKeyspaceMetaExistsAndUseGlobalGC(nil))
}
