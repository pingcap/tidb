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
	"encoding/hex"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

func TestKeyspaceRange(t *testing.T) {

	left := GetKeyspaceTxnLeftBound(0xffffff)

	logutil.BgLogger().Info("[gc worker] resolve locks by range",
		zap.String("txnLeftBound", hex.EncodeToString(left)),
	)
}
