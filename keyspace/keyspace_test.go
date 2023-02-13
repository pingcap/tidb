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

	"github.com/pingcap/tidb/config"
	"github.com/stretchr/testify/suite"
)

type keyspaceSuite struct {
	suite.Suite
}

func TestSetKeyspaceName(t *testing.T) {
	suite.Run(t, new(keyspaceSuite))
}

func (k *keyspaceSuite) TearDownTest() {
	// Clear keyspace setting
	conf := config.GetGlobalConfig()
	conf.KeyspaceName = ""
	config.StoreGlobalConfig(conf)
}

func (k *keyspaceSuite) TestSetKeyspaceNameInConf() {
	keyspaceNameInCfg := "test_keyspace_cfg"

	// Set KeyspaceName in conf
	c1 := config.GetGlobalConfig()
	c1.KeyspaceName = keyspaceNameInCfg

	getKeyspaceName := GetKeyspaceNameBySettings()

	// Check the keyspaceName which get from GetKeyspaceNameBySettings, equals keyspaceNameInCfg which is in conf.
	// The cfg.keyspaceName get higher weights than KEYSPACE_NAME in system env.
	k.Equal(keyspaceNameInCfg, getKeyspaceName)
	k.Equal(false, IsKeyspaceNameEmpty(getKeyspaceName))
}

func (k *keyspaceSuite) TestNoKeyspaceNameSet() {
	getKeyspaceName := GetKeyspaceNameBySettings()

	k.Equal("", getKeyspaceName)
	k.Equal(true, IsKeyspaceNameEmpty(getKeyspaceName))
}
