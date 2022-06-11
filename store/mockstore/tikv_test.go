// Copyright 2018 PingCAP, Inc.
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

package mockstore

import (
	"testing"

	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
)

func TestConfig(t *testing.T) {
	tidbcfg.UpdateGlobal(func(conf *tidbcfg.Config) {
		conf.TxnLocalLatches = config.TxnLocalLatches{
			Enabled:  true,
			Capacity: 10240,
		}
	})

	type LatchEnableChecker interface {
		IsLatchEnabled() bool
	}

	var driver MockTiKVDriver
	store, err := driver.Open("mocktikv://")
	require.NoError(t, err)
	require.True(t, store.(LatchEnableChecker).IsLatchEnabled())
	store.Close()

	tidbcfg.UpdateGlobal(func(conf *tidbcfg.Config) {
		conf.TxnLocalLatches = config.TxnLocalLatches{
			Enabled:  false,
			Capacity: 10240,
		}
	})
	store, err = driver.Open("mocktikv://")
	require.NoError(t, err)
	require.False(t, store.(LatchEnableChecker).IsLatchEnabled())
	store.Close()

	store, err = driver.Open(":")
	require.Error(t, err)
	if store != nil {
		store.Close()
	}

	store, err = driver.Open("faketikv://")
	require.Error(t, err)
	if store != nil {
		store.Close()
	}
}
