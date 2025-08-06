// Copyright 2023 PingCAP, Inc.
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

package testenv

import (
	"runtime"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/keyspace"
)

// SetGOMAXPROCSForTest sets GOMAXPROCS to 16 if it is greater than 16.
func SetGOMAXPROCSForTest() {
	runtime.GOMAXPROCS(min(16, runtime.GOMAXPROCS(0)))
}

// UpdateConfigForNextgen updates the global config to use SYSTEM keyspace.
func UpdateConfigForNextgen(t testing.TB) {
	t.Helper()
	// in nextgen, SYSTEM ks must be bootstrapped first, to make UT easier, we
	// always run them inside SYSTEM keyspace, if your test requires bootstrapping
	// multiple keyspace, you should use CreateMockStoreAndDomainForKS instead.
	bak := *config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(&bak)
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = keyspace.System
		conf.Instance.TiDBServiceScope = handle.NextGenTargetScope
	})
}
