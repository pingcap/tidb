// Copyright 2025 PingCAP, Inc.
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

package teststore

import (
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/mockstore"
)

// NewMockStoreWithoutBootstrap creates a mock store without bootstrap.
// This is a wrapper function for mockstore.NewMockStore to avoid potential circular dependency.
// If you need to bootstrap the store, use `testkit.CreateMockStore` or `testkit.CreateMockStoreAndDomain` instead.
func NewMockStoreWithoutBootstrap(opts ...mockstore.MockTiKVStoreOption) (kv.Storage, error) {
	store, err := mockstore.NewMockStore(opts...)
	if err != nil {
		return nil, err
	}

	if kerneltype.IsNextGen() {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = keyspace.System
			conf.Instance.TiDBServiceScope = handle.NextGenTargetScope
		})
		kvstore.SetSystemStorage(store)
	}
	return store, nil
}
