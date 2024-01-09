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

package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

// InitTableTest inits needed components for table_test.
// it disables disttask and mock cpu count to 8.
func InitTableTest(t *testing.T) (kv.Storage, *storage.TaskManager, context.Context) {
	store, pool := getResourcePool(t)
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu"))
	})
	return store, getTaskManager(t, pool), ctx
}

// InitTableTestWithCancel inits needed components with context.CancelFunc for table_test.
func InitTableTestWithCancel(t *testing.T) (*storage.TaskManager, context.Context, context.CancelFunc) {
	_, pool := getResourcePool(t)
	ctx, cancel := context.WithCancel(context.Background())
	ctx = util.WithInternalSourceType(ctx, "table_test")
	return getTaskManager(t, pool), ctx, cancel
}

func getResourcePool(t *testing.T) (kv.Storage, *pools.ResourcePool) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask"))
	}()
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)

	t.Cleanup(func() {
		pool.Close()
	})
	return store, pool
}

func getTaskManager(t *testing.T, pool *pools.ResourcePool) *storage.TaskManager {
	manager := storage.NewTaskManager(pool)
	storage.SetTaskManager(manager)
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)
	return manager
}
