// Copyright 2024 PingCAP, Inc.
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

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestOwnerManager(t *testing.T) {
	bak := config.GetGlobalConfig().Store
	t.Cleanup(func() {
		config.GetGlobalConfig().Store = bak
		globalOwnerManager = &ownerManager{}
	})
	config.GetGlobalConfig().Store = config.StoreTypeUniStore
	globalOwnerManager = &ownerManager{}
	ctx := context.Background()
	require.NoError(t, StartOwnerManager(ctx, nil))
	require.Nil(t, globalOwnerManager.etcdCli)
	require.Nil(t, globalOwnerManager.ownerMgr)
	require.Empty(t, globalOwnerManager.id)
	CloseOwnerManager()

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	cli := cluster.RandClient()
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/injectEtcdClient", func(cliP **clientv3.Client) {
		*cliP = cli
	})
	config.GetGlobalConfig().Store = config.StoreTypeTiKV
	require.NoError(t, StartOwnerManager(ctx, nil))
	require.Same(t, cli, globalOwnerManager.etcdCli)
	require.NotEmpty(t, globalOwnerManager.id)
	require.NotNil(t, globalOwnerManager.ownerMgr)
	CloseOwnerManager()
	cluster.TakeClient(0)
}
