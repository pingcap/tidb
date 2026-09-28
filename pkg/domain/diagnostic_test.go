// Copyright 2026 PingCAP, Inc.
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

package domain

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestDiagnosticServerID(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires intest")
	}
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	client := cluster.RandClient()
	ctx := context.Background()
	defer diagnosticmode.SetForTest(true)()
	original := config.GetGlobalConfig()
	defer config.StoreGlobalConfig(original)
	for _, bits32 := range []bool{false, true} {
		config.UpdateGlobal(func(c *config.Config) { c.Enable32BitsConnectionID = bits32 })
		t.Run(fmt.Sprintf("32bits=%v", bits32), func(t *testing.T) {
			first := &Domain{etcdClient: client}
			second := &Domain{etcdClient: client}
			info, err := infosync.GlobalInfoSyncerInit(ctx, "diagnostic", first.ServerID,
				client, client, nil, nil, keyspace.CodecV1, true, nil)
			require.NoError(t, err)
			require.NoError(t, first.acquireServerID(ctx))
			defer first.releaseServerID(ctx)
			require.NoError(t, second.acquireServerID(ctx))
			defer second.releaseServerID(ctx)
			require.NotZero(t, first.ServerID())
			require.NotEqual(t, first.ServerID(), second.ServerID())
			require.NotNil(t, first.serverIDSession)
			require.NoError(t, first.refreshServerIDTTL(ctx))
			require.NoError(t, info.ServerInfoSyncer().StoreServerInfo(ctx))
			resp, err := client.Get(ctx, "/tidb/server/info/", clientv3.WithPrefix())
			require.NoError(t, err)
			require.Empty(t, resp.Kvs)
			require.Equal(t, first.ServerID(), info.ServerInfoSyncer().GetLocalServerInfo().ServerIDGetter())
			firstID := first.ServerID()
			first.releaseServerID(ctx)
			resp, err = client.Get(ctx, fmt.Sprintf("%s/%d", serverIDEtcdPath, firstID))
			require.NoError(t, err)
			require.Empty(t, resp.Kvs)
			require.Zero(t, first.ServerID())
		})
	}
}
