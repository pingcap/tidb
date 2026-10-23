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
	"crypto/tls"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestOwnerManager(t *testing.T) {
	bak := config.GetGlobalConfig().Store
	t.Cleanup(func() {
		config.GetGlobalConfig().Store = bak
		globalOwnerManager = &ownerManager{}
	})
	config.GetGlobalConfig().Store = "unistore"
	globalOwnerManager = &ownerManager{}
	ctx := context.Background()
	store := &mockEtcdBackend{}
	require.NoError(t, StartOwnerManager(ctx, store))
	require.Nil(t, globalOwnerManager.etcdCli)
	require.Nil(t, globalOwnerManager.ownerMgr)
	require.Empty(t, globalOwnerManager.id)
	CloseOwnerManager()

<<<<<<< HEAD
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	cli := cluster.RandClient()
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/injectEtcdClient", func(cliP **clientv3.Client) {
		*cliP = cli
	})
	config.GetGlobalConfig().Store = "tikv"
	require.NoError(t, StartOwnerManager(ctx, nil))
	require.Same(t, cli, globalOwnerManager.etcdCli)
=======
	config.GetGlobalConfig().Store = config.StoreTypeTiKV
	require.NoError(t, StartOwnerManager(ctx, store))
	require.NotNil(t, globalOwnerManager.etcdCli)
>>>>>>> 22a8df5a753 (ddl: Fix ddl owner doesn't use etcd keyspace prefix (#60403))
	require.NotEmpty(t, globalOwnerManager.id)
	require.NotNil(t, globalOwnerManager.ownerMgr)
	CloseOwnerManager()
}

type mockEtcdBackend struct {
	kv.Storage
	kv.EtcdBackend
}

func (mebd *mockEtcdBackend) EtcdAddrs() ([]string, error) {
	return []string{"localhost:2379"}, nil
}

func (mebd *mockEtcdBackend) TLSConfig() *tls.Config {
	return nil
}

func (mebd *mockEtcdBackend) GetCodec() tikv.Codec {
	c, _ := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{})
	return c
}
