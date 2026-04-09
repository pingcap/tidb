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

package autoid

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/keyspace"
	metaautoid "github.com/pingcap/tidb/pkg/meta/autoid"
	tidbetcd "github.com/pingcap/tidb/pkg/util/etcd"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

type codecOnlyStore struct {
	*utilmock.Store
	codec tikv.Codec
}

func (s *codecOnlyStore) GetCodec() tikv.Codec {
	return s.codec
}

func TestKeyspaceLeaderPathIsDiscoverable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	keyspaceMeta := &keyspacepb.KeyspaceMeta{Id: 4242, Name: "autoid-test"}
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, keyspaceMeta)
	require.NoError(t, err)

	store := &codecOnlyStore{
		Store: &utilmock.Store{},
		codec: codec,
	}

	etcdEndpoints := cluster.Client(0).Endpoints()
	service := New("127.0.0.1:4001", etcdEndpoints, store, nil)
	defer service.Close()

	require.Eventually(t, service.leaderShip.IsOwner, 5*time.Second, 100*time.Millisecond)

	discoverEtcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer discoverEtcdCli.Close()
	tidbetcd.SetEtcdCliByNamespace(discoverEtcdCli, keyspace.MakeKeyspaceEtcdNamespace(codec))

	discover := metaautoid.NewClientDiscover(discoverEtcdCli)
	cli, _, err := discover.GetClient(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cli)

	rawEtcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer rawEtcdCli.Close()

	expectedLeaderPrefix := keyspace.MakeKeyspaceEtcdNamespace(codec) + autoIDLeaderPath
	getOpts := append([]clientv3.OpOption{clientv3.WithPrefix()}, clientv3.WithFirstCreate()...)
	resp, err := rawEtcdCli.Get(context.Background(), expectedLeaderPrefix, getOpts...)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.True(t, strings.HasPrefix(string(resp.Kvs[0].Key), expectedLeaderPrefix+"/"))
	require.Equal(t, "127.0.0.1:4001", string(resp.Kvs[0].Value))
}
