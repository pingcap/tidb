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

package store

import (
	"context"
	"crypto/tls"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

type mockEtcdBackend struct {
	kv.Storage
	kv.EtcdBackend
	pdAddrs   []string
	metaAddrs []string
	codec     tikv.Codec
}

func (mebd *mockEtcdBackend) EtcdAddrs() ([]string, error) {
	return mebd.metaAddrs, nil
}

func (mebd *mockEtcdBackend) GetPDAddrs() ([]string, error) {
	return mebd.pdAddrs, nil
}

func (*mockEtcdBackend) TLSConfig() *tls.Config { return nil }

func (*mockEtcdBackend) StartGCWorker() error { return nil }

func (mebd *mockEtcdBackend) GetCodec() tikv.Codec { return mebd.codec }

func TestNewEtcdCliGetEtcdAddrs(t *testing.T) {
	etcdStore, addrs, err := GetEtcdAddrs(nil)
	require.NoError(t, err)
	require.Empty(t, addrs)
	require.Nil(t, etcdStore)

	etcdStore, addrs, err = GetEtcdAddrs(&mockEtcdBackend{
		pdAddrs:   []string{"localhost:2379"},
		metaAddrs: []string{"localhost:2389"},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"localhost:2389"}, addrs)
	require.NotNil(t, etcdStore)

	cli, err := NewEtcdCli(nil)
	require.NoError(t, err)
	require.Nil(t, cli)
}

func TestNewEtcdCliUsesMetaServiceGroupAndNamespace(t *testing.T) {
	integration.BeforeTestExternal(t)
	metaCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer metaCluster.Terminate(t)

	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Id:   42,
		Name: "ks1",
		Config: map[string]string{
			"gc_management_type":       "keyspace_level",
			"meta_service_group_id":    "group1",
			"meta_service_group_addrs": strings.Join(metaCluster.Client(0).Endpoints(), ","),
		},
	}
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, keyspaceMeta)
	require.NoError(t, err)

	backend := &mockEtcdBackend{
		pdAddrs:   []string{"localhost:2379"},
		metaAddrs: metaCluster.Client(0).Endpoints(),
		codec:     codec,
	}
	cli, err := NewEtcdCli(backend)
	require.NoError(t, err)
	defer cli.Close()

	ctx := context.Background()
	_, err = cli.Put(ctx, "direct-key", "meta-group")
	require.NoError(t, err)

	namespacePrefix := keyspace.MakeKeyspaceEtcdNamespace(codec)
	metaResp, err := metaCluster.Client(0).Get(ctx, namespacePrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, metaResp.Kvs, 1)
}
