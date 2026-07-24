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

package importer

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/keyspace"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/metaservice"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

type metaServiceGroupPDClient struct {
	pd.Client
	members      []*pdpb.Member
	keyspaceMeta *keyspacepb.KeyspaceMeta
}

func (c *metaServiceGroupPDClient) GetAllMembers(context.Context) (*pdpb.GetMembersResponse, error) {
	return &pdpb.GetMembersResponse{Members: c.members}, nil
}

func (c *metaServiceGroupPDClient) LoadKeyspace(context.Context, string) (*keyspacepb.KeyspaceMeta, error) {
	return c.keyspaceMeta, nil
}

func (*metaServiceGroupPDClient) Close() {}

type metaServiceGroupStore struct {
	*utilmock.Store
	codec tikv.Codec
	pdCli pd.Client
}

func (s *metaServiceGroupStore) GetCodec() tikv.Codec         { return s.codec }
func (s *metaServiceGroupStore) GetPDClient() pd.Client       { return s.pdCli }
func (*metaServiceGroupStore) GetPDHTTPClient() pdhttp.Client { return nil }

var _ tidbkv.StorageWithPD = (*metaServiceGroupStore)(nil)

func TestNewEtcdClientForAllocatorRebaseUsesMetaServiceGroup(t *testing.T) {
	integration.BeforeTestExternal(t)
	metaCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer metaCluster.Terminate(t)

	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Id:   45,
		Name: "ks4",
		Config: map[string]string{
			"gc_management_type":      "keyspace_level",
			metaservice.GroupIDKey:    "group4",
			metaservice.GroupAddrsKey: strings.Join(metaCluster.Client(0).Endpoints(), ","),
		},
	}
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, keyspaceMeta)
	require.NoError(t, err)

	pdCli := &metaServiceGroupPDClient{
		members: []*pdpb.Member{{
			ClientUrls: []string{"http://127.0.0.1:2379"},
		}},
		keyspaceMeta: keyspaceMeta,
	}
	store := &metaServiceGroupStore{
		Store: &utilmock.Store{},
		codec: codec,
		pdCli: pdCli,
	}
	tls, err := common.NewTLS("", "", "", "127.0.0.1:10080", nil, nil, nil)
	require.NoError(t, err)

	etcdCli, err := newEtcdClientForAllocatorRebase(context.Background(), store, tls, []string{"pd-proxy:2379"})
	require.NoError(t, err)
	defer etcdCli.Close()

	_, err = etcdCli.Put(context.Background(), "rebase-key", "1")
	require.NoError(t, err)

	prefix := keyspace.MakeKeyspaceEtcdNamespace(codec)
	resp, err := metaCluster.Client(0).Get(context.Background(), prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Contains(t, string(resp.Kvs[0].Key), "rebase-key")

	globalKeyspaceMeta := &keyspacepb.KeyspaceMeta{
		Id:     46,
		Name:   "ks-global",
		Config: map[string]string{"gc_management_type": "keyspace_level"},
	}
	globalCodec, err := tikv.NewCodecV2(tikv.ModeTxn, globalKeyspaceMeta)
	require.NoError(t, err)
	globalStore := &metaServiceGroupStore{
		Store: &utilmock.Store{},
		codec: globalCodec,
		pdCli: &metaServiceGroupPDClient{
			members: []*pdpb.Member{{
				ClientUrls: []string{"http://internal-pd:2379"},
			}},
			keyspaceMeta: globalKeyspaceMeta,
		},
	}
	globalEtcdCli, err := newEtcdClientForAllocatorRebase(
		context.Background(), globalStore, tls, []string{"pd-proxy:2379"},
	)
	require.NoError(t, err)
	defer globalEtcdCli.Close()
	require.Equal(t, []string{"pd-proxy:2379"}, globalEtcdCli.Endpoints())
}
