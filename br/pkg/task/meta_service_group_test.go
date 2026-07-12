// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/metaservice"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
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

func TestDialEtcdWithCfgUsesMetaServiceGroup(t *testing.T) {
	integration.BeforeTestExternal(t)
	// Use a dedicated embedded etcd cluster as the meta service group target and
	// verify the direct BR register key lands there after namespacing.
	metaCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer metaCluster.Terminate(t)

	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Id:   42,
		Name: "ks1",
		Config: map[string]string{
			"gc_management_type":      "keyspace_level",
			metaservice.GroupIDKey:    "group1",
			metaservice.GroupAddrsKey: strings.Join(metaCluster.Client(0).Endpoints(), ","),
		},
	}
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, keyspaceMeta)
	require.NoError(t, err)

	// Stub PD only for keyspace metadata and member discovery so the test can
	// exercise the real dial helper without depending on a full PD server.
	mockPD := &metaServiceGroupPDClient{
		members: []*pdpb.Member{{
			ClientUrls: []string{"http://127.0.0.1:2379"},
		}},
		keyspaceMeta: keyspaceMeta,
	}
	orig := newPDClientWithAPIContext
	newPDClientWithAPIContext = func(context.Context, pd.APIContext, caller.Component, []string, pd.SecurityOption, ...opt.ClientOption) (pd.Client, error) {
		return mockPD, nil
	}
	t.Cleanup(func() {
		newPDClientWithAPIContext = orig
	})

	etcdCli, err := dialEtcdWithCfg(context.Background(), Config{
		PD:           []string{"127.0.0.1:2379"},
		KeyspaceName: keyspaceMeta.Name,
		TLS:          TLSConfig{},
	})
	require.NoError(t, err)
	defer etcdCli.Close()

	register := utils.NewTaskRegisterWithTTL(etcdCli, time.Minute, utils.RegisterRestore, "restore-test")
	require.NoError(t, register.RegisterTaskOnce(context.Background()))

	// The visible key in raw etcd should carry both the keyspace namespace and
	// the original BR register suffix.
	prefix := keyspace.MakeKeyspaceEtcdNamespace(codec)
	resp, err := metaCluster.Client(0).Get(context.Background(), prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Contains(t, string(resp.Kvs[0].Key), "/tidb/brie/import/restore/restore-test")
}
