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

package serverinfo

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestDiagnosticServerInfoLifecycle(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires intest")
	}
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	client := cluster.RandClient()
	ctx := context.Background()
	normal := NewSyncer("normal", func() uint64 { return 1 }, client, nil)
	require.NoError(t, normal.NewSessionAndStoreServerInfo(ctx))
	require.NoError(t, normal.NewTopologySessionAndStoreServerInfo(ctx))
	defer normal.RevokeSession()
	defer normal.RemoveTopologyInfo()
	defer normal.RemoveServerInfo()
	// The diagnostic instance has the same advertised address. Its cleanup must
	// not remove the live instance's information or endpoint claim.
	before, err := client.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	leasesBefore, err := client.Leases(ctx)
	require.NoError(t, err)
	defer diagnosticmode.SetForTest(true)()
	diagnostic := NewSyncer("diagnostic", func() uint64 { return 2 }, client, nil)
	require.NoError(t, diagnostic.NewSessionAndStoreServerInfo(ctx))
	require.Nil(t, diagnostic.session)
	require.NoError(t, diagnostic.NewTopologySessionAndStoreServerInfo(ctx))
	require.Nil(t, diagnostic.topologySession)
	require.NoError(t, diagnostic.StoreServerInfo(ctx))
	require.NoError(t, diagnostic.StoreTopologyInfo(ctx))
	require.NoError(t, diagnostic.updateTopologyAliveness(ctx))
	require.NoError(t, diagnostic.Restart(ctx))
	require.NoError(t, diagnostic.RestartTopology(ctx))
	require.Nil(t, diagnostic.Done())
	require.Nil(t, diagnostic.TopologyDone())
	exit := make(chan struct{})
	close(exit)
	diagnostic.ServerInfoSyncLoop(nil, exit)
	diagnostic.TopologySyncLoop(exit)
	require.NoError(t, diagnostic.UpdateServerLabel(ctx, map[string]string{"zone": "diagnostic"}))
	require.Equal(t, "diagnostic", diagnostic.GetLocalServerInfo().Labels["zone"])
	local, err := diagnostic.GetServerInfoByID(ctx, "diagnostic")
	require.NoError(t, err)
	require.Same(t, diagnostic.GetLocalServerInfo(), local)
	all, err := diagnostic.GetAllServerInfo(ctx)
	require.NoError(t, err)
	require.Contains(t, all, "normal")
	require.NotContains(t, all, "diagnostic")
	remote, err := diagnostic.GetServerInfoByID(ctx, "normal")
	require.NoError(t, err)
	require.Equal(t, "normal", remote.ID)
	topology, err := diagnostic.GetAllTiDBTopology(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, topology)
	diagnostic.RemoveServerInfo()
	diagnostic.RemoveTopologyInfo()
	diagnostic.RevokeSession()
	after, err := client.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, before.Kvs, after.Kvs)
	leasesAfter, err := client.Leases(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, leasesBefore.Leases, leasesAfter.Leases)
}

func TestDiagnosticLocalServerInfo(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires intest")
	}
	defer diagnosticmode.SetForTest(true)()
	s := NewSyncer("local", func() uint64 { return 123 }, nil, nil)
	require.Equal(t, uint64(123), s.GetLocalServerInfo().ServerIDGetter())
	require.NoError(t, s.UpdateServerLabel(context.Background(), map[string]string{"zone": "local"}))
	require.Equal(t, "local", s.GetLocalServerInfo().Labels["zone"])
	all, err := s.GetAllServerInfo(context.Background())
	require.NoError(t, err)
	require.Empty(t, all)
	_, err = s.GetServerInfoByID(context.Background(), "other")
	require.Error(t, err)
	local, err := s.GetServerInfoByID(context.Background(), "local")
	require.NoError(t, err)
	require.Same(t, s.GetLocalServerInfo(), local)
}
