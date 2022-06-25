// Copyright 2022 PingCAP, Inc.
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

package node

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/pingcap/tidb/util/etcd"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
)

var nodePrefix = path.Join(DefaultRootPath, NodePrefix[PumpNode])

type RegisrerTestClient interface {
	Node(context.Context, string, string) (*Status, int64, error)
}

func TestUpdateNodeInfo(t *testing.T) {
	integration.BeforeTest(t)
	testEtcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), DefaultRootPath)
	r := NewEtcdRegistry(etcdclient, time.Duration(30)*time.Second)
	ns := &Status{
		NodeID:  "test",
		Addr:    "test",
		State:   Online,
		IsAlive: true,
	}

	err := r.UpdateNode(context.Background(), nodePrefix, ns)
	require.NoError(t, err)
	mustEqualStatus(t, r, ns.NodeID, ns)

	ns.Addr = "localhost:1234"
	err = r.UpdateNode(context.Background(), nodePrefix, ns)
	require.NoError(t, err)
	mustEqualStatus(t, r, ns.NodeID, ns)
	// use Nodes() to query node status
	ss, _, err := r.Nodes(context.Background(), nodePrefix)
	require.NoError(t, err)
	require.Len(t, ss, 1)
}

func TestRegisterNode(t *testing.T) {
	integration.BeforeTest(t)
	testEtcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), DefaultRootPath)
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	ns := &Status{
		NodeID:  "test",
		Addr:    "test",
		State:   Online,
		IsAlive: true,
	}
	err := r.UpdateNode(context.Background(), nodePrefix, ns)
	require.NoError(t, err)
	mustEqualStatus(t, r, ns.NodeID, ns)

	ns.State = Offline
	err = r.UpdateNode(context.Background(), nodePrefix, ns)
	require.NoError(t, err)
	mustEqualStatus(t, r, ns.NodeID, ns)

	// TODO: now don't have function to delete node, maybe do it later
	//err = r.UnregisterNode(context.Background(), nodePrefix, ns.NodeID)
	//require.NoError(t, err)
	//exist, err := r.checkNodeExists(context.Background(), nodePrefix, ns.NodeID)
	//require.NoError(t, err)
	//require.False(t, exist)
}

func TestRefreshNode(t *testing.T) {
	integration.BeforeTest(t)
	testEtcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), DefaultRootPath)
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	ns := &Status{
		NodeID:  "test",
		Addr:    "test",
		State:   Online,
		IsAlive: true,
	}
	err := r.UpdateNode(context.Background(), nodePrefix, ns)
	require.NoError(t, err)

	ns.IsAlive = true
	mustEqualStatus(t, r, ns.NodeID, ns)

	// TODO: fix it later
	//time.Sleep(2 * time.Second)
	//ns.IsAlive = false
	//mustEqualStatus(t, r, ns.NodeID, ns)
}

func mustEqualStatus(t *testing.T, r RegisrerTestClient, nodeID string, status *Status) {
	ns, _, err := r.Node(context.Background(), nodePrefix, nodeID)
	require.NoError(t, err)
	require.Equal(t, status, ns)
}
