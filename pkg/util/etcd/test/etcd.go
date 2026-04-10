// Copyright 2018 PingCAP, Inc.
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

package etcd_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func testSetupOriginal(t *testing.T) (context.Context, *clientv3.Client, *integration.ClusterV3) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	return context.Background(), cluster.RandClient(), cluster
}

func RunSetEtcdCliByNamespace(t *testing.T) {
	integration.BeforeTest(t)
	ctx, origEtcdCli, etcdMockCluster := testSetupOriginal(t)
	defer etcdMockCluster.Terminate(t)

	namespacePrefix := "testNamespace/"
	key := "testkey"
	obj := "test"

	unprefixedKV := origEtcdCli.KV
	cliNamespace := origEtcdCli
	etcd.ExportSetEtcdCliByNamespace(cliNamespace, namespacePrefix)

	_, err := cliNamespace.Put(ctx, key, obj)
	require.NoError(t, err)

	// verify that kv pair is empty before set
	getResp, err := unprefixedKV.Get(ctx, namespacePrefix+key)
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1)
}
