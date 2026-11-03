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

package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func testSetupOriginal(t *testing.T) (context.Context, *clientv3.Client, *integration.ClusterV3) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	return context.Background(), cluster.RandClient(), cluster
}

// TestEtcdOpenTelemetryCompatibility ensures embedded etcd still builds and
// serves RPCs after upgrading OpenTelemetry dependencies.
func TestEtcdOpenTelemetryCompatibility(t *testing.T) {
	integration.BeforeTest(t)
	ctx, cli, cluster := testSetupOriginal(t)
	t.Cleanup(func() { cluster.Terminate(t) })

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	putResp, err := cli.Put(ctx, "otel-compatibility", "value")
	require.NoError(t, err)

	// Exercise both unary and streaming RPCs.
	watch := cli.Watch(ctx, "otel-compatibility", clientv3.WithRev(putResp.Header.Revision))
	select {
	case resp := <-watch:
		require.NoError(t, resp.Err())
		require.Len(t, resp.Events, 1)
		require.Equal(t, "value", string(resp.Events[0].Kv.Value))
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestSetEtcdCliByNamespace(t *testing.T) {
	integration.BeforeTest(t)
	ctx, origEtcdCli, etcdMockCluster := testSetupOriginal(t)
	defer etcdMockCluster.Terminate(t)

	namespacePrefix := "testNamespace/"
	key := "testkey"
	obj := "test"

	unprefixedKV := origEtcdCli.KV
	cliNamespace := origEtcdCli
	SetEtcdCliByNamespace(cliNamespace, namespacePrefix)

	_, err := cliNamespace.Put(ctx, key, obj)
	require.NoError(t, err)

	// verify that kv pair is empty before set
	getResp, err := unprefixedKV.Get(ctx, namespacePrefix+key)
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1)
}
