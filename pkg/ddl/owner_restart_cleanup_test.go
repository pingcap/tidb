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

package ddl

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestCleanupStaleDDLOwnerKeys(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}

	_, err := infosync.GlobalInfoSyncerInit(context.Background(), "t", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, true, nil)
	require.NoError(t, err)

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	cli := cluster.RandClient()

	clearKeys := func() {
		ctx, cancel := context.WithTimeout(context.Background(), etcd.KeyOpDefaultTimeout)
		_, _ = cli.Delete(ctx, DDLOwnerKey, clientv3.WithPrefix())
		cancel()
	}

	mockGetAllServerInfo := func(t *testing.T, infos map[string]*serverinfo.ServerInfo) {
		b, err := json.Marshal(infos)
		require.NoError(t, err)
		inTerms := fmt.Sprintf("return(`%s`)", string(b))
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/serverinfo/mockGetAllServerInfo", inTerms)
	}

	t.Run("delete keys for stale instance after restart", func(t *testing.T) {
		clearKeys()
		ctx, cancel := context.WithTimeout(context.Background(), etcd.KeyOpDefaultTimeout)
		_, err := cli.Put(ctx, DDLOwnerKey+"/stale", string(owner.JoinOwnerValues([]byte("old"), []byte{byte(owner.OpSyncUpgradingState)})))
		require.NoError(t, err)
		_, err = cli.Put(ctx, DDLOwnerKey+"/other", "other")
		require.NoError(t, err)
		cancel()

		mockGetAllServerInfo(t, map[string]*serverinfo.ServerInfo{
			"old": {
				StaticInfo: serverinfo.StaticInfo{
					ID:             "old",
					IP:             "127.0.0.1",
					Port:           4000,
					StartTimestamp: 1,
				},
			},
			"self": {
				StaticInfo: serverinfo.StaticInfo{
					ID:             "self",
					IP:             "127.0.0.1",
					Port:           4000,
					StartTimestamp: 2,
				},
			},
			"other": {
				StaticInfo: serverinfo.StaticInfo{
					ID:             "other",
					IP:             "127.0.0.2",
					Port:           4000,
					StartTimestamp: 2,
				},
			},
		})

		cleanupStaleDDLOwnerKeys(context.Background(), cli, "self")

		ctx, cancel = context.WithTimeout(context.Background(), etcd.KeyOpDefaultTimeout)
		defer cancel()
		resp, err := cli.Get(ctx, DDLOwnerKey+"/stale")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)

		resp, err = cli.Get(ctx, DDLOwnerKey+"/other")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
	})

	t.Run("single node deletes unknown key even if stale server info is removed", func(t *testing.T) {
		clearKeys()
		ctx, cancel := context.WithTimeout(context.Background(), etcd.KeyOpDefaultTimeout)
		_, err := cli.Put(ctx, DDLOwnerKey+"/unknown", "old")
		require.NoError(t, err)
		_, err = cli.Put(ctx, DDLOwnerKey+"/self", string(owner.JoinOwnerValues([]byte("self"), []byte{byte(owner.OpSyncUpgradingState)})))
		require.NoError(t, err)
		cancel()

		mockGetAllServerInfo(t, map[string]*serverinfo.ServerInfo{
			"self": {
				StaticInfo: serverinfo.StaticInfo{
					ID:             "self",
					IP:             "127.0.0.1",
					Port:           4000,
					StartTimestamp: 2,
				},
			},
		})

		cleanupStaleDDLOwnerKeys(context.Background(), cli, "self")

		ctx, cancel = context.WithTimeout(context.Background(), etcd.KeyOpDefaultTimeout)
		defer cancel()
		resp, err := cli.Get(ctx, DDLOwnerKey+"/unknown")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)

		resp, err = cli.Get(ctx, DDLOwnerKey+"/self")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
	})
}
