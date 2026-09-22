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

package crossks

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	etcdutil "github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/tests/v3/integration"
)

type reportedSession struct {
	sessionctx.Context
	info *sessmgr.ProcessInfo
}

func (s *reportedSession) ShowProcess() *sessmgr.ProcessInfo { return s.info }

type reportTSStore struct {
	kv.Storage
	codec   tikv.Codec
	version kv.Version
}

func (s *reportTSStore) GetCodec() tikv.Codec                      { return s.codec }
func (s *reportTSStore) CurrentVersion(string) (kv.Version, error) { return s.version, nil }

func TestCrossKSMinStartTSReporter(t *testing.T) {
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { cluster.Terminate(t) })
	previousLimit := vardef.GCMaxWaitTime.Load()
	vardef.GCMaxWaitTime.Store(600)
	t.Cleanup(func() { vardef.GCMaxWaitTime.Store(previousLimit) })
	ctx := context.Background()
	now := time.Now()
	currentTS := oracle.GoTimeToTS(now)
	oldTS := oracle.GoTimeToTS(now.Add(-time.Minute))
	newTS := oracle.GoTimeToTS(now.Add(-30 * time.Second))

	for _, keyspaceGC := range []bool{false, true} {
		t.Run(strconv.FormatBool(keyspaceGC), func(t *testing.T) {
			cli, err := clientv3.New(clientv3.Config{Endpoints: cluster.Client(0).Endpoints()})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cli.Close()) })
			meta := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42}, Name: "query-user"}
			if keyspaceGC {
				meta.Config = map[string]string{pd.KeyspaceConfigGCManagementType: pd.KeyspaceConfigGCManagementTypeKeyspaceLevel}
			}
			codec, err := tikv.NewCodecV2(tikv.ModeTxn, meta)
			require.NoError(t, err)
			prefix := keyspace.MakeKeyspaceEtcdNamespace(codec)
			etcdutil.SetEtcdCliByNamespace(cli, prefix)
			lease, err := concurrency.NewSession(cli)
			require.NoError(t, err)
			t.Cleanup(lease.Orphan)
			coordinator := newSchemaCoordinator()
			r := &minStartTSReporter{coordinator: coordinator, etcdCli: cli, serverID: "query-worker"}
			store := &reportTSStore{codec: codec, version: kv.NewVersion(currentTS)}
			rootPath := infosync.ServerMinStartTSPath + "/query-worker"
			path, otherPath := rootPath, prefix+rootPath
			if keyspaceGC {
				path, otherPath = otherPath, path
			}
			check := func(expected uint64) {
				r.ReportMinStartTS(store, lease)
				resp, err := cluster.Client(0).Get(ctx, path)
				require.NoError(t, err)
				require.Len(t, resp.Kvs, 1)
				require.Equal(t, strconv.FormatUint(expected, 10), string(resp.Kvs[0].Value))
				require.EqualValues(t, lease.Lease(), resp.Kvs[0].Lease)
				other, err := cluster.Client(0).Get(ctx, otherPath)
				require.NoError(t, err)
				require.Empty(t, other.Kvs)
			}
			old := &reportedSession{info: &sessmgr.ProcessInfo{CurTxnStartTS: oldTS}}
			same := &reportedSession{info: &sessmgr.ProcessInfo{CurTxnStartTS: oldTS}}
			newer := &reportedSession{info: &sessmgr.ProcessInfo{CurTxnStartTS: newTS}}
			idle := &reportedSession{}
			expired := &reportedSession{info: &sessmgr.ProcessInfo{CurTxnStartTS: oracle.GoTimeToTS(now.Add(-601 * time.Second))}}
			for _, se := range []*reportedSession{old, same, newer, idle, expired} {
				coordinator.StoreInternalSession(se)
			}
			check(oldTS)
			coordinator.DeleteInternalSession(old)
			check(oldTS)
			coordinator.DeleteInternalSession(same)
			check(newTS)
			coordinator.DeleteInternalSession(newer)
			check(currentTS)
			// Runtime teardown revokes this lease; the GC key must disappear with it.
			_, err = cli.Revoke(ctx, lease.Lease())
			require.NoError(t, err)
			resp, err := cluster.Client(0).Get(ctx, path)
			require.NoError(t, err)
			require.Empty(t, resp.Kvs)
			// A restarted server-info session must attach subsequent reports to its new lease.
			lease, err = concurrency.NewSession(cli)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, lease.Close()) })
			coordinator.StoreInternalSession(old)
			check(oldTS)
		})
	}
}
