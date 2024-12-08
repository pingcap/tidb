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

package workloadrepo

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func setupDomainAndContext(t *testing.T) (context.Context, kv.Storage, *domain.Domain, string) {
	ctx := context.Background()
	var cancel context.CancelFunc
	if ddl, ok := t.Deadline(); ok {
		ctx, cancel = context.WithDeadline(ctx, ddl)
	}
	t.Cleanup(func() {
		if cancel != nil {
			cancel()
		}
	})

	store, dom := testkit.CreateMockStoreAndDomain(t)

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()

	lcurl, err := url.Parse("http://127.0.0.1:0")
	require.NoError(t, err)
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{*lcurl}, []url.URL{*lcurl}
	lpurl, err := url.Parse("http://127.0.0.1:0")
	require.NoError(t, err)
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{*lpurl}, []url.URL{*lpurl}

	cfg.InitialCluster = "default=" + lpurl.String()
	cfg.Logger = "zap"
	embedEtcd, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(embedEtcd.Close)

	select {
	case <-embedEtcd.Server.ReadyNotify():
	case <-time.After(5 * time.Second):
		embedEtcd.Server.Stop() // trigger a shutdown
		require.False(t, true, "server took too long to start")
	}

	return ctx, store, dom, embedEtcd.Clients[0].Addr().String()
}

func setupWorker(ctx context.Context, t *testing.T, addr string, dom *domain.Domain, id string, testWorker bool) *worker {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{addr},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = etcdCli.Close()
	})

	wrk := &worker{}
	if !testWorker {
		wrk = &workerCtx
	}
	owner.ManagerSessionTTL = 3
	initializeWorker(wrk,
		etcdCli, func(s1, s2 string) owner.Manager {
			return owner.NewOwnerManager(ctx, etcdCli, s1, id, s2)
		},
		dom.SysSessionPool(),
	)
	wrk.samplingInterval = 1
	wrk.snapshotInterval = 1
	wrk.instanceID = id
	t.Cleanup(func() {
		wrk.stop()
	})

	return wrk
}

func TestMultipleWorker(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)

	_, ok := dom.InfoSchema().SchemaByName(model.NewCIStr("workload_schema"))
	require.False(t, ok)

	wrk1 := setupWorker(ctx, t, addr, dom, "worker1", true)
	wrk2 := setupWorker(ctx, t, addr, dom, "worker2", true)
	require.NoError(t, wrk1.setRepositoryDest(ctx, "table"))
	require.NoError(t, wrk2.setRepositoryDest(ctx, "table"))

	require.Eventually(t, func() bool {
		return wrk1.checkTablesExists(ctx) && wrk2.checkTablesExists(ctx)
	}, time.Minute, time.Second)

	tk := testkit.NewTestKit(t, store)

	// sampling succeeded
	require.Eventually(t, func() bool {
		res := tk.MustQuery("select instance_id, count(*) from workload_schema.hist_memory_usage group by instance_id").Rows()
		return len(res) >= 2
	}, time.Minute, time.Second)
}

func TestGlobalWorker(t *testing.T) {
	ctx, store, dom, addr := setupDomainAndContext(t)
	tk := testkit.NewTestKit(t, store)

	_, ok := dom.InfoSchema().SchemaByName(model.NewCIStr("workload_schema"))
	require.False(t, ok)

	wrk := setupWorker(ctx, t, addr, dom, "worker", false)
	tk.MustExec("set @@global.tidb_workload_repository_dest='table'")

	require.Eventually(t, func() bool {
		return wrk.checkTablesExists(ctx)
	}, time.Minute, time.Second)

	// sampling succeeded
	require.Eventually(t, func() bool {
		res := tk.MustQuery("select instance_id, count(*) from workload_schema.hist_memory_usage group by instance_id").Rows()
		return len(res) >= 1
	}, time.Minute, time.Second)
}
