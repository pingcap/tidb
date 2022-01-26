// Copyright 2021 PingCAP, Inc.
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

package globalconfigsync_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/tidb/domain/globalconfigsync"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testbridge"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/integration"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testbridge.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestGlobalConfigSyncer(t *testing.T) {
	syncer := globalconfigsync.NewGlobalConfigSyncer(nil)
	syncer.Notify("a", "b")
	require.Equal(t, len(syncer.NotifyCh), 1)
	entry := <-syncer.NotifyCh
	require.Equal(t, entry.Name, "a")
	err := syncer.StoreGlobalConfig(context.Background(), entry)
	require.NoError(t, err)
}

func TestStoreGlobalConfig(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	session.SetSchemaLease(50 * time.Millisecond)
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domain.Close()

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	domain.GetGlobalConfigSyncer().SetEtcdClient(cluster.RandClient())

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	_, err = se.Execute(context.Background(), "set @@global.tidb_enable_top_sql=1;")
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		resp, err := cluster.RandClient().Get(context.Background(), "/global/config/enable_resource_metering")
		require.NoError(t, err)
		require.NotNil(t, resp, nil)

		if len(resp.Kvs) == 0 {
			// writing to ectd is async, so we should retry if not synced yet
			time.Sleep(100 * time.Millisecond)
			continue
		}

		require.Equal(t, len(resp.Kvs), 1)
		require.Equal(t, resp.Kvs[0].Key, []byte("/global/config/enable_resource_metering"))
		require.Equal(t, resp.Kvs[0].Value, []byte("true"))
		return
	}

	require.Fail(t, "timeout for waiting etcd synced")
}
