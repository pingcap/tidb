// Copyright 2023 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/domain/globalconfigsync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.opencensus.io/stats/view"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestGlobalConfigSyncer(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		view.Stop()
		err := store.Close()
		require.NoError(t, err)
	}()
	client := store.(kv.StorageWithPD).GetPDClient()
	syncer := globalconfigsync.NewGlobalConfigSyncer(client)
	syncer.Notify(pd.GlobalConfigItem{Name: "a", Value: "b"})
	err = syncer.StoreGlobalConfig(context.Background(), <-syncer.NotifyCh)
	require.NoError(t, err)
	items, revision, err := client.LoadGlobalConfig(context.Background(), []string{"a"}, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(items))
	require.Equal(t, "/global/config/a", items[0].Name)
	require.Equal(t, int64(0), revision)
	require.Equal(t, "b", items[0].Value)
}

func TestStoreGlobalConfig(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		view.Stop()
		err := store.Close()
		require.NoError(t, err)
	}()
	session.SetSchemaLease(50 * time.Millisecond)
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domain.Close()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	_, err = se.Execute(context.Background(), "set @@global.tidb_enable_top_sql=1;")
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "set @@global.tidb_source_id=2;")
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		client :=
			store.(kv.StorageWithPD).GetPDClient()
		// enable top sql will be translated to enable_resource_metering
		items, _, err := client.LoadGlobalConfig(context.Background(), []string{"enable_resource_metering", "source_id"}, "")
		require.NoError(t, err)
		if len(items) == 2 && items[0].Value == "" {
			continue
		}
		require.Len(t, items, 2)
		require.Equal(t, items[0].Name, "/global/config/enable_resource_metering")
		require.Equal(t, items[0].Value, "true")
		require.Equal(t, items[1].Name, "/global/config/source_id")
		require.Equal(t, items[1].Value, "2")
		return
	}
	require.Fail(t, "timeout for waiting global config synced")
}
