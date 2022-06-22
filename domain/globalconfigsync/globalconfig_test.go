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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
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
		err := store.Close()
		require.NoError(t, err)
	}()
	client := store.(kv.StorageWithPD).GetPDClient()
	syncer := globalconfigsync.NewGlobalConfigSyncer(client)
	syncer.Notify(pd.GlobalConfigItem{Name: "a", Value: "b"})
	err = syncer.StoreGlobalConfig(context.Background(), <-syncer.NotifyCh)
	require.NoError(t, err)
	items, err := client.LoadGlobalConfig(context.Background(), []string{"a"})
	require.NoError(t, err)
	require.Equal(t, len(items), 1)
	require.Equal(t, items[0].Name, "/global/config/a")
	require.Equal(t, items[0].Value, "b")
}

func TestStoreGlobalConfig(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTest(t)

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

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	_, err = se.Execute(context.Background(), "set @@global.tidb_enable_top_sql=1;")
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		client :=
			store.(kv.StorageWithPD).GetPDClient()
		// enable top sql will be translated to enable_resource_metering
		items, err := client.LoadGlobalConfig(context.Background(), []string{"enable_resource_metering"})
		require.NoError(t, err)
		if len(items) == 1 && items[0].Value == "" {
			continue
		}
		require.Len(t, items, 1)
		require.Equal(t, items[0].Name, "/global/config/enable_resource_metering")
		require.Equal(t, items[0].Value, "true")
		return
	}
	require.Fail(t, "timeout for waiting global config synced")
}
