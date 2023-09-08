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

package importer_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/etcd"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const addrFmt = "http://127.0.0.1:%d"

func createMockETCD(t *testing.T) (string, *embed.Etcd) {
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	// rand port in [20000, 60000)
	randPort := int(rand.Int31n(40000)) + 20000
	clientAddr := fmt.Sprintf(addrFmt, randPort)
	lcurl, _ := url.Parse(clientAddr)
	cfg.LCUrls, cfg.ACUrls = []url.URL{*lcurl}, []url.URL{*lcurl}
	lpurl, _ := url.Parse(fmt.Sprintf(addrFmt, randPort+1))
	cfg.LPUrls, cfg.APUrls = []url.URL{*lpurl}, []url.URL{*lpurl}
	cfg.InitialCluster = "default=" + lpurl.String()
	cfg.Logger = "zap"
	embedEtcd, err := embed.StartEtcd(cfg)
	require.NoError(t, err)

	select {
	case <-embedEtcd.Server.ReadyNotify():
	case <-time.After(5 * time.Second):
		embedEtcd.Server.Stop() // trigger a shutdown
		require.False(t, true, "server took too long to start")
	}
	return clientAddr, embedEtcd
}

func TestCheckRequirements(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := util.WithInternalSourceType(context.Background(), kv.InternalImportInto)
	conn := tk.Session().(sqlexec.SQLExecutor)

	_, err := conn.Execute(ctx, "create table test.t(id int primary key)")
	require.NoError(t, err)
	is := tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema)
	tableObj, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	c := &importer.LoadDataController{
		Plan: &importer.Plan{
			DBName: "test",
		},
		Table: tableObj,
	}
	require.ErrorIs(t, c.CheckRequirements(ctx, conn), exeerrors.ErrLoadDataPreCheckFailed)

	// now checkTotalFileSize pass, and try next pre-check item
	c.TotalFileSize = 1
	// non-empty table
	_, err = conn.Execute(ctx, "insert into test.t values(1)")
	require.NoError(t, err)
	require.ErrorIs(t, c.CheckRequirements(ctx, conn), exeerrors.ErrLoadDataPreCheckFailed)
	// table not exists
	_, err = conn.Execute(ctx, "drop table if exists test.t")
	require.NoError(t, err)
	require.ErrorContains(t, c.CheckRequirements(ctx, conn), "doesn't exist")

	// create table again, now checkTableEmpty pass
	_, err = conn.Execute(ctx, "create table test.t(id int primary key)")
	require.NoError(t, err)

	clientAddr, embedEtcd := createMockETCD(t)
	require.NotNil(t, embedEtcd)
	t.Cleanup(func() {
		embedEtcd.Close()
	})
	backup := importer.GetEtcdClient
	importer.GetEtcdClient = func() (*etcd.Client, error) {
		etcdCli, err := clientv3.New(clientv3.Config{
			Endpoints: []string{clientAddr},
		})
		require.NoError(t, err)
		return etcd.NewClient(etcdCli, ""), nil
	}
	t.Cleanup(func() {
		importer.GetEtcdClient = backup
	})
	// mock a PiTR task
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{clientAddr},
	})
	t.Cleanup(func() {
		require.NoError(t, etcdCli.Close())
	})
	require.NoError(t, err)
	pitrKey := streamhelper.PrefixOfTask() + "test"
	_, err = etcdCli.Put(ctx, pitrKey, "")
	require.NoError(t, err)
	err = c.CheckRequirements(ctx, conn)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataPreCheckFailed)
	require.ErrorContains(t, err, "found PiTR log streaming")

	// remove PiTR task, and mock a CDC task
	_, err = etcdCli.Delete(ctx, pitrKey)
	require.NoError(t, err)
	// example: /tidb/cdc/<clusterID>/<namespace>/changefeed/info/<changefeedID>
	cdcKey := utils.CDCPrefix + "test_cluster/test_ns/changefeed/info/test_cf"
	_, err = etcdCli.Put(ctx, cdcKey, `{"state":"normal"}`)
	require.NoError(t, err)
	err = c.CheckRequirements(ctx, conn)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataPreCheckFailed)
	require.ErrorContains(t, err, "found CDC changefeed")

	// remove CDC task, pass
	_, err = etcdCli.Delete(ctx, cdcKey)
	require.NoError(t, err)
	require.NoError(t, c.CheckRequirements(ctx, conn))
}
