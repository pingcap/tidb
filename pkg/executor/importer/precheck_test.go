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
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/cdcutil"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/etcd"
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
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{*lcurl}, []url.URL{*lcurl}
	lpurl, _ := url.Parse(fmt.Sprintf(addrFmt, randPort+1))
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{*lpurl}, []url.URL{*lpurl}
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
	conn := tk.Session().GetSQLExecutor()

	_, err := conn.Execute(ctx, "create table test.t(id int primary key)")
	require.NoError(t, err)
	is := tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema)
	tableObj, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	c := &importer.LoadDataController{
		Plan: &importer.Plan{
			DBName:         "test",
			DataSourceType: importer.DataSourceTypeFile,
			TableInfo:      tableObj.Meta(),
		},
		Table: tableObj,
	}

	// create a dummy job
	_, err = importer.CreateJob(ctx, conn, "test", "tttt", tableObj.Meta().ID, "root", &importer.ImportParameters{}, 0)
	require.NoError(t, err)
	// there is active job on the target table already
	jobID, err := importer.CreateJob(ctx, conn, "test", "t", tableObj.Meta().ID, "root", &importer.ImportParameters{}, 0)
	require.NoError(t, err)
	err = c.CheckRequirements(ctx, conn)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataPreCheckFailed)
	require.ErrorContains(t, err, "there is active job on the target table already")
	// cancel the job
	require.NoError(t, importer.CancelJob(ctx, conn, jobID))

	// source data file size = 0
	require.ErrorIs(t, c.CheckRequirements(ctx, conn), exeerrors.ErrLoadDataPreCheckFailed)

	// make checkTotalFileSize pass
	c.TotalFileSize = 1
	// global sort with thread count < 8
	c.ThreadCnt = 7
	c.CloudStorageURI = "s3://test"
	err = c.CheckRequirements(ctx, conn)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataPreCheckFailed)
	require.ErrorContains(t, err, "global sort requires at least 8 threads")

	// reset fields, make global sort thread check pass
	c.ThreadCnt = 1
	c.CloudStorageURI = ""
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
	// disable precheck, should pass
	c.DisablePrecheck = true
	require.NoError(t, c.CheckRequirements(ctx, conn))
	c.DisablePrecheck = false // revert back

	// remove PiTR task, and mock a CDC task
	_, err = etcdCli.Delete(ctx, pitrKey)
	require.NoError(t, err)
	// example: /tidb/cdc/<clusterID>/<namespace>/changefeed/info/<changefeedID>
	cdcKey := cdcutil.CDCPrefix + "testcluster/test_ns/changefeed/info/test_cf"
	_, err = etcdCli.Put(ctx, cdcKey, `{"state":"normal"}`)
	require.NoError(t, err)
	err = c.CheckRequirements(ctx, conn)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataPreCheckFailed)
	require.ErrorContains(t, err, "found CDC changefeed")

	// remove CDC task, pass
	_, err = etcdCli.Delete(ctx, cdcKey)
	require.NoError(t, err)
	require.NoError(t, c.CheckRequirements(ctx, conn))

	// with global sort
	c.Plan.ThreadCnt = 8
	c.Plan.CloudStorageURI = ":"
	require.ErrorIs(t, c.CheckRequirements(ctx, conn), exeerrors.ErrLoadDataInvalidURI)
	c.Plan.CloudStorageURI = "sdsdsdsd://sdsdsdsd"
	require.ErrorIs(t, c.CheckRequirements(ctx, conn), exeerrors.ErrLoadDataInvalidURI)
	c.Plan.CloudStorageURI = "local:///tmp"
	require.ErrorContains(t, c.CheckRequirements(ctx, conn), "unsupported cloud storage uri scheme: local")
	// this mock cannot mock credential check, so we just skip it.
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()
	require.NoError(t, backend.CreateBucket("test-bucket"))
	c.Plan.CloudStorageURI = fmt.Sprintf("s3://test-bucket/path?region=us-east-1&endpoint=%s&access-key=xxxxxx&secret-access-key=xxxxxx", ts.URL)
	require.NoError(t, c.CheckRequirements(ctx, conn))
}
