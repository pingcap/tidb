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

package addindextest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/phayes/freeport"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func init() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Path = "127.0.0.1:2379"
	})
}

func genStorageURI(t *testing.T) (host string, port uint16, uri string) {
	gcsHost := "127.0.0.1"
	// for fake gcs server, we must use this endpoint format
	// NOTE: must end with '/'
	gcsEndpointFormat := "http://%s:%d/storage/v1/"
	freePort, err := freeport.GetFreePort()
	require.NoError(t, err)
	gcsEndpoint := fmt.Sprintf(gcsEndpointFormat, gcsHost, freePort)
	return gcsHost, uint16(freePort),
		fmt.Sprintf("gs://sorted/addindex?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)
}

func checkFileCleaned(t *testing.T, jobID int64, sortStorageURI string) {
	storeBackend, err := storage.ParseBackend(sortStorageURI, nil)
	require.NoError(t, err)
	extStore, err := storage.NewWithDefaultOpt(context.Background(), storeBackend)
	require.NoError(t, err)
	prefix := strconv.Itoa(int(jobID))
	dataFiles, statFiles, err := external.GetAllFileNames(context.Background(), extStore, prefix)
	require.NoError(t, err)
	require.Greater(t, jobID, int64(0))
	require.Equal(t, 0, len(dataFiles))
	require.Equal(t, 0, len(statFiles))
}

func TestGlobalSortBasic(t *testing.T) {
	gcsHost, gcsPort, cloudStorageURI := genStorageURI(t)
	opt := fakestorage.Options{
		Scheme:     "http",
		Host:       gcsHost,
		Port:       gcsPort,
		PublicHost: gcsHost,
	}
	server, err := fakestorage.NewServerWithOptions(opt)
	require.NoError(t, err)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished", "return()"))
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set @@global.tidb_ddl_enable_fast_reorg = 1;`)
	tk.MustExec("set @@global.tidb_enable_dist_task = 1;")
	tk.MustExec(fmt.Sprintf(`set @@global.tidb_cloud_storage_uri = "%s"`, cloudStorageURI))
	defer func() {
		tk.MustExec("set @@global.tidb_enable_dist_task = 0;")
		variable.CloudStorageURI.Store("")
	}()

	tk.MustExec("create table t (a int, b int, c int);")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	size := 100
	for i := 0; i < size; i++ {
		sb.WriteString(fmt.Sprintf("(%d, %d, %d)", i, i, i))
		if i != size-1 {
			sb.WriteString(",")
		}
	}
	sb.WriteString(";")
	tk.MustExec(sb.String())

	var jobID int64
	origin := dom.DDL().GetHook()
	onJobUpdated := func(job *model.Job) {
		jobID = job.ID
	}
	hook := &callback.TestDDLCallback{}
	hook.OnJobUpdatedExported.Store(&onJobUpdated)
	dom.DDL().SetHook(hook)

	tk.MustExec("alter table t add index idx(a);")
	dom.DDL().SetHook(origin)
	tk.MustExec("admin check table t;")
	<-scheduler.WaitCleanUpFinished
	checkFileCleaned(t, jobID, cloudStorageURI)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/forceMergeSort", "return()"))
	tk.MustExec("alter table t add index idx1(a);")
	dom.DDL().SetHook(origin)
	tk.MustExec("admin check table t;")
	<-scheduler.WaitCleanUpFinished

	checkFileCleaned(t, jobID, cloudStorageURI)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/forceMergeSort"))
}

func TestGlobalSortMultiSchemaChange(t *testing.T) {
	gcsHost, gcsPort, cloudStorageURI := genStorageURI(t)
	opt := fakestorage.Options{
		Scheme:     "http",
		Host:       gcsHost,
		Port:       gcsPort,
		PublicHost: gcsHost,
	}
	server, err := fakestorage.NewServerWithOptions(opt)
	require.NoError(t, err)
	t.Cleanup(server.Stop)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")

	tk.MustExec("create table t_rowid (a int, b bigint, c varchar(255));")
	tk.MustExec("create table t_int_handle (a bigint primary key, b varchar(255));")
	tk.MustExec("create table t_common_handle (a int, b bigint, c varchar(255), primary key (a, c) clustered);")
	tk.MustExec(`create table t_partition (a bigint primary key, b int, c char(10)) partition by hash(a) partitions 2;`)
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t_rowid values (%d, %d, '%d');", i, i, i))
		tk.MustExec(fmt.Sprintf("insert into t_int_handle values (%d, '%d');", i, i))
		tk.MustExec(fmt.Sprintf("insert into t_common_handle values (%d, %d, '%d');", i, i, i))
		tk.MustExec(fmt.Sprintf("insert into t_partition values (%d, %d, '%d');", i, i, i))
	}
	tableNames := []string{"t_rowid", "t_int_handle", "t_common_handle", "t_partition"}

	testCases := []struct {
		name            string
		enableFastReorg string
		enableDistTask  string
		cloudStorageURI string
	}{
		{"txn_backfill", "0", "0", ""},
		{"ingest_backfill", "1", "0", ""},
		{"ingest_dist_backfill", "1", "1", ""},
		{"ingest_dist_gs_backfill", "1", "1", cloudStorageURI},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = " + tc.enableFastReorg + ";")
			tk.MustExec("set @@global.tidb_enable_dist_task = " + tc.enableDistTask + ";")
			tk.MustExec("set @@global.tidb_cloud_storage_uri = '" + tc.cloudStorageURI + "';")
			for _, tn := range tableNames {
				tk.MustExec("alter table " + tn + " add index idx_1(a), add index idx_2(b, a);")
				tk.MustExec("admin check table " + tn + ";")
				tk.MustExec("alter table " + tn + " drop index idx_1, drop index idx_2;")
			}
		})
	}

	tk.MustExec("set @@global.tidb_enable_dist_task = 0;")
	tk.MustExec("set @@global.tidb_cloud_storage_uri = '';")
}

func TestAddIndexIngestShowReorgTp(t *testing.T) {
	gcsHost, gcsPort, cloudStorageURI := genStorageURI(t)
	opt := fakestorage.Options{
		Scheme:     "http",
		Host:       gcsHost,
		Port:       gcsPort,
		PublicHost: gcsHost,
	}
	server, err := fakestorage.NewServerWithOptions(opt)
	require.NoError(t, err)
	t.Cleanup(server.Stop)

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec("set @@global.tidb_cloud_storage_uri = '" + cloudStorageURI + "';")
	tk.MustExec("set @@global.tidb_enable_dist_task = 0;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = 1;")

	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1), (2), (3);")
	tk.MustExec("alter table t add index idx(a);")

	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobType, rowCnt := rows[0][3].(string), rows[0][7].(string)
	require.True(t, strings.Contains(jobType, "ingest"))
	require.False(t, strings.Contains(jobType, "cloud"))
	require.Equal(t, rowCnt, "3")
}
