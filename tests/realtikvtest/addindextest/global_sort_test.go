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

package addindextest_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/ddl/util/callback"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

var (
	gcsHost = "127.0.0.1"
	gcsPort = uint16(4443)
	// for fake gcs server, we must use this endpoint format
	// NOTE: must end with '/'
	gcsEndpointFormat = "http://%s:%d/storage/v1/"
	gcsEndpoint       = fmt.Sprintf(gcsEndpointFormat, gcsHost, gcsPort)
)

func TestGlobalSortCleanupCloudFiles(t *testing.T) {
	var err error
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/dispatcher/WaitCleanUpFinished", "return()"))
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set @@global.tidb_ddl_enable_fast_reorg = 1;`)
	tk.MustExec("set @@global.tidb_enable_dist_task = 1;")
	sortStorageURI := fmt.Sprintf("gs://sorted/addindex?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)
	variable.CloudStorageURI.Store(sortStorageURI)
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
	<-dispatcher.WaitCleanUpFinished
	storeBackend, err := storage.ParseBackend(sortStorageURI, nil)
	require.NoError(t, err)
	opts := &storage.ExternalStorageOptions{NoCredentials: true}
	extStore, err := storage.New(context.Background(), storeBackend, opts)
	require.NoError(t, err)
	prefix := strconv.Itoa(int(jobID))
	dataFiles, statFiles, err := external.GetAllFileNames(context.Background(), extStore, prefix)
	require.NoError(t, err)
	require.Greater(t, jobID, int64(0))
	require.Equal(t, 0, len(dataFiles))
	require.Equal(t, 0, len(statFiles))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/dispatcher/WaitCleanUpFinished"))
}
