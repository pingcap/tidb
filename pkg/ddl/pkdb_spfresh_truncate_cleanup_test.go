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

package ddl_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestTruncateSPFreshPartialBootstrapCleanupPreservesLiveTable(t *testing.T) {
	ddl.MockSPFreshBuildLifecycle(t)
	var failResponse atomic.Bool
	var admitted sync.Map
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/spfresh/bootstrap":
			var body struct {
				TableID int64 `json:"table_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			// Model successful admission followed by a failed acknowledgement.
			admitted.Store(body.TableID, true)
			if failResponse.Load() {
				http.Error(w, "truncate bootstrap acknowledgement lost", 400)
				return
			}
		case "/spfresh/teardown":
			data, err := io.ReadAll(r.Body)
			var body tipb.SPFreshTeardownRequest
			if err == nil {
				err = proto.Unmarshal(data, &body)
			}
			if err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			admitted.Delete(body.TableId)
		default:
			http.Error(w, "unexpected request", 400)
			return
		}
		w.WriteHeader(200)
	}))
	t.Cleanup(server.Close)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) { conf.TiKVAPIServiceAddrs = []string{server.URL} })
	waitTime := ddl.GetWaitTimeWhenErrorOccurred()
	ddl.SetWaitTimeWhenErrorOccurred(time.Millisecond)
	defer ddl.SetWaitTimeWhenErrorOccurred(waitTime)
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	limit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 0")
	require.NoError(t, ddlutil.LoadDDLVars(tk.Session()))
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", limit))
	tk.MustExec("use test")
	tk.MustExec("create table trunc_spfresh(id int primary key, v vector(3), vector index vi((VEC_COSINE_DISTANCE(v))) using spfresh)")
	tk.MustExec("insert into trunc_spfresh values (1, '[1,2,3]')")
	oldID := external.GetTableByName(t, tk, "test", "trunc_spfresh").Meta().ID
	failResponse.Store(true)
	require.ErrorContains(t, tk.ExecToErr("truncate table trunc_spfresh"), "truncate bootstrap acknowledgement lost")
	require.Equal(t, oldID, external.GetTableByName(t, tk, "test", "trunc_spfresh").Meta().ID)
	tk.MustQuery("select id from trunc_spfresh").Check(testkit.Rows("1"))

	// Cleanup must use the recorded endpoint even if current routing changes.
	config.UpdateGlobal(func(conf *config.Config) { conf.TiKVAPIServiceAddrs = []string{"http://127.0.0.1:1"} })
	var records []*meta.SPFreshBootstrapJob
	require.Eventually(t, func() bool {
		version, err := store.CurrentVersion(kv.GlobalTxnScope)
		if err != nil {
			return false
		}
		records, err = meta.ReadSPFreshBootstrapJobs(store.GetSnapshot(version), 0, 100)
		return err == nil && len(records) == 2 && records[1].NextSweepAtUnixMilli > 0
	}, 15*time.Second, 50*time.Millisecond)
	failed := records[1]
	require.True(t, failed.Finished)
	require.False(t, failed.Succeeded)
	require.Len(t, failed.Targets, 1)
	failedID := failed.Targets[0].TableID
	require.NotEqual(t, oldID, failedID)
	_, present := admitted.Load(failedID)
	require.False(t, present)
	_, present = admitted.Load(oldID)
	require.True(t, present)
	for id, count := range map[int64]string{oldID: "0", failedID: "1"} {
		prefix := tablecodec.GenTablePrefix(id)
		query := fmt.Sprintf("select count(*) from (select start_key,end_key from mysql.gc_delete_range union all select start_key,end_key from mysql.gc_delete_range_done) g where start_key='%s' and end_key='%s'", prefix, prefix.PrefixNext())
		tk.MustQuery(query).Check(testkit.Rows(count))
	}
	tk.MustQuery("select id from trunc_spfresh").Check(testkit.Rows("1"))

	failResponse.Store(false)
	config.UpdateGlobal(func(conf *config.Config) { conf.TiKVAPIServiceAddrs = []string{server.URL} })
	tk.MustExec("truncate table trunc_spfresh")
	newID := external.GetTableByName(t, tk, "test", "trunc_spfresh").Meta().ID
	require.NotEqual(t, oldID, newID)
	require.NotEqual(t, failedID, newID)
	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	records, err = meta.ReadSPFreshBootstrapJobs(store.GetSnapshot(version), 0, 100)
	require.NoError(t, err)
	require.Len(t, records, 3)
	require.True(t, records[2].Finished)
	require.True(t, records[2].Succeeded)
	require.Equal(t, newID, records[2].Targets[0].TableID)
	_, present = admitted.Load(newID)
	require.True(t, present)
	_, present = admitted.Load(oldID)
	require.False(t, present)
	tk.MustQuery("select id from trunc_spfresh").Check(testkit.Rows())
	tk.MustExec("insert into trunc_spfresh values (2, '[4,5,6]')")
	tk.MustQuery("select id from trunc_spfresh").Check(testkit.Rows("2"))

	// Ordinary truncates should not create SPFresh admission records.
	tk.MustExec("create table ordinary_trunc(id int)")
	tk.MustExec("truncate table ordinary_trunc")
	version, err = store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	records, err = meta.ReadSPFreshBootstrapJobs(store.GetSnapshot(version), 0, 100)
	require.NoError(t, err)
	require.Len(t, records, 3)
}

func TestTruncateSPFreshCancellationFencesLateAdmission(t *testing.T) {
	endpoint, calls, _ := mockSwitchableSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) { conf.TiKVAPIServiceAddrs = []string{endpoint} })
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table trunc_fence(id int primary key, v vector(3), vector index vi((VEC_COSINE_DISTANCE(v))) using spfresh)")
	oldID := external.GetTableByName(t, tk, "test", "trunc_fence").Meta().ID
	calls.Store(0)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/truncateTableErr", "return(true)")
	require.ErrorContains(t, tk.ExecToErr("truncate table trunc_fence"), "occur an error after dropping table")
	require.Zero(t, calls.Load())
	require.Equal(t, oldID, external.GetTableByName(t, tk, "test", "trunc_fence").Meta().ID)
	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	records, err := meta.ReadSPFreshBootstrapJobs(store.GetSnapshot(version), 0, 100)
	require.NoError(t, err)
	require.Len(t, records, 2)
	canceled := records[1]
	require.True(t, canceled.Finished)
	require.False(t, canceled.Succeeded)
	require.Empty(t, canceled.Targets)
	late := records[0].Targets[0]
	late.TableID += 10000
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err = kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		_, err := meta.NewMutator(txn).RegisterSPFreshBootstrap(canceled.JobID, late)
		return err
	})
	require.ErrorContains(t, err, "already finished")
}
