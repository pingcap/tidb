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

package executor_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestSPFreshCachedReadSkipsUnusedWarmupButKeepsFreshReadTS(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()
	var readTS atomic.Uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshSearchTestBootstrap(t, w, r) {
			return
		}
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		request := decodeSPFreshSearchRequestForTest(t, data)
		readTS.Store(request.StartTs)
		data, err = proto.Marshal(spfreshSuccessResponseForTest(&tipb.SPFreshSearchResult{}))
		require.NoError(t, err)
		_, _ = w.Write(data)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(c *config.Config) { c.TiKVAPIServiceAddrs = []string{server.URL} })
	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=ON")
	tk.MustExec("create table spfresh_tso(id int primary key, label int, vec vector(3), vector index vi ((vec_l2_distance(vec))) using spfresh)")
	tk.MustExec("insert into spfresh_tso values (1, 7, '[1,0,0]')")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD", "return"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD"))
	})
	const query = "select id from spfresh_tso order by vec_l2_distance(vec, '[1,0,0]') limit 1"
	for _, mode := range []string{"direct", "prepared", "transaction", "snapshot"} {
		t.Run(mode, func(t *testing.T) {
			sql := query
			if mode == "prepared" {
				tk.MustExec("prepare q from 'select id from spfresh_tso order by vec_l2_distance(vec, ? ) limit 1'")
				tk.MustExec("set @v='[1,0,0]'")
				sql = "execute q using @v"
			}
			if mode == "transaction" {
				tk.MustExec("begin")
				// Initialize a transaction through an ordinary read first.
				tk.MustQuery("select id from spfresh_tso where id > 0").Check(testkit.Rows("1"))
			}
			if mode == "snapshot" {
				tk.Session().GetSessionVars().SnapshotTS = readTS.Load()
			}
			for iteration := range 2 {
				before := readTS.Load()
				tk.Session().SetValue(sessiontxn.TsoRequestCount, uint64(0))
				tk.MustQuery(sql).Check(testkit.Rows())
				require.Greater(t, readTS.Load(), before, "each execution must allocate a fresh worker timestamp")
				require.Equal(t, uint64(0), tk.Session().Value(sessiontxn.TsoRequestCount))
				if mode == "direct" || mode == "prepared" {
					require.Equal(t, iteration > 0, tk.Session().GetSessionVars().FoundInPlanCache)
				}
			}
			tk.Session().GetSessionVars().SnapshotTS = 0
			if mode == "transaction" {
				tk.MustExec("rollback")
			}
		})
	}
	tk.Session().SetValue(sessiontxn.TsoRequestCount, uint64(0))
	tk.MustQuery("select id from spfresh_tso where label=7 order by vec_l2_distance(vec, '[1,0,0]') limit 1").Check(testkit.Rows("1"))
	require.Greater(t, tk.Session().Value(sessiontxn.TsoRequestCount).(uint64), uint64(0), "a vector query whose filter prevents SPFresh must retain ordinary transaction reads")
	tk.Session().SetValue(sessiontxn.TsoRequestCount, uint64(0))
	tk.MustQuery("select id from spfresh_tso where id > 0").Check(testkit.Rows("1"))
	require.Greater(t, tk.Session().Value(sessiontxn.TsoRequestCount).(uint64), uint64(0), "ordinary SQL still warms its transaction")
}
