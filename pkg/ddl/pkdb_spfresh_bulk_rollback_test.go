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
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestSPFreshBulkBuildFailureRollsBackIndexMetadata(t *testing.T) {
	ddl.MockSPFreshBuildLifecycle(t)
	var teardownCalls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		switch {
		case r.URL.Path == "/spfresh/bulk-building" && r.Method == http.MethodPost:
			http.Error(w, "mock SPFresh bulk reorg failure", http.StatusInternalServerError)
		case r.URL.Path == "/spfresh/teardown" && r.Method == http.MethodPost:
			teardownCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/spfresh/build" && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected SPFresh request", http.StatusBadRequest)
		}
	})
	workerEndpoints := make([]string, 4)
	for i := range workerEndpoints {
		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)
		workerEndpoints[i] = server.URL
	}

	restoreConfig := config.RestoreFunc()
	t.Cleanup(restoreConfig)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = workerEndpoints
	})
	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id bigint primary key, embedding vector(768) not null)")
	originalURI := variable.CloudStorageURI.Load()
	variable.CloudStorageURI.Store("s3://spfresh-mvp/rollback-test")
	t.Cleanup(func() { variable.CloudStorageURI.Store(originalURI) })

	err := tk.ExecToErr("alter table t add vector index idx_emb((VEC_COSINE_DISTANCE(embedding))) USING SPFresh")
	require.ErrorContains(t, err, "mock SPFresh bulk reorg failure")
	require.Empty(t, external.GetTableByName(t, tk, "test", "t").Meta().Indices)
	require.Positive(t, teardownCalls.Load())

	// A failed build must not leave a hidden write-reorganization index that
	// traps a later DROP INDEX in the DDL error-count retry loop.
	tk.MustExec("alter table t drop index if exists idx_emb")
}
