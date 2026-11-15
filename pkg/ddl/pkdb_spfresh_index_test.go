// Copyright 2022 PingCAP, Inc.
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
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func mockSPFreshBootstrapEndpoint(t *testing.T) string {
	t.Helper()
	endpoint, _, _ := mockSwitchableSPFreshBootstrapEndpoint(t)
	return endpoint
}

func mockCountingSPFreshBootstrapEndpoint(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	endpoint, calls, _ := mockSwitchableSPFreshBootstrapEndpoint(t)
	return endpoint, calls
}

func mockCountingSPFreshRuntimeConfigEndpoint(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	var updates atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "unexpected SPFresh request method", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/spfresh/bootstrap":
		case "/spfresh/runtime-config":
			updates.Add(1)
		default:
			http.Error(w, "unexpected SPFresh request", http.StatusBadRequest)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server.URL, &updates
}

func mockSwitchableSPFreshBootstrapEndpoint(t *testing.T) (string, *atomic.Int64, *atomic.Bool) {
	ddl.MockSPFreshBuildLifecycle(t)
	t.Helper()
	var calls atomic.Int64
	var failBootstrap atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "unexpected SPFresh request method", http.StatusBadRequest)
			return
		}
		fail := false
		switch r.URL.Path {
		case "/spfresh/bootstrap":
			calls.Add(1)
			fail = failBootstrap.Load()
		case "/spfresh/teardown":
		case "/spfresh/runtime-config":
		default:
			http.Error(w, "unexpected SPFresh request", http.StatusBadRequest)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		if fail {
			http.Error(w, "SPFresh bootstrap failed", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server.URL, &calls, &failBootstrap
}

func TestSPFreshVectorIndexRequiresRemoteBackend(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()

	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b vector(3));")
	tk.MustContainErrMsg("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh;",
		"SPFresh vector index requires remote backend")
}

func TestRetrySPFreshVectorIndexWhilePreviousCreationRollsBack(t *testing.T) {
	ddl.MockSPFreshBuildLifecycle(t)
	var failBootstrap atomic.Bool
	failBootstrap.Store(true)
	var teardownBlocked atomic.Bool
	teardownStarted := make(chan struct{})
	releaseTeardown := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		switch {
		case r.URL.Path == "/spfresh/bootstrap" && r.Method == http.MethodPost:
			if failBootstrap.Load() {
				http.Error(w, "SPFresh bootstrap failed", http.StatusBadRequest)
				return
			}
		case r.URL.Path == "/spfresh/teardown" && r.Method == http.MethodPost:
			if teardownBlocked.CompareAndSwap(false, true) {
				close(teardownStarted)
				<-releaseTeardown
			}
		case r.URL.Path == "/spfresh/build" && r.Method == http.MethodDelete:
		default:
			http.Error(w, "unexpected SPFresh request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	var teardownReleased atomic.Bool
	release := func() {
		if teardownReleased.CompareAndSwap(false, true) {
			close(releaseTeardown)
		}
	}
	defer release()

	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{server.URL}
	})

	waitTime := ddl.GetWaitTimeWhenErrorOccurred()
	ddl.SetWaitTimeWhenErrorOccurred(time.Millisecond)
	defer ddl.SetWaitTimeWhenErrorOccurred(waitTime)

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	limit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 1")
	require.NoError(t, ddlutil.LoadDDLVars(tk.Session()))
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", limit))
	tk.MustExec("create table t(id int primary key, embedding vector(3))")

	var submittedJobs atomic.Int64
	secondJobSubmitted := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/waitJobSubmitted", func() {
		if submittedJobs.Add(1) == 2 {
			close(secondJobSubmitted)
		}
	})

	firstResult := make(chan error, 1)
	go func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		firstResult <- tk.ExecToErr("alter table t add vector index idx_emb((VEC_COSINE_DISTANCE(embedding))) USING SPFresh")
	}()

	select {
	case <-teardownStarted:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "timed out waiting for the failed SPFresh index to start rollback teardown")
	}
	failBootstrap.Store(false)

	secondResult := make(chan error, 1)
	go func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		secondResult <- tk.ExecToErr("alter table t add vector index idx_emb_retry((VEC_COSINE_DISTANCE(embedding))) USING SPFresh")
	}()

	select {
	case <-secondJobSubmitted:
	case err := <-secondResult:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "timed out waiting for the retry DDL job to be submitted")
	}
	release()

	select {
	case err := <-firstResult:
		require.ErrorContains(t, err, "SPFresh bootstrap failed")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "timed out waiting for the failed SPFresh index DDL")
	}
	select {
	case err := <-secondResult:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "timed out waiting for the retried SPFresh index DDL")
	}

	tbl := external.GetTableByName(t, tk, "test", "t")
	require.Len(t, tbl.Meta().Indices, 1)
	require.Equal(t, "idx_emb_retry", tbl.Meta().Indices[0].Name.L)
	require.Equal(t, model.StatePublic, tbl.Meta().Indices[0].State)
}

func TestMultiSchemaAddSPFreshVectorIndexRejected(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})
	originalURI := variable.CloudStorageURI.Load()
	variable.CloudStorageURI.Store("")
	t.Cleanup(func() { variable.CloudStorageURI.Store(originalURI) })

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, c int, v vector(3), v2 vector(3));")
	createTableJobID := tk.MustQuery("admin show ddl jobs 1").Rows()[0][0].(string)

	for _, route := range []struct {
		name string
		uri  string
	}{
		{name: "legacy"},
		{name: "bulk", uri: "s3://spfresh-mvp/multi-schema"},
	} {
		t.Run(route.name, func(t *testing.T) {
			variable.CloudStorageURI.Store(route.uri)
			for _, sql := range []string{
				`alter table t add vector index idx_vec((VEC_L2_DISTANCE(v))) USING SPFresh, add index idx_c(c)`,
				`alter table t add index idx_c(c), add vector index idx_vec((VEC_L2_DISTANCE(v))) USING SPFresh`,
				`alter table t add vector index idx_vec((VEC_L2_DISTANCE(v))) USING SPFresh, add vector index idx_vec2((VEC_L2_DISTANCE(v2))) USING SPFresh`,
				`alter table t add vector index idx_vec((VEC_L2_DISTANCE(v))) USING SPFresh, add column d int`,
				`alter table t add column d int, add vector index idx_vec((VEC_L2_DISTANCE(v))) USING SPFresh`,
				`alter table t add vector index idx_vec((VEC_L2_DISTANCE(v))) USING SPFresh, modify column c bigint`,
				`alter table t drop column c, add vector index idx_vec((VEC_L2_DISTANCE(v))) USING SPFresh`,
				`alter table t add vector index idx_vec((VEC_L2_DISTANCE(v))) USING SPFresh, comment = 'rejected'`,
			} {
				tk.MustContainErrMsg(sql, "SPFresh vector index cannot be built in multi-schema change")
				row := tk.MustQuery("admin show ddl jobs 1").Rows()[0]
				require.Equal(t, createTableJobID, row[0])
				require.Equal(t, "create table", row[3])
			}
			tbl := external.GetTableByName(t, tk, "test", "t")
			require.Len(t, tbl.Meta().Columns, 4)
			require.Empty(t, tbl.Meta().Indices)
			require.Empty(t, tbl.Meta().Comment)
		})
	}
	variable.CloudStorageURI.Store("")

	// Standalone SPFresh creation and unrelated multi-schema changes remain supported.
	tk.MustExec(`alter table t add vector index idx_vec((VEC_L2_DISTANCE(v))) USING SPFresh`)
	tk.MustExec(`create vector index idx_vec2 on t ((VEC_L2_DISTANCE(v2))) USING SPFresh`)
	tk.MustExec(`alter table t add column d int, add index idx_c(c)`)
}

func TestCreateSPFreshVectorIndexRequiresRemoteBackend(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b vector(3));")
	tk.MustContainErrMsg("create vector index idx on t ((VEC_COSINE_DISTANCE(b))) USING SPFresh;",
		"SPFresh vector index requires remote backend")
}

func TestSPFreshVectorIndexRejectsPartitionedTables(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	const errMsg = "SPFresh vector index does not support partitioned tables"
	tk.MustContainErrMsg(`create table t_inline(
		a int,
		b vector(3),
		vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh
	) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (maxvalue)
	);`, errMsg)

	tk.MustExec(`create table t_alter(
		a int,
		b vector(3)
	) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (maxvalue)
	);`)
	tk.MustContainErrMsg("alter table t_alter add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh;", errMsg)

	tk.MustExec(`create table t_create(
		a int,
		b vector(3)
	) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (maxvalue)
	);`)
	tk.MustContainErrMsg("create vector index idx on t_create ((VEC_COSINE_DISTANCE(b))) USING SPFresh;", errMsg)

	tk.MustExec("create table t_repartition(a int, b vector(3));")
	tk.MustExec("alter table t_repartition add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh;")
	tk.MustContainErrMsg(`alter table t_repartition partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (maxvalue)
	);`, errMsg)
}

func TestSPFreshVectorIndexRejectsInvalidColumnAndMetric(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b vector, c vector(3));")

	cases := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "alter_non_fixed_dimension",
			sql:     "alter table t add vector index idx_unfixed((VEC_COSINE_DISTANCE(b))) USING SPFresh;",
			wantErr: "vector index can only be defined on fixed-dimension vector columns",
		},
		{
			name:    "create_non_fixed_dimension",
			sql:     "create vector index idx_unfixed_create on t ((VEC_COSINE_DISTANCE(b))) USING SPFresh;",
			wantErr: "vector index can only be defined on fixed-dimension vector columns",
		},
		{
			name:    "non_vector_column",
			sql:     "alter table t add vector index idx_nonvec((VEC_COSINE_DISTANCE(a))) USING SPFresh;",
			wantErr: "vector index can only be added on a vector column",
		},
		{
			name:    "unsupported_metric",
			sql:     "alter table t add vector index idx_l1((VEC_L1_DISTANCE(c))) USING SPFresh;",
			wantErr: "currently only VEC_L2_DISTANCE, VEC_COSINE_DISTANCE and VEC_NEGATIVE_INNER_PRODUCT is supported in SPFresh vector index",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tk.MustContainErrMsg(tc.sql, tc.wantErr)
		})
	}
}

func TestSPFreshVectorIndexAcceptsDimensionAbove4096(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_spfresh_dim(id int primary key, vec vector(4097))")
	tk.MustExec("create vector index idx_vec on t_spfresh_dim ((vec_l2_distance(vec))) using spfresh")

	tbl := external.GetTableByName(t, tk, "test", "t_spfresh_dim")
	require.Len(t, tbl.Meta().Indices, 1)
	require.Equal(t, uint64(4097), tbl.Meta().Indices[0].VectorInfo.Dimension)
}

func TestAddSPFreshVectorIndexIsNotColumnar(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b vector(3));")
	tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh;")

	tbl := external.GetTableByName(t, tk, "test", "t")
	require.Nil(t, tbl.Meta().TiFlashReplica)
	require.Len(t, tbl.Meta().Indices, 1)
	require.Equal(t, pmodel.IndexTypeSPFresh, tbl.Meta().Indices[0].Tp)
	require.NotNil(t, tbl.Meta().Indices[0].VectorInfo)
	require.Equal(t, model.VectorIndexKindSPFresh, tbl.Meta().Indices[0].VectorInfo.Kind)
	require.False(t, tbl.Meta().Indices[0].IsTiFlashLocalIndex())
}

func TestModifyVectorColumnToNotNullWithSPFreshIndex(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b vector(3) default null)")
	tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh;")
	tk.MustExec("insert into t values (1, '[1,2.1,3.3]'), (2, '[2,3.1,4.3]')")

	tk.MustQuery("select 1 from t where b is null limit 1").Check(testkit.Rows())
	tk.MustExec("alter table t modify column b vector(3) not null")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` vector(3) NOT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  VECTOR INDEX `idx`((VEC_COSINE_DISTANCE(`b`))) USING SPFRESH\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestModifyVectorColumnToNullableWithSPFreshIndex(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b vector(3) not null)")
	tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh;")
	tk.MustExec("insert into t values (1, '[1,2.1,3.3]'), (2, '[2,3.1,4.3]')")

	tk.MustExec("alter table t modify column b vector(3) null")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` vector(3) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  VECTOR INDEX `idx`((VEC_COSINE_DISTANCE(`b`))) USING SPFRESH\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestDropVectorColumnWithSPFreshIndexRejected(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b vector(3))")
	tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh;")

	tk.MustContainErrMsg("alter table t drop column b", "can't drop column b with SPFresh index covered now")
}

func TestDropStoredColumnWithSPFreshIndexRejected(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, tenant_id bigint unsigned not null, b vector(3))")
	tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh STORING (tenant_id)")

	tk.MustContainErrMsg("alter table t drop column tenant_id", "can't drop column with SPFresh index covered now")
}

func TestModifyVectorColumnWithSPFreshIndexRejected(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b vector(3) default null)")
	tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh;")

	tk.MustContainErrMsg("alter table t modify column b vector(2)", "[ddl:8200]Unsupported modify column: column has SPFresh index, drop SPFresh index first")
}

func TestModifyStoredColumnWithSPFreshIndexRejected(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, tenant_id bigint unsigned not null, b vector(3))")
	tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh STORING (tenant_id)")

	tk.MustContainErrMsg("alter table t modify column tenant_id bigint", "[ddl:8200]Unsupported modify column: column has SPFresh index, drop SPFresh index first")
	tk.MustContainErrMsg("alter table t modify column tenant_id bigint unsigned comment 'noop'", "[ddl:8200]Unsupported modify column: column has SPFresh index, drop SPFresh index first")
	tk.MustContainErrMsg("alter table t change column tenant_id tenant_ts bigint unsigned not null", "[ddl:8200]Unsupported modify column: column has SPFresh index, drop SPFresh index first")
	tk.MustContainErrMsg("alter table t rename column tenant_id to tenant_ts", "[ddl:8200]Unsupported modify column: column has SPFresh index, drop SPFresh index first")
	tk.MustContainErrMsg("alter table t rename column a to id", "[ddl:8200]Unsupported modify column: column has SPFresh index, drop SPFresh index first")
}

func TestConvertCharsetWithStoredColumnSPFreshIndexRejected(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, stored_col varchar(8) character set utf8 not null, free_col varchar(8) character set utf8 not null, b vector(3)) default charset=utf8")
	tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh STORING (stored_col)")

	tk.MustContainErrMsg("alter table t convert to character set utf8mb4 collate utf8mb4_bin", "[ddl:8200]Unsupported modify column: column has SPFresh index, drop SPFresh index first")
	tk.MustExec("alter table t modify column free_col varchar(8) character set utf8mb4 collate utf8mb4_bin not null")

	tk.MustExec("create table t_non_string(a int primary key, stored_col bigint not null, free_col varchar(8) character set utf8 not null, b vector(3)) default charset=utf8")
	tk.MustExec("alter table t_non_string add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh STORING (stored_col)")
	tk.MustExec("alter table t_non_string convert to character set utf8mb4 collate utf8mb4_bin")
}

func TestTruncateTableSPFreshBootstrapBeforeSchemaPublish(t *testing.T) {
	spfreshBootstrapURL, bootstrapCalls, failBootstrap := mockSwitchableSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	waitTime := ddl.GetWaitTimeWhenErrorOccurred()
	ddl.SetWaitTimeWhenErrorOccurred(time.Millisecond)
	defer ddl.SetWaitTimeWhenErrorOccurred(waitTime)

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	limit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 1")
	require.NoError(t, ddlutil.LoadDDLVars(tk.Session()))
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", limit))
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b vector(3))")
	tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh;")
	oldTableID := external.GetTableByName(t, tk, "test", "t").Meta().ID
	bootstrapCalls.Store(0)
	failBootstrap.Store(true)

	err := tk.ExecToErr("truncate table t")
	require.Error(t, err)
	require.Contains(t, err.Error(), "SPFresh bootstrap failed")
	require.Greater(t, bootstrapCalls.Load(), int64(0))
	require.Equal(t, oldTableID, external.GetTableByName(t, tk, "test", "t").Meta().ID)
}

func TestCreateTableWithForeignKeySPFreshBootstrapBeforeSchemaPublish(t *testing.T) {
	spfreshBootstrapURL, bootstrapCalls, failBootstrap := mockSwitchableSPFreshBootstrapEndpoint(t)
	failBootstrap.Store(true)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	waitTime := ddl.GetWaitTimeWhenErrorOccurred()
	ddl.SetWaitTimeWhenErrorOccurred(time.Millisecond)
	defer ddl.SetWaitTimeWhenErrorOccurred(waitTime)

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	limit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 0")
	require.NoError(t, ddlutil.LoadDDLVars(tk.Session()))
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", limit))
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("create table parent(id int primary key)")

	err := tk.ExecToErr(`create table child(
		id int primary key,
		parent_id int,
		embedding vector(3),
		foreign key(parent_id) references parent(id),
		vector index idx_embedding((VEC_COSINE_DISTANCE(embedding))) USING SPFresh
	)`)
	require.ErrorContains(t, err, "SPFresh bootstrap failed")
	require.Greater(t, bootstrapCalls.Load(), int64(0))

	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("child"))
	if !infoschema.ErrTableNotExists.Equal(err) {
		require.NoError(t, err)
		require.NotEqual(t, model.StatePublic, tbl.Meta().State)
	}
}

func TestBatchCreateTableSPFreshBootstrapBeforeSchemaPublish(t *testing.T) {
	spfreshBootstrapURL, bootstrapCalls, failBootstrap := mockSwitchableSPFreshBootstrapEndpoint(t)
	failBootstrap.Store(true)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	waitTime := ddl.GetWaitTimeWhenErrorOccurred()
	ddl.SetWaitTimeWhenErrorOccurred(time.Millisecond)
	defer ddl.SetWaitTimeWhenErrorOccurred(waitTime)

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	limit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 0")
	require.NoError(t, ddlutil.LoadDDLVars(tk.Session()))
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", limit))
	tk.MustExec("use test")

	infos := []*model.TableInfo{
		buildCreateTableInfoForBatch(t, store, tk.Session(), "create table t_plain(a int)"),
		buildCreateTableInfoForBatch(t, store, tk.Session(), `create table t_spfresh(
			a int,
			embedding vector(3),
			vector index idx_embedding((VEC_COSINE_DISTANCE(embedding))) USING SPFresh
		)`),
	}
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	err := dom.DDLExecutor().BatchCreateTableWithInfo(tk.Session(), pmodel.NewCIStr("test"), infos, ddl.WithOnExist(ddl.OnExistError))
	require.ErrorContains(t, err, "SPFresh bootstrap failed")
	require.Greater(t, bootstrapCalls.Load(), int64(0))

	for _, tblName := range []string{"t_plain", "t_spfresh"} {
		_, err = dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr(tblName))
		require.True(t, infoschema.ErrTableNotExists.Equal(err), "%s became visible after failed SPFresh bootstrap: %v", tblName, err)
	}
}

func TestSPFreshVectorIndexRollbackWaitsForTeardown(t *testing.T) {
	ddl.MockSPFreshBuildLifecycle(t)
	var teardownAttempts atomic.Int64
	secondRoundStarted := make(chan struct{})
	thirdRoundStarted := make(chan struct{})
	releaseSecondRound := make(chan struct{})
	releaseThirdRound := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		switch {
		case r.URL.Path == "/spfresh/bootstrap" && r.Method == http.MethodPost:
			http.Error(w, "SPFresh bootstrap failed", http.StatusBadRequest)
		case r.URL.Path == "/spfresh/teardown" && r.Method == http.MethodPost:
			attempt := teardownAttempts.Add(1)
			switch attempt {
			case 4:
				close(secondRoundStarted)
				<-releaseSecondRound
				http.Error(w, "persist teardown metadata failed", http.StatusServiceUnavailable)
			case 7:
				close(thirdRoundStarted)
				<-releaseThirdRound
				w.WriteHeader(http.StatusOK)
			default:
				http.Error(w, "persist teardown metadata failed", http.StatusServiceUnavailable)
			}
		case r.URL.Path == "/spfresh/build" && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected SPFresh request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)

	var secondRoundReleased atomic.Bool
	releaseSecond := func() {
		if secondRoundReleased.CompareAndSwap(false, true) {
			close(releaseSecondRound)
		}
	}
	defer releaseSecond()
	var thirdRoundReleased atomic.Bool
	releaseThird := func() {
		if thirdRoundReleased.CompareAndSwap(false, true) {
			close(releaseThirdRound)
		}
	}
	defer releaseThird()

	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{server.URL}
	})

	waitTime := ddl.GetWaitTimeWhenErrorOccurred()
	ddl.SetWaitTimeWhenErrorOccurred(time.Millisecond)
	defer ddl.SetWaitTimeWhenErrorOccurred(waitTime)

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	limit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 1")
	require.NoError(t, ddlutil.LoadDDLVars(tk.Session()))
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", limit))
	tk.MustExec("create table t(id int primary key, embedding vector(3))")
	tableID := external.GetTableByName(t, tk, "test", "t").Meta().ID

	result := make(chan error, 1)
	go func() {
		ddlTK := testkit.NewTestKit(t, store)
		ddlTK.MustExec("use test")
		result <- ddlTK.ExecToErr("alter table t add vector index idx_emb((VEC_COSINE_DISTANCE(embedding))) USING SPFresh")
	}()

	waitForRound := func(started <-chan struct{}, round string) {
		t.Helper()
		select {
		case <-started:
		case <-time.After(30 * time.Second):
			require.FailNow(t, "timed out waiting for SPFresh teardown retry", "round: %s", round)
		}
	}
	getRollbackJob := func() *model.Job {
		t.Helper()
		jobs, err := ddl.GetAllDDLJobs(context.Background(), testkit.NewTestKit(t, store).Session())
		require.NoError(t, err)
		for _, job := range jobs {
			if job.Type == model.ActionAddIndex && job.TableID == tableID {
				require.Equal(t, model.JobStateRollingback, job.State)
				require.Equal(t, model.StateDeleteOnly, job.SchemaState)
				return job
			}
		}
		require.FailNow(t, "SPFresh add-index rollback job not found")
		return nil
	}
	assertDeleteOnlyIndex := func() {
		t.Helper()
		indices := external.GetTableByName(t, tk, "test", "t").Meta().Indices
		require.Len(t, indices, 1)
		require.Equal(t, model.StateDeleteOnly, indices[0].State)
	}

	waitForRound(secondRoundStarted, "second")
	firstRetryJob := getRollbackJob()
	assertDeleteOnlyIndex()
	releaseSecond()

	waitForRound(thirdRoundStarted, "third")
	secondRetryJob := getRollbackJob()
	assertDeleteOnlyIndex()
	require.Equal(t, firstRetryJob.ErrorCount, secondRetryJob.ErrorCount)
	releaseThird()

	select {
	case err := <-result:
		require.ErrorContains(t, err, "SPFresh bootstrap failed")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "timed out waiting for SPFresh add-index rollback")
	}
	require.Empty(t, external.GetTableByName(t, tk, "test", "t").Meta().Indices)
}

func TestAddSPFreshVectorIndexAllowsDMLOnBackfilledRows(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})
	defer restore()

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	disableDistTaskAndFastReorg(t)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_txn_assertion_level='STRICT'")
	tk.MustExec("create table t(a int primary key, b vector(3))")
	tk.MustExec("insert into t values (1, '[1,2,3]'), (2, '[4,5,6]')")
	tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh")

	tk.MustExec("update t set b='[7,8,9]' where a=1")
	tk.MustExec("delete from t where a=2")
	tk.MustQuery("select a, b from t").Check(testkit.Rows("1 [7,8,9]"))
}

func TestSPFreshBuildFirstDDLTransactionRetry(t *testing.T) {
	for _, failure := range []string{"job_update", "commit"} {
		t.Run(failure, func(t *testing.T) {
			spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
			restore := config.RestoreFunc()
			defer restore()
			config.UpdateGlobal(func(conf *config.Config) { conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL} })
			store, _ := testkit.CreateMockStoreAndDomain(t)
			defer ingesttestutil.InjectMockBackendCtx(t, store)()
			tk := testkit.NewTestKit(t, store)
			disableDistTaskAndFastReorg(t)
			tk.MustExec("use test")
			tk.MustExec("create table t(a int primary key, b vector(3))")
			var injected, commitFailureEnabled, retried atomic.Bool
			var failedBase, admittedBase atomic.Uint64
			if failure == "commit" {
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/session/mockCommitError", "return(true)")
				t.Cleanup(func() {
					if commitFailureEnabled.Swap(false) {
						require.NoError(t, failpoint.Disable("tikvclient/mockCommitErrorOpt"))
					}
				})
			}
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
				if job.Type != model.ActionAddIndex {
					return
				}
				if commitFailureEnabled.Swap(false) {
					require.NoError(t, failpoint.Disable("tikvclient/mockCommitErrorOpt"))
				}
				if injected.Load() && job.SchemaState == model.StateNone {
					require.Zero(t, job.RealStartTS, "failed initial transaction must reload the unadmitted job")
					retried.Store(true)
				}
				if job.SchemaState == model.StateDeleteOnly {
					admittedBase.Store(job.RealStartTS)
				}
			})
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable", func(job *model.Job, err *error) {
				if job.Type != model.ActionAddIndex || job.SchemaState != model.StateDeleteOnly || !injected.CompareAndSwap(false, true) {
					return
				}
				failedBase.Store(job.RealStartTS)
				if failure == "job_update" {
					*err = kv.ErrTxnRetryable
				} else {
					require.NoError(t, failpoint.Enable("tikvclient/mockCommitErrorOpt", "return(true)"))
					commitFailureEnabled.Store(true)
				}
			})
			tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh")
			require.True(t, injected.Load())
			require.True(t, retried.Load())
			require.Greater(t, admittedBase.Load(), failedBase.Load())
			require.Equal(t, model.StatePublic, external.GetTableByName(t, tk, "test", "t").Meta().Indices[0].State)
		})
	}
}

func TestSPFreshEmptyBuildUsesAdmittedGeneration(t *testing.T) {
	for _, scenario := range []string{"empty", "all_null", "deleted_before_scan", "insert_after_scan"} {
		t.Run(scenario, func(t *testing.T) {
			spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
			restore := config.RestoreFunc()
			defer restore()
			config.UpdateGlobal(func(conf *config.Config) { conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL} })
			store, _ := testkit.CreateMockStoreAndDomain(t)
			defer ingesttestutil.InjectMockBackendCtx(t, store)()
			tk := testkit.NewTestKit(t, store)
			metadataTK := testkit.NewTestKit(t, store)
			dmlTK := testkit.NewTestKit(t, store)
			disableDistTaskAndFastReorg(t)
			tk.MustExec("use test")
			tk.MustExec("create table t(a int primary key, b vector(3))")
			if scenario == "all_null" {
				tk.MustExec("insert into t values (1, NULL), (2, NULL)")
			}
			if scenario == "deleted_before_scan" {
				tk.MustExec("insert into t values (1, '[1,2,3]')")
				var deleted atomic.Bool
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionBeforeScanV2", func() {
					if deleted.CompareAndSwap(false, true) {
						dmlTK.MustExec("delete from test.t")
					}
				})
			}
			var jobID atomic.Int64
			var bootstrapped atomic.Bool
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
				if job.Type == model.ActionAddIndex {
					jobID.Store(job.ID)
				}
			})
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockBootstrapEmptySPFreshIndexes", func(_ string, _ []int64, snapshot uint64, _ *error) {
				rows := metadataTK.MustQuery(fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where job_id = %d", jobID.Load())).Rows()
				require.Len(t, rows, 1)
				job := new(model.Job)
				require.NoError(t, job.Decode([]byte(rows[0][0].(string))))
				require.Equal(t, model.StateWriteReorganization, job.SchemaState)
				require.Equal(t, job.RealStartTS, snapshot)
				require.Equal(t, snapshot, job.ReorgMeta.SPFreshBuildGeneration, "zero input must retain the already committed generation")
				if scenario == "insert_after_scan" && !bootstrapped.Load() {
					dmlTK.MustExec("insert into test.t values (1, '[1,2,3]')")
				}
				bootstrapped.Store(true)
			})
			tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh")
			require.True(t, bootstrapped.Load())
			idx := external.GetTableByName(t, tk, "test", "t").Meta().Indices[0]
			require.Equal(t, model.StatePublic, idx.State)
			require.Equal(t, model.BackfillStateInapplicable, idx.BackfillState)
			if scenario == "insert_after_scan" {
				tk.MustQuery("select a from t").Check(testkit.Rows("1"))
			}
		})
	}
}

func TestCreateSPFreshVectorIndexWithRemoteBackendConfig(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b vector(3));")
	tk.MustExec("create vector index idx on t ((VEC_COSINE_DISTANCE(b))) USING SPFresh;")

	tbl := external.GetTableByName(t, tk, "test", "t")
	require.Nil(t, tbl.Meta().TiFlashReplica)
	require.Len(t, tbl.Meta().Indices, 1)
	require.Equal(t, pmodel.IndexTypeSPFresh, tbl.Meta().Indices[0].Tp)
	require.NotNil(t, tbl.Meta().Indices[0].VectorInfo)
	require.Equal(t, model.VectorIndexKindSPFresh, tbl.Meta().Indices[0].VectorInfo.Kind)
	require.False(t, tbl.Meta().Indices[0].IsTiFlashLocalIndex())
}

func TestSPFreshVectorIndexStoredColumnMetadata(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, tenant_id bigint unsigned not null, country varchar(2), b vector(3))")
	tk.MustExec("create vector index idx on t ((VEC_COSINE_DISTANCE(b))) USING SPFresh STORING (tenant_id, country)")

	tbl := external.GetTableByName(t, tk, "test", "t")
	idx := tbl.Meta().FindIndexByName("idx")
	require.NotNil(t, idx)
	require.NotNil(t, idx.VectorInfo)
	require.Equal(t, []int64{2, 3}, idx.VectorInfo.StoredColumnIDs)

	rows := tk.MustQuery("show create table t").Rows()
	createSQL := rows[0][1].(string)
	require.Contains(t, createSQL, "USING SPFRESH STORING (`tenant_id`,`country`)")
	tk.MustExec(strings.Replace(createSQL, "CREATE TABLE `t`", "CREATE TABLE `t_roundtrip`", 1))
	tbl = external.GetTableByName(t, tk, "test", "t_roundtrip")
	idx = tbl.Meta().FindIndexByName("idx")
	require.NotNil(t, idx)
	require.NotNil(t, idx.VectorInfo)
	require.Equal(t, []int64{2, 3}, idx.VectorInfo.StoredColumnIDs)

	tk.MustContainErrMsg(
		"create table t_hnsw(a int, tenant_id bigint unsigned not null, b vector(3), vector index bad_idx((vec_cosine_distance(b))) using hnsw storing (tenant_id))",
		"STORING can be only used in SPFresh VECTOR INDEX",
	)
	tk.MustContainErrMsg(
		"create vector index dup_idx on t ((VEC_COSINE_DISTANCE(b))) USING SPFresh STORING (tenant_id, tenant_id)",
		"duplicate SPFresh stored column tenant_id",
	)
	tk.MustContainErrMsg(
		"create vector index bad_storing_vector on t ((VEC_COSINE_DISTANCE(b))) USING SPFresh STORING (b)",
		"SPFresh STORING cannot include indexed vector column b",
	)
}

func TestAlterStoredColumnDefaultKeepsSPFreshStoredColumnBinding(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, tenant_id bigint not null default 7, b vector(3))")
	tk.MustExec("create vector index idx on t ((VEC_COSINE_DISTANCE(b))) USING SPFresh STORING (tenant_id)")

	tbl := external.GetTableByName(t, tk, "test", "t")
	idx := tbl.Meta().FindIndexByName("idx")
	require.NotNil(t, idx)
	require.Len(t, idx.VectorInfo.StoredColumnIDs, 1)
	originalColumnID := idx.VectorInfo.StoredColumnIDs[0]

	tk.MustExec("alter table t alter column tenant_id set default 9")
	tbl = external.GetTableByName(t, tk, "test", "t")
	idx = tbl.Meta().FindIndexByName("idx")
	require.NotNil(t, idx)
	require.Equal(t, []int64{originalColumnID}, idx.VectorInfo.StoredColumnIDs)
	col := tbl.Meta().FindPublicColumnByName("tenant_id")
	require.NotNil(t, col)
	require.Equal(t, "9", fmt.Sprint(col.GetDefaultValue()))
}

func TestSPFreshVectorIndexParamMetadata(t *testing.T) {
	spfreshBootstrapURL, bootstrapCalls := mockCountingSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	assertConfig := func(tableName, indexName string, searchBeamSize uint32, oversampleFactor float64, readOnly *bool) {
		t.Helper()
		tbl := external.GetTableByName(t, tk, "test", tableName)
		idx := tbl.Meta().FindIndexByName(indexName)
		require.NotNil(t, idx)
		require.NotNil(t, idx.VectorInfo)
		cfg := idx.VectorInfo.SearchConfig
		require.NotNil(t, cfg)
		require.Equal(t, searchBeamSize, cfg.SearchBeamSize)
		require.Equal(t, oversampleFactor, cfg.OversampleFactor)
		if readOnly == nil {
			require.Nil(t, cfg.ReadOnly)
		} else {
			require.NotNil(t, cfg.ReadOnly)
			require.Equal(t, *readOnly, *cfg.ReadOnly)
		}
	}
	assertIndexOptions := func(tableName, indexName string, rotAlgorithm string, minPartitionSize, maxPartitionSize, writeBeamSize uint64, buildShard uint32) {
		t.Helper()
		tbl := external.GetTableByName(t, tk, "test", tableName)
		idx := tbl.Meta().FindIndexByName(indexName)
		require.NotNil(t, idx)
		require.NotNil(t, idx.VectorInfo)
		opts := idx.VectorInfo.IndexOptions
		require.NotNil(t, opts)
		require.Equal(t, rotAlgorithm, opts.RotAlgorithm)
		require.Equal(t, minPartitionSize, opts.MinPartitionSize)
		require.Equal(t, maxPartitionSize, opts.MaxPartitionSize)
		require.Equal(t, writeBeamSize, opts.WriteBeamSize)
		require.Equal(t, buildShard, opts.BuildShard)
	}
	assertRuntimeConfig := func(tableName, indexName string, maxFixupWorkers, maxFixups, timeoutMS, maxAttempts uint64) {
		t.Helper()
		tbl := external.GetTableByName(t, tk, "test", tableName)
		idx := tbl.Meta().FindIndexByName(indexName)
		require.NotNil(t, idx)
		require.NotNil(t, idx.VectorInfo)
		require.Equal(t, &model.SPFreshVectorRuntimeConfig{
			HammingFactor:      96,
			Rabitq4Factor:      4,
			MaxFixupWorkers:    maxFixupWorkers,
			MaxFixups:          maxFixups,
			StalledOpTimeoutMS: timeoutMS,
			MaxInsertAttempts:  maxAttempts,
		}, idx.VectorInfo.RuntimeConfig)
	}
	boolPtr := func(v bool) *bool { return &v }

	tk.MustExec(`create table t_inline(
		a int,
		b vector(3),
		vector index idx((vec_l2_distance(b))) using spfresh
			vector_index_param "rot_algorithm=matrix, min_partition_size=32, max_partition_size=256, build_shard=4, write_beam_size=24, read_only=true, search_beam_size=64, oversample_factor=1.25,max_fixup_workers=37,max_fixups=400,stalled_op_timeout_ms=2000,max_insert_attempts=64"
	)`)
	assertConfig("t_inline", "idx", 64, 1.25, boolPtr(true))
	assertIndexOptions("t_inline", "idx", "matrix", 32, 256, 24, 4)
	assertRuntimeConfig("t_inline", "idx", 37, 400, 2000, 64)
	rows := tk.MustQuery("show create table t_inline").Rows()
	require.Contains(t, rows[0][1].(string), "USING SPFRESH VECTOR_INDEX_PARAM 'rot_algorithm=matrix,min_partition_size=32,max_partition_size=256,build_shard=4,write_beam_size=24,search_beam_size=64,oversample_factor=1.25,read_only=true,max_fixup_workers=37,max_fixups=400,stalled_op_timeout_ms=2000,max_insert_attempts=64'")

	tk.MustExec("create table t_alter(a int, b vector(3))")
	tk.MustExec(`alter table t_alter add vector index idx((vec_cosine_distance(b))) using spfresh
		vector_index_param "write_beam_size=16,search_beam_size=32,build_shard=1"`)
	assertConfig("t_alter", "idx", 32, 0, nil)
	assertIndexOptions("t_alter", "idx", "", 0, 0, 16, 1)
	assertRuntimeConfig("t_alter", "idx", 0, 200, 0, 32)
	tk.MustExec("create index idx_vec on t_alter(a)")
	tk.MustExec(`alter table t_alter alter index idx set vector_index_param "search_beam_size=48"`)
	assertConfig("t_alter", "idx", 48, 0, nil)
	assertIndexOptions("t_alter", "idx", "", 0, 0, 16, 1)
	tbl := external.GetTableByName(t, tk, "test", "t_alter")
	idx := tbl.Meta().FindIndexByName("idx_vec")
	require.NotNil(t, idx)
	require.Nil(t, idx.VectorInfo)

	tk.MustExec("create table t_create_idx(a int, b vector(3))")
	tk.MustExec(`create vector index idx using spfresh on t_create_idx ((vec_l2_distance(b)))
		vector_index_param "rot_algorithm=none,read_only=true,build_shard=256"`)
	assertConfig("t_create_idx", "idx", 0, 0, boolPtr(true))
	assertIndexOptions("t_create_idx", "idx", "none", 0, 0, 0, 256)

	bootstrapCallsBeforeAlter := bootstrapCalls.Load()
	tk.MustExec(`alter table t_inline alter index idx set vector_index_param "oversample_factor=2,read_only=false,max_fixup_workers=11,max_fixups=800,stalled_op_timeout_ms=3000,max_insert_attempts=96"`)
	assertConfig("t_inline", "idx", 64, 2, boolPtr(false))
	assertIndexOptions("t_inline", "idx", "matrix", 32, 256, 24, 4)
	assertRuntimeConfig("t_inline", "idx", 11, 800, 3000, 96)
	require.Equal(t, bootstrapCallsBeforeAlter, bootstrapCalls.Load())
	rows = tk.MustQuery("show create table t_inline").Rows()
	require.Contains(t, rows[0][1].(string), "USING SPFRESH VECTOR_INDEX_PARAM 'rot_algorithm=matrix,min_partition_size=32,max_partition_size=256,build_shard=4,write_beam_size=24,search_beam_size=64,oversample_factor=2,read_only=false,max_fixup_workers=11,max_fixups=800,stalled_op_timeout_ms=3000,max_insert_attempts=96'")

	tk.MustExec(`alter table t_inline alter index idx set vector_index_param "max_fixups=801"`)
	assertRuntimeConfig("t_inline", "idx", 11, 801, 3000, 96)

	tk.MustExec(`alter table t_inline alter index idx set vector_index_param "hamming_factor=64,rabitq4_factor=3"`)
	tbl = external.GetTableByName(t, tk, "test", "t_inline")
	cfg := tbl.Meta().FindIndexByName("idx").VectorInfo.RuntimeConfig
	require.Equal(t, uint64(64), cfg.HammingFactor)
	require.Equal(t, uint64(3), cfg.Rabitq4Factor)
	require.Equal(t, bootstrapCallsBeforeAlter, bootstrapCalls.Load())
	rows = tk.MustQuery("show create table t_inline").Rows()
	require.Contains(t, rows[0][1].(string), "hamming_factor=64,rabitq4_factor=3")
	require.Error(t, tk.ExecToErr(`alter table t_inline alter index idx set vector_index_param "hamming_factor=2"`))

	bootstrapCallsBeforeClear := bootstrapCalls.Load()
	tk.MustExec(`alter table t_inline alter index idx set vector_index_param " "`)
	require.Equal(t, bootstrapCallsBeforeClear, bootstrapCalls.Load())
	tbl = external.GetTableByName(t, tk, "test", "t_inline")
	idx = tbl.Meta().FindIndexByName("idx")
	require.NotNil(t, idx)
	require.NotNil(t, idx.VectorInfo)
	require.Nil(t, idx.VectorInfo.SearchConfig)
	assertIndexOptions("t_inline", "idx", "matrix", 32, 256, 24, 4)
	assertRuntimeConfig("t_inline", "idx", 0, 200, 0, 32)
	rows = tk.MustQuery("show create table t_inline").Rows()
	createSQL := rows[0][1].(string)
	require.Contains(t, createSQL, "USING SPFRESH")
	require.Contains(t, createSQL, "VECTOR_INDEX_PARAM 'rot_algorithm=matrix,min_partition_size=32,max_partition_size=256,build_shard=4,write_beam_size=24'")
	tk.MustExec(strings.Replace(createSQL, "CREATE TABLE `t_inline`", "CREATE TABLE `t_inline_roundtrip`", 1))
	tbl = external.GetTableByName(t, tk, "test", "t_inline_roundtrip")
	idx = tbl.Meta().FindIndexByName("idx")
	require.NotNil(t, idx)
	require.Equal(t, pmodel.IndexTypeSPFresh, idx.Tp)
	require.NotNil(t, idx.VectorInfo)
	require.Equal(t, model.VectorIndexKindSPFresh, idx.VectorInfo.Kind)
	require.Nil(t, idx.VectorInfo.SearchConfig)
	assertIndexOptions("t_inline_roundtrip", "idx", "matrix", 32, 256, 24, 4)
	tk.MustExec("create table t_auto(a int, b vector(3), vector index idx((vec_l2_distance(b))) using spfresh vector_index_param 'max_partition_size=512')")
	assertIndexOptions("t_auto", "idx", "", 0, 512, 0, 0)
	require.NotContains(t, tk.MustQuery("show create table t_auto").Rows()[0][1].(string), "build_shard")

	tk.MustContainErrMsg(`create table t_hnsw(a int, b vector(3),
		vector index idx((vec_l2_distance(b))) using hnsw vector_index_param "search_beam_size=1")`,
		"VECTOR_INDEX_PARAM can be only used in SPFresh VECTOR INDEX")
	tk.MustContainErrMsg(`create index bad_param on t_alter(a) vector_index_param "search_beam_size=1"`,
		"VECTOR_INDEX_PARAM can be only used in SPFresh VECTOR INDEX")
	// Derive the rejected value from the default so this case keeps covering the
	// min <= max rule when the default max_partition_size changes.
	badMinPartitionSize := model.DefaultSPFreshVectorMaxPartitionSize() + 1
	tk.MustContainErrMsg(fmt.Sprintf(`create table t_bad_min(a int, b vector(3),
		vector index idx((vec_l2_distance(b))) using spfresh vector_index_param "min_partition_size=%d")`,
		badMinPartitionSize),
		"min_partition_size must be <= max_partition_size")
	tk.MustContainErrMsg(`create table t_bad_write_beam(a int, b vector(3),
		vector index idx((vec_l2_distance(b))) using spfresh vector_index_param "write_beam_size=4097")`,
		"write_beam_size must be an unsigned integer in [1, 4096]")
	tk.MustContainErrMsg(`create table t_bad_build_shard(a int, b vector(3),
		vector index idx((vec_l2_distance(b))) using spfresh vector_index_param "build_shard=0")`,
		"build_shard must be an unsigned integer in [1, 256]")
	tk.MustContainErrMsg(`create table t_bad_search_beam(a int, b vector(3),
		vector index idx((vec_l2_distance(b))) using spfresh vector_index_param "search_beam_size=4097")`,
		"search_beam_size must be an unsigned integer in [1, 4096]")
	tk.MustContainErrMsg(`alter table t_alter alter index bad_param set vector_index_param "search_beam_size=1"`,
		"Key 'bad_param' doesn't exist in table 't_alter'")
	tk.MustExec("create index normal_idx on t_alter(a)")
	tk.MustContainErrMsg(`alter table t_alter alter index normal_idx set vector_index_param "search_beam_size=1"`,
		"VECTOR_INDEX_PARAM can be only set on SPFresh vector index")
	tk.MustContainErrMsg(`alter table t_alter alter index idx set vector_index_param "base_beam_size=1"`,
		`unknown VECTOR_INDEX_PARAM key "base_beam_size"`)
	tk.MustContainErrMsg(`alter table t_alter alter index idx set vector_index_param "search_beam_size=4097"`,
		"search_beam_size must be an unsigned integer in [1, 4096]")
	tk.MustContainErrMsg(`alter table t_alter alter index idx set vector_index_param "write_beam_size=24"`,
		`unknown VECTOR_INDEX_PARAM key "write_beam_size"`)
	tk.MustContainErrMsg(`alter table t_alter alter index idx set vector_index_param "build_shard=4"`,
		`unknown VECTOR_INDEX_PARAM key "build_shard"`)
	tk.MustContainErrMsg(`alter table t_alter alter index idx set vector_index_param "max_fixups=1025"`,
		"max_fixups must be an unsigned integer in [1, 1024]")
}

func TestAlterSPFreshRuntimeConfigRetriesAfterMetadataFailure(t *testing.T) {
	workerURL, runtimeUpdates := mockCountingSPFreshRuntimeConfigEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{workerURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b vector(3), vector index idx((vec_l2_distance(b))) using spfresh)")

	var metadataUpdateAttempts atomic.Int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeAlterVectorIndexParamMetadataUpdate", func(injectedErr *error) {
		if metadataUpdateAttempts.Add(1) == 1 {
			*injectedErr = errors.New("injected TiDB metadata update failure")
		}
	})

	tk.MustExec(`alter table t alter index idx set vector_index_param "max_fixups=400"`)
	require.Equal(t, int64(2), runtimeUpdates.Load())

	tbl := external.GetTableByName(t, tk, "test", "t")
	idx := tbl.Meta().FindIndexByName("idx")
	require.Equal(t, uint64(400), idx.VectorInfo.RuntimeConfig.MaxFixups)
}

func TestMultiSchemaAlterSPFreshRuntimeConfigResumesAfterTerminalFanoutFailure(t *testing.T) {
	type workerState struct {
		failUpdate atomic.Bool
		updates    atomic.Int64
	}
	newWorker := func(state *workerState) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			switch r.URL.Path {
			case "/spfresh/bootstrap":
				w.WriteHeader(http.StatusOK)
			case "/spfresh/runtime-config":
				state.updates.Add(1)
				if state.failUpdate.Load() {
					http.Error(w, "index not found on worker", http.StatusNotFound)
					return
				}
				w.WriteHeader(http.StatusOK)
			default:
				http.Error(w, "unexpected SPFresh request", http.StatusBadRequest)
			}
		}))
	}

	var firstState, secondState workerState
	first := newWorker(&firstState)
	defer first.Close()
	second := newWorker(&secondState)
	defer second.Close()

	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{first.URL, second.URL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	adminTK := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tests := []struct {
		name      string
		tableName string
		failFirst bool
	}{
		{name: "partial success", tableName: "t_partial"},
		{name: "all terminal", tableName: "t_terminal", failFirst: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tk.MustExec(fmt.Sprintf("create table %s(a int, b vector(3), vector index idx((vec_l2_distance(b))) using spfresh)", test.tableName))
			firstBefore := firstState.updates.Load()
			secondBefore := secondState.updates.Load()
			firstState.failUpdate.Store(test.failFirst)
			secondState.failUpdate.Store(true)

			alterDone := make(chan error, 1)
			go func() {
				_, err := tk.Exec(fmt.Sprintf(`alter table %s alter index idx set vector_index_param "max_fixups=400", add column c int`, test.tableName))
				alterDone <- err
			}()

			require.Eventually(t, func() bool {
				return firstState.updates.Load() == firstBefore+1 && secondState.updates.Load() == secondBefore+1
			}, 10*time.Second, 10*time.Millisecond)
			var jobID int64
			require.Eventually(t, func() bool {
				for _, row := range adminTK.MustQuery("admin show ddl jobs").Rows() {
					if row[3].(string) == model.ActionMultiSchemaChange.String() && row[len(row)-2].(string) == "paused" {
						var err error
						jobID, err = strconv.ParseInt(row[0].(string), 10, 64)
						return err == nil
					}
				}
				return false
			}, 10*time.Second, 10*time.Millisecond)
			select {
			case err := <-alterDone:
				t.Fatalf("paused ALTER returned unexpectedly: %v", err)
			case <-time.After(100 * time.Millisecond):
			}

			firstState.failUpdate.Store(false)
			secondState.failUpdate.Store(false)
			adminTK.MustQuery(fmt.Sprintf("admin resume ddl jobs %d", jobID)).Check(
				testkit.Rows(fmt.Sprintf("%d successful", jobID)),
			)
			select {
			case err := <-alterDone:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("runtime config DDL did not finish after resume")
			}
			require.Equal(t, firstBefore+2, firstState.updates.Load())
			require.Equal(t, secondBefore+2, secondState.updates.Load())
			tbl := external.GetTableByName(t, tk, "test", test.tableName)
			idx := tbl.Meta().FindIndexByName("idx")
			require.Equal(t, uint64(400), idx.VectorInfo.RuntimeConfig.MaxFixups)
			require.NotNil(t, model.FindColumnInfo(tbl.Meta().Columns, "c"))
		})
	}
}

func TestCreateSPFreshVectorIndexSupportsInnerProductDistance(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b vector(3));")
	tk.MustExec("create vector index idx_ip on t ((VEC_NEGATIVE_INNER_PRODUCT(b))) USING SPFresh;")

	tbl := external.GetTableByName(t, tk, "test", "t")
	require.Nil(t, tbl.Meta().TiFlashReplica)
	require.Len(t, tbl.Meta().Indices, 1)
	idx := tbl.Meta().Indices[0]
	require.Equal(t, pmodel.IndexTypeSPFresh, idx.Tp)
	require.NotNil(t, idx.VectorInfo)
	require.Equal(t, model.VectorIndexKindSPFresh, idx.VectorInfo.Kind)
	require.Equal(t, model.DistanceMetricInnerProduct, idx.VectorInfo.DistanceMetric)
	require.False(t, idx.IsTiFlashLocalIndex())
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` vector(3) DEFAULT NULL,\n" +
		"  VECTOR INDEX `idx_ip`((VEC_NEGATIVE_INNER_PRODUCT(`b`))) USING SPFRESH\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestAddSPFreshVectorIndexWritesHandleVectorPayload(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	disableDistTaskAndFastReorg(t)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, b vector(3));")
	tk.MustExec("insert into t values (42, '[1,2,3]');")
	tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING SPFresh;")

	snap := store.GetSnapshot(kv.MaxVersion)
	got, err := snap.Get(context.Background(), kv.IntHandle(42).Encoded())
	require.NoError(t, err)

	vec, err := types.ParseVectorFloat32("[1,2,3]")
	require.NoError(t, err)
	expectedPayload := append([]byte(nil), vec.ZeroCopySerialize()[4:]...)
	expectedPayload = append(expectedPayload, 0, 0, 0, 0)
	require.Equal(t, expectedPayload, got.Value)
}

func disableDistTaskAndFastReorg(t *testing.T) {
	t.Helper()
	prevEnableDistTask := variable.EnableDistTask.Load()
	prevEnableFastReorg := variable.EnableFastReorg.Load()
	variable.EnableDistTask.Store(false)
	variable.EnableFastReorg.Store(false)
	t.Cleanup(func() {
		variable.EnableDistTask.Store(prevEnableDistTask)
		variable.EnableFastReorg.Store(prevEnableFastReorg)
	})
}
func buildCreateTableInfoForBatch(t *testing.T, store kv.Storage, sctx sessionctx.Context, sql string) *model.TableInfo {
	t.Helper()
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	createStmt := stmt.(*ast.CreateTableStmt)
	tbInfo, err := ddl.BuildTableInfoFromAST(ddl.NewMetaBuildContextWithSctx(sctx), createStmt)
	require.NoError(t, err)
	return tbInfo
}

func TestCTASSPFreshBootstrapBeforeLoadAndRollback(t *testing.T) {
	endpoint, calls, failBootstrap := mockSwitchableSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) { conf.TiKVAPIServiceAddrs = []string{endpoint} })
	waitTime := ddl.GetWaitTimeWhenErrorOccurred()
	ddl.SetWaitTimeWhenErrorOccurred(time.Millisecond)
	defer ddl.SetWaitTimeWhenErrorOccurred(waitTime)
	store, dom := testkit.CreateMockStoreAndDomain(t)
	dom.InfoSyncer().SetSessionManager(&testkit.MockSessionManager{})
	tk := testkit.NewTestKit(t, store)
	limit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 0")
	require.NoError(t, ddlutil.LoadDDLVars(tk.Session()))
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", limit))
	tk.MustExec("use test")
	tk.MustExec("create table ctas_src(id int primary key, embedding vector(3))")
	tk.MustExec("insert into ctas_src values (1, '[1,2,3]'), (2, '[4,5,6]')")
	sql := "create table %s(id int primary key, embedding vector(3), vector index vi((VEC_COSINE_DISTANCE(embedding))) using spfresh) as select * from ctas_src"
	failBootstrap.Store(true)
	require.ErrorContains(t, tk.ExecToErr(fmt.Sprintf(sql, "ctas_failed")), "SPFresh bootstrap failed")
	require.Positive(t, calls.Load())
	_, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("ctas_failed"))
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	records, err := meta.ReadSPFreshBootstrapJobs(store.GetSnapshot(version), 0, 100)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.True(t, records[0].Finished)
	require.False(t, records[0].Succeeded)
	require.Len(t, records[0].Targets, 1)
	failBootstrap.Store(false)
	calls.Store(0)
	tk.MustExec(fmt.Sprintf(sql, "ctas_ok"))
	require.Positive(t, calls.Load())
	tk.MustQuery("select id, cast(embedding as char) from ctas_ok order by id").Check(testkit.Rows("1 [1,2,3]", "2 [4,5,6]"))
	version, err = store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	records, err = meta.ReadSPFreshBootstrapJobs(store.GetSnapshot(version), 0, 100)
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.True(t, records[1].Finished)
	require.True(t, records[1].Succeeded)
	// The load can fail after remote bootstrap succeeds. Its rollback must also
	// publish cleanup ownership, rather than leaving an active intent forever.
	duplicateSQL := strings.Replace(fmt.Sprintf(sql, "ctas_duplicate"), "select * from ctas_src", "select 1 as id, embedding from ctas_src", 1)
	require.ErrorContains(t, tk.ExecToErr(duplicateSQL), "Duplicate entry")
	_, err = dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("ctas_duplicate"))
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
	version, err = store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	records, err = meta.ReadSPFreshBootstrapJobs(store.GetSnapshot(version), 0, 100)
	require.NoError(t, err)
	require.Len(t, records, 3)
	require.True(t, records[2].Finished)
	require.False(t, records[2].Succeeded)
}

func TestDropSPFreshIndexSchedulesPhysicalGC(t *testing.T) {
	endpoint := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) { conf.TiKVAPIServiceAddrs = []string{endpoint} })
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table drop_spfresh(id int primary key, v vector(3), vector index vi((VEC_COSINE_DISTANCE(v))) using spfresh)")
	info := external.GetTableByName(t, tk, "test", "drop_spfresh").Meta()
	require.Len(t, info.Indices, 1)
	start := tablecodec.EncodeTableIndexPrefix(info.ID, info.Indices[0].ID)
	end := tablecodec.EncodeTableIndexPrefix(info.ID, info.Indices[0].ID+1)
	tk.MustExec("alter table drop_spfresh drop index vi")
	// The mock delete-range emulator can already have moved the task to done.
	sql := fmt.Sprintf("select count(*) from (select start_key,end_key from mysql.gc_delete_range union all select start_key,end_key from mysql.gc_delete_range_done) g where start_key='%s' and end_key='%s'", start, end)
	tk.MustQuery(sql).Check(testkit.Rows("1"))
	tk.MustExec("insert into drop_spfresh values (1, '[1,2,3]')")
	tk.MustQuery("select id from drop_spfresh").Check(testkit.Rows("1"))
}

func TestBatchSPFreshPartialBootstrapCleanupSurvivesEndpointChange(t *testing.T) {
	var firstTable atomic.Int64
	var teardownCalls atomic.Int64
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
			firstTable.CompareAndSwap(0, body.TableID)
			if body.TableID != firstTable.Load() {
				http.Error(w, "second bootstrap failed", 400)
				return
			}
			w.WriteHeader(200)
		case "/spfresh/teardown":
			teardownCalls.Add(1)
			w.WriteHeader(200)
		default:
			http.Error(w, "unexpected request", 400)
		}
	}))
	t.Cleanup(server.Close)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) { conf.TiKVAPIServiceAddrs = []string{server.URL} })
	waitTime := ddl.GetWaitTimeWhenErrorOccurred()
	ddl.SetWaitTimeWhenErrorOccurred(time.Millisecond)
	defer ddl.SetWaitTimeWhenErrorOccurred(waitTime)
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	limit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 0")
	require.NoError(t, ddlutil.LoadDDLVars(tk.Session()))
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", limit))
	tk.MustExec("use test")
	infos := make([]*model.TableInfo, 0, 2)
	for _, name := range []string{"batch_one", "batch_two"} {
		infos = append(infos, buildCreateTableInfoForBatch(t, store, tk.Session(), fmt.Sprintf("create table %s(id int, v vector(3), vector index vi((VEC_COSINE_DISTANCE(v))) using spfresh)", name)))
	}
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	err := dom.DDLExecutor().BatchCreateTableWithInfo(tk.Session(), pmodel.NewCIStr("test"), infos, ddl.WithOnExist(ddl.OnExistError))
	require.ErrorContains(t, err, "second bootstrap failed")
	config.UpdateGlobal(func(conf *config.Config) { conf.TiKVAPIServiceAddrs = []string{"http://127.0.0.1:1"} })
	var record *meta.SPFreshBootstrapJob
	require.Eventually(t, func() bool {
		version, err := store.CurrentVersion(kv.GlobalTxnScope)
		if err != nil {
			return false
		}
		records, err := meta.ReadSPFreshBootstrapJobs(store.GetSnapshot(version), 0, 100)
		if err != nil || len(records) != 1 {
			return false
		}
		record = records[0]
		return record.NextSweepAtUnixMilli > 0
	}, 15*time.Second, 50*time.Millisecond)
	require.True(t, record.Finished)
	require.False(t, record.Succeeded)
	require.Len(t, record.Targets, 2)
	require.GreaterOrEqual(t, teardownCalls.Load(), int64(2))
	for _, target := range record.Targets {
		// Both failed fresh table IDs get physical cleanup, including the table
		// whose bootstrap response failed and may have hidden remote side effects.
		prefix := tablecodec.GenTablePrefix(target.TableID)
		end := prefix.PrefixNext()
		sql := fmt.Sprintf("select count(*) from (select start_key,end_key from mysql.gc_delete_range union all select start_key,end_key from mysql.gc_delete_range_done) g where start_key='%s' and end_key='%s'", prefix, end)
		tk.MustQuery(sql).Check(testkit.Rows("1"))
	}
}
