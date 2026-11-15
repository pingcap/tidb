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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestSPFreshBulkPublicRequiresReady(t *testing.T) {
	originalURI := variable.CloudStorageURI.Load()
	variable.CloudStorageURI.Store("")
	t.Cleanup(func() { variable.CloudStorageURI.Store(originalURI) })

	core, observed := observer.New(zap.InfoLevel)
	originalLogger := logutil.AppLogger
	logutil.AppLogger = zap.New(core)
	t.Cleanup(func() { logutil.AppLogger = originalLogger })

	store := testkit.CreateMockStore(t)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	t.Cleanup(se.Close)
	execSQL := func(sql string) {
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
		recordSet, err := se.ExecuteInternal(ctx, sql)
		require.NoError(t, err)
		if recordSet != nil {
			require.NoError(t, recordSet.Close())
		}
	}
	execSQL("use test")
	execSQL("create table spfresh_t11_ordinary (id bigint primary key, value bigint not null)")
	execSQL("insert into spfresh_t11_ordinary values (1, 10), (2, 20)")
	execSQL("create index idx_value on spfresh_t11_ordinary(value)")
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("spfresh_t11_ordinary"))
	require.NoError(t, err)
	require.Equal(t, model.StatePublic, tbl.Meta().FindIndexByName("idx_value").State)
	require.Empty(t, observed.FilterMessage("SPFresh MVP bulk stage completed").FilterField(zap.String("stage", "publish")).All())
}

func TestSPFreshBulkReadyPublicRealTiKV(t *testing.T) {
	minioURI := os.Getenv("SPFRESH_MVP_MINIO_URI")
	workerURL := os.Getenv("SPFRESH_MVP_WORKER_URL")
	tikvPath := os.Getenv("SPFRESH_MVP_TIKV_PATH")
	if minioURI == "" || workerURL == "" || tikvPath == "" {
		t.Skip("SPFRESH_MVP_MINIO_URI, SPFRESH_MVP_WORKER_URL, and SPFRESH_MVP_TIKV_PATH are required")
	}

	proxy := newSPFreshBulkReadyIntegrationProxy(t, workerURL)
	restoreBulkSeams := ddl.ConfigureSPFreshBulkReadyIntegrationForTest()
	t.Cleanup(restoreBulkSeams)
	realStore, err := ddl.OpenSPFreshBulkRealTiKVStoreForTest(tikvPath)
	require.NoError(t, err)
	originalConfig := config.GetGlobalConfig()
	integrationConfig := *originalConfig
	integrationConfig.Store = "tikv"
	integrationConfig.TempDir = t.TempDir()
	integrationConfig.KeyspaceName = "a"
	integrationConfig.TiKVAPIServiceAddrs = []string{proxy.URL}
	config.StoreGlobalConfig(&integrationConfig)
	t.Cleanup(func() { config.StoreGlobalConfig(originalConfig) })
	originalMemRoot, originalDiskRoot := ingest.LitMemRoot, ingest.LitDiskRoot
	originalInitialized := ingest.LitInitialized
	t.Cleanup(func() {
		ingest.LitMemRoot = originalMemRoot
		ingest.LitDiskRoot = originalDiskRoot
		ingest.LitInitialized = originalInitialized
	})
	require.True(t, ingest.InitGlobalLightningEnv(t.TempDir()))
	originalURI := variable.CloudStorageURI.Load()
	t.Cleanup(func() { variable.CloudStorageURI.Store(originalURI) })

	session.SetSchemaLease(100 * time.Millisecond)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(realStore)
	require.NoError(t, err)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, realStore.Close())
	})
	tk := newSPFreshBulkReadyIntegrationTestKit(t, realStore)
	tk.MustExec("create database if not exists spfresh_t11")
	tk.MustExec("use spfresh_t11")

	for _, test := range []struct {
		name             string
		distanceFunction string
	}{
		{name: "l2", distanceFunction: "VEC_L2_DISTANCE"},
		{name: "cosine", distanceFunction: "VEC_COSINE_DISTANCE"},
	} {
		t.Run("ready_then_public_"+test.name, func(t *testing.T) {
			metricURI := withSPFreshBulkRunIDSuffix(t, minioURI, "-"+test.name)
			variable.CloudStorageURI.Store(metricURI)
			tableName := "ready_" + test.name
			createSPFreshBulkReadyIntegrationTable(t, tk, tableName)
			signalFile := t.TempDir() + "/allow-public"
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/create-index-stuck-before-public", fmt.Sprintf("return(%q)", signalFile))
			result := make(chan error, 1)
			go func() {
				ddlTK := newSPFreshBulkReadyIntegrationTestKit(t, realStore)
				result <- ddlTK.ExecToErr(fmt.Sprintf(
					"create vector index idx_vec on spfresh_t11.%s ((%s(vec))) using spfresh",
					tableName, test.distanceFunction,
				))
			}()

			ready := waitSPFreshBulkReadyResponse(t, proxy.ready)
			require.NotZero(t, ready.IndexTS)
			require.NotZero(t, ready.SourceSnapshotTS)
			require.NotZero(t, ready.RootLevel)
			tbl := waitSPFreshBulkIntegrationIndex(t, dom, tableName)
			require.NotEqual(t, model.StatePublic, tbl.Indices[0].State)
			select {
			case err := <-result:
				require.Failf(t, "DDL returned before Public signal", "error=%v", err)
			default:
			}
			require.NoError(t, os.WriteFile(signalFile, []byte("ready"), 0o600))
			require.NoError(t, waitSPFreshBulkDDLResult(t, result))
			tbl = waitSPFreshBulkIntegrationIndex(t, dom, tableName)
			require.Equal(t, model.StatePublic, tbl.Indices[0].State)
			queryTK := newSPFreshBulkReadyIntegrationTestKit(t, realStore)
			queryTK.MustQuery(fmt.Sprintf(
				"explain select /*+ USE_INDEX(%s, idx_vec) */ id from spfresh_t11.%s order by %s(vec, '%s') limit 10",
				tableName, tableName, test.distanceFunction, spfreshBulkReadyIntegrationQueryVector(),
			)).CheckContain("SPFreshVectorScan")
			t.Logf(
				"non-skip real %s DDL/query: Building -> Ready(B=%d,index_ts=%d,root_level=%d) -> Public -> SPFreshVectorScan",
				test.name, ready.SourceSnapshotTS, ready.IndexTS, ready.RootLevel,
			)
		})
	}

	t.Run("partial_ingest_rolls_back_index", func(t *testing.T) {
		variable.CloudStorageURI.Store(withSPFreshBulkRunIDSuffix(t, minioURI, "-partial-ingest"))
		createSPFreshBulkReadyIntegrationTable(t, tk, "partial_ingest")
		readyCallsBefore := proxy.readyCalls.Load()
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/spfreshBulkAfterBatchImport", "return(true)")
		err := tk.ExecToErr("create vector index idx_vec on spfresh_t11.partial_ingest ((VEC_L2_DISTANCE(vec))) using spfresh")
		require.ErrorContains(t, err, "injected failure after SPFresh batch ingest")
		require.Equal(t, readyCallsBefore, proxy.readyCalls.Load())
		tbl := waitSPFreshBulkIntegrationTable(t, dom, "partial_ingest")
		require.Empty(t, tbl.Indices)
		tk.MustQuery("select count(*) from spfresh_t11.partial_ingest").Check(testkit.Rows("400"))
		// DDL rollback schedules physical removal through the normal GC queue.
		// disableGC=true keeps this record observable in the isolated test cluster.
		gcRows := tk.MustQuery(fmt.Sprintf("select job_id from mysql.gc_delete_range where start_key='%s' and end_key='%s'",
			tablecodec.EncodeTableIndexPrefix(tbl.ID, tbl.MaxIndexID),
			tablecodec.EncodeTableIndexPrefix(tbl.ID, tbl.MaxIndexID+1),
		)).Rows()
		require.Len(t, gcRows, 1)
		backend, err := storage.ParseBackend(variable.CloudStorageURI.Load(), nil)
		require.NoError(t, err)
		backend.GetS3().Prefix = fmt.Sprintf("%s-j%v", backend.GetS3().Prefix, gcRows[0][0])
		objects, err := storage.NewWithDefaultOpt(context.Background(), backend)
		require.NoError(t, err)
		defer objects.Close()
		require.Eventually(t, func() bool {
			count := 0
			err := objects.WalkDir(context.Background(), nil, func(string, int64) error { count++; return nil })
			return err == nil && count == 0
		}, 30*time.Second, 100*time.Millisecond, "failed build objects were not drained by durable cleanup")
		t.Log("real partial ingest: no Ready/Public, metadata removed, source rows preserved, delete-range queued, S3 prefix empty")
	})

	t.Run("corrupt_run_rolls_back_index", func(t *testing.T) {
		corruptURI := withSPFreshBulkRunIDSuffix(t, minioURI, "-corrupt")
		variable.CloudStorageURI.Store(corruptURI)
		createSPFreshBulkReadyIntegrationTable(t, tk, "corrupt_table")
		proxy.corruptNext.Store(true)
		readyCallsBefore := proxy.readyCalls.Load()
		result := make(chan error, 1)
		go func() {
			ddlTK := newSPFreshBulkReadyIntegrationTestKit(t, realStore)
			result <- ddlTK.ExecToErr("create vector index idx_vec on spfresh_t11.corrupt_table ((VEC_L2_DISTANCE(vec))) using spfresh")
		}()
		built := waitSPFreshBulkBuildResponse(t, proxy.corruptBuild)
		backend, err := storage.ParseBackend(corruptURI, nil)
		require.NoError(t, err)
		// Corrupt the exact job-scoped object returned by the current worker.
		rootURI, err := url.Parse(built.Root.Reference.URI)
		require.NoError(t, err)
		require.Equal(t, backend.GetS3().Bucket, rootURI.Host)
		require.True(t, strings.HasSuffix(rootURI.Path, "/runs/root.spfmvp"))
		backend.GetS3().Prefix = strings.TrimPrefix(strings.TrimSuffix(rootURI.Path, "/runs/root.spfmvp"), "/")
		objectStore, err := storage.NewWithDefaultOpt(context.Background(), backend)
		require.NoError(t, err)
		root, err := objectStore.ReadFile(context.Background(), "runs/root.spfmvp")
		require.NoError(t, err)
		root[len(root)-1] ^= 0xff
		require.NoError(t, objectStore.WriteFile(context.Background(), "runs/root.spfmvp", root))
		objectStore.Close()
		close(proxy.releaseCorrupt)
		err = waitSPFreshBulkDDLResult(t, result)
		require.Error(t, err)
		require.Equal(t, readyCallsBefore, proxy.readyCalls.Load())
		tbl := waitSPFreshBulkIntegrationTable(t, dom, "corrupt_table")
		require.Empty(t, tbl.Indices)
		t.Logf(
			"non-skip corrupt Run: root_uri=%s records=%d; Ready not called, index metadata rolled back",
			built.Root.Reference.URI, built.Root.RecordCount,
		)
	})
}

type spfreshBulkReadyIntegrationProxy struct {
	URL            string
	upstream       string
	ready          chan remote.SPFreshBulkReadyResponse
	corruptBuild   chan remote.SPFreshBulkBuildResult
	releaseCorrupt chan struct{}
	corruptNext    atomic.Bool
	readyCalls     atomic.Int32
}

func newSPFreshBulkReadyIntegrationProxy(t *testing.T, upstream string) *spfreshBulkReadyIntegrationProxy {
	proxy := &spfreshBulkReadyIntegrationProxy{
		upstream:       strings.TrimSuffix(upstream, "/"),
		ready:          make(chan remote.SPFreshBulkReadyResponse, 1),
		corruptBuild:   make(chan remote.SPFreshBulkBuildResult, 1),
		releaseCorrupt: make(chan struct{}),
	}
	server := httptest.NewServer(http.HandlerFunc(proxy.forward))
	proxy.URL = server.URL
	t.Cleanup(server.Close)
	return proxy
}

func (p *spfreshBulkReadyIntegrationProxy) forward(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	forwarded, err := http.NewRequestWithContext(
		req.Context(), req.Method, p.upstream+req.URL.RequestURI(), bytes.NewReader(body),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	forwarded.Header = req.Header.Clone()
	response, err := http.DefaultClient.Do(forwarded)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	responseBody, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		http.Error(w, readErr.Error(), http.StatusBadGateway)
		return
	}
	if response.StatusCode == http.StatusOK && req.URL.Path == "/spfresh/bulk-build" && p.corruptNext.Swap(false) {
		var result remote.SPFreshBulkBuildResult
		if err := json.Unmarshal(responseBody, &result); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		p.corruptBuild <- result
		<-p.releaseCorrupt
	}
	if req.URL.Path == "/spfresh/bulk-ready" {
		p.readyCalls.Add(1)
		if response.StatusCode == http.StatusOK {
			var result remote.SPFreshBulkReadyResponse
			if err := json.Unmarshal(responseBody, &result); err == nil {
				p.ready <- result
			}
		}
	}
	for name, values := range response.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(responseBody)
}

func createSPFreshBulkReadyIntegrationTable(t *testing.T, tk *testkit.TestKit, name string) {
	tk.MustExec(fmt.Sprintf("create table spfresh_t11.%s (id bigint primary key, vec vector(768) not null)", name))
	for first := 1; first <= 400; first += 50 {
		last := min(first+49, 400)
		var statement strings.Builder
		fmt.Fprintf(&statement, "insert into spfresh_t11.%s values ", name)
		for handle := first; handle <= last; handle++ {
			if handle != first {
				statement.WriteByte(',')
			}
			fmt.Fprintf(&statement, "(%d,'[%d", handle, handle)
			for dimension := 1; dimension < int(pkdbspfreshbulk.Dimension); dimension++ {
				statement.WriteString(",0")
			}
			statement.WriteString("]')")
		}
		tk.MustExec(statement.String())
	}
}

func spfreshBulkReadyIntegrationQueryVector() string {
	var vector strings.Builder
	vector.WriteString("[1")
	for dimension := 1; dimension < int(pkdbspfreshbulk.Dimension); dimension++ {
		vector.WriteString(",0")
	}
	vector.WriteByte(']')
	return vector.String()
}

func newSPFreshBulkReadyIntegrationTestKit(t *testing.T, store kv.Storage) *testkit.TestKit {
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	se.SetConnectionID(uint64(time.Now().UnixNano()))
	t.Cleanup(func() { se.Close() })
	return testkit.NewTestKitWithSession(t, store, se)
}

func waitSPFreshBulkReadyResponse(
	t *testing.T,
	responses <-chan remote.SPFreshBulkReadyResponse,
) remote.SPFreshBulkReadyResponse {
	select {
	case response := <-responses:
		return response
	case <-time.After(10 * time.Minute):
		require.FailNow(t, "timed out waiting for real SPFresh Ready response")
		return remote.SPFreshBulkReadyResponse{}
	}
}

func waitSPFreshBulkBuildResponse(
	t *testing.T,
	responses <-chan remote.SPFreshBulkBuildResult,
) remote.SPFreshBulkBuildResult {
	select {
	case response := <-responses:
		return response
	case <-time.After(10 * time.Minute):
		require.FailNow(t, "timed out waiting for real SPFresh bulk-build response")
		return remote.SPFreshBulkBuildResult{}
	}
}

func waitSPFreshBulkDDLResult(t *testing.T, result <-chan error) error {
	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Minute):
		require.FailNow(t, "timed out waiting for real SPFresh DDL")
		return fmt.Errorf("unreachable")
	}
}

func waitSPFreshBulkIntegrationIndex(t *testing.T, dom *domain.Domain, tableName string) *model.TableInfo {
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		tbl := waitSPFreshBulkIntegrationTable(t, dom, tableName)
		if len(tbl.Indices) != 0 {
			return tbl
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.FailNow(t, "timed out waiting for SPFresh index metadata")
	return nil
}

func waitSPFreshBulkIntegrationTable(t *testing.T, dom *domain.Domain, tableName string) *model.TableInfo {
	tbl, err := dom.InfoSchema().TableInfoByName(
		pmodel.NewCIStr("spfresh_t11"), pmodel.NewCIStr(tableName),
	)
	require.NoError(t, err)
	return tbl
}

func withSPFreshBulkRunIDSuffix(t *testing.T, rawURI, suffix string) string {
	parsed, err := url.Parse(rawURI)
	require.NoError(t, err)
	parsed.Path = strings.TrimSuffix(parsed.Path, "/") + suffix
	return parsed.String()
}
