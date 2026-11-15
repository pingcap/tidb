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

package ddl

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/store/mockstore/mockstorage"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

func TestSPFreshBulkProofStagingProgressesWithOneObjectBudget(t *testing.T) {
	store, run := newSPFreshBulkFinalManifestFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	oldLimit := memory.ServerMemoryLimit.Load()
	t.Cleanup(func() { memory.ServerMemoryLimit.Store(oldLimit) })
	// The 4 MiB object allowance fits one fixture object, including its 2 MiB
	// scratch reservation, but cannot admit two at once.
	memory.ServerMemoryLimit.Store(8 << 20)
	var consumed atomic.Int32
	require.NoError(t, proveAndStageSPFreshTestBuild(ctx, store, run, func(context.Context, spfreshBulkSortedRun, pkdbspfreshbulk.ManifestRef) error {
		consumed.Add(1)
		return nil
	}))
	require.Equal(t, int32(6), consumed.Load())
	spfreshBulkReads.mu.Lock()
	defer spfreshBulkReads.mu.Unlock()
	require.Zero(t, spfreshBulkReads.used)
}

func TestSPFreshBulkProofStagingRejectsOversizedObjectsBeforeImport(t *testing.T) {
	store, run := newSPFreshBulkFinalManifestFixture(t)
	oldLimit := memory.ServerMemoryLimit.Swap(8)
	t.Cleanup(func() { memory.ServerMemoryLimit.Store(oldLimit) })
	var consumed atomic.Int32
	err := proveAndStageSPFreshTestBuild(context.Background(), store, run, func(context.Context, spfreshBulkSortedRun, pkdbspfreshbulk.ManifestRef) error {
		consumed.Add(1)
		return nil
	})
	require.ErrorContains(t, err, "exceeds budget")
	require.Zero(t, consumed.Load())
	require.Nil(t, run.finalManifest)
	exists, err := store.FileExists(context.Background(), "final.json")
	require.NoError(t, err)
	require.False(t, exists)
	spfreshBulkReads.mu.Lock()
	defer spfreshBulkReads.mu.Unlock()
	require.Zero(t, spfreshBulkReads.used)
}

func TestSPFreshBulkStagingFailureRejectsFinalManifestAndReleasesReads(t *testing.T) {
	store, run := newSPFreshBulkFinalManifestFixture(t)
	err := proveAndStageSPFreshTestBuild(context.Background(), store, run, func(context.Context, spfreshBulkSortedRun, pkdbspfreshbulk.ManifestRef) error {
		return errors.New("injected memory import failure")
	})
	require.ErrorContains(t, err, "injected memory import failure")
	require.Nil(t, run.finalManifest)
	exists, err := store.FileExists(context.Background(), "final.json")
	require.NoError(t, err)
	require.False(t, exists)
	spfreshBulkReads.mu.Lock()
	defer spfreshBulkReads.mu.Unlock()
	require.Zero(t, spfreshBulkReads.used)
}

func TestSPFreshBulkImportFailureStopsBeforeVerifyAndPublish(t *testing.T) {
	var calls []string
	stage := func(name string) func(context.Context, *spfreshBulkRunContext) error {
		return func(context.Context, *spfreshBulkRunContext) error {
			calls = append(calls, name)
			return nil
		}
	}
	err := runSPFreshBulkReorgWithStages(
		context.Background(),
		&spfreshBulkRunContext{sourceRows: 1},
		spfreshBulkStages{
			building:          stage("building"),
			scanAndUpload:     stage("scan_upload"),
			buildAndWriteRuns: stage("build_runs"),
			importRuns: func(context.Context, *spfreshBulkRunContext) error {
				calls = append(calls, "import")
				return errors.New("import failed")
			},
			verify:  stage("unexpected_verify"),
			publish: stage("unexpected_publish"),
		},
	)
	require.EqualError(t, err, "import failed")
	require.Equal(t, []string{"building", "scan_upload", "build_runs", "import"}, calls)
}

func TestSPFreshBulkSortedRunRealTiKVImport(t *testing.T) {
	minioURI := os.Getenv("SPFRESH_MVP_MINIO_URI")
	workerURL := os.Getenv("SPFRESH_MVP_WORKER_URL")
	tikvPath := os.Getenv("SPFRESH_MVP_TIKV_PATH")
	if minioURI == "" || workerURL == "" || tikvPath == "" {
		t.Skip("SPFRESH_MVP_MINIO_URI, SPFRESH_MVP_WORKER_URL, and SPFRESH_MVP_TIKV_PATH are required")
	}
	ctx := context.Background()
	runID, artifactPrefix, err := parseSPFreshBulkStorageURI(minioURI)
	require.NoError(t, err)

	sourceStore := newSPFreshBulkMockStore(t)
	physicalTable, indexInfo := newSPFreshBulkScanTestTable(t)
	rows := make(map[int64][]float32, 400)
	for handle := int64(1); handle <= 400; handle++ {
		rows[handle] = testSPFreshBulkVector(handle, uint32(handle)+0x3f800000)
	}
	writeSPFreshBulkTestRows(t, sourceStore, physicalTable.Meta(), rows)
	sourceSnapshot := currentSPFreshBulkTestVersion(t, sourceStore)
	run := newSPFreshBulkScanTestRun(indexInfo.ID, sourceSnapshot, runID)
	run.cloudStorageURI = minioURI
	run.artifactPrefix = artifactPrefix
	run.workerURL = workerURL
	run.nonRootRunURI = artifactPrefix + "/runs/non-root.spfmvp"
	run.rootRunURI = artifactPrefix + "/runs/root.spfmvp"
	objectBackend, err := storage.ParseBackend(minioURI, nil)
	require.NoError(t, err)
	objectStore, err := storage.NewWithDefaultOpt(ctx, objectBackend)
	require.NoError(t, err)
	defer objectStore.Close()
	require.NoError(t, initializeSPFreshBulkLifecycle(ctx, objectStore))
	require.NoError(t, scanAndUploadSPFreshBaseFragmentsToStore(
		ctx,
		exprstatic.NewExprContext(),
		NewReorgContext(),
		sourceStore,
		physicalTable,
		run,
		objectStore, testSPFreshBulkScanLayout(t, 1),
		spfreshBulkScanConfig{
			fragmentTargetBytes: 128 * spfreshBulkBaseRecordEncodedBytes,
		},
	))
	require.NoError(t, buildAndWriteSPFreshSortedRuns(ctx, http.DefaultClient, run))
	var allRecords []pkdbspfreshbulk.SortedRunRecord
	require.NoError(t, proveAndStageSPFreshSingleRuns(ctx, objectStore, run,
		func(_ context.Context, decoded spfreshBulkSortedRun, _ pkdbspfreshbulk.ManifestRef) error {
			for _, record := range decoded.records {
				allRecords = append(allRecords, pkdbspfreshbulk.SortedRunRecord{Key: bytes.Clone(record.Key), Value: bytes.Clone(record.Value)})
			}
			return nil
		}))

	realStore, err := openSPFreshBulkRealTiKVStore(tikvPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, realStore.Close()) }()

	originalConfig := config.GetGlobalConfig()
	integrationConfig := *originalConfig
	integrationConfig.Store = "tikv"
	integrationConfig.TempDir = t.TempDir()
	_, _, integrationConfig.KeyspaceName, err = tikvconfig.ParsePath(tikvPath)
	require.NoError(t, err)
	config.StoreGlobalConfig(&integrationConfig)
	defer config.StoreGlobalConfig(originalConfig)
	originalMemRoot := ingest.LitMemRoot
	originalDiskRoot := ingest.LitDiskRoot
	originalInitialized := ingest.LitInitialized
	defer func() {
		ingest.LitMemRoot = originalMemRoot
		ingest.LitDiskRoot = originalDiskRoot
		ingest.LitInitialized = originalInitialized
	}()
	require.True(t, ingest.InitGlobalLightningEnv(t.TempDir()))

	job := newSPFreshBulkTestJob()
	job.Type = model.ActionAddIndex
	job.RealStartTS = sourceSnapshot
	job.ReorgMeta.UseCloudStorage = true
	require.NoError(t, importSPFreshSortedRuns(ctx, realStore, job, run))
	defer run.staging.close()
	require.NotZero(t, run.bulkImportTS)

	beforeSnapshot := realStore.GetSnapshot(kv.NewVersion(run.bulkImportTS - 1))
	atSnapshot := realStore.GetSnapshot(kv.NewVersion(run.bulkImportTS))

	for _, record := range allRecords {
		key, err := realStore.GetCodec().DecodeKey(record.Key)
		require.NoError(t, err)
		_, err = beforeSnapshot.Get(ctx, kv.Key(key))
		require.ErrorIs(t, err, kv.ErrNotExist)
		actual, err := atSnapshot.Get(ctx, kv.Key(key))
		require.NoError(t, err)
		require.Equal(t, record.Value, actual)
	}
	t.Logf(
		"non-skip real Lightning TxnSstWriter/MultiIngest: bulk_import_ts=%d non_root=%d root=%d ts_minus_1_absent=%d ts_present_exact_values=%d order=non_root_then_root",
		run.bulkImportTS,
		run.nonRootRecordCount,
		run.rootRecordCount,
		len(allRecords),
		len(allRecords),
	)
}

// Preserve the actual API V2 codec: mockstorage.GetCodec constructs API V1.
// The underlying client is real TiKV; only the TiDB storage adapter is reused.
type spfreshBulkRealStorage interface {
	kv.Storage
	kv.StorageWithPD
	kv.EtcdBackend
	tikv.Storage
}

type spfreshBulkRealStore struct {
	spfreshBulkRealStorage
	codec tikv.Codec
}

func (s *spfreshBulkRealStore) GetCodec() tikv.Codec { return s.codec }

func openSPFreshBulkRealTiKVStore(path string) (kv.Storage, error) {
	pdAddresses, _, keyspaceName, err := tikvconfig.ParsePath(path)
	if err != nil {
		return nil, err
	}
	pdClient, err := pd.NewClient(pdAddresses, pd.SecurityOption{})
	if err != nil {
		return nil, err
	}
	safePointKV, err := tikv.NewEtcdSafePointKV(pdAddresses, nil)
	if err != nil {
		pdClient.Close()
		return nil, err
	}
	codecPDClient := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	if keyspaceName != "" {
		codecPDClient, err = tikv.NewCodecPDClientWithKeyspace(tikv.ModeTxn, pdClient, keyspaceName)
		if err != nil {
			_ = safePointKV.Close()
			pdClient.Close()
			return nil, err
		}
	}
	rpcClient := tikv.NewRPCClient(tikv.WithCodec(codecPDClient.GetCodec()))
	tikvStore, err := tikv.NewKVStore(
		fmt.Sprintf("spfresh-bulk-integration-%d", pdClient.GetClusterID(context.Background())),
		codecPDClient,
		safePointKV,
		rpcClient,
		tikv.WithPDHTTPClient("spfresh-bulk-integration", pdAddresses),
	)
	if err != nil {
		_ = safePointKV.Close()
		pdClient.Close()
		return nil, err
	}
	store, err := mockstorage.NewMockStorage(tikvStore)
	if err != nil {
		_ = tikvStore.Close()
		return nil, err
	}
	return &spfreshBulkRealStore{spfreshBulkRealStorage: store.(spfreshBulkRealStorage), codec: codecPDClient.GetCodec()}, nil
}
