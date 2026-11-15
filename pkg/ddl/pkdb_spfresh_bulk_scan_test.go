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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestSPFreshBulkReadRPCBatchRespectsVectorWidth(t *testing.T) {
	for _, dimension := range []uint32{1, 7, 768, 960, 1024, pkdbspfreshbulk.MaxDimension} {
		rows := spfreshBulkReadRPCBatchRows(dimension)
		require.Greater(t, rows, 1) // The client interprets <= 1 as its default.
		require.LessOrEqual(t, rows, spfreshBulkReadRPCMaxRows)
		require.LessOrEqual(t, rows*int(dimension)*4, spfreshBulkReadRPCVectorBytes)
	}
	require.Equal(t, 1024, spfreshBulkReadRPCBatchRows(768))
	require.Equal(t, 64, spfreshBulkReadRPCBatchRows(pkdbspfreshbulk.MaxDimension))
}

func TestSPFreshBulkSnapshotScanUsesOnlyBAndSQLVectorSemantics(t *testing.T) {
	store := newSPFreshBulkMockStore(t)
	physicalTable, indexInfo := newSPFreshBulkScanTestTable(t)
	originalZero := testSPFreshBulkVector(1, 0x3f800001)
	originalTwo := testSPFreshBulkVector(2, 0x40000001)
	writeSPFreshBulkTestRows(t, store, physicalTable.Meta(), map[int64][]float32{
		0: originalZero,
		2: originalTwo,
	})
	snapshot := currentSPFreshBulkTestVersion(t, store)
	writeSPFreshBulkTestRows(t, store, physicalTable.Meta(), map[int64][]float32{
		0: testSPFreshBulkVector(101, 0x40400001),
		3: testSPFreshBulkVector(3, 0x40800001),
	})

	run := newSPFreshBulkScanTestRun(indexInfo.ID, snapshot, "snapshot-scan")
	objectStore := storage.NewMemStorage()
	require.NoError(t, scanAndUploadSPFreshBaseFragmentsToStore(
		context.Background(),
		exprstatic.NewExprContext(),
		NewReorgContext(),
		store,
		physicalTable,
		run,
		objectStore, testSPFreshBulkScanLayout(t, 2),
		spfreshBulkScanConfig{
			fragmentTargetBytes: spfreshBulkBaseRecordEncodedBytes,
		},
	))

	records := readSPFreshBulkBaseRecords(t, objectStore, run)
	require.Len(t, records, 2)
	require.Equal(t, int64(0), records[0].Handle)
	require.Equal(t, int64(2), records[1].Handle)
	require.Equal(t, math.Float32bits(originalZero[0]), math.Float32bits(records[0].Vector[0]))
	require.Equal(t, math.Float32bits(originalZero[767]), math.Float32bits(records[0].Vector[767]))
	require.Equal(t, math.Float32bits(originalTwo[0]), math.Float32bits(records[1].Vector[0]))
	require.NotEqual(t, math.Float32bits(float32(101)), math.Float32bits(records[0].Vector[0]))
	require.Equal(t, uint64(2), run.sourceRows)
	require.Positive(t, run.sourceRawBytes)
	sourceFingerprint, err := pkdbspfreshbulk.FingerprintRecords(records)
	require.NoError(t, err)
	require.Equal(t, sourceFingerprint, run.sourceFingerprint)
	sampleRecords := readSPFreshBulkSampleRecords(t, objectStore, run)
	require.Equal(t, records, sampleRecords)
	require.Equal(t, uint64(2), run.sampleRows)
	require.Equal(t, sourceFingerprint, run.sampleFingerprint)
}

func TestSPFreshBulkScanUploadFragmentsConserveAndDoNotOverwrite(t *testing.T) {
	for _, role := range []string{"reader", "consumer", "uploader"} {
		for _, state := range []string{"configured", "active"} {
			gauge := metrics.SPFreshBulkPipelineWorkers.WithLabelValues(role, state)
			baseline := testutil.ToFloat64(gauge)
			t.Cleanup(func() { require.Equal(t, baseline, testutil.ToFloat64(gauge)) })
		}
	}
	store := newSPFreshBulkMockStore(t)
	physicalTable, indexInfo := newSPFreshBulkScanTestTable(t)
	writeSPFreshBulkTestRows(t, store, physicalTable.Meta(), map[int64][]float32{
		0: testSPFreshBulkVector(0, 0x3f800001),
		1: testSPFreshBulkVector(1, 0x40000001),
		2: testSPFreshBulkVector(2, 0x40400001),
	})
	snapshot := currentSPFreshBulkTestVersion(t, store)
	run := newSPFreshBulkScanTestRun(indexInfo.ID, snapshot, "scan-upload")
	run.distance = "cosine"
	objectStore := storage.NewMemStorage()
	config := spfreshBulkScanConfig{
		fragmentTargetBytes: 2 * spfreshBulkBaseRecordEncodedBytes,
	}
	require.NoError(t, scanAndUploadSPFreshBaseFragmentsToStore(
		context.Background(), exprstatic.NewExprContext(), NewReorgContext(),
		store, physicalTable, run, objectStore, testSPFreshBulkScanLayout(t, 2), config,
	))

	require.NotEmpty(t, run.baseFragmentURIs)
	require.Len(t, run.baseFragmentBytes, len(run.baseFragmentURIs))
	require.Len(t, run.baseFragmentSHA256, len(run.baseFragmentURIs))
	for i, uri := range run.baseFragmentURIs {
		require.Contains(t, uri, run.artifactPrefix+"/base/part-")
		if i > 0 {
			require.Less(t, run.baseFragmentURIs[i-1], uri)
		}
	}
	var recordCount uint64
	for ordinal, uri := range run.baseFragmentURIs {
		key := uri[len(run.artifactPrefix)+1:]
		encoded, err := objectStore.ReadFile(context.Background(), key)
		require.NoError(t, err)
		header, records, err := pkdbspfreshbulk.DecodeBaseFragment(encoded)
		require.NoError(t, err)
		require.Equal(t, run.buildID, header.BuildID)
		require.Equal(t, run.tableID, header.TableID)
		require.Equal(t, run.indexID, header.IndexID)
		require.Equal(t, run.sourceSnapshotTS, header.SourceSnapshotTS)
		require.Equal(t, run.distance, header.Distance)
		require.Equal(t, uint64(len(records)), header.RecordCount)
		require.Equal(t, records[0].Handle, header.MinHandle)
		require.Equal(t, records[len(records)-1].Handle, header.MaxHandle)
		if ordinal == 0 {
			require.Equal(t, int64(0), header.MinHandle)
		}
		if ordinal == len(run.baseFragmentURIs)-1 {
			require.Equal(t, int64(2), header.MaxHandle)
		}
		require.Equal(t, uint64(len(encoded)), run.baseFragmentBytes[ordinal])
		digest := sha256.Sum256(encoded)
		require.Equal(t, hex.EncodeToString(digest[:]), run.baseFragmentSHA256[ordinal])
		require.Equal(t, pkdbspfreshbulk.NewManifestRef(uri, encoded), run.baseFragmentRefs[ordinal])
		recordCount += uint64(len(records))
	}
	require.Equal(t, uint64(3), recordCount)
	var totalBytes uint64
	for _, size := range run.baseFragmentBytes {
		totalBytes += size
	}
	require.Equal(t, totalBytes, run.baseObjectBytes)
	require.Len(t, run.baseFragmentRefs, len(run.baseFragmentURIs))
	require.Equal(t, uint64(3), run.sampleRows)
	require.Len(t, run.sampleFragmentRefs, 2)
	sampleRecords := readSPFreshBulkSampleRecords(t, objectStore, run)
	require.Len(t, sampleRecords, 3)
	sampleFingerprint, err := pkdbspfreshbulk.FingerprintRecords(sampleRecords)
	require.NoError(t, err)
	require.Equal(t, sampleFingerprint, run.sampleFingerprint)

	firstKey := strings.TrimPrefix(run.baseFragmentURIs[0], run.artifactPrefix+"/")
	first, err := objectStore.ReadFile(context.Background(), firstKey)
	require.NoError(t, err)
	retryRun := newSPFreshBulkScanTestRun(indexInfo.ID, snapshot, "scan-upload")
	err = scanAndUploadSPFreshBaseFragmentsToStore(
		context.Background(), exprstatic.NewExprContext(), NewReorgContext(),
		store, physicalTable, retryRun, objectStore, testSPFreshBulkScanLayout(t, 2), config,
	)
	require.ErrorContains(t, err, "already exists")
	stored, err := objectStore.ReadFile(context.Background(), firstKey)
	require.NoError(t, err)
	require.True(t, bytes.Equal(first, stored))
}

// Exercise row decoding, arena growth, and fragment ownership together. Stored
// primary keys and defaults are absent from the physical row.
func TestSPFreshBulkStoredSnapshotScanPreservesRowsAcrossFragments(t *testing.T) {
	store := newSPFreshBulkMockStore(t)
	info, index := newSPFreshBulkTestTable()
	info.State = model.StatePublic
	index.State = model.StateWriteReorganization
	blobType := types.NewFieldType(mysql.TypeBlob)
	defaultType := types.NewFieldType(mysql.TypeLonglong)
	info.Columns = append(info.Columns,
		&model.ColumnInfo{ID: 3, Name: pmodel.NewCIStr("payload"), Offset: 2, State: model.StatePublic, FieldType: *blobType},
		&model.ColumnInfo{ID: 4, Name: pmodel.NewCIStr("fallback"), Offset: 3, State: model.StatePublic, FieldType: *defaultType, OriginDefaultValue: int64(42)},
	)
	index.VectorInfo.StoredColumnIDs = []int64{1, 3, 4}
	tbl := tables.MockTableFromMeta(info).(table.PhysicalTable)
	payloads := []string{"first", strings.Repeat("growth", 1024), "boundary", strings.Repeat("last", 2048)}
	txn, err := store.Begin()
	require.NoError(t, err)
	encoder := rowcodec.Encoder{Enable: true}
	expected := make([]pkdbspfreshbulk.BaseFragmentRecord, len(payloads))
	for i, payload := range payloads {
		handle := int64(i + 1)
		values := testSPFreshBulkVector(handle, uint32(i)+0x3f800000)
		vector, err := types.CreateVectorFloat32(values)
		require.NoError(t, err)
		raw, err := tablecodec.EncodeRow(time.UTC,
			[]types.Datum{types.NewVectorFloat32Datum(vector), types.NewBytesDatum([]byte(payload))},
			[]int64{2, 3}, nil, nil, nil, &encoder)
		require.NoError(t, err)
		require.NoError(t, txn.Set(tablecodec.EncodeRowKeyWithHandle(info.ID, kv.IntHandle(handle)), raw))
		sidecar, err := tablecodec.EncodeOldRow(time.UTC,
			[]types.Datum{types.NewIntDatum(handle), types.NewBytesDatum([]byte(payload)), types.NewIntDatum(42)},
			[]int64{1, 3, 4}, nil, nil)
		require.NoError(t, err)
		digest, err := pkdbspfreshbulk.RecordDigestWithPayload(handle, values, sidecar)
		require.NoError(t, err)
		expected[i] = pkdbspfreshbulk.BaseFragmentRecord{Handle: handle, Vector: values, StoredPayload: sidecar, SourceDigestSHA256: digest}
	}
	require.NoError(t, txn.Commit(context.Background()))
	previousProcs := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })
	for _, procs := range []int{1, 7} {
		runtime.GOMAXPROCS(procs)
		run := newSPFreshBulkScanTestRun(index.ID, currentSPFreshBulkTestVersion(t, store), fmt.Sprintf("stored-scan-%d", procs))
		objectStore := storage.NewMemStorage()
		require.NoError(t, scanAndUploadSPFreshBaseFragmentsToStore(context.Background(), exprstatic.NewExprContext(), NewReorgContext(),
			store, tbl, run, objectStore, testSPFreshBulkScanLayout(t, 2), spfreshBulkScanConfig{fragmentTargetBytes: 13 * 1024}))
		require.Greater(t, len(run.baseFragmentURIs), 1)
		require.Equal(t, expected, readSPFreshBulkBaseRecords(t, objectStore, run))
		fingerprint, err := pkdbspfreshbulk.FingerprintRecords(expected)
		require.NoError(t, err)
		require.Equal(t, fingerprint, run.sourceFingerprint)
		sample := readSPFreshBulkSampleRecords(t, objectStore, run)
		require.Len(t, sample, len(expected))
		for i, row := range sample {
			require.Empty(t, row.StoredPayload)
			require.Equal(t, expected[i].SourceDigestSHA256, row.SourceDigestSHA256)
		}
	}
}

func TestSPFreshBulkStoredEncodingRetainsArenaPrefixWhenEncoderReservesMoreThanItWrites(t *testing.T) {
	info, index := newSPFreshBulkTestTable()
	info.State = model.StatePublic
	index.State = model.StateWriteReorganization
	index.VectorInfo.StoredColumnIDs = []int64{1}
	tbl := tables.MockTableFromMeta(info)
	encoder, err := newSPFreshBulkStoredEncoder(tbl, index, tbl.Cols()[1], time.UTC)
	require.NoError(t, err)
	// A small integer sidecar fits in the tail, but the codec reserves space
	// for maximum-length integer encodings and therefore replaces that tail.
	arena := make([]byte, 4, 12)
	copy(arena, "kept")
	payload, arena, err := encoder.encodeInto(arena, []types.Datum{types.NewIntDatum(7)}, time.UTC)
	require.NoError(t, err)
	expected, err := tablecodec.EncodeOldRow(time.UTC, []types.Datum{types.NewIntDatum(7)}, []int64{1}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, expected, payload)
	require.Equal(t, append([]byte("kept"), expected...), arena)
}

func TestSPFreshBulkBottomKSampleIsBoundedAndOrderIndependent(t *testing.T) {
	for _, dimension := range []int{768, 960, 1024} {
		for _, limit := range []int{7, 1031} {
			t.Run(fmt.Sprintf("d%d/k%d", dimension, limit), func(t *testing.T) {
				records := make([]pkdbspfreshbulk.BaseFragmentRecord, 2*limit+1)
				for i := range records {
					vector := make([]float32, dimension)
					vector[0] = float32(i)
					vector[dimension-1] = math.Float32frombits(uint32(i) + 0x3f800000)
					records[i] = pkdbspfreshbulk.BaseFragmentRecord{Handle: int64(i - limit), Vector: vector}
				}
				forward, err := newSPFreshBulkBottomKSample(limit, dimension)
				require.NoError(t, err)
				reverse, err := newSPFreshBulkBottomKSample(limit, dimension)
				require.NoError(t, err)
				for _, record := range records {
					digest, err := pkdbspfreshbulk.RecordDigest(record.Handle, record.Vector)
					require.NoError(t, err)
					forward.AddWithDigest(record, digest)
				}
				for i := len(records) - 1; i >= 0; i-- {
					record := records[i]
					digest, err := pkdbspfreshbulk.RecordDigest(record.Handle, record.Vector)
					require.NoError(t, err)
					reverse.AddWithDigest(record, digest)
				}
				selected := forward.Records(limit)
				require.Len(t, selected, limit)
				require.Equal(t, selected, reverse.Records(limit))
				for i, record := range selected {
					require.Equal(t, records[int(record.Handle)+limit].Vector, record.Vector)
					if i > 0 {
						require.Less(t, selected[i-1].Handle, record.Handle)
					}
				}
			})
		}
	}
}

func TestSPFreshBulkSampleRetainsStoredSourceDigests(t *testing.T) {
	records := make([]pkdbspfreshbulk.BaseFragmentRecord, 32)
	for i := range records {
		records[i] = pkdbspfreshbulk.BaseFragmentRecord{
			Handle: int64(i), Vector: testSPFreshBulkVector(int64(i), uint32(i)+0x3f800000),
			StoredPayload: []byte{byte(i), 1, 2, 3},
		}
		digest, err := pkdbspfreshbulk.RecordDigestWithPayload(records[i].Handle, records[i].Vector, records[i].StoredPayload)
		require.NoError(t, err)
		records[i].SourceDigestSHA256 = digest
	}
	sample, err := newSPFreshBulkBottomKSample(7, int(pkdbspfreshbulk.DefaultDimension))
	require.NoError(t, err)
	for _, record := range records {
		sample.AddWithDigest(record, record.SourceDigestSHA256)
	}
	projected, digests := sample.RecordsWithDigests(7)
	var expected pkdbspfreshbulk.FingerprintAccumulator
	for i, record := range projected {
		require.Empty(t, record.StoredPayload)
		require.Equal(t, records[record.Handle].SourceDigestSHA256, digests[i])
		vectorDigest, err := pkdbspfreshbulk.RecordDigest(record.Handle, record.Vector)
		require.NoError(t, err)
		require.NotEqual(t, vectorDigest, digests[i])
		require.NoError(t, expected.AddDigest(digests[i]))
	}
	objectStore := storage.NewMemStorage()
	run := newSPFreshBulkScanTestRun(4, 123, "sample-source-digests")
	refs, _, _, fingerprint, err := writeSPFreshBulkSampleFragmentsWithDigests(
		context.Background(), objectStore, run, 16*spfreshBulkBaseRecordEncodedBytes, projected, digests, nil,
	)
	require.NoError(t, err)
	require.Equal(t, expected.Fingerprint(), fingerprint)
	run.sampleFragmentRefs = refs
	decoded := readSPFreshBulkSampleRecords(t, objectStore, run)
	for i, record := range decoded {
		require.Empty(t, record.StoredPayload)
		require.Equal(t, digests[i], record.SourceDigestSHA256)
	}

}

func TestSPFreshBulkBuildRequestUsesRunDistanceAndStepContextCancellation(t *testing.T) {
	run := newSPFreshBulkScanTestRun(4, 123, "cosine-request")
	run.distance = "cosine"
	run.sourceRows = 3
	run.baseObjectBytes = 4096
	run.baseFragmentURIs = []string{run.artifactPrefix + "/base/part-00000.spfmvp"}
	run.baseFragmentRefs = []pkdbspfreshbulk.ManifestRef{pkdbspfreshbulk.NewManifestRef(run.baseFragmentURIs[0], []byte("source"))}

	requestStarted := make(chan struct{})
	requestCanceled := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		var body remote.SPFreshBulkBuildRequest
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		require.Equal(t, run.distance, body.Distance)
		close(requestStarted)
		<-request.Context().Done()
		requestCanceled <- request.Context().Err()
	}))
	defer server.Close()
	run.workerURL = server.URL
	client := server.Client()
	client.Timeout = time.Millisecond
	stepCtx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- buildAndWriteSPFreshSortedRuns(stepCtx, client, run)
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		require.FailNow(t, "bulk build request did not reach worker")
	}
	select {
	case err := <-result:
		require.FailNow(t, "copied client used original timeout", err)
	case <-time.After(10 * time.Millisecond):
	}
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	require.ErrorIs(t, <-requestCanceled, context.Canceled)
	require.Equal(t, time.Millisecond, client.Timeout)
}

func TestSPFreshBulkDDLScanFailureStopsBeforeWorkerAndPublish(t *testing.T) {
	var calls []string
	err := runSPFreshBulkReorgWithStages(
		context.Background(),
		&spfreshBulkRunContext{},
		spfreshBulkStages{
			building: func(context.Context, *spfreshBulkRunContext) error {
				calls = append(calls, "building")
				return nil
			},
			scanAndUpload: func(context.Context, *spfreshBulkRunContext) error {
				calls = append(calls, "scan_upload")
				return context.Canceled
			},
			buildAndWriteRuns: func(context.Context, *spfreshBulkRunContext) error {
				calls = append(calls, "unexpected_worker")
				return nil
			},
			importRuns: func(context.Context, *spfreshBulkRunContext) error {
				calls = append(calls, "unexpected_import")
				return nil
			},
			verify:  func(context.Context, *spfreshBulkRunContext) error { return nil },
			publish: func(context.Context, *spfreshBulkRunContext) error { return nil },
		},
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{"building", "scan_upload"}, calls)
}

func TestSPFreshBulkSnapshotMinIOWorkerIntegration(t *testing.T) {
	minioURI := os.Getenv("SPFRESH_MVP_MINIO_URI")
	workerURL := os.Getenv("SPFRESH_MVP_WORKER_URL")
	if minioURI == "" || workerURL == "" {
		t.Skip("SPFRESH_MVP_MINIO_URI and SPFRESH_MVP_WORKER_URL are required")
	}
	runID, artifactPrefix, err := parseSPFreshBulkStorageURI(minioURI)
	require.NoError(t, err)
	store := newSPFreshBulkMockStore(t)
	physicalTable, indexInfo := newSPFreshBulkScanTestTable(t)
	rows := make(map[int64][]float32, 400)
	for handle := int64(1); handle <= 400; handle++ {
		rows[handle] = testSPFreshBulkVector(handle, uint32(handle)+0x3f800000)
	}
	writeSPFreshBulkTestRows(t, store, physicalTable.Meta(), rows)
	snapshot := currentSPFreshBulkTestVersion(t, store)

	run := newSPFreshBulkScanTestRun(indexInfo.ID, snapshot, runID)
	run.cloudStorageURI = minioURI
	run.artifactPrefix = artifactPrefix
	run.baseFragmentURIs = nil
	run.workerURL = workerURL
	run.nonRootRunURI = artifactPrefix + "/runs/non-root.spfmvp"
	run.rootRunURI = artifactPrefix + "/runs/root.spfmvp"
	backend, err := storage.ParseBackend(minioURI, nil)
	require.NoError(t, err)
	externalStorage, err := storage.NewWithDefaultOpt(context.Background(), backend)
	require.NoError(t, err)
	defer externalStorage.Close()
	require.NoError(t, initializeSPFreshBulkLifecycle(context.Background(), externalStorage))

	require.NoError(t, scanAndUploadSPFreshBaseFragmentsToStore(
		context.Background(), exprstatic.NewExprContext(), NewReorgContext(),
		store, physicalTable, run, externalStorage, testSPFreshBulkScanLayout(t, 1),
		spfreshBulkScanConfig{
			fragmentTargetBytes: 128 * spfreshBulkBaseRecordEncodedBytes,
		},
	))
	require.NoError(t, buildAndWriteSPFreshSortedRuns(context.Background(), http.DefaultClient, run))
	require.Equal(t, uint64(400), run.sourceRows)
	require.Equal(t, uint64(400), run.leafEntryCount)
	require.NotZero(t, run.nonRootRecordCount)
	require.NotZero(t, run.rootRecordCount)

	nonRootBytes, err := externalStorage.ReadFile(context.Background(), "runs/non-root.spfmvp")
	require.NoError(t, err)
	nonRootHeader, nonRootRecords, err := pkdbspfreshbulk.DecodeSortedRun(nonRootBytes)
	require.NoError(t, err)
	require.Equal(t, "non_root", nonRootHeader.RunRole)
	require.Equal(t, run.nonRootRecordCount, uint64(len(nonRootRecords)))
	rootBytes, err := externalStorage.ReadFile(context.Background(), "runs/root.spfmvp")
	require.NoError(t, err)
	rootHeader, rootRecords, err := pkdbspfreshbulk.DecodeSortedRun(rootBytes)
	require.NoError(t, err)
	require.Equal(t, "root", rootHeader.RunRole)
	require.Equal(t, run.rootRecordCount, uint64(len(rootRecords)))
	t.Logf("non-skip conservation: rows=%d base_bytes=%d fragments=%d leaf_entries=%d", run.sourceRows, run.baseObjectBytes, len(run.baseFragmentURIs), run.leafEntryCount)
}

func newSPFreshBulkScanTestTable(t testing.TB) (table.PhysicalTable, *model.IndexInfo) {
	tblInfo, indexInfo := newSPFreshBulkTestTable()
	tblInfo.State = model.StatePublic
	indexInfo.State = model.StateWriteReorganization
	tbl := tables.MockTableFromMeta(tblInfo)
	physicalTable, ok := tbl.(table.PhysicalTable)
	require.True(t, ok)
	return physicalTable, indexInfo
}

func newSPFreshBulkMockStore(t testing.TB) kv.Storage {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func newSPFreshBulkScanTestRun(indexID int64, snapshot uint64, runID string) *spfreshBulkRunContext {
	artifactPrefix := "s3://spfresh-mvp/" + runID
	return &spfreshBulkRunContext{
		buildID:          runID,
		runID:            runID,
		sourceSnapshotTS: snapshot,
		keyspaceID:       1,
		tableID:          3,
		indexID:          indexID,
		dimension:        pkdbspfreshbulk.DefaultDimension,
		distance:         "l2",
		algorithmSeed:    spfreshBulkAlgorithmSeed,
		targetOccupancy:  spfreshBulkTargetOccupancy,
		minPartitionSize: pkdbspfreshbulk.DefaultMinPartitionSize,
		maxPartitionSize: pkdbspfreshbulk.DefaultMaxPartitionSize,
		artifactPrefix:   artifactPrefix,
		nonRootRunURI:    artifactPrefix + "/runs/non-root.spfmvp",
		rootRunURI:       artifactPrefix + "/runs/root.spfmvp",
	}
}

func writeSPFreshBulkTestRows(
	t testing.TB,
	store kv.Storage,
	tblInfo *model.TableInfo,
	rows map[int64][]float32,
) {
	txn, err := store.Begin()
	require.NoError(t, err)
	encoder := rowcodec.Encoder{Enable: true}
	vectorColumn := tblInfo.Columns[1]
	for handle, values := range rows {
		vector, err := types.CreateVectorFloat32(values)
		require.NoError(t, err)
		row, err := tablecodec.EncodeRow(
			time.UTC,
			[]types.Datum{types.NewVectorFloat32Datum(vector)},
			[]int64{vectorColumn.ID},
			nil,
			nil,
			nil,
			&encoder,
		)
		require.NoError(t, err)
		require.NoError(t, txn.Set(
			tablecodec.EncodeRowKeyWithHandle(tblInfo.ID, kv.IntHandle(handle)),
			row,
		))
	}
	require.NoError(t, txn.Commit(context.Background()))
}

func currentSPFreshBulkTestVersion(t testing.TB, store kv.Storage) uint64 {
	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	return version.Ver
}

func testSPFreshBulkVector(marker int64, tailBits uint32) []float32 {
	vector := make([]float32, pkdbspfreshbulk.Dimension)
	vector[0] = float32(marker)
	vector[1] = float32(marker) / 10
	vector[len(vector)-1] = math.Float32frombits(tailBits)
	return vector
}

func readSPFreshBulkBaseRecords(
	t *testing.T,
	objectStore storage.ExternalStorage,
	run *spfreshBulkRunContext,
) []pkdbspfreshbulk.BaseFragmentRecord {
	var records []pkdbspfreshbulk.BaseFragmentRecord
	for _, uri := range run.baseFragmentURIs {
		key := uri[len(run.artifactPrefix)+1:]
		encoded, err := objectStore.ReadFile(context.Background(), key)
		require.NoError(t, err)
		_, fragmentRecords, err := pkdbspfreshbulk.DecodeBaseFragment(encoded)
		require.NoError(t, err)
		records = append(records, fragmentRecords...)
	}
	return records
}

func readSPFreshBulkSampleRecords(
	t *testing.T,
	objectStore storage.ExternalStorage,
	run *spfreshBulkRunContext,
) []pkdbspfreshbulk.BaseFragmentRecord {
	var records []pkdbspfreshbulk.BaseFragmentRecord
	for _, reference := range run.sampleFragmentRefs {
		key := reference.URI[len(run.artifactPrefix)+1:]
		encoded, err := objectStore.ReadFile(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, reference, pkdbspfreshbulk.NewManifestRef(reference.URI, encoded))
		_, fragmentRecords, err := pkdbspfreshbulk.DecodeBaseFragment(encoded)
		require.NoError(t, err)
		records = append(records, fragmentRecords...)
	}
	return records
}

func sampleHandles(records []pkdbspfreshbulk.BaseFragmentRecord) []int64 {
	handles := make([]int64, len(records))
	for i := range records {
		handles[i] = records[i].Handle
	}
	return handles
}

// A range boundary may equal a row key or fall in a gap. The inclusive
// underlying snapshot iterator must not duplicate a boundary row.
func TestSPFreshBulkRangeReadsPreserveSnapshotAndOrder(t *testing.T) {
	store := newSPFreshBulkMockStore(t)
	tbl, _ := newSPFreshBulkScanTestTable(t)
	rows := map[int64][]float32{}
	for _, h := range []int64{-9, -1, 0, 2, 9} {
		rows[h] = testSPFreshBulkVector(h, 0x3f800001)
	}
	writeSPFreshBulkTestRows(t, store, tbl.Meta(), rows)
	version := currentSPFreshBulkTestVersion(t, store)
	writeSPFreshBulkTestRows(t, store, tbl.Meta(), map[int64][]float32{1: testSPFreshBulkVector(1, 0x3f800001)})
	key := func(h int64) kv.Key { return tablecodec.EncodeRowKeyWithHandle(tbl.GetPhysicalID(), kv.IntHandle(h)) }
	ranges := []kv.KeyRange{{StartKey: tbl.RecordPrefix(), EndKey: key(0)}, {StartKey: key(0), EndKey: key(3)}, {StartKey: key(3), EndKey: tbl.RecordPrefix().PrefixNext()}}
	var got []int64
	_, _, _, err := readSPFreshBulkRanges(context.Background(), NewReorgContext(), store, tbl.RecordPrefix(), &spfreshBulkRunContext{dimension: pkdbspfreshbulk.DefaultDimension, sourceSnapshotTS: version}, 2, spfreshBulkReadBatchBytes, spfreshTestRangeSource(ranges),
		spfreshTestRangeCallbacks(func(h kv.Handle, _ kv.Key, _ []byte) (bool, error) { got = append(got, h.IntValue()); return true, nil }))
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{-9, -1, 0, 2, 9}, got)
}

func TestSPFreshBulkRangeReadsJoinOnConsumerFailure(t *testing.T) {
	store := newSPFreshBulkMockStore(t)
	tbl, _ := newSPFreshBulkScanTestTable(t)
	rows := map[int64][]float32{}
	for h := int64(0); h < 3000; h++ {
		rows[h] = testSPFreshBulkVector(h, 0x3f800001)
	}
	writeSPFreshBulkTestRows(t, store, tbl.Meta(), rows)
	version := currentSPFreshBulkTestVersion(t, store)
	middle := tablecodec.EncodeRowKeyWithHandle(tbl.GetPhysicalID(), kv.IntHandle(1500))
	ranges := []kv.KeyRange{{StartKey: tbl.RecordPrefix(), EndKey: middle}, {StartKey: middle, EndKey: tbl.RecordPrefix().PrefixNext()}}
	expected := errors.New("stop consuming")
	var calls atomic.Int32
	_, _, _, err := readSPFreshBulkRanges(context.Background(), NewReorgContext(), store, tbl.RecordPrefix(), &spfreshBulkRunContext{dimension: pkdbspfreshbulk.DefaultDimension, sourceSnapshotTS: version}, 2, spfreshBulkReadBatchBytes, spfreshTestRangeSource(ranges),
		spfreshTestRangeCallbacks(func(kv.Handle, kv.Key, []byte) (bool, error) { calls.Add(1); return false, expected }))
	require.ErrorIs(t, err, expected)
	require.GreaterOrEqual(t, calls.Load(), int32(1))
	require.LessOrEqual(t, calls.Load(), int32(2))
	stoppedCalls := calls.Load()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, err = readSPFreshBulkRanges(ctx, NewReorgContext(), store, tbl.RecordPrefix(), &spfreshBulkRunContext{dimension: pkdbspfreshbulk.DefaultDimension, sourceSnapshotTS: version}, 2, spfreshBulkReadBatchBytes, spfreshTestRangeSource(ranges),
		spfreshTestRangeCallbacks(func(kv.Handle, kv.Key, []byte) (bool, error) { calls.Add(1); return true, nil }))
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, stoppedCalls, calls.Load())

	// A fresh attempt after cancellation must preserve every row's bytes
	// across multiple batches while the other range is reading concurrently.
	column := tbl.Meta().Columns[1]
	decoder := rowcodec.NewDatumMapDecoder([]rowcodec.ColInfo{{ID: column.ID, Ft: &column.FieldType}}, time.UTC)
	decoded := make(map[int64]types.Datum)
	var next int64
	_, _, _, err = readSPFreshBulkRanges(context.Background(), NewReorgContext(), store, tbl.RecordPrefix(), &spfreshBulkRunContext{dimension: pkdbspfreshbulk.DefaultDimension, sourceSnapshotTS: version}, 2, spfreshBulkReadBatchBytes, spfreshTestRangeSource(ranges),
		spfreshTestRangeCallbacks(func(h kv.Handle, _ kv.Key, raw []byte) (bool, error) {
			datum, err := decodeSPFreshBulkVector(decoder, decoded, column.ID, raw)
			if err != nil {
				return false, err
			}
			if !slices.Equal(rows[h.IntValue()], datum.GetVectorFloat32().Elements()) {
				return false, fmt.Errorf("source vector for handle %d differs", h.IntValue())
			}
			next++
			return true, nil
		}))
	require.NoError(t, err)
	require.EqualValues(t, len(rows), next)
}

// Returns another bounded metadata page as soon as the prior range is consumed.
func spfreshTestRangeSource(ranges []kv.KeyRange) func(context.Context) (kv.KeyRange, error) {
	return func(ctx context.Context) (kv.KeyRange, error) {
		if err := ctx.Err(); err != nil {
			return kv.KeyRange{}, err
		}
		if len(ranges) == 0 {
			return kv.KeyRange{}, io.EOF
		}
		next := ranges[0]
		ranges = ranges[1:]
		return next, nil
	}
}

func TestSPFreshBulkRangeReadsRefillCompletedSlots(t *testing.T) {
	store := newSPFreshBulkMockStore(t)
	tbl, _ := newSPFreshBulkScanTestTable(t)
	writeSPFreshBulkTestRows(t, store, tbl.Meta(), map[int64][]float32{
		0: testSPFreshBulkVector(0, 0x3f800001),
		1: testSPFreshBulkVector(1, 0x3f800001),
		2: testSPFreshBulkVector(2, 0x3f800001),
	})
	key := func(h int64) kv.Key { return tablecodec.EncodeRowKeyWithHandle(tbl.GetPhysicalID(), kv.IntHandle(h)) }
	ranges := []kv.KeyRange{{StartKey: tbl.RecordPrefix(), EndKey: key(1)},
		{StartKey: key(1), EndKey: key(2)}, {StartKey: key(2), EndKey: tbl.RecordPrefix().PrefixNext()}}
	nextRange := spfreshTestRangeSource(ranges)
	requested := 0
	source := func(ctx context.Context) (kv.KeyRange, error) {
		r, err := nextRange(ctx)
		if err == nil {
			requested++
		}
		return r, err
	}
	var handles []int64
	_, _, _, err := readSPFreshBulkRanges(context.Background(), NewReorgContext(), store,
		tbl.RecordPrefix(), &spfreshBulkRunContext{dimension: pkdbspfreshbulk.DefaultDimension, sourceSnapshotTS: currentSPFreshBulkTestVersion(t, store)},
		2, spfreshBulkReadBatchBytes, source, spfreshTestRangeCallbacks(func(h kv.Handle, _ kv.Key, _ []byte) (bool, error) {
			handles = append(handles, h.IntValue())
			return true, nil
		}))
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{0, 1, 2}, handles)
	require.Equal(t, 3, requested)
}

func TestSPFreshBulkLogicalRangesPreserveSparseSnapshotAndExtremeHandles(t *testing.T) {
	store := newSPFreshBulkMockStore(t)
	tbl, _ := newSPFreshBulkScanTestTable(t)
	handles := []int64{math.MinInt64, -9, -1, 0, 2, 9, math.MaxInt64}
	rows := make(map[int64][]float32, len(handles))
	for _, handle := range handles {
		rows[handle] = testSPFreshBulkVector(handle, 0x3f800001)
	}
	writeSPFreshBulkTestRows(t, store, tbl.Meta(), rows)
	run := &spfreshBulkRunContext{dimension: pkdbspfreshbulk.DefaultDimension, sourceSnapshotTS: currentSPFreshBulkTestVersion(t, store)}
	// A later committed row must affect neither endpoint planning nor consumption.
	writeSPFreshBulkTestRows(t, store, tbl.Meta(), map[int64][]float32{3: testSPFreshBulkVector(3, 0x40000001)})
	key := func(h int64) kv.Key { return tablecodec.EncodeRowKeyWithHandle(tbl.GetPhysicalID(), kv.IntHandle(h)) }
	for _, tc := range []struct {
		name     string
		keyRange kv.KeyRange
		expected []int64
	}{
		{"full", kv.KeyRange{StartKey: tbl.RecordPrefix(), EndKey: tbl.RecordPrefix().PrefixNext()}, handles},
		{"bounded", kv.KeyRange{StartKey: key(-2), EndKey: key(9)}, []int64{-1, 0, 2}},
		{"singleton", kv.KeyRange{StartKey: key(8), EndKey: key(10)}, []int64{9}},
		{"empty", kv.KeyRange{StartKey: key(3), EndKey: key(8)}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, parts := range []int{1, 3, 4, 8} {
				ranges, probeRows, probeBytes, err := splitSPFreshBulkReadRange(context.Background(), NewReorgContext(), store, tbl, run, tc.keyRange, parts)
				require.NoError(t, err)
				require.LessOrEqual(t, len(ranges), parts)
				if parts == 1 {
					require.Zero(t, probeRows)
					require.Zero(t, probeBytes)
				} else if len(tc.expected) > 0 {
					require.GreaterOrEqual(t, probeRows, uint64(2))
					require.LessOrEqual(t, probeRows, uint64(4))
					require.Positive(t, probeBytes)
				}
				if len(ranges) > 0 {
					require.Equal(t, tc.keyRange.StartKey, ranges[0].StartKey)
					require.Equal(t, tc.keyRange.EndKey, ranges[len(ranges)-1].EndKey)
				}
				for i, r := range ranges {
					require.Less(t, r.StartKey.Cmp(r.EndKey), 0)
					if i > 0 {
						require.Equal(t, ranges[i-1].EndKey, r.StartKey)
					}
				}
				var got []int64
				_, _, _, err = readSPFreshBulkRanges(context.Background(), NewReorgContext(), store, tbl.RecordPrefix(), run, 2, spfreshBulkReadBatchBytes, spfreshTestRangeSource(ranges),
					spfreshTestRangeCallbacks(func(h kv.Handle, _ kv.Key, _ []byte) (bool, error) { got = append(got, h.IntValue()); return true, nil }))
				require.NoError(t, err)
				require.ElementsMatch(t, tc.expected, got)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, err := splitSPFreshBulkReadRange(ctx, NewReorgContext(), store, tbl, run,
		kv.KeyRange{StartKey: tbl.RecordPrefix(), EndKey: tbl.RecordPrefix().PrefixNext()}, 4)
	require.ErrorIs(t, err, context.Canceled)
}

// These tests share collectors/decoders. Serialize only their assertions;
// production and the dedicated parallelism test own independent range state.
func spfreshTestRangeCallbacks(fn recordIterFunc) spfreshBulkRangeConsumerFactory {
	var mu sync.Mutex
	return func(context.Context) (spfreshBulkRangeCallbacks, error) {
		return spfreshBulkRangeCallbacks{
			row: func(_ int, h kv.Handle, value []byte) (bool, error) {
				mu.Lock()
				defer mu.Unlock()
				return fn(h, nil, value)
			},
			finish: func(int) error { return nil },
		}, nil
	}
}

type spfreshBlockedSourceStore struct {
	kv.Storage
	ctx     context.Context
	first   kv.Key
	release <-chan struct{}
}

func (s *spfreshBlockedSourceStore) GetSnapshot(v kv.Version) kv.Snapshot {
	return &spfreshBlockedSourceSnapshot{Snapshot: s.Storage.GetSnapshot(v), source: s}
}

type spfreshBlockedSourceSnapshot struct {
	kv.Snapshot
	source *spfreshBlockedSourceStore
}

func (s *spfreshBlockedSourceSnapshot) Iter(start, end kv.Key) (kv.Iterator, error) {
	if start.Cmp(s.source.first) == 0 {
		select {
		case <-s.source.release:
		case <-s.source.ctx.Done():
			return nil, s.source.ctx.Err()
		}
	}
	return s.Snapshot.Iter(start, end)
}

func TestSPFreshBulkRangeReadsConsumeReadyRangeBeforeBlockedPredecessor(t *testing.T) {
	store := newSPFreshBulkMockStore(t)
	tbl, _ := newSPFreshBulkScanTestTable(t)
	rows := make(map[int64][]float32)
	for h := int64(0); h < 3000; h++ {
		rows[h] = testSPFreshBulkVector(h, 0x3f800001)
	}
	writeSPFreshBulkTestRows(t, store, tbl.Meta(), rows)
	version := currentSPFreshBulkTestVersion(t, store)
	middle := tablecodec.EncodeRowKeyWithHandle(tbl.GetPhysicalID(), kv.IntHandle(1500))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	release := make(chan struct{})
	blocked := &spfreshBlockedSourceStore{Storage: store, ctx: ctx, first: tbl.RecordPrefix(), release: release}
	ranges := []kv.KeyRange{{StartKey: tbl.RecordPrefix(), EndKey: middle}, {StartKey: middle, EndKey: tbl.RecordPrefix().PrefixNext()}}
	next := int64(0)
	first := true
	finished := make(map[int]bool)
	last := make(map[int]int64)
	var mu sync.Mutex
	callbacks := func(context.Context) (spfreshBulkRangeCallbacks, error) {
		return spfreshBulkRangeCallbacks{
			row: func(ordinal int, h kv.Handle, _ []byte) (bool, error) {
				mu.Lock()
				defer mu.Unlock()
				if first {
					close(release)
					first = false
					if ordinal != 1 {
						return false, errors.New("ready range must unblock its predecessor")
					}
				}
				if previous, ok := last[ordinal]; ok && h.IntValue() <= previous {
					return false, fmt.Errorf("range %d handles are not increasing", ordinal)
				}
				last[ordinal] = h.IntValue()
				next++
				return true, nil
			},
			finish: func(ordinal int) error {
				mu.Lock()
				defer mu.Unlock()
				if finished[ordinal] {
					return fmt.Errorf("range %d finished more than once", ordinal)
				}
				finished[ordinal] = true
				return nil
			},
		}, nil
	}
	// A small queue stays bounded while ready ranges make progress.
	_, _, _, err := readSPFreshBulkRanges(ctx, NewReorgContext(), blocked, tbl.RecordPrefix(),
		&spfreshBulkRunContext{dimension: pkdbspfreshbulk.DefaultDimension, sourceSnapshotTS: version}, 2, 64, spfreshTestRangeSource(ranges), callbacks)
	require.NoError(t, err)
	require.EqualValues(t, 3000, next)
	require.Len(t, finished, 2)
}

func testSPFreshBulkScanLayout(t testing.TB, shards uint32) spfreshBulkLayout {
	t.Helper()
	layout, err := planSPFreshBulkLayout(spfreshBulkLayoutEstimate{}, shards, pkdbspfreshbulk.DefaultDimension, nil)
	require.NoError(t, err)
	return layout
}

// Both automatic and explicit plans must sample in the first scan. No source
// object is opened/read for sampling after it has been uploaded.
func TestSPFreshBulkScanLayoutBranchesAvoidSourceObjectReads(t *testing.T) {
	store := newSPFreshBulkMockStore(t)
	tbl, index := newSPFreshBulkScanTestTable(t)
	input := make(map[int64][]float32)
	for i := int64(0); i < 5; i++ {
		input[i] = testSPFreshBulkVector(i, 0x3f800000)
	}
	writeSPFreshBulkTestRows(t, store, tbl.Meta(), input)
	snapshot := currentSPFreshBulkTestVersion(t, store)
	for _, explicit := range []bool{false, true} {
		for _, shards := range []uint32{1, 3} {
			t.Run(fmt.Sprintf("explicit=%v/shards=%d", explicit, shards), func(t *testing.T) {
				layout, err := planSPFreshBulkLayout(spfreshBulkLayoutEstimate{}, shards, pkdbspfreshbulk.DefaultDimension, nil)
				require.NoError(t, err)
				layout.explicit = explicit
				run := newSPFreshBulkScanTestRun(index.ID, snapshot, "layout-branches")
				objectStore := &spfreshBulkCountingSourceReads{ExternalStorage: storage.NewMemStorage()}
				require.NoError(t, scanAndUploadSPFreshBaseFragmentsToStore(context.Background(), exprstatic.NewExprContext(), NewReorgContext(),
					store, tbl, run, objectStore, layout, spfreshBulkScanConfig{fragmentTargetBytes: 2 * spfreshBulkBaseRecordEncodedBytes}))
				require.Zero(t, objectStore.reads.Load())
				require.Equal(t, uint64(len(input)), run.sourceRows)
				require.Equal(t, shards, run.shardCount)
				if shards == 1 {
					require.Zero(t, run.sampleRows)
					require.Empty(t, run.sampleFragmentRefs)
				} else {
					require.Equal(t, run.sourceRows, run.sampleRows)
					require.Equal(t, run.sourceFingerprint, run.sampleFingerprint)
				}
				records := readSPFreshBulkBaseRecords(t, objectStore, run)
				require.Len(t, records, len(input))
				for _, record := range records {
					require.Equal(t, input[record.Handle], record.Vector)
				}
			})
		}
	}
}

type spfreshBulkCountingSourceReads struct {
	storage.ExternalStorage
	reads atomic.Uint64
}

func (s *spfreshBulkCountingSourceReads) ReadFile(ctx context.Context, name string) ([]byte, error) {
	if strings.HasPrefix(name, "base/") {
		s.reads.Add(1)
	}
	return s.ExternalStorage.ReadFile(ctx, name)
}

func (s *spfreshBulkCountingSourceReads) Open(ctx context.Context, name string, option *storage.ReaderOption) (storage.ExternalFileReader, error) {
	if strings.HasPrefix(name, "base/") {
		s.reads.Add(1)
	}
	return s.ExternalStorage.Open(ctx, name, option)
}

func TestSPFreshBulkEmptyScanUsesAdmittedSnapshot(t *testing.T) {
	store := newSPFreshBulkMockStore(t)
	tbl, index := newSPFreshBulkScanTestTable(t)
	snapshot := currentSPFreshBulkTestVersion(t, store)
	// The fixed-B source is empty even though the table has data when scanning starts.
	writeSPFreshBulkTestRows(t, store, tbl.Meta(), map[int64][]float32{1: testSPFreshBulkVector(1, 0x3f800001)})
	run := newSPFreshBulkScanTestRun(index.ID, snapshot, "empty-snapshot")
	require.NoError(t, scanAndUploadSPFreshBaseFragmentsToStore(context.Background(), exprstatic.NewExprContext(), NewReorgContext(), store, tbl, run,
		storage.NewMemStorage(), testSPFreshBulkScanLayout(t, 2), spfreshBulkScanConfig{fragmentTargetBytes: spfreshBulkBaseRecordEncodedBytes}))
	require.Zero(t, run.sourceRows)
	require.Zero(t, run.sourceFingerprint.Count)
	require.Empty(t, run.baseFragmentRefs)
	require.Equal(t, snapshot, run.sourceSnapshotTS)
	require.ErrorContains(t, run.checkReadyForPublic(), "before Ready")
}

func TestSPFreshBulkRangeConsumersOverlapAndJoin(t *testing.T) {
	for _, role := range []string{"reader", "consumer"} {
		gauge := metrics.SPFreshBulkPipelineWorkers.WithLabelValues(role, "active")
		baseline := testutil.ToFloat64(gauge)
		t.Cleanup(func() { require.Equal(t, baseline, testutil.ToFloat64(gauge)) })
	}
	store := newSPFreshBulkMockStore(t)
	tbl, _ := newSPFreshBulkScanTestTable(t)
	writeSPFreshBulkTestRows(t, store, tbl.Meta(), map[int64][]float32{
		0: testSPFreshBulkVector(0, 0x3f800001), 1: testSPFreshBulkVector(1, 0x3f800001),
	})
	middle := tablecodec.EncodeRowKeyWithHandle(tbl.GetPhysicalID(), kv.IntHandle(1))
	ranges := []kv.KeyRange{{StartKey: tbl.RecordPrefix(), EndKey: middle}, {StartKey: middle, EndKey: tbl.RecordPrefix().PrefixNext()}}
	for _, mode := range []string{"success", "error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			first, second := make(chan struct{}), make(chan struct{})
			expected := errors.New("consumer failed")
			var active, finished atomic.Int32
			factory := func(consumerCtx context.Context) (spfreshBulkRangeCallbacks, error) {
				return spfreshBulkRangeCallbacks{
					row: func(ordinal int, _ kv.Handle, _ []byte) (bool, error) {
						active.Add(1)
						defer active.Add(-1)
						if ordinal == 0 {
							close(first)
							select {
							case <-second:
							case <-consumerCtx.Done():
								return false, consumerCtx.Err()
							}
							if mode == "error" {
								return false, expected
							}
							if mode == "cancel" {
								cancel()
								return false, consumerCtx.Err()
							}
						} else {
							select {
							case <-first:
							case <-consumerCtx.Done():
								return false, consumerCtx.Err()
							}
							close(second)
							if mode != "success" {
								<-consumerCtx.Done()
								return false, consumerCtx.Err()
							}
						}
						return true, nil
					},
					finish: func(int) error { finished.Add(1); return nil },
				}, nil
			}
			_, _, _, err := readSPFreshBulkRanges(ctx, NewReorgContext(), store, tbl.RecordPrefix(),
				&spfreshBulkRunContext{dimension: pkdbspfreshbulk.DefaultDimension, sourceSnapshotTS: currentSPFreshBulkTestVersion(t, store)},
				2, spfreshBulkReadBatchBytes, spfreshTestRangeSource(ranges), factory)
			switch mode {
			case "success":
				require.NoError(t, err)
				require.EqualValues(t, 2, finished.Load())
			case "error":
				require.ErrorIs(t, err, expected)
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Zero(t, active.Load(), "all consumers must join before returning")
		})
	}
}

func TestSPFreshBulkConcurrentSampleMatchesSerialReservoir(t *testing.T) {
	const count, limit, dims = 10000, 137, 3
	serial, err := newSPFreshBulkBottomKSample(limit, dims)
	require.NoError(t, err)
	parallel, err := newSPFreshBulkBottomKSample(limit, dims)
	require.NoError(t, err)
	records := make([]pkdbspfreshbulk.BaseFragmentRecord, count)
	digests := make([][sha256.Size]byte, count)
	for row := range count {
		records[row] = pkdbspfreshbulk.BaseFragmentRecord{Handle: int64(row) - count/2, Vector: []float32{float32(row), 2, 3}}
		digests[row], err = pkdbspfreshbulk.RecordDigestWithPayload(records[row].Handle, records[row].Vector, nil)
		require.NoError(t, err)
		serial.AddWithDigest(records[row], digests[row])
	}
	var group sync.WaitGroup
	for worker := range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			for row := count - 1 - worker; row >= 0; row -= 8 {
				parallel.AddWithDigest(records[row], digests[row])
			}
		}()
	}
	group.Wait()
	want, wantDigests := serial.RecordsWithDigests(limit)
	got, gotDigests := parallel.RecordsWithDigests(limit)
	require.Equal(t, want, got)
	require.Equal(t, wantDigests, gotDigests)
	capacity := 0
	for _, block := range parallel.vectorBlocks {
		capacity += cap(block)
	}
	require.Equal(t, limit*dims, capacity)
}

func TestSPFreshBulkSourceConcurrencyReservesResources(t *testing.T) {
	for _, tc := range []struct {
		cpus   int
		memory uint64
		want   int
	}{
		{24, 0, 0}, {24, 256 << 20, 0}, {24, 512 << 20, 1},
		{24, 1 << 30, 2}, {24, 4 << 30, 8}, {24, 16 << 30, 12},
		{12, 16 << 30, 6}, {6, 16 << 30, 4}, {1, 16 << 30, 1},
	} {
		require.Equal(t, tc.want, spfreshBulkReadConcurrency(tc.cpus, tc.memory))
	}
}

func TestSPFreshBulkUploadConcurrencyUsesOnlySpareResources(t *testing.T) {
	for _, tc := range []struct {
		readers, cpus int
		quota         uint64
		want          int
	}{
		{4, 24, 16 << 30, 8}, {4, 12, 16 << 30, 5},
		{4, 4, 16 << 30, 4}, {4, 24, 1 << 30, 4},
		{4, 24, 0, 4}, {1, 1, 16 << 30, 1},
	} {
		require.Equal(t, tc.want, spfreshBulkUploadConcurrency(tc.readers, tc.cpus, tc.quota, 64<<20))
	}
}

// Multiple builds must not reset each other's contribution, including when a
// canceled pipeline still has queued fragments at teardown.
func TestSPFreshPendingFragmentsConcurrentBuildCleanup(t *testing.T) {
	baseline := testutil.ToFloat64(metrics.SPFreshBulkPendingFragments)
	var first, second atomic.Int64
	first.Store(2)
	second.Store(3)
	stopFirst := observeSPFreshPendingFragments(func() int { return int(first.Load()) })
	stopSecond := observeSPFreshPendingFragments(func() int { return int(second.Load()) })
	defer stopSecond()
	firstStopped := false
	defer func() {
		if !firstStopped {
			stopFirst()
		}
	}()
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.SPFreshBulkPendingFragments) == baseline+5
	}, 5*time.Second, 10*time.Millisecond)
	stopFirst()
	firstStopped = true
	require.Equal(t, baseline+3, testutil.ToFloat64(metrics.SPFreshBulkPendingFragments))
	second.Store(0)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.SPFreshBulkPendingFragments) == baseline
	}, 5*time.Second, 10*time.Millisecond)
}
