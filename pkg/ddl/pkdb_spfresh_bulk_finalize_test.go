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
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Complete fixture shards through the same proof/import consumer as the live
// coordinator. Scheduling before the last RPC is tested in coordinator tests.
func proveSPFreshTestBuild(ctx context.Context, store storage.ExternalStorage, run *spfreshBulkRunContext) error {
	return proveAndStageSPFreshTestBuild(ctx, store, run, discardSPFreshTestRun)
}

func discardSPFreshTestRun(context.Context, spfreshBulkSortedRun, pkdbspfreshbulk.ManifestRef) error {
	return nil
}

func TestSPFreshBulkStoredSummaryMatchesSchema(t *testing.T) {
	for _, tc := range []struct {
		name          string
		types         []byte
		summaryColumn int64
		summaryKind   byte
	}{
		{name: "varchar", types: []byte{mysql.TypeVarchar}},
		{name: "float", types: []byte{mysql.TypeFloat}},
		{name: "double", types: []byte{mysql.TypeDouble}},
		{name: "integer", types: []byte{mysql.TypeLonglong}, summaryColumn: 1, summaryKind: 1},
		{name: "date", types: []byte{mysql.TypeDate}, summaryColumn: 1, summaryKind: 2},
		{name: "mixed", types: []byte{mysql.TypeVarchar, mysql.TypeLong}, summaryColumn: 2, summaryKind: 1},
	} {
		for _, path := range []string{"single_leaf_root", "single_non_root", "sharded"} {
			for _, withSummary := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/summary=%t", tc.name, path, withSummary), func(t *testing.T) {
					var store storage.ExternalStorage
					var run *spfreshBulkRunContext
					if path == "sharded" {
						store, run = newSPFreshBulkFinalManifestFixture(t)
					} else {
						store, run = newSPFreshSingleProofFixture(t, path == "single_leaf_root")
					}
					var values []types.Datum
					for i, tp := range tc.types {
						fieldType, err := (&tipb.FieldType{Tp: int32(tp)}).Marshal()
						require.NoError(t, err)
						run.storedColumns = append(run.storedColumns, pkdbspfreshbulk.StoredColumn{ColumnID: int64(i + 1), FieldType: fieldType})
						// Nullable STORING columns still require a summary when their
						// type supports it, even when every stored value is NULL.
						values = append(values, types.NewIntDatum(int64(i+1)), types.Datum{})
					}
					payload, err := codec.EncodeValue(time.UTC, nil, values...)
					require.NoError(t, err)
					var summary []byte
					if withSummary {
						// Current LeafSummary encoding: one all-NULL column.
						summary = binary.BigEndian.AppendUint32([]byte{1}, 1)
						summary = binary.BigEndian.AppendUint64(summary, uint64(max(1, tc.summaryColumn)))
						summary = append(summary, max(byte(1), tc.summaryKind), 1)
					}
					mutate := func(records []pkdbspfreshbulk.SortedRunRecord) []pkdbspfreshbulk.SortedRunRecord {
						prefix, err := spfreshBulkIndexKeyPrefix(run)
						require.NoError(t, err)
						var summaries []pkdbspfreshbulk.SortedRunRecord
						for i := range records {
							_, level, suffix, err := decodeSPFreshBulkLogicalKey(records[i].Key, prefix)
							require.NoError(t, err)
							if level == spfreshBulkLeafPartitionLevel {
								records[i].Value = append(bytes.Clone(records[i].Value), payload...)
							} else if summary != nil && level == 0 && suffix[0] == spfreshBulkStateMetadataItem && binary.BigEndian.Uint32(records[i].Value) == spfreshBulkLeafPartitionLevel {
								key := bytes.Clone(records[i].Key)
								key[len(key)-1] = 3
								summaries = append(summaries, pkdbspfreshbulk.SortedRunRecord{Key: key, Value: summary})
							}
						}
						return append(records, summaries...)
					}
					if path == "sharded" {
						for i := range run.distributedBuild.shardResultRefs {
							mutateSPFreshBulkShardRun(t, store, run, i, mutate)
						}
						err = proveSPFreshTestBuild(context.Background(), store, run)
					} else {
						ref, recordCount, objectBytes := &run.nonRootRef, &run.nonRootRecordCount, &run.nonRootObjectBytes
						if path == "single_leaf_root" {
							ref, recordCount, objectBytes = &run.rootRef, &run.rootRecordCount, &run.rootObjectBytes
						}
						key := strings.TrimPrefix(ref.URI, run.artifactPrefix+"/")
						encoded, readErr := store.ReadFile(context.Background(), key)
						require.NoError(t, readErr)
						header, records, decodeErr := pkdbspfreshbulk.DecodeSortedRun(encoded)
						require.NoError(t, decodeErr)
						records = mutate(records)
						sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i].Key, records[j].Key) < 0 })
						header.RecordCount = uint64(len(records))
						encoded, err = pkdbspfreshbulk.EncodeSortedRun(header, records)
						require.NoError(t, err)
						require.NoError(t, store.WriteFile(context.Background(), key, encoded))
						*ref = pkdbspfreshbulk.NewManifestRef(ref.URI, encoded)
						*recordCount, *objectBytes = uint64(len(records)), uint64(len(encoded))
						err = proveAndStageSPFreshSingleRuns(context.Background(), store, run, discardSPFreshTestRun)
					}
					if withSummary == (tc.summaryColumn != 0) {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, "SPFresh")
					}
				})
			}
		}
	}
}

func proveAndStageSPFreshTestBuild(ctx context.Context, store storage.ExternalStorage, run *spfreshBulkRunContext, stage spfreshBulkRunConsumer) error {
	d := run.distributedBuild
	d.shardEvidence = make([]spfreshBulkShardEvidence, len(d.shardInputRefs))
	group, workCtx := errgroup.WithContext(ctx)
	group.SetLimit(runtime.GOMAXPROCS(0))
	for i := range d.shardEvidence {
		group.Go(func() error {
			var err error
			d.shardEvidence[i], err = proveSPFreshBulkShard(workCtx, store, run, d.plan, i, d.shardInputRefs[i], func() (pkdbspfreshbulk.ManifestRef, error) { return d.shardResultRefs[i], nil }, stage)
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return writeSPFreshFinalManifestToStore(ctx, store, run, stage)
}

type concurrentProofStorage struct {
	storage.ExternalStorage
	started atomic.Int32
	active  atomic.Int32
	ready   chan struct{}
	fail    bool
}

func (s *concurrentProofStorage) Open(ctx context.Context, key string, options *storage.ReaderOption) (storage.ExternalFileReader, error) {
	if strings.HasPrefix(key, "shards/") && strings.HasSuffix(key, "/input.json") {
		s.active.Add(1)
		defer s.active.Add(-1)
		if s.started.Add(1) == 4 {
			close(s.ready)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.ready:
		}
		if s.fail {
			if key == "shards/0/input.json" {
				return nil, fmt.Errorf("injected shard proof failure")
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	return s.ExternalStorage.Open(ctx, key, options)
}

func TestSPFreshBulkFinalManifestParallelProofPreservesOrderAndCancels(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(previous)
	for _, fail := range []bool{false, true} {
		store, run := newSPFreshBulkFinalManifestFixture(t)
		gated := &concurrentProofStorage{ExternalStorage: store, ready: make(chan struct{}), fail: fail}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := proveSPFreshTestBuild(ctx, gated, run)
		cancel()
		require.Equal(t, int32(4), gated.started.Load())
		require.Zero(t, gated.active.Load())
		spfreshBulkReads.mu.Lock()
		reserved := spfreshBulkReads.used
		spfreshBulkReads.mu.Unlock()
		require.Zero(t, reserved)
		if fail {
			require.ErrorContains(t, err, "injected shard proof failure")
			require.Nil(t, run.finalManifest)
			exists, err := store.FileExists(context.Background(), "final.json")
			require.NoError(t, err)
			require.False(t, exists)
		} else {
			require.NoError(t, err)
			for i, shard := range run.finalManifest.ShardRuns {
				require.Equal(t, uint32(i), shard.ShardID)
			}
		}
	}
}

func TestSPFreshBulkRefinedFinalManifestProvesTreeAndImportOrder(t *testing.T) {
	objectStore, run := newSPFreshBulkFinalManifestFixture(t)
	// Four actual tasks from two coarse buckets must pass the complete proof
	// and import ordering checks; coarse count never truncates final evidence.
	run.distributedBuild.plan.CoarseShardCount = 2
	ctx := context.Background()
	for taskID, reference := range run.distributedBuild.shardInputRefs {
		inputPath := strings.TrimPrefix(reference.URI, run.artifactPrefix+"/")
		encoded, err := objectStore.ReadFile(ctx, inputPath)
		require.NoError(t, err)
		input, err := pkdbspfreshbulk.DecodeShardInputJSON(encoded)
		require.NoError(t, err)
		fragmentPath := strings.TrimPrefix(input.RaBitQFragments[0].URI, run.artifactPrefix+"/")
		encoded, err = objectStore.ReadFile(ctx, fragmentPath)
		require.NoError(t, err)
		header, rows, err := pkdbspfreshbulk.DecodeRaBitQFragment(encoded)
		require.NoError(t, err)
		header.ArtifactID = fmt.Sprintf("refine-shard-%d-worker-00000-input-00000", taskID)
		encoded, err = pkdbspfreshbulk.EncodeRaBitQFragment(header, rows)
		require.NoError(t, err)
		refinedPath := fmt.Sprintf("refine/shards/%d/input/worker-00000/part-00000.spfmvp", taskID)
		require.NoError(t, objectStore.WriteFile(ctx, refinedPath, encoded))
		input.RaBitQFragments[0] = pkdbspfreshbulk.NewManifestRef(run.artifactPrefix+"/"+refinedPath, encoded)
		input.Bytes = uint64(len(encoded))
		encoded, err = pkdbspfreshbulk.EncodeShardInputJSON(input)
		require.NoError(t, err)
		require.NoError(t, objectStore.WriteFile(ctx, inputPath, encoded))
		run.distributedBuild.shardInputRefs[taskID] = pkdbspfreshbulk.NewManifestRef(reference.URI, encoded)
	}
	require.NoError(t, proveSPFreshTestBuild(context.Background(), objectStore, run))
	require.NotNil(t, run.finalManifest)
	require.Equal(t, uint64(10), run.finalManifest.PartitionCount)
	require.Equal(t, uint64(4), run.finalManifest.LeafPartitionCount)
	require.Equal(t, uint64(4), run.finalManifest.LeafEntryCount)
	require.Equal(t, uint32(4), run.finalManifest.RootLevel)
	require.Equal(t, uint64(34), run.finalManifest.MaxPartitionKey)

	encoded, err := objectStore.ReadFile(context.Background(), "final.json")
	require.NoError(t, err)
	decoded, err := pkdbspfreshbulk.DecodeFinalManifestJSON(encoded)
	require.NoError(t, err)
	require.Equal(t, *run.finalManifest, decoded)
	require.Equal(t, pkdbspfreshbulk.NewManifestRef(run.artifactPrefix+"/final.json", encoded), run.finalManifestRef)

	for shardID := range 4 {
		require.Equal(t, uint32(shardID), decoded.ShardRuns[shardID].ShardID)
	}

	run.bulkImportTS = run.sourceSnapshotTS + 1
	run.importCompleted = true
	require.NoError(t, verifySPFreshDistributedImportState(run))
	run.importCompleted = false
	require.ErrorContains(t, verifySPFreshDistributedImportState(run), "does not match FinalManifest")
}

func TestSPFreshBulkFinalManifestOmitsRedundantMergeLayer(t *testing.T) {
	objectStore, run := newSPFreshBulkFinalManifestFixture(t)
	rootRecords := spfreshBulkTestPartitionRecords(
		t, run, pkdbspfreshbulk.RootPartitionKey, pkdbspfreshbulk.ShardHandoffLevel+1,
		[]uint64{3, 11, 19, 27},
	)
	rootBytes := encodeSPFreshBulkTestRun(
		t, run, "merge-root", "root", 0,
		pkdbspfreshbulk.RootPartitionKey, pkdbspfreshbulk.RootPartitionKey, rootRecords,
	)
	rootURI := run.artifactPrefix + "/merge/root.spfmvp"
	require.NoError(t, objectStore.WriteFile(context.Background(), "merge/root.spfmvp", rootBytes))
	run.distributedBuild.merge = remote.SPFreshBulkMergeResponse{
		Root: pkdbspfreshbulk.NewManifestRef(rootURI, rootBytes), SourceRows: run.sourceRows,
		Fingerprint: run.sourceFingerprint, RootLevel: pkdbspfreshbulk.ShardHandoffLevel + 1,
		MaxPartitionKey: 27, RootObjectBytes: uint64(len(rootBytes)),
	}

	withSPFreshStagingEngines(t, run, func(staging *spfreshBulkStaging) {
		require.NoError(t, proveAndStageSPFreshTestBuild(context.Background(), objectStore, run, staging.append))
		require.NoError(t, staging.validateManifest(*run.finalManifest))
		require.Len(t, staging.refs, 5)
		require.Equal(t, run.finalManifest.Root, staging.rootRef)
	})
	require.Nil(t, run.finalManifest.MergeNonRoot)
	require.Equal(t, uint64(9), run.finalManifest.PartitionCount)
	require.Equal(t, uint32(3), run.finalManifest.RootLevel)
	require.Equal(t, uint64(27), run.finalManifest.MaxPartitionKey)
}

func TestSPFreshBulkFinalManifestRejectsCrossShardFingerprintReassociation(t *testing.T) {
	objectStore, run := newSPFreshBulkFinalManifestFixture(t)
	results := make([]pkdbspfreshbulk.ShardResult, 2)
	for shardID := range 2 {
		path := fmt.Sprintf("shards/%d/result.json", shardID)
		encoded, err := objectStore.ReadFile(context.Background(), path)
		require.NoError(t, err)
		results[shardID], err = pkdbspfreshbulk.DecodeShardResultJSON(encoded)
		require.NoError(t, err)
	}
	results[0].Fingerprint, results[1].Fingerprint = results[1].Fingerprint, results[0].Fingerprint
	for shardID := range 2 {
		path := fmt.Sprintf("shards/%d/result.json", shardID)
		encoded, err := pkdbspfreshbulk.EncodeShardResultJSON(results[shardID])
		require.NoError(t, err)
		require.NoError(t, objectStore.WriteFile(context.Background(), path, encoded))
		run.distributedBuild.shardResultRefs[shardID] = pkdbspfreshbulk.NewManifestRef(
			run.artifactPrefix+"/"+path,
			encoded,
		)
	}
	require.ErrorContains(
		t,
		proveSPFreshTestBuild(context.Background(), objectStore, run),
		"does not match BuildPlan",
	)
}

func TestSPFreshFragmentURIRequiresCurrentProducerAndCanonicalPath(t *testing.T) {
	const prefix = "s3://spfresh-mvp/build"
	workerID, ordinal, artifact, err := parseSPFreshRabitqFragmentURI(prefix, 7, 4, prefix+"/shuffle/shards/3/input/worker-00002/part-00007.spfmvp")
	require.NoError(t, err)
	require.Equal(t, uint64(2), workerID)
	require.Equal(t, uint64(7), ordinal)
	require.Equal(t, "shuffle-shard-3-worker-00002-input-00007", artifact)
	_, _, artifact, err = parseSPFreshRabitqFragmentURI(prefix, 7, 4, prefix+"/refine/shards/7/input/worker-00000/part-00000.spfmvp")
	require.NoError(t, err)
	require.Equal(t, "refine-shard-7-worker-00000-input-00000", artifact)
	for _, uri := range []string{
		prefix + "/shuffle/shards/3/input/worker-2/part-00007.spfmvp",
		prefix + "/shuffle/shards/3/input/worker-00004/part-00007.spfmvp",
		prefix + "/refine/shards/2/input/worker-00000/part-00000.spfmvp",
		prefix + "/refine/shards/7/input/worker-00001/part-00000.spfmvp",
		prefix + "/shards/7/input/worker-00000/part-00000.spfmvp",
	} {
		_, _, _, err := parseSPFreshRabitqFragmentURI(prefix, 7, 4, uri)
		require.Error(t, err, uri)
	}
}

func TestSPFreshBulkFinalManifestRejectsMissingEntries(t *testing.T) {
	store, run := newSPFreshBulkFinalManifestFixture(t)
	mutateSPFreshBulkShardRun(t, store, run, 0, func(records []pkdbspfreshbulk.SortedRunRecord) []pkdbspfreshbulk.SortedRunRecord {
		return removeSPFreshBulkLeafRecord(t, run, records)
	})
	require.Error(t, proveSPFreshTestBuild(context.Background(), store, run))
}

func TestSPFreshBulkSortedRunRejectsNonQ7Centroids(t *testing.T) {
	for _, metadata := range []bool{false, true} {
		t.Run(fmt.Sprintf("metadata=%t", metadata), func(t *testing.T) {
			store, run := newSPFreshBulkFinalManifestFixture(t)
			mutateSPFreshBulkShardRun(t, store, run, 0, func(records []pkdbspfreshbulk.SortedRunRecord) []pkdbspfreshbulk.SortedRunRecord {
				prefix, err := spfreshBulkIndexKeyPrefix(run)
				require.NoError(t, err)
				for i := range records {
					_, level, suffix, err := decodeSPFreshBulkLogicalKey(records[i].Key, prefix)
					require.NoError(t, err)
					if metadata && level == 0 && suffix[0] == spfreshBulkCentroidMetadataItem ||
						!metadata && level > spfreshBulkLeafPartitionLevel {
						records[i].Value = make([]byte, int(run.dimension)*4)
						return records
					}
				}
				t.Fatal("fixture has no matching centroid")
				return nil
			})
			require.ErrorContains(t, proveSPFreshTestBuild(context.Background(), store, run), "invalid Q7C1 encoding")
		})
	}
}

func newSPFreshBulkFinalManifestFixture(t *testing.T, keyspaceIDs ...uint32) (storage.ExternalStorage, *spfreshBulkRunContext) {
	t.Helper()
	t.Cleanup(func() {
		spfreshBulkReads.mu.Lock()
		reserved := spfreshBulkReads.used
		spfreshBulkReads.mu.Unlock()
		require.Zero(t, reserved, "proof success or failure leaked an object reservation")
	})
	objectStore := storage.NewMemStorage()
	run := newSPFreshBulkScanTestRun(4, 123, "t7-final")
	if len(keyspaceIDs) != 0 {
		run.keyspaceID = keyspaceIDs[0]
	}
	run.cloudStorageURI = "s3://spfresh-mvp/t7-final"
	identity := spfreshBulkBuildIdentity(run)

	vectors := make([]pkdbspfreshbulk.BaseFragmentRecord, 4)
	fingerprints := make([]pkdbspfreshbulk.MultisetFingerprint, 4)
	for shardID := range 4 {
		vector := make([]float32, pkdbspfreshbulk.Dimension)
		vector[shardID] = 1
		vectors[shardID] = pkdbspfreshbulk.BaseFragmentRecord{Handle: int64(shardID + 1), Vector: vector}
		var err error
		fingerprints[shardID], err = pkdbspfreshbulk.FingerprintRecords(vectors[shardID : shardID+1])
		require.NoError(t, err)
	}
	run.sourceRows = 4
	var err error
	run.sourceFingerprint, err = pkdbspfreshbulk.MergeFingerprints(fingerprints)
	require.NoError(t, err)

	plan := pkdbspfreshbulk.BuildPlan{
		Identity: identity, CoarseShardCount: 4,
		SourceRows: 4, SourceFingerprint: run.sourceFingerprint,
		Endpoints: []string{"http://worker-0", "http://worker-1", "http://worker-2", "http://worker-3"},
	}
	resultRefs := make([]pkdbspfreshbulk.ManifestRef, 4)
	inputRefs := make([]pkdbspfreshbulk.ManifestRef, 4)
	for shardID := range 4 {
		plannedRange, err := pkdbspfreshbulk.PartitionRange(run.sourceRows, uint32(shardID))
		require.NoError(t, err)
		minKey := plannedRange.MinPartitionKey
		maxKey := minKey + 1
		shardPath := fmt.Sprintf("shards/%d", shardID)
		basePath := fmt.Sprintf("shuffle/%s/input/worker-%05d/part-00000.spfmvp", shardPath, shardID)
		baseURI := run.artifactPrefix + "/" + basePath
		baseHeader := pkdbspfreshbulk.RaBitQFragmentHeader{
			CommonHeader: pkdbspfreshbulk.CommonHeader{
				BuildID: identity.BuildID, ArtifactID: fmt.Sprintf("shuffle-shard-%d-worker-%05d-input-00000", shardID, shardID),
				TableID: identity.TableID, IndexID: identity.IndexID, Dimension: identity.Dimension,
				Distance: identity.Distance, RecordCount: 1, Producer: "tikv-worker",
			},
			MinHandle: vectors[shardID].Handle, MaxHandle: vectors[shardID].Handle,
		}
		sourceDigestBytes, decodeErr := hex.DecodeString(fingerprints[shardID].XOR256)
		require.NoError(t, decodeErr)
		var sourceDigest [32]byte
		copy(sourceDigest[:], sourceDigestBytes)
		quantized := []pkdbspfreshbulk.RaBitQFragmentRecord{{
			Handle: vectors[shardID].Handle, SourceDigestSHA256: sourceDigest,
			EncodedVector: make([]byte, pkdbspfreshbulk.RaBitQValueLen),
		}}
		baseBytes, encodeErr := pkdbspfreshbulk.EncodeRaBitQFragment(baseHeader, quantized)
		require.NoError(t, encodeErr)
		baseRef := pkdbspfreshbulk.NewManifestRef(baseURI, baseBytes)
		require.NoError(t, objectStore.WriteFile(context.Background(), basePath, baseBytes))
		input := pkdbspfreshbulk.ShardInput{
			Identity: identity, ShardID: uint32(shardID), RaBitQFragments: []pkdbspfreshbulk.ManifestRef{baseRef},
			Rows: 1, Bytes: uint64(len(baseBytes)), Fingerprint: fingerprints[shardID],
		}
		inputBytes, encodeErr := pkdbspfreshbulk.EncodeShardInputJSON(input)
		require.NoError(t, encodeErr)
		inputURI := run.artifactPrefix + "/" + shardPath + "/input.json"
		inputRefs[shardID] = pkdbspfreshbulk.NewManifestRef(inputURI, inputBytes)
		require.NoError(t, objectStore.WriteFile(context.Background(), shardPath+"/input.json", inputBytes))

		nonRootURI := run.artifactPrefix + "/" + shardPath + "/non-root.spfmvp"
		nonRootBytes := encodeSPFreshBulkTestRun(t, run, fmt.Sprintf("shard-%d-non-root", shardID), "non_root", 1,
			minKey, maxKey,
			append(spfreshBulkTestPartitionRecords(t, run, minKey, 1, []uint64{uint64(shardID + 1)}),
				spfreshBulkTestPartitionRecords(t, run, maxKey, 2, []uint64{minKey})...))
		nonRootRef := pkdbspfreshbulk.NewManifestRef(nonRootURI, nonRootBytes)
		require.NoError(t, objectStore.WriteFile(context.Background(), shardPath+"/non-root.spfmvp", nonRootBytes))
		centroid, encodeErr := pkdbspfreshbulk.EncodeRoutingCentroid(vectors[shardID].Vector, run.distance)
		require.NoError(t, encodeErr)
		result := pkdbspfreshbulk.ShardResult{
			Identity: identity, ShardID: uint32(shardID), NonRoot: nonRootRef,
			LeafEntryCount: 1, Fingerprint: fingerprints[shardID],
			Frontier:        []pkdbspfreshbulk.FrontierSummary{{NodeID: 1, PartitionKey: maxKey, Level: 2, CentroidF32LEBase64: centroid}},
			MinPartitionKey: minKey, MaxPartitionKey: maxKey,
		}
		resultBytes, encodeErr := pkdbspfreshbulk.EncodeShardResultJSON(result)
		require.NoError(t, encodeErr)
		resultURI := run.artifactPrefix + "/" + shardPath + "/result.json"
		resultRefs[shardID] = pkdbspfreshbulk.NewManifestRef(resultURI, resultBytes)
		require.NoError(t, objectStore.WriteFile(context.Background(), shardPath+"/result.json", resultBytes))
	}

	mergeRecords := spfreshBulkTestPartitionRecords(t, run, 34, 3, []uint64{3, 11, 19, 27})
	mergeBytes := encodeSPFreshBulkTestRun(t, run, "merge-non-root", "non_root", 0, 34, 34, mergeRecords)
	mergeURI := run.artifactPrefix + "/merge/non-root.spfmvp"
	mergeRef := pkdbspfreshbulk.NewManifestRef(mergeURI, mergeBytes)
	require.NoError(t, objectStore.WriteFile(context.Background(), "merge/non-root.spfmvp", mergeBytes))
	rootRecords := spfreshBulkTestPartitionRecords(t, run, pkdbspfreshbulk.RootPartitionKey, 4, []uint64{34})
	rootBytes := encodeSPFreshBulkTestRun(t, run, "merge-root", "root", 0, 1, 1, rootRecords)
	rootURI := run.artifactPrefix + "/merge/root.spfmvp"
	rootRef := pkdbspfreshbulk.NewManifestRef(rootURI, rootBytes)
	require.NoError(t, objectStore.WriteFile(context.Background(), "merge/root.spfmvp", rootBytes))

	run.distributedBuild = &spfreshBulkDistributedResult{
		plan: plan, shardInputRefs: inputRefs, shardResultRefs: resultRefs,
		merge: remote.SPFreshBulkMergeResponse{
			MergeNonRoot: &mergeRef, Root: rootRef, SourceRows: 4, Fingerprint: run.sourceFingerprint,
			MergePartitionCount: 1, RootLevel: 4, MaxPartitionKey: 34,
			MergeNonRootObjectBytes: uint64(len(mergeBytes)),
		},
	}
	return objectStore, run
}

func spfreshBulkTestPartitionRecords(
	t *testing.T,
	run *spfreshBulkRunContext,
	partitionKey uint64,
	level uint32,
	children []uint64,
) []pkdbspfreshbulk.SortedRunRecord {
	t.Helper()
	prefix, err := spfreshBulkIndexKeyPrefix(run)
	require.NoError(t, err)
	key := func(dataLevel uint32, suffix []byte) []byte {
		result := append([]byte{}, prefix...)
		result = binary.BigEndian.AppendUint64(result, partitionKey)
		result = binary.BigEndian.AppendUint32(result, dataLevel)
		return append(result, suffix...)
	}
	state := make([]byte, spfreshBulkPartitionStateValueLen)
	binary.BigEndian.PutUint32(state, level)
	binary.BigEndian.PutUint32(state[4:], spfreshBulkReadyPartitionState)
	centroid := spfreshBulkTestQ7Centroid(uint32(spfreshBulkDimension))
	hints := make([]byte, spfreshBulkPartitionHintsValueLen)
	binary.BigEndian.PutUint32(hints[8:], uint32(len(children)))
	records := []pkdbspfreshbulk.SortedRunRecord{
		{Key: key(0, []byte{spfreshBulkStateMetadataItem}), Value: state},
		{Key: key(0, []byte{spfreshBulkCentroidMetadataItem}), Value: centroid},
		{Key: key(0, []byte{spfreshBulkHintsMetadataItem}), Value: hints},
	}
	for _, child := range children {
		suffix := binary.BigEndian.AppendUint64(nil, child)
		value := make([]byte, 12+(int(run.dimension)+7)/8+(int(run.dimension)*6+7)/8)
		if level == spfreshBulkLeafPartitionLevel {
			suffix = codec.EncodeInt(nil, int64(child))
		} else {
			value = spfreshBulkTestQ7Centroid(uint32(spfreshBulkDimension))
		}
		records = append(records, pkdbspfreshbulk.SortedRunRecord{Key: key(level, suffix), Value: value})
	}
	sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i].Key, records[j].Key) < 0 })
	return records
}

func encodeSPFreshBulkTestRun(
	t *testing.T,
	run *spfreshBulkRunContext,
	artifactID, role string,
	leafEntries uint64,
	minPartitionKey, maxPartitionKey uint64,
	records []pkdbspfreshbulk.SortedRunRecord,
) []byte {
	t.Helper()
	header := pkdbspfreshbulk.SortedRunHeader{
		CommonHeader: pkdbspfreshbulk.CommonHeader{
			BuildID: run.buildID, ArtifactID: artifactID, TableID: run.tableID, IndexID: run.indexID,
			Dimension: uint32(spfreshBulkDimension), Distance: run.distance,
			RecordCount: uint64(len(records)), Producer: "tikv-worker",
		},
		RunRole: role, LeafEntryCount: leafEntries, MinKeyBase64: base64.StdEncoding.EncodeToString(records[0].Key),
		MaxKeyBase64:  base64.StdEncoding.EncodeToString(records[len(records)-1].Key),
		AlgorithmSeed: run.algorithmSeed, TargetOccupancy: run.targetOccupancy,
		MinPartitionSize: run.minPartitionSize, MaxPartitionSize: run.maxPartitionSize,
	}
	encoded, err := pkdbspfreshbulk.EncodeSortedRun(header, records)
	require.NoError(t, err)
	return encoded
}

func mutateSPFreshBulkShardRun(
	t *testing.T,
	objectStore storage.ExternalStorage,
	run *spfreshBulkRunContext,
	shardID int,
	mutate func([]pkdbspfreshbulk.SortedRunRecord) []pkdbspfreshbulk.SortedRunRecord,
) {
	t.Helper()
	resultPath := fmt.Sprintf("shards/%d/result.json", shardID)
	resultBytes, err := objectStore.ReadFile(context.Background(), resultPath)
	require.NoError(t, err)
	result, err := pkdbspfreshbulk.DecodeShardResultJSON(resultBytes)
	require.NoError(t, err)
	runPath := fmt.Sprintf("shards/%d/non-root.spfmvp", shardID)
	runBytes, err := objectStore.ReadFile(context.Background(), runPath)
	require.NoError(t, err)
	header, records, err := pkdbspfreshbulk.DecodeSortedRun(runBytes)
	require.NoError(t, err)
	// DecodeSortedRun borrows immutable object bytes; corruption fixtures own
	// the bytes they deliberately modify before re-encoding the object.
	for i := range records {
		records[i].Key = bytes.Clone(records[i].Key)
		records[i].Value = bytes.Clone(records[i].Value)
	}
	records = mutate(records)
	sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i].Key, records[j].Key) < 0 })
	header.RecordCount = uint64(len(records))
	header.MinKeyBase64 = base64.StdEncoding.EncodeToString(records[0].Key)
	header.MaxKeyBase64 = base64.StdEncoding.EncodeToString(records[len(records)-1].Key)
	runBytes, err = pkdbspfreshbulk.EncodeSortedRun(header, records)
	require.NoError(t, err)
	require.NoError(t, objectStore.WriteFile(context.Background(), runPath, runBytes))
	result.NonRoot = pkdbspfreshbulk.NewManifestRef(run.artifactPrefix+"/"+runPath, runBytes)
	resultBytes, err = pkdbspfreshbulk.EncodeShardResultJSON(result)
	require.NoError(t, err)
	require.NoError(t, objectStore.WriteFile(context.Background(), resultPath, resultBytes))
	run.distributedBuild.shardResultRefs[shardID] = pkdbspfreshbulk.NewManifestRef(
		run.artifactPrefix+"/"+resultPath,
		resultBytes,
	)
}

func removeSPFreshBulkLeafRecord(
	t *testing.T,
	run *spfreshBulkRunContext,
	records []pkdbspfreshbulk.SortedRunRecord,
) []pkdbspfreshbulk.SortedRunRecord {
	t.Helper()
	prefix, err := spfreshBulkIndexKeyPrefix(run)
	require.NoError(t, err)
	for index, record := range records {
		_, level, _, decodeErr := decodeSPFreshBulkLogicalKey(record.Key, prefix)
		require.NoError(t, decodeErr)
		if level == spfreshBulkLeafPartitionLevel {
			return append(records[:index:index], records[index+1:]...)
		}
	}
	t.Fatal("fixture has no leaf record")
	return nil
}

// An exactly represented unit centroid, independent of the quantizer's choice
// of codes. The encoder-produced seven-dimensional golden below checks the ABI.
func spfreshBulkTestQ7Centroid(dimension uint32) []byte {
	signs := (int(dimension) + 7) / 8
	encoded := make([]byte, 20+signs+(int(dimension)*6+7)/8)
	copy(encoded, "Q7C1")
	binary.LittleEndian.PutUint32(encoded[4:8], dimension)
	binary.LittleEndian.PutUint32(encoded[8:12], math.Float32bits(1))
	binary.LittleEndian.PutUint32(encoded[12:16], math.Float32bits(1))
	encoded[20] = 1
	encoded[20+signs] = 1
	return encoded
}

func TestSPFreshBulkQ7CentroidValidation(t *testing.T) {
	// Also asserted against Centroid::new in the Rust centroid tests: coordinates
	// [0.5, -1, 1.5, 0, 31.5, -2, 3.5], scale 0.5, squared norm 1012.
	golden, err := hex.DecodeString("51374331070000000000003f00007d44000000005d8130003f7100")
	require.NoError(t, err)
	require.NoError(t, validateSPFreshBulkCentroidBytes(golden, "l2", 7, true))
	require.NoError(t, validateSPFreshBulkCentroidBytes(golden, "cosine", 7, false))
	require.Error(t, validateSPFreshBulkCentroidBytes(golden, "cosine", 7, true))
	for _, dimension := range []uint32{1, 7, 8, 9, 768, 1024, pkdbspfreshbulk.MaxDimension} {
		require.NoError(t, validateSPFreshBulkCentroidBytes(spfreshBulkTestQ7Centroid(dimension), "cosine", dimension, true))
	}
	valid := spfreshBulkTestQ7Centroid(7)
	for name, mutate := range map[string]func([]byte) []byte{
		"old FP32":     func([]byte) []byte { return make([]byte, 7*4) },
		"old metadata": func([]byte) []byte { return make([]byte, 4+7*4) },
		"truncated":    func(v []byte) []byte { return v[:len(v)-1] },
		"extra bytes":  func(v []byte) []byte { return append(v, 0) },
		"magic":        func(v []byte) []byte { v[0] = 0; return v },
		"dimension":    func(v []byte) []byte { v[4] = 8; return v },
		"scale NaN": func(v []byte) []byte {
			binary.LittleEndian.PutUint32(v[8:12], math.Float32bits(float32(math.NaN())))
			return v
		},
		"negative error": func(v []byte) []byte { binary.LittleEndian.PutUint32(v[16:20], math.Float32bits(-1)); return v },
		"forged norm":    func(v []byte) []byte { binary.LittleEndian.PutUint32(v[8:12], math.Float32bits(2)); return v },
		"overflowing reconstruction": func(v []byte) []byte {
			binary.LittleEndian.PutUint32(v[8:12], math.Float32bits(math.MaxFloat32))
			return v
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, validateSPFreshBulkCentroidBytes(mutate(bytes.Clone(valid)), "cosine", 7, true))
		})
	}
	require.Error(t, validateSPFreshBulkCentroidBytes(valid, "l2", 0, false))
	require.Error(t, validateSPFreshBulkCentroidBytes(valid, "l2", pkdbspfreshbulk.MaxDimension+1, false))
}

// Run all shard identities and the final tree proof using unprefixed API v1
// index records, so a leftover keyspace prefix in any producer or consumer is
// observable before the manifest can authorize an import.
func TestSPFreshBulkAPIV1FinalManifest(t *testing.T) {
	store, run := newSPFreshBulkFinalManifestFixture(t, math.MaxUint32)
	require.NoError(t, proveSPFreshTestBuild(context.Background(), store, run))
	require.NotNil(t, run.finalManifest)
	require.Equal(t, uint64(4), run.finalManifest.LeafEntryCount)
	require.Equal(t, uint64(4), run.finalManifest.LeafPartitionCount)
	encoded, err := store.ReadFile(context.Background(), "final.json")
	require.NoError(t, err)
	decoded, err := pkdbspfreshbulk.DecodeFinalManifestJSON(encoded)
	require.NoError(t, err)
	require.Equal(t, *run.finalManifest, decoded)
}
