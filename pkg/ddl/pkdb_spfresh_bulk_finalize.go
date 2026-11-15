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
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	spfreshBulkTxnKeyspacePrefixLength = 4
	spfreshBulkPartitionKeyLength      = 8
	spfreshBulkPartitionLevelLength    = 4
	spfreshBulkInvalidPartitionLevel   = 0
	spfreshBulkLeafPartitionLevel      = 1
	spfreshBulkReadyPartitionState     = 1
	spfreshBulkStateMetadataItem       = 0
	spfreshBulkCentroidMetadataItem    = 1
	spfreshBulkHintsMetadataItem       = 2
	spfreshBulkPartitionStateValueLen  = 40
	spfreshBulkPartitionHintsValueLen  = 16
)

type spfreshBulkRunEvidence struct {
	recordCount        uint64
	partitionCount     uint64
	leafPartitionCount uint64
	leafEntryCount     uint64
	minPartitionKey    uint64
	maxPartitionKey    uint64
	rootLevel          uint32
}

type spfreshBulkShardInputEvidence struct {
	rows        uint64
	bytes       uint64
	fingerprint pkdbspfreshbulk.MultisetFingerprint
}

type spfreshBulkShardEvidence struct {
	rows        uint64
	fingerprint pkdbspfreshbulk.MultisetFingerprint
	output      spfreshBulkRunEvidence
	run         pkdbspfreshbulk.FinalShardRun
}

type spfreshBulkTreeEvidence struct {
	partitions      uint64
	leaves          uint64
	leafEntries     uint64
	maxPartitionKey uint64
}

func (tree *spfreshBulkTreeEvidence) add(output spfreshBulkRunEvidence) error {
	for _, pair := range []struct {
		target *uint64
		value  uint64
	}{
		{&tree.partitions, output.partitionCount}, {&tree.leaves, output.leafPartitionCount}, {&tree.leafEntries, output.leafEntryCount},
	} {
		value, err := checkedAddSPFreshBulk(*pair.target, pair.value, "run counts")
		if err != nil {
			return err
		}
		*pair.target = value
	}
	tree.maxPartitionKey = max(tree.maxPartitionKey, output.maxPartitionKey)
	return nil
}

// Direct input refs are authenticated by the worker against the source snapshot
// frozen by TiDB. Each output is authenticated and imported once, root last.
// Algorithm correctness belongs to construction tests, not a second tree walk.
func proveAndStageSPFreshSingleRuns(ctx context.Context, store storage.ExternalStorage, run *spfreshBulkRunContext, stage spfreshBulkRunConsumer) error {
	started := time.Now()
	if run == nil || run.shardCount != 1 || run.builtRootLevel == 0 || run.sourceRows == 0 {
		return errors.New("SPFresh single-tree proof requires completed direct-build facts")
	}
	var tree spfreshBulkTreeEvidence
	var leafEntries uint64
	artifactCount := 0
	for _, role := range []string{spfreshBulkNonRootRole, spfreshBulkRootRole} {
		reference, artifactID := run.rootRef, "root"
		minKey, maxKey := uint64(1), uint64(1)
		if role == spfreshBulkNonRootRole {
			if run.builtRootLevel == 1 {
				if run.nonRootRef.URI != "" || run.nonRootRecordCount != 0 || run.nonRootObjectBytes != 0 {
					return errors.New("SPFresh leaf root has unexpected non-root output")
				}
				continue
			}
			reference, artifactID = run.nonRootRef, "non-root"
			minKey, maxKey = 2, run.builtMaxPartitionKey
		}
		if role == spfreshBulkRootRole {
			expected := tree
			rootEntries, rootLeaves := uint64(0), uint64(0)
			if run.builtRootLevel == spfreshBulkLeafPartitionLevel {
				rootEntries, rootLeaves = run.sourceRows, 1
			}
			if err := expected.add(spfreshBulkRunEvidence{partitionCount: 1, leafPartitionCount: rootLeaves, leafEntryCount: rootEntries, maxPartitionKey: 1}); err != nil {
				return err
			}
			if expected.partitions != run.builtPartitionCount || expected.leaves != run.builtLeafPartitions || expected.leafEntries != run.sourceRows || expected.maxPartitionKey != run.builtMaxPartitionKey || run.leafEntryCount != run.sourceRows {
				return errors.New("SPFresh single-tree counts differ from source or build result")
			}
		}
		if reference.URI != run.artifactPrefix+"/runs/"+artifactID+".spfmvp" {
			return errors.New("SPFresh single-tree run is outside its exact output path")
		}
		expectedRecords, expectedBytes, err := spfreshBulkRunStats(run, role)
		if err != nil {
			return err
		}
		evidence, err := inspectSPFreshPinnedSortedRun(ctx, store, run, reference, role, artifactID, minKey, maxKey,
			func(ctx context.Context, decoded spfreshBulkSortedRun, ref pkdbspfreshbulk.ManifestRef) error {
				if decoded.header.RecordCount != expectedRecords || decoded.encodedBytes != expectedBytes {
					return errors.New("SPFresh single-tree run stats do not match worker output")
				}
				return stage(ctx, decoded, ref)
			})
		if err != nil {
			return err
		}
		artifactCount++
		leafEntries, err = checkedAddSPFreshBulk(leafEntries, evidence.leafEntryCount, "single-tree leaf entries")
		if err != nil {
			return err
		}
		if err := tree.add(evidence); err != nil {
			return err
		}
	}
	if leafEntries != run.sourceRows || tree.leafEntries != run.sourceRows || run.leafEntryCount != run.sourceRows ||
		tree.partitions != run.builtPartitionCount || tree.leaves != run.builtLeafPartitions || tree.maxPartitionKey != run.builtMaxPartitionKey {
		return errors.New("SPFresh single-tree proof does not match source or worker tree facts")
	}
	metrics.SPFreshBulkProofDuration.WithLabelValues("single_tree").Observe(time.Since(started).Seconds())
	logSPFreshBulkImportPhase(run, "tree", "prove_stage", started, "", artifactCount, 0, 0)
	return nil
}

// writeSPFreshFinalManifest combines completed shard proofs with merge/root
// proof, checks aggregate run counts, then immutably writes final.json.
func writeSPFreshFinalManifest(ctx context.Context, run *spfreshBulkRunContext) error {
	if run == nil {
		return errors.New("SPFresh FinalManifest requires run context")
	}
	backend, err := storage.ParseBackend(run.cloudStorageURI, nil)
	if err != nil {
		return errors.Annotate(err, "parse SPFresh FinalManifest storage")
	}
	objectStore, err := storage.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		return errors.Annotate(err, "open SPFresh FinalManifest storage")
	}
	defer objectStore.Close()
	return writeSPFreshFinalManifestToStore(ctx, objectStore, run, run.staging.append)
}

// writeSPFreshFinalManifestToStore completes the TiDB artifact trust boundary.
// Shard evidence comes from authenticated bytes consumed alongside memory
// import, never from worker counts alone. Merge/root use the same consumer.
func writeSPFreshFinalManifestToStore(
	ctx context.Context,
	objectStore storage.ExternalStorage,
	run *spfreshBulkRunContext,
	stage spfreshBulkRunConsumer,
) error {
	started := time.Now()
	if run == nil || run.distributedBuild == nil {
		return errors.New("SPFresh FinalManifest requires completed distributed build")
	}
	distributed := run.distributedBuild
	identity := spfreshBulkBuildIdentity(run)
	plan := distributed.plan
	if plan.Identity != identity || plan.SourceRows != run.sourceRows ||
		plan.SourceFingerprint != run.sourceFingerprint ||
		plan.CoarseShardCount == 0 || len(distributed.shardInputRefs) == 0 ||
		len(distributed.shardResultRefs) != len(distributed.shardInputRefs) ||
		len(distributed.shardEvidence) != len(distributed.shardInputRefs) {
		return errors.New("SPFresh FinalManifest BuildPlan does not match accepted run")
	}

	evidence := distributed.shardEvidence

	combineStarted := time.Now()
	var inputFingerprint pkdbspfreshbulk.FingerprintAccumulator
	var inputRows uint64
	var tree spfreshBulkTreeEvidence
	shardRuns := make([]pkdbspfreshbulk.FinalShardRun, len(evidence))
	for shardIndex, shard := range evidence {
		var err error
		inputRows, err = checkedAddSPFreshBulk(inputRows, shard.rows, "ShardInput rows")
		if err != nil {
			return err
		}
		if err := inputFingerprint.Merge(shard.fingerprint); err != nil {
			return errors.Annotatef(err, "combine SPFresh ShardInput %d fingerprint", shardIndex)
		}
		if err := tree.add(shard.output); err != nil {
			return err
		}
		shardRuns[shardIndex] = shard.run
	}

	if inputRows != run.sourceRows || inputFingerprint.Fingerprint() != run.sourceFingerprint {
		return errors.New("SPFresh combined ShardInputs do not conserve source rows and fingerprint")
	}

	if distributed.merge.MergeNonRoot == nil {
		if distributed.merge.MergePartitionCount != 0 || distributed.merge.MergeNonRootObjectBytes != 0 {
			return errors.New("SPFresh direct root has inconsistent merge non-root facts")
		}
	} else {
		lastRange, err := pkdbspfreshbulk.PartitionRange(plan.SourceRows, uint32(len(distributed.shardInputRefs)-1))
		if err != nil {
			return err
		}
		lastShardMax := lastRange.MaxPartitionKey
		if lastShardMax == math.MaxUint64 {
			return errors.New("SPFresh merge partition range overflows")
		}
		mergeStart := lastShardMax + 1
		mergeEvidence, err := inspectSPFreshPinnedSortedRun(
			ctx, objectStore, run, *distributed.merge.MergeNonRoot, spfreshBulkNonRootRole,
			"merge-non-root", mergeStart, distributed.merge.MaxPartitionKey, stage,
		)
		if err != nil {
			return err
		}
		if mergeEvidence.partitionCount != distributed.merge.MergePartitionCount || mergeEvidence.leafPartitionCount != 0 {
			return errors.New("SPFresh merge non-root partition count does not match merge result")
		}
		if err := tree.add(mergeEvidence); err != nil {
			return err
		}
	}
	// The root is a single partition and is consumed last, after every
	// non-root import and assignment/conservation check has succeeded.
	if err := tree.add(spfreshBulkRunEvidence{partitionCount: 1, maxPartitionKey: pkdbspfreshbulk.RootPartitionKey}); err != nil {
		return err
	}
	if tree.leafEntries != run.sourceRows || tree.maxPartitionKey != distributed.merge.MaxPartitionKey ||
		distributed.merge.SourceRows != run.sourceRows || distributed.merge.Fingerprint != run.sourceFingerprint {
		return errors.New("SPFresh merged tree does not conserve final rows, fingerprint, or max key")
	}

	manifest := pkdbspfreshbulk.FinalManifest{
		Identity: identity, ShardRuns: shardRuns,
		MergeNonRoot: distributed.merge.MergeNonRoot, Root: distributed.merge.Root,
		SourceRows: run.sourceRows, LeafEntryCount: tree.leafEntries, Fingerprint: run.sourceFingerprint,
		PartitionCount: tree.partitions, LeafPartitionCount: tree.leaves,
		RootLevel: distributed.merge.RootLevel, MaxPartitionKey: tree.maxPartitionKey,
	}
	manifestBytes, err := pkdbspfreshbulk.EncodeFinalManifestJSON(manifest)
	if err != nil {
		return errors.Annotate(err, "encode SPFresh FinalManifest")
	}
	const manifestKey = "final.json"
	if err := pkdbspfreshbulk.WriteImmutableObject(ctx, objectStore, manifestKey, manifestBytes); err != nil {
		return err
	}
	run.finalManifestRef = pkdbspfreshbulk.NewManifestRef(run.artifactPrefix+"/"+manifestKey, manifestBytes)
	run.finalManifest = &manifest
	run.leafEntryCount = manifest.LeafEntryCount
	if _, err := inspectSPFreshPinnedSortedRun(ctx, objectStore, run, distributed.merge.Root, spfreshBulkRootRole,
		"merge-root", pkdbspfreshbulk.RootPartitionKey, pkdbspfreshbulk.RootPartitionKey, stage); err != nil {
		return err
	}
	metrics.SPFreshBulkProofDuration.WithLabelValues("combine").Observe(time.Since(combineStarted).Seconds())
	metrics.SPFreshBulkProofDuration.WithLabelValues("total").Observe(time.Since(started).Seconds())
	return nil
}

// proveSPFreshBulkShard authenticates one shard without modifying shared run state.
func proveSPFreshBulkShard(ctx context.Context, objectStore storage.ExternalStorage, run *spfreshBulkRunContext, plan pkdbspfreshbulk.BuildPlan, shardIndex int, inputReference pkdbspfreshbulk.ManifestRef, awaitResult spfreshBulkShardResult, stage spfreshBulkRunConsumer) (spfreshBulkShardEvidence, error) {
	identity := spfreshBulkBuildIdentity(run)
	started := time.Now()
	inputEvidence, err := readSPFreshShardInputEvidence(
		ctx, objectStore, run, identity, uint32(shardIndex), plan.SourceRows,
		len(plan.Endpoints), inputReference,
	)
	shardID := uint32(shardIndex)
	logSPFreshDistributedStage(run.buildID, "prove_shard_input", &shardID, "", inputEvidence.rows, inputEvidence.bytes, started, err)
	if err != nil {
		return spfreshBulkShardEvidence{}, err
	}
	metrics.SPFreshBulkProofDuration.WithLabelValues("input").Observe(time.Since(started).Seconds())
	reference, err := awaitResult()
	if err != nil {
		return spfreshBulkShardEvidence{}, err
	}
	started = time.Now()

	expectedResultURI := fmt.Sprintf("%s/shards/%d/result.json", run.artifactPrefix, shardIndex)
	encoded, err := readSPFreshPinnedObject(ctx, objectStore, run.artifactPrefix, reference, expectedResultURI)
	if err != nil {
		return spfreshBulkShardEvidence{}, err
	}
	result, err := pkdbspfreshbulk.DecodeShardResultJSON(encoded)
	if err != nil {
		return spfreshBulkShardEvidence{}, errors.Annotatef(err, "decode SPFresh ShardResult %d", shardIndex)
	}
	plannedRange, err := pkdbspfreshbulk.PartitionRange(plan.SourceRows, uint32(shardIndex))
	if err != nil {
		return spfreshBulkShardEvidence{}, err
	}
	expectedRunURI := fmt.Sprintf("%s/shards/%d/non-root.spfmvp", run.artifactPrefix, shardIndex)
	if result.Identity != identity || result.ShardID != uint32(shardIndex) ||
		result.LeafEntryCount != inputEvidence.rows ||
		result.Fingerprint != inputEvidence.fingerprint ||
		result.MinPartitionKey != plannedRange.MinPartitionKey ||
		result.MaxPartitionKey > plannedRange.MaxPartitionKey || result.NonRoot.URI != expectedRunURI {
		return spfreshBulkShardEvidence{}, errors.Errorf("SPFresh ShardResult %d does not match BuildPlan", shardIndex)
	}
	runEvidence, err := inspectSPFreshPinnedSortedRun(
		ctx, objectStore, run, result.NonRoot, spfreshBulkNonRootRole,
		fmt.Sprintf("shard-%d-non-root", shardIndex), result.MinPartitionKey, result.MaxPartitionKey, stage,
	)
	if err != nil {
		return spfreshBulkShardEvidence{}, err
	}
	if runEvidence.leafEntryCount != result.LeafEntryCount {
		return spfreshBulkShardEvidence{}, errors.Errorf("SPFresh shard %d leaf count does not match ShardInput", shardIndex)
	}
	metrics.SPFreshBulkProofDuration.WithLabelValues("output").Observe(time.Since(started).Seconds())
	// Unlike prove_stage_shard, this interval excludes input proof and RPC waiting.
	logSPFreshDistributedStage(run.buildID, "prove_shard_output", &shardID, "", inputEvidence.rows, 0, started, nil)
	return spfreshBulkShardEvidence{
		rows: inputEvidence.rows, fingerprint: inputEvidence.fingerprint,
		output: runEvidence,
		run: pkdbspfreshbulk.FinalShardRun{
			ShardID: result.ShardID, NonRoot: result.NonRoot,
			LeafEntryCount: result.LeafEntryCount, Fingerprint: result.Fingerprint,
			MinPartitionKey: result.MinPartitionKey, MaxPartitionKey: result.MaxPartitionKey,
		},
	}, nil
}

func readSPFreshPinnedObject(
	ctx context.Context,
	objectStore storage.ExternalStorage,
	prefix string,
	reference pkdbspfreshbulk.ManifestRef,
	expectedURI string,
) ([]byte, error) {
	if reference.URI != expectedURI {
		return nil, errors.Errorf("SPFresh object URI %q, expected %q", reference.URI, expectedURI)
	}
	key, err := exactSPFreshBulkObjectKey(prefix, reference.URI)
	if err != nil {
		return nil, err
	}
	encoded, err := pkdbspfreshbulk.ReadObject(ctx, objectStore, key)
	if err != nil {
		return nil, err
	}
	if err := reference.VerifyBytes(encoded); err != nil {
		return nil, err
	}
	return encoded, nil
}

// readSPFreshShardInputEvidence authenticates the small immutable assignment.
// Workers own fragment consumption and return its actual conservation facts.
func readSPFreshShardInputEvidence(
	ctx context.Context,
	objectStore storage.ExternalStorage,
	run *spfreshBulkRunContext,
	identity pkdbspfreshbulk.BuildIdentity,
	shardID uint32,
	sourceRows uint64,
	workerCount int,
	reference pkdbspfreshbulk.ManifestRef,
) (spfreshBulkShardInputEvidence, error) {
	expectedInputURI := fmt.Sprintf("%s/shards/%d/input.json", run.artifactPrefix, shardID)
	encoded, err := readSPFreshPinnedObject(
		ctx, objectStore, run.artifactPrefix, reference, expectedInputURI,
	)
	if err != nil {
		return spfreshBulkShardInputEvidence{}, err
	}
	input, err := pkdbspfreshbulk.DecodeShardInputJSON(encoded)
	if err != nil {
		return spfreshBulkShardInputEvidence{},
			errors.Annotatef(err, "decode SPFresh ShardInput %d", shardID)
	}
	if input.Identity != identity || input.ShardID != shardID || input.Rows == 0 ||
		input.Rows > sourceRows || len(input.RaBitQFragments) == 0 {
		return spfreshBulkShardInputEvidence{},
			errors.Errorf("SPFresh ShardInput %d does not match FinalManifest authority", shardID)
	}

	// The worker consumes these pinned fragments. TiDB checks the assignment
	// boundary without downloading or decoding the same row data again.
	for _, reference := range input.RaBitQFragments {
		if _, _, _, err := parseSPFreshRabitqFragmentURI(run.artifactPrefix, shardID, workerCount, reference.URI); err != nil {
			return spfreshBulkShardInputEvidence{}, err
		}
	}
	return spfreshBulkShardInputEvidence{rows: input.Rows, bytes: input.Bytes, fingerprint: input.Fingerprint}, nil
}

func parseSPFreshRabitqFragmentURI(artifactPrefix string, taskID uint32, workerCount int, uri string) (uint64, uint64, string, error) {
	relative, ok := strings.CutPrefix(uri, artifactPrefix+"/")
	if !ok {
		return 0, 0, "", errors.New("SPFresh fragment is outside its build prefix")
	}
	parts := strings.Split(relative, "/")
	if len(parts) != 6 || (parts[0] != "shuffle" && parts[0] != "refine") || parts[1] != "shards" || parts[3] != "input" {
		return 0, 0, "", errors.New("SPFresh fragment has no current producer namespace")
	}
	sourceID, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil || parts[2] != strconv.FormatUint(sourceID, 10) || (parts[0] == "refine" && sourceID != uint64(taskID)) {
		return 0, 0, "", errors.New("SPFresh fragment shard identity is invalid")
	}
	var workerID, ordinal uint64
	suffix := strings.Join(parts[4:], "/")
	if matched, err := fmt.Sscanf(suffix, "worker-%d/part-%d.spfmvp", &workerID, &ordinal); err != nil || matched != 2 || suffix != fmt.Sprintf("worker-%05d/part-%05d.spfmvp", workerID, ordinal) || workerCount <= 0 || workerID >= uint64(workerCount) || (parts[0] == "refine" && workerID != 0) {
		return 0, 0, "", errors.New("SPFresh fragment is not a canonical producer output")
	}
	artifactID := fmt.Sprintf("%s-shard-%d-worker-%05d-input-%05d", parts[0], sourceID, workerID, ordinal)
	return workerID, ordinal, artifactID, nil
}

// spfreshBulkHasLeafSummary matches TiKV's partition_meta::leaf_summary_kind.
// Other legal STORING types carry sidecars without leaf summaries.
func spfreshBulkHasLeafSummary(columns []pkdbspfreshbulk.StoredColumn) (bool, error) {
	hasSummary := false
	for _, column := range columns {
		var fieldType tipb.FieldType
		if err := fieldType.Unmarshal(column.FieldType); err != nil {
			return false, errors.Annotatef(err, "decode SPFresh stored column %d field type", column.ColumnID)
		}
		switch fieldType.Tp {
		case int32(mysql.TypeTiny), int32(mysql.TypeShort), int32(mysql.TypeLong), int32(mysql.TypeLonglong), int32(mysql.TypeInt24),
			int32(mysql.TypeYear), int32(mysql.TypeDuration), int32(mysql.TypeNewDate), int32(mysql.TypeDate), int32(mysql.TypeDatetime), int32(mysql.TypeTimestamp):
			hasSummary = true
		}
	}
	return hasSummary, nil
}

// inspectSPFreshPinnedSortedRun checks one run's identity, key range and
// framing while consuming the same bytes for memory import.
func inspectSPFreshPinnedSortedRun(
	ctx context.Context,
	objectStore storage.ExternalStorage,
	run *spfreshBulkRunContext,
	reference pkdbspfreshbulk.ManifestRef,
	expectedRole, expectedArtifactID string,
	minPartitionKey, maxPartitionKey uint64,
	stage spfreshBulkRunConsumer,
) (spfreshBulkRunEvidence, error) {
	hasLeafSummary, err := spfreshBulkHasLeafSummary(run.storedColumns)
	if err != nil {
		return spfreshBulkRunEvidence{}, err
	}
	encoded, release, err := readSPFreshBudgetedPinnedSortedRun(ctx, objectStore, run.artifactPrefix, reference)
	if err != nil {
		return spfreshBulkRunEvidence{}, err
	}
	defer release()
	header, records, err := pkdbspfreshbulk.DecodeSortedRun(encoded)
	if err != nil {
		return spfreshBulkRunEvidence{}, errors.Annotatef(err, "decode SPFresh SortedRun %q", reference.URI)
	}
	if header.BuildID != run.buildID || header.ArtifactID != expectedArtifactID ||
		header.TableID != run.tableID || header.IndexID != run.indexID ||
		header.Dimension != run.dimension || header.Distance != run.distance ||
		header.Producer != "tikv-worker" || header.RunRole != expectedRole ||
		header.AlgorithmSeed != run.algorithmSeed || header.TargetOccupancy != run.targetOccupancy ||
		header.MinPartitionSize != run.minPartitionSize || header.MaxPartitionSize != run.maxPartitionSize {
		return spfreshBulkRunEvidence{}, errors.Errorf("SPFresh SortedRun %q identity does not match admitted build", reference.URI)
	}
	prefix, err := spfreshBulkIndexKeyPrefix(run)
	if err != nil {
		return spfreshBulkRunEvidence{}, err
	}
	evidence := spfreshBulkRunEvidence{recordCount: uint64(len(records)), minPartitionKey: math.MaxUint64}
	// Count partitions in the already ordered import stream. Do not retain
	// handles, child edges or a reconstructed tree.
	var currentPartition uint64
	var level uint32
	var entries, expectedEntries uint64
	var metadata uint8
	leafValueBytes := 12 + (int(run.dimension)+7)/8 + (int(run.dimension)*6+7)/8
	finishPartition := func() error {
		expectedMetadata := uint8(7)
		if level == spfreshBulkLeafPartitionLevel && hasLeafSummary {
			expectedMetadata |= 8
		}
		if metadata != expectedMetadata || level == 0 || entries == 0 || entries != expectedEntries {
			return errors.New("SPFresh partition metadata and entries do not agree")
		}
		evidence.partitionCount++
		if level == spfreshBulkLeafPartitionLevel {
			evidence.leafPartitionCount++
			evidence.leafEntryCount += entries
		}
		return nil
	}
	for _, record := range records {
		partitionKey, dataLevel, suffix, err := decodeSPFreshBulkLogicalKey(record.Key, prefix)
		if err != nil {
			return spfreshBulkRunEvidence{}, err
		}
		if partitionKey < minPartitionKey || partitionKey > maxPartitionKey {
			return spfreshBulkRunEvidence{}, errors.New("SPFresh partition is outside its assigned run range")
		}
		if currentPartition != partitionKey {
			if currentPartition != 0 {
				if err := finishPartition(); err != nil {
					return spfreshBulkRunEvidence{}, err
				}
			}
			currentPartition, level, entries, expectedEntries, metadata = partitionKey, 0, 0, 0, 0
		}
		evidence.minPartitionKey = min(evidence.minPartitionKey, partitionKey)
		evidence.maxPartitionKey = max(evidence.maxPartitionKey, partitionKey)
		if dataLevel == 0 {
			if len(suffix) != 1 {
				return spfreshBulkRunEvidence{}, errors.New("SPFresh metadata key has invalid suffix")
			}
			switch suffix[0] {
			case spfreshBulkStateMetadataItem:
				if len(record.Value) != spfreshBulkPartitionStateValueLen || binary.BigEndian.Uint32(record.Value[4:8]) != spfreshBulkReadyPartitionState {
					return spfreshBulkRunEvidence{}, errors.New("SPFresh partition state is invalid")
				}
				level = binary.BigEndian.Uint32(record.Value[:4])
				metadata |= 1
				if partitionKey == pkdbspfreshbulk.RootPartitionKey {
					evidence.rootLevel = level
				}
			case spfreshBulkCentroidMetadataItem:
				if err := validateSPFreshBulkCentroidBytes(record.Value, run.distance, run.dimension, false); err != nil {
					return spfreshBulkRunEvidence{}, errors.Annotate(err, "validate SPFresh partition centroid metadata")
				}
				metadata |= 2
			case spfreshBulkHintsMetadataItem:
				if len(record.Value) != spfreshBulkPartitionHintsValueLen {
					return spfreshBulkRunEvidence{}, errors.New("SPFresh partition hints are invalid")
				}
				expectedEntries = uint64(binary.BigEndian.Uint32(record.Value[8:12]))
				metadata |= 4
			case 3:
				if level != spfreshBulkLeafPartitionLevel || !hasLeafSummary || len(record.Value) == 0 {
					return spfreshBulkRunEvidence{}, errors.New("SPFresh leaf summary is invalid")
				}
				metadata |= 8
			default:
				return spfreshBulkRunEvidence{}, errors.New("SPFresh run has unknown partition metadata")
			}
		} else {
			if dataLevel != level || len(suffix) != spfreshBulkPartitionKeyLength || len(record.Value) == 0 {
				return spfreshBulkRunEvidence{}, errors.New("SPFresh entry has invalid level, key or value")
			}
			valueBytes := len(record.Value)
			if level == spfreshBulkLeafPartitionLevel {
				if valueBytes < leafValueBytes || len(run.storedColumns) == 0 && valueBytes != leafValueBytes {
					return spfreshBulkRunEvidence{}, errors.New("SPFresh leaf value length is invalid")
				}
			} else if err := validateSPFreshBulkCentroidBytes(record.Value, run.distance, run.dimension, true); err != nil {
				return spfreshBulkRunEvidence{}, errors.Annotate(err, "validate SPFresh interior centroid")
			}
			entries++
		}
	}
	if err := finishPartition(); err != nil {
		return spfreshBulkRunEvidence{}, err
	}
	if evidence.recordCount != header.RecordCount || evidence.minPartitionKey != minPartitionKey || evidence.maxPartitionKey != maxPartitionKey || evidence.leafEntryCount != header.LeafEntryCount {
		return spfreshBulkRunEvidence{}, errors.New("SPFresh run counts or key range do not match its header")
	}
	if expectedRole == spfreshBulkRootRole {
		expectedLevel := run.builtRootLevel
		if run.distributedBuild != nil {
			expectedLevel = run.distributedBuild.merge.RootLevel
		}
		expectedLeaves := uint64(0)
		if expectedLevel == spfreshBulkLeafPartitionLevel {
			expectedLeaves = run.sourceRows
		}
		if evidence.partitionCount != 1 || evidence.leafEntryCount != expectedLeaves {
			return spfreshBulkRunEvidence{}, errors.New("SPFresh root counts differ from build result")
		}
		if evidence.rootLevel != expectedLevel {
			return spfreshBulkRunEvidence{}, errors.New("SPFresh root level differs from build result")
		}
	}
	// Keep the immutable body under its read lease until all memory imports and
	// retries have drained, including on cancellation. No second read is needed.
	started := time.Now()
	if err := stage(ctx, spfreshBulkSortedRun{header: header, records: records, encodedBytes: uint64(len(encoded))}, reference); err != nil {
		return spfreshBulkRunEvidence{}, err
	}
	metrics.SPFreshBulkProofDuration.WithLabelValues("import_memory").Observe(time.Since(started).Seconds())
	return evidence, nil
}

func spfreshBulkIndexKeyPrefix(run *spfreshBulkRunContext) ([]byte, error) {
	if run.keyspaceID == math.MaxUint32 {
		return tablecodec.EncodeIndexSeekKey(run.tableID, run.indexID, nil), nil
	}
	if run.keyspaceID == 0 || run.keyspaceID > 0x00ff_ffff {
		return nil, errors.New("SPFresh keyspace ID does not fit API V2 prefix")
	}
	prefix := []byte{'x', byte(run.keyspaceID >> 16), byte(run.keyspaceID >> 8), byte(run.keyspaceID)}
	return append(prefix, tablecodec.EncodeIndexSeekKey(run.tableID, run.indexID, nil)...), nil
}

func decodeSPFreshBulkLogicalKey(key, prefix []byte) (uint64, uint32, []byte, error) {
	minimum := len(prefix) + spfreshBulkPartitionKeyLength + spfreshBulkPartitionLevelLength + 1
	if len(key) < minimum || !bytes.HasPrefix(key, prefix) {
		return 0, 0, nil, errors.New("SPFresh logical key has wrong identity prefix or is truncated")
	}
	partitionOffset := len(prefix)
	levelOffset := partitionOffset + spfreshBulkPartitionKeyLength
	suffixOffset := levelOffset + spfreshBulkPartitionLevelLength
	return binary.BigEndian.Uint64(key[partitionOffset:levelOffset]),
		binary.BigEndian.Uint32(key[levelOffset:suffixOffset]), key[suffixOffset:], nil
}

// Both partition means and interior routing entries use the worker's Q7C1
// encoding. Check the represented norm rather than trusting its f32 header.
func validateSPFreshBulkCentroidBytes(encoded []byte, distance string, dimension uint32, requireUnit bool) error {
	if dimension == 0 || dimension > pkdbspfreshbulk.MaxDimension {
		return errors.New("SPFresh centroid has invalid dimension")
	}
	signBytes := (int(dimension) + 7) / 8
	magnitudeBytes := (int(dimension)*6 + 7) / 8
	if len(encoded) != 20+signBytes+magnitudeBytes || string(encoded[:4]) != "Q7C1" ||
		binary.LittleEndian.Uint32(encoded[4:8]) != dimension {
		return errors.New("SPFresh centroid has invalid Q7C1 encoding")
	}
	var header [3]float32
	for i := range header {
		header[i] = math.Float32frombits(binary.LittleEndian.Uint32(encoded[8+i*4 : 12+i*4]))
		if math.IsNaN(float64(header[i])) || math.IsInf(float64(header[i]), 0) || header[i] < 0 {
			return errors.New("SPFresh centroid has invalid Q7 metadata")
		}
	}
	magnitudes := encoded[20+signBytes:]
	var codeNormSquared uint64
	for i := 0; i < int(dimension); i++ {
		bit := i * 6
		offset := bit / 8
		word := uint16(magnitudes[offset])
		if offset+1 < len(magnitudes) {
			word |= uint16(magnitudes[offset+1]) << 8
		}
		code := uint64((word >> (bit % 8)) & 63)
		codeNormSquared += code * code
	}
	scale := float64(header[0])
	normSquared := scale * scale * float64(codeNormSquared)
	if header[1] != float32(normSquared) {
		return errors.New("SPFresh centroid squared norm does not match its Q7 codes and scale")
	}
	if distance == "cosine" && requireUnit && math.Abs(math.Sqrt(normSquared)-1) > 1e-4 {
		return errors.New("SPFresh COSINE centroid is not unit normalized")
	}
	return nil
}

func checkedAddSPFreshBulk(left, right uint64, label string) (uint64, error) {
	if math.MaxUint64-left < right {
		return 0, errors.Errorf("SPFresh %s overflow", label)
	}
	return left + right, nil
}
