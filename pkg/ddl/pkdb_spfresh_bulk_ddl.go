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
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/tikvapi"
	"go.uber.org/zap"
)

const (
	spfreshBulkAlgorithmSeed   = uint64(20260831)
	spfreshBulkTargetOccupancy = uint32(75)
	spfreshBulkBucket          = "spfresh-mvp"
)

var spfreshBulkCloudStorageURI = func() string {
	return variable.CloudStorageURI.Load()
}

// spfreshReorgState freezes the route for one admitted snapshot, including an empty URI.
// A missing bulk context must not change the route persisted in the DDL job.
type spfreshReorgState struct {
	capturedStartTS uint64
	legacyRoute     *backend.SPFreshBuildRoutePlan
	bulkSelected    bool
	bulk            *spfreshBulkRunContext
}

type spfreshBulkRunContext struct {
	storedColumns        []pkdbspfreshbulk.StoredColumn
	handleColumn         *pkdbspfreshbulk.HandleColumn
	buildID              string
	runID                string
	cloudStorageURI      string
	sourceSnapshotTS     uint64
	keyspaceID           uint32
	tableID              int64
	indexID              int64
	vectorColumnID       int64
	dimension            uint32
	distance             string
	algorithmSeed        uint64
	targetOccupancy      uint32
	minPartitionSize     uint64
	maxPartitionSize     uint64
	buildShard           uint32
	workerURL            string
	workerEndpoints      []string
	workerMemoryBudgets  []uint64
	artifactPrefix       string
	baseFragmentURIs     []string
	baseFragmentBytes    []uint64
	baseFragmentSHA256   []string
	baseFragmentRefs     []pkdbspfreshbulk.ManifestRef
	sourceRows           uint64
	sourceRawBytes       uint64
	baseObjectBytes      uint64
	sourceFingerprint    pkdbspfreshbulk.MultisetFingerprint
	sampleFragmentRefs   []pkdbspfreshbulk.ManifestRef
	sampleRows           uint64
	sampleObjectBytes    uint64
	sampleFingerprint    pkdbspfreshbulk.MultisetFingerprint
	shardCount           uint32
	nonRootRunURI        string
	rootRunURI           string
	leafEntryCount       uint64
	nonRootRecordCount   uint64
	nonRootObjectBytes   uint64
	nonRootRef           pkdbspfreshbulk.ManifestRef
	rootRecordCount      uint64
	rootObjectBytes      uint64
	rootRef              pkdbspfreshbulk.ManifestRef
	builtPartitionCount  uint64
	builtLeafPartitions  uint64
	builtRootLevel       uint32
	builtMaxPartitionKey uint64
	bulkImportTS         uint64
	readyIndexTS         uint64
	emptyReady           bool
	rootLevel            uint32
	readyStartedAt       time.Time
	distributedBuild     *spfreshBulkDistributedResult
	finalManifestRef     pkdbspfreshbulk.ManifestRef
	finalManifest        *pkdbspfreshbulk.FinalManifest
	importCompleted      bool
	staging              *spfreshBulkStaging
}

type spfreshBulkStages struct {
	building          func(context.Context, *spfreshBulkRunContext) error
	scanAndUpload     func(context.Context, *spfreshBulkRunContext) error
	buildAndWriteRuns func(context.Context, *spfreshBulkRunContext) error
	importRuns        func(context.Context, *spfreshBulkRunContext) error
	verify            func(context.Context, *spfreshBulkRunContext) error
	publish           func(context.Context, *spfreshBulkRunContext) error
	publishEmpty      func(context.Context, *spfreshBulkRunContext) error
}

func admitSPFreshBulkBuild(
	w *worker,
	job *model.Job,
	tblInfo *model.TableInfo,
	indexInfos []*model.IndexInfo,
) (bool, error) {
	if !hasSPFreshIndex(indexInfos) {
		return false, nil
	}
	if job == nil || job.ReorgMeta == nil {
		return false, errors.New("SPFresh MVP bulk admission requires DDL reorg metadata")
	}
	if job.RealStartTS == 0 {
		return false, errors.New("SPFresh build admission requires a base timestamp")
	}
	state := &w.jobContext(job.ID, job.ReorgMeta).spfresh
	if state.capturedStartTS != 0 && state.capturedStartTS != job.RealStartTS {
		if job.SchemaState != model.StateNone {
			return state.bulkSelected, errors.New("SPFresh build admission snapshot changed after schema progress; cancel and rebuild")
		}
		// A failed first DDL transaction reloads the job with a new RealStartTS.
		// Discard that transaction's route and source context before retrying admission.
		*state = spfreshReorgState{}
	}
	wasCaptured := state.capturedStartTS != 0
	run, selected, err := prepareSPFreshBulkAdmissionOnce(
		state,
		job,
		tblInfo,
		indexInfos,
		func() (uint32, []string, error) {
			targets, err := resolveSPFreshWorkerTargets(w.ctx, w.store)
			return targets.keyspaceID, targets.workerAddrs, err
		},
	)
	job.ReorgMeta.UseCloudStorage = selected
	if err != nil || !selected {
		return selected, err
	}
	if run.sourceSnapshotTS != job.RealStartTS {
		return true, errors.New("SPFresh bulk source snapshot differs from protected build admission; cancel and rebuild")
	}
	if wasCaptured {
		return true, nil
	}
	logutil.DDLLogger().Info("SPFresh MVP bulk admission completed",
		zap.String("path", "bulk"),
		zap.String("build_id", run.buildID),
		zap.String("run_id", run.runID),
		zap.Uint64("source_snapshot_ts", run.sourceSnapshotTS),
		zap.Uint32("keyspace_id", run.keyspaceID),
		zap.Int64("table_id", run.tableID),
		zap.Int64("index_id", run.indexID),
		zap.String("worker", run.workerURL),
		zap.String("artifact_uri", run.artifactPrefix),
		zap.String("stage", "admission"))
	return true, nil
}

func initSPFreshReorg(
	w *worker,
	jobCtx *jobContext,
	job *model.Job,
	tblInfo *model.TableInfo,
	indexInfos []*model.IndexInfo,
) error {
	if job.MultiSchemaInfo != nil {
		return errSPFreshMultiSchemaChange
	}
	selected, err := admitSPFreshBulkBuild(w, job, tblInfo, indexInfos)
	if err != nil {
		return err
	}
	if selected {
		run := w.jobContext(job.ID, job.ReorgMeta).spfresh.bulk
		if err := jobCtx.metaMut.RegisterSPFreshBulkCleanup(meta.SPFreshBulkCleanup{
			JobID: job.ID, RunID: run.runID, StorageURI: run.cloudStorageURI,
			WorkerEndpoints: run.workerEndpoints,
		}); err != nil {
			return err
		}
		job.ReorgMeta.SPFreshBulkRunID = run.runID
		return nil
	}
	// SPFresh builds publish through Storage; they never merge local temp indexes.
	job.ReorgMeta.ReorgTp = model.ReorgTypeIngest
	return nil
}

// isSPFreshBulkReorg uses durable job metadata so losing the in-process context
// cannot cause a bulk build to fall back to the ordinary backfill path.
func isSPFreshBulkReorg(job *model.Job, indexInfos []*model.IndexInfo) bool {
	return job != nil && job.ReorgMeta != nil && job.ReorgMeta.UseCloudStorage &&
		len(indexInfos) == 1 && indexInfos[0] != nil && indexInfos[0].IsSPFreshIndex()
}

func prepareSPFreshBulkAdmissionOnce(
	state *spfreshReorgState,
	job *model.Job,
	tblInfo *model.TableInfo,
	indexInfos []*model.IndexInfo,
	resolveTargets func() (uint32, []string, error),
) (*spfreshBulkRunContext, bool, error) {
	if state.capturedStartTS != 0 {
		if state.bulkSelected && state.bulk == nil {
			return nil, true, errors.New("SPFresh MVP bulk admission context is missing")
		}
		return state.bulk, state.bulkSelected, nil
	}
	cloudStorageURI := spfreshBulkCloudStorageURI()
	state.capturedStartTS = job.RealStartTS
	state.bulkSelected = cloudStorageURI != ""
	if cloudStorageURI == "" {
		return nil, false, nil
	}
	if job == nil || job.ReorgMeta == nil || !job.ReorgMeta.IsDistReorg {
		return nil, true, errors.New("SPFresh S3 bulk scan requires distributed reorganization")
	}
	if len(indexInfos) != 1 || !indexInfos[0].IsSPFreshIndex() {
		return nil, true, errors.New("SPFresh MVP bulk admission requires exactly one SPFresh index")
	}
	keyspaceID, workerEndpoints, err := resolveTargets()
	if err != nil {
		return nil, true, err
	}
	run, selected, err := prepareSPFreshBulkAdmission(
		cloudStorageURI,
		job,
		tblInfo,
		indexInfos[0],
		keyspaceID,
		workerEndpoints,
	)
	if err == nil && selected {
		state.bulk = run
	}
	return run, selected, err
}

func prepareSPFreshBulkAdmission(
	cloudStorageURI string,
	job *model.Job,
	tblInfo *model.TableInfo,
	indexInfo *model.IndexInfo,
	keyspaceID uint32,
	workerEndpoints []string,
) (*spfreshBulkRunContext, bool, error) {
	if cloudStorageURI == "" {
		return nil, false, nil
	}
	if job == nil || job.ReorgMeta == nil || !job.ReorgMeta.IsDistReorg {
		return nil, true, errors.New("SPFresh S3 bulk scan requires distributed reorganization")
	}
	if job.RealStartTS == 0 {
		return nil, true, errors.New("SPFresh MVP bulk source snapshot timestamp is not set")
	}
	distance, dimension, err := validateSPFreshBulkShape(tblInfo, indexInfo)
	if err != nil {
		return nil, true, err
	}
	minPartitionSize := pkdbspfreshbulk.DefaultMinPartitionSize
	maxPartitionSize := pkdbspfreshbulk.DefaultMaxPartitionSize
	var buildShard uint32
	if options := indexInfo.VectorInfo.IndexOptions; options != nil {
		buildShard = options.BuildShard
		if options.MinPartitionSize != 0 {
			minPartitionSize = options.MinPartitionSize
		}
		if options.MaxPartitionSize != 0 {
			maxPartitionSize = options.MaxPartitionSize
		}
	}
	if maxPartitionSize < 2 {
		return nil, true, errors.New("SPFresh max_partition_size must be at least 2")
	}
	if minPartitionSize > maxPartitionSize {
		return nil, true, errors.New("SPFresh min_partition_size cannot exceed max_partition_size")
	}
	if buildShard > model.SPFreshMaxBuildShards {
		return nil, true, errors.Errorf("SPFresh build_shard must be in [1, %d] when set", model.SPFreshMaxBuildShards)
	}
	runID, artifactPrefix, err := parseSPFreshBulkStorageURI(cloudStorageURI)
	if err != nil {
		return nil, true, err
	}
	// A configured namespace may serve successive DDL jobs. Never let a delayed
	// cleanup from an older job delete a later build's objects.
	if job.ID <= 0 {
		return nil, true, errors.New("SPFresh bulk build requires a persistent DDL job ID")
	}
	parsed, err := storage.ParseRawURL(cloudStorageURI)
	if err != nil {
		return nil, true, err
	}
	parsed.Path = fmt.Sprintf("/%s-j%d", runID, job.ID)
	parsed.RawPath = ""
	cloudStorageURI = parsed.String()
	runID, artifactPrefix, err = parseSPFreshBulkStorageURI(cloudStorageURI)
	if err != nil {
		return nil, true, err
	}
	// Distributed manifests bind BuildID to the immutable object prefix.
	buildID := runID
	workerEndpoints, err = tikvapi.NormalizeEndpoints(workerEndpoints, true)
	if err != nil {
		return nil, true, err
	}
	sort.Strings(workerEndpoints)
	if len(workerEndpoints) == 0 {
		return nil, true, errors.New("SPFresh MVP bulk admission requires at least one worker endpoint")
	}
	storedColumns, handleColumn, err := remote.SPFreshBulkSchema(tblInfo, indexInfo)
	if err != nil {
		return nil, true, err
	}
	workerURL := workerEndpoints[0]
	return &spfreshBulkRunContext{
		storedColumns:    storedColumns,
		handleColumn:     handleColumn,
		buildID:          buildID,
		runID:            runID,
		cloudStorageURI:  cloudStorageURI,
		sourceSnapshotTS: job.RealStartTS,
		keyspaceID:       keyspaceID,
		tableID:          tblInfo.ID,
		indexID:          indexInfo.ID,
		vectorColumnID:   tblInfo.Columns[indexInfo.Columns[0].Offset].ID,
		dimension:        dimension,
		distance:         distance,
		algorithmSeed:    spfreshBulkAlgorithmSeed,
		targetOccupancy:  spfreshBulkTargetOccupancy,
		minPartitionSize: minPartitionSize,
		maxPartitionSize: maxPartitionSize,
		buildShard:       buildShard,
		workerURL:        workerURL,
		workerEndpoints:  append([]string(nil), workerEndpoints...),
		artifactPrefix:   artifactPrefix,
		nonRootRunURI:    artifactPrefix + "/runs/non-root.spfmvp",
		rootRunURI:       artifactPrefix + "/runs/root.spfmvp",
	}, true, nil
}

func validateSPFreshBulkShape(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) (string, uint32, error) {
	if tblInfo == nil || indexInfo == nil {
		return "", 0, errors.New("SPFresh MVP bulk admission requires table and index metadata")
	}
	if tblInfo.Partition != nil {
		return "", 0, errors.New("SPFresh MVP bulk build does not support partitioned tables")
	}
	if !tblInfo.PKIsHandle || tblInfo.IsCommonHandle {
		return "", 0, errors.New("SPFresh MVP bulk table requires a single BIGINT primary-key handle")
	}
	pkColumn := tblInfo.GetPkColInfo()
	if pkColumn == nil || pkColumn.GetType() != mysql.TypeLonglong || mysql.HasUnsignedFlag(pkColumn.GetFlag()) {
		return "", 0, errors.New("SPFresh MVP bulk table requires a signed BIGINT primary-key handle")
	}
	if !indexInfo.IsSPFreshIndex() || indexInfo.VectorInfo == nil || len(indexInfo.Columns) != 1 {
		return "", 0, errors.New("SPFresh MVP bulk build requires one single-column SPFresh vector index")
	}
	if indexInfo.Unique || indexInfo.Primary || indexInfo.Global || indexInfo.MVIndex {
		return "", 0, errors.New("SPFresh MVP bulk build does not support unique, primary, global, or multi-valued vector indexes")
	}
	storedColumns, err := model.ResolveSPFreshStoredColumns(tblInfo, indexInfo)
	if err != nil {
		return "", 0, err
	}
	for _, column := range storedColumns {
		if _, err := validateSPFreshStoredColumnDDL(column); err != nil {
			return "", 0, err
		}
	}
	if indexInfo.VectorInfo.Dimension == 0 || indexInfo.VectorInfo.Dimension > uint64(pkdbspfreshbulk.MaxDimension) {
		return "", 0, errors.Errorf("SPFresh MVP bulk index requires VECTOR dimension in 1..=%d", pkdbspfreshbulk.MaxDimension)
	}
	if indexInfo.VectorInfo.DistanceMetric != model.DistanceMetricL2 &&
		indexInfo.VectorInfo.DistanceMetric != model.DistanceMetricCosine {
		return "", 0, errors.New("SPFresh MVP bulk index requires L2 or COSINE distance")
	}
	distance, ok := model.SPFreshDistanceMetricAPIString(indexInfo.VectorInfo.DistanceMetric)
	if !ok {
		return "", 0, errors.New("SPFresh MVP bulk index distance is not supported")
	}
	columnOffset := indexInfo.Columns[0].Offset
	if columnOffset < 0 || columnOffset >= len(tblInfo.Columns) {
		return "", 0, errors.New("SPFresh MVP bulk vector column offset is invalid")
	}
	vectorColumn := tblInfo.Columns[columnOffset]
	if vectorColumn == pkColumn || vectorColumn.Hidden ||
		vectorColumn.GetType() != mysql.TypeTiDBVectorFloat32 ||
		uint64(vectorColumn.GetFlen()) != indexInfo.VectorInfo.Dimension ||
		!mysql.HasNotNullFlag(vectorColumn.GetFlag()) {
		return "", 0, errors.New("SPFresh MVP bulk table requires one visible fixed-dimension VECTOR NOT NULL column")
	}
	return distance, uint32(indexInfo.VectorInfo.Dimension), nil
}

func parseSPFreshBulkStorageURI(rawURI string) (string, string, error) {
	if _, err := storage.ParseBackend(rawURI, nil); err != nil {
		return "", "", errors.Annotate(err, "parse SPFresh MVP bulk storage URI")
	}
	parsed, err := storage.ParseRawURL(rawURI)
	if err != nil {
		return "", "", errors.Annotate(err, "parse SPFresh MVP bulk storage URI")
	}
	if parsed.Scheme != "s3" || !strings.EqualFold(parsed.Host, spfreshBulkBucket) {
		return "", "", errors.Errorf("SPFresh MVP bulk storage URI must use s3://%s/<run-id>", spfreshBulkBucket)
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return "", "", errors.New("SPFresh MVP bulk storage URI does not allow userinfo or fragment")
	}
	runID := strings.Trim(parsed.EscapedPath(), "/")
	decodedRunID, err := url.PathUnescape(runID)
	if err != nil || decodedRunID == "" || decodedRunID == "." || decodedRunID == ".." || strings.Contains(decodedRunID, "/") {
		return "", "", errors.New("SPFresh MVP bulk storage URI must contain exactly one run-id path component")
	}
	if len(decodedRunID) > 128 {
		return "", "", errors.New("SPFresh MVP bulk run-id exceeds 128 bytes")
	}
	for _, char := range decodedRunID {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char == '-' || char == '_' || char == '.') {
			return "", "", errors.New("SPFresh MVP bulk run-id contains an invalid character")
		}
	}
	return decodedRunID, fmt.Sprintf("s3://%s/%s", spfreshBulkBucket, url.PathEscape(decodedRunID)), nil
}

func productionSPFreshBulkStages(w *worker, job *model.Job, tbl table.Table, reorgCtx *ReorgContext) spfreshBulkStages {
	return spfreshBulkStages{
		building: func(ctx context.Context, run *spfreshBulkRunContext) error {
			return remote.InitSPFreshBulkBuilding(
				ctx,
				util.InternalHTTPClient(),
				run.workerURL,
				run.buildID,
				run.keyspaceID,
				run.tableID,
				run.indexID,
				run.sourceSnapshotTS,
			)
		},
		scanAndUpload: func(ctx context.Context, run *spfreshBulkRunContext) error {
			return scanAndUploadSPFreshBaseFragments(ctx, w, tbl, reorgCtx, run, productionSPFreshBulkScanConfig)
		},
		buildAndWriteRuns: func(ctx context.Context, run *spfreshBulkRunContext) error {
			if run.shardCount == 1 {
				return buildAndWriteSPFreshSortedRuns(ctx, util.InternalHTTPClient(), run)
			}
			staging, err := newSPFreshBulkStaging(ctx, w.store, job, run)
			if err != nil {
				return err
			}
			run.staging = staging
			if err := coordinateSPFreshDistributedBuild(ctx, util.InternalHTTPClient(), run); err != nil {
				return err
			}
			return writeSPFreshFinalManifest(ctx, run)
		},
		importRuns: func(ctx context.Context, run *spfreshBulkRunContext) error {
			if run.shardCount == 1 {
				return importSPFreshSortedRuns(ctx, w.store, job, run)
			}
			return importSPFreshFinalManifestRuns(ctx, run)
		},
		verify: func(ctx context.Context, run *spfreshBulkRunContext) error {
			if run.shardCount == 1 {
				return verifySPFreshSingleImportState(run)
			}
			return verifySPFreshDistributedImport(ctx, run)
		},
		publish: publishSPFreshBulkReady,
		publishEmpty: func(ctx context.Context, run *spfreshBulkRunContext) error {
			clusterID, keyspaceID, err := resolveSPFreshStoreIdentity(ctx, w.store)
			if err != nil {
				return err
			}
			if keyspaceID != run.keyspaceID {
				return errors.New("SPFresh bulk keyspace differs from its admitted identity")
			}
			targets := spfreshWorkerTargets{clusterID: clusterID, keyspaceID: keyspaceID, workerAddrs: run.workerEndpoints}
			return bootstrapSPFreshEmptyIndex(ctx, util.InternalHTTPClient(), targets, tbl.Meta(), run.indexID, run.sourceSnapshotTS)
		},
	}
}

func runSPFreshBulkOrchestrator(
	ctx context.Context,
	run *spfreshBulkRunContext,
	stages spfreshBulkStages,
) error {
	if run == nil {
		return errors.New("SPFresh MVP bulk in-process context is missing")
	}
	defer func() {
		if run.staging != nil {
			run.staging.close()
			run.staging = nil
		}
	}()
	if err := stages.building(ctx, run); err != nil {
		return err
	}

	started := time.Now()
	if err := stages.scanAndUpload(ctx, run); err != nil {
		return err
	}
	logSPFreshBulkStage(run, "base_scan", started)
	logSPFreshBulkStage(run, "base_upload", started)
	if run.sourceRows == 0 {
		run.readyStartedAt = time.Now()
		if err := stages.publishEmpty(ctx, run); err != nil {
			return err
		}
		run.emptyReady = true
		return nil
	}

	started = time.Now()
	if err := stages.buildAndWriteRuns(ctx, run); err != nil {
		return err
	}
	logSPFreshBulkStage(run, "offline_build", started)
	logSPFreshBulkStage(run, "run_write", started)

	started = time.Now()
	if err := stages.importRuns(ctx, run); err != nil {
		return err
	}
	logSPFreshBulkStage(run, "import_total", started)

	started = time.Now()
	if err := stages.verify(ctx, run); err != nil {
		return err
	}
	logSPFreshBulkStage(run, "verify", started)
	run.readyStartedAt = time.Now()
	return stages.publish(ctx, run)
}

func logSPFreshBulkStage(run *spfreshBulkRunContext, stage string, started time.Time) {
	fields := []zap.Field{
		zap.String("path", "bulk"),
		zap.String("build_id", run.buildID),
		zap.String("run_id", run.runID),
		zap.String("stage", stage),
		zap.Uint64("source_snapshot_ts", run.sourceSnapshotTS),
		zap.Int64("duration_ms", time.Since(started).Milliseconds()),
	}
	switch stage {
	case "base_scan":
		fields = append(fields,
			zap.Uint64("rows_in", run.sourceRows),
			zap.Uint64("record_count", run.sourceRows),
			zap.Uint64("bytes_in", run.sourceRawBytes),
			zap.Uint64("fingerprint_count", run.sourceFingerprint.Count),
			zap.String("fingerprint_xor256", run.sourceFingerprint.XOR256),
			zap.Uint32("coarse_shard_count", run.shardCount),
			zap.Uint64("sample_rows", run.sampleRows),
			zap.String("fingerprint_sum256", run.sourceFingerprint.Sum256))
	case "base_upload":
		fields = append(fields,
			zap.Uint64("rows_out", run.sourceRows),
			zap.Uint64("record_count", run.sourceRows),
			zap.Uint64("bytes_out", run.baseObjectBytes),
			zap.Strings("artifact_uris", run.baseFragmentURIs),
			zap.Strings("artifact_sha256", run.baseFragmentSHA256))
	case "offline_build":
		partitionCount := run.builtPartitionCount
		leafPartitionCount := run.builtLeafPartitions
		if run.finalManifest != nil {
			partitionCount = run.finalManifest.PartitionCount
			leafPartitionCount = run.finalManifest.LeafPartitionCount
		}
		fields = append(fields,
			zap.Uint64("rows_in", run.sourceRows),
			zap.Uint64("bytes_in", run.baseObjectBytes),
			zap.Uint64("leaf_entries", run.leafEntryCount),
			zap.Uint64("fingerprint_count", run.sourceFingerprint.Count),
			zap.String("fingerprint_xor256", run.sourceFingerprint.XOR256),
			zap.Uint64("partition_count", partitionCount),
			zap.Uint64("leaf_partition_count", leafPartitionCount),
			zap.String("fingerprint_sum256", run.sourceFingerprint.Sum256))
	case "run_write":
		fields = append(fields,
			zap.String("non_root_artifact_uri", run.nonRootRunURI),
			zap.Uint64("non_root_record_count", run.nonRootRecordCount),
			zap.Uint64("non_root_bytes", run.nonRootObjectBytes),
			zap.String("root_artifact_uri", run.rootRunURI),
			zap.Uint64("root_record_count", run.rootRecordCount),
			zap.Uint64("root_bytes", run.rootObjectBytes))
	case "import_total":
		partitionCount := run.builtPartitionCount
		leafPartitionCount := run.builtLeafPartitions
		leafEntryCount := run.leafEntryCount
		if run.finalManifest != nil {
			partitionCount = run.finalManifest.PartitionCount
			leafPartitionCount = run.finalManifest.LeafPartitionCount
			leafEntryCount = run.finalManifest.LeafEntryCount
		}
		fields = append(fields,
			zap.Uint64("bulk_import_ts", run.bulkImportTS),
			zap.Uint64("partition_count", partitionCount),
			zap.Uint64("leaf_partition_count", leafPartitionCount),
			zap.Uint64("leaf_entries", leafEntryCount))
	case "verify":
		fields = append(fields,
			zap.Uint64("source_rows", run.sourceRows),
			zap.Uint64("leaf_entries", run.leafEntryCount),
			zap.Uint64("fingerprint_count", run.sourceFingerprint.Count),
			zap.String("fingerprint_xor256", run.sourceFingerprint.XOR256),
			zap.String("fingerprint_sum256", run.sourceFingerprint.Sum256),
			zap.Uint64("bulk_import_ts", run.bulkImportTS),
			zap.Uint64("ready_watermark", run.sourceSnapshotTS))
	case "publish":
		fields = append(fields,
			zap.Uint64("bulk_import_ts", run.bulkImportTS),
			zap.Uint64("ready_watermark", run.sourceSnapshotTS),
			zap.Uint64("ready_index_ts", run.readyIndexTS),
			zap.String("schema_state", "public"),
			zap.Uint32("root_level", run.rootLevel))
	}
	logutil.DDLLogger().Info("SPFresh MVP bulk stage completed", fields...)
}

func runSPFreshBulkReorg(ctx context.Context, w *worker, job *model.Job, tbl table.Table) error {
	reorgCtx := w.jobContext(job.ID, job.ReorgMeta)
	run := reorgCtx.spfresh.bulk
	if job.ReorgMeta.SPFreshBulkRunID == "" || run != nil && run.runID != job.ReorgMeta.SPFreshBulkRunID {
		return errors.New("SPFresh bulk cleanup ownership is missing or inconsistent; cancel and rebuild")
	}
	if run == nil {
		return errors.New("SPFresh MVP bulk in-process context is missing; refusing fallback")
	}
	if err := requestSPFreshBuildLifecycle(ctx, w.store, run.workerEndpoints, run.tableID, run.indexID, job.ReorgMeta.SPFreshBuildGeneration, "begin"); err != nil {
		return err
	}
	err := runSPFreshBulkReorgWithStages(ctx, run, productionSPFreshBulkStages(w, job, tbl, reorgCtx))
	if err != nil {
		if cancelErr := cancelSPFreshBulkRun(run); cancelErr != nil {
			logutil.DDLLogger().Warn("SPFresh failed build retirement remains pending", zap.String("build_id", run.buildID), zap.Error(cancelErr))
		}
	}
	return err
}

func runSPFreshBulkReorgWithStages(
	ctx context.Context,
	run *spfreshBulkRunContext,
	stages spfreshBulkStages,
) error {
	if run == nil {
		return errors.New("SPFresh MVP bulk in-process context is missing; refusing fallback")
	}
	return runSPFreshBulkOrchestrator(ctx, run, stages)
}
