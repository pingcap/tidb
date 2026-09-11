// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"encoding/hex"
	"encoding/json"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	diststorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tici"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// LitBackfillScheduler wraps BaseScheduler.
type LitBackfillScheduler struct {
	*scheduler.BaseScheduler
	d          *ddl
	GlobalSort bool
}

// storageWithPDAndCodec is the minimal store capability required by TiCI pre-split.
type storageWithPDAndCodec interface {
	kv.StorageWithPD
	GetCodec() tikv.Codec
}

var _ scheduler.Extension = (*LitBackfillScheduler)(nil)

func newLitBackfillScheduler(ctx context.Context, d *ddl, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
	sch := LitBackfillScheduler{
		d:             d,
		BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param),
	}
	return &sch
}

// NewBackfillingSchedulerForTest creates a new backfillingSchedulerExt, only used for test now.
func NewBackfillingSchedulerForTest(d DDL) (scheduler.Extension, error) {
	ddl, ok := d.(*ddl)
	if !ok {
		return nil, errors.New("The getDDL result should be the type of *ddl")
	}
	return &LitBackfillScheduler{
		d: ddl,
	}, nil
}

// Init implements BaseScheduler interface.
func (sch *LitBackfillScheduler) Init() (err error) {
	taskMeta := &BackfillTaskMeta{}
	if err = json.Unmarshal(sch.BaseScheduler.GetTask().Meta, taskMeta); err != nil {
		return errors.Annotate(err, "unmarshal task meta failed")
	}
	sch.GlobalSort = len(taskMeta.CloudStorageURI) > 0
	sch.BaseScheduler.Extension = sch
	return sch.BaseScheduler.Init()
}

// Close implements BaseScheduler interface.
func (sch *LitBackfillScheduler) Close() {
	sch.BaseScheduler.Close()
}

// OnTick implements scheduler.Extension interface.
func (*LitBackfillScheduler) OnTick(_ context.Context, _ *proto.Task) {
}

// OnNextSubtasksBatch generate batch of next step's plan.
func (sch *LitBackfillScheduler) OnNextSubtasksBatch(
	ctx context.Context,
	taskHandle diststorage.TaskHandle,
	task *proto.Task,
	execIDs []string,
	nextStep proto.Step,
) (subtaskMeta [][]byte, err error) {
	logger := logutil.DDLLogger().With(
		zap.Stringer("type", task.Type),
		zap.Int64("task-id", task.ID),
		zap.String("curr-step", proto.Step2Str(task.Type, task.Step)),
		zap.String("next-step", proto.Step2Str(task.Type, nextStep)),
	)
	var backfillMeta BackfillTaskMeta
	if err := json.Unmarshal(task.Meta, &backfillMeta); err != nil {
		return nil, err
	}
	job := &backfillMeta.Job
	tblInfo, err := getTblInfo(ctx, sch.d, job)
	if err != nil {
		return nil, err
	}
	logger.Info("on next subtasks batch")
	storeWithPD := sch.d.store.(kv.StorageWithPD)
	// TODO: use planner.
	switch nextStep {
	case proto.BackfillStepReadIndex:
		if tblInfo.Partition != nil {
			return generatePartitionPlan(ctx, storeWithPD, tblInfo, backfillMeta.ScanSnapshotTS)
		}
		return generateNonPartitionPlan(ctx, sch.d, tblInfo, job, sch.GlobalSort, len(execIDs), backfillMeta.ScanSnapshotTS, logger)
	case proto.BackfillStepMergeSort:
		return generateMergePlan(ctx, taskHandle, task, len(execIDs), backfillMeta.CloudStorageURI, backfillMeta.ScanSnapshotTS, logger)
	case proto.BackfillStepWriteAndIngest:
		if sch.GlobalSort {
			failpoint.Inject("mockWriteIngest", func() {
				m := &BackfillSubTaskMeta{
					MetaGroups: []*external.SortedKVMeta{},
				}
				metaBytes, _ := m.Marshal()
				metaArr := make([][]byte, 0, 16)
				metaArr = append(metaArr, metaBytes)
				failpoint.Return(metaArr, nil)
			})
			storeWithPDAndCodec, err := getStorageWithPDAndCodec(sch.d.store)
			if err != nil {
				return nil, err
			}
			return generateGlobalSortIngestPlan(
				ctx,
				storeWithPDAndCodec,
				taskHandle,
				task,
				&backfillMeta,
				backfillMeta.CloudStorageURI,
				backfillMeta.ScanSnapshotTS,
				logger)
		}
		return nil, nil
	default:
		return nil, nil
	}
}

// getStorageWithPDAndCodec validates that the store can provide both PD access and keyspace codec information.
func getStorageWithPDAndCodec(store kv.Storage) (storageWithPDAndCodec, error) {
	storeWithPDAndCodec, ok := store.(storageWithPDAndCodec)
	if !ok {
		return nil, errors.Errorf("store %T does not implement storageWithPDAndCodec", store)
	}
	return storeWithPDAndCodec, nil
}

// GetNextStep implements scheduler.Extension interface.
func (sch *LitBackfillScheduler) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.BackfillStepReadIndex
	case proto.BackfillStepReadIndex:
		if sch.GlobalSort {
			return proto.BackfillStepMergeSort
		}
		return proto.StepDone
	case proto.BackfillStepMergeSort:
		return proto.BackfillStepWriteAndIngest
	case proto.BackfillStepWriteAndIngest:
		return proto.StepDone
	default:
		return proto.StepDone
	}
}

func skipMergeSort(stats []external.MultipleFilesStat, concurrency int) bool {
	failpoint.Inject("forceMergeSort", func() {
		failpoint.Return(false)
	})
	return external.GetMaxOverlappingTotal(stats) <= external.GetAdjustedMergeSortOverlapThreshold(concurrency)
}

// OnDone implements scheduler.Extension interface.
func (*LitBackfillScheduler) OnDone(_ context.Context, _ diststorage.TaskHandle, _ *proto.Task) error {
	return nil
}

// GetEligibleInstances implements scheduler.Extension interface.
func (*LitBackfillScheduler) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]string, error) {
	return nil, nil
}

// IsRetryableErr implements scheduler.Extension interface.
func (*LitBackfillScheduler) IsRetryableErr(error) bool {
	return true
}

// ModifyMeta implements scheduler.Extension interface.
func (sch *LitBackfillScheduler) ModifyMeta(oldMeta []byte, modifies []proto.Modification) ([]byte, error) {
	taskMeta := &BackfillTaskMeta{}
	if err := json.Unmarshal(oldMeta, taskMeta); err != nil {
		return nil, errors.Trace(err)
	}
	for _, m := range modifies {
		switch m.Type {
		case proto.ModifyBatchSize:
			taskMeta.Job.ReorgMeta.SetBatchSize(int(m.To))
		case proto.ModifyMaxWriteSpeed:
			taskMeta.Job.ReorgMeta.SetMaxWriteSpeed(int(m.To))
		default:
			logutil.DDLLogger().Warn("invalid modify type",
				zap.Int64("taskId", sch.GetTask().ID), zap.Stringer("modify", m))
		}
	}
	return json.Marshal(taskMeta)
}

func getTblInfo(ctx context.Context, d *ddl, job *model.Job) (tblInfo *model.TableInfo, err error) {
	err = kv.RunInNewTxn(ctx, d.store, true, func(_ context.Context, txn kv.Transaction) error {
		tblInfo, err = meta.NewMutator(txn).GetTable(job.SchemaID, job.TableID)
		return err
	})
	if err != nil {
		return nil, err
	}

	return tblInfo, nil
}

const (
	scanRegionBackoffBase = 200 * time.Millisecond
	scanRegionBackoffMax  = 2 * time.Second
)

func generatePartitionPlan(
	ctx context.Context,
	store kv.StorageWithPD,
	tblInfo *model.TableInfo,
	scanSnapshotTS uint64,
) (metas [][]byte, err error) {
	defs := tblInfo.Partition.Definitions
	physicalIDs := make([]int64, len(defs))
	for i := range defs {
		physicalIDs[i] = defs[i].ID
	}

	subTaskMetas := make([][]byte, 0, len(physicalIDs))
	for _, physicalID := range physicalIDs {
		// It should be different for each subtask to determine if there are duplicate entries.
		importTS, err := allocNewTS(ctx, store)
		if err != nil {
			return nil, err
		}
		subTaskMeta := &BackfillSubTaskMeta{
			PhysicalTableID: physicalID,
			TS:              importTS,
			ScanSnapshotTS:  scanSnapshotTS,
		}

		metaBytes, err := json.Marshal(subTaskMeta)
		if err != nil {
			return nil, err
		}

		subTaskMetas = append(subTaskMetas, metaBytes)
	}
	return subTaskMetas, nil
}

func generateNonPartitionPlan(
	ctx context.Context,
	d *ddl,
	tblInfo *model.TableInfo,
	job *model.Job,
	useCloud bool,
	nodeCnt int,
	scanSnapshotTS uint64,
	logger *zap.Logger,
) (metas [][]byte, err error) {
	tbl, err := getTable(d.ddlCtx.getAutoIDRequirement(), job.SchemaID, tblInfo)
	if err != nil {
		return nil, err
	}
	ver, err := getValidCurrentVersion(d.store)
	if err != nil {
		return nil, errors.Trace(err)
	}

	startKey, endKey, err := getTableRange(d.jobContext(job.ID, job.ReorgMeta), d.store, tbl.(table.PhysicalTable), ver.Ver, job.Priority)
	if startKey == nil && endKey == nil {
		// Empty table.
		return nil, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	subTaskMetas := make([][]byte, 0, 4)
	backoffer := backoff.NewExponential(scanRegionBackoffBase, 2, scanRegionBackoffMax)
	err = handle.RunWithRetry(ctx, 8, backoffer, logutil.DDLLogger(), func(_ context.Context) (bool, error) {
		regionCache := d.store.(helper.Storage).GetRegionCache()
		recordRegionMetas, err := regionCache.LoadRegionsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 20000, nil), startKey, endKey)
		if err != nil {
			return false, err
		}
		sort.Slice(recordRegionMetas, func(i, j int) bool {
			return bytes.Compare(recordRegionMetas[i].StartKey(), recordRegionMetas[j].StartKey()) < 0
		})

		// Check if regions are continuous.
		shouldRetry := false
		cur := recordRegionMetas[0]
		for _, m := range recordRegionMetas[1:] {
			if !bytes.Equal(cur.EndKey(), m.StartKey()) {
				shouldRetry = true
				break
			}
			cur = m
		}

		if shouldRetry {
			return true, nil
		}

		regionBatch := CalculateRegionBatch(len(recordRegionMetas), nodeCnt, !useCloud)
		logger.Info("calculate region batch",
			zap.Int("totalRegionCnt", len(recordRegionMetas)),
			zap.Int("regionBatch", regionBatch),
			zap.Int("instanceCnt", nodeCnt),
			zap.Bool("useCloud", useCloud),
		)

		for i := 0; i < len(recordRegionMetas); i += regionBatch {
			// It should be different for each subtask to determine if there are duplicate entries.
			importTS, err := allocNewTS(ctx, d.store.(kv.StorageWithPD))
			if err != nil {
				return true, nil
			}
			end := i + regionBatch
			if end > len(recordRegionMetas) {
				end = len(recordRegionMetas)
			}
			batch := recordRegionMetas[i:end]
			subTaskMeta := &BackfillSubTaskMeta{
				RowStart:       batch[0].StartKey(),
				RowEnd:         batch[len(batch)-1].EndKey(),
				TS:             importTS,
				ScanSnapshotTS: scanSnapshotTS,
			}
			if i == 0 {
				subTaskMeta.RowStart = startKey
			}
			if end == len(recordRegionMetas) {
				subTaskMeta.RowEnd = endKey
			}
			metaBytes, err := subTaskMeta.Marshal()
			if err != nil {
				return false, err
			}
			subTaskMetas = append(subTaskMetas, metaBytes)
		}
		return false, nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(subTaskMetas) == 0 {
		return nil, errors.Errorf("regions are not continuous")
	}
	return subTaskMetas, nil
}

// CalculateRegionBatch is exported for test.
func CalculateRegionBatch(totalRegionCnt int, nodeCnt int, useLocalDisk bool) int {
	failpoint.Inject("mockRegionBatch", func(val failpoint.Value) {
		failpoint.Return(val.(int))
	})
	var regionBatch int
	if useLocalDisk {
		// We want to avoid too may partial imports when using local disk. So we
		// limit the node count to 3 so that at most 3 partial imports if disk
		// space is enough.
		nodeCnt = min(3, nodeCnt)
	}
	avgTasksPerInstance := (totalRegionCnt + nodeCnt - 1) / nodeCnt // ceiling
	if useLocalDisk {
		// Special handling for small table, in this case, we want to do it on
		// one node. 100 region data is about 10GiB.
		regionBatch = min(max(100, avgTasksPerInstance), totalRegionCnt)
	} else {
		// For cloud storage, each subtask should contain no more than 4000 regions.
		regionBatch = min(4000, avgTasksPerInstance)
	}
	return regionBatch
}

func generateGlobalSortIngestPlan(
	ctx context.Context,
	store storageWithPDAndCodec,
	taskHandle diststorage.TaskHandle,
	task *proto.Task,
	backfillMeta *BackfillTaskMeta,
	cloudStorageURI string,
	scanSnapshotTS uint64,
	logger *zap.Logger,
) ([][]byte, error) {
	var (
		kvMetaGroups []*external.SortedKVMeta
		eleIDs       []int64
	)
	for _, step := range []proto.Step{proto.BackfillStepMergeSort, proto.BackfillStepReadIndex} {
		hasSubtasks := false
		err := forEachBackfillSubtaskMeta(ctx, cloudStorageURI, taskHandle, task.ID, step, func(subtask *BackfillSubTaskMeta) {
			hasSubtasks = true
			if kvMetaGroups == nil {
				kvMetaGroups = make([]*external.SortedKVMeta, len(subtask.MetaGroups))
				eleIDs = subtask.EleIDs
			}
			for i, cur := range subtask.MetaGroups {
				if kvMetaGroups[i] == nil {
					kvMetaGroups[i] = &external.SortedKVMeta{}
				}
				kvMetaGroups[i].Merge(cur)
			}
		})
		if err != nil {
			return nil, err
		}
		if hasSubtasks {
			break
		}
		// If there is no subtask for merge sort step,
		// it means the merge sort step is skipped.
	}

	instanceIDs, err := scheduler.GetLiveExecIDs(ctx)
	if err != nil {
		return nil, err
	}
	iCnt := int64(len(instanceIDs))
	metaArr := make([]*BackfillSubTaskMeta, 0, 16)
	for i, g := range kvMetaGroups {
		if g == nil {
			logger.Error("meet empty kv group when getting subtask summary",
				zap.Int64("taskID", task.ID))
			return nil, errors.Errorf("subtask kv group %d is empty", i)
		}
		eleID := int64(0)
		// in case the subtask metadata is written by an old version of TiDB.
		if i < len(eleIDs) {
			eleID = eleIDs[i]
		}
		newMeta, err := splitSubtaskMetaForOneKVMetaGroup(ctx, store, g, eleID, cloudStorageURI, iCnt, scanSnapshotTS, logger)
		if err != nil {
			return nil, errors.Trace(err)
		}
		metaArr = append(metaArr, newMeta...)
	}

	if shouldCallTiCIPreSplit(backfillMeta) {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
		err := triggerTiCIPreSplitImportShards(timeoutCtx, store, backfillMeta, cloudStorageURI, eleIDs, kvMetaGroups, logger)
		cancel()
		if err != nil {
			logger.Error("tici pre-split shard failed, fallback to default global-sort ingest planning",
				zap.Int64("taskID", task.ID),
				zap.Int64("jobID", backfillMeta.Job.ID),
				zap.String("jobType", backfillMeta.Job.Type.String()),
				zap.Error(err))
		}
	}
	failpoint.Inject("mockGenerateGlobalSortIngestPlanAfterPreSplit", func() {
		m := &BackfillSubTaskMeta{
			MetaGroups: []*external.SortedKVMeta{},
		}
		metaBytes, _ := m.Marshal()
		failpoint.Return([][]byte{metaBytes}, nil)
	})
	// write external meta to storage when using global sort
	for i, m := range metaArr {
		if err := writeExternalBackfillSubTaskMeta(ctx, cloudStorageURI, m, external.PlanMetaPath(
			task.ID,
			proto.Step2Str(proto.Backfill, proto.BackfillStepWriteAndIngest),
			i+1,
		)); err != nil {
			return nil, err
		}
	}
	metas := make([][]byte, 0, len(metaArr))
	for _, m := range metaArr {
		metaBytes, err := m.Marshal()
		if err != nil {
			return nil, err
		}
		metas = append(metas, metaBytes)
	}
	return metas, nil
}

// shouldCallTiCIPreSplit reports whether the current backfill job should try the TiCI pre-split optimization.
func shouldCallTiCIPreSplit(backfillMeta *BackfillTaskMeta) bool {
	if backfillMeta == nil {
		return false
	}
	switch backfillMeta.Job.Type {
	case model.ActionAddFullTextIndex, model.ActionAddHybridIndex:
		return true
	default:
		return false
	}
}

// triggerTiCIPreSplitImportShards builds and sends a best-effort TiCI pre-split request.
func triggerTiCIPreSplitImportShards(
	ctx context.Context,
	store storageWithPDAndCodec,
	backfillMeta *BackfillTaskMeta,
	cloudStorageURI string,
	eleIDs []int64,
	kvMetaGroups []*external.SortedKVMeta,
	logger *zap.Logger,
) error {
	reportGroups, err := buildTiCIPreSplitReportGroups(ctx, store, eleIDs, kvMetaGroups, cloudStorageURI, logger)
	if err != nil {
		return err
	}
	req, err := buildTiCIPreSplitImportShardsRequest(backfillMeta, reportGroups, kvMetaGroups)
	if err != nil {
		return err
	}
	if req == nil {
		return nil
	}
	return tici.PreSplitImportShards(ctx, store, req)
}

// buildTiCIPreSplitImportShardsRequest merges per-range report groups into one TiCI request.
func buildTiCIPreSplitImportShardsRequest(
	backfillMeta *BackfillTaskMeta,
	reportGroups []*tici.PreSplitImportShardMeta,
	kvMetaGroups []*external.SortedKVMeta,
) (*tici.PreSplitImportShardsRequest, error) {
	if backfillMeta == nil {
		return nil, errors.New("backfill meta is nil")
	}
	if len(reportGroups) == 0 {
		return nil, nil
	}
	dataFileCount, statFileCount := countUniqueFilesForTiCIPreSplitRequest(kvMetaGroups)
	req := &tici.PreSplitImportShardsRequest{
		TidbTaskId:     ticiTaskIDForDDL(backfillMeta.Job.ID),
		TableId:        backfillMeta.Job.TableID,
		ScanSnapshotTs: backfillMeta.ScanSnapshotTS,
		IndexIds:       append([]int64(nil), backfillMeta.EleIDs...),
		DataFileCount:  dataFileCount,
		StatFileCount:  statFileCount,
		MetaGroups:     make([]*tici.PreSplitImportShardMeta, 0, len(reportGroups)),
	}

	for i, groupReq := range reportGroups {
		if groupReq == nil {
			return nil, errors.Errorf("report group %d is empty", i)
		}
		if len(req.StartKey) == 0 && len(req.EndKey) == 0 {
			req.StartKey = groupReq.StartKey
			req.EndKey = groupReq.EndKey
		} else {
			req.StartKey = external.BytesMin(req.StartKey, groupReq.StartKey)
			req.EndKey = external.BytesMax(req.EndKey, groupReq.EndKey)
		}
		req.TotalKvSize += groupReq.TotalKvSize
		req.TotalKvCnt += groupReq.TotalKvCnt
		req.MetaGroups = append(req.MetaGroups, groupReq)
		if groupReq.EleId > 0 && !slices.Contains(req.IndexIds, groupReq.EleId) {
			req.IndexIds = append(req.IndexIds, groupReq.EleId)
		}
	}
	return req, nil
}

// countUniqueFilesForTiCIPreSplitRequest deduplicates shared data/stat files before filling request metadata.
func countUniqueFilesForTiCIPreSplitRequest(
	kvMetaGroups []*external.SortedKVMeta,
) (dataFileCount int32, statFileCount int32) {
	dataFiles := make(map[string]struct{})
	statFiles := make(map[string]struct{})
	for _, kvMeta := range kvMetaGroups {
		if kvMeta == nil {
			continue
		}
		for _, dataFile := range kvMeta.GetDataFiles() {
			dataFiles[dataFile] = struct{}{}
		}
		for _, statFile := range kvMeta.GetStatFiles() {
			statFiles[statFile] = struct{}{}
		}
	}
	return int32(len(dataFiles)), int32(len(statFiles))
}

const ticiPreSplitReportGroupSize int64 = units.GiB

// buildTiCIPreSplitReportGroups splits all KV meta groups into TiCI report groups with exact size and key counts.
func buildTiCIPreSplitReportGroups(
	ctx context.Context,
	store kv.StorageWithPD,
	eleIDs []int64,
	kvMetaGroups []*external.SortedKVMeta,
	cloudStorageURI string,
	logger *zap.Logger,
) ([]*tici.PreSplitImportShardMeta, error) {
	failpoint.Inject("mockBuildTiCIPreSplitReportGroups", func() {
		reportGroups := make([]*tici.PreSplitImportShardMeta, 0, len(kvMetaGroups))
		for i, kvMeta := range kvMetaGroups {
			if kvMeta == nil {
				continue
			}
			reportGroup := &tici.PreSplitImportShardMeta{
				StartKey:      kvMeta.StartKey,
				EndKey:        kvMeta.EndKey,
				TotalKvSize:   kvMeta.TotalKVSize,
				TotalKvCnt:    kvMeta.TotalKVCnt,
				DataFileCount: int32(len(kvMeta.GetDataFiles())),
				StatFileCount: int32(len(kvMeta.GetStatFiles())),
			}
			if i < len(eleIDs) {
				reportGroup.EleId = eleIDs[i]
			}
			reportGroups = append(reportGroups, reportGroup)
		}
		failpoint.Return(reportGroups, nil)
	})
	reportGroups := make([]*tici.PreSplitImportShardMeta, 0, len(kvMetaGroups))
	for i, kvMeta := range kvMetaGroups {
		if kvMeta == nil {
			return nil, errors.Errorf("subtask kv group %d is empty", i)
		}
		eleID := int64(0)
		if i < len(eleIDs) {
			eleID = eleIDs[i]
		}
		groups, err := splitTiCIPreSplitReportGroupsForOneKVMetaGroup(ctx, store, kvMeta, eleID, cloudStorageURI, logger)
		if err != nil {
			return nil, err
		}
		reportGroups = append(reportGroups, groups...)
	}
	return reportGroups, nil
}

// splitTiCIPreSplitReportGroupsForOneKVMetaGroup turns one KV meta group into TiCI pre-split report groups.
func splitTiCIPreSplitReportGroupsForOneKVMetaGroup(
	ctx context.Context,
	store kv.StorageWithPD,
	kvMeta *external.SortedKVMeta,
	eleID int64,
	cloudStorageURI string,
	logger *zap.Logger,
) ([]*tici.PreSplitImportShardMeta, error) {
	if len(kvMeta.StartKey) == 0 && len(kvMeta.EndKey) == 0 {
		return nil, nil
	}
	splitter, err := getRangeSplitterWithGroupSize(ctx, store, cloudStorageURI, ticiPreSplitReportGroupSize, kvMeta.MultipleFilesStats, logger)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := splitter.Close()
		if err != nil {
			logger.Error("failed to close tici pre-split range splitter", zap.Error(err))
		}
	}()

	reportGroups := make([]*tici.PreSplitImportShardMeta, 0, max(1, int(kvMeta.TotalKVSize/uint64(ticiPreSplitReportGroupSize))+1))
	startKey := kvMeta.StartKey
	var endKey kv.Key
	for {
		endKeyOfGroup, dataFiles, statFiles, groupSize, groupKeyCnt, _, _, err := splitter.SplitOneRangesGroup()
		if err != nil {
			return nil, err
		}
		if len(endKeyOfGroup) == 0 {
			endKey = kvMeta.EndKey
		} else {
			endKey = kv.Key(endKeyOfGroup).Clone()
		}
		if bytes.Compare(startKey, endKey) >= 0 {
			return nil, errors.Errorf("invalid tici report range, startKey: %s, endKey: %s",
				hex.EncodeToString(startKey), hex.EncodeToString(endKey))
		}
		reportGroup := &tici.PreSplitImportShardMeta{
			EleId:         eleID,
			StartKey:      startKey,
			EndKey:        endKey,
			TotalKvSize:   groupSize,
			TotalKvCnt:    groupKeyCnt,
			DataFileCount: int32(len(dataFiles)),
			StatFileCount: int32(len(statFiles)),
		}
		reportGroups = append(reportGroups, reportGroup)
		if len(endKeyOfGroup) == 0 {
			break
		}
		startKey = endKey
	}
	return reportGroups, nil
}

func allocNewTS(ctx context.Context, store kv.StorageWithPD) (uint64, error) {
	pdCli := store.GetPDClient()
	p, l, err := pdCli.GetTS(ctx)
	if err != nil {
		return 0, err
	}
	ts := oracle.ComposeTS(p, l)
	return ts, nil
}

func splitSubtaskMetaForOneKVMetaGroup(
	ctx context.Context,
	store kv.StorageWithPD,
	kvMeta *external.SortedKVMeta,
	eleID int64,
	cloudStorageURI string,
	instanceCnt int64,
	scanSnapshotTS uint64,
	logger *zap.Logger,
) (metaArr []*BackfillSubTaskMeta, err error) {
	if len(kvMeta.StartKey) == 0 && len(kvMeta.EndKey) == 0 {
		// Skip global sort for empty table.
		return nil, nil
	}
	failpoint.Inject("mockSplitSubtaskMetaForOneKVMetaGroup", func() {
		m := &BackfillSubTaskMeta{
			MetaGroups: []*external.SortedKVMeta{{
				StartKey:    kvMeta.StartKey,
				EndKey:      kvMeta.EndKey,
				TotalKVSize: kvMeta.TotalKVSize,
				TotalKVCnt:  kvMeta.TotalKVCnt,
			}},
			DataFiles:      kvMeta.GetDataFiles(),
			StatFiles:      kvMeta.GetStatFiles(),
			ScanSnapshotTS: scanSnapshotTS,
		}
		if eleID > 0 {
			m.EleIDs = []int64{eleID}
		}
		failpoint.Return([]*BackfillSubTaskMeta{m}, nil)
	})
	importTS, err := allocNewTS(ctx, store)
	if err != nil {
		return nil, err
	}
	failpoint.Inject("mockTSForGlobalSort", func(val failpoint.Value) {
		i := val.(int)
		importTS = uint64(i)
	})
	splitter, err := getRangeSplitter(
		ctx, store, cloudStorageURI, int64(kvMeta.TotalKVSize), instanceCnt, kvMeta.MultipleFilesStats, logger)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := splitter.Close()
		if err != nil {
			logger.Error("failed to close range splitter", zap.Error(err))
		}
	}()

	startKey := kvMeta.StartKey
	var endKey kv.Key
	for {
		endKeyOfGroup, dataFiles, statFiles, _, _, interiorRangeJobKeys, interiorRegionSplitKeys, err := splitter.SplitOneRangesGroup()
		if err != nil {
			return nil, err
		}
		if len(endKeyOfGroup) == 0 {
			endKey = kvMeta.EndKey
		} else {
			endKey = kv.Key(endKeyOfGroup).Clone()
		}
		logger.Info("split subtask range",
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)))

		if bytes.Compare(startKey, endKey) >= 0 {
			return nil, errors.Errorf("invalid range, startKey: %s, endKey: %s",
				hex.EncodeToString(startKey), hex.EncodeToString(endKey))
		}
		rangeJobKeys := make([][]byte, 0, len(interiorRangeJobKeys)+2)
		rangeJobKeys = append(rangeJobKeys, startKey)
		rangeJobKeys = append(rangeJobKeys, interiorRangeJobKeys...)
		rangeJobKeys = append(rangeJobKeys, endKey)
		regionSplitKeys := make([][]byte, 0, len(interiorRegionSplitKeys)+2)
		regionSplitKeys = append(regionSplitKeys, startKey)
		regionSplitKeys = append(regionSplitKeys, interiorRegionSplitKeys...)
		regionSplitKeys = append(regionSplitKeys, endKey)
		m := &BackfillSubTaskMeta{
			MetaGroups: []*external.SortedKVMeta{{
				StartKey: startKey,
				EndKey:   endKey,
				// Keep the historical evenly-divided size in ingest subtask meta
				// for compatibility. TiCI pre-split report groups calculate exact
				// per-group size/count separately.
				TotalKVSize: kvMeta.TotalKVSize / uint64(instanceCnt),
			}},
			DataFiles:      dataFiles,
			StatFiles:      statFiles,
			RangeJobKeys:   rangeJobKeys,
			RangeSplitKeys: regionSplitKeys,
			TS:             importTS,
			ScanSnapshotTS: scanSnapshotTS,
		}
		if eleID > 0 {
			m.EleIDs = []int64{eleID}
		}
		metaArr = append(metaArr, m)
		if len(endKeyOfGroup) == 0 {
			break
		}
		startKey = endKey
	}
	return metaArr, nil
}

func generateMergePlan(
	ctx context.Context,
	taskHandle diststorage.TaskHandle,
	task *proto.Task,
	nodeCnt int,
	cloudStorageURI string,
	scanSnapshotTS uint64,
	logger *zap.Logger,
) ([][]byte, error) {
	// check data files overlaps,
	// if data files overlaps too much, we need a merge step.
	var (
		multiStatsGroup [][]external.MultipleFilesStat
		kvMetaGroups    []*external.SortedKVMeta
		eleIDs          []int64
	)
	err := forEachBackfillSubtaskMeta(ctx, cloudStorageURI, taskHandle, task.ID, proto.BackfillStepReadIndex,
		func(subtask *BackfillSubTaskMeta) {
			if kvMetaGroups == nil {
				kvMetaGroups = make([]*external.SortedKVMeta, len(subtask.MetaGroups))
				multiStatsGroup = make([][]external.MultipleFilesStat, len(subtask.MetaGroups))
				eleIDs = subtask.EleIDs
			}
			for i, g := range subtask.MetaGroups {
				if kvMetaGroups[i] == nil {
					kvMetaGroups[i] = &external.SortedKVMeta{}
					multiStatsGroup[i] = make([]external.MultipleFilesStat, 0, 100)
				}
				kvMetaGroups[i].Merge(g)
				multiStatsGroup[i] = append(multiStatsGroup[i], g.MultipleFilesStats...)
			}
		})
	if err != nil {
		return nil, err
	}

	allSkip := true
	for _, multiStats := range multiStatsGroup {
		if !skipMergeSort(multiStats, task.Concurrency) {
			allSkip = false
			break
		}
	}
	if allSkip {
		logger.Info("skip merge sort")
		return nil, nil
	}

	metaArr := make([]*BackfillSubTaskMeta, 0, 16)
	for i, g := range kvMetaGroups {
		dataFiles := make([]string, 0, 1000)
		if g == nil {
			logger.Error("meet empty kv group when getting subtask summary",
				zap.Int64("taskID", task.ID))
			return nil, errors.Errorf("subtask kv group %d is empty", i)
		}
		for _, m := range g.MultipleFilesStats {
			for _, filePair := range m.Filenames {
				dataFiles = append(dataFiles, filePair[0])
			}
		}
		var eleID []int64
		if i < len(eleIDs) {
			eleID = []int64{eleIDs[i]}
		}
		dataFilesGroup, err := external.DivideMergeSortDataFiles(dataFiles, nodeCnt, task.Concurrency)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, files := range dataFilesGroup {
			m := &BackfillSubTaskMeta{
				DataFiles:      files,
				EleIDs:         eleID,
				ScanSnapshotTS: scanSnapshotTS,
			}
			metaArr = append(metaArr, m)
		}
	}

	// write external meta to storage when using global sort
	for i, m := range metaArr {
		if err := writeExternalBackfillSubTaskMeta(ctx, cloudStorageURI, m, external.PlanMetaPath(
			task.ID,
			proto.Step2Str(proto.Backfill, proto.BackfillStepMergeSort),
			i+1)); err != nil {
			return nil, err
		}
	}
	metas := make([][]byte, 0, len(metaArr))
	for _, m := range metaArr {
		metaBytes, err := m.Marshal()
		if err != nil {
			return nil, err
		}
		metas = append(metas, metaBytes)
	}
	return metas, nil
}

func getRangeSplitter(
	ctx context.Context,
	store kv.StorageWithPD,
	cloudStorageURI string,
	totalSize int64,
	instanceCnt int64,
	multiFileStat []external.MultipleFilesStat,
	logger *zap.Logger,
) (*external.RangeSplitter, error) {
	rangeGroupSize := totalSize / instanceCnt
	if rangeGroupSize <= 0 {
		rangeGroupSize = 1
	}
	return getRangeSplitterWithGroupSize(ctx, store, cloudStorageURI, rangeGroupSize, multiFileStat, logger)
}

// getRangeSplitterWithGroupSize builds a range splitter that groups ranges by the provided target size.
func getRangeSplitterWithGroupSize(
	ctx context.Context,
	store kv.StorageWithPD,
	cloudStorageURI string,
	rangeGroupSize int64,
	multiFileStat []external.MultipleFilesStat,
	logger *zap.Logger,
) (*external.RangeSplitter, error) {
	backend, err := storage.ParseBackend(cloudStorageURI, nil)
	if err != nil {
		return nil, err
	}
	extStore, err := storage.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		return nil, err
	}

	rangeGroupKeys := int64(math.MaxInt64)

	var regionSplitSize = int64(config.SplitRegionSize)
	var regionSplitKeys = int64(config.SplitRegionKeys)
	if store != nil {
		pdCli := store.GetPDClient()
		tls, err := ingest.NewDDLTLS()
		if err == nil {
			size, keys, err := local.GetRegionSplitSizeKeys(ctx, pdCli, tls)
			if err == nil {
				regionSplitSize = max(regionSplitSize, size)
				regionSplitKeys = max(regionSplitKeys, keys)
			} else {
				logger.Warn("fail to get region split keys and size", zap.Error(err))
			}
		} else {
			logger.Warn("fail to get region split keys and size", zap.Error(err))
		}
	}
	nodeRc := handle.GetNodeResource()
	rangeSize, rangeKeys := external.CalRangeSize(nodeRc.TotalMem/int64(nodeRc.TotalCPU), regionSplitSize, regionSplitKeys)
	logutil.DDLIngestLogger().Info("split kv range with split size and keys",
		zap.Int64("region-split-size", regionSplitSize),
		zap.Int64("region-split-keys", regionSplitKeys),
		zap.Int64("range-size", rangeSize),
		zap.Int64("range-keys", rangeKeys),
	)
	return external.NewRangeSplitter(ctx, multiFileStat, extStore,
		rangeGroupSize, rangeGroupKeys,
		rangeSize, rangeKeys,
		regionSplitSize, regionSplitKeys)
}

func forEachBackfillSubtaskMeta(
	ctx context.Context,
	cloudStorageURI string,
	taskHandle diststorage.TaskHandle,
	gTaskID int64,
	step proto.Step,
	fn func(subtask *BackfillSubTaskMeta),
) error {
	subTaskMetas, err := taskHandle.GetPreviousSubtaskMetas(gTaskID, step)
	if err != nil {
		return errors.Trace(err)
	}
	for _, subTaskMeta := range subTaskMetas {
		subtask, err := decodeBackfillSubTaskMeta(ctx, cloudStorageURI, subTaskMeta)
		if err != nil {
			logutil.DDLLogger().Error("unmarshal error", zap.Error(err))
			return errors.Trace(err)
		}
		fn(subtask)
	}
	return nil
}
