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
	"sort"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbconfig "github.com/pingcap/tidb/pkg/config"
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
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
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
	nodeRes    *proto.NodeResource
}

var _ scheduler.Extension = (*LitBackfillScheduler)(nil)

func newLitBackfillScheduler(ctx context.Context, d *ddl, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
	sch := LitBackfillScheduler{
		d:             d,
		BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param),
		nodeRes:       param.GetNodeResource(),
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
		d:       ddl,
		nodeRes: &proto.NodeResource{TotalCPU: 4, TotalMem: 16 * units.GiB, TotalDisk: 100 * units.GiB},
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
	logger.Info("on next subtasks batch")
	storeWithPD := sch.d.store.(kv.StorageWithPD)
	// TODO: use planner.
	switch nextStep {
	case proto.BackfillStepReadIndex:
		taskKS := task.Keyspace
		store := sch.d.store
		if taskKS != tidbconfig.GetGlobalKeyspaceName() {
			taskMgr, err := diststorage.GetTaskManager()
			if err != nil {
				return nil, errors.Trace(err)
			}
			err = taskMgr.WithNewSession(func(se sessionctx.Context) error {
				store, err = se.GetSQLServer().GetKSStore(taskKS)
				return err
			})
			if err != nil {
				return nil, err
			}
		}
		tblInfo, err := getTblInfo(ctx, store, job)
		if err != nil {
			return nil, err
		}
		// TODO(tangenta): use available disk during adding index.
		availableDisk := sch.nodeRes.GetTaskDiskResource(task.Concurrency, vardef.DDLDiskQuota.Load())
		logger.Info("available local disk space resource", zap.String("size", units.BytesSize(float64(availableDisk))))
		return generateReadIndexPlan(ctx, sch.d, store, tblInfo, job, sch.GlobalSort, len(execIDs), logger)
	case proto.BackfillStepMergeSort:
		return generateMergePlan(ctx, taskHandle, task, len(execIDs), backfillMeta.CloudStorageURI, logger)
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
			return generateGlobalSortIngestPlan(
				ctx,
				storeWithPD,
				taskHandle,
				task,
				backfillMeta.CloudStorageURI,
				logger)
		}
		return nil, nil
	default:
		return nil, nil
	}
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

func getTblInfo(ctx context.Context, store kv.Storage, job *model.Job) (tblInfo *model.TableInfo, err error) {
	err = kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
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

func generateReadIndexPlan(
	ctx context.Context,
	d *ddl,
	store kv.Storage,
	tblInfo *model.TableInfo,
	job *model.Job,
	useCloud bool,
	nodeCnt int,
	logger *zap.Logger,
) (metas [][]byte, err error) {
	tbl, err := getTable(d.ddlCtx.getAutoIDRequirement(), job.SchemaID, tblInfo)
	if err != nil {
		return nil, err
	}
	jobReorgCtx := d.jobContext(job.ID, job.ReorgMeta)
	if tblInfo.Partition == nil {
		return generatePlanForPhysicalTable(ctx, jobReorgCtx, store, tbl.(table.PhysicalTable), job, useCloud, nodeCnt, logger)
	}
	defs := tblInfo.Partition.Definitions
	for _, def := range defs {
		partTbl := tbl.GetPartitionedTable().GetPartition(def.ID)
		partMeta, err := generatePlanForPhysicalTable(ctx, jobReorgCtx, store, partTbl, job, useCloud, nodeCnt, logger)
		if err != nil {
			return nil, err
		}
		metas = append(metas, partMeta...)
	}
	return metas, nil
}

func generatePlanForPhysicalTable(
	ctx context.Context,
	reorgCtx *ReorgContext,
	store kv.Storage,
	tbl table.PhysicalTable,
	job *model.Job,
	useCloud bool,
	nodeCnt int,
	logger *zap.Logger,
) (metas [][]byte, err error) {
	ver, err := getValidCurrentVersion(store)
	if err != nil {
		return nil, errors.Trace(err)
	}

	startKey, endKey, err := getTableRange(reorgCtx, store, tbl, ver.Ver, job.Priority)
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
		regionCache := store.(helper.Storage).GetRegionCache()
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
			importTS, err := allocNewTS(ctx, store.(kv.StorageWithPD))
			if err != nil {
				return true, nil
			}
			end := min(i+regionBatch, len(recordRegionMetas))
			batch := recordRegionMetas[i:end]
			subTaskMeta := &BackfillSubTaskMeta{
				PhysicalTableID: tbl.GetPhysicalID(),
				RowStart:        batch[0].StartKey(),
				RowEnd:          batch[len(batch)-1].EndKey(),
				TS:              importTS,
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
	avgTasksPerInstance := (totalRegionCnt + nodeCnt - 1) / nodeCnt // ceiling
	if useLocalDisk {
		regionBatch = avgTasksPerInstance
	} else {
		// For cloud storage, each subtask should contain no more than 4000 regions.
		regionBatch = min(4000, avgTasksPerInstance)
	}
	regionBatch = max(regionBatch, 1)
	return regionBatch
}

func generateGlobalSortIngestPlan(
	ctx context.Context,
	store kv.StorageWithPD,
	taskHandle diststorage.TaskHandle,
	task *proto.Task,
	cloudStorageURI string,
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
		newMeta, err := splitSubtaskMetaForOneKVMetaGroup(ctx, store, g, eleID, cloudStorageURI, iCnt, logger)
		if err != nil {
			return nil, errors.Trace(err)
		}
		metaArr = append(metaArr, newMeta...)
	}
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
	logger *zap.Logger,
) (metaArr []*BackfillSubTaskMeta, err error) {
	if len(kvMeta.StartKey) == 0 && len(kvMeta.EndKey) == 0 {
		// Skip global sort for empty table.
		return nil, nil
	}
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
		endKeyOfGroup, dataFiles, statFiles, interiorRangeJobKeys, interiorRegionSplitKeys, err := splitter.SplitOneRangesGroup()
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
				StartKey:    startKey,
				EndKey:      endKey,
				TotalKVSize: kvMeta.TotalKVSize / uint64(instanceCnt),
			}},
			DataFiles:      dataFiles,
			StatFiles:      statFiles,
			RangeJobKeys:   rangeJobKeys,
			RangeSplitKeys: regionSplitKeys,
			TS:             importTS,
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
				DataFiles: files,
				EleIDs:    eleID,
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
	backend, err := storage.ParseBackend(cloudStorageURI, nil)
	if err != nil {
		return nil, err
	}
	extStore, err := storage.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		return nil, err
	}

	rangeGroupSize := totalSize / instanceCnt
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
