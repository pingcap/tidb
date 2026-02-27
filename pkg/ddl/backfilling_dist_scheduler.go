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
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfmetric"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	diststorage "github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
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
	d              *ddl
	GlobalSort     bool
	MergeTempIndex bool
	nodeRes        *proto.NodeResource
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
	sch.MergeTempIndex = taskMeta.MergeTempIndex
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
	nodeCnt := len(execIDs)
	if kerneltype.IsNextGen() {
		// in nextgen, node resource are scaled out automatically, we only consider
		// the max allowed node for the task, and ignore how many node currently
		// available.
		// in some UT, task.MaxNodeCount might not initialize due to below check,
		// so we add a max(1, ...) to avoid nodeCnt being 0:
		// https://github.com/pingcap/tidb/blob/f13d6599e37d7f660d413c481892e57af418c77d/pkg/ddl/reorg_util.go#L82-L83
		nodeCnt = max(task.MaxNodeCount, 1)
	}
	logger := logutil.DDLLogger().With(
		zap.Stringer("type", task.Type),
		zap.Int64("task-id", task.ID),
		zap.Int("node-count", nodeCnt),
		zap.String("curr-step", proto.Step2Str(task.Type, task.Step)),
		zap.String("next-step", proto.Step2Str(task.Type, nextStep)),
	)
	var backfillMeta BackfillTaskMeta
	if err := json.Unmarshal(task.Meta, &backfillMeta); err != nil {
		return nil, err
	}
	job := &backfillMeta.Job
	logger.Info("on next subtasks batch")
	store, tbl, err := getUserStoreAndTable(ctx, sch.d, sch.d.store, task.Keyspace, job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// TODO: use planner.
	switch nextStep {
	case proto.BackfillStepReadIndex:
		// TODO(tangenta): use available disk during adding index.
		availableDisk := sch.nodeRes.GetTaskDiskResource(&task.TaskBase, vardef.DDLDiskQuota.Load())
		logger.Info("available local disk space resource", zap.String("size", units.BytesSize(float64(availableDisk))))
		return generateReadIndexPlan(ctx, sch.d, store, tbl, job, sch.GlobalSort, nodeCnt, logger)
	case proto.BackfillStepMergeSort:
		metaBytes, err2 := generateMergeSortPlan(ctx, taskHandle, task, nodeCnt, backfillMeta.CloudStorageURI, logger)
		if err2 != nil {
			return nil, err2
		}
		if len(metaBytes) > 0 {
			dxfmetric.ScheduleEventCounter.WithLabelValues(fmt.Sprint(task.ID), dxfmetric.EventMergeSort).Inc()
		}
		return metaBytes, nil
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
				store.(kv.StorageWithPD),
				taskHandle,
				task,
				backfillMeta.CloudStorageURI,
				logger)
		}
		return nil, nil
	case proto.BackfillStepMergeTempIndex:
		return generateMergeTempIndexPlan(ctx, store, tbl, nodeCnt, backfillMeta.EleIDs, logger)
	default:
		return nil, nil
	}
}

func getUserStoreAndTable(
	ctx context.Context,
	d *ddl,
	schStore kv.Storage,
	taskKeyspace string,
	job *model.Job,
) (kv.Storage, table.Table, error) {
	store := schStore
	if taskKeyspace != d.store.GetKeyspace() {
		taskMgr, err := diststorage.GetTaskManager()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		err = taskMgr.WithNewSession(func(se sessionctx.Context) error {
			store, err = se.GetSQLServer().GetKSStore(taskKeyspace)
			return err
		})
		if err != nil {
			return nil, nil, err
		}
	}
	tblInfo, err := getTblInfo(ctx, store, job)
	if err != nil {
		return nil, nil, err
	}
	tbl, err := getTable(d.ddlCtx.getAutoIDRequirement(), job.SchemaID, tblInfo)
	if err != nil {
		return nil, nil, err
	}
	return store, tbl, nil
}

// GetNextStep implements scheduler.Extension interface.
func (sch *LitBackfillScheduler) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit:
		if sch.MergeTempIndex {
			return proto.BackfillStepMergeTempIndex
		}
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
	case proto.BackfillStepMergeTempIndex:
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
	tbl table.Table,
	job *model.Job,
	useCloud bool,
	nodeCnt int,
	logger *zap.Logger,
) (metas [][]byte, err error) {
	jobReorgCtx := d.jobContext(job.ID, job.ReorgMeta)
	if tbl.Meta().Partition == nil {
		return generatePlanForPhysicalTable(ctx, jobReorgCtx, store, tbl.(table.PhysicalTable), job, useCloud, nodeCnt, logger)
	}
	defs := tbl.Meta().Partition.Definitions
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
	objStore, err := handle.NewObjStore(ctx, cloudStorageURI)
	if err != nil {
		return nil, err
	}
	defer func() {
		objStore.Close()
	}()
	for _, step := range []proto.Step{proto.BackfillStepMergeSort, proto.BackfillStepReadIndex} {
		hasSubtasks := false
		err := forEachBackfillSubtaskMeta(ctx, objStore, taskHandle, task.ID, step, func(subtask *BackfillSubTaskMeta) {
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
		if err := writeExternalBackfillSubTaskMeta(ctx, objStore, m, external.PlanMetaPath(
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
			zap.String("endKey", hex.EncodeToString(endKey)),
			zap.Int("dataFilesCnt", len(dataFiles)),
			zap.Int("rangeJobKeysCnt", len(interiorRangeJobKeys)),
			zap.Int("regionSplitKeysCnt", len(interiorRegionSplitKeys)),
		)

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

func generateMergeSortPlan(
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
	objStore, err := handle.NewObjStore(ctx, cloudStorageURI)
	if err != nil {
		return nil, err
	}
	defer func() {
		objStore.Close()
	}()
	err = forEachBackfillSubtaskMeta(ctx, objStore, taskHandle, task.ID, proto.BackfillStepReadIndex,
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
	concurrency := task.GetRuntimeSlots()
	for _, multiStats := range multiStatsGroup {
		if !skipMergeSort(multiStats, concurrency) {
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
		dataFilesGroup, err := external.DivideMergeSortDataFiles(dataFiles, nodeCnt, concurrency)
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
		if err := writeExternalBackfillSubTaskMeta(ctx, objStore, m, external.PlanMetaPath(
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
	backend, err := objstore.ParseBackend(cloudStorageURI, nil)
	if err != nil {
		return nil, err
	}
	extStore, err := objstore.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		return nil, err
	}

	rangeGroupSize := totalSize / instanceCnt
	rangeGroupKeys := int64(math.MaxInt64)

	regionSplitSize, regionSplitKeys := handle.GetDefaultRegionSplitConfig()
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
	extStore storeapi.Storage,
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
		subtask, err := decodeBackfillSubTaskMeta(ctx, extStore, subTaskMeta)
		if err != nil {
			logutil.DDLLogger().Error("unmarshal error", zap.Error(err))
			return errors.Trace(err)
		}
		fn(subtask)
	}
	return nil
}

func generateMergeTempIndexPlan(
	ctx context.Context,
	store kv.Storage,
	tbl table.Table,
	nodeCnt int,
	idxIDs []int64,
	logger *zap.Logger,
) ([][]byte, error) {
	tblInfo := tbl.Meta()
	idxInfos, err := findIndexInfosByIDs(tblInfo, idxIDs)
	if err != nil {
		return nil, err
	}
	physicalTbl := tbl.(table.PhysicalTable)
	if tblInfo.Partition == nil {
		allMeta := make([][]byte, 0, 16)
		for _, idxInfo := range idxInfos {
			meta, err := genMergeTempPlanForOneIndex(ctx, store, physicalTbl, idxInfo, nodeCnt, logger)
			if err != nil {
				return nil, err
			}
			allMeta = append(allMeta, meta...)
		}
		return allMeta, nil
	}

	allMeta := make([][]byte, 0, 16)
	for _, idxInfo := range idxInfos {
		if idxInfo.Global {
			meta, err := genMergeTempPlanForOneIndex(ctx, store, physicalTbl, idxInfo, nodeCnt, logger)
			if err != nil {
				return nil, err
			}
			allMeta = append(allMeta, meta...)
			continue
		}
		defs := tblInfo.Partition.Definitions
		for _, def := range defs {
			partTbl := tbl.GetPartitionedTable().GetPartition(def.ID)
			partMeta, err := genMergeTempPlanForOneIndex(ctx, store, partTbl, idxInfo, nodeCnt, logger)
			if err != nil {
				return nil, err
			}
			allMeta = append(allMeta, partMeta...)
		}
	}
	return allMeta, nil
}

func findIndexInfosByIDs(
	tblInfo *model.TableInfo,
	idxIDs []int64,
) ([]*model.IndexInfo, error) {
	idxInfos := make([]*model.IndexInfo, 0, len(idxIDs))
	for _, id := range idxIDs {
		idx := model.FindIndexInfoByID(tblInfo.Indices, id)
		if idx == nil {
			return nil, errors.Errorf("index ID %d not found", id)
		}
		idxInfos = append(idxInfos, idx)
	}
	return idxInfos, nil
}

func genMergeTempPlanForOneIndex(
	ctx context.Context,
	store kv.Storage,
	tbl table.PhysicalTable,
	idxInfo *model.IndexInfo,
	nodeCnt int,
	logger *zap.Logger,
) ([][]byte, error) {
	pid := tbl.GetPhysicalID()
	start, end := encodeTempIndexRange(pid, idxInfo.ID, idxInfo.ID)

	subTaskMetas := make([][]byte, 0, 4)
	backoffer := backoff.NewExponential(scanRegionBackoffBase, 2, scanRegionBackoffMax)
	err := handle.RunWithRetry(ctx, 8, backoffer, logutil.DDLLogger(), func(_ context.Context) (bool, error) {
		regionCache := store.(helper.Storage).GetRegionCache()
		regionMetas, err := regionCache.LoadRegionsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 20000, nil), start, end)
		if err != nil {
			return false, err
		}
		sort.Slice(regionMetas, func(i, j int) bool {
			return bytes.Compare(regionMetas[i].StartKey(), regionMetas[j].StartKey()) < 0
		})

		// Check if regions are continuous.
		shouldRetry := false
		cur := regionMetas[0]
		for _, m := range regionMetas[1:] {
			if !bytes.Equal(cur.EndKey(), m.StartKey()) {
				shouldRetry = true
				break
			}
			cur = m
		}

		if shouldRetry {
			return true, nil
		}

		regionBatch := calculateTempIndexRegionBatch(len(regionMetas), nodeCnt)
		logger.Info("calculate temp index region batch",
			zap.Int64("physicalTableID", pid),
			zap.Int("totalRegionCnt", len(regionMetas)),
			zap.Int("regionBatch", regionBatch),
			zap.Int("instanceCnt", nodeCnt),
		)

		for i := 0; i < len(regionMetas); i += regionBatch {
			endIdx := min(i+regionBatch, len(regionMetas))
			batch := regionMetas[i:endIdx]
			subTaskMeta := &BackfillSubTaskMeta{
				PhysicalTableID: pid,
				SortedKVMeta: external.SortedKVMeta{
					StartKey: batch[0].StartKey(),
					EndKey:   batch[len(batch)-1].EndKey(),
				},
			}
			if i == 0 {
				subTaskMeta.StartKey = start
			}
			if endIdx == len(regionMetas) {
				subTaskMeta.EndKey = end
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

func calculateTempIndexRegionBatch(totalRegionCnt int, nodeCnt int) int {
	var regionBatch int
	avgTasksPerInstance := (totalRegionCnt + nodeCnt - 1) / nodeCnt // ceiling
	regionBatch = max(avgTasksPerInstance, 1)
	return regionBatch
}
