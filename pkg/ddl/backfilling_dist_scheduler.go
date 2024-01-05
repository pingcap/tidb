// Copyright 2023 PingCAP, Inc.
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
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler/execute"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// BackfillGlobalMeta is the global task meta for backfilling index.
type BackfillGlobalMeta struct {
	Job model.Job `json:"job"`
	// EleIDs stands for the index/column IDs to backfill with distributed framework.
	EleIDs []int64 `json:"ele_ids"`
	// EleTypeKey is the type of the element to backfill with distributed framework.
	// For now, only index type is supported.
	EleTypeKey []byte `json:"ele_type_key"`

	CloudStorageURI string `json:"cloud_storage_uri"`
	// UseMergeSort indicate whether the backfilling task use merge sort step for global sort.
	// Merge Sort step aims to support more data.
	UseMergeSort bool `json:"use_merge_sort"`
}

// BackfillSubTaskMeta is the sub-task meta for backfilling index.
type BackfillSubTaskMeta struct {
	PhysicalTableID int64 `json:"physical_table_id"`

	RangeSplitKeys        [][]byte `json:"range_split_keys"`
	DataFiles             []string `json:"data-files"`
	StatFiles             []string `json:"stat-files"`
	external.SortedKVMeta `json:",inline"`
}

// NewBackfillSubtaskExecutor creates a new backfill subtask executor.
func NewBackfillSubtaskExecutor(_ context.Context, taskMeta []byte, d *ddl,
	bc ingest.BackendCtx, stage proto.Step, summary *execute.Summary) (execute.SubtaskExecutor, error) {
	bgm := &BackfillGlobalMeta{}
	err := json.Unmarshal(taskMeta, bgm)
	if err != nil {
		return nil, err
	}
	jobMeta := &bgm.Job

	_, tbl, err := d.getTableByTxn((*asAutoIDRequirement)(d.ddlCtx), jobMeta.SchemaID, jobMeta.TableID)
	if err != nil {
		return nil, err
	}
	indexInfos := make([]*model.IndexInfo, 0, len(bgm.EleIDs))
	for _, eid := range bgm.EleIDs {
		indexInfo := model.FindIndexInfoByID(tbl.Meta().Indices, eid)
		if indexInfo == nil {
			logutil.BgLogger().Warn("index info not found", zap.String("category", "ddl-ingest"),
				zap.Int64("table ID", tbl.Meta().ID), zap.Int64("index ID", eid))
			return nil, errors.Errorf("index info not found: %d", eid)
		}
		indexInfos = append(indexInfos, indexInfo)
	}

	switch stage {
	case proto.StepOne:
		jc := d.jobContext(jobMeta.ID, jobMeta.ReorgMeta)
		d.setDDLLabelForTopSQL(jobMeta.ID, jobMeta.Query)
		d.setDDLSourceForDiagnosis(jobMeta.ID, jobMeta.Type)
		return newReadIndexExecutor(
			d, &bgm.Job, indexInfos, tbl.(table.PhysicalTable), jc, bc, summary, bgm.CloudStorageURI), nil
	case proto.StepTwo:
		return newMergeSortExecutor(jobMeta.ID, indexInfos[0], tbl.(table.PhysicalTable), bc, bgm.CloudStorageURI)
	case proto.StepThree:
		if len(bgm.CloudStorageURI) > 0 {
			return newCloudImportExecutor(&bgm.Job, jobMeta.ID, indexInfos[0], tbl.(table.PhysicalTable), bc, bgm.CloudStorageURI)
		}
		return nil, errors.Errorf("local import does not have write & ingest step")
	default:
		return nil, errors.Errorf("unknown step %d for job %d", stage, jobMeta.ID)
	}
}

type backfillDistScheduler struct {
	*scheduler.BaseScheduler
	d          *ddl
	task       *proto.Task
	taskTable  scheduler.TaskTable
	backendCtx ingest.BackendCtx
	jobID      int64
}

func newBackfillDistScheduler(ctx context.Context, id string, task *proto.Task, taskTable scheduler.TaskTable, d *ddl) scheduler.Scheduler {
	s := &backfillDistScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(ctx, id, task.ID, taskTable),
		d:             d,
		task:          task,
		taskTable:     taskTable,
	}
	s.BaseScheduler.Extension = s
	return s
}

func (s *backfillDistScheduler) Init(ctx context.Context) error {
	err := s.BaseScheduler.Init(ctx)
	if err != nil {
		return err
	}
	d := s.d

	bgm := &BackfillGlobalMeta{}
	err = json.Unmarshal(s.task.Meta, bgm)
	if err != nil {
		return errors.Trace(err)
	}
	job := &bgm.Job
	_, tbl, err := d.getTableByTxn((*asAutoIDRequirement)(d.ddlCtx), job.SchemaID, job.TableID)
	if err != nil {
		return errors.Trace(err)
	}
	// We only support adding multiple unique indexes or multiple non-unique indexes,
	// we use the first index uniqueness here.
	idx := model.FindIndexInfoByID(tbl.Meta().Indices, bgm.EleIDs[0])
	if idx == nil {
		return errors.Trace(errors.Errorf("index info not found: %d", bgm.EleIDs[0]))
	}
	pdLeaderAddr := d.store.(tikv.Storage).GetRegionCache().PDClient().GetLeaderAddr()
	bc, err := ingest.LitBackCtxMgr.Register(ctx, idx.Unique, job.ID, d.etcdCli, pdLeaderAddr, job.ReorgMeta.ResourceGroupName)
	if err != nil {
		return errors.Trace(err)
	}
	s.backendCtx = bc
	s.jobID = job.ID
	return nil
}

func (s *backfillDistScheduler) GetSubtaskExecutor(ctx context.Context, task *proto.Task, summary *execute.Summary) (execute.SubtaskExecutor, error) {
	switch task.Step {
	case proto.StepOne, proto.StepTwo, proto.StepThree:
		return NewBackfillSubtaskExecutor(ctx, task.Meta, s.d, s.backendCtx, task.Step, summary)
	default:
		return nil, errors.Errorf("unknown backfill step %d for task %d", task.Step, task.ID)
	}
}

func (*backfillDistScheduler) IsIdempotent(*proto.Subtask) bool {
	return true
}

<<<<<<< HEAD
func (s *backfillDistScheduler) Close() {
	if s.backendCtx != nil {
		ingest.LitBackCtxMgr.Unregister(s.jobID)
	}
	s.BaseScheduler.Close()
=======
// LitBackfillScheduler wraps BaseScheduler.
type LitBackfillScheduler struct {
	*scheduler.BaseScheduler
	d *ddl
}

func newLitBackfillScheduler(ctx context.Context, d *ddl, taskMgr scheduler.TaskManager,
	nodeMgr *scheduler.NodeManager, task *proto.Task) scheduler.Scheduler {
	sch := LitBackfillScheduler{
		d:             d,
		BaseScheduler: scheduler.NewBaseScheduler(ctx, taskMgr, nodeMgr, task),
	}
	return &sch
}

// Init implements BaseScheduler interface.
func (sch *LitBackfillScheduler) Init() (err error) {
	taskMeta := &BackfillTaskMeta{}
	if err = json.Unmarshal(sch.BaseScheduler.Task.Meta, taskMeta); err != nil {
		return errors.Annotate(err, "unmarshal task meta failed")
	}
	sch.BaseScheduler.Extension = &BackfillingSchedulerExt{
		d:          sch.d,
		GlobalSort: len(taskMeta.CloudStorageURI) > 0}
	return sch.BaseScheduler.Init()
}

// Close implements BaseScheduler interface.
func (sch *LitBackfillScheduler) Close() {
	sch.BaseScheduler.Close()
}

func getTblInfo(d *ddl, job *model.Job) (tblInfo *model.TableInfo, err error) {
	err = kv.RunInNewTxn(d.ctx, d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		tblInfo, err = meta.NewMeta(txn).GetTable(job.SchemaID, job.TableID)
		return err
	})
	if err != nil {
		return nil, err
	}

	return tblInfo, nil
}

func generatePartitionPlan(tblInfo *model.TableInfo) (metas [][]byte, err error) {
	defs := tblInfo.Partition.Definitions
	physicalIDs := make([]int64, len(defs))
	for i := range defs {
		physicalIDs[i] = defs[i].ID
	}

	subTaskMetas := make([][]byte, 0, len(physicalIDs))
	for _, physicalID := range physicalIDs {
		subTaskMeta := &BackfillSubTaskMeta{
			PhysicalTableID: physicalID,
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
	d *ddl,
	tblInfo *model.TableInfo,
	job *model.Job,
	useCloud bool,
	instanceCnt int) (metas [][]byte, err error) {
	tbl, err := getTable((*asAutoIDRequirement)(d.ddlCtx), job.SchemaID, tblInfo)
	if err != nil {
		return nil, err
	}
	ver, err := getValidCurrentVersion(d.store)
	if err != nil {
		return nil, errors.Trace(err)
	}

	startKey, endKey, err := getTableRange(d.jobContext(job.ID, job.ReorgMeta), d.ddlCtx, tbl.(table.PhysicalTable), ver.Ver, job.Priority)
	if startKey == nil && endKey == nil {
		// Empty table.
		return nil, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionCache := d.store.(helper.Storage).GetRegionCache()
	recordRegionMetas, err := regionCache.LoadRegionsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 20000, nil), startKey, endKey)
	if err != nil {
		return nil, err
	}
	regionBatch := calculateRegionBatch(len(recordRegionMetas), instanceCnt, !useCloud)

	subTaskMetas := make([][]byte, 0, 4)
	sort.Slice(recordRegionMetas, func(i, j int) bool {
		return bytes.Compare(recordRegionMetas[i].StartKey(), recordRegionMetas[j].StartKey()) < 0
	})
	for i := 0; i < len(recordRegionMetas); i += regionBatch {
		end := i + regionBatch
		if end > len(recordRegionMetas) {
			end = len(recordRegionMetas)
		}
		batch := recordRegionMetas[i:end]
		subTaskMeta := &BackfillSubTaskMeta{
			SortedKVMeta: external.SortedKVMeta{
				StartKey: batch[0].StartKey(),
				EndKey:   batch[len(batch)-1].EndKey(),
			},
		}
		if i == 0 {
			subTaskMeta.StartKey = startKey
		}
		if end == len(recordRegionMetas) {
			subTaskMeta.EndKey = endKey
		}
		metaBytes, err := json.Marshal(subTaskMeta)
		if err != nil {
			return nil, err
		}
		subTaskMetas = append(subTaskMetas, metaBytes)
	}
	return subTaskMetas, nil
}

func calculateRegionBatch(totalRegionCnt int, instanceCnt int, useLocalDisk bool) int {
	var regionBatch int
	avgTasksPerInstance := totalRegionCnt / instanceCnt
	if useLocalDisk {
		// Make subtask large enough to reduce the overhead of local/global flush.
		avgTasksPerDisk := int(int64(variable.DDLDiskQuota.Load()) / int64(config.SplitRegionSize))
		regionBatch = min(avgTasksPerDisk, avgTasksPerInstance)
	} else {
		regionBatch = min(100, avgTasksPerInstance)
	}
	regionBatch = max(regionBatch, 1)
	return regionBatch
}

func generateGlobalSortIngestPlan(
	ctx context.Context,
	taskHandle scheduler.TaskHandle,
	task *proto.Task,
	jobID int64,
	cloudStorageURI string,
	step proto.Step,
	logger *zap.Logger,
) ([][]byte, error) {
	startKeyFromSumm, endKeyFromSumm, totalSize, multiFileStat, err := getSummaryFromLastStep(taskHandle, task.ID, step)
	if err != nil {
		return nil, err
	}
	if len(startKeyFromSumm) == 0 && len(endKeyFromSumm) == 0 {
		// Skip global sort for empty table.
		return nil, nil
	}
	instanceIDs, err := scheduler.GenerateTaskExecutorNodes(ctx)
	if err != nil {
		return nil, err
	}
	splitter, err := getRangeSplitter(
		ctx, cloudStorageURI, jobID, int64(totalSize), int64(len(instanceIDs)), multiFileStat, logger)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := splitter.Close()
		if err != nil {
			logger.Error("failed to close range splitter", zap.Error(err))
		}
	}()

	metaArr := make([][]byte, 0, 16)
	startKey := startKeyFromSumm
	var endKey kv.Key
	for {
		endKeyOfGroup, dataFiles, statFiles, rangeSplitKeys, err := splitter.SplitOneRangesGroup()
		if err != nil {
			return nil, err
		}
		if len(endKeyOfGroup) == 0 {
			endKey = endKeyFromSumm
		} else {
			endKey = kv.Key(endKeyOfGroup).Clone()
		}
		logger.Info("split subtask range",
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)))

		if startKey.Cmp(endKey) >= 0 {
			return nil, errors.Errorf("invalid range, startKey: %s, endKey: %s",
				hex.EncodeToString(startKey), hex.EncodeToString(endKey))
		}
		m := &BackfillSubTaskMeta{
			SortedKVMeta: external.SortedKVMeta{
				StartKey:    startKey,
				EndKey:      endKey,
				TotalKVSize: totalSize / uint64(len(instanceIDs)),
			},
			DataFiles:      dataFiles,
			StatFiles:      statFiles,
			RangeSplitKeys: rangeSplitKeys,
		}
		metaBytes, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		metaArr = append(metaArr, metaBytes)
		if len(endKeyOfGroup) == 0 {
			return metaArr, nil
		}
		startKey = endKey
	}
}

func generateMergePlan(
	taskHandle scheduler.TaskHandle,
	task *proto.Task,
	logger *zap.Logger,
) ([][]byte, error) {
	// check data files overlaps,
	// if data files overlaps too much, we need a merge step.
	subTaskMetas, err := taskHandle.GetPreviousSubtaskMetas(task.ID, StepReadIndex)
	if err != nil {
		return nil, err
	}
	multiStats := make([]external.MultipleFilesStat, 0, 100)
	for _, bs := range subTaskMetas {
		var subtask BackfillSubTaskMeta
		err = json.Unmarshal(bs, &subtask)
		if err != nil {
			return nil, err
		}
		multiStats = append(multiStats, subtask.MultipleFilesStats...)
	}
	if skipMergeSort(multiStats) {
		logger.Info("skip merge sort")
		return nil, nil
	}

	// generate merge sort plan.
	_, _, _, multiFileStat, err := getSummaryFromLastStep(taskHandle, task.ID, StepReadIndex)
	if err != nil {
		return nil, err
	}
	dataFiles := make([]string, 0, 1000)
	for _, m := range multiFileStat {
		for _, filePair := range m.Filenames {
			dataFiles = append(dataFiles, filePair[0])
		}
	}

	start := 0
	step := external.MergeSortFileCountStep
	metaArr := make([][]byte, 0, 16)
	for start < len(dataFiles) {
		end := start + step
		if end > len(dataFiles) {
			end = len(dataFiles)
		}
		m := &BackfillSubTaskMeta{
			DataFiles: dataFiles[start:end],
		}
		metaBytes, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		metaArr = append(metaArr, metaBytes)

		start = end
	}
	return metaArr, nil
}

func getRangeSplitter(
	ctx context.Context,
	cloudStorageURI string,
	jobID int64,
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
	bcCtx, ok := ingest.LitBackCtxMgr.Load(jobID)
	if !ok {
		return nil, errors.Errorf("backend context not found")
	}

	local := bcCtx.GetLocalBackend()
	if local == nil {
		return nil, errors.Errorf("local backend not found")
	}
	maxSizePerRange, maxKeysPerRange, err := local.GetRegionSplitSizeKeys(ctx)
	if err != nil {
		logger.Warn("fail to get region split keys and size", zap.Error(err))
	}
	maxSizePerRange = max(maxSizePerRange, int64(config.SplitRegionSize))
	maxKeysPerRange = max(maxKeysPerRange, int64(config.SplitRegionKeys))

	return external.NewRangeSplitter(ctx, multiFileStat, extStore,
		rangeGroupSize, rangeGroupKeys, maxSizePerRange, maxKeysPerRange, true)
}

func getSummaryFromLastStep(
	taskHandle scheduler.TaskHandle,
	gTaskID int64,
	step proto.Step,
) (startKey, endKey kv.Key, totalKVSize uint64, multiFileStat []external.MultipleFilesStat, err error) {
	subTaskMetas, err := taskHandle.GetPreviousSubtaskMetas(gTaskID, step)
	if err != nil {
		return nil, nil, 0, nil, errors.Trace(err)
	}
	for _, subTaskMeta := range subTaskMetas {
		var subtask BackfillSubTaskMeta
		err := json.Unmarshal(subTaskMeta, &subtask)
		if err != nil {
			return nil, nil, 0, nil, errors.Trace(err)
		}
		// Skip empty subtask.StartKey/EndKey because it means
		// no records need to be written in this subtask.
		if subtask.StartKey == nil || subtask.EndKey == nil {
			continue
		}

		if len(startKey) == 0 {
			startKey = subtask.StartKey
		} else {
			startKey = external.BytesMin(startKey, subtask.StartKey)
		}
		if len(endKey) == 0 {
			endKey = subtask.EndKey
		} else {
			endKey = external.BytesMax(endKey, subtask.EndKey)
		}
		totalKVSize += subtask.TotalKVSize

		multiFileStat = append(multiFileStat, subtask.MultipleFilesStats...)
	}
	return startKey, endKey, totalKVSize, multiFileStat, nil
}

// StepStr convert proto.Step to string.
func StepStr(step proto.Step) string {
	switch step {
	case proto.StepInit:
		return "init"
	case StepReadIndex:
		return "read-index"
	case StepMergeSort:
		return "merge-sort"
	case StepWriteAndIngest:
		return "write&ingest"
	case proto.StepDone:
		return "done"
	default:
		return "unknown"
	}
>>>>>>> 678cb671e63 (ddl: skip sorting step when there are no subtasks (#49866))
}
