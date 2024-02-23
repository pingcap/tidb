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
<<<<<<< HEAD
=======
	"math"
	"sort"
	"time"
>>>>>>> 1d68adada36 (ddl, disttask: add scan-region check before dispatch subtasks (#51242))

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler/execute"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/backoff"
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
	unique, err := decodeIndexUniqueness(job)
	if err != nil {
		return err
	}
	pdLeaderAddr := d.store.(tikv.Storage).GetRegionCache().PDClient().GetLeaderAddr()
	bc, err := ingest.LitBackCtxMgr.Register(ctx, unique, job.ID, d.etcdCli, pdLeaderAddr, job.ReorgMeta.ResourceGroupName)
	if err != nil {
		return errors.Trace(err)
	}
	s.backendCtx = bc
	s.jobID = job.ID
	return nil
}

func decodeIndexUniqueness(job *model.Job) (bool, error) {
	unique := make([]bool, 1)
	err := job.DecodeArgs(&unique[0])
	if err != nil {
		err = job.DecodeArgs(&unique)
	}
	if err != nil {
		return false, errors.Trace(err)
	}
	// We only support adding multiple unique indexes or multiple non-unique indexes,
	// we use the first index uniqueness here.
	return unique[0], nil
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

func newLitBackfillScheduler(ctx context.Context, d *ddl, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
	sch := LitBackfillScheduler{
		d:             d,
		BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param),
	}
	return &sch
}

// Init implements BaseScheduler interface.
func (sch *LitBackfillScheduler) Init() (err error) {
	taskMeta := &BackfillTaskMeta{}
	if err = json.Unmarshal(sch.BaseScheduler.GetTask().Meta, taskMeta); err != nil {
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
	err = kv.RunInNewTxn(d.ctx, d.store, true, func(_ context.Context, txn kv.Transaction) error {
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

const (
	scanRegionBackoffBase = 200 * time.Millisecond
	scanRegionBackoffMax  = 2 * time.Second
)

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

	subTaskMetas := make([][]byte, 0, 4)
	backoffer := backoff.NewExponential(scanRegionBackoffBase, 2, scanRegionBackoffMax)
	err = handle.RunWithRetry(d.ctx, 8, backoffer, logutil.Logger(d.ctx), func(_ context.Context) (bool, error) {
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

		regionBatch := calculateRegionBatch(len(recordRegionMetas), instanceCnt, !useCloud)

		for i := 0; i < len(recordRegionMetas); i += regionBatch {
			end := i + regionBatch
			if end > len(recordRegionMetas) {
				end = len(recordRegionMetas)
			}
			batch := recordRegionMetas[i:end]
			subTaskMeta := &BackfillSubTaskMeta{
				RowStart: batch[0].StartKey(),
				RowEnd:   batch[len(batch)-1].EndKey(),
			}
			if i == 0 {
				subTaskMeta.RowStart = startKey
			}
			if end == len(recordRegionMetas) {
				subTaskMeta.RowEnd = endKey
			}
			metaBytes, err := json.Marshal(subTaskMeta)
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

func calculateRegionBatch(totalRegionCnt int, instanceCnt int, useLocalDisk bool) int {
	var regionBatch int
	avgTasksPerInstance := (totalRegionCnt + instanceCnt - 1) / instanceCnt // ceiling
	if useLocalDisk {
		regionBatch = avgTasksPerInstance
	} else {
		// For cloud storage, each subtask should contain no more than 100 regions.
		regionBatch = min(100, avgTasksPerInstance)
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
	step proto.Step,
	logger *zap.Logger,
) ([][]byte, error) {
	var kvMetaGroups []*external.SortedKVMeta
	err := forEachBackfillSubtaskMeta(taskHandle, task.ID, step, func(subtask *BackfillSubTaskMeta) {
		if kvMetaGroups == nil {
			kvMetaGroups = make([]*external.SortedKVMeta, len(subtask.MetaGroups))
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
	instanceIDs, err := scheduler.GetLiveExecIDs(ctx)
	if err != nil {
		return nil, err
	}
	metaArr := make([][]byte, 0, 16)
	for i, g := range kvMetaGroups {
		if g == nil {
			logger.Error("meet empty kv group when getting subtask summary",
				zap.Int64("taskID", task.ID))
			return nil, errors.Errorf("subtask kv group %d is empty", i)
		}
		newMeta, err := splitSubtaskMetaForOneKVMetaGroup(ctx, store, g, cloudStorageURI, int64(len(instanceIDs)), logger)
		if err != nil {
			return nil, errors.Trace(err)
		}
		metaArr = append(metaArr, newMeta...)
	}
	return metaArr, nil
}

func splitSubtaskMetaForOneKVMetaGroup(
	ctx context.Context,
	store kv.StorageWithPD,
	kvMeta *external.SortedKVMeta,
	cloudStorageURI string,
	instanceCnt int64,
	logger *zap.Logger,
) (metaArr [][]byte, err error) {
	if len(kvMeta.StartKey) == 0 && len(kvMeta.EndKey) == 0 {
		// Skip global sort for empty table.
		return nil, nil
	}
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
		endKeyOfGroup, dataFiles, statFiles, rangeSplitKeys, err := splitter.SplitOneRangesGroup()
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
		m := &BackfillSubTaskMeta{
			MetaGroups: []*external.SortedKVMeta{{
				StartKey:    startKey,
				EndKey:      endKey,
				TotalKVSize: kvMeta.TotalKVSize / uint64(instanceCnt),
			}},
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
			break
		}
		startKey = endKey
	}
	return metaArr, nil
}

func generateMergePlan(
	taskHandle diststorage.TaskHandle,
	task *proto.Task,
	logger *zap.Logger,
) ([][]byte, error) {
	// check data files overlaps,
	// if data files overlaps too much, we need a merge step.
	multiStats := make([]external.MultipleFilesStat, 0, 100)
	var kvMetaGroups []*external.SortedKVMeta
	err := forEachBackfillSubtaskMeta(taskHandle, task.ID, proto.BackfillStepReadIndex,
		func(subtask *BackfillSubTaskMeta) {
			if kvMetaGroups == nil {
				kvMetaGroups = make([]*external.SortedKVMeta, len(subtask.MetaGroups))
			}
			for i, g := range subtask.MetaGroups {
				if kvMetaGroups[i] == nil {
					kvMetaGroups[i] = &external.SortedKVMeta{}
				}
				kvMetaGroups[i].Merge(g)
				multiStats = append(multiStats, g.MultipleFilesStats...)
			}
		})
	if err != nil {
		return nil, err
	}
	if skipMergeSort(multiStats) {
		logger.Info("skip merge sort")
		return nil, nil
	}

	// generate merge sort plan.
	dataFiles := make([]string, 0, 1000)
	for i, g := range kvMetaGroups {
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

	var maxSizePerRange = int64(config.SplitRegionSize)
	var maxKeysPerRange = int64(config.SplitRegionKeys)
	if store != nil {
		pdCli := store.GetPDClient()
		tls, err := ingest.NewDDLTLS()
		if err == nil {
			size, keys, err := local.GetRegionSplitSizeKeys(ctx, pdCli, tls)
			if err == nil {
				maxSizePerRange = max(maxSizePerRange, size)
				maxKeysPerRange = max(maxKeysPerRange, keys)
			} else {
				logger.Warn("fail to get region split keys and size", zap.Error(err))
			}
		} else {
			logger.Warn("fail to get region split keys and size", zap.Error(err))
		}
	}

	return external.NewRangeSplitter(ctx, multiFileStat, extStore,
		rangeGroupSize, rangeGroupKeys, maxSizePerRange, maxKeysPerRange, true)
}

func forEachBackfillSubtaskMeta(
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
		subtask, err := decodeBackfillSubTaskMeta(subTaskMeta)
		if err != nil {
			logutil.BgLogger().Error("unmarshal error",
				zap.String("category", "ddl"),
				zap.Error(err))
			return errors.Trace(err)
		}
		fn(subtask)
	}
	return nil
>>>>>>> 1d68adada36 (ddl, disttask: add scan-region check before dispatch subtasks (#51242))
}
