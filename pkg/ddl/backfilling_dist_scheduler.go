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
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfmetric"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	diststorage "github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/backoff"
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
