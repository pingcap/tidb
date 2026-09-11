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

package importinto

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler/execute"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/executor/asyncloaddata"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// importStepExecutor is a executor for import step.
// SubtaskExecutor is equivalent to a Lightning instance.
type importStepExecutor struct {
	taskID        int64
	taskMeta      *TaskMeta
	tableImporter *importer.TableImporter
	sharedVars    sync.Map
	logger        *zap.Logger

	indexMemorySizeLimit uint64

	importCtx    context.Context
	importCancel context.CancelFunc
	wg           sync.WaitGroup
}

func getTableImporter(ctx context.Context, taskID int64, taskMeta *TaskMeta) (*importer.TableImporter, error) {
	idAlloc := kv.NewPanickingAllocators(taskMeta.Plan.TableInfo.SepAutoInc())
	tbl, err := tables.TableFromMeta(idAlloc, taskMeta.Plan.TableInfo)
	if err != nil {
		return nil, err
	}
	astArgs, err := importer.ASTArgsFromStmt(taskMeta.Stmt)
	if err != nil {
		return nil, err
	}
	controller, err := importer.NewLoadDataController(&taskMeta.Plan, tbl, astArgs)
	if err != nil {
		return nil, err
	}
	if err = controller.InitDataStore(ctx); err != nil {
		return nil, err
	}

	return importer.NewTableImporter(&importer.JobImportParam{
		GroupCtx: ctx,
		Progress: asyncloaddata.NewProgress(false),
		Job:      &asyncloaddata.Job{},
	}, controller, taskID)
}

func (s *importStepExecutor) Init(ctx context.Context) error {
	s.logger.Info("init subtask env")
	tableImporter, err := getTableImporter(ctx, s.taskID, s.taskMeta)
	if err != nil {
		return err
	}
	s.tableImporter = tableImporter

	// we need this sub context since Cleanup which wait on this routine is called
	// before parent context is canceled in normal flow.
	s.importCtx, s.importCancel = context.WithCancel(ctx)
	// only need to check disk quota when we are using local sort.
	if s.tableImporter.IsLocalSort() {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.tableImporter.CheckDiskQuota(s.importCtx)
		}()
	}
	s.indexMemorySizeLimit = getWriterMemorySizeLimit(s.tableImporter.Plan)
	s.logger.Info("index writer memory size limit",
		zap.String("limit", units.BytesSize(float64(s.indexMemorySizeLimit))))
	return nil
}

func (s *importStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := s.logger.With(zap.Int64("subtask-id", subtask.ID))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()
	bs := subtask.Meta
	var subtaskMeta ImportStepMeta
	err = json.Unmarshal(bs, &subtaskMeta)
	if err != nil {
		return errors.Trace(err)
	}

	var dataEngine, indexEngine *backend.OpenedEngine
	if s.tableImporter.IsLocalSort() {
		dataEngine, err = s.tableImporter.OpenDataEngine(ctx, subtaskMeta.ID)
		if err != nil {
			return err
		}
		// Unlike in Lightning, we start an index engine for each subtask,
		// whereas previously there was only a single index engine globally.
		// This is because the scheduler currently does not have a post-processing mechanism.
		// If we import the index in `cleanupSubtaskEnv`, the dispatcher will not wait for the import to complete.
		// Multiple index engines may suffer performance degradation due to range overlap.
		// These issues will be alleviated after we integrate s3 sorter.
		// engineID = -1, -2, -3, ...
		indexEngine, err = s.tableImporter.OpenIndexEngine(ctx, common.IndexEngineID-subtaskMeta.ID)
		if err != nil {
			return err
		}
	}
	sharedVars := &SharedVars{
		TableImporter:    s.tableImporter,
		DataEngine:       dataEngine,
		IndexEngine:      indexEngine,
		Progress:         asyncloaddata.NewProgress(false),
		Checksum:         &verification.KVChecksum{},
		SortedDataMeta:   &external.SortedKVMeta{},
		SortedIndexMetas: make(map[int64]*external.SortedKVMeta),
	}
	s.sharedVars.Store(subtaskMeta.ID, sharedVars)

	source := operator.NewSimpleDataChannel(make(chan *importStepMinimalTask))
	op := newEncodeAndSortOperator(ctx, s, sharedVars, subtask.ID, s.indexMemorySizeLimit)
	op.SetSource(source)
	pipeline := operator.NewAsyncPipeline(op)
	if err = pipeline.Execute(); err != nil {
		return err
	}

outer:
	for _, chunk := range subtaskMeta.Chunks {
		// TODO: current workpool impl doesn't drain the input channel, it will
		// just return on context cancel(error happened), so we add this select.
		select {
		case source.Channel() <- &importStepMinimalTask{
			Plan:       s.taskMeta.Plan,
			Chunk:      chunk,
			SharedVars: sharedVars,
		}:
		case <-op.Done():
			break outer
		}
	}
	source.Finish()

	return pipeline.Close()
}

func (s *importStepExecutor) OnFinished(ctx context.Context, subtask *proto.Subtask) error {
	var subtaskMeta ImportStepMeta
	if err := json.Unmarshal(subtask.Meta, &subtaskMeta); err != nil {
		return errors.Trace(err)
	}
	s.logger.Info("on subtask finished", zap.Int32("engine-id", subtaskMeta.ID))

	val, ok := s.sharedVars.Load(subtaskMeta.ID)
	if !ok {
		return errors.Errorf("sharedVars %d not found", subtaskMeta.ID)
	}
	sharedVars, ok := val.(*SharedVars)
	if !ok {
		return errors.Errorf("sharedVars %d not found", subtaskMeta.ID)
	}

	var dataKVCount int64
	if s.tableImporter.IsLocalSort() {
		// TODO: we should close and cleanup engine in all case, since there's no checkpoint.
		s.logger.Info("import data engine", zap.Int32("engine-id", subtaskMeta.ID))
		closedDataEngine, err := sharedVars.DataEngine.Close(ctx)
		if err != nil {
			return err
		}
		dataKVCount, err = s.tableImporter.ImportAndCleanup(ctx, closedDataEngine)
		if err != nil {
			return err
		}

		s.logger.Info("import index engine", zap.Int32("engine-id", subtaskMeta.ID))
		if closedEngine, err := sharedVars.IndexEngine.Close(ctx); err != nil {
			return err
		} else if _, err := s.tableImporter.ImportAndCleanup(ctx, closedEngine); err != nil {
			return err
		}
	}
	// there's no imported dataKVCount on this stage when using global sort.

	sharedVars.mu.Lock()
	defer sharedVars.mu.Unlock()
	subtaskMeta.Checksum.Sum = sharedVars.Checksum.Sum()
	subtaskMeta.Checksum.KVs = sharedVars.Checksum.SumKVS()
	subtaskMeta.Checksum.Size = sharedVars.Checksum.SumSize()
	subtaskMeta.Result = Result{
		LoadedRowCnt: uint64(dataKVCount),
		ColSizeMap:   sharedVars.Progress.GetColSize(),
	}
	allocators := sharedVars.TableImporter.Allocators()
	subtaskMeta.MaxIDs = map[autoid.AllocatorType]int64{
		autoid.RowIDAllocType:    allocators.Get(autoid.RowIDAllocType).Base(),
		autoid.AutoIncrementType: allocators.Get(autoid.AutoIncrementType).Base(),
		autoid.AutoRandomType:    allocators.Get(autoid.AutoRandomType).Base(),
	}
	subtaskMeta.SortedDataMeta = sharedVars.SortedDataMeta
	subtaskMeta.SortedIndexMetas = sharedVars.SortedIndexMetas
	s.sharedVars.Delete(subtaskMeta.ID)
	newMeta, err := json.Marshal(subtaskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	subtask.Meta = newMeta
	return nil
}

func (s *importStepExecutor) Cleanup(_ context.Context) (err error) {
	s.logger.Info("cleanup subtask env")
	s.importCancel()
	s.wg.Wait()
	return s.tableImporter.Close()
}

func (s *importStepExecutor) Rollback(context.Context) error {
	// TODO: add rollback
	s.logger.Info("rollback")
	return nil
}

type mergeSortStepExecutor struct {
	scheduler.EmptySubtaskExecutor
	taskID     int64
	taskMeta   *TaskMeta
	logger     *zap.Logger
	controller *importer.LoadDataController
	// subtask of a task is run in serial now, so we don't need lock here.
	// change to SyncMap when we support parallel subtask in the future.
	subtaskSortedKVMeta *external.SortedKVMeta
}

var _ execute.SubtaskExecutor = &mergeSortStepExecutor{}

func (m *mergeSortStepExecutor) Init(ctx context.Context) error {
	controller, err := buildController(&m.taskMeta.Plan, m.taskMeta.Stmt)
	if err != nil {
		return err
	}
	if err = controller.InitDataStore(ctx); err != nil {
		return err
	}
	m.controller = controller
	return nil
}

func (m *mergeSortStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := m.logger.With(zap.Int64("subtask-id", subtask.ID))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	sm := &MergeSortStepMeta{}
	err = json.Unmarshal(subtask.Meta, sm)
	if err != nil {
		return errors.Trace(err)
	}

	var mu sync.Mutex
	m.subtaskSortedKVMeta = &external.SortedKVMeta{}
	onClose := func(summary *external.WriterSummary) {
		mu.Lock()
		defer mu.Unlock()
		m.subtaskSortedKVMeta.MergeSummary(summary)
	}

	prefix := subtaskPrefix(m.taskID, subtask.ID)

	return external.MergeOverlappingFiles(ctx, sm.DataFiles, m.controller.GlobalSortStore, 64*1024,
		prefix, getKVGroupBlockSize(sm.KVGroup), 8*1024, 1*size.MB, 8*1024,
		onClose, int(m.taskMeta.Plan.ThreadCnt), false)
}

func (m *mergeSortStepExecutor) OnFinished(_ context.Context, subtask *proto.Subtask) error {
	var subtaskMeta MergeSortStepMeta
	if err := json.Unmarshal(subtask.Meta, &subtaskMeta); err != nil {
		return errors.Trace(err)
	}
	subtaskMeta.SortedKVMeta = *m.subtaskSortedKVMeta
	m.subtaskSortedKVMeta = nil
	newMeta, err := json.Marshal(subtaskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	subtask.Meta = newMeta
	return nil
}

type writeAndIngestStepExecutor struct {
	taskID        int64
	taskMeta      *TaskMeta
	logger        *zap.Logger
	tableImporter *importer.TableImporter
}

var _ execute.SubtaskExecutor = &writeAndIngestStepExecutor{}

func (e *writeAndIngestStepExecutor) Init(ctx context.Context) error {
	tableImporter, err := getTableImporter(ctx, e.taskID, e.taskMeta)
	if err != nil {
		return err
	}
	e.tableImporter = tableImporter
	return nil
}

func (e *writeAndIngestStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	sm := &WriteIngestStepMeta{}
	err = json.Unmarshal(subtask.Meta, sm)
	if err != nil {
		return errors.Trace(err)
	}

	logger := e.logger.With(zap.Int64("subtask-id", subtask.ID),
		zap.String("kv-group", sm.KVGroup))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	_, engineUUID := backend.MakeUUID("", subtask.ID)
	localBackend := e.tableImporter.Backend()
	err = localBackend.CloseEngine(ctx, &backend.EngineConfig{
		External: &backend.ExternalEngineConfig{
			StorageURI:      e.taskMeta.Plan.CloudStorageURI,
			DataFiles:       sm.DataFiles,
			StatFiles:       sm.StatFiles,
			StartKey:        sm.StartKey,
			EndKey:          sm.EndKey,
			SplitKeys:       sm.RangeSplitKeys,
			RegionSplitSize: sm.RangeSplitSize,
			TotalFileSize:   int64(sm.TotalKVSize),
			TotalKVCount:    0,
			CheckHotspot:    false,
		},
	}, engineUUID)
	if err != nil {
		return err
	}
	return localBackend.ImportEngine(ctx, engineUUID, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
}

func (e *writeAndIngestStepExecutor) OnFinished(_ context.Context, subtask *proto.Subtask) error {
	var subtaskMeta WriteIngestStepMeta
	if err := json.Unmarshal(subtask.Meta, &subtaskMeta); err != nil {
		return errors.Trace(err)
	}
	if subtaskMeta.KVGroup != dataKVGroup {
		return nil
	}

	// only data kv group has loaded row count
	_, engineUUID := backend.MakeUUID("", subtask.ID)
	localBackend := e.tableImporter.Backend()
	_, kvCount := localBackend.GetExternalEngineKVStatistics(engineUUID)
	subtaskMeta.Result.LoadedRowCnt = uint64(kvCount)

	newMeta, err := json.Marshal(subtaskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	subtask.Meta = newMeta
	return nil
}

func (e *writeAndIngestStepExecutor) Cleanup(_ context.Context) (err error) {
	e.logger.Info("cleanup subtask env")
	return e.tableImporter.Close()
}

func (e *writeAndIngestStepExecutor) Rollback(context.Context) error {
	e.logger.Info("rollback")
	return nil
}

type postStepExecutor struct {
	scheduler.EmptySubtaskExecutor
	taskID   int64
	taskMeta *TaskMeta
	logger   *zap.Logger
}

var _ execute.SubtaskExecutor = &postStepExecutor{}

func (p *postStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := p.logger.With(zap.Int64("subtask-id", subtask.ID))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()
	stepMeta := PostProcessStepMeta{}
	if err = json.Unmarshal(subtask.Meta, &stepMeta); err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("waitBeforePostProcess", func() {
		time.Sleep(5 * time.Second)
	})
	return postProcess(ctx, p.taskMeta, &stepMeta, logger)
}

type importScheduler struct {
	*scheduler.BaseScheduler
}

func newImportScheduler(ctx context.Context, id string, task *proto.Task, taskTable scheduler.TaskTable) scheduler.Scheduler {
	s := &importScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(ctx, id, task.ID, taskTable),
	}
	s.BaseScheduler.Extension = s
	return s
}

func (s *importScheduler) Run(ctx context.Context, task *proto.Task) error {
	metrics := metricsManager.getOrCreateMetrics(task.ID)
	defer metricsManager.unregister(task.ID)
	subCtx := metric.WithCommonMetric(ctx, metrics)
	return s.BaseScheduler.Run(subCtx, task)
}

func (*importScheduler) IsIdempotent(*proto.Subtask) bool {
	// import don't have conflict detection and resolution now, so it's ok
	// to import data twice.
	return true
}

func (*importScheduler) GetSubtaskExecutor(_ context.Context, task *proto.Task, _ *execute.Summary) (execute.SubtaskExecutor, error) {
	taskMeta := TaskMeta{}
	if err := json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return nil, errors.Trace(err)
	}
	logger := logutil.BgLogger().With(
		zap.Stringer("type", proto.ImportInto),
		zap.Int64("task-id", task.ID),
		zap.String("step", stepStr(task.Step)),
	)
	logger.Info("create step scheduler")

	switch task.Step {
	case StepImport, StepEncodeAndSort:
		return &importStepExecutor{
			taskID:   task.ID,
			taskMeta: &taskMeta,
			logger:   logger,
		}, nil
	case StepMergeSort:
		return &mergeSortStepExecutor{
			taskID:   task.ID,
			taskMeta: &taskMeta,
			logger:   logger,
		}, nil
	case StepWriteAndIngest:
		return &writeAndIngestStepExecutor{
			taskID:   task.ID,
			taskMeta: &taskMeta,
			logger:   logger,
		}, nil
	case StepPostProcess:
		return &postStepExecutor{
			taskID:   task.ID,
			taskMeta: &taskMeta,
			logger:   logger,
		}, nil
	default:
		return nil, errors.Errorf("unknown step %d for import task %d", task.Step, task.ID)
	}
}

func init() {
	scheduler.RegisterTaskType(proto.ImportInto, newImportScheduler)
}
