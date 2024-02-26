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
	"strconv"
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
	brlogutil "github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// importStepExecutor is a executor for import step.
// StepExecutor is equivalent to a Lightning instance.
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
	idAlloc := kv.NewPanickingAllocators(0)
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

	return importer.NewTableImporter(ctx, controller, strconv.FormatInt(taskID, 10))
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
		// This is because the executor currently does not have a post-processing mechanism.
		// If we import the index in `cleanupSubtaskEnv`, the scheduler will not wait for the import to complete.
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
		Progress:         importer.NewProgress(),
		Checksum:         verification.NewKVGroupChecksumWithKeyspace(s.tableImporter.GetCodec()),
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

func (*importStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return nil
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

	var dataKVCount uint64
	if s.tableImporter.IsLocalSort() {
		// TODO: we should close and cleanup engine in all case, since there's no checkpoint.
		s.logger.Info("import data engine", zap.Int32("engine-id", subtaskMeta.ID))
		closedDataEngine, err := sharedVars.DataEngine.Close(ctx)
		if err != nil {
			return err
		}
		dataKVCount2, err := s.tableImporter.ImportAndCleanup(ctx, closedDataEngine)
		if err != nil {
			return err
		}
		dataKVCount = uint64(dataKVCount2)

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
	subtaskMeta.Checksum = map[int64]Checksum{}
	for id, c := range sharedVars.Checksum.GetInnerChecksums() {
		subtaskMeta.Checksum[id] = Checksum{
			Sum:  c.Sum(),
			KVs:  c.SumKVS(),
			Size: c.SumSize(),
		}
	}
	subtaskMeta.Result = Result{
		LoadedRowCnt: dataKVCount,
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

type mergeSortStepExecutor struct {
	taskexecutor.EmptyStepExecutor
	taskID     int64
	taskMeta   *TaskMeta
	logger     *zap.Logger
	controller *importer.LoadDataController
	// subtask of a task is run in serial now, so we don't need lock here.
	// change to SyncMap when we support parallel subtask in the future.
	subtaskSortedKVMeta *external.SortedKVMeta
	partSize            int64
}

var _ execute.StepExecutor = &mergeSortStepExecutor{}

func (m *mergeSortStepExecutor) Init(ctx context.Context) error {
	controller, err := buildController(&m.taskMeta.Plan, m.taskMeta.Stmt)
	if err != nil {
		return err
	}
	if err = controller.InitDataStore(ctx); err != nil {
		return err
	}
	m.controller = controller
	// 10000 = max part num
	m.partSize = int64(getWriterMemorySizeLimit(&m.taskMeta.Plan) / 10000 * uint64(external.MergeSortOverlapThreshold))
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

	logger.Info("merge sort partSize", zap.String("size", units.BytesSize(float64(m.partSize))))

	err = external.MergeOverlappingFiles(
		logutil.WithFields(ctx, zap.String("kv-group", sm.KVGroup), zap.Int64("subtask-id", subtask.ID)),
		sm.DataFiles,
		m.controller.GlobalSortStore,
		m.partSize,
		64*1024,
		prefix,
		getKVGroupBlockSize(sm.KVGroup),
		external.DefaultMemSizeLimit,
		8*1024,
		1*size.MB,
		8*1024,
		onClose,
		m.taskMeta.Plan.ThreadCnt,
		false)
	logger.Info(
		"merge sort finished",
		zap.String("kv-group", sm.KVGroup),
		zap.Uint64("total-kv-size", m.subtaskSortedKVMeta.TotalKVSize),
		zap.Uint64("total-kv-count", m.subtaskSortedKVMeta.TotalKVCnt),
		brlogutil.Key("start-key", m.subtaskSortedKVMeta.StartKey),
		brlogutil.Key("end-key", m.subtaskSortedKVMeta.EndKey),
	)
	return err
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

var _ execute.StepExecutor = &writeAndIngestStepExecutor{}

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

func (*writeAndIngestStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return nil
}

func (e *writeAndIngestStepExecutor) OnFinished(ctx context.Context, subtask *proto.Subtask) error {
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
	err := localBackend.CleanupEngine(ctx, engineUUID)
	if err != nil {
		e.logger.Warn("failed to cleanup engine", zap.Error(err))
	}

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

type postProcessStepExecutor struct {
	taskexecutor.EmptyStepExecutor
	taskID   int64
	taskMeta *TaskMeta
	logger   *zap.Logger
}

var _ execute.StepExecutor = &postProcessStepExecutor{}

// NewPostProcessStepExecutor creates a new post process step executor.
// exported for testing.
func NewPostProcessStepExecutor(taskID int64, taskMeta *TaskMeta, logger *zap.Logger) execute.StepExecutor {
	return &postProcessStepExecutor{
		taskID:   taskID,
		taskMeta: taskMeta,
		logger:   logger,
	}
}

func (p *postProcessStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
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

type importExecutor struct {
	*taskexecutor.BaseTaskExecutor
}

func newImportExecutor(ctx context.Context, id string, task *proto.Task, taskTable taskexecutor.TaskTable) taskexecutor.TaskExecutor {
	metrics := metricsManager.getOrCreateMetrics(task.ID)
	subCtx := metric.WithCommonMetric(ctx, metrics)
	s := &importExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(subCtx, id, task, taskTable),
	}
	s.BaseTaskExecutor.Extension = s
	return s
}

func (*importExecutor) IsIdempotent(*proto.Subtask) bool {
	// import don't have conflict detection and resolution now, so it's ok
	// to import data twice.
	return true
}

func (*importExecutor) IsRetryableError(err error) bool {
	return common.IsRetryableError(err)
}

func (*importExecutor) GetStepExecutor(task *proto.Task, _ *proto.StepResource) (execute.StepExecutor, error) {
	taskMeta := TaskMeta{}
	if err := json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return nil, errors.Trace(err)
	}
	logger := logutil.BgLogger().With(
		zap.Stringer("type", proto.ImportInto),
		zap.Int64("task-id", task.ID),
		zap.String("step", proto.Step2Str(task.Type, task.Step)),
	)

	switch task.Step {
	case proto.ImportStepImport, proto.ImportStepEncodeAndSort:
		return &importStepExecutor{
			taskID:   task.ID,
			taskMeta: &taskMeta,
			logger:   logger,
		}, nil
	case proto.ImportStepMergeSort:
		return &mergeSortStepExecutor{
			taskID:   task.ID,
			taskMeta: &taskMeta,
			logger:   logger,
		}, nil
	case proto.ImportStepWriteAndIngest:
		return &writeAndIngestStepExecutor{
			taskID:   task.ID,
			taskMeta: &taskMeta,
			logger:   logger,
		}, nil
	case proto.ImportStepPostProcess:
		return NewPostProcessStepExecutor(task.ID, &taskMeta, logger), nil
	default:
		return nil, errors.Errorf("unknown step %d for import task %d", task.Step, task.ID)
	}
}

func (e *importExecutor) Close() {
	task := e.GetTask()
	metricsManager.unregister(task.ID)
	e.BaseTaskExecutor.Close()
}

func init() {
	taskexecutor.RegisterTaskType(proto.ImportInto, newImportExecutor)
}
