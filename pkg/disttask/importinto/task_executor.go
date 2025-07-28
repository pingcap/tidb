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
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	brlogutil "github.com/pingcap/tidb/br/pkg/logutil"
	tidbconfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// importStepExecutor is a executor for import step.
// StepExecutor is equivalent to a Lightning instance.
type importStepExecutor struct {
	taskexecutor.BaseStepExecutor

	taskID        int64
	taskMeta      *TaskMeta
	tableImporter *importer.TableImporter
	store         tidbkv.Storage
	sharedVars    sync.Map
	logger        *zap.Logger

	dataKVMemSizePerCon     uint64
	perIndexKVMemSizePerCon uint64
	indexBlockSize          int
	dataBlockSize           int

	importCtx    context.Context
	importCancel context.CancelFunc
	wg           sync.WaitGroup
	op           atomic.Pointer[encodeAndSortOperator]
	indicesGenKV map[int64]genKVIndex
}

func getTableImporter(
	ctx context.Context,
	taskID int64,
	taskMeta *TaskMeta,
	store tidbkv.Storage,
) (*importer.TableImporter, error) {
	idAlloc := kv.NewPanickingAllocators(taskMeta.Plan.TableInfo.SepAutoInc(), 0)
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

	return importer.NewTableImporter(ctx, controller, strconv.FormatInt(taskID, 10), store)
}

func (s *importStepExecutor) Init(ctx context.Context) error {
	s.logger.Info("init subtask env")
	tableImporter, err := getTableImporter(ctx, s.taskID, s.taskMeta, s.store)
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
	s.dataKVMemSizePerCon, s.perIndexKVMemSizePerCon = getWriterMemorySizeLimit(s.GetResource(), s.tableImporter.Plan)
	s.dataBlockSize = getAdjustedBlockSize(s.dataKVMemSizePerCon, tidbconfig.MaxTxnEntrySizeLimit)
	s.indexBlockSize = getAdjustedBlockSize(s.perIndexKVMemSizePerCon, external.DefaultBlockSize)
	s.logger.Info("KV writer memory buf info",
		zap.String("data-buf-limit", units.BytesSize(float64(s.dataKVMemSizePerCon))),
		zap.String("per-index-buf-limit", units.BytesSize(float64(s.perIndexKVMemSizePerCon))),
		zap.String("data-buf-block-size", units.BytesSize(float64(s.dataBlockSize))),
		zap.String("index-buf-block-size", units.BytesSize(float64(s.indexBlockSize))))
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

	// read import step meta from external storage when using global sort.
	if subtaskMeta.ExternalPath != "" {
		if err := subtaskMeta.ReadJSONFromExternalStorage(ctx, s.tableImporter.GlobalSortStore, &subtaskMeta); err != nil {
			return errors.Trace(err)
		}
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
		Checksum:         verification.NewKVGroupChecksumWithKeyspace(s.tableImporter.GetKeySpace()),
		SortedDataMeta:   &external.SortedKVMeta{},
		SortedIndexMetas: make(map[int64]*external.SortedKVMeta),
	}
	s.sharedVars.Store(subtaskMeta.ID, sharedVars)

	source := operator.NewSimpleDataChannel(make(chan *importStepMinimalTask))
	op := newEncodeAndSortOperator(ctx, s, sharedVars, subtask.ID, int(s.GetResource().CPU.Capacity()))
	s.op.Store(op)
	defer s.op.Store(nil)
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

	if err = pipeline.Close(); err != nil {
		return err
	}
	return s.onFinished(ctx, subtask)
}

func (*importStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return nil
}

func (s *importStepExecutor) onFinished(ctx context.Context, subtask *proto.Subtask) error {
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
		subtaskMeta.Checksum[id] = *newFromKVChecksum(c)
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
	subtaskMeta.RecordedConflictKVCount = sharedVars.RecordedConflictKVCount
	// if using global sort, write the external meta to external storage.
	if s.tableImporter.IsGlobalSort() {
		subtaskMeta.ExternalPath = external.SubtaskMetaPath(s.taskID, subtask.ID)
		if err := subtaskMeta.WriteJSONToExternalStorage(ctx, s.tableImporter.GlobalSortStore, subtaskMeta); err != nil {
			return errors.Trace(err)
		}
	}

	s.sharedVars.Delete(subtaskMeta.ID)
	newMeta, err := subtaskMeta.Marshal()
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

func (*importStepExecutor) TaskMetaModified(context.Context, []byte) error {
	return nil
}

func (s *importStepExecutor) ResourceModified(_ context.Context, newResource *proto.StepResource) error {
	currOp := s.op.Load()
	if currOp == nil {
		s.logger.Info("ResourceModified called but no subtask running")
		return errors.Errorf("no subtask running")
	}

	currSize := currOp.GetWorkerPoolSize()
	target := int32(newResource.CPU.Capacity())
	if target != currSize {
		s.logger.Info("ResourceModified: tuning worker pool size",
			zap.String("step", "importStepExecutor"),
			zap.Int32("old", currSize),
			zap.Int32("new", target))
		currOp.TuneWorkerPoolSize(target, true)
	}

	s.logger.Info("ResourceModified: finished tuning worker pool size",
		zap.String("step", "importStepExecutor"),
		zap.Int32("old", currSize),
		zap.Int32("new", currOp.GetWorkerPoolSize()))

	return nil
}

type mergeSortStepExecutor struct {
	taskexecutor.BaseStepExecutor
	taskID     int64
	taskMeta   *TaskMeta
	logger     *zap.Logger
	controller *importer.LoadDataController
	mergeOp    atomic.Pointer[external.MergeOperator]
	// subtask of a task is run in serial now, so we don't need lock here.
	// change to SyncMap when we support parallel subtask in the future.
	subtaskSortedKVMeta *external.SortedKVMeta
	// part-size for uploading merged files, it's calculated by:
	// 	max(max-merged-files * max-file-size / max-part-num(10000), min-part-size)
	dataKVPartSize  int64
	indexKVPartSize int64

	indicesGenKV map[int64]genKVIndex
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
	dataKVMemSizePerCon, perIndexKVMemSizePerCon := getWriterMemorySizeLimit(m.GetResource(), &m.taskMeta.Plan)
	m.dataKVPartSize = max(external.MinUploadPartSize, int64(dataKVMemSizePerCon*uint64(external.MaxMergingFilesPerThread)/10000))
	m.indexKVPartSize = max(external.MinUploadPartSize, int64(perIndexKVMemSizePerCon*uint64(external.MaxMergingFilesPerThread)/10000))

	m.logger.Info("merge sort partSize",
		zap.String("data-kv", units.BytesSize(float64(m.dataKVPartSize))),
		zap.String("index-kv", units.BytesSize(float64(m.indexKVPartSize))),
	)
	return nil
}

func (m *mergeSortStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	sm := &MergeSortStepMeta{}
	err = json.Unmarshal(subtask.Meta, sm)
	if err != nil {
		return errors.Trace(err)
	}
	// read merge sort step meta from external storage when using global sort.
	if sm.ExternalPath != "" {
		if err := sm.ReadJSONFromExternalStorage(ctx, m.controller.GlobalSortStore, sm); err != nil {
			return errors.Trace(err)
		}
	}
	logger := m.logger.With(zap.Int64("subtask-id", subtask.ID), zap.String("kv-group", sm.KVGroup))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	var mu sync.Mutex
	m.subtaskSortedKVMeta = &external.SortedKVMeta{}
	onClose := func(summary *external.WriterSummary) {
		mu.Lock()
		defer mu.Unlock()
		m.subtaskSortedKVMeta.MergeSummary(summary)
	}

	prefix := subtaskPrefix(m.taskID, subtask.ID)

	partSize := m.dataKVPartSize
	if sm.KVGroup != dataKVGroup {
		partSize = m.indexKVPartSize
	}
	onDup, err := getOnDupForKVGroup(m.indicesGenKV, sm.KVGroup)
	if err != nil {
		return errors.Trace(err)
	}

	concurrency := int(m.GetResource().CPU.Capacity())

	opCtx, _ := operator.NewContext(ctx)

	op := external.NewMergeOperator(
		opCtx,
		m.controller.GlobalSortStore,
		partSize,
		prefix,
		external.DefaultOneWriterBlockSize,
		onClose,
		int(m.GetResource().CPU.Capacity()),
		false,
		onDup,
	)
	m.mergeOp.Store(op)
	defer m.mergeOp.Store(nil)

	if err = external.MergeOverlappingFiles(
		opCtx,
		sm.DataFiles,
		concurrency,
		op,
	); err != nil {
		return errors.Trace(err)
	}

	logger.Info(
		"merge sort finished",
		zap.Uint64("total-kv-size", m.subtaskSortedKVMeta.TotalKVSize),
		zap.Uint64("total-kv-count", m.subtaskSortedKVMeta.TotalKVCnt),
		brlogutil.Key("start-key", m.subtaskSortedKVMeta.StartKey),
		brlogutil.Key("end-key", m.subtaskSortedKVMeta.EndKey),
	)

	return m.onFinished(ctx, subtask)
}

func (m *mergeSortStepExecutor) onFinished(ctx context.Context, subtask *proto.Subtask) error {
	var subtaskMeta MergeSortStepMeta
	if err := json.Unmarshal(subtask.Meta, &subtaskMeta); err != nil {
		return errors.Trace(err)
	}
	subtaskMeta.SortedKVMeta = *m.subtaskSortedKVMeta
	subtaskMeta.RecordedConflictKVCount = subtaskMeta.SortedKVMeta.ConflictInfo.Count
	subtaskMeta.ExternalPath = external.SubtaskMetaPath(m.taskID, subtask.ID)
	if err := subtaskMeta.WriteJSONToExternalStorage(ctx, m.controller.GlobalSortStore, subtaskMeta); err != nil {
		return errors.Trace(err)
	}

	m.subtaskSortedKVMeta = nil
	newMeta, err := subtaskMeta.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	subtask.Meta = newMeta
	return nil
}

func (*mergeSortStepExecutor) TaskMetaModified(context.Context, []byte) error {
	return nil
}

func (m *mergeSortStepExecutor) ResourceModified(_ context.Context, newResource *proto.StepResource) error {
	currOp := m.mergeOp.Load()
	if currOp == nil {
		m.logger.Info("ResourceModified called but no subtask running")
		return errors.Errorf("no subtask running")
	}

	currSize := currOp.GetWorkerPoolSize()
	target := int32(newResource.CPU.Capacity())
	if target != currSize {
		m.logger.Info("ResourceModified: tuning worker pool size",
			zap.String("step", "mergeSortStepExecutor"),
			zap.Int32("old", currOp.GetWorkerPoolSize()),
			zap.Int32("new", target))
		currOp.TuneWorkerPoolSize(target, true)
	}

	m.logger.Info("ResourceModified: finished tuning worker pool size",
		zap.String("step", "mergeSortStepExecutor"),
		zap.Int32("old", currSize),
		zap.Int32("new", currOp.GetWorkerPoolSize()))

	return nil
}

func getOnDupForKVGroup(indicesGenKV map[int64]genKVIndex, kvGroup string) (common.OnDuplicateKey, error) {
	if kvGroup == dataKVGroup {
		return common.OnDuplicateKeyRecord, nil
	}

	indexID, err2 := kvGroup2IndexID(kvGroup)
	if err2 != nil {
		// shouldn't happen
		return common.OnDuplicateKeyIgnore, errors.Trace(err2)
	}
	info, ok := indicesGenKV[indexID]
	if !ok {
		// shouldn't happen
		return common.OnDuplicateKeyIgnore, errors.Errorf("unknown index %d", indexID)
	}
	if info.unique {
		return common.OnDuplicateKeyRecord, nil
	}
	return common.OnDuplicateKeyRemove, nil
}

type writeAndIngestStepExecutor struct {
	taskexecutor.BaseStepExecutor

	taskID        int64
	taskMeta      *TaskMeta
	logger        *zap.Logger
	tableImporter *importer.TableImporter
	store         tidbkv.Storage

	engine atomic.Pointer[external.Engine]

	indicesGenKV map[int64]genKVIndex
}

var _ execute.StepExecutor = &writeAndIngestStepExecutor{}

func (e *writeAndIngestStepExecutor) Init(ctx context.Context) error {
	tableImporter, err := getTableImporter(ctx, e.taskID, e.taskMeta, e.store)
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

	// read write and ingest step meta from external storage when using global sort.
	if sm.ExternalPath != "" {
		if err := sm.ReadJSONFromExternalStorage(ctx, e.tableImporter.GlobalSortStore, sm); err != nil {
			return errors.Trace(err)
		}
	}

	logger := e.logger.With(zap.Int64("subtask-id", subtask.ID),
		zap.String("kv-group", sm.KVGroup))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	_, engineUUID := backend.MakeUUID("", subtask.ID)
	localBackend := e.tableImporter.Backend()
	// compatible with old version task meta
	jobKeys := sm.RangeJobKeys
	if jobKeys == nil {
		jobKeys = sm.RangeSplitKeys
	}
	onDup, err := getOnDupForKVGroup(e.indicesGenKV, sm.KVGroup)
	if err != nil {
		return errors.Trace(err)
	}

	err = localBackend.CloseEngine(ctx, &backend.EngineConfig{
		External: &backend.ExternalEngineConfig{
			StorageURI:    e.taskMeta.Plan.CloudStorageURI,
			DataFiles:     sm.DataFiles,
			StatFiles:     sm.StatFiles,
			StartKey:      sm.StartKey,
			EndKey:        sm.EndKey,
			JobKeys:       jobKeys,
			SplitKeys:     sm.RangeSplitKeys,
			TotalFileSize: int64(sm.TotalKVSize),
			TotalKVCount:  0,
			CheckHotspot:  false,
			MemCapacity:   e.GetResource().Mem.Capacity(),
			OnDup:         onDup,
			FilePrefix:    subtaskPrefix(e.taskID, subtask.ID),
		},
		TS: sm.TS,
	}, engineUUID)
	if err != nil {
		return err
	}
	eng := localBackend.GetExternalEngine(engineUUID)
	if eng == nil {
		return errors.Errorf("external engine %s not found", engineUUID)
	}
	e.engine.Store(eng)
	defer e.engine.Store(nil)
	err = localBackend.ImportEngine(ctx, engineUUID, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	if err != nil {
		return errors.Trace(err)
	}
	return e.onFinished(ctx, subtask)
}

func (*writeAndIngestStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return nil
}

func (e *writeAndIngestStepExecutor) onFinished(ctx context.Context, subtask *proto.Subtask) error {
	var subtaskMeta WriteIngestStepMeta
	if err := json.Unmarshal(subtask.Meta, &subtaskMeta); err != nil {
		return errors.Trace(err)
	}

	// only data kv group has loaded row count
	_, engineUUID := backend.MakeUUID("", subtask.ID)
	localBackend := e.tableImporter.Backend()
	if subtaskMeta.KVGroup == dataKVGroup {
		// only set row count for data kv group
		_, kvCount := localBackend.GetExternalEngineKVStatistics(engineUUID)
		subtaskMeta.Result.LoadedRowCnt = uint64(kvCount)
	}
	subtaskMeta.ConflictInfo = localBackend.GetExternalEngineConflictInfo(engineUUID)
	subtaskMeta.RecordedConflictKVCount = subtaskMeta.ConflictInfo.Count
	err := localBackend.CleanupEngine(ctx, engineUUID)
	if err != nil {
		e.logger.Warn("failed to cleanup engine", zap.Error(err))
	}
	subtaskMeta.ExternalPath = external.SubtaskMetaPath(e.taskID, subtask.ID)
	if err := subtaskMeta.WriteJSONToExternalStorage(ctx, e.tableImporter.GlobalSortStore, subtaskMeta); err != nil {
		return errors.Trace(err)
	}
	newMeta, err := subtaskMeta.Marshal()
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

func (e *writeAndIngestStepExecutor) TaskMetaModified(_ context.Context, newMeta []byte) error {
	newTaskMeta := &TaskMeta{}
	if err := json.Unmarshal(newMeta, newTaskMeta); err != nil {
		return errors.Trace(err)
	}
	if newTaskMeta.Plan.MaxWriteSpeed != e.taskMeta.Plan.MaxWriteSpeed {
		oldSpeed := e.taskMeta.Plan.MaxWriteSpeed
		newSpeed := newTaskMeta.Plan.MaxWriteSpeed
		e.logger.Info("TaskMetaModified: updating max_write_speed",
			zap.String("step", "writeAndIngestStepExecutor"),
			zap.Int64("old", int64(oldSpeed)),
			zap.Int64("new", int64(newSpeed)),
		)
		e.tableImporter.Backend().UpdateWriteSpeedLimit(int(newSpeed))
		e.taskMeta.Plan.MaxWriteSpeed = newSpeed
	}
	return nil
}

func (e *writeAndIngestStepExecutor) ResourceModified(ctx context.Context, newResource *proto.StepResource) error {
	eng := e.engine.Load()
	if eng == nil {
		e.logger.Info("ResourceModified called but no subtask running")
		return errors.Errorf("no subtask running")
	}

	newConcurrency := int(newResource.CPU.Capacity())
	newMem := newResource.Mem.Capacity()
	if err := eng.UpdateResource(ctx, newConcurrency, newMem); err != nil {
		return err
	}
	e.logger.Info("ResourceModified: updated engine resource",
		zap.String("step", "writeAndIngestStepExecutor"),
		zap.Stringer("newResource", newResource),
	)
	e.tableImporter.Backend().SetConcurrency(newConcurrency)
	return nil
}

type postProcessStepExecutor struct {
	taskexecutor.BaseStepExecutor
	taskID   int64
	store    tidbkv.Storage
	taskMeta *TaskMeta
	logger   *zap.Logger
}

var _ execute.StepExecutor = &postProcessStepExecutor{}

// NewPostProcessStepExecutor creates a new post process step executor.
// exported for testing.
func NewPostProcessStepExecutor(taskID int64, store tidbkv.Storage, taskMeta *TaskMeta, logger *zap.Logger) execute.StepExecutor {
	return &postProcessStepExecutor{
		taskID:   taskID,
		store:    store,
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
	return postProcess(ctx, p.store, p.taskMeta, &stepMeta, logger)
}

func (*postProcessStepExecutor) TaskMetaModified(context.Context, []byte) error {
	return nil
}

func (*postProcessStepExecutor) ResourceModified(context.Context, *proto.StepResource) error {
	return nil
}

type importExecutor struct {
	*taskexecutor.BaseTaskExecutor
	store        tidbkv.Storage
	indicesGenKV map[int64]genKVIndex
}

// NewImportExecutor creates a new import task executor.
func NewImportExecutor(
	ctx context.Context,
	task *proto.Task,
	param taskexecutor.Param,
	store tidbkv.Storage,
) taskexecutor.TaskExecutor {
	metrics := metricsManager.getOrCreateMetrics(task.ID)
	subCtx := metric.WithCommonMetric(ctx, metrics)
	s := &importExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(subCtx, task, param),
		store:            store,
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

func (e *importExecutor) GetStepExecutor(task *proto.Task) (execute.StepExecutor, error) {
	taskMeta := TaskMeta{}
	if err := json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return nil, errors.Trace(err)
	}
	logger := logutil.BgLogger().With(
		zap.Stringer("type", proto.ImportInto),
		zap.Int64("task-id", task.ID),
		zap.String("step", proto.Step2Str(task.Type, task.Step)),
	)
	indicesGenKV := getIndicesGenKV(taskMeta.Plan.TableInfo)
	logger.Info("got indices that generate kv", zap.Any("indices", indicesGenKV))

	switch task.Step {
	case proto.ImportStepImport, proto.ImportStepEncodeAndSort:
		return &importStepExecutor{
			taskID:       task.ID,
			taskMeta:     &taskMeta,
			logger:       logger,
			store:        e.store,
			indicesGenKV: indicesGenKV,
		}, nil
	case proto.ImportStepMergeSort:
		return &mergeSortStepExecutor{
			taskID:       task.ID,
			taskMeta:     &taskMeta,
			logger:       logger,
			indicesGenKV: indicesGenKV,
		}, nil
	case proto.ImportStepWriteAndIngest:
		return &writeAndIngestStepExecutor{
			taskID:       task.ID,
			taskMeta:     &taskMeta,
			logger:       logger,
			store:        e.store,
			indicesGenKV: indicesGenKV,
		}, nil
	case proto.ImportStepCollectConflicts:
		return &collectConflictsStepExecutor{
			taskID:   task.ID,
			taskMeta: &taskMeta,
			logger:   logger,
			store:    e.store,
		}, nil
	case proto.ImportStepConflictResolution:
		return &conflictResolutionStepExecutor{
			taskID:   task.ID,
			taskMeta: &taskMeta,
			logger:   logger,
			store:    e.store,
		}, nil
	case proto.ImportStepPostProcess:
		return NewPostProcessStepExecutor(task.ID, e.store, &taskMeta, logger), nil
	default:
		return nil, errors.Errorf("unknown step %d for import task %d", task.Step, task.ID)
	}
}

func (e *importExecutor) Close() {
	task := e.GetTaskBase()
	metricsManager.unregister(task.ID)
	e.BaseTaskExecutor.Close()
}
