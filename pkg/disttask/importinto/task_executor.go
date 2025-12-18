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
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbconfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/disttask/framework/metering"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	dxfstorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// importStepExecutor is a executor for import step.
// StepExecutor is equivalent to a Lightning instance.
type importStepExecutor struct {
	taskexecutor.BaseStepExecutor
	execute.NoopCollector

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

	summary execute.SubtaskSummary
	memPool *mydump.Pool
}

func getTableImporter(
	ctx context.Context,
	taskID int64,
	taskMeta *TaskMeta,
	store tidbkv.Storage,
	logger *zap.Logger,
) (*importer.TableImporter, error) {
	idAlloc := kv.NewPanickingAllocators(taskMeta.Plan.TableInfo.SepAutoInc())
	tbl, err := tables.TableFromMeta(idAlloc, taskMeta.Plan.TableInfo)
	if err != nil {
		return nil, err
	}
	astArgs, err := importer.ASTArgsFromStmt(taskMeta.Stmt)
	if err != nil {
		return nil, err
	}
	controller, err := importer.NewLoadDataController(&taskMeta.Plan, tbl, astArgs, importer.WithLogger(logger))
	if err != nil {
		return nil, err
	}
	if err = controller.InitDataStore(ctx); err != nil {
		return nil, err
	}

	return importer.NewTableImporter(ctx, controller, strconv.FormatInt(taskID, 10), store)
}

func (s *importStepExecutor) Init(ctx context.Context) (err error) {
	s.logger.Info("init subtask env")
	var tableImporter *importer.TableImporter
	var taskManager *dxfstorage.TaskManager
	tableImporter, err = getTableImporter(ctx, s.taskID, s.taskMeta, s.store, s.logger)
	if err != nil {
		return err
	}
	s.tableImporter = tableImporter
	defer func() {
		if err == nil {
			return
		}
		if err2 := s.tableImporter.Close(); err2 != nil {
			s.logger.Warn("close importer failed", zap.Error(err2))
		}
	}()

	if kerneltype.IsClassic() {
		taskManager, err = dxfstorage.GetTaskManager()
		if err != nil {
			return err
		}
		if err = taskManager.WithNewTxn(ctx, func(se sessionctx.Context) error {
			// User can write table between precheck and alter table mode to Import,
			// so check table emptyness again.
			isEmpty, err2 := ddl.CheckImportIntoTableIsEmpty(s.store, se, s.tableImporter.Table)
			if err2 != nil {
				return err2
			}
			if !isEmpty {
				return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("target table is not empty")
			}
			return nil
		}); err != nil {
			return err
		}
	}
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

	if s.memPool == nil {
		s.memPool = mydump.GetPool(int(s.GetResource().Mem.Capacity()))
	}

	s.dataKVMemSizePerCon, s.perIndexKVMemSizePerCon = getWriterMemorySizeLimit(s.GetResource(), s.tableImporter.Plan)
	s.dataBlockSize = external.GetAdjustedBlockSize(s.dataKVMemSizePerCon, tidbconfig.MaxTxnEntrySizeLimit)
	s.indexBlockSize = external.GetAdjustedBlockSize(s.perIndexKVMemSizePerCon, external.DefaultBlockSize)
	s.logger.Info("KV writer memory buf info",
		zap.String("data-buf-limit", units.BytesSize(float64(s.dataKVMemSizePerCon))),
		zap.String("per-index-buf-limit", units.BytesSize(float64(s.perIndexKVMemSizePerCon))),
		zap.String("data-buf-block-size", units.BytesSize(float64(s.dataBlockSize))),
		zap.String("index-buf-block-size", units.BytesSize(float64(s.indexBlockSize))))
	return nil
}

// Accepted implements Collector.Accepted interface.
func (s *importStepExecutor) Accepted(bytes int64) {
	s.summary.Bytes.Add(bytes)
	metering.NewRecorder(s.store, metering.TaskTypeImportInto, s.taskID).
		RecordReadDataBytes(uint64(bytes))
}

// Processed implements Collector.Processed interface.
func (s *importStepExecutor) Processed(bytes, rowCnt int64) {
	s.summary.RowCnt.Add(rowCnt)
	metering.NewRecorder(s.store, metering.TaskTypeImportInto, s.taskID).
		RecordWriteDataBytes(uint64(bytes))
}

func (s *importStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := s.logger.With(zap.Int64("subtask-id", subtask.ID))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	s.summary.Reset()

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
		Checksum:         verification.NewKVGroupChecksumWithKeyspace(s.tableImporter.GetKeySpace()),
		SortedDataMeta:   &external.SortedKVMeta{},
		SortedIndexMetas: make(map[int64]*external.SortedKVMeta),
		summary:          &s.summary,
		pool:             s.memPool,
	}
	s.sharedVars.Store(subtaskMeta.ID, sharedVars)

	source := operator.NewSimpleDataChannel(make(chan *importStepMinimalTask))
	op := newEncodeAndSortOperator(ctx, s, sharedVars, s, subtask.ID, int(s.GetResource().CPU.Capacity()))
	op.SetSource(source)
	pipeline := operator.NewAsyncPipeline(op)
	if err = pipeline.Execute(); err != nil {
		return err
	}

	panicked := atomic.Bool{}
outer:
	for _, chunk := range subtaskMeta.Chunks {
		// TODO: current workpool impl doesn't drain the input channel, it will
		// just return on context cancel(error happened), so we add this select.
		select {
		case source.Channel() <- &importStepMinimalTask{
			Plan:       s.taskMeta.Plan,
			Chunk:      chunk,
			SharedVars: sharedVars,
			panicked:   &panicked,
			logger:     logger,
		}:
		case <-op.Done():
			break outer
		}
	}
	source.Finish()

	if err = pipeline.Close(); err != nil {
		return err
	}
	if panicked.Load() {
		return errors.Errorf("panic occurred during import, please check log")
	}
	return s.onFinished(ctx, subtask)
}

func (s *importStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	s.summary.Update()
	return &s.summary
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

	if s.tableImporter.IsLocalSort() {
		// TODO: we should close and cleanup engine in all case, since there's no checkpoint.
		s.logger.Info("import data engine", zap.Int32("engine-id", subtaskMeta.ID))
		closedDataEngine, err := sharedVars.DataEngine.Close(ctx)
		if err != nil {
			return err
		}
		if _, err := s.tableImporter.ImportAndCleanup(ctx, closedDataEngine); err != nil {
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
	subtaskMeta.Checksum = map[int64]Checksum{}
	for id, c := range sharedVars.Checksum.GetInnerChecksums() {
		subtaskMeta.Checksum[id] = Checksum{
			Sum:  c.Sum(),
			KVs:  c.SumKVS(),
			Size: c.SumSize(),
		}
	}
	allocators := sharedVars.TableImporter.Allocators()
	subtaskMeta.MaxIDs = map[autoid.AllocatorType]int64{
		autoid.RowIDAllocType:    allocators.Get(autoid.RowIDAllocType).Base(),
		autoid.AutoIncrementType: allocators.Get(autoid.AutoIncrementType).Base(),
		autoid.AutoRandomType:    allocators.Get(autoid.AutoRandomType).Base(),
	}
	subtaskMeta.SortedDataMeta = sharedVars.SortedDataMeta
	subtaskMeta.SortedIndexMetas = sharedVars.SortedIndexMetas
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

type mergeSortStepExecutor struct {
	taskexecutor.BaseStepExecutor
	taskID    int64
	taskMeta  *TaskMeta
	logger    *zap.Logger
	sortStore storage.ExternalStorage
	// subtask of a task is run in serial now, so we don't need lock here.
	// change to SyncMap when we support parallel subtask in the future.
	subtaskSortedKVMeta *external.SortedKVMeta
	// part-size for uploading merged files, it's calculated by:
	// 	max(max-merged-files * max-file-size / max-part-num(10000), min-part-size)
	dataKVPartSize  int64
	indexKVPartSize int64
	store           tidbkv.Storage

	summary execute.SubtaskSummary
}

var _ execute.StepExecutor = &mergeSortStepExecutor{}

func (m *mergeSortStepExecutor) Init(ctx context.Context) error {
	store, err := importer.GetSortStore(ctx, m.taskMeta.Plan.CloudStorageURI)
	if err != nil {
		return err
	}
	m.sortStore = store
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

	m.summary.Reset()

	// read merge sort step meta from external storage when using global sort.
	if sm.ExternalPath != "" {
		if err := sm.ReadJSONFromExternalStorage(ctx, m.sortStore, sm); err != nil {
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
	onWriterClose := func(summary *external.WriterSummary) {
		mu.Lock()
		defer mu.Unlock()
		m.subtaskSortedKVMeta.MergeSummary(summary)
		metering.NewRecorder(m.store, metering.TaskTypeImportInto, subtask.TaskID).
			RecordPutRequestCount(summary.PutRequestCount)
	}
	onReaderClose := func(summary *external.ReaderSummary) {
		m.summary.GetReqCnt.Add(summary.GetRequestCount)
		metering.NewRecorder(m.store, metering.TaskTypeImportInto, subtask.TaskID).
			RecordGetRequestCount(summary.GetRequestCount)
	}

	prefix := subtaskPrefix(m.taskID, subtask.ID)

	partSize := m.dataKVPartSize
	if sm.KVGroup != dataKVGroup {
		partSize = m.indexKVPartSize
	}
	err = external.MergeOverlappingFiles(
		logutil.WithFields(ctx, zap.String("kv-group", sm.KVGroup), zap.Int64("subtask-id", subtask.ID)),
		sm.DataFiles,
		m.sortStore,
		partSize,
		prefix,
		external.DefaultOneWriterBlockSize,
		onWriterClose,
		onReaderClose,
		external.NewMergeCollector(ctx, &m.summary),
		int(m.GetResource().CPU.Capacity()),
		false,
		engineapi.OnDuplicateKeyIgnore)
	logger.Info(
		"merge sort finished",
		zap.Uint64("total-kv-size", m.subtaskSortedKVMeta.TotalKVSize),
		zap.Uint64("total-kv-count", m.subtaskSortedKVMeta.TotalKVCnt),
		brlogutil.Key("start-key", m.subtaskSortedKVMeta.StartKey),
		brlogutil.Key("end-key", m.subtaskSortedKVMeta.EndKey),
	)
	if err != nil {
		return errors.Trace(err)
	}
	return m.onFinished(ctx, subtask)
}

func (m *mergeSortStepExecutor) onFinished(ctx context.Context, subtask *proto.Subtask) error {
	var subtaskMeta MergeSortStepMeta
	if err := json.Unmarshal(subtask.Meta, &subtaskMeta); err != nil {
		return errors.Trace(err)
	}
	subtaskMeta.SortedKVMeta = *m.subtaskSortedKVMeta
	subtaskMeta.ExternalPath = external.SubtaskMetaPath(m.taskID, subtask.ID)
	if err := subtaskMeta.WriteJSONToExternalStorage(ctx, m.sortStore, subtaskMeta); err != nil {
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

// Cleanup implements the StepExecutor.Cleanup interface.
func (m *mergeSortStepExecutor) Cleanup(ctx context.Context) (err error) {
	m.logger.Info("cleanup subtask env")
	if m.sortStore != nil {
		m.sortStore.Close()
	}
	return m.BaseStepExecutor.Cleanup(ctx)
}

func (m *mergeSortStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	m.summary.Update()
	return &m.summary
}

type ingestCollector struct {
	execute.NoopCollector
	summary *execute.SubtaskSummary
	kvGroup string
}

func (c *ingestCollector) Processed(bytes, rowCnt int64) {
	c.summary.Bytes.Add(bytes)
	if c.kvGroup == dataKVGroup {
		c.summary.RowCnt.Add(rowCnt)
	}
}

type writeAndIngestStepExecutor struct {
	taskexecutor.BaseStepExecutor

	taskID        int64
	taskMeta      *TaskMeta
	logger        *zap.Logger
	tableImporter *importer.TableImporter
	store         tidbkv.Storage

	summary execute.SubtaskSummary
}

var _ execute.StepExecutor = &writeAndIngestStepExecutor{}

func (e *writeAndIngestStepExecutor) Init(ctx context.Context) error {
	tableImporter, err := getTableImporter(ctx, e.taskID, e.taskMeta, e.store, e.logger)
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

	e.summary.Reset()

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

	collector := &ingestCollector{
		summary: &e.summary,
		kvGroup: sm.KVGroup,
	}
	localBackend.SetCollector(collector)

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
			OnReaderClose: func(summary *external.ReaderSummary) {
				e.summary.GetReqCnt.Add(summary.GetRequestCount)
				metering.NewRecorder(e.store, metering.TaskTypeImportInto, subtask.TaskID).
					RecordGetRequestCount(summary.GetRequestCount)
			},
		},
		TS: sm.TS,
	}, engineUUID)
	if err != nil {
		return err
	}
	err = localBackend.ImportEngine(ctx, engineUUID, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	if err != nil {
		return errors.Trace(err)
	}
	return e.onFinished(ctx, subtask)
}

func (e *writeAndIngestStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	e.summary.Update()
	return &e.summary
}

func (e *writeAndIngestStepExecutor) onFinished(ctx context.Context, subtask *proto.Subtask) error {
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
	err := localBackend.CleanupEngine(ctx, engineUUID)
	if err != nil {
		e.logger.Warn("failed to cleanup engine", zap.Error(err))
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

type postProcessStepExecutor struct {
	taskexecutor.BaseStepExecutor
	taskID       int64
	store        tidbkv.Storage
	taskTbl      taskexecutor.TaskTable
	taskMeta     *TaskMeta
	taskKeyspace string
	logger       *zap.Logger
}

var _ execute.StepExecutor = &postProcessStepExecutor{}

// NewPostProcessStepExecutor creates a new post process step executor.
// exported for testing.
func NewPostProcessStepExecutor(
	taskID int64,
	store tidbkv.Storage,
	taskTbl taskexecutor.TaskTable,
	taskMeta *TaskMeta,
	taskKeyspace string,
	logger *zap.Logger,
) execute.StepExecutor {
	return &postProcessStepExecutor{
		taskID:       taskID,
		store:        store,
		taskTbl:      taskTbl,
		taskMeta:     taskMeta,
		taskKeyspace: taskKeyspace,
		logger:       logger,
	}
}

func (p *postProcessStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := p.logger.With(zap.Int64("subtask-id", subtask.ID))
	logTask := log.BeginTask(logger, "run subtask")
	defer func() {
		logTask.End(zapcore.ErrorLevel, err)
	}()
	stepMeta := PostProcessStepMeta{}
	if err = json.Unmarshal(subtask.Meta, &stepMeta); err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("waitBeforePostProcess", func() {
		time.Sleep(5 * time.Second)
	})
	return p.postProcess(ctx, &stepMeta, logger)
}

type importExecutor struct {
	*taskexecutor.BaseTaskExecutor
	store tidbkv.Storage
}

// NewImportExecutor creates a new import task executor.
func NewImportExecutor(
	ctx context.Context,
	task *proto.Task,
	param taskexecutor.Param,
) taskexecutor.TaskExecutor {
	metrics := metricsManager.getOrCreateMetrics(task.ID)
	subCtx := metric.WithCommonMetric(ctx, metrics)

	s := &importExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(subCtx, task, param),
		store:            param.Store,
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
		zap.Int64("task-id", task.ID),
		zap.String("task-key", task.Key),
		zap.String("step", proto.Step2Str(task.Type, task.Step)),
	)
	store := e.store
	if e.store.GetKeyspace() != task.Keyspace {
		var err error
		err = e.GetTaskTable().WithNewSession(func(se sessionctx.Context) error {
			store, err = se.GetSQLServer().GetKSStore(task.Keyspace)
			return err
		})
		if err != nil {
			return nil, err
		}
	}

	switch task.Step {
	case proto.ImportStepImport, proto.ImportStepEncodeAndSort:
		return &importStepExecutor{
			taskID:   task.ID,
			taskMeta: &taskMeta,
			logger:   logger,
			store:    store,
		}, nil
	case proto.ImportStepMergeSort:
		return &mergeSortStepExecutor{
			taskID:   task.ID,
			taskMeta: &taskMeta,
			logger:   logger,
			store:    store,
		}, nil
	case proto.ImportStepWriteAndIngest:
		return &writeAndIngestStepExecutor{
			taskID:   task.ID,
			taskMeta: &taskMeta,
			logger:   logger,
			store:    store,
		}, nil
	case proto.ImportStepPostProcess:
		return NewPostProcessStepExecutor(task.ID, store, e.GetTaskTable(), &taskMeta, task.Keyspace, logger), nil
	default:
		return nil, errors.Errorf("unknown step %d for import task %d", task.Step, task.ID)
	}
}

func (e *importExecutor) Close() {
	task := e.GetTaskBase()
	metricsManager.unregister(task.ID)
	e.BaseTaskExecutor.Close()
}
