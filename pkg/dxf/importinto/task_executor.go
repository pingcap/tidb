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
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/metering"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	dxfstorage "github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/operator"
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
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
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
	indicesGenKV map[int64]importer.GenKVIndex

	summary execute.SubtaskSummary
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

	failpoint.Inject("createTableImporterForTest", func() {
		failpoint.Return(importer.NewTableImporterForTest(ctx, controller, strconv.FormatInt(taskID, 10), store))
	})
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
	// when creating the table importer, it's using plan.ThreadCnt, not the resource
	// CPU, we should refactor it later, that requires refactors the local backend.
	tableImporter.Backend().SetWorkerConcurrency(int(s.GetResource().CPU.Capacity()))
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
}

// Processed implements Collector.Processed interface.
func (s *importStepExecutor) Processed(_, rowCnt int64) {
	s.summary.RowCnt.Add(rowCnt)
}

func (s *importStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := s.logger.With(zap.Int64("subtask-id", subtask.ID))
	task := log.BeginTask(logger, "run subtask")
	var (
		dataKVFiles, indexKVFiles atomic.Int64
		accessRec                 = &recording.AccessStats{}
		objStore                  objstore.ExternalStorage
	)
	defer func() {
		task.End(zapcore.ErrorLevel, err, zap.Int64("data-kv-files", dataKVFiles.Load()),
			zap.Int64("index-kv-files", indexKVFiles.Load()),
			zap.Stringer("obj-store-access", accessRec))
	}()

	bs := subtask.Meta
	var subtaskMeta ImportStepMeta
	err = json.Unmarshal(bs, &subtaskMeta)
	if err != nil {
		return errors.Trace(err)
	}

	if s.tableImporter.IsGlobalSort() {
		var err3 error
		accessRec, objStore, err3 = handle.NewObjStoreWithRecording(ctx, s.tableImporter.CloudStorageURI)
		if err3 != nil {
			return err3
		}
		defer func() {
			objStore.Close()
			s.summary.MergeObjStoreRequests(&accessRec.Requests)
			s.GetMeterRecorder().MergeObjStoreAccess(accessRec)
		}()
	}
	// read import step meta from external storage when using global sort.
	if subtaskMeta.ExternalPath != "" {
		if err := subtaskMeta.ReadJSONFromExternalStorage(ctx, objStore, &subtaskMeta); err != nil {
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
		globalSortStore:  objStore,
		dataKVFileCount:  &dataKVFiles,
		indexKVFileCount: &indexKVFiles,
	}
	s.sharedVars.Store(subtaskMeta.ID, sharedVars)

	wctx := workerpool.NewContext(ctx)
	tasks := make([]*importStepMinimalTask, 0, len(subtaskMeta.Chunks))
	for _, chunk := range subtaskMeta.Chunks {
		tasks = append(tasks, &importStepMinimalTask{
			Plan:       s.taskMeta.Plan,
			Chunk:      chunk,
			SharedVars: sharedVars,
			logger:     logger,
		})
	}

	sourceOp := operator.NewSimpleDataSource(wctx, tasks)
	op := newEncodeAndSortOperator(wctx, s, sharedVars, s, subtask.ID, int(s.GetResource().CPU.Capacity()))
	operator.Compose(sourceOp, op)

	pipe := operator.NewAsyncPipeline(sourceOp, op)
	if err := pipe.Execute(); err != nil {
		return err
	}

	err = pipe.Close()
	if opErr := wctx.OperatorErr(); opErr != nil {
		return opErr
	}
	if err != nil {
		return err
	}

	return s.onFinished(ctx, subtask, objStore)
}

func (s *importStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	s.summary.Update()
	return &s.summary
}

func (s *importStepExecutor) ResetSummary() {
	s.summary.Reset()
}

func (s *importStepExecutor) onFinished(ctx context.Context, subtask *proto.Subtask, extStore objstore.ExternalStorage) error {
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
		subtaskMeta.Checksum[id] = *newFromKVChecksum(c)
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
		if err := subtaskMeta.WriteJSONToExternalStorage(ctx, extStore, subtaskMeta); err != nil {
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
	task     *proto.TaskBase
	taskMeta *TaskMeta
	logger   *zap.Logger
	// subtask of a task is run in serial now, so we don't need lock here.
	// change to SyncMap when we support parallel subtask in the future.
	subtaskSortedKVMeta *external.SortedKVMeta
	// part-size for uploading merged files, it's calculated by:
	// 	max(max-merged-files * max-file-size / max-part-num(10000), min-part-size)
	dataKVPartSize  int64
	indexKVPartSize int64
	store           tidbkv.Storage
	indicesGenKV    map[int64]importer.GenKVIndex

	summary execute.SubtaskSummary
}

var _ execute.StepExecutor = &mergeSortStepExecutor{}

func (m *mergeSortStepExecutor) Init(context.Context) error {
	dataKVMemSizePerCon, perIndexKVMemSizePerCon := getWriterMemorySizeLimit(m.GetResource(), &m.taskMeta.Plan)
	m.dataKVPartSize = max(external.MinUploadPartSize, int64(dataKVMemSizePerCon*uint64(external.MaxMergingFilesPerThread)/external.MaxUploadPartCount))
	m.indexKVPartSize = max(external.MinUploadPartSize, int64(perIndexKVMemSizePerCon*uint64(external.MaxMergingFilesPerThread)/external.MaxUploadPartCount))

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

	accessRec, objStore, err := handle.NewObjStoreWithRecording(ctx, m.taskMeta.Plan.CloudStorageURI)
	if err != nil {
		return err
	}
	defer func() {
		objStore.Close()
		m.summary.MergeObjStoreRequests(&accessRec.Requests)
		m.GetMeterRecorder().MergeObjStoreAccess(accessRec)
	}()
	// read merge sort step meta from external storage when using global sort.
	if sm.ExternalPath != "" {
		if err := sm.ReadJSONFromExternalStorage(ctx, objStore, sm); err != nil {
			return errors.Trace(err)
		}
	}
	logger := m.logger.With(zap.Int64("subtask-id", subtask.ID), zap.String("kv-group", sm.KVGroup))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err, zap.Stringer("obj-store-access", accessRec))
	}()

	var mu sync.Mutex
	m.subtaskSortedKVMeta = &external.SortedKVMeta{}
	onWriterClose := func(summary *external.WriterSummary) {
		mu.Lock()
		defer mu.Unlock()
		m.subtaskSortedKVMeta.MergeSummary(summary)
	}

	prefix := subtaskPrefix(m.task.ID, subtask.ID)

	partSize := m.dataKVPartSize
	if sm.KVGroup != external.DataKVGroup {
		partSize = m.indexKVPartSize
	}
	onDup, err := getOnDupForKVGroup(m.indicesGenKV, sm.KVGroup)
	if err != nil {
		return errors.Trace(err)
	}

	wctx := workerpool.NewContext(ctx)
	op := external.NewMergeOperator(
		wctx,
		objStore,
		partSize,
		prefix,
		external.DefaultOneWriterBlockSize,
		onWriterClose,
		external.NewMergeCollector(ctx, &m.summary),
		int(m.GetResource().CPU.Capacity()),
		false,
		onDup,
	)

	if err = external.MergeOverlappingFiles(
		wctx,
		sm.DataFiles,
		int(m.GetResource().CPU.Capacity()), // the concurrency used to split subtask
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

	return m.onFinished(ctx, subtask, objStore)
}

func (m *mergeSortStepExecutor) onFinished(ctx context.Context, subtask *proto.Subtask, sortStore objstore.ExternalStorage) error {
	var subtaskMeta MergeSortStepMeta
	if err := json.Unmarshal(subtask.Meta, &subtaskMeta); err != nil {
		return errors.Trace(err)
	}
	subtaskMeta.SortedKVMeta = *m.subtaskSortedKVMeta
	subtaskMeta.RecordedConflictKVCount = subtaskMeta.SortedKVMeta.ConflictInfo.Count
	subtaskMeta.ExternalPath = external.SubtaskMetaPath(m.task.ID, subtask.ID)
	if err := subtaskMeta.WriteJSONToExternalStorage(ctx, sortStore, subtaskMeta); err != nil {
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
	return m.BaseStepExecutor.Cleanup(ctx)
}

func (m *mergeSortStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	m.summary.Update()
	return &m.summary
}

func (m *mergeSortStepExecutor) ResetSummary() {
	m.summary.Reset()
}

func getOnDupForKVGroup(indicesGenKV map[int64]importer.GenKVIndex, kvGroup string) (engineapi.OnDuplicateKey, error) {
	if kerneltype.IsNextGen() {
		// will adapt for next-gen later
		return engineapi.OnDuplicateKeyIgnore, nil
	}
	if kvGroup == external.DataKVGroup {
		return engineapi.OnDuplicateKeyRecord, nil
	}

	indexID, err2 := external.KVGroup2IndexID(kvGroup)
	if err2 != nil {
		// shouldn't happen
		return engineapi.OnDuplicateKeyIgnore, errors.Trace(err2)
	}
	info, ok := indicesGenKV[indexID]
	if !ok {
		// shouldn't happen
		return engineapi.OnDuplicateKeyIgnore, errors.Errorf("unknown index %d", indexID)
	}
	if info.Unique {
		return engineapi.OnDuplicateKeyRecord, nil
	}
	return engineapi.OnDuplicateKeyRemove, nil
}

type ingestCollector struct {
	execute.NoopCollector
	summary  *execute.SubtaskSummary
	kvGroup  string
	meterRec *metering.Recorder
}

func (c *ingestCollector) Processed(bytes, rowCnt int64) {
	c.summary.Bytes.Add(bytes)
	if c.kvGroup == external.DataKVGroup {
		c.summary.RowCnt.Add(rowCnt)
	}
	// since the region job might be retried, this value might be larger than
	// the total KV size.
	c.meterRec.IncClusterWriteBytes(uint64(bytes))
}

type writeAndIngestStepExecutor struct {
	taskexecutor.BaseStepExecutor

	taskID        int64
	taskMeta      *TaskMeta
	logger        *zap.Logger
	tableImporter *importer.TableImporter
	store         tidbkv.Storage
	indicesGenKV  map[int64]importer.GenKVIndex

	summary execute.SubtaskSummary
}

var _ execute.StepExecutor = &writeAndIngestStepExecutor{}

func (e *writeAndIngestStepExecutor) Init(ctx context.Context) error {
	tableImporter, err := getTableImporter(ctx, e.taskID, e.taskMeta, e.store, e.logger)
	if err != nil {
		return err
	}
	tableImporter.Backend().SetWorkerConcurrency(int(e.GetResource().CPU.Capacity()))
	e.tableImporter = tableImporter
	return nil
}

func (e *writeAndIngestStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	sm := &WriteIngestStepMeta{}
	err = json.Unmarshal(subtask.Meta, sm)
	if err != nil {
		return errors.Trace(err)
	}

	accessRec, objStore, err := handle.NewObjStoreWithRecording(ctx, e.tableImporter.CloudStorageURI)
	if err != nil {
		return err
	}
	meterRec := e.GetMeterRecorder()
	defer func() {
		objStore.Close()
		e.summary.MergeObjStoreRequests(&accessRec.Requests)
		meterRec.MergeObjStoreAccess(accessRec)
	}()
	// read write and ingest step meta from external storage when using global sort.
	if sm.ExternalPath != "" {
		if err := sm.ReadJSONFromExternalStorage(ctx, objStore, sm); err != nil {
			return errors.Trace(err)
		}
	}

	logger := e.logger.With(zap.Int64("subtask-id", subtask.ID),
		zap.String("kv-group", sm.KVGroup))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err, zap.Stringer("obj-store-access", accessRec))
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

	collector := &ingestCollector{
		summary:  &e.summary,
		kvGroup:  sm.KVGroup,
		meterRec: meterRec,
	}
	localBackend.SetCollector(collector)

	err = localBackend.CloseEngine(ctx, &backend.EngineConfig{
		External: &backend.ExternalEngineConfig{
			ExtStore:      objStore,
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
	err = localBackend.ImportEngine(ctx, engineUUID, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	if err != nil {
		return errors.Trace(err)
	}
	return e.onFinished(ctx, subtask, objStore)
}

func (e *writeAndIngestStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	e.summary.Update()
	return &e.summary
}

func (e *writeAndIngestStepExecutor) ResetSummary() {
	e.summary.Reset()
}

func (e *writeAndIngestStepExecutor) onFinished(ctx context.Context, subtask *proto.Subtask, objStore objstore.ExternalStorage) error {
	var subtaskMeta WriteIngestStepMeta
	if err := json.Unmarshal(subtask.Meta, &subtaskMeta); err != nil {
		return errors.Trace(err)
	}

	// only data kv group has loaded row count
	_, engineUUID := backend.MakeUUID("", subtask.ID)
	localBackend := e.tableImporter.Backend()
	subtaskMeta.ConflictInfo = localBackend.GetExternalEngineConflictInfo(engineUUID)
	subtaskMeta.RecordedConflictKVCount = subtaskMeta.ConflictInfo.Count
	err := localBackend.CleanupEngine(ctx, engineUUID)
	if err != nil {
		e.logger.Warn("failed to cleanup engine", zap.Error(err))
	}
	subtaskMeta.ExternalPath = external.SubtaskMetaPath(e.taskID, subtask.ID)
	if err := subtaskMeta.WriteJSONToExternalStorage(ctx, objStore, subtaskMeta); err != nil {
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
	store        tidbkv.Storage
	indicesGenKV map[int64]importer.GenKVIndex
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
	indicesGenKV := importer.GetIndicesGenKV(taskMeta.Plan.TableInfo)
	logger.Info("got indices that generate kv", zap.Any("indices", indicesGenKV))

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
			taskID:       task.ID,
			taskMeta:     &taskMeta,
			logger:       logger,
			store:        store,
			indicesGenKV: indicesGenKV,
		}, nil
	case proto.ImportStepMergeSort:
		return &mergeSortStepExecutor{
			task:         &task.TaskBase,
			taskMeta:     &taskMeta,
			logger:       logger,
			store:        store,
			indicesGenKV: indicesGenKV,
		}, nil
	case proto.ImportStepWriteAndIngest:
		return &writeAndIngestStepExecutor{
			taskID:       task.ID,
			taskMeta:     &taskMeta,
			logger:       logger,
			store:        store,
			indicesGenKV: indicesGenKV,
		}, nil
	case proto.ImportStepCollectConflicts:
		return NewCollectConflictsStepExecutor(&task.TaskBase, store, &taskMeta, logger), nil
	case proto.ImportStepConflictResolution:
		return NewConflictResolutionStepExecutor(&task.TaskBase, store, &taskMeta, logger), nil
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
