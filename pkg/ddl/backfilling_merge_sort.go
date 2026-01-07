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
	goerrors "errors"
	"path"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
)

type mergeSortExecutor struct {
	taskexecutor.BaseStepExecutor
	task          *proto.TaskBase
	store         kv.Storage
	jobID         int64
	indexes       []*model.IndexInfo
	ptbl          table.PhysicalTable
	cloudStoreURI string

	mergeOp atomic.Pointer[external.MergeOperator]

	mu                  sync.Mutex
	subtaskSortedKVMeta *external.SortedKVMeta
	summary             *execute.SubtaskSummary
}

func newMergeSortExecutor(
	task *proto.TaskBase,
	store kv.Storage,
	jobID int64,
	indexes []*model.IndexInfo,
	ptbl table.PhysicalTable,
	cloudStoreURI string,
) (*mergeSortExecutor, error) {
	return &mergeSortExecutor{
		task:          task,
		store:         store,
		jobID:         jobID,
		indexes:       indexes,
		ptbl:          ptbl,
		cloudStoreURI: cloudStoreURI,
		summary:       &execute.SubtaskSummary{},
	}, nil
}

func (*mergeSortExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge sort executor init subtask exec env")
	return nil
}

func (m *mergeSortExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("merge sort executor run subtask")

	accessRec, objStore, err := handle.NewObjStoreWithRecording(ctx, m.cloudStoreURI)
	if err != nil {
		return err
	}
	defer func() {
		objStore.Close()
		m.summary.MergeObjStoreRequests(&accessRec.Requests)
		m.GetMeterRecorder().MergeObjStoreAccess(accessRec)
	}()

	sm, err := decodeBackfillSubTaskMeta(ctx, objStore, subtask.Meta)
	if err != nil {
		return err
	}

	m.subtaskSortedKVMeta = &external.SortedKVMeta{}
	onWriterClose := func(summary *external.WriterSummary) {
		m.mu.Lock()
		m.subtaskSortedKVMeta.MergeSummary(summary)
		m.mu.Unlock()
	}

	prefix := path.Join(strconv.Itoa(int(subtask.TaskID)), strconv.Itoa(int(subtask.ID)))
	res := m.GetResource()
	memSizePerCon := res.Mem.Capacity() / res.CPU.Capacity()
	partSize := max(external.MinUploadPartSize, memSizePerCon*int64(external.MaxMergingFilesPerThread)/external.MaxUploadPartCount)

	wctx := workerpool.NewContext(ctx)
	op := external.NewMergeOperator(
		wctx,
		objStore,
		partSize,
		prefix,
		external.DefaultBlockSize,
		onWriterClose,
		external.NewMergeCollector(ctx, nil),
		int(res.CPU.Capacity()),
		true,
		engineapi.OnDuplicateKeyError,
	)

	m.mergeOp.Store(op)
	defer m.mergeOp.Store(nil)

	failpoint.InjectCall("mergeOverlappingFiles", op)

	err = external.MergeOverlappingFiles(
		wctx,
		sm.DataFiles,
		int(m.GetResource().CPU.Capacity()), // the concurrency used to split subtask
		op,
	)

	failpoint.Inject("mockMergeSortRunSubtaskError", func(_ failpoint.Value) {
		err = context.DeadlineExceeded
	})
	if err != nil {
		currentIdx, _, err2 := getIndexInfoAndID(sm.EleIDs, m.indexes)
		if err2 == nil {
			return ingest.TryConvertToKeyExistsErr(err, currentIdx, m.ptbl.Meta())
		}
		return errors.Trace(err)
	}
	return m.onFinished(ctx, subtask, sm, objStore)
}

func (*mergeSortExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge cleanup subtask exec env")
	return nil
}

func (m *mergeSortExecutor) onFinished(ctx context.Context, subtask *proto.Subtask, sm *BackfillSubTaskMeta, extStore objstore.ExternalStorage) error {
	logutil.Logger(ctx).Info("merge sort finish subtask")
	sm.MetaGroups = []*external.SortedKVMeta{m.subtaskSortedKVMeta}
	m.subtaskSortedKVMeta = nil
	// write external meta to storage when using global sort
	if err := writeExternalBackfillSubTaskMeta(ctx, extStore, sm, external.SubtaskMetaPath(subtask.TaskID, subtask.ID)); err != nil {
		return err
	}

	newMeta, err := sm.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	subtask.Meta = newMeta
	return nil
}

func (m *mergeSortExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return m.summary
}

func (m *mergeSortExecutor) ResetSummary() {
	m.summary.Reset()
}

func (m *mergeSortExecutor) ResourceModified(_ context.Context, newResource *proto.StepResource) error {
	currOp := m.mergeOp.Load()
	if currOp == nil {
		// let framework retry
		return goerrors.New("no subtask running")
	}

	targetConcurrency := int32(newResource.CPU.Capacity())
	currentConcurrency := currOp.GetWorkerPoolSize()
	// TODO(joechenrh): Currently, the worker pool size matches the task count for most times,
	// so wait here blocks until the subtask finish. Maybe we may improve this later by killing
	// tasks directly when reducing workers if tasks are idempotent.
	if targetConcurrency != currentConcurrency {
		currOp.TuneWorkerPoolSize(targetConcurrency, true)
	}

	return nil
}
