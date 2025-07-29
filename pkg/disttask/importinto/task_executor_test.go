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
	"fmt"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestImportTaskExecutor(t *testing.T) {
	ctx := context.Background()
	executor := NewImportExecutor(
		ctx,
		&proto.Task{
			TaskBase: proto.TaskBase{ID: 1},
		},
		taskexecutor.NewParamForTest(nil, nil, nil, ":4000"),
		nil,
	).(*importExecutor)

	require.NotNil(t, executor.BaseTaskExecutor.Extension)
	require.True(t, executor.IsIdempotent(&proto.Subtask{}))

	taskMeta := `{"Plan": {"TableInfo": {}}}`
	for _, step := range []proto.Step{
		proto.ImportStepImport,
		proto.ImportStepEncodeAndSort,
		proto.ImportStepMergeSort,
		proto.ImportStepWriteAndIngest,
		proto.ImportStepPostProcess,
	} {
		exe, err := executor.GetStepExecutor(&proto.Task{TaskBase: proto.TaskBase{Step: step}, Meta: []byte(taskMeta)})
		require.NoError(t, err)
		require.NotNil(t, exe)
	}
	_, err := executor.GetStepExecutor(&proto.Task{TaskBase: proto.TaskBase{Step: proto.StepInit}, Meta: []byte(taskMeta)})
	require.Error(t, err)
	_, err = executor.GetStepExecutor(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepImport}, Meta: []byte("")})
	require.Error(t, err)
}

type dummyTunable struct{ size int32 }

func (d *dummyTunable) TuneWorkerPoolSize(n int32, wait bool) { d.size = n }
func (d *dummyTunable) GetWorkerPoolSize() int32              { return d.size }

type noopWorker struct {
	ctx context.Context
}

func (w *noopWorker) HandleTask(task *importStepMinimalTask, done func(workerpool.None)) {
	select {
	case <-w.ctx.Done():
		done(workerpool.None{})
	case <-time.After(3 * time.Second):
		// Being defensive, we should not block the worker forever.
		done(workerpool.None{})
	}
}

func (w *noopWorker) Close() {}

type simpleLimiter struct{ limit int }

func (l *simpleLimiter) WaitN(ctx context.Context, id uint64, n int) error { return nil }
func (l *simpleLimiter) Limit() int                                        { return l.limit }
func (l *simpleLimiter) UpdateLimit(limit int)                             { l.limit = limit }

func setUnexportedField(obj any, field string, val any) {
	v := reflect.ValueOf(obj).Elem().FieldByName(field)
	reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem().Set(reflect.ValueOf(val))
}

func getUnexportedField(obj any, field string) any {
	v := reflect.ValueOf(obj).Elem().FieldByName(field)
	return reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem().Interface()
}

func newDummyEncodeOp() *encodeAndSortOperator {
	ctx, cancel := context.WithCancel(context.Background())
	pool := workerpool.NewWorkerPool("dummy", util.ImportInto, 1, func() workerpool.Worker[*importStepMinimalTask, workerpool.None] {
		return &noopWorker{ctx: ctx}
	})
	op := &encodeAndSortOperator{ctx: ctx, cancel: cancel}
	op.AsyncOperator = operator.NewAsyncOperator(ctx, pool)
	op.AsyncOperator.Open()
	return op
}

func TestImportStepExecutorResourceModified(t *testing.T) {
	tblImporter := &importer.TableImporter{
		LoadDataController: &importer.LoadDataController{Plan: &importer.Plan{ThreadCnt: 1, DesiredTableInfo: &model.TableInfo{}}},
	}
	e := &importStepExecutor{taskID: 1, taskMeta: &TaskMeta{Plan: importer.Plan{ThreadCnt: 1, DesiredTableInfo: &model.TableInfo{}}}, tableImporter: tblImporter, logger: zap.NewNop()}
	newRes := &proto.StepResource{CPU: proto.NewAllocatable(2), Mem: proto.NewAllocatable(2 * units.GiB)}
	require.ErrorContains(t, e.ResourceModified(context.Background(), newRes), "no subtask running")

	op := newDummyEncodeOp()
	e.op.Store(op)
	t.Cleanup(func() {
		op.cancel()
		op.AsyncOperator.Close()
	})

	require.NoError(t, e.ResourceModified(context.Background(), newRes))
	require.Equal(t, int32(2), op.GetWorkerPoolSize())
}

func TestImportStepExecutorTaskMetaModified(t *testing.T) {
	e := &importStepExecutor{}
	require.NoError(t, e.TaskMetaModified(context.Background(), []byte("whatever")))
}

type mockExternalStorage struct{}

func (*mockExternalStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	return nil
}
func (*mockExternalStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	return nil, nil
}
func (*mockExternalStorage) FileExists(ctx context.Context, name string) (bool, error) {
	return false, nil
}
func (*mockExternalStorage) DeleteFile(ctx context.Context, name string) error { return nil }
func (*mockExternalStorage) Open(ctx context.Context, path string, opt *storage.ReaderOption) (storage.ExternalFileReader, error) {
	return nil, nil
}
func (*mockExternalStorage) DeleteFiles(ctx context.Context, names []string) error { return nil }
func (*mockExternalStorage) WalkDir(ctx context.Context, opt *storage.WalkOption, fn func(string, int64) error) error {
	return nil
}
func (*mockExternalStorage) URI() string { return "mock://" }
func (*mockExternalStorage) Create(ctx context.Context, path string, opt *storage.WriterOption) (storage.ExternalFileWriter, error) {
	return nil, nil
}
func (*mockExternalStorage) Rename(ctx context.Context, oldName, newName string) error { return nil }
func (*mockExternalStorage) Close()                                                    {}

func TestMergeSortStepExecutorResourceModified(t *testing.T) {
	m := &mergeSortStepExecutor{taskID: 1, taskMeta: &TaskMeta{Plan: importer.Plan{ThreadCnt: 1, DesiredTableInfo: &model.TableInfo{}}}, logger: zap.NewNop()}
	res := &proto.StepResource{CPU: proto.NewAllocatable(4), Mem: proto.NewAllocatable(4 * units.GiB)}
	require.ErrorContains(t, m.ResourceModified(context.Background(), res), "no subtask running")

	opCtx, _ := operator.NewContext(context.Background())
	mockStore := &mockExternalStorage{}
	mergeOp := external.NewMergeOperator(opCtx, mockStore, 0, "", 0, func(summary *external.WriterSummary) {
		fmt.Println("onClose called with summary")
	}, 1, false, common.OnDuplicateKeyIgnore)
	require.NoError(t, mergeOp.Open())
	m.mergeOp.Store(mergeOp)
	t.Cleanup(func() {
		opCtx.Cancel()
		mergeOp.AsyncOperator.Close()
	})

	require.NoError(t, m.ResourceModified(context.Background(), res))
	require.Equal(t, int32(4), mergeOp.GetWorkerPoolSize())
}

func TestMergeSortStepExecutorTaskMetaModified(t *testing.T) {
	m := &mergeSortStepExecutor{}
	require.NoError(t, m.TaskMetaModified(context.Background(), nil))
}

func newBackendWithLimiter() *local.Backend {
	b := &local.Backend{}
	lim := &simpleLimiter{}
	setUnexportedField(b, "writeLimiter", lim)
	return b
}

func TestWriteAndIngestStepExecutorTaskMetaModified(t *testing.T) {
	backend := newBackendWithLimiter()
	ti := &importer.TableImporter{LoadDataController: &importer.LoadDataController{Plan: &importer.Plan{}}}
	setUnexportedField(ti, "backend", backend)

	e := &writeAndIngestStepExecutor{taskID: 1, taskMeta: &TaskMeta{Plan: importer.Plan{MaxWriteSpeed: 100}}, tableImporter: ti, logger: zap.NewNop()}
	newMeta, _ := json.Marshal(&TaskMeta{Plan: importer.Plan{MaxWriteSpeed: 200}})
	require.NoError(t, e.TaskMetaModified(context.Background(), newMeta))
	lim := getUnexportedField(backend, "writeLimiter").(*simpleLimiter)
	require.Equal(t, 200, lim.limit)
	require.Equal(t, importer.Plan{MaxWriteSpeed: 200}, e.taskMeta.Plan)
}

func TestWriteAndIngestStepExecutorResourceModified(t *testing.T) {
	ti := &importer.TableImporter{LoadDataController: &importer.LoadDataController{Plan: &importer.Plan{}}}
	backend := newBackendWithLimiter()
	setUnexportedField(ti, "backend", backend)

	e := &writeAndIngestStepExecutor{taskID: 1, taskMeta: &TaskMeta{}, tableImporter: ti, logger: zap.NewNop()}
	res := &proto.StepResource{CPU: proto.NewAllocatable(4), Mem: proto.NewAllocatable(units.GiB)}
	require.Error(t, e.ResourceModified(context.Background(), res))

	eng := &external.Engine{}
	setUnexportedField(eng, "readyCh", make(chan struct{}, 1))
	w := &dummyTunable{}
	eng.SetWorker(w)
	ch := getUnexportedField(eng, "readyCh").(chan struct{})
	ch <- struct{}{}
	e.engine.Store(eng)

	require.NoError(t, e.ResourceModified(context.Background(), res))
	require.Equal(t, int32(4), w.size)
	require.Equal(t, int32(4), backend.WorkerConcurrency.Load())
}

func TestCollectConflictsStepExecutorTaskMetaModified(t *testing.T) {
	e := &collectConflictsStepExecutor{}
	require.NoError(t, e.TaskMetaModified(context.Background(), []byte("whatever")))
}

func TestCollectConflictsStepExecutorResourceModified(t *testing.T) {
	e := &collectConflictsStepExecutor{}
	r := &proto.StepResource{}
	require.NoError(t, e.ResourceModified(context.Background(), r))
}

func TestConflictResolutionStepExecutorTaskMetaModified(t *testing.T) {
	e := &conflictResolutionStepExecutor{}
	require.NoError(t, e.TaskMetaModified(context.Background(), []byte("whatever")))
}

func TestConflictResolutionStepExecutorResourceModified(t *testing.T) {
	e := &conflictResolutionStepExecutor{}
	r := &proto.StepResource{}
	require.NoError(t, e.ResourceModified(context.Background(), r))
}

func TestPostProcessStepExecutorTaskMetaModified(t *testing.T) {
	e := &postProcessStepExecutor{}
	require.NoError(t, e.TaskMetaModified(context.Background(), []byte("whatever")))
}

func TestPostProcessStepExecutorResourceModified(t *testing.T) {
	e := &postProcessStepExecutor{}
	r := &proto.StepResource{}
	require.NoError(t, e.ResourceModified(context.Background(), r))
}
