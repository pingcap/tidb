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
	"path"
	"strconv"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
)

type mergeSortExecutor struct {
	taskexecutor.EmptyStepExecutor
	jobID         int64
	idxNum        int
	ptbl          table.PhysicalTable
	cloudStoreURI string

	mu                  sync.Mutex
	subtaskSortedKVMeta *external.SortedKVMeta
}

func newMergeSortExecutor(
	jobID int64,
	idxNum int,
	ptbl table.PhysicalTable,
	cloudStoreURI string,
) (*mergeSortExecutor, error) {
	return &mergeSortExecutor{
		jobID:         jobID,
		idxNum:        idxNum,
		ptbl:          ptbl,
		cloudStoreURI: cloudStoreURI,
	}, nil
}

func (*mergeSortExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge sort executor init subtask exec env")
	return nil
}

func (m *mergeSortExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("merge sort executor run subtask")

	sm, err := decodeBackfillSubTaskMeta(subtask.Meta)
	if err != nil {
		return err
	}

	m.subtaskSortedKVMeta = &external.SortedKVMeta{}
	onClose := func(summary *external.WriterSummary) {
		m.mu.Lock()
		m.subtaskSortedKVMeta.MergeSummary(summary)
		m.mu.Unlock()
	}

	storeBackend, err := storage.ParseBackend(m.cloudStoreURI, nil)
	if err != nil {
		return err
	}
	store, err := storage.NewWithDefaultOpt(ctx, storeBackend)
	if err != nil {
		return err
	}

	prefix := path.Join(strconv.Itoa(int(m.jobID)), strconv.Itoa(int(subtask.ID)))
	res := m.GetResource()
	memSizePerCon := res.Mem.Capacity() / int64(subtask.Concurrency)
	partSize := max(external.MinUploadPartSize, memSizePerCon*int64(external.MaxMergingFilesPerThread)/10000)

	return external.MergeOverlappingFiles(
		ctx,
		sm.DataFiles,
		store,
		partSize,
		prefix,
		external.DefaultBlockSize,
		onClose,
		subtask.Concurrency,
		true,
	)
}

func (*mergeSortExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge cleanup subtask exec env")
	return nil
}

func (m *mergeSortExecutor) OnFinished(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("merge sort finish subtask")
	sm, err := decodeBackfillSubTaskMeta(subtask.Meta)
	if err != nil {
		return err
	}
	sm.MetaGroups = []*external.SortedKVMeta{m.subtaskSortedKVMeta}
	m.subtaskSortedKVMeta = nil
	newMeta, err := json.Marshal(sm)
	if err != nil {
		return errors.Trace(err)
	}
	subtask.Meta = newMeta
	return nil
}
