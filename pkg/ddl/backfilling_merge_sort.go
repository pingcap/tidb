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
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

type mergeSortExecutor struct {
	jobID               int64
	idxNum              int
	ptbl                table.PhysicalTable
	bc                  ingest.BackendCtx
	cloudStoreURI       string
	mu                  sync.Mutex
	subtaskSortedKVMeta *external.SortedKVMeta
}

func newMergeSortExecutor(
	jobID int64,
	idxNum int,
	ptbl table.PhysicalTable,
	bc ingest.BackendCtx,
	cloudStoreURI string,
) (*mergeSortExecutor, error) {
	return &mergeSortExecutor{
		jobID:         jobID,
		idxNum:        idxNum,
		ptbl:          ptbl,
		bc:            bc,
		cloudStoreURI: cloudStoreURI,
	}, nil
}

func (*mergeSortExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge sort executor init subtask exec env")
	return nil
}

func (m *mergeSortExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("merge sort executor run subtask")

	sm := &BackfillSubTaskMeta{}
	err := json.Unmarshal(subtask.Meta, sm)
	if err != nil {
		logutil.BgLogger().Error("unmarshal error",
			zap.String("category", "ddl"),
			zap.Error(err))
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

	partSize, err := getMergeSortPartSize(int(variable.GetDDLReorgWorkerCounter()), m.idxNum)
	if err != nil {
		return err
	}

	return external.MergeOverlappingFiles(
		ctx,
		sm.DataFiles,
		store,
		int64(partSize),
		64*1024,
		prefix,
		external.DefaultBlockSize,
		8*1024,
		1*size.MB,
		8*1024,
		onClose,
		int(variable.GetDDLReorgWorkerCounter()), true)
}

func (*mergeSortExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge cleanup subtask exec env")
	return nil
}

func (m *mergeSortExecutor) OnFinished(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("merge sort finish subtask")
	var subtaskMeta BackfillSubTaskMeta
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

func (*mergeSortExecutor) Rollback(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge sort executor rollback backfill add index task")
	return nil
}
