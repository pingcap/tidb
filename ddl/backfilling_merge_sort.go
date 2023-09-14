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

	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
)

type mergeSortExecutor struct {
	jobID int64
	index *model.IndexInfo
	ptbl  table.PhysicalTable
	bc    ingest.BackendCtx
}

func newMergeSortExecutor(
	jobID int64,
	index *model.IndexInfo,
	ptbl table.PhysicalTable,
	bc ingest.BackendCtx,
) *mergeSortExecutor {
	return &mergeSortExecutor{
		jobID: jobID,
		index: index,
		ptbl:  ptbl,
		bc:    bc,
	}
}

func (m *mergeSortExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge sort stage init subtask exec env")
	return nil
}

func (m *mergeSortExecutor) RunSubtask(ctx context.Context, _ *proto.Subtask) error {
	logutil.Logger(ctx).Info("merge stage split subtask")

	// ywq todo how to？
	onClose := func(summary *external.WriterSummary) {
		sum, _ := r.subtaskSummary.Load(subtaskID)
		s := sum.(*readIndexSummary)
		s.mu.Lock()
		if len(s.minKey) == 0 || summary.Min.Cmp(s.minKey) < 0 {
			s.minKey = summary.Min.Clone()
		}
		if len(s.maxKey) == 0 || summary.Max.Cmp(s.maxKey) > 0 {
			s.maxKey = summary.Max.Clone()
		}
		s.totalSize += summary.TotalSize
		for _, f := range summary.MultipleFilesStats {
			for _, filename := range f.Filenames {
				s.dataFiles = append(s.dataFiles, filename[0])
				s.statFiles = append(s.statFiles, filename[1])
			}
		}
		s.mu.Unlock()
	}

}

func (*mergeSortExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge sort stage cleanup subtask exec env")
	return nil
}

func (*mergeSortExecutor) OnFinished(ctx context.Context, _ *proto.Subtask) error {
	logutil.Logger(ctx).Info("merge sort stage finish subtask")
	return nil
}

func (*mergeSortExecutor) Rollback(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge sort stage rollback backfill add index task")
	return nil
}
