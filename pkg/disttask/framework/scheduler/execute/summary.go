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

package execute

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Summary is used to collect the summary of subtasks execution.
type Summary struct {
	mu struct {
		sync.Mutex
		RowCount map[int64]int64 // subtask ID -> row count
	}
}

// NewSummary creates a new Summary.
func NewSummary() *Summary {
	return &Summary{
		mu: struct {
			sync.Mutex
			RowCount map[int64]int64
		}{
			RowCount: map[int64]int64{},
		},
	}
}

// UpdateRowCount updates the row count of the subtask.
func (s *Summary) UpdateRowCount(subtaskID int64, rowCount int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.RowCount[subtaskID] = rowCount
}

// UpdateRowCountLoop updates the row count of the subtask periodically.
func (s *Summary) UpdateRowCountLoop(ctx context.Context, taskMgr *storage.TaskManager) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.PersistRowCount(ctx, taskMgr)
		}
	}
}

// PersistRowCount persists the row count of the subtask to the storage.
func (s *Summary) PersistRowCount(ctx context.Context, taskMgr *storage.TaskManager) {
	var copiedRowCount map[int64]int64
	s.mu.Lock()
	if len(s.mu.RowCount) == 0 {
		s.mu.Unlock()
		return
	}
	copiedRowCount = make(map[int64]int64, len(s.mu.RowCount))
	for subtaskID, rowCount := range s.mu.RowCount {
		copiedRowCount[subtaskID] = rowCount
	}
	s.mu.Unlock()

	for subtaskID, rowCount := range copiedRowCount {
		err := taskMgr.UpdateSubtaskRowCount(subtaskID, rowCount)
		if err != nil {
			logutil.Logger(ctx).Warn("update subtask row count failed", zap.Error(err))
		}
	}
	s.mu.Lock()
	for subtaskID := range copiedRowCount {
		delete(s.mu.RowCount, subtaskID)
	}
	s.mu.Unlock()
}
