// Copyright 2026 PingCAP, Inc.
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

package importer

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/duplicate"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/extsort"
	"go.uber.org/zap"
)

// summaryDuplicates stores only winning source IDs of the existing pre-dedup
// pass. A source matching several indexes is still classified exactly once by
// the normal row reader. Statistics failures never fail duplicate handling.
type summaryDuplicates struct {
	sorter   extsort.ExternalSorter
	failed   atomic.Bool
	finished bool
	table    string
	path     string
	logger   log.Logger
}

// Many workers can encounter the same unavailable sorter. Preserve best-effort
// import behavior while reporting the first cause once for this table's statistics.
func (s *summaryDuplicates) fail(operation string, err error) {
	if s.failed.CompareAndSwap(false, true) {
		logger := s.logger
		if logger.Logger == nil {
			logger = log.L()
		}
		logger.Warn("duplicate statistics are incomplete", zap.String("table", s.table),
			zap.String("path", s.path), zap.String("operation", operation), zap.Error(err))
	}
}

func (s *summaryDuplicates) ready() bool { return s != nil && s.sorter != nil && !s.failed.Load() }

// complete survives resource cleanup so delayed post-processing can confirm
// staged rows. Later lookup failures still invalidate the completed pass.
func (s *summaryDuplicates) complete() bool { return s != nil && s.finished && !s.failed.Load() }

func (s *summaryDuplicates) close() {
	if s != nil && s.sorter != nil {
		sorter := s.sorter
		s.sorter = nil
		_ = sorter.Close()
	}
}

func (tr *TableImporter) prepareSummaryDuplicates(fresh bool, dir string) {
	state := &summaryDuplicates{table: tr.tableName, path: dir, logger: tr.logger}
	marker := filepath.Join(dir, "pkdb-statistics-complete")
	if !fresh {
		version, err := os.ReadFile(marker)
		if err != nil {
			// Older checkpoints and unfinished optional passes normally lack a marker.
			if !os.IsNotExist(err) {
				state.fail("read completion marker", err)
			}
			return
		}
		if string(version) != "1" {
			return
		}
	}
	// Winner entries contain only source IDs; cap the per-worker statistics
	// buffer independently of the primary duplicate-policy sorter.
	sorter, err := extsort.OpenDiskSorter(filepath.Join(dir, "pkdb-statistics"), &extsort.DiskSorterOptions{WriterBufferSize: 1 << 20})
	if err != nil {
		state.fail("open", err)
		return
	}
	state.sorter = sorter
	tr.summaryDuplicates = state
	// If the duplicate pass is restarted, discard any partial statistics sorter.
	// Its content is observational and is never used by the import policy.
	if fresh {
		// Cleanup consumes the old sorter even when removing its files fails.
		tr.summaryDuplicates.sorter = nil
		if err = sorter.CloseAndCleanup(); err != nil {
			state.fail("cleanup", err)
			return
		}
		sorter, err = extsort.OpenDiskSorter(filepath.Join(dir, "pkdb-statistics"), &extsort.DiskSorterOptions{WriterBufferSize: 1 << 20})
		if err != nil {
			state.fail("reopen", err)
			return
		}
		tr.summaryDuplicates.sorter = sorter
		_ = os.Remove(marker)
	}
}

func (tr *TableImporter) finishSummaryDuplicates(ctx context.Context, dir string) {
	s := tr.summaryDuplicates
	if !s.ready() {
		return
	}
	if !s.sorter.IsSorted() {
		if err := s.sorter.Sort(ctx); err != nil {
			s.fail("sort", err)
			return
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "pkdb-statistics-complete"), []byte("1"), 0600); err != nil {
		s.fail("write completion marker", err)
		return
	}
	s.finished = true
}

func (tr *TableImporter) summaryDuplicateHandler(original duplicate.HandlerConstructor) duplicate.HandlerConstructor {
	s := tr.summaryDuplicates
	if !s.ready() {
		return original
	}
	return func(ctx context.Context) (duplicate.Handler, error) {
		handler, err := original(ctx)
		if err != nil {
			return nil, err
		}
		return &summaryDuplicateHandler{Handler: handler, ctx: ctx, state: s}, nil
	}
}

type summaryDuplicateHandler struct {
	duplicate.Handler
	ctx    context.Context
	writer extsort.Writer
	state  *summaryDuplicates
	last   []byte
}

func (h *summaryDuplicateHandler) Begin(key []byte) error {
	h.last = h.last[:0]
	return h.Handler.Begin(key)
}
func (h *summaryDuplicateHandler) Append(id []byte) error {
	h.last = append(h.last[:0], id...)
	return h.Handler.Append(id)
}
func (h *summaryDuplicateHandler) End() error {
	if err := h.Handler.End(); err != nil {
		return err
	}
	if !h.state.ready() {
		return nil
	}
	// Most workers see no duplicate group; error policy never accepts one.
	// Allocate the small statistics buffer only after the policy succeeds.
	if h.writer == nil {
		var err error
		h.writer, err = h.state.sorter.NewWriter(h.ctx)
		if err != nil {
			h.state.fail("new writer", err)
			return nil
		}
	}
	if err := h.writer.Put(h.last, []byte{1}); err != nil {
		h.state.fail("write winner", err)
	}
	return nil
}
func (h *summaryDuplicateHandler) Close() error {
	if h.writer != nil {
		if err := h.writer.Close(); err != nil {
			h.state.fail("close writer", err)
		}
	}
	return h.Handler.Close()
}

type summaryWinnerLookup struct {
	state       *summaryDuplicates
	iter        extsort.Iterator
	initialized bool
}

func (tr *TableImporter) summaryWinnerLookup(ctx context.Context) *summaryWinnerLookup {
	lookup := &summaryWinnerLookup{state: tr.summaryDuplicates}
	if !lookup.state.ready() {
		return lookup
	}
	iter, err := lookup.state.sorter.NewIterator(ctx)
	if err != nil {
		lookup.state.fail("new iterator", err)
		return lookup
	}
	lookup.iter = iter
	return lookup
}

func (l *summaryWinnerLookup) contains(rowID int64) bool {
	if l.iter == nil {
		return false
	}
	key := common.EncodeIntRowID(rowID)
	if !l.initialized {
		l.iter.Seek(key)
		l.initialized = true
	}
	for l.iter.Valid() && bytes.Compare(l.iter.UnsafeKey(), key) < 0 {
		l.iter.Next()
	}
	if err := l.iter.Error(); err != nil {
		l.state.fail("read winner", err)
		return false
	}
	return l.iter.Valid() && bytes.Equal(l.iter.UnsafeKey(), key)
}
func (l *summaryWinnerLookup) close() {
	if l.iter != nil {
		_ = l.iter.Close()
	}
}
