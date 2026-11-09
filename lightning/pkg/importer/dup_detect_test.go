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

package importer

import (
	"context"
	"github.com/cockroachdb/pebble/vfs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/duplicate"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
	"github.com/pingcap/tidb/pkg/util/extsort"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var (
	exampleHandleKey = tablecodec.EncodeRowKeyWithHandle(121, kv.IntHandle(22))
	exampleIndexID   = int64(23)
	exampleIndexKey  = tablecodec.EncodeIndexSeekKey(122, exampleIndexID, nil)
)

func TestErrorOnDup(t *testing.T) {
	h := &errorOnDup{}
	require.NoError(t, h.Begin(exampleHandleKey))
	require.NoError(t, h.Append([]byte{1}))
	require.NoError(t, h.Append([]byte{2}))
	err := h.End()
	require.ErrorIs(t, err, ErrDuplicateKey)
	dupErr := errors.Cause(err).(*errors.Error)
	require.Equal(t, conflictOnHandle, dupErr.Args()[0])
	require.Equal(t, [][]byte{{1}, {2}}, dupErr.Args()[1])
	require.NoError(t, h.Close())

	h = &errorOnDup{}
	require.NoError(t, h.Begin(exampleIndexKey))
	require.NoError(t, h.Append([]byte{11}))
	require.NoError(t, h.Append([]byte{12}))
	err = h.End()
	require.ErrorIs(t, err, ErrDuplicateKey)
	dupErr = errors.Cause(err).(*errors.Error)
	require.Equal(t, int64(23), dupErr.Args()[0])
	require.Equal(t, [][]byte{{11}, {12}}, dupErr.Args()[1])
	require.NoError(t, h.Close())
}

func TestReplaceOnDup(t *testing.T) {
	runDupHandlerTest(t,
		func(w extsort.Writer) duplicate.Handler { return &replaceOnDup{w: w} },
		[]dupRecord{{
			exampleHandleKey, [][]byte{[]byte("01"), []byte("02"), []byte("03")}},
			{exampleIndexKey, [][]byte{[]byte("11"), []byte("12"), []byte("13")}}},
		map[int64][][]byte{
			conflictOnHandle: {[]byte("01"), []byte("02")},
			exampleIndexID:   {[]byte("11"), []byte("12")},
		},
	)
}

type dupRecord struct {
	key    []byte
	rowIDs [][]byte
}

func runDupHandlerTest(
	t *testing.T,
	makeHandler func(w extsort.Writer) duplicate.Handler,
	input []dupRecord,
	ignoredRowIDs map[int64][][]byte,
) {
	ignoreRows, err := extsort.OpenDiskSorter(t.TempDir(), nil)
	require.NoError(t, err)
	defer ignoreRows.Close()

	ctx := context.Background()
	w, err := ignoreRows.NewWriter(ctx)
	require.NoError(t, err)
	h := makeHandler(w)

	for _, r := range input {
		require.NoError(t, h.Begin(r.key))
		for _, rowID := range r.rowIDs {
			require.NoError(t, h.Append(rowID))
		}
		require.NoError(t, h.End())
	}
	require.NoError(t, h.Close())

	require.NoError(t, ignoreRows.Sort(ctx))
	it, err := ignoreRows.NewIterator(ctx)
	require.NoError(t, err)

	rowIDs := map[int64][][]byte{}
	for it.First(); it.Valid(); it.Next() {
		_, idxID, err := codec.DecodeVarint(it.UnsafeValue())
		require.NoError(t, err)
		rowIDs[idxID] = append(rowIDs[idxID], slices.Clone(it.UnsafeKey()))
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())

	require.Equal(t, ignoredRowIDs, rowIDs)
}

func TestSimplifyTable(t *testing.T) {
	testCases := []struct {
		table             string
		colPerm           []int
		expTable          string
		expTableHasNoCols bool
		expColPerm        []int
	}{
		{
			table:             "CREATE TABLE t(a int, b int, c int)",
			colPerm:           []int{0, 1, 2, -1},
			expTableHasNoCols: true,
			expColPerm:        []int{-1},
		},
		{
			table:      "CREATE TABLE t(a int PRIMARY KEY, b int, c int)",
			colPerm:    []int{2, 0, 1},
			expTable:   "CREATE TABLE t(a int PRIMARY KEY)",
			expColPerm: []int{2},
		},
		{
			table:      "CREATE TABLE t(a int UNIQUE KEY, b int, c int, d int, INDEX idx_b(b), INDEX idx_c(c), UNIQUE INDEX idx_bc(b, c))",
			colPerm:    []int{0, 1, 2, 3, 10},
			expTable:   "CREATE TABLE t(a int UNIQUE KEY, b int, c int, UNIQUE INDEX idx_bc(b, c))",
			expColPerm: []int{0, 1, 2, 10},
		},
		{
			table:      "CREATE TABLE t(a int, b int, c int, d int, INDEX idx_b(b), INDEX idx_c(c), UNIQUE INDEX idx_cd(c, d))",
			colPerm:    []int{0, 1, 2, 3, 10},
			expTable:   "CREATE TABLE t(c int, d int, UNIQUE INDEX idx_cd(c, d))",
			expColPerm: []int{2, 3, 10},
		},
	}
	for _, tc := range testCases {
		p := parser.New()
		originalTblInfo, err := dbutiltest.GetTableInfoBySQL(tc.table, p)
		require.NoError(t, err)

		// run twice to make sure originalTblInfo is not changed
		for i := 0; i < 2; i++ {
			actualTblInfo, actualColPerm := simplifyTable(originalTblInfo, tc.colPerm)
			if tc.expTableHasNoCols {
				require.Empty(t, actualTblInfo.Columns)
			} else {
				expTblInfo, err := dbutiltest.GetTableInfoBySQL(tc.expTable, p)
				require.NoError(t, err)

				require.Equal(t, len(expTblInfo.Columns), len(actualTblInfo.Columns))
				for i, col := range actualTblInfo.Columns {
					require.Equal(t, expTblInfo.Columns[i].Name, col.Name)
					require.Equal(t, expTblInfo.Columns[i].Offset, col.Offset)
				}

				require.Equal(t, len(expTblInfo.Indices), len(actualTblInfo.Indices))
				for i, idxInfo := range actualTblInfo.Indices {
					require.Equal(t, expTblInfo.Indices[i].Name, idxInfo.Name)
					require.Equal(t, expTblInfo.Indices[i].Columns, idxInfo.Columns)
				}
			}
			require.Equal(t, tc.expColPerm, actualColPerm)
		}
	}
}

// TestDuplicateSummaryWinners records the same winning source through two
// indexes, then reloads its sorter. Statistics must preserve one winner lookup
// per source while the original handler continues to emit the losing row IDs.
func TestDuplicateSummaryWinners(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "duplicates")
	tr := &TableImporter{}
	tr.prepareSummaryDuplicates(true, dir)
	ignore, err := extsort.OpenDiskSorter(filepath.Join(t.TempDir(), "ignore"), nil)
	require.NoError(t, err)
	defer ignore.CloseAndCleanup()
	constructor := tr.summaryDuplicateHandler(makeDupHandlerConstructor(ignore, config.ReplaceOnDup))
	handler, err := constructor(ctx)
	require.NoError(t, err)
	for _, key := range [][]byte{exampleHandleKey, exampleIndexKey} {
		require.NoError(t, handler.Begin(key))
		require.NoError(t, handler.Append(common.EncodeIntRowID(1)))
		require.NoError(t, handler.Append(common.EncodeIntRowID(2)))
		require.NoError(t, handler.End())
	}
	require.NoError(t, handler.Close())
	tr.finishSummaryDuplicates(ctx, dir)
	require.True(t, tr.summaryDuplicates.ready())
	lookup := tr.summaryWinnerLookup(ctx)
	require.False(t, lookup.contains(1))
	require.True(t, lookup.contains(2))
	lookup.close()
	tr.summaryDuplicates.close()
	tr = &TableImporter{}
	tr.prepareSummaryDuplicates(false, dir)
	defer tr.summaryDuplicates.close()
	require.True(t, tr.summaryDuplicates.ready())
	lookup = tr.summaryWinnerLookup(ctx)
	defer lookup.close()
	require.False(t, lookup.contains(1))
	require.True(t, lookup.contains(2))
}

// summaryFailureFS fails one fresh-sorter lifecycle operation while forwarding
// unrelated filesystem work to the real test directory.
type summaryFailureFS struct {
	vfs.FS
	path       string
	cleanupErr error
	reopenErr  error
	removed    bool
}

func (fs *summaryFailureFS) RemoveAll(path string) error {
	if path == fs.path {
		if fs.cleanupErr != nil {
			return fs.cleanupErr
		}
		fs.removed = true
	}
	return fs.FS.RemoveAll(path)
}
func (fs *summaryFailureFS) MkdirAll(path string, mode os.FileMode) error {
	if path == fs.path && fs.removed && fs.reopenErr != nil {
		return fs.reopenErr
	}
	return fs.FS.MkdirAll(path, mode)
}

// TestDuplicateSummaryPreparationFailure injects cleanup and reopen failures
// after the first successful open. Statistics must degrade without closing an
// already released sorter, while the successful path remains usable.
func TestDuplicateSummaryPreparationFailure(t *testing.T) {
	for _, scenario := range []string{"cleanup", "reopen", "success"} {
		t.Run(scenario, func(t *testing.T) {
			core, observed := observer.New(zap.WarnLevel)
			dir := t.TempDir()
			originalFS := vfs.Default
			fs := &summaryFailureFS{FS: originalFS, path: filepath.Join(dir, "pkdb-statistics")}
			if scenario == "cleanup" {
				fs.cleanupErr = errors.New("cleanup failed")
			}
			if scenario == "reopen" {
				fs.reopenErr = errors.New("reopen failed")
			}
			vfs.Default = fs
			defer func() { vfs.Default = originalFS }()
			tr := &TableImporter{logger: log.Logger{Logger: zap.New(core)}}
			tr.prepareSummaryDuplicates(true, dir)
			require.Equal(t, scenario == "success", tr.summaryDuplicates.ready())
			require.NotPanics(t, func() { tr.summaryDuplicates.close() })
			warnings := observed.FilterMessage("duplicate statistics are incomplete").All()
			if scenario == "success" {
				require.Empty(t, warnings)
			} else {
				require.Len(t, warnings, 1)
				require.Equal(t, scenario, warnings[0].ContextMap()["operation"])
				require.Contains(t, warnings[0].ContextMap()["error"], scenario+" failed")
			}
		})
	}
}

type summaryCountingSorter struct {
	extsort.ExternalSorter
	calls  int
	writer *summaryCountingWriter
}

func (s *summaryCountingSorter) NewWriter(context.Context) (extsort.Writer, error) {
	s.calls++
	return s.writer, nil
}

type summaryCountingWriter struct {
	keys     [][]byte
	closes   int
	putErr   error
	closeErr error
}

func (w *summaryCountingWriter) Put(key, _ []byte) error {
	w.keys = append(w.keys, slices.Clone(key))
	return w.putErr
}
func (w *summaryCountingWriter) Flush() error { return nil }
func (w *summaryCountingWriter) Close() error { w.closes++; return w.closeErr }

// TestDuplicateSummaryWriterIsLazy constructs workers without duplicates and
// with error/replace policies. Only successfully handled duplicate groups need
// a statistics writer, and later groups must reuse it.
func TestDuplicateSummaryWriterIsLazy(t *testing.T) {
	for _, scenario := range []string{"no duplicates", "error", "replace"} {
		t.Run(scenario, func(t *testing.T) {
			sorter := &summaryCountingSorter{writer: &summaryCountingWriter{}}
			tr := &TableImporter{summaryDuplicates: &summaryDuplicates{sorter: sorter}}
			constructor := tr.summaryDuplicateHandler(func(context.Context) (duplicate.Handler, error) {
				if scenario == "replace" {
					return &replaceOnDup{w: &summaryCountingWriter{}}, nil
				}
				return &errorOnDup{}, nil
			})
			handler, err := constructor(context.Background())
			require.NoError(t, err)
			require.Zero(t, sorter.calls)
			if scenario != "no duplicates" {
				for range 2 {
					require.NoError(t, handler.Begin(exampleHandleKey))
					require.NoError(t, handler.Append(common.EncodeIntRowID(1)))
					require.NoError(t, handler.Append(common.EncodeIntRowID(2)))
					err = handler.End()
					if scenario == "error" {
						require.ErrorIs(t, err, ErrDuplicateKey)
						break
					}
					require.NoError(t, err)
				}
			}
			require.NoError(t, handler.Close())
			if scenario == "replace" {
				require.Equal(t, 1, sorter.calls)
				require.Len(t, sorter.writer.keys, 2)
				require.Equal(t, 1, sorter.writer.closes)
			} else {
				require.Zero(t, sorter.calls)
			}
		})
	}
}

// Expected missing/unsupported resume metadata stays quiet; actual marker I/O
// failures report their cause without turning optional statistics into an import error.
func TestDuplicateSummaryMarkerDiagnostics(t *testing.T) {
	for _, scenario := range []string{"missing", "unsupported", "read error", "write error"} {
		t.Run(scenario, func(t *testing.T) {
			core, observed := observer.New(zap.WarnLevel)
			dir := t.TempDir()
			marker := filepath.Join(dir, "pkdb-statistics-complete")
			if scenario == "unsupported" {
				require.NoError(t, os.WriteFile(marker, []byte("2"), 0600))
			}
			if scenario == "read error" {
				require.NoError(t, os.Mkdir(marker, 0700))
			}
			tr := &TableImporter{tableName: "table", logger: log.Logger{Logger: zap.New(core)}}
			tr.prepareSummaryDuplicates(scenario == "write error", dir)
			defer tr.summaryDuplicates.close()
			if scenario == "write error" {
				require.NoError(t, os.Mkdir(marker, 0700))
				tr.finishSummaryDuplicates(context.Background(), dir)
			}
			warnings := observed.FilterMessage("duplicate statistics are incomplete").All()
			if scenario == "missing" || scenario == "unsupported" {
				require.Empty(t, warnings)
			} else {
				require.Len(t, warnings, 1)
				require.Equal(t, "table", warnings[0].ContextMap()["table"])
				require.Contains(t, warnings[0].ContextMap()["operation"], "completion marker")
				require.NotEmpty(t, warnings[0].ContextMap()["error"])
			}
			require.False(t, tr.summaryDuplicates.ready())
		})
	}
}

// Writer/close failures must leave the underlying duplicate policy successful,
// and repeated failures in the shared sorter must produce only one warning.
func TestDuplicateSummaryWriterFailureDiagnostics(t *testing.T) {
	for _, operation := range []string{"write winner", "close writer"} {
		t.Run(operation, func(t *testing.T) {
			core, observed := observer.New(zap.WarnLevel)
			writer := &summaryCountingWriter{closeErr: errors.New("statistics close failed")}
			if operation == "write winner" {
				writer.putErr = errors.New("statistics write failed")
			}
			state := &summaryDuplicates{sorter: &summaryCountingSorter{writer: writer}, logger: log.Logger{Logger: zap.New(core)}}
			tr := &TableImporter{summaryDuplicates: state}
			constructor := tr.summaryDuplicateHandler(func(context.Context) (duplicate.Handler, error) {
				return &replaceOnDup{w: &summaryCountingWriter{}}, nil
			})
			handler, err := constructor(context.Background())
			require.NoError(t, err)
			for range 2 {
				require.NoError(t, handler.Begin(exampleHandleKey))
				require.NoError(t, handler.Append(common.EncodeIntRowID(1)))
				require.NoError(t, handler.Append(common.EncodeIntRowID(2)))
				require.NoError(t, handler.End())
			}
			require.NoError(t, handler.Close())
			warnings := observed.FilterMessage("duplicate statistics are incomplete").All()
			require.Len(t, warnings, 1)
			require.Equal(t, operation, warnings[0].ContextMap()["operation"])
			require.Contains(t, warnings[0].ContextMap()["error"], "statistics")
			require.False(t, state.ready())
		})
	}
}
