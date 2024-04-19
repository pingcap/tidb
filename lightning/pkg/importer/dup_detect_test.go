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
	"slices"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/duplicate"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
	"github.com/pingcap/tidb/pkg/util/extsort"
	"github.com/stretchr/testify/require"
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
