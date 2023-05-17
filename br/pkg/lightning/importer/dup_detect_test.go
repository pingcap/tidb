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
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/duplicate"
	"github.com/pingcap/tidb/util/extsort"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestErrorOnDup(t *testing.T) {
	h := &errorOnDup{}
	err := h.Begin([]byte("key"))
	require.Error(t, err)
	require.NoError(t, h.Close())
}

func TestReplaceOnDup(t *testing.T) {
	runDupHandlerTest(t,
		func(w extsort.Writer) duplicate.Handler { return &replaceOnDup{w: w} },
		[]dupRecord{{
			[]byte("key1"), [][]byte{[]byte("01"), []byte("02"), []byte("03")}},
			{[]byte("key2"), [][]byte{[]byte("11"), []byte("12"), []byte("13")}}},
		[][]byte{[]byte("01"), []byte("02"), []byte("11"), []byte("12")},
	)
}

func TestIgnoreOnDup(t *testing.T) {
	runDupHandlerTest(t,
		func(w extsort.Writer) duplicate.Handler { return &ignoreOnDup{w: w} },
		[]dupRecord{{
			[]byte("key1"), [][]byte{[]byte("01"), []byte("02"), []byte("03")}},
			{[]byte("key2"), [][]byte{[]byte("11"), []byte("12"), []byte("13")}}},
		[][]byte{[]byte("02"), []byte("03"), []byte("12"), []byte("13")},
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
	ignoredRowIDs [][]byte,
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

	var rowIDs [][]byte
	for it.First(); it.Valid(); it.Next() {
		rowIDs = append(rowIDs, slices.Clone(it.UnsafeKey()))
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())

	require.Equal(t, ignoredRowIDs, rowIDs)
}
