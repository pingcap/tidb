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

package ddl

import (
	"context"
	"testing"

	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/stretchr/testify/require"
)

func TestSPFreshMemoryImportRangesAndRetry(t *testing.T) {
	// The full API V2 prefix must survive the memory adapter unchanged.
	key := func(s string) []byte { return append([]byte{'x', 0, 0, 42}, s...) }
	records := []pkdbspfreshbulk.SortedRunRecord{
		{Key: key("a"), Value: []byte("one")},
		{Key: key("c"), Value: []byte("two")},
		{Key: key("e"), Value: []byte("three")},
	}
	e := newSPFreshBulkImportEngine(spfreshBulkSortedRun{records: records}, "run", 123)
	defer e.Close()
	require.Equal(t, uint64(123), e.GetTS())
	start, end, err := e.GetKeyRange()
	require.NoError(t, err)
	require.Equal(t, key("a"), start)
	require.Equal(t, key("e\x00"), end)
	for _, tc := range []struct {
		name         string
		lower, upper []byte
		want         []int
	}{
		{"all", nil, nil, []int{0, 1, 2}},
		{"exclusive end", key("a"), key("e"), []int{0, 1}},
		{"gap", key("b"), key("d"), []int{1}},
		{"last", key("e"), end, []int{2}},
		{"before", nil, key("a"), nil},
		{"after", key("f"), nil, nil},
		{"empty gap", key("b"), key("c"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first, last, err := e.GetFirstAndLastKey(tc.lower, tc.upper)
			require.NoError(t, err)
			if len(tc.want) == 0 {
				require.Nil(t, first)
				require.Nil(t, last)
			} else {
				require.Equal(t, records[tc.want[0]].Key, first)
				require.Equal(t, records[tc.want[len(tc.want)-1]].Key, last)
			}
			// Region rescans/retries create fresh iterators over the same bytes.
			for range 2 {
				iter := e.NewIter(context.Background(), tc.lower, tc.upper, nil)
				var seen int
				for iter.First(); iter.Valid(); iter.Next() {
					require.Less(t, seen, len(tc.want))
					r := records[tc.want[seen]]
					require.Same(t, &r.Key[0], &iter.Key()[0])
					require.Same(t, &r.Value[0], &iter.Value()[0])
					iter.ReleaseBuf()
					require.Equal(t, r.Value, iter.Value())
					seen++
				}
				require.Equal(t, len(tc.want), seen)
				require.NoError(t, iter.Error())
				require.NoError(t, iter.Close())
			}
		})
	}
	ch := make(chan common.DataAndRanges, 1)
	require.NoError(t, e.LoadIngestData(context.Background(), ch))
	loaded := <-ch
	require.Same(t, e, loaded.Data)
	require.Equal(t, []common.Range{{Start: start, End: end}}, loaded.SortedRanges)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	iter := e.NewIter(ctx, nil, nil, nil)
	require.False(t, iter.First())
	require.ErrorIs(t, iter.Error(), context.Canceled)
	require.NoError(t, iter.Close())
	require.ErrorIs(t, e.LoadIngestData(ctx, make(chan common.DataAndRanges)), context.Canceled)
}

func TestSPFreshMemoryImportOwnershipAndCompletion(t *testing.T) {
	records := []pkdbspfreshbulk.SortedRunRecord{{Key: []byte("key"), Value: []byte("value")}}
	e := newSPFreshBulkImportEngine(spfreshBulkSortedRun{records: records}, "run", 123)
	e.IncRef()
	e.IncRef()
	e.Finish(3, 0)
	e.Finish(5, 1)
	size, count := e.ImportedStatistics()
	require.Equal(t, int64(8), size)
	require.Equal(t, int64(1), count)
	require.NoError(t, e.Close())
	e.DecRef()
	require.NotNil(t, e.records, "a remaining region job can retry")
	e.DecRef()
	require.Zero(t, e.refs.Load())
	require.Nil(t, e.records)
	require.Nil(t, e.splitKeys)
}

func TestSPFreshMemoryImportMissingCompletionBlocksReady(t *testing.T) {
	store, run := newSPFreshBulkFinalManifestFixture(t)
	withSPFreshStagingEngines(t, run, func(staging *spfreshBulkStaging) {
		run.staging = staging
		staging.importEngine = func(context.Context, *spfreshBulkImportEngine) error { return nil }
		err := proveAndStageSPFreshTestBuild(context.Background(), store, run, staging.append)
		require.ErrorContains(t, err, "import incomplete")
		require.Nil(t, run.finalManifest)
		require.False(t, run.importCompleted)
		require.Error(t, run.checkReadyForPublic())
	})
}
