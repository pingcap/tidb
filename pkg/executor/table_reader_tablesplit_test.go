// Copyright 2025 PingCAP, Inc.
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

package executor

import (
	"context"
	"encoding/hex"
	"testing"

	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

type mockTableSplitRangeBuilder struct {
	pids     []int64
	kvRanges [][]kv.KeyRange
}

func (m mockTableSplitRangeBuilder) buildKeyRange(dctx *distsqlctx.DistSQLContext, ranges []*ranger.Range) ([][]kv.KeyRange, error) {
	return m.kvRanges, nil
}

func (m mockTableSplitRangeBuilder) buildKeyRangeSeparately(dctx *distsqlctx.DistSQLContext, ranges []*ranger.Range) ([]int64, [][]kv.KeyRange, error) {
	return m.pids, m.kvRanges, nil
}

func newTableReaderForTableSplitTest(t *testing.T, kvRanges [][]kv.KeyRange, tableSplit *ast.TableSplit) *TableReaderExecutor {
	sctx := mock.NewContext()
	sctx.BindDomain(domain.NewMockDomain())
	return &TableReaderExecutor{
		tableReaderExecutorContext: newTableReaderExecutorContext(sctx),
		dagPB:                      &tipb.DAGRequest{},
		storeType:                  kv.TiFlash,
		kvRangeBuilder: mockTableSplitRangeBuilder{
			pids:     []int64{1, 2},
			kvRanges: kvRanges,
		},
		tableSplit: tableSplit,
	}
}

func makeTableSplit(start, end string) *ast.TableSplit {
	return &ast.TableSplit{
		Start: hex.EncodeToString([]byte(start)),
		End:   hex.EncodeToString([]byte(end)),
	}
}

func TestTableSplitTiFlashSeparateRequests(t *testing.T) {
	kvRanges := [][]kv.KeyRange{
		{
			{StartKey: []byte("a"), EndKey: []byte("g")},
			{StartKey: []byte("h"), EndKey: []byte("n")},
		},
		{
			{StartKey: []byte("p"), EndKey: []byte("z")},
		},
	}
	exec := newTableReaderForTableSplitTest(t, kvRanges, makeTableSplit("c", "t"))

	kvReqs, err := exec.buildKVReqSeparately(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, kvReqs, 2)
	require.Equal(t, []kv.KeyRange{
		{StartKey: []byte("c"), EndKey: []byte("g")},
		{StartKey: []byte("h"), EndKey: []byte("n")},
	}, kvReqs[0].KeyRanges.FirstPartitionRange())
	require.Equal(t, []kv.KeyRange{
		{StartKey: []byte("p"), EndKey: []byte("t")},
	}, kvReqs[1].KeyRanges.FirstPartitionRange())
}

func TestTableSplitTiFlashPartitionTableScan(t *testing.T) {
	kvRanges := [][]kv.KeyRange{
		{
			{StartKey: []byte("a"), EndKey: []byte("g")},
			{StartKey: []byte("h"), EndKey: []byte("n")},
		},
		{
			{StartKey: []byte("p"), EndKey: []byte("z")},
		},
	}
	exec := newTableReaderForTableSplitTest(t, kvRanges, makeTableSplit("c", "t"))

	kvReq, err := exec.buildKVReqForPartitionTableScan(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, kvReq.PartitionIDAndRanges, 2)
	require.Equal(t, []kv.KeyRange{
		{StartKey: []byte("c"), EndKey: []byte("g")},
		{StartKey: []byte("h"), EndKey: []byte("n")},
	}, kvReq.PartitionIDAndRanges[0].KeyRanges)
	require.Equal(t, []kv.KeyRange{
		{StartKey: []byte("p"), EndKey: []byte("t")},
	}, kvReq.PartitionIDAndRanges[1].KeyRanges)
}

// TestTableSplitNoOverlap tests the case where the split range doesn't overlap
// any of the key ranges, resulting in empty ranges.
func TestTableSplitNoOverlap(t *testing.T) {
	kvRanges := [][]kv.KeyRange{
		{
			{StartKey: []byte("a"), EndKey: []byte("c")},
			{StartKey: []byte("d"), EndKey: []byte("f")},
		},
		{
			{StartKey: []byte("g"), EndKey: []byte("i")},
		},
	}
	// Split range "x" to "z" doesn't overlap any of the key ranges above
	exec := newTableReaderForTableSplitTest(t, kvRanges, makeTableSplit("x", "z"))

	// Test buildKVReqSeparately with no overlap
	kvReqs, err := exec.buildKVReqSeparately(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, kvReqs, 2)
	// All ranges should be empty after intersection
	require.Empty(t, kvReqs[0].KeyRanges.FirstPartitionRange())
	require.Empty(t, kvReqs[1].KeyRanges.FirstPartitionRange())

	// Test buildKVReqForPartitionTableScan with no overlap
	exec2 := newTableReaderForTableSplitTest(t, kvRanges, makeTableSplit("x", "z"))
	kvReq, err := exec2.buildKVReqForPartitionTableScan(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, kvReq.PartitionIDAndRanges, 2)
	require.Empty(t, kvReq.PartitionIDAndRanges[0].KeyRanges)
	require.Empty(t, kvReq.PartitionIDAndRanges[1].KeyRanges)
}

// TestTableSplitPartialOverlap tests the case where the split range overlaps
// only some of the partitions.
func TestTableSplitPartialOverlap(t *testing.T) {
	kvRanges := [][]kv.KeyRange{
		{
			{StartKey: []byte("a"), EndKey: []byte("c")},
		},
		{
			{StartKey: []byte("m"), EndKey: []byte("p")},
		},
	}
	// Split range "k" to "z" overlaps only the second partition
	exec := newTableReaderForTableSplitTest(t, kvRanges, makeTableSplit("k", "z"))

	kvReqs, err := exec.buildKVReqSeparately(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, kvReqs, 2)
	// First partition should have empty ranges (no overlap with "k"-"z")
	require.Empty(t, kvReqs[0].KeyRanges.FirstPartitionRange())
	// Second partition should have the intersected range
	require.Equal(t, []kv.KeyRange{
		{StartKey: []byte("m"), EndKey: []byte("p")},
	}, kvReqs[1].KeyRanges.FirstPartitionRange())
}

// makeTableSplitRaw creates a TableSplit with raw hex strings (for testing invalid hex).
func makeTableSplitRaw(start, end string) *ast.TableSplit {
	return &ast.TableSplit{
		Start: start,
		End:   end,
	}
}

// TestTableSplitInvalidHex tests error handling when TableSplit contains invalid hex strings.
func TestTableSplitInvalidHex(t *testing.T) {
	kvRanges := [][]kv.KeyRange{
		{
			{StartKey: []byte("a"), EndKey: []byte("z")},
		},
	}

	// Test invalid start hex
	exec := newTableReaderForTableSplitTest(t, kvRanges, makeTableSplitRaw("zz", "6666"))
	_, err := exec.buildKVReqSeparately(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encoding/hex")

	// Test invalid end hex
	exec2 := newTableReaderForTableSplitTest(t, kvRanges, makeTableSplitRaw("6161", "invalid"))
	_, err = exec2.buildKVReqSeparately(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encoding/hex")

	// Test buildKVReqForPartitionTableScan with invalid hex
	exec3 := newTableReaderForTableSplitTest(t, kvRanges, makeTableSplitRaw("not-hex", "6666"))
	_, err = exec3.buildKVReqForPartitionTableScan(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encoding/hex")
}
