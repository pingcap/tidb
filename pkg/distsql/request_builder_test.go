// Copyright 2018 PingCAP, Inc.
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

package distsql

import (
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain/resourcegroup"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

type handleRange struct {
	start int64
	end   int64
}

func TestTableHandlesToKVRanges(t *testing.T) {
	handles := []kv.Handle{
		kv.IntHandle(0),
		kv.IntHandle(2),
		kv.IntHandle(3),
		kv.IntHandle(4),
		kv.IntHandle(5),
		kv.IntHandle(10),
		kv.IntHandle(11),
		kv.IntHandle(100),
		kv.IntHandle(9223372036854775806),
		kv.IntHandle(9223372036854775807),
	} // Build expected key ranges.
	hrs := make([]*handleRange, 0, len(handles))
	hrs = append(hrs, &handleRange{start: 0, end: 0})
	hrs = append(hrs, &handleRange{start: 2, end: 5})
	hrs = append(hrs, &handleRange{start: 10, end: 11})
	hrs = append(hrs, &handleRange{start: 100, end: 100})
	hrs = append(hrs, &handleRange{start: 9223372036854775806, end: 9223372036854775807})

	// Build key ranges.
	expect := getExpectedRanges(1, hrs)
	actual, hints := TableHandlesToKVRanges(1, handles)

	// Compare key ranges and expected key ranges.
	require.Equal(t, len(expect), len(actual))
	require.Equal(t, hints, []int{1, 4, 2, 1, 2})
	for i := range actual {
		require.Equal(t, expect[i].StartKey, actual[i].StartKey)
		require.Equal(t, expect[i].EndKey, actual[i].EndKey)
	}
}

func TestTablePartitionHandlesToKVRanges(t *testing.T) {
	handles := []kv.Handle{
		// Partition handles in different partitions
		kv.NewPartitionHandle(1, kv.IntHandle(0)),
		kv.NewPartitionHandle(2, kv.IntHandle(2)),
		kv.NewPartitionHandle(2, kv.IntHandle(3)),
		kv.NewPartitionHandle(2, kv.IntHandle(4)),
		kv.NewPartitionHandle(3, kv.IntHandle(5)),
		kv.NewPartitionHandle(1, kv.IntHandle(10)),
		kv.NewPartitionHandle(2, kv.IntHandle(11)),
		kv.NewPartitionHandle(3, kv.IntHandle(100)),
		kv.NewPartitionHandle(1, kv.IntHandle(9223372036854775806)),
		kv.NewPartitionHandle(1, kv.IntHandle(9223372036854775807)),
	}

	// Build expected key ranges.
	hrs := make([]*handleRange, 0, len(handles))
	hrs = append(hrs, &handleRange{start: 0, end: 0})
	hrs = append(hrs, &handleRange{start: 2, end: 4})
	hrs = append(hrs, &handleRange{start: 5, end: 5})
	hrs = append(hrs, &handleRange{start: 10, end: 10})
	hrs = append(hrs, &handleRange{start: 11, end: 11})
	hrs = append(hrs, &handleRange{start: 100, end: 100})
	hrs = append(hrs, &handleRange{start: 9223372036854775806, end: 9223372036854775807})

	expect := append(getExpectedRanges(1, hrs[:1]), getExpectedRanges(2, hrs[1:2])...)
	expect = append(expect, getExpectedRanges(3, hrs[2:3])...)
	expect = append(expect, getExpectedRanges(1, hrs[3:4])...)
	expect = append(expect, getExpectedRanges(2, hrs[4:5])...)
	expect = append(expect, getExpectedRanges(3, hrs[5:6])...)
	expect = append(expect, getExpectedRanges(1, hrs[6:])...)

	// Build actual key ranges.
	actual, hints := TableHandlesToKVRanges(0, handles)

	// Compare key ranges and expected key ranges.
	require.Equal(t, len(expect), len(actual))
	require.Equal(t, hints, []int{1, 3, 1, 1, 1, 1, 2})
	for i := range actual {
		require.Equal(t, expect[i].StartKey, actual[i].StartKey)
		require.Equal(t, expect[i].EndKey, actual[i].EndKey)
	}
}

func TestTableRangesToKVRanges(t *testing.T) {
	ranges := []*ranger.Range{
		{
			LowVal:    []types.Datum{types.NewIntDatum(1)},
			HighVal:   []types.Datum{types.NewIntDatum(2)},
			Collators: collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:      []types.Datum{types.NewIntDatum(2)},
			HighVal:     []types.Datum{types.NewIntDatum(4)},
			LowExclude:  true,
			HighExclude: true,
			Collators:   collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:      []types.Datum{types.NewIntDatum(4)},
			HighVal:     []types.Datum{types.NewIntDatum(19)},
			HighExclude: true,
			Collators:   collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:     []types.Datum{types.NewIntDatum(19)},
			HighVal:    []types.Datum{types.NewIntDatum(32)},
			LowExclude: true,
			Collators:  collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:     []types.Datum{types.NewIntDatum(34)},
			HighVal:    []types.Datum{types.NewIntDatum(34)},
			LowExclude: true,
			Collators:  collate.GetBinaryCollatorSlice(1),
		},
	}

	actual := TableRangesToKVRanges(13, ranges)
	expect := []kv.KeyRange{
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xd, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xd, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xd, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xd, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xd, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xd, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x13},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xd, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x14},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xd, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x21},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xd, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x23},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xd, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x23},
		},
	}
	for i := 0; i < len(expect); i++ {
		require.Equal(t, expect[i], actual[i])
	}
}

func TestIndexRangesToKVRanges(t *testing.T) {
	ranges := []*ranger.Range{
		{
			LowVal:    []types.Datum{types.NewIntDatum(1)},
			HighVal:   []types.Datum{types.NewIntDatum(2)},
			Collators: collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:      []types.Datum{types.NewIntDatum(2)},
			HighVal:     []types.Datum{types.NewIntDatum(4)},
			LowExclude:  true,
			HighExclude: true,
			Collators:   collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:      []types.Datum{types.NewIntDatum(4)},
			HighVal:     []types.Datum{types.NewIntDatum(19)},
			HighExclude: true,
			Collators:   collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:     []types.Datum{types.NewIntDatum(19)},
			HighVal:    []types.Datum{types.NewIntDatum(32)},
			LowExclude: true,
			Collators:  collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:     []types.Datum{types.NewIntDatum(34)},
			HighVal:    []types.Datum{types.NewIntDatum(34)},
			LowExclude: true,
			Collators:  collate.GetBinaryCollatorSlice(1),
		},
	}

	expect := []kv.KeyRange{
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x13},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x14},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x21},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x23},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x23},
		},
	}

	actual, err := IndexRangesToKVRanges(DefaultDistSQLContext, 12, 15, ranges)
	require.NoError(t, err)
	for i := range actual.FirstPartitionRange() {
		require.Equal(t, expect[i], actual.FirstPartitionRange()[i])
	}
}

func TestRequestBuilder1(t *testing.T) {
	ranges := []*ranger.Range{
		{
			LowVal:    []types.Datum{types.NewIntDatum(1)},
			HighVal:   []types.Datum{types.NewIntDatum(2)},
			Collators: collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:      []types.Datum{types.NewIntDatum(2)},
			HighVal:     []types.Datum{types.NewIntDatum(4)},
			LowExclude:  true,
			HighExclude: true,
			Collators:   collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:      []types.Datum{types.NewIntDatum(4)},
			HighVal:     []types.Datum{types.NewIntDatum(19)},
			HighExclude: true,
			Collators:   collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:     []types.Datum{types.NewIntDatum(19)},
			HighVal:    []types.Datum{types.NewIntDatum(32)},
			LowExclude: true,
			Collators:  collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:     []types.Datum{types.NewIntDatum(34)},
			HighVal:    []types.Datum{types.NewIntDatum(34)},
			LowExclude: true,
			Collators:  collate.GetBinaryCollatorSlice(1),
		},
	}

	actual, err := (&RequestBuilder{}).SetHandleRanges(nil, 12, false, ranges).
		SetDAGRequest(&tipb.DAGRequest{}).
		SetDesc(false).
		SetKeepOrder(false).
		SetFromSessionVars(DefaultDistSQLContext).
		Build()
	require.NoError(t, err)
	expect := &kv.Request{
		Tp:      103,
		StartTs: 0x0,
		Data:    []uint8{0x18, 0x0, 0x20, 0x0, 0x40, 0x0, 0x5a, 0x0},
		KeyRanges: kv.NewNonPartitionedKeyRanges([]kv.KeyRange{
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3},
			},
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4},
			},
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x13},
			},
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x14},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x21},
			},
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x23},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x23},
			},
		}),
		Cacheable:         true,
		KeepOrder:         false,
		Desc:              false,
		Concurrency:       variable.DefDistSQLScanConcurrency,
		IsolationLevel:    0,
		Priority:          0,
		NotFillCache:      false,
		ReplicaRead:       kv.ReplicaReadLeader,
		ReadReplicaScope:  kv.GlobalReplicaScope,
		ResourceGroupName: resourcegroup.DefaultResourceGroupName,
	}
	expect.Paging.MinPagingSize = paging.MinPagingSize
	expect.Paging.MaxPagingSize = paging.MaxPagingSize
	actual.ResourceGroupTagger = nil
	require.Equal(t, expect, actual)
}

func TestRequestBuilder2(t *testing.T) {
	ranges := []*ranger.Range{
		{
			LowVal:    []types.Datum{types.NewIntDatum(1)},
			HighVal:   []types.Datum{types.NewIntDatum(2)},
			Collators: collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:      []types.Datum{types.NewIntDatum(2)},
			HighVal:     []types.Datum{types.NewIntDatum(4)},
			LowExclude:  true,
			HighExclude: true,
			Collators:   collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:      []types.Datum{types.NewIntDatum(4)},
			HighVal:     []types.Datum{types.NewIntDatum(19)},
			HighExclude: true,
			Collators:   collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:     []types.Datum{types.NewIntDatum(19)},
			HighVal:    []types.Datum{types.NewIntDatum(32)},
			LowExclude: true,
			Collators:  collate.GetBinaryCollatorSlice(1),
		},
		{
			LowVal:     []types.Datum{types.NewIntDatum(34)},
			HighVal:    []types.Datum{types.NewIntDatum(34)},
			LowExclude: true,
			Collators:  collate.GetBinaryCollatorSlice(1),
		},
	}

	actual, err := (&RequestBuilder{}).SetIndexRanges(DefaultDistSQLContext, 12, 15, ranges).
		SetDAGRequest(&tipb.DAGRequest{}).
		SetDesc(false).
		SetKeepOrder(false).
		SetFromSessionVars(DefaultDistSQLContext).
		Build()
	require.NoError(t, err)
	expect := &kv.Request{
		Tp:      103,
		StartTs: 0x0,
		Data:    []uint8{0x18, 0x0, 0x20, 0x0, 0x40, 0x0, 0x5a, 0x0},
		KeyRanges: kv.NewNonPartitionedKeyRanges([]kv.KeyRange{
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3},
			},
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4},
			},
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x13},
			},
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x14},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x21},
			},
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x23},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x23},
			},
		}),
		Cacheable:         true,
		KeepOrder:         false,
		Desc:              false,
		Concurrency:       variable.DefDistSQLScanConcurrency,
		IsolationLevel:    0,
		Priority:          0,
		NotFillCache:      false,
		ReplicaRead:       kv.ReplicaReadLeader,
		ReadReplicaScope:  kv.GlobalReplicaScope,
		ResourceGroupName: resourcegroup.DefaultResourceGroupName,
	}
	expect.Paging.MinPagingSize = paging.MinPagingSize
	expect.Paging.MaxPagingSize = paging.MaxPagingSize
	actual.ResourceGroupTagger = nil
	require.Equal(t, expect, actual)
}

func TestRequestBuilder3(t *testing.T) {
	handles := []kv.Handle{kv.IntHandle(0), kv.IntHandle(2), kv.IntHandle(3), kv.IntHandle(4),
		kv.IntHandle(5), kv.IntHandle(10), kv.IntHandle(11), kv.IntHandle(100)}

	actual, err := (&RequestBuilder{}).SetTableHandles(15, handles).
		SetDAGRequest(&tipb.DAGRequest{}).
		SetDesc(false).
		SetKeepOrder(false).
		SetFromSessionVars(DefaultDistSQLContext).
		Build()
	require.NoError(t, err)
	expect := &kv.Request{
		Tp:      103,
		StartTs: 0x0,
		Data:    []uint8{0x18, 0x0, 0x20, 0x0, 0x40, 0x0, 0x5a, 0x0},
		KeyRanges: kv.NewNonParitionedKeyRangesWithHint([]kv.KeyRange{
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			},
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6},
			},
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xa},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc},
			},
			{
				StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64},
				EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x65},
			},
		}, []int{1, 4, 2, 1}),
		Cacheable:         true,
		KeepOrder:         false,
		Desc:              false,
		Concurrency:       variable.DefDistSQLScanConcurrency,
		IsolationLevel:    0,
		Priority:          0,
		NotFillCache:      false,
		ReplicaRead:       kv.ReplicaReadLeader,
		ReadReplicaScope:  kv.GlobalReplicaScope,
		ResourceGroupName: resourcegroup.DefaultResourceGroupName,
	}
	expect.Paging.MinPagingSize = paging.MinPagingSize
	expect.Paging.MaxPagingSize = paging.MaxPagingSize
	actual.ResourceGroupTagger = nil
	require.Equal(t, expect, actual)
}

func TestRequestBuilder4(t *testing.T) {
	keyRanges := []kv.KeyRange{
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xa},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x65},
		},
	}

	actual, err := (&RequestBuilder{}).SetKeyRanges(keyRanges).
		SetDAGRequest(&tipb.DAGRequest{}).
		SetDesc(false).
		SetKeepOrder(false).
		SetFromSessionVars(DefaultDistSQLContext).
		Build()
	require.NoError(t, err)
	expect := &kv.Request{
		Tp:                103,
		StartTs:           0x0,
		Data:              []uint8{0x18, 0x0, 0x20, 0x0, 0x40, 0x0, 0x5a, 0x0},
		KeyRanges:         kv.NewNonPartitionedKeyRanges(keyRanges),
		Cacheable:         true,
		KeepOrder:         false,
		Desc:              false,
		Concurrency:       variable.DefDistSQLScanConcurrency,
		IsolationLevel:    0,
		Priority:          0,
		NotFillCache:      false,
		ReplicaRead:       kv.ReplicaReadLeader,
		ReadReplicaScope:  kv.GlobalReplicaScope,
		ResourceGroupName: resourcegroup.DefaultResourceGroupName,
	}
	expect.Paging.MinPagingSize = paging.MinPagingSize
	expect.Paging.MaxPagingSize = paging.MaxPagingSize
	actual.ResourceGroupTagger = nil
	require.Equal(t, expect, actual)
}

func TestRequestBuilder5(t *testing.T) {
	keyRanges := []kv.KeyRange{
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xa},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x65},
		},
	}

	actual, err := (&RequestBuilder{}).SetKeyRanges(keyRanges).
		SetAnalyzeRequest(&tipb.AnalyzeReq{}, kv.RC).
		SetKeepOrder(true).
		SetConcurrency(15).
		Build()
	require.NoError(t, err)
	expect := &kv.Request{
		Tp:               104,
		StartTs:          0x0,
		Data:             []uint8{0x8, 0x0, 0x18, 0x0, 0x20, 0x0},
		KeyRanges:        kv.NewNonPartitionedKeyRanges(keyRanges),
		KeepOrder:        true,
		Desc:             false,
		Concurrency:      15,
		IsolationLevel:   kv.RC,
		Priority:         1,
		NotFillCache:     true,
		ReadReplicaScope: kv.GlobalReplicaScope,
	}
	require.Equal(t, expect, actual)
}

func TestRequestBuilder6(t *testing.T) {
	keyRanges := []kv.KeyRange{
		{
			StartKey: kv.Key{0x00, 0x01},
			EndKey:   kv.Key{0x02, 0x03},
		},
	}
	concurrency := 10
	actual, err := (&RequestBuilder{}).SetKeyRanges(keyRanges).
		SetChecksumRequest(&tipb.ChecksumRequest{}).
		SetConcurrency(concurrency).
		Build()
	require.NoError(t, err)
	expect := &kv.Request{
		Tp:               105,
		StartTs:          0x0,
		Data:             []uint8{0x10, 0x0, 0x18, 0x0},
		KeyRanges:        kv.NewNonPartitionedKeyRanges(keyRanges),
		KeepOrder:        false,
		Desc:             false,
		Concurrency:      concurrency,
		IsolationLevel:   0,
		Priority:         0,
		NotFillCache:     true,
		ReadReplicaScope: kv.GlobalReplicaScope,
	}
	require.Equal(t, expect, actual)
}

func TestRequestBuilder7(t *testing.T) {
	for _, replicaRead := range []struct {
		replicaReadType kv.ReplicaReadType
		src             string
	}{
		{kv.ReplicaReadLeader, "Leader"},
		{kv.ReplicaReadFollower, "Follower"},
		{kv.ReplicaReadMixed, "Mixed"},
	} {
		// copy iterator variable into a new variable, see issue #27779
		replicaRead := replicaRead
		t.Run(replicaRead.src, func(t *testing.T) {
			dctx := NewDistSQLContextForTest()
			dctx.ReplicaReadType = replicaRead.replicaReadType

			concurrency := 10
			actual, err := (&RequestBuilder{}).
				SetFromSessionVars(dctx).
				SetConcurrency(concurrency).
				Build()
			require.NoError(t, err)
			expect := &kv.Request{
				Tp:                0,
				StartTs:           0x0,
				KeepOrder:         false,
				KeyRanges:         kv.NewNonPartitionedKeyRanges(nil),
				Desc:              false,
				Concurrency:       concurrency,
				IsolationLevel:    0,
				Priority:          0,
				NotFillCache:      false,
				ReplicaRead:       replicaRead.replicaReadType,
				ReadReplicaScope:  kv.GlobalReplicaScope,
				ResourceGroupName: resourcegroup.DefaultResourceGroupName,
			}
			expect.Paging.MinPagingSize = paging.MinPagingSize
			expect.Paging.MaxPagingSize = paging.MaxPagingSize
			actual.ResourceGroupTagger = nil
			require.Equal(t, expect, actual)
		})
	}
}

func TestRequestBuilder8(t *testing.T) {
	dctx := NewDistSQLContextForTest()
	dctx.ResourceGroupName = "test"
	actual, err := (&RequestBuilder{}).
		SetFromSessionVars(dctx).
		Build()
	require.NoError(t, err)
	expect := &kv.Request{
		Tp:                0,
		StartTs:           0x0,
		Data:              []uint8(nil),
		KeyRanges:         kv.NewNonPartitionedKeyRanges(nil),
		Concurrency:       variable.DefDistSQLScanConcurrency,
		IsolationLevel:    0,
		Priority:          0,
		MemTracker:        (*memory.Tracker)(nil),
		SchemaVar:         0,
		ReadReplicaScope:  kv.GlobalReplicaScope,
		ResourceGroupName: "test",
	}
	expect.Paging.MinPagingSize = paging.MinPagingSize
	expect.Paging.MaxPagingSize = paging.MaxPagingSize
	actual.ResourceGroupTagger = nil
	require.Equal(t, expect, actual)
}

func TestRequestBuilderTiKVClientReadTimeout(t *testing.T) {
	dctx := NewDistSQLContextForTest()
	dctx.TiKVClientReadTimeout = 100
	actual, err := (&RequestBuilder{}).
		SetFromSessionVars(dctx).
		Build()
	require.NoError(t, err)
	expect := &kv.Request{
		Tp:                    0,
		StartTs:               0x0,
		Data:                  []uint8(nil),
		KeyRanges:             kv.NewNonPartitionedKeyRanges(nil),
		Concurrency:           variable.DefDistSQLScanConcurrency,
		IsolationLevel:        0,
		Priority:              0,
		MemTracker:            (*memory.Tracker)(nil),
		SchemaVar:             0,
		ReadReplicaScope:      kv.GlobalReplicaScope,
		TiKVClientReadTimeout: 100,
		ResourceGroupName:     resourcegroup.DefaultResourceGroupName,
	}
	expect.Paging.MinPagingSize = paging.MinPagingSize
	expect.Paging.MaxPagingSize = paging.MaxPagingSize
	actual.ResourceGroupTagger = nil
	require.Equal(t, expect, actual)
}

func TestTableRangesToKVRangesWithFbs(t *testing.T) {
	ranges := []*ranger.Range{
		{
			LowVal:    []types.Datum{types.NewIntDatum(1)},
			HighVal:   []types.Datum{types.NewIntDatum(4)},
			Collators: collate.GetBinaryCollatorSlice(1),
		},
	}
	actual := TableRangesToKVRanges(0, ranges)
	expect := []kv.KeyRange{
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5},
		},
	}

	for i := 0; i < len(actual); i++ {
		require.Equal(t, expect[i], actual[i])
	}
}

func TestIndexRangesToKVRangesWithFbs(t *testing.T) {
	ranges := []*ranger.Range{
		{
			LowVal:    []types.Datum{types.NewIntDatum(1)},
			HighVal:   []types.Datum{types.NewIntDatum(4)},
			Collators: collate.GetBinaryCollatorSlice(1),
		},
	}
	actual, err := IndexRangesToKVRanges(DefaultDistSQLContext, 0, 0, ranges)
	require.NoError(t, err)
	expect := []kv.KeyRange{
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5},
		},
	}
	for i := 0; i < len(actual.FirstPartitionRange()); i++ {
		require.Equal(t, expect[i], actual.FirstPartitionRange()[i])
	}
}

func TestScanLimitConcurrency(t *testing.T) {
	dctx := NewDistSQLContextForTest()
	for _, tt := range []struct {
		tp          tipb.ExecType
		limit       uint64
		concurrency int
		src         string
	}{
		{tipb.ExecType_TypeTableScan, 1, 1, "TblScan_Def"},
		{tipb.ExecType_TypeIndexScan, 1, 1, "IdxScan_Def"},
		{tipb.ExecType_TypeTableScan, 1000000, dctx.DistSQLConcurrency, "TblScan_SessionVars"},
		{tipb.ExecType_TypeIndexScan, 1000000, dctx.DistSQLConcurrency, "IdxScan_SessionVars"},
	} {
		// copy iterator variable into a new variable, see issue #27779
		tt := tt
		t.Run(tt.src, func(t *testing.T) {
			firstExec := &tipb.Executor{Tp: tt.tp}
			switch tt.tp {
			case tipb.ExecType_TypeTableScan:
				firstExec.TblScan = &tipb.TableScan{}
			case tipb.ExecType_TypeIndexScan:
				firstExec.IdxScan = &tipb.IndexScan{}
			}

			limitExec := &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: &tipb.Limit{Limit: tt.limit}}
			dag := &tipb.DAGRequest{Executors: []*tipb.Executor{firstExec, limitExec}}
			actual, err := (&RequestBuilder{}).
				SetDAGRequest(dag).
				SetFromSessionVars(dctx).
				Build()
			require.NoError(t, err)
			require.Equal(t, tt.concurrency, actual.Concurrency)
			require.Equal(t, actual.LimitSize, tt.limit)
		})
	}
}

func getExpectedRanges(tid int64, hrs []*handleRange) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(hrs))
	for _, hr := range hrs {
		low := codec.EncodeInt(nil, hr.start)
		high := codec.EncodeInt(nil, hr.end)
		high = kv.Key(high).PrefixNext()
		startKey := tablecodec.EncodeRowKey(tid, low)
		endKey := tablecodec.EncodeRowKey(tid, high)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs
}

func TestBuildTableRangeIntHandle(t *testing.T) {
	type Case struct {
		ids []int64
		trs []kv.KeyRange
	}
	low := codec.EncodeInt(nil, math.MinInt64)
	high := kv.Key(codec.EncodeInt(nil, math.MaxInt64)).PrefixNext()
	cases := []Case{
		{ids: []int64{1}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
		}},
		{ids: []int64{1, 2, 3}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
			{StartKey: tablecodec.EncodeRowKey(2, low), EndKey: tablecodec.EncodeRowKey(2, high)},
			{StartKey: tablecodec.EncodeRowKey(3, low), EndKey: tablecodec.EncodeRowKey(3, high)},
		}},
		{ids: []int64{1, 3}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
			{StartKey: tablecodec.EncodeRowKey(3, low), EndKey: tablecodec.EncodeRowKey(3, high)},
		}},
	}
	for _, cs := range cases {
		t.Log(cs)
		tbl := &model.TableInfo{Partition: &model.PartitionInfo{Enable: true}}
		for _, id := range cs.ids {
			tbl.Partition.Definitions = append(tbl.Partition.Definitions,
				model.PartitionDefinition{ID: id})
		}
		ranges, err := BuildTableRanges(tbl)
		require.NoError(t, err)
		require.Equal(t, cs.trs, ranges)
	}

	tbl := &model.TableInfo{ID: 7}
	ranges, err := BuildTableRanges(tbl)
	require.NoError(t, err)
	require.Equal(t, []kv.KeyRange{
		{StartKey: tablecodec.EncodeRowKey(7, low), EndKey: tablecodec.EncodeRowKey(7, high)},
	}, ranges)
}

func TestBuildTableRangeCommonHandle(t *testing.T) {
	type Case struct {
		ids []int64
		trs []kv.KeyRange
	}
	low, errL := codec.EncodeKey(time.UTC, nil, []types.Datum{types.MinNotNullDatum()}...)
	require.NoError(t, errL)
	high, errH := codec.EncodeKey(time.UTC, nil, []types.Datum{types.MaxValueDatum()}...)
	require.NoError(t, errH)
	high = kv.Key(high).PrefixNext()
	cases := []Case{
		{ids: []int64{1}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
		}},
		{ids: []int64{1, 2, 3}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
			{StartKey: tablecodec.EncodeRowKey(2, low), EndKey: tablecodec.EncodeRowKey(2, high)},
			{StartKey: tablecodec.EncodeRowKey(3, low), EndKey: tablecodec.EncodeRowKey(3, high)},
		}},
		{ids: []int64{1, 3}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
			{StartKey: tablecodec.EncodeRowKey(3, low), EndKey: tablecodec.EncodeRowKey(3, high)},
		}},
	}
	for _, cs := range cases {
		t.Log(cs)
		tbl := &model.TableInfo{Partition: &model.PartitionInfo{Enable: true}, IsCommonHandle: true}
		for _, id := range cs.ids {
			tbl.Partition.Definitions = append(tbl.Partition.Definitions,
				model.PartitionDefinition{ID: id})
		}
		ranges, err := BuildTableRanges(tbl)
		require.NoError(t, err)
		require.Equal(t, cs.trs, ranges)
	}

	tbl := &model.TableInfo{ID: 7, IsCommonHandle: true}
	ranges, errR := BuildTableRanges(tbl)
	require.NoError(t, errR)
	require.Equal(t, []kv.KeyRange{
		{StartKey: tablecodec.EncodeRowKey(7, low), EndKey: tablecodec.EncodeRowKey(7, high)},
	}, ranges)
}
