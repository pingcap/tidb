// Copyright 2017 PingCAP, Inc.
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

package statistics

import (
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

// extractSampleItemsDatums is for test purpose only to extract Datum slice
// from SampleItem slice.
func extractSampleItemsDatums(items []*SampleItem) []types.Datum {
	datums := make([]types.Datum, len(items))
	for i, item := range items {
		datums[i] = item.Value
	}
	return datums
}

func buildFMSketch(sc *stmtctx.StatementContext, values []types.Datum, maxSize int) (*FMSketch, int64, error) {
	s := NewFMSketch(maxSize)
	for _, value := range values {
		err := s.InsertValue(sc, value)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
	}
	return s, s.NDV(), nil
}

func SubTestSketch() func(*testing.T) {
	return func(t *testing.T) {
		s := createTestStatisticsSamples(t)
		sc := stmtctx.NewStmtCtxWithTimeZone(time.Local)
		maxSize := 1000
		sampleSketch, ndv, err := buildFMSketch(sc, extractSampleItemsDatums(s.samples), maxSize)
		require.NoError(t, err)
		require.Equal(t, int64(6232), ndv)

		rcSketch, ndv, err := buildFMSketch(sc, s.rc.(*recordSet).data, maxSize)
		require.NoError(t, err)
		require.Equal(t, int64(73344), ndv)

		pkSketch, ndv, err := buildFMSketch(sc, s.pk.(*recordSet).data, maxSize)
		require.NoError(t, err)
		require.Equal(t, int64(100480), ndv)

		sampleSketch.MergeFMSketch(pkSketch)
		sampleSketch.MergeFMSketch(rcSketch)
		require.Equal(t, int64(100480), sampleSketch.NDV())

		maxSize = 2
		sketch := NewFMSketch(maxSize)
		sketch.insertHashValue(1)
		sketch.insertHashValue(2)
		require.Equal(t, maxSize, len(sketch.hashset))
		sketch.insertHashValue(4)
		require.LessOrEqual(t, maxSize, len(sketch.hashset))
	}
}

func SubTestSketchProtoConversion() func(*testing.T) {
	return func(t *testing.T) {
		s := createTestStatisticsSamples(t)
		sc := stmtctx.NewStmtCtxWithTimeZone(time.Local)
		maxSize := 1000
		sampleSketch, ndv, err := buildFMSketch(sc, extractSampleItemsDatums(s.samples), maxSize)
		require.NoError(t, err)
		require.Equal(t, int64(6232), ndv)
		p := FMSketchToProto(sampleSketch)
		f := FMSketchFromProto(p)
		require.Equal(t, f.mask, sampleSketch.mask)
		require.Equal(t, len(f.hashset), len(sampleSketch.hashset))
		for key := range sampleSketch.hashset {
			_, ok := f.hashset[key]
			require.True(t, ok)
		}
	}
}

func SubTestFMSketchCoding() func(*testing.T) {
	return func(t *testing.T) {
		s := createTestStatisticsSamples(t)
		sc := stmtctx.NewStmtCtxWithTimeZone(time.Local)
		maxSize := 1000
		sampleSketch, ndv, err := buildFMSketch(sc, extractSampleItemsDatums(s.samples), maxSize)
		require.NoError(t, err)
		require.Equal(t, int64(6232), ndv)
		bytes, err := EncodeFMSketch(sampleSketch)
		require.NoError(t, err)
		fmsketch, err := DecodeFMSketch(bytes)
		require.NoError(t, err)
		require.Equal(t, fmsketch.NDV(), sampleSketch.NDV())

		rcSketch, ndv, err := buildFMSketch(sc, s.rc.(*recordSet).data, maxSize)
		require.NoError(t, err)
		require.Equal(t, int64(73344), ndv)
		bytes, err = EncodeFMSketch(rcSketch)
		require.NoError(t, err)
		fmsketch, err = DecodeFMSketch(bytes)
		require.NoError(t, err)
		require.Equal(t, fmsketch.NDV(), rcSketch.NDV())

		pkSketch, ndv, err := buildFMSketch(sc, s.pk.(*recordSet).data, maxSize)
		require.NoError(t, err)
		require.Equal(t, int64(100480), ndv)
		bytes, err = EncodeFMSketch(pkSketch)
		require.NoError(t, err)
		fmsketch, err = DecodeFMSketch(bytes)
		require.NoError(t, err)
		require.Equal(t, fmsketch.NDV(), pkSketch.NDV())
	}
}

func SubTestSampledNDV() func(*testing.T) {
	return func(t *testing.T) {
		left, err := newSampledFMSketch(&tipb.FMSketch{Hashset: []uint64{1, 3}, MultiHashset: []uint64{2}},
			ndvSample{rate: .1, rows: 50, samples: 5, nulls: 10})
		require.NoError(t, err)
		right, err := newSampledFMSketch(&tipb.FMSketch{Hashset: []uint64{1, 4}},
			ndvSample{rate: .1, rows: 30, samples: 3, nulls: 10})
		require.NoError(t, err)
		for _, inputs := range [][2]*FMSketch{{left, right}, {right, left}} {
			merged := inputs[0].Copy()
			require.NoError(t, merged.MergeFMSketch(inputs[1]))
			require.Equal(t, map[uint64]struct{}{3: {}, 4: {}}, merged.hashset)
			require.Equal(t, map[uint64]struct{}{1: {}, 2: {}}, merged.repeated)
			require.Equal(t, int64(8), merged.NDV())
			require.Equal(t, int64(20), merged.sample.nulls)
			data, err := EncodeFMSketch(merged)
			require.NoError(t, err)
			require.Error(t, new(tipb.FMSketch).Unmarshal(data))
			restored, err := DecodeFMSketch(data)
			require.NoError(t, err)
			require.Equal(t, merged, restored)
		}
		bare, err := (&tipb.FMSketch{MultiHashset: []uint64{1}}).Marshal()
		require.NoError(t, err)
		_, err = DecodeFMSketch(bare)
		require.ErrorIs(t, err, ErrIncompatibleNDV)

		// Presence distinguishes a sampled empty response from a legacy response.
		zero, rate := int64(0), .1
		req := &tipb.AnalyzeColumnsReq{NdvRate: &rate, ColumnsInfo: []*tipb.ColumnInfo{{}}}
		for _, sampledFirst := range []bool{false, true} {
			merged := NewBernoulliRowSampleCollector(1, 1)
			for i := range 2 {
				pb := &tipb.RowSampleCollector{FmSketch: []*tipb.FMSketch{{}}, NullCounts: []int64{0}, TotalSize: []int64{0}}
				if (i == 0) == sampledFirst {
					pb.NdvSampleCount = &zero
				}
				part := NewBernoulliRowSampleCollector(1, 1)
				require.NoError(t, part.Base().FromProto(pb, memory.NewTracker(-1, -1), req))
				err := merged.MergeCollector(part)
				if i == 0 {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, ErrIncompatibleNDV)
				}
			}
		}

		// Empty sketches still carry a mode and configured rate.
		empty, err := newSampledFMSketch(&tipb.FMSketch{}, ndvSample{rate: .1, rows: 10})
		require.NoError(t, err)
		require.Equal(t, int64(0), empty.NDV())
		require.NoError(t, empty.MergeFMSketch(left))
		require.Equal(t, int64(60), empty.sample.rows)
		require.Equal(t, int64(5), empty.sample.samples)
		require.Equal(t, int64(8), empty.NDV())
		otherRate := right.Copy()
		otherRate.sample.rate = .2
		require.ErrorIs(t, left.Copy().MergeFMSketch(otherRate), ErrIncompatibleNDV)
		for _, pb := range []*tipb.FMSketch{
			{Hashset: []uint64{1}, MultiHashset: []uint64{1}},
			{Mask: 2}, {Mask: 1, Hashset: []uint64{1}},
			{MultiHashset: []uint64{1, 2, 3}},
		} {
			_, err := newSampledFMSketch(pb, *left.sample)
			require.Error(t, err)
		}
		for _, sample := range []ndvSample{{rate: .1, rows: 10, samples: 2, nulls: 10}, {rate: .1, rows: 200, samples: 1, nulls: 100}} {
			sketch, err := newSampledFMSketch(&tipb.FMSketch{}, sample)
			require.NoError(t, err)
			require.Zero(t, sketch.NDV())
		}
	}
}
