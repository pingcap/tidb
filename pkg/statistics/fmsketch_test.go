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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
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
		left := newSampledFMSketch(&tipb.FMSketch{Hashset: []uint64{1, 3}, MultiHashset: []uint64{2}},
			ndvCounts{rows: 50, samples: 5, nulls: 10})
		right := newSampledFMSketch(&tipb.FMSketch{Hashset: []uint64{1, 4}},
			ndvCounts{rows: 30, samples: 3, nulls: 10})
		for _, inputs := range [][2]*FMSketch{{left, right}, {right, left}} {
			merged := inputs[0].Copy()
			merged.MergeFMSketch(inputs[1])
			require.Equal(t, map[uint64]bool{1: true, 2: true, 3: false, 4: false}, merged.hashset)
			require.Equal(t, int64(8), merged.NDV())
			require.Equal(t, int64(20), merged.ndvCounts.nulls)
			require.Equal(t, int64(16+8*4+24), merged.MemoryUsage())
		}

		// ANALYZE merges the responses into sketches it allocated beforehand.
		// A worker that got no response returns such a sketch unchanged.
		root := NewFMSketch(MaxSketchSize)
		for _, sketch := range []*FMSketch{left, NewFMSketch(MaxSketchSize), right} {
			root.MergeFMSketch(sketch)
		}
		require.Equal(t, int64(80), root.ndvCounts.rows)
		require.Equal(t, int64(8), root.NDV())

		// Only a response that carries a sample count was sampled, even if empty.
		zero := int64(0)
		for _, sampled := range []bool{false, true} {
			pb := &tipb.RowSampleCollector{FmSketch: []*tipb.FMSketch{{}}, NullCounts: []int64{0}, TotalSize: []int64{0}}
			if sampled {
				pb.NdvSampleCount = &zero
			}
			collector := NewBernoulliRowSampleCollector(1, 1)
			collector.Base().FromProto(pb, memory.NewTracker(-1, -1))
			require.Equal(t, sampled, collector.Base().FMSketches[0].ndvCounts != nil)
		}
		empty := newSampledFMSketch(&tipb.FMSketch{}, ndvCounts{rows: 10})
		require.Equal(t, int64(0), empty.NDV())
		empty.MergeFMSketch(left)
		require.Equal(t, int64(60), empty.ndvCounts.rows)
		require.Equal(t, int64(5), empty.ndvCounts.samples)
		require.Equal(t, int64(8), empty.NDV())

		// Without sampled rows or non-NULL rows, NDV is zero.
		for _, counts := range []ndvCounts{{rows: 200}, {rows: 10, samples: 2, nulls: 10}} {
			require.Zero(t, newSampledFMSketch(&tipb.FMSketch{}, counts).NDV())
		}

		// Values the schema keeps unique need no estimate.
		singles := make([]uint64, 0, 14)
		for i := range 14 {
			singles = append(singles, uint64(i+1))
		}
		distinct := make([]*SampleItem, 0, 300)
		for i := range 300 {
			distinct = append(distinct, &SampleItem{Value: types.NewIntDatum(int64(i)), Ordinal: i})
		}
		sketch := newSampledFMSketch(&tipb.FMSketch{Hashset: singles}, ndvCounts{rows: 3000, samples: 15})
		collector := &SampleCollector{Samples: distinct, FMSketch: sketch, Count: 3000, Unique: true}
		hist, _, err := BuildHistAndTopN(mock.NewContext(), 256, 0, 1, collector, types.NewFieldType(mysql.TypeLonglong), true, nil)
		require.NoError(t, err)
		require.Equal(t, int64(3000), hist.NDV)
	}
}
