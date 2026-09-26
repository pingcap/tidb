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
	"github.com/pingcap/tidb/pkg/meta/model"
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
			ndvSample{rows: 50, samples: 5, nulls: 10})
		right := newSampledFMSketch(&tipb.FMSketch{Hashset: []uint64{1, 4}},
			ndvSample{rows: 30, samples: 3, nulls: 10})
		for _, inputs := range [][2]*FMSketch{{left, right}, {right, left}} {
			merged := inputs[0].Copy()
			merged.MergeFMSketch(inputs[1])
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
			data[1] = 2
			_, err = DecodeFMSketch(data)
			require.Error(t, err)
		}

		// Only a response that carries a sample count was sampled, even if empty.
		zero := int64(0)
		for _, sampled := range []bool{false, true} {
			pb := &tipb.RowSampleCollector{FmSketch: []*tipb.FMSketch{{}}, NullCounts: []int64{0}, TotalSize: []int64{0}}
			if sampled {
				pb.NdvSampleCount = &zero
			}
			collector := NewBernoulliRowSampleCollector(1, 1)
			collector.Base().FromProto(pb, memory.NewTracker(-1, -1))
			require.Equal(t, sampled, collector.Base().FMSketches[0].Sampled())
		}
		empty := newSampledFMSketch(&tipb.FMSketch{}, ndvSample{rows: 10})
		require.Equal(t, int64(0), empty.NDV())
		empty.MergeFMSketch(left)
		require.Equal(t, int64(60), empty.sample.rows)
		require.Equal(t, int64(5), empty.sample.samples)
		require.Equal(t, int64(8), empty.NDV())

		// The global merge keeps the scale of each partition's singletons,
		// sqrt(400/100) = 2 and sqrt(900/100) = 3. A value seen in two
		// partitions or in full input counts once.
		sampledAt := func(rows int64, singles, multis []uint64) *FMSketch {
			return newSampledFMSketch(&tipb.FMSketch{Hashset: singles, MultiHashset: multis}, ndvSample{rows: rows, samples: 100})
		}
		full := NewFMSketch(MaxSketchSize)
		full.insertHashValue(1)
		full.insertHashValue(5)
		partitions := []*FMSketch{sampledAt(400, []uint64{1, 2}, []uint64{3}), sampledAt(900, []uint64{2, 4}, nil), full}
		for _, order := range [][]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}} {
			merged := partitions[order[0]].Copy()
			for _, i := range order[1:] {
				merged.MergePartitionFMSketch(partitions[i])
			}
			require.True(t, merged.Sampled())
			require.Equal(t, map[uint64]struct{}{4: {}}, merged.hashset)
			require.Equal(t, int64(4+3), merged.NDV())
		}
		plain := full.Copy()
		plain.MergePartitionFMSketch(full)
		require.False(t, plain.Sampled())
		require.Equal(t, int64(2), plain.NDV())
		// Leveling up drops hashes together with their scales.
		leveled := sampledAt(400, []uint64{1, 2}, nil)
		leveled.maxSize = 2
		leveled.MergePartitionFMSketch(sampledAt(900, []uint64{4}, nil))
		require.Equal(t, map[uint64]float64{2: 2, 4: 3}, leveled.weights)
		require.Equal(t, int64(2*(2+3)), leveled.NDV())
		// Without sampled rows or non-NULL rows, NDV is zero.
		for _, sample := range []ndvSample{{rows: 200}, {rows: 10, samples: 2, nulls: 10}} {
			require.Zero(t, newSampledFMSketch(&tipb.FMSketch{}, sample).NDV())
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
		sketch := newSampledFMSketch(&tipb.FMSketch{Hashset: singles}, ndvSample{rows: 3000, samples: 15})
		collector := &SampleCollector{Samples: distinct, FMSketch: sketch, Count: 3000, Unique: true}
		hist, _, err := BuildHistAndTopN(mock.NewContext(), 256, 0, 1, collector, types.NewFieldType(mysql.TypeLonglong), true, nil)
		require.NoError(t, err)
		require.Equal(t, int64(3000), hist.NDV)
		handle := &model.ColumnInfo{ID: 1, Offset: 0}
		handle.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
		notNull := &model.ColumnInfo{ID: 3, Offset: 2}
		notNull.AddFlag(mysql.NotNullFlag)
		tblInfo := &model.TableInfo{PKIsHandle: true, Columns: []*model.ColumnInfo{handle, {ID: 2, Offset: 1}, notNull}}
		for i, cols := range [][]*model.IndexColumn{
			{{Offset: 1, Length: types.UnspecifiedLength}},
			{{Offset: 2, Length: 4}},
			{{Offset: 1, Length: types.UnspecifiedLength}, {Offset: 2, Length: types.UnspecifiedLength}},
			{{Offset: 0, Length: types.UnspecifiedLength}, {Offset: 2, Length: types.UnspecifiedLength}},
		} {
			tblInfo.Indices = append(tblInfo.Indices, &model.IndexInfo{ID: int64(i + 1), Unique: true, State: model.StatePublic, Columns: cols})
		}
		for _, want := range []struct {
			isIndex bool
			id      int64
			unique  bool
		}{{false, 1, true}, {false, 2, true}, {false, 3, false}, {true, 1, true}, {true, 2, true}, {true, 3, false}, {true, 4, true}} {
			require.Equal(t, want.unique, UniqueByDefinition(tblInfo, want.isIndex, want.id), "%+v", want)
		}
	}
}
