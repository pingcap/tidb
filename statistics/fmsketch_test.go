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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
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
		sc := &stmtctx.StatementContext{TimeZone: time.Local}
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
		sc := &stmtctx.StatementContext{TimeZone: time.Local}
		maxSize := 1000
		sampleSketch, ndv, err := buildFMSketch(sc, extractSampleItemsDatums(s.samples), maxSize)
		require.NoError(t, err)
		require.Equal(t, int64(6232), ndv)
		p := FMSketchToProto(sampleSketch)
		f := FMSketchFromProto(p)
		require.Equal(t, f.mask, sampleSketch.mask)
		require.Equal(t, len(f.hashset), len(sampleSketch.hashset))
		for val := range sampleSketch.hashset {
			require.True(t, f.hashset[val])
		}
	}
}

func SubTestFMSketchCoding() func(*testing.T) {
	return func(t *testing.T) {
		s := createTestStatisticsSamples(t)
		sc := &stmtctx.StatementContext{TimeZone: time.Local}
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
