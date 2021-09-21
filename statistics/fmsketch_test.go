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

	"github.com/pingcap/parser/mysql"
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

func setUpSuite(t *testing.T) *testStatisticsSuite {
	s := &testStatisticsSuite{}
	s.count = 100000
	samples := make([]*SampleItem, 10000)
	for i := 0; i < len(samples); i++ {
		samples[i] = &SampleItem{}
	}
	start := 1000
	samples[0].Value.SetInt64(0)
	for i := 1; i < start; i++ {
		samples[i].Value.SetInt64(2)
	}
	for i := start; i < len(samples); i++ {
		samples[i].Value.SetInt64(int64(i))
	}
	for i := start; i < len(samples); i += 3 {
		samples[i].Value.SetInt64(samples[i].Value.GetInt64() + 1)
	}
	for i := start; i < len(samples); i += 5 {
		samples[i].Value.SetInt64(samples[i].Value.GetInt64() + 2)
	}
	sc := new(stmtctx.StatementContext)
	samples, err := SortSampleItems(sc, samples)
	require.Nil(t, err)
	s.samples = samples

	rc := &recordSet{
		data:   make([]types.Datum, s.count),
		count:  s.count,
		cursor: 0,
	}
	rc.setFields(mysql.TypeLonglong)
	rc.data[0].SetInt64(0)
	for i := 1; i < start; i++ {
		rc.data[i].SetInt64(2)
	}
	for i := start; i < rc.count; i++ {
		rc.data[i].SetInt64(int64(i))
	}
	for i := start; i < rc.count; i += 3 {
		rc.data[i].SetInt64(rc.data[i].GetInt64() + 1)
	}
	for i := start; i < rc.count; i += 5 {
		rc.data[i].SetInt64(rc.data[i].GetInt64() + 2)
	}
	err = types.SortDatums(sc, rc.data)
	require.Nil(t, err)
	s.rc = rc

	pk := &recordSet{
		data:   make([]types.Datum, s.count),
		count:  s.count,
		cursor: 0,
	}
	pk.setFields(mysql.TypeLonglong)
	for i := 0; i < rc.count; i++ {
		pk.data[i].SetInt64(int64(i))
	}
	s.pk = pk
	return s
}

func TestSketch(t *testing.T) {
	s := setUpSuite(t)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	maxSize := 1000
	sampleSketch, ndv, err := buildFMSketch(sc, extractSampleItemsDatums(s.samples), maxSize)
	require.Nil(t, err)
	require.Equal(t, int64(6232), ndv)

	rcSketch, ndv, err := buildFMSketch(sc, s.rc.(*recordSet).data, maxSize)
	require.Nil(t, err)
	require.Equal(t, int64(73344), ndv)

	pkSketch, ndv, err := buildFMSketch(sc, s.pk.(*recordSet).data, maxSize)
	require.Nil(t, err)
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

func TestSketchProtoConversion(t *testing.T) {
	s := setUpSuite(t)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	maxSize := 1000
	sampleSketch, ndv, err := buildFMSketch(sc, extractSampleItemsDatums(s.samples), maxSize)
	require.Nil(t, err)
	require.Equal(t, int64(6232), ndv)
	p := FMSketchToProto(sampleSketch)
	f := FMSketchFromProto(p)
	require.Equal(t, f.mask, sampleSketch.mask)
	require.Equal(t, len(f.hashset), len(sampleSketch.hashset))
	for val := range sampleSketch.hashset {
		require.True(t, f.hashset[val])
	}
}

func TestFMSketchCoding(t *testing.T) {
	s := setUpSuite(t)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	maxSize := 1000
	sampleSketch, ndv, err := buildFMSketch(sc, extractSampleItemsDatums(s.samples), maxSize)
	require.Nil(t, err)
	require.Equal(t, int64(6232), ndv)
	bytes, err := EncodeFMSketch(sampleSketch)
	require.Nil(t, err)
	fmsketch, err := DecodeFMSketch(bytes)
	require.Nil(t, err)
	require.Equal(t, fmsketch.NDV(), sampleSketch.NDV())

	rcSketch, ndv, err := buildFMSketch(sc, s.rc.(*recordSet).data, maxSize)
	require.Nil(t, err)
	require.Equal(t, int64(73344), ndv)
	bytes, err = EncodeFMSketch(rcSketch)
	require.Nil(t, err)
	fmsketch, err = DecodeFMSketch(bytes)
	require.Nil(t, err)
	require.Equal(t, fmsketch.NDV(), rcSketch.NDV())

	pkSketch, ndv, err := buildFMSketch(sc, s.pk.(*recordSet).data, maxSize)
	require.Nil(t, err)
	require.Equal(t, int64(100480), ndv)
	bytes, err = EncodeFMSketch(pkSketch)
	require.Nil(t, err)
	fmsketch, err = DecodeFMSketch(bytes)
	require.Nil(t, err)
	require.Equal(t, fmsketch.NDV(), pkSketch.NDV())
}
