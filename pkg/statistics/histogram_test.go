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

package statistics

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestTruncateHistogram(t *testing.T) {
	hist := NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLonglong), 1, 0)
	low, high := types.NewIntDatum(0), types.NewIntDatum(1)
	hist.AppendBucket(&low, &high, 0, 1)
	newHist := hist.TruncateHistogram(1)
	require.True(t, HistogramEqual(hist, newHist, true))
	newHist = hist.TruncateHistogram(0)
	require.Equal(t, 0, newHist.Len())
}

func TestValueToString4InvalidKey(t *testing.T) {
	bytes, err := codec.EncodeKey(time.UTC, nil, types.NewDatum(1), types.NewDatum(0.5))
	require.NoError(t, err)
	// Append invalid flag.
	bytes = append(bytes, 20)
	datum := types.NewDatum(bytes)
	res, err := ValueToString(nil, &datum, 3, nil)
	require.NoError(t, err)
	require.Equal(t, "(1, 0.5, \x14)", res)
}

func genBucket4Merging4Test(lower, upper, ndv, disjointNDV int64) bucket4Merging {
	l := types.NewIntDatum(lower)
	r := types.NewIntDatum(upper)
	return bucket4Merging{
		lower: &l,
		upper: &r,
		Bucket: Bucket{
			NDV:   ndv,
			Count: ndv,
		},
		disjointNDV: disjointNDV,
	}
}

func TestMergeBucketNDV(t *testing.T) {
	type testData struct {
		left   bucket4Merging
		right  bucket4Merging
		result bucket4Merging
	}
	tests := []testData{
		{
			left:   genBucket4Merging4Test(1, 2, 2, 0),
			right:  genBucket4Merging4Test(1, 2, 3, 0),
			result: genBucket4Merging4Test(1, 2, 3, 0),
		},
		{
			left:   genBucket4Merging4Test(1, 3, 2, 0),
			right:  genBucket4Merging4Test(2, 3, 2, 0),
			result: genBucket4Merging4Test(1, 3, 3, 0),
		},
		{
			left:   genBucket4Merging4Test(1, 3, 2, 0),
			right:  genBucket4Merging4Test(4, 6, 2, 2),
			result: genBucket4Merging4Test(1, 3, 2, 4),
		},
		{
			left:   genBucket4Merging4Test(1, 5, 5, 0),
			right:  genBucket4Merging4Test(2, 6, 5, 0),
			result: genBucket4Merging4Test(1, 6, 6, 0),
		},
		{
			left:   genBucket4Merging4Test(3, 5, 3, 0),
			right:  genBucket4Merging4Test(2, 6, 4, 0),
			result: genBucket4Merging4Test(2, 6, 5, 0),
		},
	}
	sc := mock.NewContext().GetSessionVars().StmtCtx
	for i, tt := range tests {
		res, err := mergeBucketNDV(sc, &tt.left, &tt.right)
		require.NoError(t, err, "failed at #%Td case", i)
		require.Equal(t, res.lower.GetInt64(), tt.result.lower.GetInt64(), "failed at #%Td case", i)
		require.Equal(t, res.upper.GetInt64(), tt.result.upper.GetInt64(), "failed at #%Td case", i)
		require.Equal(t, res.NDV, tt.result.NDV, "failed at #%Td case", i)
		require.Equal(t, res.disjointNDV, tt.result.disjointNDV, "failed at #%Td case", i)
	}
}

func TestIndexQueryBytes(t *testing.T) {
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx
	idx := &Index{Info: &model.IndexInfo{Columns: []*model.IndexColumn{{Name: ast.NewCIStr("a"), Offset: 0}}}}
	idx.Histogram = *NewHistogram(0, 15, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0)
	low, err1 := codec.EncodeKey(sc.TimeZone(), nil, types.NewBytesDatum([]byte("0")))
	require.NoError(t, err1)
	high, err2 := codec.EncodeKey(sc.TimeZone(), nil, types.NewBytesDatum([]byte("3")))
	require.NoError(t, err2)
	idx.Bounds.AppendBytes(0, low)
	idx.Bounds.AppendBytes(0, high)
	idx.Buckets = append(idx.Buckets, Bucket{Repeat: 10, Count: 20, NDV: 20})
	idx.PreCalculateScalar()
	idx.CMSketch = nil
	// Count / NDV
	require.Equal(t, idx.QueryBytes(nil, low), uint64(1))
	// Repeat
	require.Equal(t, idx.QueryBytes(nil, high), uint64(10))
}

type histogramInputAndOutput struct {
	inputHist       *Histogram
	inputHistToStr  string
	outputHistToStr string
}

func TestStandardizeForV2AnalyzeIndex(t *testing.T) {
	// 1. prepare expected input and output histograms (in string)
	testData := []*histogramInputAndOutput{
		{
			inputHistToStr: "index:0 ndv:6\n" +
				"num: 0 lower_bound: 111 upper_bound: 111 repeats: 0 ndv: 0\n" +
				"num: 0 lower_bound: 123 upper_bound: 123 repeats: 0 ndv: 0\n" +
				"num: 10 lower_bound: 34567 upper_bound: 5 repeats: 3 ndv: 2",
			outputHistToStr: "index:0 ndv:6\n" +
				"num: 10 lower_bound: 34567 upper_bound: 5 repeats: 3 ndv: 0",
		},
		{
			inputHistToStr: "index:0 ndv:6\n" +
				"num: 0 lower_bound: 111 upper_bound: 111 repeats: 0 ndv: 0\n" +
				"num: 0 lower_bound: 123 upper_bound: 123 repeats: 0 ndv: 0\n" +
				"num: 0 lower_bound: 34567 upper_bound: 5 repeats: 0 ndv: 0",
			outputHistToStr: "index:0 ndv:6",
		},
		{
			inputHistToStr: "index:0 ndv:6\n" +
				"num: 10 lower_bound: 34567 upper_bound: 5 repeats: 3 ndv: 2\n" +
				"num: 0 lower_bound: 876 upper_bound: 876 repeats: 0 ndv: 0\n" +
				"num: 0 lower_bound: 990 upper_bound: 990 repeats: 0 ndv: 0",
			outputHistToStr: "index:0 ndv:6\n" +
				"num: 10 lower_bound: 34567 upper_bound: 5 repeats: 3 ndv: 0",
		},
		{
			inputHistToStr: "index:0 ndv:6\n" +
				"num: 10 lower_bound: 111 upper_bound: 111 repeats: 10 ndv: 1\n" +
				"num: 12 lower_bound: 123 upper_bound: 34567 repeats: 4 ndv: 20\n" +
				"num: 10 lower_bound: 5 upper_bound: 990 repeats: 6 ndv: 2",
			outputHistToStr: "index:0 ndv:6\n" +
				"num: 10 lower_bound: 111 upper_bound: 111 repeats: 10 ndv: 0\n" +
				"num: 12 lower_bound: 123 upper_bound: 34567 repeats: 4 ndv: 0\n" +
				"num: 10 lower_bound: 5 upper_bound: 990 repeats: 6 ndv: 0",
		},
		{
			inputHistToStr: "index:0 ndv:6\n" +
				"num: 0 lower_bound: 111 upper_bound: 111 repeats: 0 ndv: 0\n" +
				"num: 0 lower_bound: 123 upper_bound: 123 repeats: 0 ndv: 0\n" +
				"num: 10 lower_bound: 34567 upper_bound: 34567 repeats: 3 ndv: 2\n" +
				"num: 0 lower_bound: 5 upper_bound: 5 repeats: 0 ndv: 0\n" +
				"num: 0 lower_bound: 876 upper_bound: 876 repeats: 0 ndv: 0\n" +
				"num: 10 lower_bound: 990 upper_bound: 990 repeats: 3 ndv: 2\n" +
				"num: 10 lower_bound: 95 upper_bound: 95 repeats: 3 ndv: 2",
			outputHistToStr: "index:0 ndv:6\n" +
				"num: 10 lower_bound: 34567 upper_bound: 34567 repeats: 3 ndv: 0\n" +
				"num: 10 lower_bound: 990 upper_bound: 990 repeats: 3 ndv: 0\n" +
				"num: 10 lower_bound: 95 upper_bound: 95 repeats: 3 ndv: 0",
		},
		{
			inputHistToStr: "index:0 ndv:6\n" +
				"num: 0 lower_bound: 111 upper_bound: 111 repeats: 0 ndv: 0\n" +
				"num: 0 lower_bound: 123 upper_bound: 123 repeats: 0 ndv: 0\n" +
				"num: 10 lower_bound: 34567 upper_bound: 34567 repeats: 3 ndv: 2\n" +
				"num: 0 lower_bound: 5 upper_bound: 5 repeats: 0 ndv: 0\n" +
				"num: 10 lower_bound: 876 upper_bound: 876 repeats: 3 ndv: 2\n" +
				"num: 10 lower_bound: 990 upper_bound: 990 repeats: 3 ndv: 2\n" +
				"num: 0 lower_bound: 95 upper_bound: 95 repeats: 0 ndv: 0",
			outputHistToStr: "index:0 ndv:6\n" +
				"num: 10 lower_bound: 34567 upper_bound: 34567 repeats: 3 ndv: 0\n" +
				"num: 10 lower_bound: 876 upper_bound: 876 repeats: 3 ndv: 0\n" +
				"num: 10 lower_bound: 990 upper_bound: 990 repeats: 3 ndv: 0",
		},
	}
	// 2. prepare the actual Histogram input
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx
	val0, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(111))
	require.NoError(t, err)
	val1, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(123))
	require.NoError(t, err)
	val2, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(34567))
	require.NoError(t, err)
	val3, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(5))
	require.NoError(t, err)
	val4, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(876))
	require.NoError(t, err)
	val5, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(990))
	require.NoError(t, err)
	val6, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(95))
	require.NoError(t, err)
	val0Bytes := types.NewBytesDatum(val0)
	val1Bytes := types.NewBytesDatum(val1)
	val2Bytes := types.NewBytesDatum(val2)
	val3Bytes := types.NewBytesDatum(val3)
	val4Bytes := types.NewBytesDatum(val4)
	val5Bytes := types.NewBytesDatum(val5)
	val6Bytes := types.NewBytesDatum(val6)
	hist0 := NewHistogram(0, 6, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0)
	hist0.AppendBucketWithNDV(&val0Bytes, &val0Bytes, 0, 0, 0)
	hist0.AppendBucketWithNDV(&val1Bytes, &val1Bytes, 0, 0, 0)
	hist0.AppendBucketWithNDV(&val2Bytes, &val3Bytes, 10, 3, 2)
	testData[0].inputHist = hist0
	hist1 := NewHistogram(0, 6, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0)
	hist1.AppendBucketWithNDV(&val0Bytes, &val0Bytes, 0, 0, 0)
	hist1.AppendBucketWithNDV(&val1Bytes, &val1Bytes, 0, 0, 0)
	hist1.AppendBucketWithNDV(&val2Bytes, &val3Bytes, 0, 0, 0)
	testData[1].inputHist = hist1
	hist2 := NewHistogram(0, 6, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0)
	hist2.AppendBucketWithNDV(&val2Bytes, &val3Bytes, 10, 3, 2)
	hist2.AppendBucketWithNDV(&val4Bytes, &val4Bytes, 10, 0, 0)
	hist2.AppendBucketWithNDV(&val5Bytes, &val5Bytes, 10, 0, 0)
	testData[2].inputHist = hist2
	hist3 := NewHistogram(0, 6, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0)
	hist3.AppendBucketWithNDV(&val0Bytes, &val0Bytes, 10, 10, 1)
	hist3.AppendBucketWithNDV(&val1Bytes, &val2Bytes, 22, 4, 20)
	hist3.AppendBucketWithNDV(&val3Bytes, &val5Bytes, 32, 6, 2)
	testData[3].inputHist = hist3
	hist4 := NewHistogram(0, 6, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0)
	hist4.AppendBucketWithNDV(&val0Bytes, &val0Bytes, 0, 0, 0)
	hist4.AppendBucketWithNDV(&val1Bytes, &val1Bytes, 0, 0, 0)
	hist4.AppendBucketWithNDV(&val2Bytes, &val2Bytes, 10, 3, 2)
	hist4.AppendBucketWithNDV(&val3Bytes, &val3Bytes, 10, 0, 0)
	hist4.AppendBucketWithNDV(&val4Bytes, &val4Bytes, 10, 0, 0)
	hist4.AppendBucketWithNDV(&val5Bytes, &val5Bytes, 20, 3, 2)
	hist4.AppendBucketWithNDV(&val6Bytes, &val6Bytes, 30, 3, 2)
	testData[4].inputHist = hist4
	hist5 := NewHistogram(0, 6, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0)
	hist5.AppendBucketWithNDV(&val0Bytes, &val0Bytes, 0, 0, 0)
	hist5.AppendBucketWithNDV(&val1Bytes, &val1Bytes, 0, 0, 0)
	hist5.AppendBucketWithNDV(&val2Bytes, &val2Bytes, 10, 3, 2)
	hist5.AppendBucketWithNDV(&val3Bytes, &val3Bytes, 10, 0, 0)
	hist5.AppendBucketWithNDV(&val4Bytes, &val4Bytes, 20, 3, 2)
	hist5.AppendBucketWithNDV(&val5Bytes, &val5Bytes, 30, 3, 2)
	hist5.AppendBucketWithNDV(&val6Bytes, &val6Bytes, 30, 0, 0)
	testData[5].inputHist = hist5

	// 3. the actual test
	for i, test := range testData {
		require.Equal(t, test.inputHistToStr, test.inputHist.ToString(1))
		test.inputHist.StandardizeForV2AnalyzeIndex()
		require.Equal(t, test.outputHistToStr, test.inputHist.ToString(1),
			fmt.Sprintf("testData[%d].inputHist:%s", i, test.inputHistToStr))
	}
}

func TestNewPseudoHistogramReuseChunk(t *testing.T) {
	const (
		msgPseudoSameChunk   = "pseudo histograms should share the same Bounds chunk"
		msgRegularDiffChunks = "regular histograms should have different Bounds chunks"
		msgRegularPseudoDiff = "regular and pseudo histograms should have different Bounds chunks"
	)

	// test that NewPseudoHistogram reuses the same global chunk instance
	tp1 := types.NewFieldType(mysql.TypeLonglong)
	tp2 := types.NewFieldType(mysql.TypeVarchar)
	tp3 := types.NewFieldType(mysql.TypeBlob)

	hist1 := NewPseudoHistogram(1, tp1)
	hist2 := NewPseudoHistogram(2, tp2)
	hist3 := NewPseudoHistogram(3, tp3)

	// verify that all pseudo histograms share the same Bounds chunk instance
	require.Same(t, hist1.Bounds, hist2.Bounds, msgPseudoSameChunk)
	require.Same(t, hist1.Bounds, hist3.Bounds, msgPseudoSameChunk)
	require.Same(t, hist2.Bounds, hist3.Bounds, msgPseudoSameChunk)

	// verify that regular histograms do NOT share chunks
	regularHist1 := NewHistogram(1, 0, 0, 0, tp1, 10, 0)
	regularHist2 := NewHistogram(2, 0, 0, 0, tp2, 10, 0)

	require.NotSame(t, regularHist1.Bounds, regularHist2.Bounds, msgRegularDiffChunks)
	require.NotSame(t, regularHist1.Bounds, hist1.Bounds, msgRegularPseudoDiff)
	require.NotSame(t, regularHist2.Bounds, hist1.Bounds, msgRegularPseudoDiff)

	// verify that string type field types are properly handled
	require.Equal(t, mysql.TypeLonglong, hist1.Tp.GetType())
	require.Equal(t, mysql.TypeVarchar, hist2.Tp.GetType())
	require.Equal(t, mysql.TypeBlob, hist3.Tp.GetType())

	// for string types, collation should be set to binary
	require.Equal(t, "binary", hist2.Tp.GetCollate())
	require.Equal(t, "binary", hist3.Tp.GetCollate())
}
