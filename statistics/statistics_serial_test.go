// Copyright 2021 PingCAP, Inc.
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
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func encodeKey(key types.Datum) types.Datum {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	buf, _ := codec.EncodeKey(sc, nil, key)
	return types.NewBytesDatum(buf)
}

func checkRepeats(t *testing.T, hg *Histogram) {
	for _, bkt := range hg.Buckets {
		require.Greater(t, bkt.Repeat, int64(0))
	}
}

func buildIndex(sctx sessionctx.Context, numBuckets, id int64, records sqlexec.RecordSet) (int64, *Histogram, *CMSketch, error) {
	b := NewSortedBuilder(sctx.GetSessionVars().StmtCtx, numBuckets, id, types.NewFieldType(mysql.TypeBlob), Version1)
	cms := NewCMSketch(8, 2048)
	ctx := context.Background()
	req := records.NewChunk(nil)
	it := chunk.NewIterator4Chunk(req)
	for {
		err := records.Next(ctx, req)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		for row := it.Begin(); row != it.End(); row = it.Next() {
			datums := RowToDatums(row, records.Fields())
			buf, err := codec.EncodeKey(sctx.GetSessionVars().StmtCtx, nil, datums...)
			if err != nil {
				return 0, nil, nil, errors.Trace(err)
			}
			data := types.NewBytesDatum(buf)
			err = b.Iterate(data)
			if err != nil {
				return 0, nil, nil, errors.Trace(err)
			}
			cms.InsertBytes(buf)
		}
	}
	return b.Count, b.Hist(), cms, nil
}

func SubTestBuild() func(*testing.T) {
	return func(t *testing.T) {
		s := createTestStatisticsSamples(t)
		bucketCount := int64(256)
		topNCount := 20
		ctx := mock.NewContext()
		sc := ctx.GetSessionVars().StmtCtx
		sketch, _, err := buildFMSketch(sc, s.rc.(*recordSet).data, 1000)
		require.NoError(t, err)

		collector := &SampleCollector{
			Count:     int64(s.count),
			NullCount: 0,
			Samples:   s.samples,
			FMSketch:  sketch,
		}
		col, err := BuildColumn(ctx, bucketCount, 2, collector, types.NewFieldType(mysql.TypeLonglong))
		require.NoError(t, err)
		checkRepeats(t, col)
		col.PreCalculateScalar()
		require.Equal(t, 226, col.Len())
		count, _ := col.equalRowCount(types.NewIntDatum(1000), false)
		require.Equal(t, 0, int(count))
		count = col.lessRowCount(types.NewIntDatum(1000))
		require.Equal(t, 10000, int(count))
		count = col.lessRowCount(types.NewIntDatum(2000))
		require.Equal(t, 19999, int(count))
		count = col.greaterRowCount(types.NewIntDatum(2000))
		require.Equal(t, 80000, int(count))
		count = col.lessRowCount(types.NewIntDatum(200000000))
		require.Equal(t, 100000, int(count))
		count = col.greaterRowCount(types.NewIntDatum(200000000))
		require.Equal(t, 0.0, count)
		count, _ = col.equalRowCount(types.NewIntDatum(200000000), false)
		require.Equal(t, 0.0, count)
		count = col.BetweenRowCount(types.NewIntDatum(3000), types.NewIntDatum(3500))
		require.Equal(t, 4994, int(count))
		count = col.lessRowCount(types.NewIntDatum(1))
		require.Equal(t, 5, int(count))

		colv2, topnv2, err := BuildHistAndTopN(ctx, int(bucketCount), topNCount, 2, collector, types.NewFieldType(mysql.TypeLonglong), true)
		require.NoError(t, err)
		require.NotNil(t, topnv2.TopN)
		// The most common one's occurrence is 9990, the second most common one's occurrence is 30.
		// The ndv of the histogram is 73344, the total count of it is 90010. 90010/73344 vs 30, it's not a bad estimate.
		expectedTopNCount := []uint64{9990}
		require.Equal(t, len(expectedTopNCount), len(topnv2.TopN))
		for i, meta := range topnv2.TopN {
			require.Equal(t, expectedTopNCount[i], meta.Count)
		}
		require.Equal(t, 251, colv2.Len())
		count = colv2.lessRowCount(types.NewIntDatum(1000))
		require.Equal(t, 328, int(count))
		count = colv2.lessRowCount(types.NewIntDatum(2000))
		require.Equal(t, 10007, int(count))
		count = colv2.greaterRowCount(types.NewIntDatum(2000))
		require.Equal(t, 80001, int(count))
		count = colv2.lessRowCount(types.NewIntDatum(200000000))
		require.Equal(t, 90010, int(count))
		count = colv2.greaterRowCount(types.NewIntDatum(200000000))
		require.Equal(t, 0.0, count)
		count = colv2.BetweenRowCount(types.NewIntDatum(3000), types.NewIntDatum(3500))
		require.Equal(t, 5001, int(count))
		count = colv2.lessRowCount(types.NewIntDatum(1))
		require.Equal(t, 0, int(count))

		builder := SampleBuilder{
			Sc:              mock.NewContext().GetSessionVars().StmtCtx,
			RecordSet:       s.pk,
			ColLen:          1,
			MaxSampleSize:   1000,
			MaxFMSketchSize: 1000,
			Collators:       make([]collate.Collator, 1),
			ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)},
		}
		require.NoError(t, s.pk.Close())
		collectors, _, err := builder.CollectColumnStats()
		require.NoError(t, err)
		require.Equal(t, 1, len(collectors))
		col, err = BuildColumn(mock.NewContext(), 256, 2, collectors[0], types.NewFieldType(mysql.TypeLonglong))
		require.NoError(t, err)
		checkRepeats(t, col)
		require.Equal(t, 250, col.Len())

		tblCount, col, _, err := buildIndex(ctx, bucketCount, 1, s.rc)
		require.NoError(t, err)
		checkRepeats(t, col)
		col.PreCalculateScalar()
		require.Equal(t, 100000, int(tblCount))
		count, _ = col.equalRowCount(encodeKey(types.NewIntDatum(10000)), false)
		require.Equal(t, 1, int(count))
		count = col.lessRowCount(encodeKey(types.NewIntDatum(20000)))
		require.Equal(t, 19999, int(count))
		count = col.BetweenRowCount(encodeKey(types.NewIntDatum(30000)), encodeKey(types.NewIntDatum(35000)))
		require.Equal(t, 4999, int(count))
		count = col.BetweenRowCount(encodeKey(types.MinNotNullDatum()), encodeKey(types.NewIntDatum(0)))
		require.Equal(t, 0, int(count))
		count = col.lessRowCount(encodeKey(types.NewIntDatum(0)))
		require.Equal(t, 0, int(count))

		s.pk.(*recordSet).cursor = 0
		tblCount, col, err = buildPK(ctx, bucketCount, 4, s.pk)
		require.NoError(t, err)
		checkRepeats(t, col)
		col.PreCalculateScalar()
		require.Equal(t, 100000, int(tblCount))
		count, _ = col.equalRowCount(types.NewIntDatum(10000), false)
		require.Equal(t, 1, int(count))
		count = col.lessRowCount(types.NewIntDatum(20000))
		require.Equal(t, 20000, int(count))
		count = col.BetweenRowCount(types.NewIntDatum(30000), types.NewIntDatum(35000))
		require.Equal(t, 5000, int(count))
		count = col.greaterRowCount(types.NewIntDatum(1001))
		require.Equal(t, 98998, int(count))
		count = col.lessRowCount(types.NewIntDatum(99999))
		require.Equal(t, 99999, int(count))

		datum := types.Datum{}
		datum.SetMysqlJSON(json.BinaryJSON{TypeCode: json.TypeCodeLiteral})
		item := &SampleItem{Value: datum}
		collector = &SampleCollector{
			Count:     1,
			NullCount: 0,
			Samples:   []*SampleItem{item},
			FMSketch:  sketch,
		}
		col, err = BuildColumn(ctx, bucketCount, 2, collector, types.NewFieldType(mysql.TypeJSON))
		require.NoError(t, err)
		require.Equal(t, 1, col.Len())
		require.Equal(t, col.GetUpper(0), col.GetLower(0))
	}
}

func SubTestHistogramProtoConversion() func(*testing.T) {
	return func(t *testing.T) {
		s := createTestStatisticsSamples(t)
		ctx := mock.NewContext()
		require.NoError(t, s.rc.Close())
		tblCount, col, _, err := buildIndex(ctx, 256, 1, s.rc)
		require.NoError(t, err)
		require.Equal(t, 100000, int(tblCount))

		p := HistogramToProto(col)
		h := HistogramFromProto(p)
		require.True(t, HistogramEqual(col, h, true))
	}
}
