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
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
)

type testSampleSuite struct {
	count int
	rs    sqlexec.RecordSet
}

func TestSampleSerial(t *testing.T) {
	s := createTestSampleSuite()
	t.Run("SubTestCollectColumnStats", SubTestCollectColumnStats(s))
	t.Run("SubTestMergeSampleCollector", SubTestMergeSampleCollector(s))
	t.Run("SubTestCollectorProtoConversion", SubTestCollectorProtoConversion(s))
}

func createTestSampleSuite() *testSampleSuite {
	s := new(testSampleSuite)
	s.count = 10000
	rs := &recordSet{
		data:      make([]types.Datum, s.count),
		count:     s.count,
		cursor:    0,
		firstIsID: true,
	}
	rs.setFields(mysql.TypeLonglong, mysql.TypeLonglong)
	start := 1000 // 1000 values is null
	for i := start; i < rs.count; i++ {
		rs.data[i].SetInt64(int64(i))
	}
	for i := start; i < rs.count; i += 3 {
		rs.data[i].SetInt64(rs.data[i].GetInt64() + 1)
	}
	for i := start; i < rs.count; i += 5 {
		rs.data[i].SetInt64(rs.data[i].GetInt64() + 2)
	}
	s.rs = rs
	return s
}

func SubTestCollectColumnStats(s *testSampleSuite) func(*testing.T) {
	return func(t *testing.T) {
		sc := mock.NewContext().GetSessionVars().StmtCtx
		builder := SampleBuilder{
			Sc:              sc,
			RecordSet:       s.rs,
			ColLen:          1,
			PkBuilder:       NewSortedBuilder(sc, 256, 1, types.NewFieldType(mysql.TypeLonglong), Version2),
			MaxSampleSize:   10000,
			MaxBucketSize:   256,
			MaxFMSketchSize: 1000,
			CMSketchWidth:   2048,
			CMSketchDepth:   8,
			Collators:       make([]collate.Collator, 1),
			ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)},
		}
		require.Nil(t, s.rs.Close())
		collectors, pkBuilder, err := builder.CollectColumnStats()
		require.NoError(t, err)

		require.Equal(t, int64(s.count), collectors[0].NullCount+collectors[0].Count)
		require.Equal(t, int64(6232), collectors[0].FMSketch.NDV())
		require.Equal(t, uint64(collectors[0].Count), collectors[0].CMSketch.TotalCount())
		require.Equal(t, int64(s.count), pkBuilder.Count)
		require.Equal(t, int64(s.count), pkBuilder.Hist().NDV)
	}
}

func SubTestMergeSampleCollector(s *testSampleSuite) func(*testing.T) {
	return func(t *testing.T) {
		builder := SampleBuilder{
			Sc:              mock.NewContext().GetSessionVars().StmtCtx,
			RecordSet:       s.rs,
			ColLen:          2,
			MaxSampleSize:   1000,
			MaxBucketSize:   256,
			MaxFMSketchSize: 1000,
			CMSketchWidth:   2048,
			CMSketchDepth:   8,
			Collators:       make([]collate.Collator, 2),
			ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeLonglong)},
		}
		require.Nil(t, s.rs.Close())
		sc := &stmtctx.StatementContext{TimeZone: time.Local}
		collectors, pkBuilder, err := builder.CollectColumnStats()
		require.NoError(t, err)
		require.Nil(t, pkBuilder)
		require.Len(t, collectors, 2)
		collectors[0].IsMerger = true
		collectors[0].MergeSampleCollector(sc, collectors[1])
		require.Equal(t, int64(9280), collectors[0].FMSketch.NDV())
		require.Len(t, collectors[0].Samples, 1000)
		require.Equal(t, int64(1000), collectors[0].NullCount)
		require.Equal(t, int64(19000), collectors[0].Count)
		require.Equal(t, uint64(collectors[0].Count), collectors[0].CMSketch.TotalCount())
	}
}

func SubTestCollectorProtoConversion(s *testSampleSuite) func(*testing.T) {
	return func(t *testing.T) {
		builder := SampleBuilder{
			Sc:              mock.NewContext().GetSessionVars().StmtCtx,
			RecordSet:       s.rs,
			ColLen:          2,
			MaxSampleSize:   10000,
			MaxBucketSize:   256,
			MaxFMSketchSize: 1000,
			CMSketchWidth:   2048,
			CMSketchDepth:   8,
			Collators:       make([]collate.Collator, 2),
			ColsFieldType:   []*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeLonglong)},
		}
		require.Nil(t, s.rs.Close())
		collectors, pkBuilder, err := builder.CollectColumnStats()
		require.NoError(t, err)
		require.Nil(t, pkBuilder)
		for _, collector := range collectors {
			p := SampleCollectorToProto(collector)
			s := SampleCollectorFromProto(p)
			require.Equal(t, s.Count, collector.Count)
			require.Equal(t, s.NullCount, collector.NullCount)
			require.Equal(t, s.CMSketch.TotalCount(), collector.CMSketch.TotalCount())
			require.Equal(t, s.FMSketch.NDV(), collector.FMSketch.NDV())
			require.Equal(t, s.TotalSize, collector.TotalSize)
			require.Equal(t, len(s.Samples), len(collector.Samples))
		}
	}
}
