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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	goctx "golang.org/x/net/context"
)

var _ = Suite(&testSampleSuite{})

type testSampleSuite struct {
	count int
	rs    ast.RecordSet
}

type recordSet struct {
	data   []types.Datum
	count  int
	cursor int
	fields []*ast.ResultField
}

func (r *recordSet) Fields() []*ast.ResultField {
	return r.fields
}

func (r *recordSet) setFields(tps ...uint8) {
	r.fields = make([]*ast.ResultField, len(tps))
	for i := 0; i < len(tps); i++ {
		rf := new(ast.ResultField)
		rf.Column = new(model.ColumnInfo)
		rf.Column.FieldType = *types.NewFieldType(tps[i])
		r.fields[i] = rf
	}
}

func (r *recordSet) Next(goctx.Context) (types.Row, error) {
	if r.cursor == r.count {
		return nil, nil
	}
	r.cursor++
	return types.DatumRow{types.NewIntDatum(int64(r.cursor)), r.data[r.cursor-1]}, nil
}

func (r *recordSet) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	return nil
}

func (r *recordSet) NewChunk() *chunk.Chunk {
	return nil
}

func (r *recordSet) SupportChunk() bool {
	return false
}

func (r *recordSet) Close() error {
	r.cursor = 0
	return nil
}

func (s *testSampleSuite) SetUpSuite(c *C) {
	s.count = 10000
	rs := &recordSet{
		data:   make([]types.Datum, s.count),
		count:  s.count,
		cursor: 0,
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
}

func (s *testSampleSuite) TestCollectColumnStats(c *C) {
	builder := statistics.SampleBuilder{
		Sc:              mock.NewContext().GetSessionVars().StmtCtx,
		RecordSet:       s.rs,
		ColLen:          1,
		PkID:            1,
		MaxSampleSize:   10000,
		MaxBucketSize:   256,
		MaxFMSketchSize: 1000,
		CMSketchWidth:   2048,
		CMSketchDepth:   8,
	}
	s.rs.Close()
	collectors, pkBuilder, err := builder.CollectColumnStats()
	c.Assert(err, IsNil)
	c.Assert(collectors[0].NullCount+collectors[0].Count, Equals, int64(s.count))
	c.Assert(collectors[0].FMSketch.NDV(), Equals, int64(6232))
	c.Assert(collectors[0].CMSketch.TotalCount(), Equals, uint64(collectors[0].Count))
	c.Assert(int64(pkBuilder.Count), Equals, int64(s.count))
	c.Assert(pkBuilder.Hist().NDV, Equals, int64(s.count))
}

func (s *testSampleSuite) TestMergeSampleCollector(c *C) {
	builder := statistics.SampleBuilder{
		Sc:              mock.NewContext().GetSessionVars().StmtCtx,
		RecordSet:       s.rs,
		ColLen:          2,
		PkID:            -1,
		MaxSampleSize:   1000,
		MaxBucketSize:   256,
		MaxFMSketchSize: 1000,
		CMSketchWidth:   2048,
		CMSketchDepth:   8,
	}
	s.rs.Close()
	collectors, pkBuilder, err := builder.CollectColumnStats()
	c.Assert(err, IsNil)
	c.Assert(pkBuilder, IsNil)
	c.Assert(len(collectors), Equals, 2)
	collectors[0].IsMerger = true
	collectors[0].MergeSampleCollector(collectors[1])
	c.Assert(collectors[0].FMSketch.NDV(), Equals, int64(9280))
	c.Assert(len(collectors[0].Samples), Equals, 1000)
	c.Assert(collectors[0].NullCount, Equals, int64(1000))
	c.Assert(collectors[0].Count, Equals, int64(19000))
	c.Assert(collectors[0].CMSketch.TotalCount(), Equals, uint64(collectors[0].Count))
}

func (s *testSampleSuite) TestCollectorProtoConversion(c *C) {
	builder := statistics.SampleBuilder{
		Sc:              mock.NewContext().GetSessionVars().StmtCtx,
		RecordSet:       s.rs,
		ColLen:          2,
		PkID:            -1,
		MaxSampleSize:   10000,
		MaxBucketSize:   256,
		MaxFMSketchSize: 1000,
		CMSketchWidth:   2048,
		CMSketchDepth:   8,
	}
	s.rs.Close()
	collectors, pkBuilder, err := builder.CollectColumnStats()
	c.Assert(err, IsNil)
	c.Assert(pkBuilder, IsNil)
	for _, collector := range collectors {
		p := statistics.SampleCollectorToProto(collector)
		s := statistics.SampleCollectorFromProto(p)
		c.Assert(collector.Count, Equals, s.Count)
		c.Assert(collector.NullCount, Equals, s.NullCount)
		c.Assert(collector.CMSketch.TotalCount(), Equals, s.CMSketch.TotalCount())
		c.Assert(collector.FMSketch.NDV(), Equals, s.FMSketch.NDV())
		c.Assert(len(collector.Samples), Equals, len(s.Samples))
	}
}
