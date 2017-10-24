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
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
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
}

func (r *recordSet) Fields() ([]*ast.ResultField, error) {
	return nil, nil
}

func (r *recordSet) Next() (*ast.Row, error) {
	if r.cursor == r.count {
		return nil, nil
	}
	r.cursor++
	return &ast.Row{Data: []types.Datum{types.NewIntDatum(int64(r.cursor)), r.data[r.cursor-1]}}, nil
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

func (s *testSampleSuite) TestCollectSamplesAndEstimateNDVs(c *C) {
	builder := statistics.SampleBuilder{
		Sc:            mock.NewContext().GetSessionVars().StmtCtx,
		RecordSet:     s.rs,
		ColLen:        1,
		PkID:          1,
		MaxSampleSize: 10000,
		MaxBucketSize: 256,
		MaxSketchSize: 1000,
	}
	s.rs.Close()
	collectors, pkBuilder, err := builder.CollectSamplesAndEstimateNDVs()
	c.Assert(err, IsNil)
	c.Assert(collectors[0].NullCount+collectors[0].Count, Equals, int64(s.count))
	c.Assert(collectors[0].Sketch.NDV(), Equals, int64(6232))
	c.Assert(int64(pkBuilder.Count), Equals, int64(s.count))
	c.Assert(pkBuilder.Hist().NDV, Equals, int64(s.count))
}

func (s *testSampleSuite) TestMergeSampleCollector(c *C) {
	builder := statistics.SampleBuilder{
		Sc:            mock.NewContext().GetSessionVars().StmtCtx,
		RecordSet:     s.rs,
		ColLen:        2,
		PkID:          -1,
		MaxSampleSize: 1000,
		MaxBucketSize: 256,
		MaxSketchSize: 1000,
	}
	s.rs.Close()
	collectors, pkBuilder, err := builder.CollectSamplesAndEstimateNDVs()
	c.Assert(err, IsNil)
	c.Assert(pkBuilder, IsNil)
	c.Assert(len(collectors), Equals, 2)
	collectors[0].IsMerger = true
	collectors[0].MergeSampleCollector(collectors[1])
	c.Assert(collectors[0].Sketch.NDV(), Equals, int64(9280))
	c.Assert(len(collectors[0].Samples), Equals, 1000)
	c.Assert(collectors[0].NullCount, Equals, int64(1000))
	c.Assert(collectors[0].Count, Equals, int64(19000))
}

func (s *testSampleSuite) TestCollectorProtoConversion(c *C) {
	builder := statistics.SampleBuilder{
		Sc:            mock.NewContext().GetSessionVars().StmtCtx,
		RecordSet:     s.rs,
		ColLen:        2,
		PkID:          -1,
		MaxSampleSize: 10000,
		MaxBucketSize: 256,
		MaxSketchSize: 1000,
	}
	s.rs.Close()
	collectors, pkBuilder, err := builder.CollectSamplesAndEstimateNDVs()
	c.Assert(err, IsNil)
	c.Assert(pkBuilder, IsNil)
	for _, collector := range collectors {
		p := statistics.SampleCollectorToProto(collector)
		s := statistics.SampleCollectorFromProto(p)
		c.Assert(collector.Count, Equals, s.Count)
		c.Assert(collector.NullCount, Equals, s.NullCount)
		c.Assert(collector.Sketch.NDV(), Equals, s.Sketch.NDV())
		c.Assert(len(collector.Samples), Equals, len(s.Samples))
	}
}
