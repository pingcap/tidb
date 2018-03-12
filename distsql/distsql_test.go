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
// See the License for the specific language governing permissions and
// limitations under the License.

package distsql

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

func (s *testSuite) TestSelectNormal(c *C) {
	request, err := (&RequestBuilder{}).SetKeyRanges(nil).
		SetDAGRequest(&tipb.DAGRequest{}).
		SetDesc(false).
		SetKeepOrder(false).
		SetPriority(kv.PriorityNormal).
		SetFromSessionVars(variable.NewSessionVars()).
		Build()
	c.Assert(err, IsNil)

	/// 4 int64 types.
	colTypes := []*types.FieldType{
		{
			Tp:      mysql.TypeLonglong,
			Flen:    mysql.MaxIntWidth,
			Decimal: 0,
			Flag:    mysql.BinaryFlag,
			Charset: charset.CharsetBin,
			Collate: charset.CollationBin,
		},
	}
	colTypes = append(colTypes, colTypes[0])
	colTypes = append(colTypes, colTypes[0])
	colTypes = append(colTypes, colTypes[0])

	// Test NextChunk.
	response, err := Select(context.TODO(), s.sctx, request, colTypes, statistics.NewQueryFeedback(0, nil, 0, false))
	c.Assert(err, IsNil)
	result, ok := response.(*selectResult)
	c.Assert(ok, IsTrue)
	c.Assert(result.label, Equals, "dag")
	c.Assert(result.rowLen, Equals, len(colTypes))

	response.Fetch(context.TODO())

	// Test NextChunk.
	chk := chunk.NewChunk(colTypes)
	numAllRows := 0
	for {
		err = response.NextChunk(context.TODO(), chk)
		c.Assert(err, IsNil)
		numAllRows += chk.NumRows()
		if chk.NumRows() == 0 {
			break
		}
	}
	c.Assert(numAllRows, Equals, 2)
	err = response.Close()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestSelectStreaming(c *C) {
	request, err := (&RequestBuilder{}).SetKeyRanges(nil).
		SetDAGRequest(&tipb.DAGRequest{}).
		SetDesc(false).
		SetKeepOrder(false).
		SetPriority(kv.PriorityNormal).
		SetFromSessionVars(variable.NewSessionVars()).
		SetStreaming(true).
		Build()
	c.Assert(err, IsNil)

	/// 4 int64 types.
	colTypes := []*types.FieldType{
		{
			Tp:      mysql.TypeLonglong,
			Flen:    mysql.MaxIntWidth,
			Decimal: 0,
			Flag:    mysql.BinaryFlag,
			Charset: charset.CharsetBin,
			Collate: charset.CollationBin,
		},
	}
	colTypes = append(colTypes, colTypes[0])
	colTypes = append(colTypes, colTypes[0])
	colTypes = append(colTypes, colTypes[0])

	s.sctx.GetSessionVars().EnableStreaming = true

	// Test NextChunk.
	response, err := Select(context.TODO(), s.sctx, request, colTypes, statistics.NewQueryFeedback(0, nil, 0, false))
	c.Assert(err, IsNil)
	result, ok := response.(*streamResult)
	c.Assert(ok, IsTrue)
	c.Assert(result.rowLen, Equals, len(colTypes))

	response.Fetch(context.TODO())

	// Test NextChunk.
	chk := chunk.NewChunk(colTypes)
	numAllRows := 0
	for {
		err = response.NextChunk(context.TODO(), chk)
		c.Assert(err, IsNil)
		numAllRows += chk.NumRows()
		if chk.NumRows() == 0 {
			break
		}
	}
	c.Assert(numAllRows, Equals, 2)
	err = response.Close()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestAnalyze(c *C) {
	request, err := (&RequestBuilder{}).SetKeyRanges(nil).
		SetAnalyzeRequest(&tipb.AnalyzeReq{}).
		SetKeepOrder(true).
		SetPriority(kv.PriorityLow).
		Build()
	c.Assert(err, IsNil)

	response, err := Analyze(context.TODO(), s.sctx.GetClient(), request)
	c.Assert(err, IsNil)

	result, ok := response.(*selectResult)
	c.Assert(ok, IsTrue)
	c.Assert(result.label, Equals, "analyze")

	response.Fetch(context.TODO())

	bytes, err := response.NextRaw(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bytes), Equals, 14)

	err = response.Close()
	c.Assert(err, IsNil)
}

// mockResponse implements kv.Response interface.
// Used only for test.
type mockResponse struct{ count int }

// Close implements kv.Response interface.
func (resp *mockResponse) Close() error {
	resp.count = 0
	return nil
}

// Next implements kv.Response interface.
func (resp *mockResponse) Next(ctx context.Context) (kv.ResultSubset, error) {
	if resp.count == 2 {
		return nil, nil
	}
	defer func() { resp.count++ }()

	datum := types.NewIntDatum(1)
	bytes := make([]byte, 0, 100)
	bytes, _ = codec.EncodeValue(nil, bytes, datum, datum, datum, datum)

	respPB := &tipb.SelectResponse{
		Chunks:       []tipb.Chunk{{RowsData: bytes}},
		OutputCounts: []int64{1},
	}
	respBytes, err := respPB.Marshal()
	if err != nil {
		panic(err)
	}
	return &mockResultSubset{respBytes}, nil
}

// mockResultSubset implements kv.ResultSubset interface.
// Used only for test.
type mockResultSubset struct{ data []byte }

// GetData implements kv.Response interface.
func (r *mockResultSubset) GetData() []byte { return r.data }

// GetStartKey implements kv.Response interface.
func (r *mockResultSubset) GetStartKey() kv.Key { return nil }
