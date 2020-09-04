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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cznic/mathutil"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-tipb"
)

func (s *testSuite) createSelectNormal(batch, totalRows int, c *C, planIDs []int) (*selectResult, []*types.FieldType) {
	request, err := (&RequestBuilder{}).SetKeyRanges(nil).
		SetDAGRequest(&tipb.DAGRequest{}).
		SetDesc(false).
		SetKeepOrder(false).
		SetFromSessionVars(variable.NewSessionVars()).
		SetMemTracker(memory.NewTracker(-1, -1)).
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

	// Test Next.
	var response SelectResult
	if planIDs == nil {
		response, err = Select(context.TODO(), s.sctx, request, colTypes, statistics.NewQueryFeedback(0, nil, 0, false))
	} else {
		response, err = SelectWithRuntimeStats(context.TODO(), s.sctx, request, colTypes, statistics.NewQueryFeedback(0, nil, 0, false), planIDs, 1)
	}

	c.Assert(err, IsNil)
	result, ok := response.(*selectResult)
	c.Assert(ok, IsTrue)
	c.Assert(result.label, Equals, "dag")
	c.Assert(result.sqlType, Equals, "general")
	c.Assert(result.rowLen, Equals, len(colTypes))

	resp, ok := result.resp.(*mockResponse)
	c.Assert(ok, IsTrue)
	resp.total = totalRows
	resp.batch = batch

	return result, colTypes
}

func (s *testSuite) TestSelectNormal(c *C) {
	response, colTypes := s.createSelectNormal(1, 2, c, nil)
	response.Fetch(context.TODO())

	// Test Next.
	chk := chunk.New(colTypes, 32, 32)
	numAllRows := 0
	for {
		err := response.Next(context.TODO(), chk)
		c.Assert(err, IsNil)
		numAllRows += chk.NumRows()
		if chk.NumRows() == 0 {
			break
		}
	}
	c.Assert(numAllRows, Equals, 2)
	err := response.Close()
	c.Assert(err, IsNil)
	c.Assert(response.memTracker.BytesConsumed(), Equals, int64(0))
}

func (s *testSuite) TestSelectMemTracker(c *C) {
	response, colTypes := s.createSelectNormal(2, 6, c, nil)
	response.Fetch(context.TODO())

	// Test Next.
	chk := chunk.New(colTypes, 3, 3)
	err := response.Next(context.TODO(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.IsFull(), Equals, true)
	err = response.Close()
	c.Assert(err, IsNil)
	c.Assert(response.memTracker.BytesConsumed(), Equals, int64(0))
}

func (s *testSuite) TestSelectNormalChunkSize(c *C) {
	s.sctx.GetSessionVars().EnableChunkRPC = false
	response, colTypes := s.createSelectNormal(100, 1000000, c, nil)
	response.Fetch(context.TODO())
	s.testChunkSize(response, colTypes, c)
	c.Assert(response.Close(), IsNil)
	c.Assert(response.memTracker.BytesConsumed(), Equals, int64(0))
}

func (s *testSuite) TestSelectWithRuntimeStats(c *C) {
	planIDs := []int{1, 2, 3}
	response, colTypes := s.createSelectNormal(1, 2, c, planIDs)
	if len(response.copPlanIDs) != len(planIDs) {
		c.Fatal("invalid copPlanIDs")
	}
	for i := range planIDs {
		if response.copPlanIDs[i] != planIDs[i] {
			c.Fatal("invalid copPlanIDs")
		}
	}

	response.Fetch(context.TODO())

	// Test Next.
	chk := chunk.New(colTypes, 32, 32)
	numAllRows := 0
	for {
		err := response.Next(context.TODO(), chk)
		c.Assert(err, IsNil)
		numAllRows += chk.NumRows()
		if chk.NumRows() == 0 {
			break
		}
	}
	c.Assert(numAllRows, Equals, 2)
	err := response.Close()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestSelectResultRuntimeStats(c *C) {
	basic := &execdetails.BasicRuntimeStats{}
	basic.Record(time.Second, 20)
	s1 := &selectResultRuntimeStats{
		copRespTime:      []time.Duration{time.Second, time.Millisecond},
		procKeys:         []int64{100, 200},
		backoffSleep:     map[string]time.Duration{"RegionMiss": time.Millisecond},
		totalProcessTime: time.Second,
		totalWaitTime:    time.Second,
		rpcStat:          tikv.RegionRequestRuntimeStats{},
	}
	s2 := *s1
	stmtStats := execdetails.NewRuntimeStatsColl()
	stmtStats.RegisterStats(1, s1)
	stmtStats.RegisterStats(1, &s2)
	stats := stmtStats.GetRootStats(1)
	expect := "cop_task: {num: 4, max: 1s, min: 1ms, avg: 500.5ms, p95: 1s, max_proc_keys: 200, p95_proc_keys: 200, tot_proc: 2s, tot_wait: 2s}, backoff{RegionMiss: 2ms}"
	c.Assert(stats.String(), Equals, expect)
}

func (s *testSuite) createSelectStreaming(batch, totalRows int, c *C) (*streamResult, []*types.FieldType) {
	request, err := (&RequestBuilder{}).SetKeyRanges(nil).
		SetDAGRequest(&tipb.DAGRequest{}).
		SetDesc(false).
		SetKeepOrder(false).
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

	response, err := Select(context.TODO(), s.sctx, request, colTypes, statistics.NewQueryFeedback(0, nil, 0, false))
	c.Assert(err, IsNil)
	result, ok := response.(*streamResult)
	c.Assert(ok, IsTrue)
	c.Assert(result.rowLen, Equals, len(colTypes))

	resp, ok := result.resp.(*mockResponse)
	c.Assert(ok, IsTrue)
	resp.total = totalRows
	resp.batch = batch

	return result, colTypes
}

func (s *testSuite) TestSelectStreaming(c *C) {
	response, colTypes := s.createSelectStreaming(1, 2, c)
	response.Fetch(context.TODO())

	// Test Next.
	chk := chunk.New(colTypes, 32, 32)
	numAllRows := 0
	for {
		err := response.Next(context.TODO(), chk)
		c.Assert(err, IsNil)
		numAllRows += chk.NumRows()
		if chk.NumRows() == 0 {
			break
		}
	}
	c.Assert(numAllRows, Equals, 2)
	err := response.Close()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestSelectStreamingWithNextRaw(c *C) {
	response, _ := s.createSelectStreaming(1, 2, c)
	response.Fetch(context.TODO())
	data, err := response.NextRaw(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(data), Equals, 16)
}

func (s *testSuite) TestSelectStreamingChunkSize(c *C) {
	response, colTypes := s.createSelectStreaming(100, 1000000, c)
	response.Fetch(context.TODO())
	s.testChunkSize(response, colTypes, c)
	c.Assert(response.Close(), IsNil)
}

func (s *testSuite) testChunkSize(response SelectResult, colTypes []*types.FieldType, c *C) {
	chk := chunk.New(colTypes, 32, 32)

	err := response.Next(context.TODO(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows(), Equals, 32)

	err = response.Next(context.TODO(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows(), Equals, 32)

	chk.SetRequiredRows(1, 32)
	err = response.Next(context.TODO(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows(), Equals, 1)

	chk.SetRequiredRows(2, 32)
	err = response.Next(context.TODO(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows(), Equals, 2)

	chk.SetRequiredRows(17, 32)
	err = response.Next(context.TODO(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows(), Equals, 17)

	chk.SetRequiredRows(170, 32)
	err = response.Next(context.TODO(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows(), Equals, 32)

	chk.SetRequiredRows(32, 32)
	err = response.Next(context.TODO(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows(), Equals, 32)

	chk.SetRequiredRows(0, 32)
	err = response.Next(context.TODO(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows(), Equals, 32)

	chk.SetRequiredRows(-1, 32)
	err = response.Next(context.TODO(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows(), Equals, 32)
}

func (s *testSuite) TestAnalyze(c *C) {
	s.sctx.GetSessionVars().EnableChunkRPC = false
	request, err := (&RequestBuilder{}).SetKeyRanges(nil).
		SetAnalyzeRequest(&tipb.AnalyzeReq{}).
		SetKeepOrder(true).
		Build()
	c.Assert(err, IsNil)

	response, err := Analyze(context.TODO(), s.sctx.GetClient(), request, kv.DefaultVars, true)
	c.Assert(err, IsNil)

	result, ok := response.(*selectResult)
	c.Assert(ok, IsTrue)
	c.Assert(result.label, Equals, "analyze")
	c.Assert(result.sqlType, Equals, "internal")

	response.Fetch(context.TODO())

	bytes, err := response.NextRaw(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bytes), Equals, 16)

	err = response.Close()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestChecksum(c *C) {
	s.sctx.GetSessionVars().EnableChunkRPC = false
	request, err := (&RequestBuilder{}).SetKeyRanges(nil).
		SetChecksumRequest(&tipb.ChecksumRequest{}).
		Build()
	c.Assert(err, IsNil)

	response, err := Checksum(context.TODO(), s.sctx.GetClient(), request, kv.DefaultVars)
	c.Assert(err, IsNil)

	result, ok := response.(*selectResult)
	c.Assert(ok, IsTrue)
	c.Assert(result.label, Equals, "checksum")
	c.Assert(result.sqlType, Equals, "general")

	response.Fetch(context.TODO())

	bytes, err := response.NextRaw(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bytes), Equals, 16)

	err = response.Close()
	c.Assert(err, IsNil)
}

// mockResponse implements kv.Response interface.
// Used only for test.
type mockResponse struct {
	count int
	total int
	batch int
	ctx   sessionctx.Context
	sync.Mutex
}

// Close implements kv.Response interface.
func (resp *mockResponse) Close() error {
	resp.Lock()
	defer resp.Unlock()

	resp.count = 0
	return nil
}

// Next implements kv.Response interface.
func (resp *mockResponse) Next(ctx context.Context) (kv.ResultSubset, error) {
	resp.Lock()
	defer resp.Unlock()

	if resp.count >= resp.total {
		return nil, nil
	}
	numRows := mathutil.Min(resp.batch, resp.total-resp.count)
	resp.count += numRows

	var chunks []tipb.Chunk
	if !canUseChunkRPC(resp.ctx) {
		datum := types.NewIntDatum(1)
		bytes := make([]byte, 0, 100)
		bytes, _ = codec.EncodeValue(nil, bytes, datum, datum, datum, datum)
		chunks = make([]tipb.Chunk, numRows)
		for i := range chunks {
			chkData := make([]byte, len(bytes))
			copy(chkData, bytes)
			chunks[i] = tipb.Chunk{RowsData: chkData}
		}
	} else {
		chunks = make([]tipb.Chunk, 0)
		for numRows > 0 {
			rows := mathutil.Min(numRows, 1024)
			numRows -= rows

			colTypes := make([]*types.FieldType, 4)
			for i := 0; i < 4; i++ {
				colTypes[i] = &types.FieldType{Tp: mysql.TypeLonglong}
			}
			chk := chunk.New(colTypes, numRows, numRows)

			for rowOrdinal := 0; rowOrdinal < rows; rowOrdinal++ {
				for colOrdinal := 0; colOrdinal < 4; colOrdinal++ {
					chk.AppendInt64(colOrdinal, 123)
				}
			}

			codec := chunk.NewCodec(colTypes)
			buffer := codec.Encode(chk)
			chunks = append(chunks, tipb.Chunk{RowsData: buffer})
		}
	}

	respPB := &tipb.SelectResponse{
		Chunks:       chunks,
		OutputCounts: []int64{1},
	}
	if canUseChunkRPC(resp.ctx) {
		respPB.EncodeType = tipb.EncodeType_TypeChunk
	} else {
		respPB.EncodeType = tipb.EncodeType_TypeDefault
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

// GetData implements kv.ResultSubset interface.
func (r *mockResultSubset) GetData() []byte { return r.data }

// GetStartKey implements kv.ResultSubset interface.
func (r *mockResultSubset) GetStartKey() kv.Key { return nil }

// MemSize implements kv.ResultSubset interface.
func (r *mockResultSubset) MemSize() int64 { return int64(cap(r.data)) }

// RespTime implements kv.ResultSubset interface.
func (r *mockResultSubset) RespTime() time.Duration { return 0 }

func createSelectNormal(batch, totalRows int, ctx sessionctx.Context) (*selectResult, []*types.FieldType) {
	request, _ := (&RequestBuilder{}).SetKeyRanges(nil).
		SetDAGRequest(&tipb.DAGRequest{}).
		SetDesc(false).
		SetKeepOrder(false).
		SetFromSessionVars(variable.NewSessionVars()).
		SetMemTracker(memory.NewTracker(-1, -1)).
		Build()

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

	// Test Next.
	var response SelectResult
	response, _ = Select(context.TODO(), ctx, request, colTypes, statistics.NewQueryFeedback(0, nil, 0, false))

	result, _ := response.(*selectResult)
	resp, _ := result.resp.(*mockResponse)
	resp.total = totalRows
	resp.batch = batch

	return result, colTypes
}

func BenchmarkSelectResponseChunk_BigResponse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := &testSuite{}
		s.SetUpSuite(nil)
		s.sctx.GetSessionVars().InitChunkSize = 32
		s.sctx.GetSessionVars().MaxChunkSize = 1024
		selectResult, colTypes := createSelectNormal(4000, 20000, s.sctx)
		selectResult.Fetch(context.TODO())
		chk := chunk.NewChunkWithCapacity(colTypes, 1024)
		b.StartTimer()
		for {
			err := selectResult.Next(context.TODO(), chk)
			if err != nil {
				panic(err)
			}
			if chk.NumRows() == 0 {
				break
			}
			chk.Reset()
		}
		s.TearDownSuite(nil)
	}
}

func BenchmarkSelectResponseChunk_SmallResponse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := &testSuite{}
		s.SetUpSuite(nil)
		s.sctx.GetSessionVars().InitChunkSize = 32
		s.sctx.GetSessionVars().MaxChunkSize = 1024
		selectResult, colTypes := createSelectNormal(32, 3200, s.sctx)
		selectResult.Fetch(context.TODO())
		chk := chunk.NewChunkWithCapacity(colTypes, 1024)
		b.StartTimer()
		for {
			err := selectResult.Next(context.TODO(), chk)
			if err != nil {
				panic(err)
			}
			if chk.NumRows() == 0 {
				break
			}
			chk.Reset()
		}
		s.TearDownSuite(nil)
	}
}
