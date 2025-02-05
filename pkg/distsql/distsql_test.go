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

package distsql

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestSelectNormal(t *testing.T) {
	response, colTypes := createSelectNormal(t, 1, 2, nil, nil)

	// Test Next.
	chk := chunk.New(colTypes, 32, 32)
	numAllRows := 0
	for {
		err := response.Next(context.TODO(), chk)
		require.NoError(t, err)
		numAllRows += chk.NumRows()
		if chk.NumRows() == 0 {
			break
		}
	}
	require.Equal(t, 2, numAllRows)
	require.NoError(t, response.Close())
	require.Equal(t, int64(0), response.memTracker.BytesConsumed())
}

func TestSelectMemTracker(t *testing.T) {
	response, colTypes := createSelectNormal(t, 2, 6, nil, nil)

	// Test Next.
	chk := chunk.New(colTypes, 3, 3)
	err := response.Next(context.TODO(), chk)
	require.NoError(t, err)
	require.True(t, chk.IsFull())
	require.NoError(t, response.Close())
	require.Equal(t, int64(0), response.memTracker.BytesConsumed())
}

func TestSelectNormalChunkSize(t *testing.T) {
	sctx := newMockSessionContext()
	sctx.GetSessionVars().EnableChunkRPC = false
	response, colTypes := createSelectNormal(t, 100, 1000000, nil, sctx)
	testChunkSize(t, response, colTypes)
	require.NoError(t, response.Close())
	require.Equal(t, int64(0), response.memTracker.BytesConsumed())
}

func TestSelectWithRuntimeStats(t *testing.T) {
	planIDs := []int{1, 2, 3}
	response, colTypes := createSelectNormal(t, 1, 2, planIDs, nil)

	require.Equal(t, len(planIDs), len(response.copPlanIDs), "invalid copPlanIDs")
	for i := range planIDs {
		require.Equal(t, planIDs[i], response.copPlanIDs[i], "invalid copPlanIDs")
	}

	// Test Next.
	chk := chunk.New(colTypes, 32, 32)
	numAllRows := 0
	for {
		err := response.Next(context.TODO(), chk)
		require.NoError(t, err)
		numAllRows += chk.NumRows()
		if chk.NumRows() == 0 {
			break
		}
	}
	require.Equal(t, 2, numAllRows)
	require.NoError(t, response.Close())
}

func TestSelectResultRuntimeStats(t *testing.T) {
	stmtStats := execdetails.NewRuntimeStatsColl(nil)
	basic := stmtStats.GetBasicRuntimeStats(1)
	basic.Record(time.Second, 20)
	s1 := &selectResultRuntimeStats{
		backoffSleep:       map[string]time.Duration{"RegionMiss": time.Millisecond},
		totalProcessTime:   time.Second,
		totalWaitTime:      time.Second,
		reqStat:            tikv.NewRegionRequestRuntimeStats(),
		distSQLConcurrency: 15,
	}
	s1.copRespTime.Add(execdetails.Duration(time.Second))
	s1.copRespTime.Add(execdetails.Duration(time.Millisecond))
	s1.procKeys.Add(100)
	s1.procKeys.Add(200)

	s2 := *s1
	stmtStats.RegisterStats(1, s1)
	stmtStats.RegisterStats(1, &s2)
	stats := stmtStats.GetRootStats(1)
	expect := "time:1s, loops:1, cop_task: {num: 4, max: 1s, min: 1ms, avg: 500.5ms, p95: 1s, max_proc_keys: 200, p95_proc_keys: 200, tot_proc: 2s, tot_wait: 2s, copr_cache_hit_ratio: 0.00, max_distsql_concurrency: 15}, backoff{RegionMiss: 2ms}"
	require.Equal(t, expect, stats.String())
	// Test for idempotence.
	require.Equal(t, expect, stats.String())

	s1.reqStat.RPCStats[tikvrpc.CmdCop] = &tikv.RPCRuntimeStats{
		Count:   1,
		Consume: int64(time.Second),
	}
	s1.reqStat.RecordRPCErrorStats("server_is_busy")
	s1.reqStat.RecordRPCErrorStats("server_is_busy")
	stmtStats.RegisterStats(2, s1)
	stats = stmtStats.GetRootStats(2)
	expect = "cop_task: {num: 2, max: 1s, min: 1ms, avg: 500.5ms, p95: 1s, max_proc_keys: 200, p95_proc_keys: 200, tot_proc: 1s, tot_wait: 1s, copr_cache_hit_ratio: 0.00, max_distsql_concurrency: 15}, rpc_info:{Cop:{num_rpc:1, total_time:1s}, rpc_errors:{server_is_busy:2}}, backoff{RegionMiss: 1ms}"
	require.Equal(t, expect, stats.String())
	// Test for idempotence.
	require.Equal(t, expect, stats.String())

	s1 = &selectResultRuntimeStats{
		backoffSleep:     map[string]time.Duration{"RegionMiss": time.Millisecond},
		totalProcessTime: time.Second,
		totalWaitTime:    time.Second,
		reqStat:          tikv.NewRegionRequestRuntimeStats(),
	}
	s1.copRespTime.Add(execdetails.Duration(time.Second))
	s1.procKeys.Add(100)
	expect = "cop_task: {num: 1, max: 1s, proc_keys: 100, tot_proc: 1s, tot_wait: 1s, copr_cache_hit_ratio: 0.00}, backoff{RegionMiss: 1ms}"
	require.Equal(t, expect, s1.String())
}

func TestAnalyze(t *testing.T) {
	sctx := newMockSessionContext()
	sctx.GetSessionVars().EnableChunkRPC = false
	request, err := (&RequestBuilder{}).SetKeyRanges(nil).
		SetAnalyzeRequest(&tipb.AnalyzeReq{}, kv.RC).
		SetKeepOrder(true).
		Build()
	require.NoError(t, err)

	response, err := Analyze(context.TODO(), sctx.GetClient(), request, tikvstore.DefaultVars, true, sctx.GetDistSQLCtx())
	require.NoError(t, err)

	result, ok := response.(*selectResult)
	require.True(t, ok)

	require.Equal(t, "analyze", result.label)
	require.Equal(t, "internal", result.sqlType)

	bytes, err := response.NextRaw(context.TODO())
	require.NoError(t, err)
	require.Len(t, bytes, 16)

	require.NoError(t, response.Close())
}

func TestChecksum(t *testing.T) {
	sctx := newMockSessionContext()
	sctx.GetSessionVars().EnableChunkRPC = false
	request, err := (&RequestBuilder{}).SetKeyRanges(nil).
		SetChecksumRequest(&tipb.ChecksumRequest{}).
		Build()
	require.NoError(t, err)

	response, err := Checksum(context.TODO(), sctx.GetClient(), request, tikvstore.DefaultVars)
	require.NoError(t, err)

	result, ok := response.(*selectResult)
	require.True(t, ok)
	require.Equal(t, "checksum", result.label)
	require.Equal(t, "general", result.sqlType)

	bytes, err := response.NextRaw(context.TODO())
	require.NoError(t, err)
	require.Len(t, bytes, 16)

	require.NoError(t, response.Close())
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
func (resp *mockResponse) Next(context.Context) (kv.ResultSubset, error) {
	resp.Lock()
	defer resp.Unlock()

	if resp.count >= resp.total {
		return nil, nil
	}
	numRows := min(resp.batch, resp.total-resp.count)
	resp.count += numRows

	var chunks []tipb.Chunk
	if !canUseChunkRPC(resp.ctx.GetDistSQLCtx()) {
		datum := types.NewIntDatum(1)
		bytes := make([]byte, 0, 100)
		bytes, _ = codec.EncodeValue(time.UTC, bytes, datum, datum, datum, datum)
		chunks = make([]tipb.Chunk, numRows)
		for i := range chunks {
			chkData := make([]byte, len(bytes))
			copy(chkData, bytes)
			chunks[i] = tipb.Chunk{RowsData: chkData}
		}
	} else {
		chunks = make([]tipb.Chunk, 0)
		for numRows > 0 {
			rows := min(numRows, 1024)
			numRows -= rows

			colTypes := make([]*types.FieldType, 4)
			for i := 0; i < 4; i++ {
				colTypes[i] = types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP()
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
	if canUseChunkRPC(resp.ctx.GetDistSQLCtx()) {
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

func newMockSessionContext() sessionctx.Context {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = stmtctx.NewStmtCtx()
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	ctx.GetSessionVars().StmtCtx.DiskTracker = disk.NewTracker(-1, -1)

	ctx.Store = &mock.Store{
		Client: &mock.Client{
			MockResponse: &mockResponse{
				ctx:   ctx,
				batch: 1,
				total: 2,
			},
		},
	}
	return ctx
}

func createSelectNormalByBenchmarkTest(batch, totalRows int, ctx sessionctx.Context) (*selectResult, []*types.FieldType) {
	request, _ := (&RequestBuilder{}).SetKeyRanges(nil).
		SetDAGRequest(&tipb.DAGRequest{}).
		SetDesc(false).
		SetKeepOrder(false).
		SetFromSessionVars(DefaultDistSQLContext).
		SetMemTracker(memory.NewTracker(-1, -1)).
		Build()

	// 4 int64 types.
	ftb := types.NewFieldTypeBuilder()
	ftb.SetType(mysql.TypeLonglong).SetFlag(mysql.BinaryFlag).SetFlen(mysql.MaxIntWidth).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin)
	colTypes := []*types.FieldType{
		ftb.BuildP(),
	}
	colTypes = append(colTypes, colTypes[0])
	colTypes = append(colTypes, colTypes[0])
	colTypes = append(colTypes, colTypes[0])

	// Test Next.
	var response SelectResult
	response, _ = Select(context.TODO(), ctx.GetDistSQLCtx(), request, colTypes)

	result, _ := response.(*selectResult)
	resp, _ := result.resp.(*mockResponse)
	resp.total = totalRows
	resp.batch = batch

	return result, colTypes
}

func testChunkSize(t *testing.T, response SelectResult, colTypes []*types.FieldType) {
	chk := chunk.New(colTypes, 32, 32)

	require.NoError(t, response.Next(context.TODO(), chk))
	require.Equal(t, 32, chk.NumRows())

	require.NoError(t, response.Next(context.TODO(), chk))
	require.Equal(t, 32, chk.NumRows())

	chk.SetRequiredRows(1, 32)
	require.NoError(t, response.Next(context.TODO(), chk))
	require.Equal(t, 1, chk.NumRows())

	chk.SetRequiredRows(2, 32)
	require.NoError(t, response.Next(context.TODO(), chk))
	require.Equal(t, 2, chk.NumRows())

	chk.SetRequiredRows(17, 32)
	require.NoError(t, response.Next(context.TODO(), chk))
	require.Equal(t, 17, chk.NumRows())

	chk.SetRequiredRows(170, 32)
	require.NoError(t, response.Next(context.TODO(), chk))
	require.Equal(t, 32, chk.NumRows())

	chk.SetRequiredRows(32, 32)
	require.NoError(t, response.Next(context.TODO(), chk))
	require.Equal(t, 32, chk.NumRows())

	chk.SetRequiredRows(0, 32)
	require.NoError(t, response.Next(context.TODO(), chk))
	require.Equal(t, 32, chk.NumRows())

	chk.SetRequiredRows(-1, 32)
	require.NoError(t, response.Next(context.TODO(), chk))
	require.Equal(t, 32, chk.NumRows())
}

func createSelectNormal(t *testing.T, batch, totalRows int, planIDs []int, sctx sessionctx.Context) (*selectResult, []*types.FieldType) {
	request, err := (&RequestBuilder{}).SetKeyRanges(nil).
		SetDAGRequest(&tipb.DAGRequest{}).
		SetDesc(false).
		SetKeepOrder(false).
		SetFromSessionVars(DefaultDistSQLContext).
		SetMemTracker(memory.NewTracker(-1, -1)).
		Build()
	require.NoError(t, err)

	// 4 int64 types.
	ftb := types.NewFieldTypeBuilder()
	ftb.SetType(mysql.TypeLonglong).SetFlag(mysql.BinaryFlag).SetFlen(mysql.MaxIntWidth).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin)
	colTypes := []*types.FieldType{
		ftb.BuildP(),
	}
	colTypes = append(colTypes, colTypes[0])
	colTypes = append(colTypes, colTypes[0])
	colTypes = append(colTypes, colTypes[0])

	if sctx == nil {
		sctx = newMockSessionContext()
	}

	// Test Next.
	var response SelectResult
	if planIDs == nil {
		response, err = Select(context.TODO(), sctx.GetDistSQLCtx(), request, colTypes)
	} else {
		response, err = SelectWithRuntimeStats(context.TODO(), sctx.GetDistSQLCtx(), request, colTypes, planIDs, 1)
	}

	require.NoError(t, err)
	result, ok := response.(*selectResult)

	require.True(t, ok)
	require.Equal(t, "general", result.sqlType)
	require.Equal(t, "dag", result.label)
	require.Len(t, colTypes, result.rowLen)

	resp, ok := result.resp.(*mockResponse)
	require.True(t, ok)

	resp.total = totalRows
	resp.batch = batch

	return result, colTypes
}
