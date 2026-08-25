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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/store/copr"
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
	tikvutil "github.com/tikv/client-go/v2/util"
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
	basic := stmtStats.GetBasicRuntimeStats(1, true)
	basic.Record(time.Second, 20)
	s1 := &selectResultRuntimeStats{
		backoffSleep:       map[string]time.Duration{"RegionMiss": time.Millisecond},
		totalProcessTime:   time.Second,
		totalWaitTime:      time.Second,
		reqStat:            tikv.NewRegionRequestRuntimeStats(),
		distSQLConcurrency: 15,
		fetchRspDuration:   time.Second,
	}
	s1.copRespTime.Add(execdetails.Duration(time.Second))
	s1.copRespTime.Add(execdetails.Duration(time.Millisecond))
	s1.procKeys.Add(100)
	s1.procKeys.Add(200)

	s2 := s1.Clone()
	stmtStats.RegisterStats(1, s1.Clone())
	stmtStats.RegisterStats(1, s2)
	stats := stmtStats.GetRootStats(1)
	expect := "time:1s, open:0s, close:0s, loops:1, cop_task: {num: 4, max: 1s, min: 1ms, avg: 500.5ms, p95: 1s, max_proc_keys: 200, p95_proc_keys: 200, tot_proc: 2s, tot_wait: 2s, copr_cache_hit_ratio: 0.00, max_distsql_concurrency: 15}, fetch_resp_duration: 2s, backoff{RegionMiss: 2ms}"
	require.Equal(t, expect, stats.String())
	// Test for idempotence.
	require.Equal(t, expect, stats.String())

	s1.reqStat.RecordRPCRuntimeStats(tikvrpc.CmdCop, time.Second)
	s1.reqStat.RecordRPCErrorStats("server_is_busy")
	s1.reqStat.RecordRPCErrorStats("server_is_busy")
	stmtStats.RegisterStats(2, s1.Clone())
	stats = stmtStats.GetRootStats(2)
	expect = "cop_task: {num: 2, max: 1s, min: 1ms, avg: 500.5ms, p95: 1s, max_proc_keys: 200, p95_proc_keys: 200, tot_proc: 1s, tot_wait: 1s, copr_cache_hit_ratio: 0.00, max_distsql_concurrency: 15}, fetch_resp_duration: 1s, rpc_info:{Cop:{num_rpc:1, total_time:1s}, rpc_errors:{server_is_busy:2}}, backoff{RegionMiss: 1ms}"
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
	const planID = 42
	original := config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load()
	config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Store(true)
	t.Cleanup(func() {
		config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Store(original)
	})

	dctx := newAnalyzeTestDistSQLContext()
	request, err := (&RequestBuilder{}).SetKeyRanges(nil).
		SetAnalyzeRequest(&tipb.AnalyzeReq{}, kv.RC).
		SetKeepOrder(true).
		Build()
	require.NoError(t, err)
	newResult := func(t *testing.T, dctx *distsqlctx.DistSQLContext, response *analyzeTestResponse) (SelectResult, *analyzeTestClient) {
		t.Helper()
		client := &analyzeTestClient{response: response}
		result, err := Analyze(context.TODO(), client, request, tikvstore.DefaultVars, true, dctx, planID)
		require.NoError(t, err)
		require.NotNil(t, client.option)
		return result, client
	}

	response := &analyzeTestResponse{result: &analyzeTestResultSubset{
		data: []byte("analyze payload!"),
		stats: &copr.CopRuntimeStats{CopExecDetails: execdetails.CopExecDetails{
			ScanDetail: &tikvutil.ScanDetail{
				ProcessedKeys:     13,
				TotalKeys:         17,
				ProcessedKeysSize: 19,
			},
			TimeDetail: tikvutil.TimeDetail{
				ProcessTime: 3 * time.Millisecond,
				WaitTime:    5 * time.Millisecond,
			},
		}},
	}}
	selectResponse, client := newResult(t, dctx, response)
	require.True(t, client.option.EnableCollectExecutionInfo)

	result, ok := selectResponse.(*selectResult)
	require.True(t, ok)

	require.Equal(t, "analyze", result.label)
	require.Equal(t, "internal", result.sqlType)

	bytes, err := selectResponse.NextRaw(context.TODO())
	require.NoError(t, err)
	require.Equal(t, []byte("analyze payload!"), bytes)
	details := dctx.ExecDetails.GetExecDetails()
	require.Equal(t, 1, details.RequestCount)
	require.NotNil(t, details.ScanDetail)
	require.Equal(t, int64(13), details.ScanDetail.ProcessedKeys)
	require.Equal(t, int64(17), details.ScanDetail.TotalKeys)
	require.Equal(t, int64(19), details.ScanDetail.ProcessedKeysSize)
	copStats := dctx.RuntimeStatsColl.GetCopStats(planID)
	require.NotNil(t, copStats)
	require.Contains(t, copStats.String(), "total_process_keys: 13")
	require.Contains(t, copStats.String(), "total_process_keys_size: 19")
	require.Contains(t, copStats.String(), "total_keys: 17")

	require.NoError(t, selectResponse.Close())
	require.Contains(t, dctx.RuntimeStatsColl.GetRootStats(planID).String(), "cop_task: {num: 1")
	scanBytes, ok := dctx.RuntimeStatsColl.GetAnalyzeScanBytes(planID)
	require.True(t, ok)
	require.InDelta(t, float64(19)/13*17, scanBytes, 1e-9)

	t.Run("sums estimates before independent requests are flattened", func(t *testing.T) {
		dctx := newAnalyzeTestDistSQLContext()
		for _, detail := range []*tikvutil.ScanDetail{
			{ProcessedKeys: 1, ProcessedKeysSize: 100, TotalKeys: 10},
			{ProcessedKeys: 9, ProcessedKeysSize: 9, TotalKeys: 9},
		} {
			response := &analyzeTestResponse{result: &analyzeTestResultSubset{
				data:  []byte("analyze payload!"),
				stats: &copr.CopRuntimeStats{CopExecDetails: execdetails.CopExecDetails{ScanDetail: detail}},
			}}
			result, _ := newResult(t, dctx, response)
			_, err = result.NextRaw(context.TODO())
			require.NoError(t, err)
			require.NoError(t, result.Close())
		}

		scanBytes, found := dctx.RuntimeStatsColl.GetAnalyzeScanBytes(planID)
		require.True(t, found)
		require.InDelta(t, 1009, scanBytes, 1e-9)
	})

	t.Run("collection disabled does not record details", func(t *testing.T) {
		config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Store(false)
		t.Cleanup(func() {
			config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Store(true)
		})
		dctx := newAnalyzeTestDistSQLContext()
		response := &analyzeTestResponse{result: &analyzeTestResultSubset{
			data: []byte("analyze payload!"),
			stats: &copr.CopRuntimeStats{CopExecDetails: execdetails.CopExecDetails{
				ScanDetail: &tikvutil.ScanDetail{ProcessedKeys: 1, ProcessedKeysSize: 2, TotalKeys: 3},
			}},
		}}
		result, client := newResult(t, dctx, response)
		require.False(t, client.option.EnableCollectExecutionInfo)
		_, err = result.NextRaw(context.TODO())
		require.NoError(t, err)
		require.NoError(t, result.Close())
		require.Zero(t, dctx.ExecDetails.GetExecDetails().RequestCount)
		require.False(t, dctx.RuntimeStatsColl.ExistsCopStats(planID))
		_, found := dctx.RuntimeStatsColl.GetAnalyzeScanBytes(planID)
		require.False(t, found)
	})

	t.Run("subset details survive a response error", func(t *testing.T) {
		dctx := newAnalyzeTestDistSQLContext()
		responseErr := errors.New("response error")
		response := &analyzeTestResponse{
			result: &analyzeTestResultSubset{
				stats: &copr.CopRuntimeStats{CopExecDetails: execdetails.CopExecDetails{
					ScanDetail: &tikvutil.ScanDetail{ProcessedKeys: 2, ProcessedKeysSize: 6, TotalKeys: 4},
				}},
			},
			err: responseErr,
		}
		result, _ := newResult(t, dctx, response)
		data, err := result.NextRaw(context.TODO())
		require.ErrorIs(t, err, responseErr)
		require.Nil(t, data)
		details := dctx.ExecDetails.GetExecDetails()
		require.Equal(t, 1, details.RequestCount)
		require.Equal(t, int64(2), details.ScanDetail.ProcessedKeys)
		require.NoError(t, result.Close())
		scanBytes, found := dctx.RuntimeStatsColl.GetAnalyzeScanBytes(planID)
		require.True(t, found)
		require.InDelta(t, 12, scanBytes, 1e-9)
	})

	t.Run("close collects unconsumed details", func(t *testing.T) {
		dctx := newAnalyzeTestDistSQLContext()
		response := &analyzeTestResponse{unconsumed: []*copr.CopRuntimeStats{{
			CopExecDetails: execdetails.CopExecDetails{
				ScanDetail: &tikvutil.ScanDetail{ProcessedKeys: 3, ProcessedKeysSize: 12, TotalKeys: 5},
			},
		}}}
		result, _ := newResult(t, dctx, response)
		require.NoError(t, result.Close())
		details := dctx.ExecDetails.GetExecDetails()
		require.Equal(t, 1, details.RequestCount)
		require.Equal(t, int64(3), details.ScanDetail.ProcessedKeys)
		scanBytes, found := dctx.RuntimeStatsColl.GetAnalyzeScanBytes(planID)
		require.True(t, found)
		require.InDelta(t, 20, scanBytes, 1e-9)
	})
}

func newAnalyzeTestDistSQLContext() *distsqlctx.DistSQLContext {
	sctx := newMockSessionContext()
	sctx.GetSessionVars().EnableChunkRPC = false
	dctx := sctx.GetDistSQLCtx()
	dctx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
	return dctx
}

type analyzeTestClient struct {
	kv.RequestTypeSupportedChecker
	response kv.Response
	option   *kv.ClientSendOption
}

func (c *analyzeTestClient) Send(_ context.Context, _ *kv.Request, _ any, option *kv.ClientSendOption) kv.Response {
	c.option = option
	return c.response
}

type analyzeTestResponse struct {
	result     kv.ResultSubset
	err        error
	unconsumed []*copr.CopRuntimeStats
	done       bool
}

func (r *analyzeTestResponse) Next(context.Context) (kv.ResultSubset, error) {
	if r.done {
		return nil, nil
	}
	r.done = true
	return r.result, r.err
}

func (*analyzeTestResponse) Close() error { return nil }

func (r *analyzeTestResponse) CollectUnconsumedCopRuntimeStats() []*copr.CopRuntimeStats {
	return r.unconsumed
}

type analyzeTestResultSubset struct {
	data  []byte
	stats *copr.CopRuntimeStats
}

func (r *analyzeTestResultSubset) GetData() []byte { return r.data }

func (*analyzeTestResultSubset) GetStartKey() kv.Key { return nil }

func (r *analyzeTestResultSubset) MemSize() int64 { return int64(cap(r.data)) }

func (*analyzeTestResultSubset) RespTime() time.Duration { return 0 }

func (r *analyzeTestResultSubset) GetCopRuntimeStats() *copr.CopRuntimeStats { return r.stats }

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
	// intermediateOutputs is used to mock the intermediate output from coprocessor.
	intermediateOutputs [][]*tipb.IntermediateOutput
	closed              bool
	sync.Mutex
}

// Close implements kv.Response interface.
func (resp *mockResponse) Close() error {
	resp.Lock()
	defer resp.Unlock()

	resp.closed = true
	resp.count = 0
	return nil
}

// Next implements kv.Response interface.
func (resp *mockResponse) Next(context.Context) (kv.ResultSubset, error) {
	resp.Lock()
	defer resp.Unlock()

	if resp.closed {
		panic("closed")
	}

	var intermediateOutputs []*tipb.IntermediateOutput
	if len(resp.intermediateOutputs) > 0 {
		intermediateOutputs = resp.intermediateOutputs[0]
		resp.intermediateOutputs = resp.intermediateOutputs[1:]
	}

	if resp.count >= resp.total && intermediateOutputs == nil {
		return nil, nil
	}
	numRows := max(0, min(resp.batch, resp.total-resp.count))
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
			for i := range 4 {
				colTypes[i] = types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP()
			}
			chk := chunk.New(colTypes, numRows, numRows)

			for range rows {
				for colOrdinal := range 4 {
					chk.AppendInt64(colOrdinal, 123)
				}
			}

			codec := chunk.NewCodec(colTypes)
			buffer := codec.Encode(chk)
			chunks = append(chunks, tipb.Chunk{RowsData: buffer})
		}
	}

	respPB := &tipb.SelectResponse{
		Chunks:              chunks,
		OutputCounts:        []int64{1},
		IntermediateOutputs: intermediateOutputs,
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

func mockChunk(loc *time.Location, encodeType tipb.EncodeType, colTypes []*types.FieldType, rows [][]any) tipb.Chunk {
	var chk *chunk.Chunk
	dsRows := [][]types.Datum(nil)
	switch encodeType {
	case tipb.EncodeType_TypeDefault:
		dsRows = make([][]types.Datum, 0, len(rows))
	case tipb.EncodeType_TypeChunk:
		chk = chunk.New(colTypes, len(rows), len(rows))
	default:
		panic("unsupported encode type: " + encodeType.String())
	}

	for _, row := range rows {
		if len(row) != len(colTypes) {
			panic("row length not match column length")
		}
		var ds []types.Datum
		if dsRows != nil {
			ds = make([]types.Datum, len(row))
		}
		for i, val := range row {
			switch v := val.(type) {
			case int:
				if chk != nil {
					chk.AppendInt64(i, int64(v))
				} else {
					ds[i] = types.NewIntDatum(int64(v))
				}
			case int64:
				if chk != nil {
					chk.AppendInt64(i, v)
				} else {
					ds[i] = types.NewIntDatum(v)
				}
			case uint64:
				if chk != nil {
					chk.AppendUint64(i, v)
				} else {
					ds[i] = types.NewUintDatum(v)
				}
			case string:
				if chk != nil {
					chk.AppendString(i, v)
				} else {
					ds[i] = types.NewStringDatum(v)
				}
			case []byte:
				if chk != nil {
					chk.AppendBytes(i, v)
				} else {
					ds[i] = types.NewBytesDatum(v)
				}
			case time.Time:
				tm := types.NewTime(types.FromGoTime(v.In(loc)), mysql.TypeTimestamp, 0)
				if chk != nil {
					chk.AppendTime(i, tm)
				} else {
					ds[i] = types.NewTimeDatum(tm)
				}
			case nil:
				if chk != nil {
					chk.AppendNull(i)
				} else {
					ds[i] = types.Datum{}
				}
			default:
				panic("unsupported mock type")
			}
		}
		dsRows = append(dsRows, ds)
	}

	if chk != nil {
		c := chunk.NewCodec(colTypes)
		buffer := c.Encode(chk)
		return tipb.Chunk{RowsData: buffer}
	}

	var buffer []byte
	var err error
	for _, ds := range dsRows {
		buffer, err = codec.EncodeValue(loc, buffer, ds...)
		if err != nil {
			panic(err)
		}
	}
	return tipb.Chunk{RowsData: buffer}
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
