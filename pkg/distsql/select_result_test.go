// Copyright 2019 PingCAP, Inc.
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
	"fmt"
	"testing"
	"time"

	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestUpdateCopRuntimeStats(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = stmtctx.NewStmtCtx()
	sr := selectResult{ctx: ctx.GetDistSQLCtx(), storeType: kv.TiKV, stats: &selectResultRuntimeStats{}}
	require.Nil(t, ctx.GetSessionVars().StmtCtx.RuntimeStatsColl)

	sr.rootPlanID = 1234
	backOffSleep := make(map[string]time.Duration, 1)
	backOffSleep["RegionMiss"] = time.Duration(100)
	sr.updateCopRuntimeStats(context.Background(), &copr.CopRuntimeStats{CopExecDetails: execdetails.CopExecDetails{CalleeAddress: "a", BackoffSleep: backOffSleep}}, 0, false)
	// RuntimeStatsColl is nil, so the update doesn't take efffect
	require.Equal(t, sr.stats.backoffSleep["RegionMiss"], time.Duration(0))

	ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
	// refresh the ctx after assigning `RuntimeStatsColl`.
	sr.ctx = ctx.GetDistSQLCtx()
	i := uint64(1)
	sr.selectResp = &tipb.SelectResponse{
		ExecutionSummaries: []*tipb.ExecutorExecutionSummary{
			{TimeProcessedNs: &i, NumProducedRows: &i, NumIterations: &i},
		},
	}

	require.NotEqual(t, len(sr.copPlanIDs), len(sr.selectResp.GetExecutionSummaries()))

	backOffSleep["RegionMiss"] = time.Duration(200)
	sr.updateCopRuntimeStats(context.Background(), &copr.CopRuntimeStats{CopExecDetails: execdetails.CopExecDetails{CalleeAddress: "callee", BackoffSleep: backOffSleep}}, 0, false)
	require.False(t, ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.ExistsCopStats(1234))
	require.Equal(t, sr.stats.backoffSleep["RegionMiss"], time.Duration(200))

	sr.copPlanIDs = []int{sr.rootPlanID}
	require.NotNil(t, ctx.GetSessionVars().StmtCtx.RuntimeStatsColl)
	require.Equal(t, len(sr.copPlanIDs), len(sr.selectResp.GetExecutionSummaries()))

	backOffSleep["RegionMiss"] = time.Duration(300)
	sr.updateCopRuntimeStats(context.Background(), &copr.CopRuntimeStats{CopExecDetails: execdetails.CopExecDetails{CalleeAddress: "callee", BackoffSleep: backOffSleep}}, 0, false)
	require.Equal(t, "tikv_task:{time:1ns, loops:1}", ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetCopStats(1234).String())
	require.Equal(t, sr.stats.backoffSleep["RegionMiss"], time.Duration(500))
}

func TestNewSelRespChannelIter(t *testing.T) {
	r := &selectResult{
		ctx: &distsqlctx.DistSQLContext{
			Location: time.FixedZone("-02:00", -2*3600),
		},
		fieldTypes: []*types.FieldType{
			types.NewFieldType(mysql.TypeLong),
			types.NewFieldType(mysql.TypeVarchar),
			types.NewFieldType(mysql.TypeLong),
		},
		rowLen: 3,
		intermediateOutputTypes: [][]*types.FieldType{
			{types.NewFieldType(mysql.TypeString), types.NewFieldType(mysql.TypeLong)},
			{types.NewFieldType(mysql.TypeLong)},
		},
		selectResp: &tipb.SelectResponse{
			EncodeType: tipb.EncodeType_TypeChunk,
			Chunks: []tipb.Chunk{
				{RowsData: []byte("123")},
				{RowsData: []byte("456")},
			},
			IntermediateOutputs: []*tipb.IntermediateOutput{
				{
					EncodeType: tipb.EncodeType_TypeDefault,
					Chunks: []tipb.Chunk{
						{RowsData: []byte("111")},
						{RowsData: []byte("789")},
						{RowsData: []byte("101112")},
					},
				},
				{
					EncodeType: tipb.EncodeType_TypeChunk,
					Chunks: []tipb.Chunk{
						{RowsData: []byte("1098")},
						{RowsData: []byte("765")},
					},
				},
			},
		},
	}

	// 2 is len(IntermediateOutputs) which indicates the main output
	for _, encodeType := range []tipb.EncodeType{tipb.EncodeType_TypeChunk, tipb.EncodeType_TypeDefault} {
		r.selectResp.EncodeType = encodeType
		iter, err := newSelRespChannelIter(r, 2)
		require.NoError(t, err)
		require.Equal(t, &selRespChannelIter{
			channel: 2,
			loc:     time.FixedZone("-02:00", -2*3600),
			rowLen:  3,
			fieldTypes: []*types.FieldType{
				types.NewFieldType(mysql.TypeLong),
				types.NewFieldType(mysql.TypeVarchar),
				types.NewFieldType(mysql.TypeLong),
			},
			encodeType: encodeType,
			chkData: []tipb.Chunk{
				{RowsData: []byte("123")},
				{RowsData: []byte("456")},
			},
			reserveChkSize: vardef.DefInitChunkSize,
		}, iter)
		require.Equal(t, 2, iter.Channel())
	}

	// intermediate output 0
	iter, err := newSelRespChannelIter(r, 0)
	require.NoError(t, err)
	require.Equal(t, &selRespChannelIter{
		channel:    0,
		loc:        time.FixedZone("-02:00", -2*3600),
		rowLen:     2,
		fieldTypes: []*types.FieldType{types.NewFieldType(mysql.TypeString), types.NewFieldType(mysql.TypeLong)},
		encodeType: tipb.EncodeType_TypeDefault,
		chkData: []tipb.Chunk{
			{RowsData: []byte("111")},
			{RowsData: []byte("789")},
			{RowsData: []byte("101112")},
		},
		reserveChkSize: vardef.DefInitChunkSize,
	}, iter)
	require.Equal(t, 0, iter.Channel())

	// intermediate output 1
	iter, err = newSelRespChannelIter(r, 1)
	require.NoError(t, err)
	require.Equal(t, &selRespChannelIter{
		channel:    1,
		loc:        time.FixedZone("-02:00", -2*3600),
		rowLen:     1,
		fieldTypes: []*types.FieldType{types.NewFieldType(mysql.TypeLong)},
		encodeType: tipb.EncodeType_TypeChunk,
		chkData: []tipb.Chunk{
			{RowsData: []byte("1098")},
			{RowsData: []byte("765")},
		},
		reserveChkSize: vardef.DefInitChunkSize,
	}, iter)
	require.Equal(t, 1, iter.Channel())

	// out of range
	iter, err = newSelRespChannelIter(r, 3)
	require.ErrorContains(t, err, "invalid channel 3")
	require.Nil(t, iter)
}

func TestSelRespChannelIterRead(t *testing.T) {
	loc := time.FixedZone("+01:00", 3600)
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeLong),
		types.NewFieldType(mysql.TypeTimestamp),
	}

	baseTime := time.Date(2024, 1, 12, 13, 14, 15, 0, loc)
	rows0 := [][]any{
		{"hello", int64(1), baseTime},
		{"hello2", int64(2), baseTime.Add(time.Second)},
		{"hello3", int64(3), baseTime.Add(2 * time.Second)},
		{"hello4", int64(4), baseTime.Add(3 * time.Second)},
		{"hello5", int64(5), baseTime.Add(4 * time.Second)},
		{"hello6", int64(6), baseTime.Add(4 * time.Second)},
	}

	rows1 := [][]any{
		{"hello30", int64(30), baseTime.Add(30 * time.Second)},
	}

	rows3 := [][]any{
		{"hello1000", int64(1000), baseTime.Add(1000 * time.Second)},
		{"hello1001", int64(1001), baseTime.Add(1001 * time.Second)},
		{"hello1002", int64(1002), baseTime.Add(1002 * time.Second)},
	}

	allRows := append(make([][]any, 0, 7), rows0...)
	allRows = append(allRows, rows1...)
	allRows = append(allRows, rows3...)

	verifyIter := func(encodeType tipb.EncodeType) {
		r := &selectResult{
			ctx: &distsqlctx.DistSQLContext{
				Location: loc,
			},
			intermediateOutputTypes: [][]*types.FieldType{
				{types.NewFieldType(mysql.TypeString)}, colTypes, {types.NewFieldType(mysql.TypeLonglong)},
			},
			selectResp: &tipb.SelectResponse{
				IntermediateOutputs: []*tipb.IntermediateOutput{
					{
						EncodeType: encodeType,
					},
					{
						EncodeType: encodeType,
						Chunks: []tipb.Chunk{
							mockChunk(loc, encodeType, colTypes, rows0),
							mockChunk(loc, encodeType, colTypes, rows1),
							mockChunk(loc, encodeType, colTypes, [][]any{}),
							{},
							mockChunk(loc, encodeType, colTypes, rows3),
						},
					},
					{
						EncodeType: encodeType,
						Chunks: []tipb.Chunk{
							{},
						},
					},
				},
			},
		}

		// has rows
		iter, err := newSelRespChannelIter(r, 1)
		require.NoError(t, err)
		// set reserveChkSize to 4 to make sure we can test the logic reserved chunk is full
		iter.reserveChkSize = 4
		for i := 0; i <= len(allRows); i++ {
			row, err := iter.Next()
			require.NoError(t, err)
			if i == len(allRows) {
				require.True(t, row.IsEmpty())
			} else {
				require.False(t, row.IsEmpty())
				require.Equal(t, 1, row.ChannelIndex)
				strVal := row.GetString(0)
				intVal := row.GetInt64(1)
				tmVal, err := row.GetTime(2).GoTime(loc)
				require.NoError(t, err, "row: %d", i)
				require.Equal(t, allRows[i], []any{strVal, intVal, tmVal}, "row: %d", i)
			}
		}

		// no rows
		iter, err = newSelRespChannelIter(r, 0)
		require.NoError(t, err)
		row, err := iter.Next()
		require.NoError(t, err)
		require.True(t, row.IsEmpty())

		// one empty chunk
		iter, err = newSelRespChannelIter(r, 2)
		require.NoError(t, err)
		row, err = iter.Next()
		require.NoError(t, err)
		require.True(t, row.IsEmpty())
	}

	verifyIter(tipb.EncodeType_TypeDefault)
	verifyIter(tipb.EncodeType_TypeChunk)
}

func TestSelectResultIter(t *testing.T) {
	intermediateOutputTypes := [][]*types.FieldType{
		{types.NewFieldType(mysql.TypeLong)},
		{types.NewFieldType(mysql.TypeString)},
	}

	mockIntermediateOutput := func(i int, vals []any) *tipb.IntermediateOutput {
		output := &tipb.IntermediateOutput{
			EncodeType: tipb.EncodeType_TypeChunk,
		}

		var chk [][]any
		for j, val := range vals {
			chk = append(chk, []any{val})
			if len(chk) >= 2 || j == len(vals)-1 {
				output.Chunks = append(output.Chunks, mockChunk(time.UTC, tipb.EncodeType_TypeChunk, intermediateOutputTypes[i], chk))
				chk = chk[:0]
			}
		}

		return output
	}

	cases := []struct {
		name                string
		mainRows            int
		intermediateOutputs [][]*tipb.IntermediateOutput
		channelOrders       []int
		expectedRows        []any
	}{
		{
			name:     "normal case",
			mainRows: 3,
			intermediateOutputs: [][]*tipb.IntermediateOutput{
				{
					mockIntermediateOutput(0, []any{int64(1), int64(2), int64(3), int64(4), int64(5)}),
					mockIntermediateOutput(1, []any{"aa", "bb"}),
				},
				{
					mockIntermediateOutput(0, []any{int64(11)}),
					mockIntermediateOutput(1, []any{"1aa", "1bb", "1cc"}),
				},
				{
					// Response with intermediate outputs but no main output
					mockIntermediateOutput(0, []any{int64(21), int64(22)}),
					mockIntermediateOutput(1, []any{"2aa", "2bb", "2cc", "2dd"}),
				},
				{
					// An empty response
					mockIntermediateOutput(0, nil),
					mockIntermediateOutput(1, nil),
				},
			},
			channelOrders: []int{
				2, 2, 1, 1, 0, 0, 0, 0, 0,
				2, 1, 1, 1, 0,
				1, 1, 1, 1, 0, 0,
			},
			expectedRows: []any{
				"123_123_123_123", "123_123_123_123", "aa", "bb", int64(1), int64(2), int64(3), int64(4), int64(5),
				"123_123_123_123", "1aa", "1bb", "1cc", int64(11),
				"2aa", "2bb", "2cc", "2dd", int64(21), int64(22),
			},
		},
		{
			name:     "no intermediate outputs",
			mainRows: 3,
			intermediateOutputs: [][]*tipb.IntermediateOutput{
				{
					mockIntermediateOutput(0, nil),
					mockIntermediateOutput(1, nil),
				},
				{
					mockIntermediateOutput(0, nil),
					mockIntermediateOutput(1, nil),
				},
			},
			channelOrders: []int{2, 2, 2},
			expectedRows:  []any{"123_123_123_123", "123_123_123_123", "123_123_123_123"},
		},
		{
			name:     "no main outputs",
			mainRows: 0,
			intermediateOutputs: [][]*tipb.IntermediateOutput{
				{
					mockIntermediateOutput(0, []any{int64(1), int64(2)}),
					mockIntermediateOutput(1, []any{}),
				},
				{
					mockIntermediateOutput(0, []any{}),
					mockIntermediateOutput(1, []any{"1aa", "1cc"}),
				},
			},
			channelOrders: []int{0, 0, 1, 1},
			expectedRows:  []any{int64(1), int64(2), "1aa", "1cc"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sctx := newMockSessionContext()
			sctx.GetStore().GetClient().(*mock.Client).MockResponse.(*mockResponse).intermediateOutputs = c.intermediateOutputs
			r, colTypes := createSelectNormal(t, 2, c.mainRows, nil, sctx)
			iter, err := r.IntoIter(intermediateOutputTypes)
			require.NoError(t, err)

			// selectResult will return error if methods called after `IntoIter`
			_, err = r.NextRaw(context.Background())
			require.EqualError(t, err, "selectResult is invalid after IntoIter()")
			err = r.Next(context.Background(), chunk.New(colTypes, 1, 1))
			require.EqualError(t, err, "selectResult is invalid after IntoIter()")
			_, err = r.IntoIter(intermediateOutputTypes)
			require.EqualError(t, err, "selectResult is invalid after IntoIter()")
			err = r.Close()
			require.EqualError(t, err, "selectResult is invalid after IntoIter()")

			// test iter.Next()
			var channels []int
			var rows []any
			for {
				row, err := iter.Next(context.Background())
				require.NoError(t, err)
				if row.IsEmpty() {
					break
				}
				require.LessOrEqual(t, row.ChannelIndex, len(intermediateOutputTypes))
				channels = append(channels, row.ChannelIndex)
				if row.ChannelIndex == 2 {
					rows = append(rows, fmt.Sprintf(
						"%d_%d_%d_%d",
						row.GetInt64(0), row.GetInt64(1), row.GetInt64(2), row.GetInt64(3),
					))
				} else if row.ChannelIndex == 0 {
					rows = append(rows, row.GetInt64(0))
				} else {
					rows = append(rows, row.GetString(0))
				}
			}
			require.Equal(t, c.channelOrders, channels)
			require.Equal(t, c.expectedRows, rows)

			// test iter.Close()
			require.False(t, sctx.GetStore().GetClient().(*mock.Client).MockResponse.(*mockResponse).closed)
			require.Nil(t, iter.Close())
			require.True(t, sctx.GetStore().GetClient().(*mock.Client).MockResponse.(*mockResponse).closed)

			// intermediateOutputTypes len not match
			sctx = newMockSessionContext()
			sctx.GetStore().GetClient().(*mock.Client).MockResponse.(*mockResponse).intermediateOutputs = c.intermediateOutputs
			r, _ = createSelectNormal(t, 2, c.mainRows, nil, sctx)
			iter, err = r.IntoIter(intermediateOutputTypes[1:])
			require.NoError(t, err)
			_, err = iter.Next(context.Background())
			require.ErrorContains(
				t, err,
				"The length of intermediate output types 1 mismatches the length of got intermediate outputs 2",
			)
		})
	}

	// selectResult.Next() should return error if the response contains intermediate outputs
	sctx := newMockSessionContext()
	sctx.GetStore().GetClient().(*mock.Client).MockResponse.(*mockResponse).intermediateOutputs = [][]*tipb.IntermediateOutput{
		{
			// An empty response
			mockIntermediateOutput(0, nil),
			mockIntermediateOutput(1, nil),
		},
	}
	r, colFields := createSelectNormal(t, 2, 2, nil, sctx)
	err := r.Next(context.Background(), chunk.New(colFields, 1, 1))
	require.ErrorContains(
		t, err,
		"If a response contains intermediate outputs, you should use the SelectResultIter to read the data",
	)
}
