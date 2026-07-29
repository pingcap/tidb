// Copyright 2026 PingCAP, Inc.
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

package kv_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	. "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

var newCommonHandleTPCCSink *CommonHandle
var commonHandleTPCCBytesSink []byte
var commonHandleTPCCHandleSink Handle

func BenchmarkNewCommonHandleTPCC(b *testing.B) {
	testCases := []struct {
		name   string
		datums []types.Datum
	}{
		{name: "tpcc/2-int", datums: makeCommonHandleIntDatums(2)},
		{name: "tpcc/3-int", datums: makeCommonHandleIntDatums(3)},
		{name: "tpcc/4-int", datums: makeCommonHandleIntDatums(4)},
		{name: "protected/1-int", datums: makeCommonHandleIntDatums(1)},
		{name: "protected/padded-decimal", datums: []types.Datum{
			types.NewDecimalDatum(types.NewDecFromInt(1)),
		}},
		{name: "protected/5-int", datums: makeCommonHandleIntDatums(5)},
		{name: "protected/8-int", datums: makeCommonHandleIntDatums(8)},
		{name: "protected/9-int", datums: makeCommonHandleIntDatums(9)},
		{name: "protected/16-int", datums: makeCommonHandleIntDatums(16)},
		{name: "protected/int-string", datums: []types.Datum{
			types.NewIntDatum(42),
			types.NewStringDatum("customer-0000000042"),
		}},
	}

	for _, testCase := range testCases {
		b.Run(testCase.name, func(b *testing.B) {
			encoded, err := codec.EncodeKey(time.UTC, nil, testCase.datums...)
			require.NoError(b, err)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				handle, err := NewCommonHandle(encoded)
				if err != nil {
					b.Fatal(err)
				}
				newCommonHandleTPCCSink = handle
			}
			b.StopTimer()

			require.Equal(b, len(testCase.datums), newCommonHandleTPCCSink.NumCols())
			for i := range testCase.datums {
				encodedCol := newCommonHandleTPCCSink.EncodedCol(i)
				_, datum, err := codec.DecodeOne(encodedCol)
				require.NoError(b, err, fmt.Sprintf("column %d", i))
				roundTrip, err := codec.EncodeKey(time.UTC, nil, datum)
				require.NoError(b, err, fmt.Sprintf("column %d", i))
				require.Equal(b, encodedCol, roundTrip)
			}
		})
	}
}

func BenchmarkCommonHandleAccessTPCC(b *testing.B) {
	for _, count := range []int{4, 16} {
		b.Run(fmt.Sprintf("columns=%d", count), func(b *testing.B) {
			encoded, err := codec.EncodeKey(time.UTC, nil, makeCommonHandleIntDatums(count)...)
			require.NoError(b, err)
			handle, err := NewCommonHandle(encoded)
			require.NoError(b, err)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for i := range handle.NumCols() {
					commonHandleTPCCBytesSink = handle.EncodedCol(i)
				}
			}
			b.StopTimer()

			require.NotEmpty(b, commonHandleTPCCBytesSink)
		})
	}
}

func BenchmarkCommonHandleDerivedTPCC(b *testing.B) {
	for _, count := range []int{4, 16} {
		encoded, err := codec.EncodeKey(time.UTC, nil, makeCommonHandleIntDatums(count)...)
		require.NoError(b, err)
		handle, err := NewCommonHandle(encoded)
		require.NoError(b, err)

		b.Run(fmt.Sprintf("copy/columns=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				commonHandleTPCCHandleSink = handle.Copy()
			}
		})
		b.Run(fmt.Sprintf("next/columns=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				commonHandleTPCCHandleSink = handle.Next()
			}
		})
	}
}

func TestCommonHandleTPCCProtectedShapes(t *testing.T) {
	empty, err := NewCommonHandle(nil)
	require.NoError(t, err)
	require.Zero(t, empty.NumCols())
	require.Len(t, empty.Encoded(), 9)

	for _, count := range []int{1, 2, 3, 4, 5, 8, 9, 16, 17, 32, 33} {
		encoded, err := codec.EncodeKey(time.UTC, nil, makeCommonHandleIntDatums(count)...)
		require.NoError(t, err)
		handle, err := NewCommonHandle(encoded)
		require.NoError(t, err)
		require.Equal(t, count, handle.NumCols())

		for i := range count {
			encodedCol := append([]byte(nil), handle.EncodedCol(i)...)
			_, datum, err := codec.DecodeOne(encodedCol)
			require.NoError(t, err)
			require.Equal(t, int64(i*1000+7), datum.GetInt64())
		}

		copied := handle.Copy()
		require.True(t, handle.Equal(copied))
		handle.Encoded()[0] ^= 0xff
		require.False(t, handle.Equal(copied))
		handle.Encoded()[0] ^= 0xff

		next := handle.Next()
		handle = nil
		runtime.GC()
		require.Equal(t, count, next.NumCols())
		for i := range count {
			require.NotEmpty(t, next.EncodedCol(i))
		}
	}

	encoded, err := codec.EncodeKey(time.UTC, nil, makeCommonHandleIntDatums(2)...)
	require.NoError(t, err)
	_, err = NewCommonHandle(encoded[:len(encoded)-1])
	require.Error(t, err)
}

func makeCommonHandleIntDatums(count int) []types.Datum {
	datums := make([]types.Datum, count)
	for i := range datums {
		datums[i] = types.NewIntDatum(int64(i*1000 + 7))
	}
	return datums
}
