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

package expression

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

var benchmarkExecBinaryParamResult []Expression

func binaryParamUint64(tp byte, value uint64, unsigned bool) param.BinaryParam {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, value)
	return param.BinaryParam{Tp: tp, IsUnsigned: unsigned, Val: buf}
}

func execBinaryParamBenchmarkCases() []struct {
	name   string
	params []param.BinaryParam
} {
	signed := binaryParamUint64(mysql.TypeLonglong, 734521, false)
	unsigned := binaryParamUint64(mysql.TypeLonglong, math.MaxUint64, true)
	double := binaryParamUint64(mysql.TypeDouble, math.Float64bits(123.5), false)
	datetime := param.BinaryParam{
		Tp:  mysql.TypeDatetime,
		Val: []byte{0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00},
	}

	return []struct {
		name   string
		params []param.BinaryParam
	}{
		{name: "signed-longlong", params: []param.BinaryParam{signed}},
		{name: "unsigned-longlong", params: []param.BinaryParam{unsigned}},
		{name: "null", params: []param.BinaryParam{{Tp: mysql.TypeNull, IsNull: true}}},
		{name: "varchar", params: []param.BinaryParam{{Tp: mysql.TypeVarchar, Val: []byte("point-select")}}},
		{name: "datetime", params: []param.BinaryParam{datetime}},
		{
			name: "mixed-eight",
			params: []param.BinaryParam{
				signed,
				unsigned,
				{Tp: mysql.TypeNull, IsNull: true},
				{Tp: mysql.TypeVarchar, Val: []byte("point-select")},
				double,
				datetime,
				{Tp: mysql.TypeTiny, Val: []byte{0x7f}},
				{Tp: mysql.TypeBlob, Val: []byte("binary-value")},
			},
		},
	}
}

func BenchmarkExecBinaryParam(b *testing.B) {
	for _, testCase := range execBinaryParamBenchmarkCases() {
		b.Run(testCase.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecBinaryParam(types.DefaultStmtNoWarningContext, testCase.params)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkExecBinaryParamResult = result
			}
		})
	}
}

func TestExecBinaryParamValuesAndOrder(t *testing.T) {
	testCases := execBinaryParamBenchmarkCases()
	mixed := testCases[len(testCases)-1].params
	result, err := ExecBinaryParam(types.DefaultStmtNoWarningContext, mixed)
	require.NoError(t, err)
	require.Len(t, result, len(mixed))

	expected := []any{
		int64(734521),
		uint64(math.MaxUint64),
		nil,
		"point-select",
		float64(123.5),
		types.NewTime(types.FromDate(2010, 10, 17, 19, 27, 30, 1), mysql.TypeDatetime, types.MaxFsp),
		int64(127),
		[]byte("binary-value"),
	}
	for i, expression := range result {
		constant, ok := expression.(*Constant)
		require.True(t, ok)
		require.NotNil(t, constant.RetType)
		require.Equal(t, expected[i], constant.Value.GetValue())
	}

	empty, err := ExecBinaryParam(types.DefaultStmtNoWarningContext, nil)
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Empty(t, empty)
}

func TestExecBinaryParamErrors(t *testing.T) {
	result, err := ExecBinaryParam(types.DefaultStmtNoWarningContext, []param.BinaryParam{{
		Tp:  mysql.TypeDatetime,
		Val: []byte{1},
	}})
	require.ErrorIs(t, err, mysql.ErrMalformPacket)
	require.Len(t, result, 1)
	require.Nil(t, result[0])

	result, err = ExecBinaryParam(types.DefaultStmtNoWarningContext, []param.BinaryParam{{Tp: 0xaa}})
	require.Error(t, err)
	require.Len(t, result, 1)
	require.Nil(t, result[0])
}
