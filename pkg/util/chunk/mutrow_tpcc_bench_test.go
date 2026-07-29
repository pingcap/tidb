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

package chunk

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

var (
	mutRowVarLenColumnTPCCSink   *Column
	mutRowFixedLenColumnTPCCSink *Column
	mutRowTPCCSink               MutRow
)

func BenchmarkNewMutRowFixedLenColumnTPCC(b *testing.B) {
	cases := []struct {
		name string
		size int
	}{
		{name: "float32", size: 4},
		{name: "integer-or-duration", size: 8},
		{name: "time", size: sizeTime},
		{name: "decimal", size: types.MyDecimalStructSize},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				mutRowFixedLenColumnTPCCSink = newMutRowFixedLenColumn(tc.size)
			}
			b.StopTimer()
			require.Len(b, mutRowFixedLenColumnTPCCSink.elemBuf, tc.size)
			require.Len(b, mutRowFixedLenColumnTPCCSink.data, tc.size)
			require.Len(b, mutRowFixedLenColumnTPCCSink.nullBitmap, 1)
			require.Equal(b, &mutRowFixedLenColumnTPCCSink.elemBuf[0], &mutRowFixedLenColumnTPCCSink.data[0])
			require.False(b, &mutRowFixedLenColumnTPCCSink.data[0] == &mutRowFixedLenColumnTPCCSink.nullBitmap[0])
			require.Equal(b, byte(1), mutRowFixedLenColumnTPCCSink.nullBitmap[0])
		})
	}
}

func BenchmarkNewMutRowVarLenColumnTPCC(b *testing.B) {
	for _, size := range []int{0, 2, 24, 50, 500} {
		b.Run(fmt.Sprintf("value-size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				mutRowVarLenColumnTPCCSink = newMutRowVarLenColumn(size)
			}
			require.Len(b, mutRowVarLenColumnTPCCSink.data, size)
			require.Equal(b, []int64{0, int64(size)}, mutRowVarLenColumnTPCCSink.offsets)
			require.Equal(b, byte(1), mutRowVarLenColumnTPCCSink.nullBitmap[0])
		})
	}
}

func BenchmarkMutRowFromTypesTPCC(b *testing.B) {
	warehouse, district, customer, stock := tpccUpdateFieldTypes()
	cases := []struct {
		name   string
		schema []*types.FieldType
	}{
		{name: "warehouse", schema: warehouse},
		{name: "district", schema: district},
		{name: "customer", schema: customer},
		{name: "stock", schema: stock},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				mutRowTPCCSink = MutRowFromTypes(tc.schema)
			}
			require.Equal(b, len(tc.schema), mutRowTPCCSink.Len())
		})
	}

	b.Run("payment-plus-average-new-order", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			mutRowTPCCSink = MutRowFromTypes(warehouse)
			mutRowTPCCSink = MutRowFromTypes(district)
			mutRowTPCCSink = MutRowFromTypes(customer)
			mutRowTPCCSink = MutRowFromTypes(district)
			for range 10 {
				mutRowTPCCSink = MutRowFromTypes(stock)
			}
		}
		require.Equal(b, len(stock), mutRowTPCCSink.Len())
	})
}

func BenchmarkMutRowFromValuesTPCC(b *testing.B) {
	for _, value := range []string{"", strings.Repeat("x", 24), strings.Repeat("x", 500)} {
		b.Run(fmt.Sprintf("value-size=%d", len(value)), func(b *testing.B) {
			values := []any{int64(1), value}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				mutRowTPCCSink = MutRowFromValues(values...)
			}
			b.StopTimer()
			require.Equal(b, value, mutRowTPCCSink.ToRow().GetString(1))
		})
	}
}

func BenchmarkMutRowVarLenGrowthTPCC(b *testing.B) {
	value := []byte(strings.Repeat("x", 500))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		col := newMutRowVarLenColumn(0)
		setMutRowBytes(col, value)
		col.nullBitmap[0] = 1
		mutRowVarLenColumnTPCCSink = col
	}
	b.StopTimer()
	require.Equal(b, value, mutRowVarLenColumnTPCCSink.data)
	require.Equal(b, int64(len(value)), mutRowVarLenColumnTPCCSink.offsets[1])
	require.Equal(b, byte(1), mutRowVarLenColumnTPCCSink.nullBitmap[0])
}

func TestNewMutRowVarLenColumnIndependenceAndGrowth(t *testing.T) {
	first := makeMutRowColumn("")
	second := makeMutRowColumn("")

	require.Equal(t, []int64{0, 0}, first.offsets)
	require.Len(t, first.offsets, 2)
	require.Equal(t, 2, cap(first.offsets))
	require.Empty(t, first.data)
	require.Equal(t, 1, cap(first.data))
	require.Equal(t, []byte{1}, first.nullBitmap)
	require.Equal(t, 1, cap(first.nullBitmap))
	require.False(t, &first.offsets[0] == &second.offsets[0])
	require.False(t, &first.nullBitmap[0] == &second.nullBitmap[0])

	value := []byte(strings.Repeat("x", 500))
	setMutRowBytes(first, value)
	first.nullBitmap[0] = 1
	require.Equal(t, value, first.data)
	require.Equal(t, []int64{0, int64(len(value))}, first.offsets)
	require.Equal(t, byte(1), first.nullBitmap[0])
	require.Equal(t, []int64{0, 0}, second.offsets)
	require.Empty(t, second.data)
	require.Equal(t, byte(1), second.nullBitmap[0])
}

func TestNewMutRowFixedLen8ColumnIndependence(t *testing.T) {
	first := MutRowFromValues(uint64(1))
	second := MutRowFromValues(uint64(2))
	firstCol := first.c.columns[0]
	secondCol := second.c.columns[0]

	require.Len(t, firstCol.elemBuf, sizeInt64)
	require.Len(t, firstCol.data, sizeInt64)
	require.Len(t, firstCol.nullBitmap, 1)
	require.Equal(t, sizeInt64, cap(firstCol.elemBuf))
	require.Equal(t, sizeInt64, cap(firstCol.data))
	require.Equal(t, 1, cap(firstCol.nullBitmap))
	require.Equal(t, &firstCol.elemBuf[0], &firstCol.data[0])
	require.False(t, &firstCol.data[0] == &firstCol.nullBitmap[0])
	require.False(t, &firstCol.data[0] == &secondCol.data[0])
	require.False(t, &firstCol.nullBitmap[0] == &secondCol.nullBitmap[0])

	first.SetValue(0, uint64(3))
	require.Equal(t, uint64(3), first.ToRow().GetUint64(0))
	require.Equal(t, uint64(2), second.ToRow().GetUint64(0))
	firstCol.nullBitmap[0] = 0
	require.True(t, first.ToRow().IsNull(0))
	require.False(t, second.ToRow().IsNull(0))
	require.Equal(t, byte(1), secondCol.nullBitmap[0])
}

func tpccUpdateFieldTypes() (
	warehouse, district, customer, stock []*types.FieldType,
) {
	const (
		intType = iota
		stringType
		decimalType
		datetimeType
	)
	makeTypes := func(spec ...int) []*types.FieldType {
		result := make([]*types.FieldType, len(spec))
		for i, kind := range spec {
			mysqlType := byte(mysql.TypeLong)
			switch kind {
			case stringType:
				mysqlType = mysql.TypeVarchar
			case decimalType:
				mysqlType = mysql.TypeNewDecimal
			case datetimeType:
				mysqlType = mysql.TypeDatetime
			}
			result[i] = types.NewFieldType(mysqlType)
		}
		return result
	}

	warehouse = makeTypes(
		intType,
		stringType, stringType, stringType, stringType, stringType, stringType,
		decimalType, decimalType,
	)
	district = makeTypes(
		intType, intType,
		stringType, stringType, stringType, stringType, stringType, stringType,
		decimalType, decimalType, intType,
	)
	customer = makeTypes(
		intType, intType, intType,
		stringType, stringType, stringType, stringType, stringType, stringType,
		stringType, stringType, stringType,
		datetimeType, stringType,
		decimalType, decimalType, decimalType, decimalType,
		intType, intType, stringType,
	)
	stock = makeTypes(
		intType, intType, intType,
		stringType, stringType, stringType, stringType, stringType,
		stringType, stringType, stringType, stringType, stringType,
		intType, intType, intType, stringType,
	)
	return warehouse, district, customer, stock
}
