// Copyright 2016 PingCAP, Inc.
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

package codec

import (
	"testing"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
)

var valueCnt = 100

func composeEncodedData(size int) []byte {
	values := make([]types.Datum, 0, size)
	for i := 0; i < size; i++ {
		values = append(values, types.NewDatum(i))
	}
	bs, _ := EncodeValue(nil, nil, values...)
	return bs
}

func BenchmarkDecodeWithSize(b *testing.B) {
	b.StopTimer()
	bs := composeEncodedData(valueCnt)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Decode(bs, valueCnt)
	}
}

func BenchmarkDecodeWithOutSize(b *testing.B) {
	b.StopTimer()
	bs := composeEncodedData(valueCnt)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		Decode(bs, 1)
	}
}

func BenchmarkEncodeIntWithSize(b *testing.B) {
	for i := 0; i < b.N; i++ {
		data := make([]byte, 0, 8)
		EncodeInt(data, 10)
	}
}

func BenchmarkEncodeIntWithOutSize(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncodeInt(nil, 10)
	}
}

func BenchmarkDecodeDecimal(b *testing.B) {
	dec := &types.MyDecimal{}
	dec.FromFloat64(1211.1211113)
	precision, frac := dec.PrecisionAndFrac()
	raw, _ := EncodeDecimal([]byte{}, dec, precision, frac)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeDecimal(raw)
	}
}

func BenchmarkDecodeOneToChunk(b *testing.B) {
	str := new(types.Datum)
	*str = types.NewStringDatum("a")
	var raw []byte
	raw = append(raw, bytesFlag)
	raw = EncodeBytes(raw, str.GetBytes())
	intType := types.NewFieldType(mysql.TypeLonglong)
	b.ResetTimer()
	decoder := NewDecoder(chunk.New([]*types.FieldType{intType}, 32, 32), nil)
	for i := 0; i < b.N; i++ {
		decoder.DecodeOne(raw, 0, intType)
	}
}
