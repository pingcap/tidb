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

//go:build ignore

package main

import (
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

// Row-v2 bytes for decimal values that DO and do NOT carry a declared column
// shape. `Datum.Length`/`Datum.Frac` are what `convertToMysqlDecimal` stamps
// from the target `FieldType`, and `encoder.encodeRowCols` hands them to
// `codec.EncodeDecimal`; `MyDecimal` itself never learns the column shape, so
// no round trip through the value can observe the difference.
func main() {
	stamped := columnDatum("11.99", 10, 4)
	natural := valueDatum("11.99")
	wide := columnDatum("-0.5", 20, 10)

	precision, frac := stamped.GetMysqlDecimal().PrecisionAndFrac()
	fmt.Printf("# a payload written at (10, 4) still reports PrecisionAndFrac %d,%d\n",
		precision, frac)

	emit("row_decimal_10_4_11_99", []int64{1}, stamped)
	emit("row_decimal_natural_11_99", []int64{1}, natural)
	emit("row_decimal_mixed_11_99", []int64{1, 2}, stamped, natural)
	emit("row_decimal_20_10_neg_0_5", []int64{1}, wide)

	// `codec.encode` -- the encoder behind EncodeKey/EncodeValue, and so behind
	// every index key -- reads the same `Length`/`Frac` pair.
	emitKey("key_decimal_10_4_11_99", stamped)
	emitKey("key_decimal_natural_11_99", natural)
	emitValue("value_decimal_10_4_11_99", stamped)
}

func emitKey(name string, values ...types.Datum) {
	key, err := codec.EncodeKey(time.UTC, nil, values...)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s=%x\n", name, key)
}

func emitValue(name string, values ...types.Datum) {
	value, err := codec.EncodeValue(time.UTC, nil, values...)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s=%x\n", name, value)
}

func emit(name string, colIDs []int64, values ...types.Datum) {
	encoder := rowcodec.Encoder{}
	row, err := encoder.Encode(time.UTC, colIDs, values, nil, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s=%x\n", name, row)
}

func columnDatum(literal string, flen, decimal int) types.Datum {
	datum := valueDatum(literal)
	datum.SetLength(flen)
	datum.SetFrac(decimal)
	return datum
}

func valueDatum(literal string) types.Datum {
	value := new(types.MyDecimal)
	if err := value.FromString([]byte(literal)); err != nil {
		panic(err)
	}
	var datum types.Datum
	datum.SetMysqlDecimal(value)
	return datum
}
