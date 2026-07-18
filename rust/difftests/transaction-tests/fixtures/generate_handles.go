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

// This generator executes the exact codec construction paths and values from
// pkg/kv/key_test.go TestHandle, TestPaddingHandle, TestHandleMap,
// TestCommonHandlesFitIntHandleRange, and TestHandleMapWithPartialHandle.
package main

import (
	"encoding/hex"
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

func key(values ...types.Datum) []byte {
	encoded, err := codec.EncodeKey(stmtctx.NewStmtCtx().TimeZone(), nil, values...)
	if err != nil {
		panic(err)
	}
	return encoded
}

func emit(name string, encoded []byte) {
	fmt.Printf("%s=%s\n", name, hex.EncodeToString(encoded))
}

func main() {
	emit("int_handle_min", codec.EncodeInt(nil, math.MinInt64))
	emit("int_handle_max", codec.EncodeInt(nil, math.MaxInt64))
	emit("handle_100_abc", key(types.NewIntDatum(100), types.NewStringDatum("abc")))
	emit("handle_101_abc", key(types.NewIntDatum(101), types.NewStringDatum("abc")))
	emit("handle_99_def", key(types.NewIntDatum(99), types.NewStringDatum("def")))
	emit("decimal_1", key(types.NewDecimalDatum(types.NewDecFromInt(1))))
	emit("range_int_string", key(types.NewIntDatum(101), types.NewStringDatum("abc")))
	emit("range_string_int", key(types.NewStringDatum("abc"), types.NewIntDatum(101)))
	emit("range_negative_int_string", key(types.NewIntDatum(-101), types.NewStringDatum("abc")))
	emit("range_min_max_int", key(types.NewIntDatum(math.MinInt64), types.NewIntDatum(math.MaxInt64)))
	emit("range_bytes_ff", key(types.NewBytesDatum([]byte{0xff, 0xff})))
	emit("range_bytes_00", key(types.NewBytesDatum([]byte{0x00, 0x00})))
	emit("range_binary_ff", key(types.NewBinaryLiteralDatum([]byte{0xff, 0xff})))
}
