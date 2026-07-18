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
	"encoding/hex"
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/util/codec"
)

func main() {
	transitions := []struct {
		name  string
		value int64
	}{
		{"negative_1byte_last", -0xff},
		{"negative_2byte_first", -0x100},
		{"negative_2byte_last", -0xffff},
		{"negative_3byte_first", -0x10000},
		{"negative_3byte_last", -0xffffff},
		{"negative_4byte_first", -0x1000000},
		{"negative_4byte_last", -0xffffffff},
		{"negative_5byte_first", -0x100000000},
		{"negative_5byte_last", -0xffffffffff},
		{"negative_6byte_first", -0x10000000000},
		{"negative_6byte_last", -0xffffffffffff},
		{"negative_7byte_first", -0x1000000000000},
		{"negative_7byte_last", -0xffffffffffffff},
		{"negative_8byte_first", -0x100000000000000},
		{"negative_min", math.MinInt64},
	}
	for _, transition := range transitions {
		encoded := codec.EncodeComparableVarint(nil, transition.value)
		fmt.Printf("%s=%s\n", transition.name, hex.EncodeToString(encoded))
	}
}
