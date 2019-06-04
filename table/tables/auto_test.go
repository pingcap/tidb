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
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"strconv"
	"testing"

	"github.com/pingcap/tidb/types"
)

func TestCombineMultiColumnBitRangeFn(t *testing.T) {
	f, err := newCombineMultiColumnBitRangeFn([]int{0, 1}, []bitRange{{60, 64}, {60, 64}})
	if err != nil {
		t.Fatal(err)
	}
	if f.fnBits() != 8 {
		t.Fatalf("fnBits should be 8, but got %d", f.fnBits())
	}
	row := []types.Datum{types.NewDatum(15), types.NewDatum(15)}
	v := f.fn(row)
	s := strconv.FormatUint(uint64(v), 2)
	if s != "11111111" {
		t.Fatalf("shardbit should be 11111111 but got %s", s)
	}
}
