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
	"time"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

func main() {
	literals := []string{
		"1234.00", "1234", "12.34", "12.340", "0.1234", "0.0", "0",
		"-0.0", "-0.0000", "-1234.00", "-1234", "-12.34", "-12.340", "-0.1234",
	}
	for index, literal := range literals {
		decimal := types.NewDecFromStringForTest(literal)
		encoded, err := codec.EncodeKey(time.Local, nil, types.NewDatum(decimal))
		if err != nil {
			panic(err)
		}
		fmt.Printf("decimal_%d=%s\n", index, hex.EncodeToString(encoded))
	}
}
