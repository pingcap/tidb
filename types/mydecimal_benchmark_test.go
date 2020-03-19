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

package types

import "testing"

func BenchmarkRound(b *testing.B) {
	b.StopTimer()
	var roundTo MyDecimal
	tests := []struct {
		input    string
		scale    int
		inputDec MyDecimal
	}{
		{input: "123456789.987654321", scale: 1},
		{input: "15.1", scale: 0},
		{input: "15.5", scale: 0},
		{input: "15.9", scale: 0},
		{input: "-15.1", scale: 0},
		{input: "-15.5", scale: 0},
		{input: "-15.9", scale: 0},
		{input: "15.1", scale: 1},
		{input: "-15.1", scale: 1},
		{input: "15.17", scale: 1},
		{input: "15.4", scale: -1},
		{input: "-15.4", scale: -1},
		{input: "5.4", scale: -1},
		{input: ".999", scale: 0},
		{input: "999999999", scale: -9},
	}

	for i := 0; i < len(tests); i++ {
		tests[i].inputDec.FromString([]byte(tests[i].input))
	}

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < len(tests); i++ {
			tests[i].inputDec.Round(&roundTo, tests[i].scale, ModeHalfEven)
		}
		for i := 0; i < len(tests); i++ {
			tests[i].inputDec.Round(&roundTo, tests[i].scale, ModeTruncate)
		}
		for i := 0; i < len(tests); i++ {
			tests[i].inputDec.Round(&roundTo, tests[i].scale, modeCeiling)
		}
	}
}
