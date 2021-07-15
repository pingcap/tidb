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

import (
	"math"
	"math/rand"
	"strconv"
	"testing"
)

const (
	numTestDec = 1000
)

var (
	testDec []MyDecimal
	flag    = false
)

func genTestDecimals() {
	if flag {
		return
	}
	for i := 0; i < numTestDec; i++ {
		f := rand.Float64()
		digits := rand.Int()%12 + 1
		offset := rand.Int()%digits + 1
		f = math.Round(f*math.Pow10(digits)) / math.Pow10(digits-offset)
		testDec = append(testDec, *NewDecFromFloatForTest(f))
	}
	flag = true
}

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
		err := tests[i].inputDec.FromString([]byte(tests[i].input))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < len(tests); i++ {
			err := tests[i].inputDec.Round(&roundTo, tests[i].scale, ModeHalfEven)
			if err != nil {
				b.Fatal(err)
			}
		}
		for i := 0; i < len(tests); i++ {
			err := tests[i].inputDec.Round(&roundTo, tests[i].scale, ModeTruncate)
			if err != nil {
				b.Fatal(err)
			}
		}
		for i := 0; i < len(tests); i++ {
			err := tests[i].inputDec.Round(&roundTo, tests[i].scale, modeCeiling)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkToFloat64New(b *testing.B) {
	genTestDecimals()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < numTestDec; j++ {
			f, _ := testDec[j].ToFloat64()
			_ = f
		}
	}
}

func BenchmarkToFloat64Old(b *testing.B) {
	genTestDecimals()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < numTestDec; j++ {
			f, _ := strconv.ParseFloat(testDec[j].String(), 64)
			_ = f
		}
	}
}
