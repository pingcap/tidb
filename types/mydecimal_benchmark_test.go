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
<<<<<<< HEAD
			tests[i].inputDec.Round(&roundTo, tests[i].scale, ModeHalfEven)
=======
			err := tests[i].inputDec.Round(&roundTo, tests[i].scale, ModeHalfUp)
			if err != nil {
				b.Fatal(err)
			}
>>>>>>> 0beac1800... expression: fix the wrong rounding behavior of Decimal (#33278)
		}
		for i := 0; i < len(tests); i++ {
			tests[i].inputDec.Round(&roundTo, tests[i].scale, ModeTruncate)
		}
		for i := 0; i < len(tests); i++ {
<<<<<<< HEAD
			tests[i].inputDec.Round(&roundTo, tests[i].scale, modeCeiling)
=======
			err := tests[i].inputDec.Round(&roundTo, tests[i].scale, ModeCeiling)
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

func benchmarkMyDecimalToBinOrHashCases() []string {
	return []string{
		"1.000000000000", "3", "12.000000000", "120",
		"120000", "100000000000.00000", "0.000000001200000000",
		"98765.4321", "-123.456000000000000000",
		"0", "0000000000", "0.00000000000",
	}
}

func BenchmarkMyDecimalToBin(b *testing.B) {
	cases := benchmarkMyDecimalToBinOrHashCases()
	decs := make([]*MyDecimal, 0, len(cases))
	for _, ca := range cases {
		var dec MyDecimal
		if err := dec.FromString([]byte(ca)); err != nil {
			b.Fatal(err)
		}
		decs = append(decs, &dec)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, dec := range decs {
			prec, frac := dec.PrecisionAndFrac()
			_, err := dec.ToBin(prec, frac)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkMyDecimalToHashKey(b *testing.B) {
	cases := benchmarkMyDecimalToBinOrHashCases()
	decs := make([]*MyDecimal, 0, len(cases))
	for _, ca := range cases {
		var dec MyDecimal
		if err := dec.FromString([]byte(ca)); err != nil {
			b.Fatal(err)
		}
		decs = append(decs, &dec)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, dec := range decs {
			_, err := dec.ToHashKey()
			if err != nil {
				b.Fatal(err)
			}
>>>>>>> 0beac1800... expression: fix the wrong rounding behavior of Decimal (#33278)
		}
	}
}
