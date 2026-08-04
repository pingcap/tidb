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
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build ignore

// This generator drives `textrow.FormatValueText` -- the function every value
// a SELECT returns over the MySQL TEXT protocol passes through -- against a
// real `chunk.Row`, and prints per case:
//
//	<type code> <flag> <decimal> <Table==""> <input> OK <hex bytes>
//	<type code> <flag> <decimal> <Table==""> <input> ERR
//
// Its stdout is the reviewed fixture stored in textrow_vectors.tsv.
//
// Floats are carried as their exact BIT PATTERN so no decimal round trip
// stands between Go's value and the Rust side's, and the sweep includes 400
// pseudo-random f64/f32 patterns: shortest-round-trip digits, exponent
// spelling and mantissa trimming are where a text formatter diverges, and a
// hand-picked list never reaches those shapes.

package main

import (
	"encoding/hex"
	"fmt"
	"math"
	"os"

	"github.com/pingcap/tidb/pkg/format/textrow"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func emit(input string, tp byte, flag uint16, dec uint8, tableEmpty bool, ft *types.FieldType, d types.Datum) {
	table := "t"
	if tableEmpty {
		table = ""
	}
	col := textrow.ColumnInfo{Table: table, Charset: mysql.DefaultCollationID, Flag: flag, Decimal: dec, Type: tp}
	c := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)
	c.AppendDatum(0, &d)
	enc := textrow.NewResultEncoder("utf8mb4")
	te := 0
	if tableEmpty {
		te = 1
	}
	out, err := textrow.FormatValueText(c.GetRow(0), 0, col, enc)
	if err != nil {
		fmt.Printf("%d\t%d\t%d\t%d\t%s\tERR\t\n", tp, flag, dec, te, input)
		return
	}
	fmt.Printf("%d\t%d\t%d\t%d\t%s\tOK\t%s\n", tp, flag, dec, te, input, hex.EncodeToString(out))
}

func main() {
	ftOf := func(tp byte, flag uint) *types.FieldType {
		ft := types.NewFieldType(tp)
		ft.AddFlag(flag)
		return ft
	}

	ints := []int64{0, 1, -1, 127, -128, 32767, -32768, 2147483647, -2147483648,
		9223372036854775807, -9223372036854775808, 42, -42}
	for _, tp := range []byte{mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong} {
		for _, v := range ints {
			emit(fmt.Sprintf("i:%d", v), tp, 0, 0, true, ftOf(tp, 0), types.NewIntDatum(v))
		}
	}
	for _, v := range []uint64{0, 1, 255, 65535, 4294967295, 9223372036854775808, 18446744073709551615, 42} {
		emit(fmt.Sprintf("u:%d", v), mysql.TypeLonglong, uint16(mysql.UnsignedFlag), 0, true,
			ftOf(mysql.TypeLonglong, mysql.UnsignedFlag), types.NewUintDatum(v))
	}
	for _, v := range []int64{0, 1901, 2000, 2155, 1, 99} {
		emit(fmt.Sprintf("i:%d", v), mysql.TypeYear, 0, 0, true, ftOf(mysql.TypeYear, 0), types.NewIntDatum(v))
	}

	floats := []float64{0, 1, -1, 0.5, -0.5, 1.0 / 3.0, 1e14, 1e15, 1e16, -1e15,
		1e-14, 1e-15, 1e-16, 123456789.123456789, math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.Inf(1), math.Inf(-1), math.NaN(), 3.14159265358979, -2.718281828459045, 1e300, 1e-300,
		math.Copysign(0, -1), 999999999999999.9, 1000000000000000.1, 9.999999999999999e-16, 1.0000000000000001e-15,
		0.1, 0.2, 0.3, 1e-308, 5e-324, 1.7976931348623157e308, 2.2250738585072014e-308}
	for _, prec := range []uint8{0, 2, 5, mysql.NotFixedDec} {
		for _, tableEmpty := range []bool{true, false} {
			for _, v := range floats {
				emit(fmt.Sprintf("f64:%016x", math.Float64bits(v)), mysql.TypeDouble, 0, prec, tableEmpty,
					ftOf(mysql.TypeDouble, 0), types.NewFloat64Datum(v))
				emit(fmt.Sprintf("f32:%08x", math.Float32bits(float32(v))), mysql.TypeFloat, 0, prec, tableEmpty,
					ftOf(mysql.TypeFloat, 0), types.NewFloat32Datum(float32(v)))
			}
		}
	}

	// A deterministic pseudo-random bit-pattern sweep: shortest-round-trip
	// digits, exponent spelling and mantissa trimming are where a text
	// formatter diverges, and a hand-picked list never reaches those shapes.
	seed := uint64(0x9E3779B97F4A7C15)
	for i := 0; i < 400; i++ {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		v := math.Float64frombits(seed)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			v = float64(int64(seed % 1000000)) / 1000.0
		}
		prec := []uint8{0, 3, 7, mysql.NotFixedDec}[i%4]
		emit(fmt.Sprintf("f64:%016x", math.Float64bits(v)), mysql.TypeDouble, 0, prec, i%2 == 0,
			ftOf(mysql.TypeDouble, 0), types.NewFloat64Datum(v))
		f := math.Float32frombits(uint32(seed >> 32))
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			f = float32(seed%1000000) / 1000.0
		}
		emit(fmt.Sprintf("f32:%08x", math.Float32bits(f)), mysql.TypeFloat, 0, prec, i%2 == 0,
			ftOf(mysql.TypeFloat, 0), types.NewFloat32Datum(f))
	}

	for _, s := range []string{"0", "1", "-1", "0.00", "123.4500", "-0.0001",
		"99999999999999999999999999999999999999999999999999999999999999999",
		"1.000000000000000000000000000000"} {
		var d types.MyDecimal
		if err := d.FromString([]byte(s)); err != nil {
			continue
		}
		emit(fmt.Sprintf("d:%s", s), mysql.TypeNewDecimal, 0, 0, true,
			ftOf(mysql.TypeNewDecimal, 0), types.NewDecimalDatum(&d))
	}

	byteCases := [][]byte{
		{}, []byte("hello"), []byte("中文"), {0xF0, 0x9F}, {0xFF, 0xFF}, {0x00, 0x41}, {0x80, 0x7F},
	}
	for _, tp := range []byte{mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeBit, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob} {
		for _, raw := range byteCases {
			emit(fmt.Sprintf("b:%s", hex.EncodeToString(raw)), tp, 0, 0, true,
				ftOf(tp, 0), types.NewBytesDatum(raw))
		}
	}
	// The branches this crate has not connected: Go renders each of them,
	// so the fixture carries what a client would see.
	for _, spec := range []struct {
		tp   byte
		text string
		fsp  int
	}{
		{mysql.TypeDate, "2021-01-02", 0},
		{mysql.TypeDatetime, "2021-01-02 03:04:05", 0},
		{mysql.TypeDatetime, "2021-01-02 03:04:05.123456", 6},
		{mysql.TypeTimestamp, "2021-01-02 03:04:05", 0},
		{mysql.TypeDate, "0000-00-00", 0},
	} {
		tm, err := types.ParseTime(types.DefaultStmtNoWarningContext, spec.text, spec.tp, spec.fsp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "skip %s: %v\n", spec.text, err)
			continue
		}
		ft := types.NewFieldType(spec.tp)
		ft.SetDecimal(spec.fsp)
		emit(fmt.Sprintf("t:%s", spec.text), spec.tp, 0, uint8(spec.fsp), true, ft, types.NewTimeDatum(tm))
	}
	for _, spec := range []struct {
		text string
		fsp  int
	}{{"03:04:05", 0}, {"-03:04:05", 0}, {"838:59:59", 0}, {"01:02:03.456000", 6}} {
		dur, _, err := types.ParseDuration(types.DefaultStmtNoWarningContext, spec.text, spec.fsp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "skip %s: %v\n", spec.text, err)
			continue
		}
		ft := types.NewFieldType(mysql.TypeDuration)
		ft.SetDecimal(spec.fsp)
		emit(fmt.Sprintf("dur:%s", spec.text), mysql.TypeDuration, 0, uint8(spec.fsp), true, ft, types.NewDurationDatum(dur))
	}
	{
		ft := types.NewFieldType(mysql.TypeEnum)
		ft.SetElems([]string{"a", "b"})
		emit("enum:b", mysql.TypeEnum, 0, 0, true, ft, types.NewMysqlEnumDatum(types.Enum{Name: "b", Value: 2}))
		ftSet := types.NewFieldType(mysql.TypeSet)
		ftSet.SetElems([]string{"a", "b"})
		emit("set:a,b", mysql.TypeSet, 0, 0, true, ftSet, types.NewMysqlSetDatum(types.Set{Name: "a,b", Value: 3}, "utf8mb4_bin"))
		j, err := types.ParseBinaryJSONFromString(`{"k":[1,"x",null]}`)
		if err == nil {
			emit("json:1", mysql.TypeJSON, 0, 0, true, types.NewFieldType(mysql.TypeJSON), types.NewJSONDatum(j))
		}
	}

	emit("b:78", mysql.TypeGeometry, 0, 0, true, ftOf(mysql.TypeGeometry, 0), types.NewBytesDatum([]byte("x")))
}
