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

// This generator drives the REAL `pkg/util/chunk` comparison surface --
// `GetCompareFunc`, `Compare(row, colIdx, *types.Datum)`, `Chunk.LowerBound`
// and `Chunk.UpperBound` -- and prints Go's own answers.
//
// It exists because `chunk_test.go`'s `TestCompare` only ever compares
// null < 0 < 1 per type, which cannot see any of the rules that are easy to
// port wrong:
//
//   - the string comparator is COLLATION-AWARE (utf8mb4_bin trims trailing
//     spaces, utf8mb4_general_ci folds case, binary does neither);
//   - `cmpBit` is `BinaryLiteral.Compare` (leading zero bytes stripped, LENGTH
//     compared first), while `Compare`'s bit/bytes datum arm is a RAW
//     `bytes.Compare` -- the two deliberately disagree;
//   - `cmpNameValue` orders ENUM/SET by numeric value and never by name;
//   - JSON ordering is by type precedence first;
//   - unsigned columns must not be read through the signed getter;
//   - `LowerBound`'s `match` flag, and its interaction with the null/
//     MinNotNull/MaxValue range sentinels, which `Compare` treats specially.
//
// Output lines, tab separated:
//
//	cmp	<column>	<i>	<j>	<-1|0|1>
//	lb	<column>	<probe>	<index>	<0|1 match>
//	ub	<column>	<probe>	<index>
//
// The column and probe NAMES are the contract: the Rust test builds the same
// cells and the same datums from the same names, and this file's expectations
// are Go's. Reproduce with, from the repository root:
//
//	go run rust/difftests/chunk-tests/fixtures/generate_compare_vectors.go \
//	  > rust/difftests/chunk-tests/fixtures/compare_vectors.tsv

package main

import (
	"fmt"
	"math"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const numRows = 8

func ft(tp byte) *types.FieldType { return types.NewFieldType(tp) }

func ftCollate(tp byte, charset, collate string) *types.FieldType {
	f := types.NewFieldType(tp)
	f.SetCharset(charset)
	f.SetCollate(collate)
	return f
}

func ftElems(tp byte, elems []string) *types.FieldType {
	f := types.NewFieldType(tp)
	f.SetElems(elems)
	return f
}

func unsignedFt(tp byte) *types.FieldType {
	f := types.NewFieldType(tp)
	f.SetFlag(mysql.UnsignedFlag)
	return f
}

func mustJSON(s string) types.BinaryJSON {
	j, err := types.ParseBinaryJSONFromString(s)
	if err != nil {
		panic(err)
	}
	return j
}

func dt(year, month, day, hour, minute, sec int) types.Time {
	return types.NewTime(
		types.FromDate(year, month, day, hour, minute, sec, 0),
		mysql.TypeDatetime, 0)
}

// column is one named single-type column plus the recipe that fills it.
type column struct {
	name string
	tp   *types.FieldType
	fill func(chk *chunk.Chunk, col int)
}

func appendStrings(vals []string) func(*chunk.Chunk, int) {
	return func(chk *chunk.Chunk, col int) {
		chk.AppendNull(col)
		for _, v := range vals {
			chk.AppendString(col, v)
		}
	}
}

// The seven non-null strings shared by the three string columns: they differ
// only in case, in a trailing space, and in the empty string.
var stringValues = []string{"", "A", "a", "ab", "ab ", "b", "B"}

func columns() []column {
	return []column{
		{"int_signed", ft(mysql.TypeLonglong), func(chk *chunk.Chunk, col int) {
			chk.AppendNull(col)
			for _, v := range []int64{math.MinInt64, -1, 0, 1, 42, 42, math.MaxInt64} {
				chk.AppendInt64(col, v)
			}
		}},
		{"int_unsigned", unsignedFt(mysql.TypeLonglong), func(chk *chunk.Chunk, col int) {
			chk.AppendNull(col)
			for _, v := range []uint64{0, 1, 42, 42, math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64} {
				chk.AppendUint64(col, v)
			}
		}},
		{"year", ft(mysql.TypeYear), func(chk *chunk.Chunk, col int) {
			chk.AppendNull(col)
			for _, v := range []int64{0, 1901, 1999, 2000, 2000, 2155, 9999} {
				chk.AppendInt64(col, v)
			}
		}},
		{"float", ft(mysql.TypeFloat), func(chk *chunk.Chunk, col int) {
			chk.AppendNull(col)
			for _, v := range []float32{-1.5, float32(math.Copysign(0, -1)), 0, 1.5, 1.5, math.MaxFloat32, math.SmallestNonzeroFloat32} {
				chk.AppendFloat32(col, v)
			}
		}},
		{"double", ft(mysql.TypeDouble), func(chk *chunk.Chunk, col int) {
			chk.AppendNull(col)
			for _, v := range []float64{-1.5, math.Copysign(0, -1), 0, 1.5, 1.5, math.MaxFloat64, math.SmallestNonzeroFloat64} {
				chk.AppendFloat64(col, v)
			}
		}},
		{"varchar_bin", ft(mysql.TypeVarchar), appendStrings(stringValues)},
		{"varchar_ci", ftCollate(mysql.TypeVarchar, "utf8mb4", "utf8mb4_general_ci"), appendStrings(stringValues)},
		{"blob_binary", ft(mysql.TypeBlob), appendStrings(stringValues)},
		{"datetime", ft(mysql.TypeDatetime), func(chk *chunk.Chunk, col int) {
			chk.AppendNull(col)
			for _, v := range []types.Time{
				dt(1000, 1, 1, 0, 0, 0),
				dt(2000, 1, 1, 0, 0, 0),
				dt(2000, 1, 1, 0, 0, 1),
				dt(2000, 1, 1, 0, 0, 1),
				dt(2000, 12, 31, 23, 59, 59),
				dt(2020, 6, 15, 12, 0, 0),
				dt(9999, 12, 31, 23, 59, 59),
			} {
				chk.AppendTime(col, v)
			}
		}},
		{"duration", ft(mysql.TypeDuration), func(chk *chunk.Chunk, col int) {
			chk.AppendNull(col)
			for _, v := range []time.Duration{
				-time.Hour * 838, -time.Second, 0, 0, time.Second, time.Hour, time.Hour * 838,
			} {
				chk.AppendDuration(col, types.Duration{Duration: v, Fsp: 0})
			}
		}},
		{"decimal", ft(mysql.TypeNewDecimal), func(chk *chunk.Chunk, col int) {
			chk.AppendNull(col)
			for _, s := range []string{"-99999.9", "-1.5", "0", "0.00", "1.50", "2", "10"} {
				var d types.MyDecimal
				if err := d.FromString([]byte(s)); err != nil {
					panic(err)
				}
				chk.AppendMyDecimal(col, &d)
			}
		}},
		{"enum", ftElems(mysql.TypeEnum, []string{"a", "b", "c"}), func(chk *chunk.Chunk, col int) {
			chk.AppendNull(col)
			// The names deliberately DISAGREE with the numeric order, which is
			// what proves `cmpNameValue` never looks at the name.
			for _, e := range []types.Enum{
				{Name: "c", Value: 0}, {Name: "b", Value: 1}, {Name: "a", Value: 2},
				{Name: "z", Value: 2}, {Name: "a", Value: 3}, {Name: "a", Value: 4},
				{Name: "a", Value: 5},
			} {
				chk.AppendEnum(col, e)
			}
		}},
		{"set", ftElems(mysql.TypeSet, []string{"a", "b", "c"}), func(chk *chunk.Chunk, col int) {
			chk.AppendNull(col)
			for _, s := range []types.Set{
				{Name: "c", Value: 0}, {Name: "b", Value: 1}, {Name: "a", Value: 2},
				{Name: "z", Value: 2}, {Name: "a,b", Value: 3}, {Name: "c", Value: 4},
				{Name: "a,b,c", Value: 7},
			} {
				chk.AppendSet(col, s)
			}
		}},
		{"bit", ft(mysql.TypeBit), func(chk *chunk.Chunk, col int) {
			chk.AppendNull(col)
			// Leading zero bytes are stripped before the LENGTH comparison, so
			// {0x00,0x01} must equal {0x01} and {0x01,0x00} must be the largest.
			for _, b := range [][]byte{
				{}, {0x00}, {0x00, 0x00}, {0x01}, {0x00, 0x01}, {0x02}, {0x01, 0x00},
			} {
				chk.AppendBytes(col, b)
			}
		}},
		{"json", ft(mysql.TypeJSON), func(chk *chunk.Chunk, col int) {
			chk.AppendNull(col)
			for _, s := range []string{
				`null`, `false`, `true`, `1`, `2.5`, `"abc"`, `{"a": 1}`,
			} {
				chk.AppendJSON(col, mustJSON(s))
			}
		}},
	}
}

// boundColumn is a NON-DECREASING column plus the probes to search it with.
type boundColumn struct {
	name   string
	tp     *types.FieldType
	fill   func(chk *chunk.Chunk, col int)
	probes []string
}

func probeDatum(name string) types.Datum {
	var d types.Datum
	switch name {
	case "null":
		d.SetNull()
	case "min_not_null":
		d.SetMinNotNull()
	case "max_value":
		d = types.MaxValueDatum()
	case "i0":
		d.SetInt64(0)
	case "i1":
		d.SetInt64(1)
	case "i2":
		d.SetInt64(2)
	case "i3":
		d.SetInt64(3)
	case "i4":
		d.SetInt64(4)
	case "i9":
		d.SetInt64(9)
	case "i10":
		d.SetInt64(10)
	case "s_a":
		d.SetString("a", "utf8mb4_bin")
	case "s_b":
		d.SetString("b", "utf8mb4_bin")
	case "s_d":
		d.SetString("d", "utf8mb4_bin")
	case "s_z":
		d.SetString("z", "utf8mb4_bin")
	default:
		panic("unknown probe " + name)
	}
	return d
}

func boundColumns() []boundColumn {
	intProbes := []string{"i0", "i1", "i2", "i3", "i4", "i9", "i10", "null", "min_not_null", "max_value"}
	return []boundColumn{
		{"lb_int", ft(mysql.TypeLonglong), func(chk *chunk.Chunk, col int) {
			for _, v := range []int64{1, 3, 3, 3, 5, 7, 9, 9} {
				chk.AppendInt64(col, v)
			}
		}, intProbes},
		{"lb_nullable_int", ft(mysql.TypeLonglong), func(chk *chunk.Chunk, col int) {
			// Nulls sort first, so a leading null run is still non-decreasing.
			chk.AppendNull(col)
			chk.AppendNull(col)
			for _, v := range []int64{1, 3, 3, 5, 7, 9} {
				chk.AppendInt64(col, v)
			}
		}, intProbes},
		{"lb_str", ft(mysql.TypeVarchar), func(chk *chunk.Chunk, col int) {
			for _, v := range []string{"a", "b", "b", "c", "e", "f", "g", "h"} {
				chk.AppendString(col, v)
			}
		}, []string{"s_a", "s_b", "s_d", "s_z", "null", "min_not_null", "max_value"}},
	}
}

func main() {
	cols := columns()
	tps := make([]*types.FieldType, 0, len(cols))
	for _, c := range cols {
		tps = append(tps, c.tp)
	}
	chk := chunk.NewChunkWithCapacity(tps, numRows)
	for i, c := range cols {
		c.fill(chk, i)
	}
	for i, c := range cols {
		cmpFunc := chunk.GetCompareFunc(c.tp)
		for a := range numRows {
			for b := range numRows {
				fmt.Printf("cmp\t%s\t%d\t%d\t%d\n",
					c.name, a, b, cmpFunc(chk.GetRow(a), i, chk.GetRow(b), i))
			}
		}
	}

	for _, bc := range boundColumns() {
		bchk := chunk.NewChunkWithCapacity([]*types.FieldType{bc.tp}, numRows)
		bc.fill(bchk, 0)
		for _, p := range bc.probes {
			d := probeDatum(p)
			idx, match := bchk.LowerBound(0, &d)
			m := 0
			if match {
				m = 1
			}
			fmt.Printf("lb\t%s\t%s\t%d\t%d\n", bc.name, p, idx, m)
			fmt.Printf("ub\t%s\t%s\t%d\n", bc.name, p, bchk.UpperBound(0, &d))
		}
	}
}
