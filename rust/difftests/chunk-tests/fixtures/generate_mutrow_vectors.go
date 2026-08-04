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

// This generator drives the REAL `pkg/util/chunk` MutRow -- the mutable
// one-row chunk that partition pruning (`pkg/table/tables/partition.go`,
// `pkg/planner/core/rule/rule_partition_processor.go`) and ranger detachment
// (`pkg/util/ranger/detacher.go`) evaluate expressions against -- and prints,
// for every column of every case:
//
//	<case>\t<colIdx>\t<isNull 0|1>\t<isFixed 0|1>\t<hex of Row.GetRaw>
//
// `Row.GetRaw` is exactly `col.data[offsets[i]:offsets[i+1]]` for a var-length
// column and `col.data[i*elemLen:(i+1)*elemLen]` for a fixed one, so the hex
// column is the CELL BYTE IMAGE Go builds -- including the parts that only
// MutRow's hand-rolled column construction and its grow/shrink rules can
// produce (`setMutRowBytes`, `setMutRowNameValue`, `setMutRowJSON`,
// `cleanColOfMutRow`).
//
// Its stdout is the reviewed fixture stored in mutrow_vectors.tsv. Reproduce
// with, from the repository root:
//
//	go run rust/difftests/chunk-tests/fixtures/generate_mutrow_vectors.go \
//	  > rust/difftests/chunk-tests/fixtures/mutrow_vectors.tsv

package main

import (
	"encoding/hex"
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func emit(name string, mr chunk.MutRow) {
	row := mr.ToRow()
	c := row.Chunk()
	for i := range mr.Len() {
		isNull := 0
		if row.IsNull(i) {
			isNull = 1
		}
		isFixed := 0
		if c.Column(i).IsFixed() {
			isFixed = 1
		}
		fmt.Printf("%s\t%d\t%d\t%d\t%s\n", name, i, isNull, isFixed, hex.EncodeToString(row.GetRaw(i)))
	}
}

func ft(tp byte) *types.FieldType {
	f := types.NewFieldType(tp)
	return f
}

func unsignedFt(tp byte) *types.FieldType {
	f := types.NewFieldType(tp)
	f.AddFlag(mysql.UnsignedFlag)
	return f
}

// theTypes is the field-type table every MutRowFromTypes case is built from.
// It covers each arm of `zeroValForType` that the Rust port carries.
func theTypes() []*types.FieldType {
	return []*types.FieldType{
		ft(mysql.TypeFloat),
		ft(mysql.TypeDouble),
		ft(mysql.TypeTiny),
		ft(mysql.TypeLonglong),
		unsignedFt(mysql.TypeLonglong),
		ft(mysql.TypeYear),
		ft(mysql.TypeVarchar),
		ft(mysql.TypeString),
		ft(mysql.TypeVarString),
		ft(mysql.TypeBlob),
		ft(mysql.TypeTinyBlob),
		ft(mysql.TypeMediumBlob),
		ft(mysql.TypeLongBlob),
		ft(mysql.TypeDuration),
		ft(mysql.TypeNewDecimal),
		ft(mysql.TypeDate),
		ft(mysql.TypeDatetime),
		ft(mysql.TypeTimestamp),
		ft(mysql.TypeBit),
		ft(mysql.TypeSet),
		ft(mysql.TypeEnum),
		ft(mysql.TypeJSON),
		ft(mysql.TypeTiDBVectorFloat32),
		// zeroValForType's default arm: a null cell.
		ft(mysql.TypeGeometry),
	}
}

func mustDec(s string) *types.MyDecimal {
	var d types.MyDecimal
	if err := d.FromString([]byte(s)); err != nil {
		panic(err)
	}
	return &d
}

func mustJSON(s string) types.BinaryJSON {
	j, err := types.ParseBinaryJSONFromString(s)
	if err != nil {
		panic(err)
	}
	return j
}

func aTime() types.Time {
	return types.NewTime(types.FromDate(2024, 3, 17, 4, 5, 6, 789000), mysql.TypeDatetime, 6)
}

func aDuration() types.Duration {
	return types.Duration{Duration: 3*3600*1e9 + 25*60*1e9 + 45*1e9, Fsp: 0}
}

func aVector() types.VectorFloat32 {
	return types.InitVectorFloat32(3)
}

// theDatums is the datum table MutRowFromDatums and the SetDatum sweep use.
// One entry per Kind arm of `SetDatum` plus the null/sentinel arms.
func theDatums() []types.Datum {
	var vec types.VectorFloat32 = aVector()
	copy(vec.Elements(), []float32{1.5, -2.25, 3})
	return []types.Datum{
		types.NewDatum(nil),
		types.NewIntDatum(-1),
		types.NewIntDatum(42),
		types.NewUintDatum(18446744073709551615),
		types.NewFloat64Datum(3.5),
		types.NewFloat32Datum(1.5),
		types.NewStringDatum("hello"),
		types.NewStringDatum(""),
		types.NewBytesDatum([]byte{0x00, 0x01, 0xff}),
		types.NewBinaryLiteralDatum(types.BinaryLiteral{0xab, 0xcd}),
		types.NewDurationDatum(aDuration()),
		types.NewDecimalDatum(mustDec("123.456")),
		types.NewTimeDatum(aTime()),
		types.NewMysqlEnumDatum(types.Enum{Name: "abc", Value: 2}),
		types.NewMysqlSetDatum(types.Set{Name: "a,b", Value: 3}, "utf8mb4_bin"),
		types.NewJSONDatum(mustJSON(`{"a":1}`)),
		types.NewVectorFloat32Datum(vec),
		types.MinNotNullDatum(),
		types.MaxValueDatum(),
	}
}

func main() {
	// 1. MutRowFromDatums: one column per datum, built by makeMutRowColumn.
	emit("from_datums", chunk.MutRowFromDatums(theDatums()))

	// 2. MutRowFromTypes: every zeroValForType arm.
	emit("from_types", chunk.MutRowFromTypes(theTypes()))

	// 3. SetDatum onto the matching zero-valued column (the in-place write
	//    path, whose grow rules differ from makeMutRowColumn's).
	{
		tps := []*types.FieldType{
			ft(mysql.TypeVarchar),
			ft(mysql.TypeLonglong),
			ft(mysql.TypeDouble),
			ft(mysql.TypeDatetime),
			ft(mysql.TypeDuration),
			ft(mysql.TypeNewDecimal),
			ft(mysql.TypeJSON),
			ft(mysql.TypeEnum),
			ft(mysql.TypeSet),
			ft(mysql.TypeBlob),
			ft(mysql.TypeFloat),
			ft(mysql.TypeBit),
			ft(mysql.TypeTiDBVectorFloat32),
		}
		ds := []types.Datum{
			types.NewStringDatum("a longer string than the zero value"),
			types.NewIntDatum(-7),
			types.NewFloat64Datum(-0.125),
			types.NewTimeDatum(aTime()),
			types.NewDurationDatum(aDuration()),
			types.NewDecimalDatum(mustDec("-9876.54321")),
			types.NewJSONDatum(mustJSON(`[1,"x"]`)),
			types.NewMysqlEnumDatum(types.Enum{Name: "zz", Value: 7}),
			types.NewMysqlSetDatum(types.Set{Name: "a", Value: 1}, "utf8mb4_bin"),
			types.NewBytesDatum([]byte{0xde, 0xad, 0xbe, 0xef}),
			types.NewFloat32Datum(-2.5),
			types.NewBinaryLiteralDatum(types.BinaryLiteral{0x01, 0x02, 0x03}),
			types.NewVectorFloat32Datum(aVector()),
		}
		mr := chunk.MutRowFromTypes(tps)
		mr.SetDatums(ds...)
		emit("set_datum", mr)

		// Setting every column to NULL runs cleanColOfMutRow on each: the
		// offsets go to zero, so a var-length cell reads back EMPTY, and a
		// fixed cell keeps its stale bytes.
		nulls := make([]types.Datum, len(ds))
		mr.SetDatums(nulls...)
		emit("set_datum_null", mr)
	}

	// 4. setMutRowBytes' grow-then-shrink rule: after a long value the buffer
	//    stays large, and a shorter value only reslices it.
	{
		mr := chunk.MutRowFromTypes([]*types.FieldType{ft(mysql.TypeVarchar)})
		mr.SetDatum(0, types.NewStringDatum("0123456789abcdef"))
		emit("bytes_grow", mr)
		mr.SetDatum(0, types.NewStringDatum("xy"))
		emit("bytes_shrink", mr)
		mr.SetDatum(0, types.NewStringDatum("0123456789abcdefghij"))
		emit("bytes_regrow", mr)
	}

	// 5. SetDatum's fixed-width grow rule: a column whose zero value was a
	//    var-length empty buffer still takes an int64/float32/time/decimal.
	//    NOTE what the raw hex shows: SetDatum grows `col.data` but never
	//    touches `offsets[1]`, so GetRaw/GetBytes still report an EMPTY cell
	//    while the typed getter reads the value back. The `_typed` lines pin
	//    that second half.
	{
		for _, c := range []struct {
			name string
			d    types.Datum
			read func(chunk.Row) string
		}{
			{"grow_int", types.NewIntDatum(0x0102030405060708), func(r chunk.Row) string {
				return fmt.Sprintf("%d", r.GetInt64(0))
			}},
			{"grow_float32", types.NewFloat32Datum(7.25), func(r chunk.Row) string {
				return fmt.Sprintf("%v", r.GetFloat32(0))
			}},
			{"grow_time", types.NewTimeDatum(aTime()), func(r chunk.Row) string {
				return r.GetTime(0).String()
			}},
			{"grow_duration", types.NewDurationDatum(aDuration()), func(r chunk.Row) string {
				return fmt.Sprintf("%d", int64(r.GetDuration(0, 0).Duration))
			}},
			{"grow_decimal", types.NewDecimalDatum(mustDec("1.5")), func(r chunk.Row) string {
				return r.GetMyDecimal(0).String()
			}},
		} {
			mr := chunk.MutRowFromTypes([]*types.FieldType{ft(mysql.TypeVarchar)})
			mr.SetDatum(0, c.d)
			emit(c.name, mr)
			fmt.Printf("%s_typed\t0\t0\t0\t%s\n", c.name, c.read(mr.ToRow()))
		}
	}

	// 6. SetValue: the `any` sibling of SetDatum, which does NOT grow the
	//    fixed buffers.
	{
		tps := []*types.FieldType{
			ft(mysql.TypeLonglong),
			ft(mysql.TypeLonglong),
			ft(mysql.TypeDouble),
			ft(mysql.TypeFloat),
			ft(mysql.TypeVarchar),
			ft(mysql.TypeBlob),
			ft(mysql.TypeDuration),
			ft(mysql.TypeNewDecimal),
			ft(mysql.TypeDatetime),
			ft(mysql.TypeEnum),
			ft(mysql.TypeSet),
			ft(mysql.TypeJSON),
			ft(mysql.TypeBit),
		}
		mr := chunk.MutRowFromTypes(tps)
		mr.SetValues(
			int64(-3),
			uint64(9),
			float64(2.5),
			float32(-1.25),
			"str",
			[]byte{0x10, 0x20},
			aDuration(),
			mustDec("0.001"),
			aTime(),
			types.Enum{Name: "e", Value: 4},
			types.Set{Name: "s", Value: 5},
			mustJSON(`true`),
			types.BinaryLiteral{0x7f},
		)
		emit("set_value", mr)

		// A nil value cleans the column and leaves it NULL.
		mr.SetValue(4, nil)
		mr.SetValue(0, nil)
		emit("set_value_nil", mr)
	}

	// 7. SetRow: copy one row out of a real chunk into a MutRow of the same
	//    types, including a NULL source cell.
	{
		tps := []*types.FieldType{
			ft(mysql.TypeLonglong),
			ft(mysql.TypeVarchar),
			ft(mysql.TypeDatetime),
			ft(mysql.TypeVarchar),
		}
		chk := chunk.NewChunkWithCapacity(tps, 4)
		chk.AppendInt64(0, 1234)
		chk.AppendString(1, "row-source-value")
		chk.AppendTime(2, aTime())
		chk.AppendNull(3)

		mr := chunk.MutRowFromTypes(tps)
		mr.SetRow(chk.GetRow(0))
		emit("set_row", mr)

		// Cloning must not alias the source.
		emit("set_row_clone", mr.Clone())
	}

	// 8. ShallowCopyPartialRow: fixed and variable cells, at an offset.
	{
		srcTps := []*types.FieldType{
			ft(mysql.TypeLonglong),
			ft(mysql.TypeVarchar),
			ft(mysql.TypeVarchar),
		}
		chk := chunk.NewChunkWithCapacity(srcTps, 4)
		chk.AppendInt64(0, -5)
		chk.AppendString(1, "shallow")
		chk.AppendNull(2)
		chk.AppendInt64(0, 6)
		chk.AppendString(1, "second")
		chk.AppendString(2, "notnull")

		dstTps := []*types.FieldType{
			ft(mysql.TypeVarchar),
			ft(mysql.TypeLonglong),
			ft(mysql.TypeVarchar),
			ft(mysql.TypeVarchar),
		}
		mr := chunk.MutRowFromTypes(dstTps)
		mr.SetDatum(0, types.NewStringDatum("kept"))
		mr.ShallowCopyPartialRow(1, chk.GetRow(1))
		emit("shallow_copy", mr)

		mr2 := chunk.MutRowFromTypes(dstTps)
		mr2.ShallowCopyPartialRow(1, chk.GetRow(0))
		emit("shallow_copy_null", mr2)
	}
}
