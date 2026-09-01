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

package executor

import (
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var (
	updateCloneRowBenchmarkSink []types.Datum
	updateDatumRowBenchmarkSink []types.Datum
	updateExecBenchmarkSink     *UpdateExec
)

func makeTPCCUpdateDatumRow(n int) []types.Datum {
	row := make([]types.Datum, n)
	for i := range row {
		switch i % 5 {
		case 0:
			row[i] = types.NewIntDatum(int64(i + 1))
		case 1:
			row[i] = types.NewStringDatum("tpcc-string-value")
		case 2:
			row[i] = types.NewBytesDatum([]byte("tpcc-bytes-value"))
		case 3:
			row[i] = types.NewDecimalDatum(types.NewDecFromStringForTest("12345.67"))
		case 4:
			row[i] = types.NewTimeDatum(types.ZeroTime)
		}
	}
	return row
}

func BenchmarkUpdateExecCloneRowTPCC(b *testing.B) {
	for _, tc := range []struct {
		name  string
		width int
	}{
		{name: "warehouse", width: 9},
		{name: "district", width: 11},
		{name: "customer", width: 21},
		{name: "stock", width: 17},
	} {
		b.Run(tc.name, func(b *testing.B) {
			e := &UpdateExec{}
			row := makeTPCCUpdateDatumRow(tc.width)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				updateCloneRowBenchmarkSink, err = e.fastComposeNewRow(i, row, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if len(updateCloneRowBenchmarkSink) != tc.width {
				b.Fatalf("got row width %d, want %d", len(updateCloneRowBenchmarkSink), tc.width)
			}
		})
	}
}

type updateDatumRowGetter interface {
	getDatumRow(chunk.Row, []*types.FieldType) []types.Datum
}

func getUpdateDatumRowForBenchmark(
	e *UpdateExec, row chunk.Row, fields []*types.FieldType,
) []types.Datum {
	if getter, ok := any(e).(updateDatumRowGetter); ok {
		return getter.getDatumRow(row, fields)
	}
	return row.GetDatumRow(fields)
}

func BenchmarkUpdateExecGetDatumRowTPCC(b *testing.B) {
	warehouse, district, customer, stock := tpccUpdateFieldTypes()
	for _, tc := range []struct {
		name   string
		fields []*types.FieldType
	}{
		{name: "warehouse", fields: warehouse},
		{name: "district", fields: district},
		{name: "customer", fields: customer},
		{name: "stock", fields: stock},
	} {
		chk := makeTPCCUpdateChunk(tc.fields, 32)
		b.Run(tc.name+"/cold", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				e := &UpdateExec{}
				updateExecBenchmarkSink = e
				row := chk.GetRow(i % chk.NumRows())
				updateDatumRowBenchmarkSink = getUpdateDatumRowForBenchmark(e, row, tc.fields)
				updateCloneRowBenchmarkSink = e.cloneRow(updateDatumRowBenchmarkSink)
			}
		})
		b.Run(tc.name+"/steady", func(b *testing.B) {
			e := &UpdateExec{}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				row := chk.GetRow(i % chk.NumRows())
				updateDatumRowBenchmarkSink = getUpdateDatumRowForBenchmark(e, row, tc.fields)
				updateCloneRowBenchmarkSink = e.cloneRow(updateDatumRowBenchmarkSink)
			}
		})
	}
}

func BenchmarkUpdateExecGetDatumRowColdPairedTPCC(b *testing.B) {
	warehouse, district, customer, stock := tpccUpdateFieldTypes()
	for _, tc := range []struct {
		name   string
		fields []*types.FieldType
	}{
		{name: "warehouse", fields: warehouse},
		{name: "district", fields: district},
		{name: "customer", fields: customer},
		{name: "stock", fields: stock},
	} {
		chk := makeTPCCUpdateChunk(tc.fields, 32)
		b.Run(tc.name+"/old", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				e := &UpdateExec{}
				updateExecBenchmarkSink = e
				row := chk.GetRow(i % chk.NumRows())
				updateDatumRowBenchmarkSink = row.GetDatumRow(tc.fields)
				updateCloneRowBenchmarkSink = e.cloneRow(updateDatumRowBenchmarkSink)
			}
		})
		b.Run(tc.name+"/new", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				e := &UpdateExec{}
				updateExecBenchmarkSink = e
				row := chk.GetRow(i % chk.NumRows())
				updateDatumRowBenchmarkSink = e.getDatumRow(row, tc.fields)
				updateCloneRowBenchmarkSink = e.cloneRow(updateDatumRowBenchmarkSink)
			}
		})
	}
}

func tpccUpdateFieldTypes() (
	warehouse, district, customer, stock []*types.FieldType,
) {
	const (
		intType = iota
		stringType
		decimalType
		datetimeType
	)
	makeTypes := func(spec ...int) []*types.FieldType {
		result := make([]*types.FieldType, len(spec))
		for i, kind := range spec {
			mysqlType := byte(mysql.TypeLong)
			switch kind {
			case stringType:
				mysqlType = mysql.TypeVarchar
			case decimalType:
				mysqlType = mysql.TypeNewDecimal
			case datetimeType:
				mysqlType = mysql.TypeDatetime
			}
			result[i] = types.NewFieldType(mysqlType)
		}
		return result
	}

	warehouse = makeTypes(
		intType,
		stringType, stringType, stringType, stringType, stringType, stringType,
		decimalType, decimalType,
	)
	district = makeTypes(
		intType, intType,
		stringType, stringType, stringType, stringType, stringType, stringType,
		decimalType, decimalType, intType,
	)
	customer = makeTypes(
		intType, intType, intType,
		stringType, stringType, stringType, stringType, stringType, stringType,
		stringType, stringType, stringType,
		datetimeType, stringType,
		decimalType, decimalType, decimalType, decimalType,
		intType, intType, stringType,
	)
	stock = makeTypes(
		intType, intType, intType,
		stringType, stringType, stringType, stringType, stringType,
		stringType, stringType, stringType, stringType, stringType,
		intType, intType, intType, stringType,
	)
	return warehouse, district, customer, stock
}

func makeTPCCUpdateChunk(fields []*types.FieldType, rows int) *chunk.Chunk {
	chk := chunk.NewChunkWithCapacity(fields, rows)
	for rowIdx := 0; rowIdx < rows; rowIdx++ {
		for colIdx, field := range fields {
			if (rowIdx+colIdx)%17 == 0 {
				chk.AppendNull(colIdx)
				continue
			}
			switch field.GetType() {
			case mysql.TypeLong:
				chk.AppendInt64(colIdx, int64(rowIdx+colIdx+1))
			case mysql.TypeVarchar:
				chk.AppendString(colIdx, "tpcc-string-value")
			case mysql.TypeNewDecimal:
				chk.AppendMyDecimal(colIdx, types.NewDecFromStringForTest("12345.67"))
			case mysql.TypeDatetime:
				chk.AppendTime(colIdx, types.ZeroTime)
			default:
				panic("unsupported TPCC benchmark field type")
			}
		}
	}
	return chk
}

func TestUpdateExecCloneRowBuffer(t *testing.T) {
	sourceBytes := []byte("first-row")
	firstSource := []types.Datum{
		types.NewIntDatum(1),
		types.NewBytesDatum(sourceBytes),
		types.NewDecimalDatum(types.NewDecFromStringForTest("1.25")),
	}
	e := &UpdateExec{}
	first, err := e.fastComposeNewRow(0, firstSource, nil)
	if err != nil {
		t.Fatal(err)
	}
	firstBacking := &first[0]
	retained := slices.Clone(first)

	sourceBytes[0] = 'X'
	if got := string(first[1].GetBytes()); got != "first-row" {
		t.Fatalf("composed row aliases source bytes: got %q", got)
	}

	secondSource := []types.Datum{
		types.NewIntDatum(2),
		types.NewBytesDatum([]byte("second-row")),
		types.NewDecimalDatum(types.NewDecFromStringForTest("2.50")),
	}
	second, err := e.fastComposeNewRow(1, secondSource, nil)
	if err != nil {
		t.Fatal(err)
	}
	if firstBacking != &second[0] {
		t.Fatal("row buffer was not reused for the same width")
	}
	if retained[0].GetInt64() != 1 || string(retained[1].GetBytes()) != "first-row" ||
		retained[2].GetMysqlDecimal().String() != "1.25" {
		t.Fatalf("retained shallow clone changed after buffer reuse: %v", retained)
	}

	larger, err := e.fastComposeNewRow(2, makeTPCCUpdateDatumRow(21), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(larger) != 21 {
		t.Fatalf("got grown row width %d, want 21", len(larger))
	}
	smaller, err := e.fastComposeNewRow(3, makeTPCCUpdateDatumRow(9), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(smaller) != 9 {
		t.Fatalf("got shrunk row width %d, want 9", len(smaller))
	}
	for i, d := range e.newRowData[:cap(e.newRowData)][len(smaller):] {
		if !d.IsNull() {
			t.Fatalf("truncated Datum %d was retained", i+len(smaller))
		}
	}

	other := &UpdateExec{}
	otherRow, err := other.fastComposeNewRow(0, secondSource, nil)
	if err != nil {
		t.Fatal(err)
	}
	if &otherRow[0] == &second[0] {
		t.Fatal("different UpdateExec instances share row storage")
	}
}

func TestUpdateExecDatumRowBuffers(t *testing.T) {
	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeNewDecimal),
		types.NewFieldType(mysql.TypeDatetime),
		types.NewFieldType(mysql.TypeLong),
	}
	chk := chunk.NewChunkWithCapacity(fields, 2)
	chk.AppendString(0, "first-row")
	chk.AppendMyDecimal(1, types.NewDecFromStringForTest("1.25"))
	chk.AppendTime(2, types.ZeroTime)
	chk.AppendInt64(3, 1)
	for colIdx := range fields {
		chk.AppendNull(colIdx)
	}

	e := &UpdateExec{}
	first := e.getDatumRow(chk.GetRow(0), fields)
	firstBacking := &first[0]
	if len(first) != len(fields) || cap(first) != len(fields) {
		t.Fatalf("input row length/capacity = %d/%d, want %d/%d",
			len(first), cap(first), len(fields), len(fields))
	}
	retainedInput := slices.Clone(first)
	firstComposed := e.cloneRow(first)
	firstComposedBacking := &firstComposed[0]
	retainedComposed := slices.Clone(firstComposed)
	if cap(firstComposed) != len(fields) {
		t.Fatalf("composed row capacity = %d, want %d", cap(firstComposed), len(fields))
	}
	if firstBacking == firstComposedBacking {
		t.Fatal("input and composed rows alias")
	}

	second := e.getDatumRow(chk.GetRow(1), fields)
	if firstBacking != &second[0] {
		t.Fatal("input row buffer was not reused")
	}
	for i, datum := range second {
		if !datum.IsNull() || datum.GetValue() != nil || len(datum.GetBytes()) != 0 {
			t.Fatalf("NULL Datum %d retained previous state: %v", i, datum)
		}
	}
	secondComposed := e.cloneRow(second)
	if firstComposedBacking != &secondComposed[0] {
		t.Fatal("composed row buffer was not reused")
	}
	if retainedInput[0].GetString() != "first-row" ||
		retainedInput[1].GetMysqlDecimal().String() != "1.25" {
		t.Fatalf("retained input changed after buffer reuse: %v", retainedInput)
	}
	if retainedComposed[0].GetString() != "first-row" ||
		retainedComposed[1].GetMysqlDecimal().String() != "1.25" {
		t.Fatalf("retained composed row changed after buffer reuse: %v", retainedComposed)
	}

	_, _, customerFields, _ := tpccUpdateFieldTypes()
	customerChunk := makeTPCCUpdateChunk(customerFields, 1)
	grown := e.getDatumRow(customerChunk.GetRow(0), customerFields)
	if len(grown) != len(customerFields) || cap(grown) != len(customerFields) {
		t.Fatalf("grown input length/capacity = %d/%d, want %d/%d",
			len(grown), cap(grown), len(customerFields), len(customerFields))
	}
	grownComposed := e.cloneRow(grown)
	if len(grownComposed) != len(customerFields) || cap(grownComposed) != len(customerFields) {
		t.Fatalf("grown composed length/capacity = %d/%d, want %d/%d",
			len(grownComposed), cap(grownComposed), len(customerFields), len(customerFields))
	}

	other := &UpdateExec{}
	otherInput := other.getDatumRow(chk.GetRow(0), fields)
	if &otherInput[0] == &second[0] {
		t.Fatal("different UpdateExec instances share input row storage")
	}
}
