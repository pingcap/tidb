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

	"github.com/pingcap/tidb/pkg/types"
)

var updateCloneRowBenchmarkSink []types.Datum

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
