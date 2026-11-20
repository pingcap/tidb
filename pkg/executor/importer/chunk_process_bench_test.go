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

package importer

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	lightningkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func BenchmarkChunkEncodePipeline(b *testing.B) {
	testCases := make([]chunkEncodeBenchmarkCase, 0, 5)
	for _, indexCount := range []int{0, 3, 16} {
		testCases = append(testCases, chunkEncodeBenchmarkCase{
			name:      fmt.Sprintf("NarrowInt/Indexes%d", indexCount),
			createSQL: chunkEncodeBenchmarkTableSQL(indexCount),
			row: []types.Datum{
				types.NewIntDatum(1),
				types.NewIntDatum(2),
				types.NewIntDatum(3),
				types.NewIntDatum(4),
			},
			sourceRowBytes: 8, // "1,2,3,4\n"
		})
	}
	testCases = append(testCases,
		chunkEncodeBenchmarkCase{
			name:      "WideString/Indexes3",
			createSQL: chunkEncodeWideStringTableSQL(),
			row: []types.Datum{
				types.NewIntDatum(1),
				types.NewIntDatum(42),
				types.NewStringDatum(strings.Repeat("c", 60)),
				types.NewStringDatum(strings.Repeat("p", 30)),
			},
			sourceRowBytes: 106,
		},
		chunkEncodeBenchmarkCase{
			name:           "WideInt/Indexes16",
			createSQL:      chunkEncodeWideIntTableSQL(),
			row:            chunkEncodeWideIntRow(),
			sourceRowBytes: 17 * 8,
		},
	)

	for _, testCase := range testCases {
		b.Run(testCase.name, func(b *testing.B) {
			benchmarkChunkEncodePipeline(b, testCase)
		})
	}
}

type chunkEncodeBenchmarkCase struct {
	name           string
	createSQL      string
	row            []types.Datum
	sourceRowBytes int64
}

func benchmarkChunkEncodePipeline(b *testing.B, testCase chunkEncodeBenchmarkCase) {
	tbl := newChunkEncodeBenchmarkTable(b, testCase.createSQL)
	encoder, err := NewTableKVEncoderForDupResolve(
		&encode.EncodingConfig{
			Table:           tbl,
			Logger:          log.L(),
			SkipEncodeRowID: true,
			SessionOptions: encode.SessionOptions{
				SQLMode:   mysql.ModeStrictAllTables,
				Timestamp: 1234567890,
			},
		},
		&LoadDataController{
			ASTArgs: &ASTArgs{},
			Plan:    &Plan{},
			Table:   tbl,
		},
	)
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, encoder.Close()) })

	rowNumber := 0
	readFn := func(context.Context, []types.Datum) (rowToEncode, bool, error) {
		if rowNumber >= b.N {
			return rowToEncode{}, true, nil
		}
		rowNumber++
		return rowToEncode{
			row:       testCase.row,
			rowID:     int64(rowNumber),
			endOffset: -1,
			startPos:  -1,
			resetFn:   func() {},
		}, false, nil
	}

	var encodedBytes int64
	sendFn := func(_ context.Context, batch *encodedKVGroupBatch) error {
		dataBytes, indexBytes := batch.groupChecksum.DataAndIndexSumSize()
		encodedBytes += int64(dataBytes + indexBytes)
		batch.reset()
		return nil
	}
	chunkEncoder := newChunkEncoder(
		"benchmark",
		readFn,
		-1,
		sendFn,
		nil,
		encoder,
		nil,
		nil,
	)

	b.SetBytes(testCase.sourceRowBytes)
	b.ReportAllocs()
	b.ResetTimer()
	require.NoError(b, chunkEncoder.encodeLoop(context.Background()))
	b.StopTimer()

	elapsed := b.Elapsed().Seconds()
	b.ReportMetric(float64(encodedBytes)/(1024*1024)/elapsed, "encoded-MiB/s")
	b.ReportMetric(float64(b.N)/elapsed, "row/s")
}

func newChunkEncodeBenchmarkTable(b *testing.B, createSQL string) table.Table {
	b.Helper()
	stmt, err := parser.New().ParseOneStmt(createSQL, "", "")
	require.NoError(b, err)
	tblInfo, err := ddl.MockTableInfo(utilmock.NewContext(), stmt.(*ast.CreateTableStmt), 1)
	require.NoError(b, err)
	tblInfo.State = model.StatePublic
	tbl, err := tables.TableFromMeta(lightningkv.NewPanickingAllocators(tblInfo.SepAutoInc()), tblInfo)
	require.NoError(b, err)
	return tbl
}

func chunkEncodeBenchmarkTableSQL(indexCount int) string {
	indexColumns := []string{
		"k1",
		"k2",
		"k3",
		"k1, k2",
		"k2, k1",
		"k1, k3",
		"k3, k1",
		"k2, k3",
		"k3, k2",
		"k1, k2, k3",
		"k1, k3, k2",
		"k2, k1, k3",
		"k2, k3, k1",
		"k3, k1, k2",
		"k3, k2, k1",
		"id, k1",
	}
	var builder strings.Builder
	builder.WriteString("create table t(id bigint primary key clustered, k1 bigint, k2 bigint, k3 bigint")
	for i, columns := range indexColumns[:indexCount] {
		fmt.Fprintf(&builder, ", key idx_%02d(%s)", i, columns)
	}
	builder.WriteByte(')')
	return builder.String()
}

func chunkEncodeWideStringTableSQL() string {
	return "create table t(" +
		"id bigint primary key clustered, k bigint, c varchar(60), pad varchar(30), " +
		"key idx_k(k), key idx_c(c), key idx_k_c(k, c))"
}

func chunkEncodeWideIntTableSQL() string {
	var builder strings.Builder
	builder.WriteString("create table t(id bigint primary key clustered")
	for i := range 16 {
		fmt.Fprintf(&builder, ", c%d bigint not null", i)
	}
	for i := range 16 {
		fmt.Fprintf(&builder, ", key idx_c%d(c%d)", i, i)
	}
	builder.WriteByte(')')
	return builder.String()
}

func chunkEncodeWideIntRow() []types.Datum {
	row := make([]types.Datum, 17)
	for i := range row {
		row[i] = types.NewIntDatum(int64(i + 1))
	}
	return row
}
