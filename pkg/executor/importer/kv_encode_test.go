// Copyright 2025 PingCAP, Inc.
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

package importer_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	lightningkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestKVEncoderForDupResolve(t *testing.T) {
	table := newKVEncoderTestTable(t, "create table t(a bigint primary key nonclustered) SHARD_ROW_ID_BITS = 6")

	doTestFn := func(t *testing.T, useIdentityAutoRowID bool, checkerFn func(handleVal int64)) {
		encodeCfg := &encode.EncodingConfig{
			Table:                table,
			UseIdentityAutoRowID: useIdentityAutoRowID,
		}
		controller := &importer.LoadDataController{
			ASTArgs: &importer.ASTArgs{},
			Plan:    &importer.Plan{},
			Table:   table,
		}
		encoder, err := importer.NewTableKVEncoderForDupResolve(encodeCfg, controller)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, encoder.Close()) })
		for range 10 {
			pairs, err := encoder.Encode([]types.Datum{types.NewDatum(1)}, 1)
			require.NoError(t, err)
			require.Len(t, pairs.Pairs, 2)
			var metRecordKey bool
			for _, pair := range pairs.Pairs {
				if !tablecodec.IsRecordKey(pair.Key) {
					continue
				}
				metRecordKey = true
				handle, err := tablecodec.DecodeRowKey(pair.Key)
				require.NoError(t, err)
				checkerFn(handle.IntValue())
			}
			require.True(t, metRecordKey)
		}
	}

	t.Run("identity auto row id", func(t *testing.T) {
		doTestFn(t, true, func(handleVal int64) {
			require.EqualValues(t, 1, handleVal)
		})
	})

	t.Run("without identity auto row id", func(t *testing.T) {
		// we loop 10 times, at least one should have shard bit larger than 1
		var handleLargerThanOneCount int
		doTestFn(t, false, func(handleVal int64) {
			if handleVal > 1 {
				handleLargerThanOneCount++
			}
		})
		require.Greater(t, handleLargerThanOneCount, 1)
	})
}

func newKVEncoderTestTable(t testing.TB, createSQL string) table.Table {
	t.Helper()

	stmt, err := parser.New().ParseOneStmt(createSQL, "", "")
	require.NoError(t, err)
	tblInfo, err := ddl.MockTableInfo(utilmock.NewContext(), stmt.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	tblInfo.State = model.StatePublic
	tbl, err := tables.TableFromMeta(lightningkv.NewPanickingAllocators(tblInfo.SepAutoInc()), tblInfo)
	require.NoError(t, err)
	return tbl
}

func BenchmarkTableKVEncoder(b *testing.B) {
	testCases := []struct {
		name           string
		createSQL      string
		row            []types.Datum
		sourceRowBytes int64
	}{
		{
			name: "PKOnly",
			createSQL: `create table t(
				id bigint primary key clustered,
				k bigint not null,
				c varchar(60),
				pad varchar(30))`,
			row: []types.Datum{
				types.NewIntDatum(1),
				types.NewIntDatum(42),
				types.NewStringDatum(strings.Repeat("c", 60)),
				types.NewStringDatum(strings.Repeat("p", 30)),
			},
			sourceRowBytes: 110,
		},
		{
			name: "ThreeIndexes",
			createSQL: `create table t(
				id bigint primary key clustered,
				k bigint not null,
				c varchar(60),
				pad varchar(30),
				key idx_k(k),
				key idx_c(c),
				key idx_k_c(k, c))`,
			row: []types.Datum{
				types.NewIntDatum(1),
				types.NewIntDatum(42),
				types.NewStringDatum(strings.Repeat("c", 60)),
				types.NewStringDatum(strings.Repeat("p", 30)),
			},
			sourceRowBytes: 110,
		},
		{
			name:           "SixteenIndexes",
			createSQL:      sixteenIndexTableSQL(),
			row:            sixteenIndexRow(),
			sourceRowBytes: 17 * 8,
		},
	}

	for _, testCase := range testCases {
		b.Run(testCase.name, func(b *testing.B) {
			tbl := newKVEncoderTestTable(b, testCase.createSQL)
			encoder, err := importer.NewTableKVEncoderForDupResolve(
				&encode.EncodingConfig{
					Table:  tbl,
					Logger: log.L(),
					SessionOptions: encode.SessionOptions{
						SQLMode:   mysql.ModeStrictAllTables,
						Timestamp: 1234567890,
					},
				},
				&importer.LoadDataController{
					ASTArgs: &importer.ASTArgs{},
					Plan:    &importer.Plan{},
					Table:   tbl,
				},
			)
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, encoder.Close()) })

			var encodedBytes, kvCount int64
			b.SetBytes(testCase.sourceRowBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				pairs, err := encoder.Encode(testCase.row, int64(i+1))
				if err != nil {
					b.Fatal(err)
				}
				encodedBytes += int64(pairs.Size())
				kvCount += int64(len(pairs.Pairs))
				pairs.Clear()
			}
			b.StopTimer()

			elapsed := b.Elapsed().Seconds()
			b.ReportMetric(float64(encodedBytes)/(1024*1024)/elapsed, "encoded-MiB/s")
			b.ReportMetric(float64(kvCount)/elapsed, "kv/s")
		})
	}
}

func sixteenIndexTableSQL() string {
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

func sixteenIndexRow() []types.Datum {
	row := make([]types.Datum, 17)
	for i := range row {
		row[i] = types.NewIntDatum(int64(i + 1))
	}
	return row
}

func TestKVEncoderCastErrorMessage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(c1 tinyint)")

	do, err := session.GetDomain(store)
	require.NoError(t, err)
	table, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	encodeCfg := &encode.EncodingConfig{
		Table:  table,
		Logger: log.L(),
		SessionOptions: encode.SessionOptions{
			SQLMode:   mysql.ModeStrictAllTables,
			Timestamp: 1234567890,
		},
	}
	controller := &importer.LoadDataController{
		ASTArgs: &importer.ASTArgs{},
		Plan:    &importer.Plan{},
		Table:   table,
	}
	encoder, err := importer.NewTableKVEncoderForDupResolve(encodeCfg, controller)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, encoder.Close()) })

	_, err = encoder.Encode([]types.Datum{types.NewIntDatum(10000000)}, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "[Import:ErrCastValue]Value conversion failed for column 'c1'. Expected type: tinyint(4), received value: 10000000. Reason: [types:1690]constant 10000000 overflows tinyint")
}

func TestKVEncoderCastEnumErrorMessage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(c1 enum('a','b'))")

	do, err := session.GetDomain(store)
	require.NoError(t, err)
	table, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	encodeCfg := &encode.EncodingConfig{
		Table:  table,
		Logger: log.L(),
		SessionOptions: encode.SessionOptions{
			SQLMode:   mysql.ModeStrictAllTables,
			Timestamp: 1234567890,
		},
	}
	controller := &importer.LoadDataController{
		ASTArgs: &importer.ASTArgs{},
		Plan:    &importer.Plan{},
		Table:   table,
	}
	encoder, err := importer.NewTableKVEncoderForDupResolve(encodeCfg, controller)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, encoder.Close()) })

	_, err = encoder.Encode([]types.Datum{types.NewStringDatum("c")}, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "[Import:ErrCastValue]Value conversion failed for column 'c1'. Expected type: enum('a','b'), received value: \"c\". Reason:")
	require.Contains(t, err.Error(), "Data truncated")
}
