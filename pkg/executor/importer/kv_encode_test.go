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

func newKVEncoderTestTable(t *testing.T, createSQL string) table.Table {
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

func TestKVEncoderMissingTemporalDefaults(t *testing.T) {
	for _, tp := range []string{"date", "datetime", "timestamp"} {
		for _, tc := range []struct {
			name, definition, want string
			missingDefault         bool
		}{
			{"fixed", "not null default '2000-01-01'", "2000-01-01", false},
			{"nullable", "null", "", false},
			{"required", "not null", "", true},
			{"current", "not null default current_timestamp", "2009-02-13 23:31:30", false},
			{"explicit", "not null default '2000-01-01'", "2010-02-03", false},
			{"explicit null", "null default '2000-01-01'", "", false},
		} {
			if tp == "date" && tc.name == "current" {
				continue
			}
			t.Run(tp+"/"+tc.name, func(t *testing.T) {
				tbl := newKVEncoderTestTable(t, "create table t(id int primary key clustered, v "+tp+" "+tc.definition+")")
				cols := tbl.VisibleCols()
				ctrl := &importer.LoadDataController{
					ASTArgs: &importer.ASTArgs{}, Plan: &importer.Plan{}, Table: tbl,
					InsertColumns: cols,
					FieldMappings: []*importer.FieldMapping{{Column: cols[0]}, {Column: cols[1]}},
				}
				encoder, err := importer.NewTableKVEncoder(&encode.EncodingConfig{
					Table: tbl, Logger: log.L(),
					SessionOptions: encode.SessionOptions{SQLMode: mysql.ModeStrictAllTables, Timestamp: 1234567890, SysVars: map[string]string{"time_zone": "+00:00"}},
				}, ctrl)
				require.NoError(t, err)
				defer func() { require.NoError(t, encoder.Close()) }()
				input := []types.Datum{types.NewIntDatum(1)}
				if tc.name == "explicit" {
					input = append(input, types.NewStringDatum("2010-02-03"))
				} else if tc.name == "explicit null" {
					input = append(input, types.NewDatum(nil))
				}
				pairs, err := encoder.Encode(input, 1)
				if tc.missingDefault {
					require.ErrorContains(t, err, "doesn't have a default value")
					return
				}
				require.NoError(t, err)
				require.Len(t, pairs.Pairs, 1)
				handle, err := tablecodec.DecodeRowKey(pairs.Pairs[0].Key)
				require.NoError(t, err)
				row, _, err := tables.DecodeRawRowData(encoder.SessionCtx.GetExprCtx(), tbl, handle, cols, pairs.Pairs[0].Val)
				require.NoError(t, err)
				if tc.name == "nullable" || tc.name == "explicit null" {
					require.True(t, row[1].IsNull())
				} else {
					want := tc.want
					if (tc.name == "fixed" || tc.name == "explicit") && tp != "date" {
						want += " 00:00:00"
					}
					require.Equal(t, want, row[1].GetMysqlTime().String())
				}
			})
		}
	}
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
