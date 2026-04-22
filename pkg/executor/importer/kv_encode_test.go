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

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestKVEncoderForDupResolve(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a bigint primary key nonclustered) SHARD_ROW_ID_BITS = 6")
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	table, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

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
		for range 10 {
			pairs, err := encoder.Encode([]types.Datum{types.NewDatum(1)}, nil, 1)
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

	_, err = encoder.Encode([]types.Datum{types.NewIntDatum(10000000)}, nil, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "[Import:ErrCastValue]Value conversion failed for column 'c1'. Expected type: tinyint(4), received value: 10000000. Reason: [types:1690]constant 10000000 overflows tinyint")
}

// TestKVEncoderNotNullInsertColumn verifies that the insert-column loop still
// enforces NOT-NULL via CheckNotNull / HandleBadNull after the single-pass
// buildRecord refactor. This is the common path for parquet imports because
// definition-level NULL cells produce skipCast=true, so the bypass path must
// not skip NOT-NULL enforcement.
func TestKVEncoderNotNullInsertColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(c1 int not null)")

	do, err := session.GetDomain(store)
	require.NoError(t, err)
	tbl, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	run := func(t *testing.T, sqlMode mysql.SQLMode, input types.Datum, skipCast []bool) (*kv.Pairs, error) {
		encodeCfg := &encode.EncodingConfig{
			Table:  tbl,
			Logger: log.L(),
			SessionOptions: encode.SessionOptions{
				SQLMode:   sqlMode,
				Timestamp: 1234567890,
			},
		}
		controller := &importer.LoadDataController{
			ASTArgs: &importer.ASTArgs{},
			Plan:    &importer.Plan{},
			Table:   tbl,
		}
		encoder, err := importer.NewTableKVEncoderForDupResolve(encodeCfg, controller)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, encoder.Close()) })
		return encoder.Encode([]types.Datum{input}, skipCast, 1)
	}

	t.Run("strict mode rejects null via cast", func(t *testing.T) {
		_, err := run(t, mysql.ModeStrictAllTables, types.NewDatum(nil), nil)
		require.Error(t, err)
	})

	t.Run("strict mode rejects null via skipCast", func(t *testing.T) {
		// skipCast=true simulates the parquet NULL path; the encoder must
		// still enforce NOT-NULL instead of silently storing NULL.
		_, err := run(t, mysql.ModeStrictAllTables, types.NewDatum(nil), []bool{true})
		require.Error(t, err)
	})

	t.Run("non-strict mode substitutes zero value via skipCast", func(t *testing.T) {
		pairs, err := run(t, mysql.SQLMode(0), types.NewDatum(nil), []bool{true})
		require.NoError(t, err)
		require.NotEmpty(t, pairs.Pairs)
	})
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

	_, err = encoder.Encode([]types.Datum{types.NewStringDatum("c")}, nil, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "[Import:ErrCastValue]Value conversion failed for column 'c1'. Expected type: enum('a','b'), received value: \"c\". Reason:")
	require.Contains(t, err.Error(), "Data truncated")
}
