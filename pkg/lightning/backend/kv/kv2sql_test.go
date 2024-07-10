// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestIterRawIndexKeysClusteredPK(t *testing.T) {
	p := parser.New()
	node, _, err := p.ParseSQL("create table t (a varchar(10) primary key, b int, index idx(b));")
	require.NoError(t, err)
	mockSctx := mock.NewContext()
	mockSctx.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	info, err := ddl.MockTableInfo(mockSctx, node[0].(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	info.State = model.StatePublic
	require.True(t, info.IsCommonHandle)
	tbl, err := tables.TableFromMeta(kv.NewPanickingAllocators(info.SepAutoInc(), 0), info)
	require.NoError(t, err)

	sessionOpts := &encode.SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	}
	decoder, err := kv.NewTableKVDecoder(tbl, "`test`.`c1`", sessionOpts, log.L())
	require.NoError(t, err)

	sctx := kv.NewSession(sessionOpts, log.L())
	handle, err := tbl.AddRecord(sctx.GetTableCtx(), []types.Datum{types.NewIntDatum(1), types.NewIntDatum(2)})
	require.NoError(t, err)
	paris := sctx.TakeKvPairs()
	require.Len(t, paris.Pairs, 2)
	_, rowValue := paris.Pairs[0].Key, paris.Pairs[0].Val
	idxKey := paris.Pairs[1].Key

	deleteKeys := make([][]byte, 0, 2)
	err = decoder.IterRawIndexKeys(handle, rowValue, func(bs []byte) error {
		deleteKeys = append(deleteKeys, bs)
		return nil
	})
	require.NoError(t, err)

	expected := [][]byte{idxKey}
	require.Equal(t, expected, deleteKeys)
}

func TestIterRawIndexKeysIntPK(t *testing.T) {
	p := parser.New()
	node, _, err := p.ParseSQL("create table t (a int primary key, b int, index idx(b));")
	require.NoError(t, err)
	mockSctx := mock.NewContext()
	mockSctx.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	info, err := ddl.MockTableInfo(mockSctx, node[0].(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	info.State = model.StatePublic
	require.True(t, info.PKIsHandle)
	tbl, err := tables.TableFromMeta(kv.NewPanickingAllocators(info.SepAutoInc(), 0), info)
	require.NoError(t, err)

	sessionOpts := &encode.SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	}
	decoder, err := kv.NewTableKVDecoder(tbl, "`test`.`c1`", sessionOpts, log.L())
	require.NoError(t, err)

	sctx := kv.NewSession(sessionOpts, log.L())
	handle, err := tbl.AddRecord(sctx.GetTableCtx(), []types.Datum{types.NewIntDatum(1), types.NewIntDatum(2)})
	require.NoError(t, err)
	paris := sctx.TakeKvPairs()
	require.Len(t, paris.Pairs, 2)
	_, rowValue := paris.Pairs[0].Key, paris.Pairs[0].Val
	idxKey := paris.Pairs[1].Key

	deleteKeys := make([][]byte, 0, 2)
	err = decoder.IterRawIndexKeys(handle, rowValue, func(bs []byte) error {
		deleteKeys = append(deleteKeys, bs)
		return nil
	})
	require.NoError(t, err)

	expected := [][]byte{idxKey}
	require.Equal(t, expected, deleteKeys)
}
