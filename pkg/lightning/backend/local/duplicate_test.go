// Copyright 2022 PingCAP, Inc.
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

package local_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	lkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestBuildDupTask(t *testing.T) {
	p := parser.New()
	node, _, err := p.ParseSQL("create table t (a int, b int, index idx(a), index idx(b));")
	require.NoError(t, err)
	info, err := ddl.MockTableInfo(mock.NewContext(), node[0].(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	info.State = model.StatePublic
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(info.SepAutoInc(), 0), info)
	require.NoError(t, err)

	// Test build duplicate detecting task.
	testCases := []struct {
		sessOpt       *encode.SessionOptions
		hasTableRange bool
	}{
		{&encode.SessionOptions{}, true},
		{&encode.SessionOptions{IndexID: info.Indices[0].ID}, false},
		{&encode.SessionOptions{IndexID: info.Indices[1].ID}, false},
	}
	for _, tc := range testCases {
		dupMgr, err := local.NewDupeDetector(tbl, "t", nil, nil, keyspace.CodecV1, nil,
			tc.sessOpt, 4, log.FromContext(context.Background()), "test", "lightning")
		require.NoError(t, err)
		tasks, err := local.BuildDuplicateTaskForTest(dupMgr)
		require.NoError(t, err)
		var hasRecordKey bool
		for _, task := range tasks {
			tableID, _, isRecordKey, err := tablecodec.DecodeKeyHead(task.StartKey)
			require.NoError(t, err)
			require.Equal(t, info.ID, tableID)
			if isRecordKey {
				hasRecordKey = true
			}
		}
		require.Equal(t, tc.hasTableRange, hasRecordKey)
	}
}

func buildTableForTestConvertToErrFoundConflictRecords(t *testing.T, node []ast.StmtNode) (table.Table, *lkv.Pairs) {
	mockSctx := mock.NewContext()
	info, err := ddl.MockTableInfo(mockSctx, node[0].(*ast.CreateTableStmt), 108)
	require.NoError(t, err)
	info.State = model.StatePublic
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(info.SepAutoInc(), 0), info)
	require.NoError(t, err)

	sessionOpts := encode.SessionOptions{
		SQLMode:   mysql.ModeStrictAllTables,
		Timestamp: 1234567890,
	}

	encoder, err := lkv.NewBaseKVEncoder(&encode.EncodingConfig{
		Table:          tbl,
		SessionOptions: sessionOpts,
		Logger:         log.L(),
	})
	require.NoError(t, err)
	encoder.SessionCtx.GetSessionVars().RowEncoder.Enable = true

	data1 := []types.Datum{
		types.NewIntDatum(1),
		types.NewIntDatum(6),
		types.NewStringDatum("1.csv"),
		types.NewIntDatum(101),
	}
	data2 := []types.Datum{
		types.NewIntDatum(2),
		types.NewIntDatum(6),
		types.NewStringDatum("2.csv"),
		types.NewIntDatum(102),
	}
	data3 := []types.Datum{
		types.NewIntDatum(3),
		types.NewIntDatum(7),
		types.NewStringDatum("3.csv"),
		types.NewIntDatum(103),
	}
	tctx := encoder.SessionCtx.GetTableCtx()
	_, err = encoder.Table.AddRecord(tctx, data1)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data2)
	require.NoError(t, err)
	_, err = encoder.Table.AddRecord(tctx, data3)
	require.NoError(t, err)
	return tbl, encoder.SessionCtx.TakeKvPairs()
}

func TestRetrieveKeyAndValueFromErrFoundDuplicateKeys(t *testing.T) {
	p := parser.New()
	node, _, err := p.ParseSQL("create table a (a int primary key, b int not null, c text, d int, key key_b(b));")
	require.NoError(t, err)

	_, kvPairs := buildTableForTestConvertToErrFoundConflictRecords(t, node)

	data1RowKey := kvPairs.Pairs[0].Key
	data1RowValue := kvPairs.Pairs[0].Val

	originalErr := common.ErrFoundDuplicateKeys.FastGenByArgs(data1RowKey, data1RowValue)
	rawKey, rawValue, err := local.RetrieveKeyAndValueFromErrFoundDuplicateKeys(originalErr)
	require.NoError(t, err)
	require.Equal(t, data1RowKey, rawKey)
	require.Equal(t, data1RowValue, rawValue)
}

func TestConvertToErrFoundConflictRecordsSingleColumnsIndex(t *testing.T) {
	p := parser.New()
	node, _, err := p.ParseSQL("create table a (a int primary key, b int not null, c text, d int, unique key key_b(b));")
	require.NoError(t, err)

	tbl, kvPairs := buildTableForTestConvertToErrFoundConflictRecords(t, node)

	data2RowKey := kvPairs.Pairs[2].Key
	data2RowValue := kvPairs.Pairs[2].Val
	data3IndexKey := kvPairs.Pairs[5].Key
	data3IndexValue := kvPairs.Pairs[5].Val

	originalErr := common.ErrFoundDuplicateKeys.FastGenByArgs(data2RowKey, data2RowValue)

	newErr := local.ConvertToErrFoundConflictRecords(originalErr, tbl)
	require.EqualError(t, newErr, "[Lightning:Restore:ErrFoundDataConflictRecords]found data conflict records in table a, primary key is '2', row data is '(2, 6, \"2.csv\", 102)'")

	originalErr = common.ErrFoundDuplicateKeys.FastGenByArgs(data3IndexKey, data3IndexValue)

	newErr = local.ConvertToErrFoundConflictRecords(originalErr, tbl)
	require.EqualError(t, newErr, "[Lightning:Restore:ErrFoundIndexConflictRecords]found index conflict records in table a, index name is 'a.key_b', unique key is '[7]', primary key is '3'")
}

func TestConvertToErrFoundConflictRecordsMultipleColumnsIndex(t *testing.T) {
	p := parser.New()
	node, _, err := p.ParseSQL("create table a (a int primary key, b int not null, c text, d int, unique key key_bd(b,d));")
	require.NoError(t, err)

	tbl, kvPairs := buildTableForTestConvertToErrFoundConflictRecords(t, node)

	data2RowKey := kvPairs.Pairs[2].Key
	data2RowValue := kvPairs.Pairs[2].Val
	data3IndexKey := kvPairs.Pairs[5].Key
	data3IndexValue := kvPairs.Pairs[5].Val

	originalErr := common.ErrFoundDuplicateKeys.FastGenByArgs(data2RowKey, data2RowValue)

	newErr := local.ConvertToErrFoundConflictRecords(originalErr, tbl)
	require.EqualError(t, newErr, "[Lightning:Restore:ErrFoundDataConflictRecords]found data conflict records in table a, primary key is '2', row data is '(2, 6, \"2.csv\", 102)'")

	originalErr = common.ErrFoundDuplicateKeys.FastGenByArgs(data3IndexKey, data3IndexValue)

	newErr = local.ConvertToErrFoundConflictRecords(originalErr, tbl)
	require.EqualError(t, newErr, "[Lightning:Restore:ErrFoundIndexConflictRecords]found index conflict records in table a, index name is 'a.key_bd', unique key is '[7 103]', primary key is '3'")
}
