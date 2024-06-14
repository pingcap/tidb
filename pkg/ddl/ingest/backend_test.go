// Copyright 2024 PingCAP, Inc.
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

package ingest_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	lkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestHandleErrorAfterCollectRemoteDuplicateRows(t *testing.T) {
	p := parser.New()
	node, _, err := p.ParseSQL("create table a (a int primary key, b int not null, c text, d int, unique key key_bd(b,d));")
	require.NoError(t, err)

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

	kvPairs := encoder.SessionCtx.TakeKvPairs()
	//data2RowKey := kvPairs.Pairs[2].Key
	//data2RowValue := kvPairs.Pairs[2].Val
	data3IndexKey := kvPairs.Pairs[5].Key
	data3IndexValue := kvPairs.Pairs[5].Val

	originalErr := common.ErrFoundDuplicateKeys.FastGenByArgs(data3IndexKey, data3IndexValue)

	newErr := local.ConvertToErrFoundConflictRecords(originalErr, tbl)
	require.EqualError(t, newErr, "[Lightning:Restore:ErrFoundIndexConflictRecords]found index conflict records in table a, index name is 'a.key_bd', unique key is '[7 103]', primary key is '3'")

	store := realtikvtest.CreateMockStoreAndSetup(t)
	discovery := store.(tikv.Storage).GetRegionCache().PDClient().GetServiceDiscovery()
	bCtx, err := ingest.LitBackCtxMgr.Register(context.Background(), 1, false, nil, discovery, "test")
	require.NoError(t, err)

	newErr = bCtx.HandleErrorAfterCollectRemoteDuplicateRows(newErr, 0, tbl, true)
	fmt.Println(newErr)
}
