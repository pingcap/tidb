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

package tblsession_test

import (
	"testing"

	_ "github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	_ "github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/table/tblsession"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type mockTemporaryData struct {
	variable.TemporaryTableData
	size int64
}

func (m *mockTemporaryData) GetTableSize(tableID int64) int64 {
	return tableID*1000000 + m.size
}

func TestSessionMutateContextFields(t *testing.T) {
	sctx := mock.NewContext()
	ctx := tblsession.NewMutateContext(sctx)
	// expression
	require.True(t, sctx.GetExprCtx() == ctx.GetExprCtx())
	// ConnectionID
	sctx.GetSessionVars().ConnectionID = 12345
	require.Equal(t, uint64(12345), ctx.ConnectionID())
	// restricted SQL
	sctx.GetSessionVars().InRestrictedSQL = false
	require.False(t, ctx.InRestrictedSQL())
	sctx.GetSessionVars().InRestrictedSQL = true
	require.True(t, ctx.InRestrictedSQL())
	// AssertionLevel
	ctx.GetSessionVars().AssertionLevel = variable.AssertionLevelFast
	require.Equal(t, variable.AssertionLevelFast, ctx.TxnAssertionLevel())
	ctx.GetSessionVars().AssertionLevel = variable.AssertionLevelStrict
	require.Equal(t, variable.AssertionLevelStrict, ctx.TxnAssertionLevel())
	// EnableMutationChecker
	ctx.GetSessionVars().EnableMutationChecker = true
	require.True(t, ctx.EnableMutationChecker())
	ctx.GetSessionVars().EnableMutationChecker = false
	require.False(t, ctx.EnableMutationChecker())
	// encoding config
	sctx.GetSessionVars().EnableRowLevelChecksum = true
	sctx.GetSessionVars().RowEncoder.Enable = true
	sctx.GetSessionVars().InRestrictedSQL = false
	cfg := ctx.GetRowEncodingConfig()
	require.True(t, cfg.IsRowLevelChecksumEnabled)
	require.Equal(t, sctx.GetSessionVars().IsRowLevelChecksumEnabled(), cfg.IsRowLevelChecksumEnabled)
	require.True(t, cfg.IsRowLevelChecksumEnabled)
	require.Same(t, &sctx.GetSessionVars().RowEncoder, cfg.RowEncoder)
	sctx.GetSessionVars().RowEncoder.Enable = false
	cfg = ctx.GetRowEncodingConfig()
	require.False(t, cfg.IsRowLevelChecksumEnabled)
	require.Equal(t, sctx.GetSessionVars().IsRowLevelChecksumEnabled(), cfg.IsRowLevelChecksumEnabled)
	require.Same(t, &sctx.GetSessionVars().RowEncoder, cfg.RowEncoder)
	require.False(t, cfg.IsRowLevelChecksumEnabled)
	sctx.GetSessionVars().RowEncoder.Enable = true
	sctx.GetSessionVars().InRestrictedSQL = true
	require.Equal(t, sctx.GetSessionVars().IsRowLevelChecksumEnabled(), cfg.IsRowLevelChecksumEnabled)
	require.False(t, cfg.IsRowLevelChecksumEnabled)
	sctx.GetSessionVars().InRestrictedSQL = false
	sctx.GetSessionVars().EnableRowLevelChecksum = false
	require.Equal(t, sctx.GetSessionVars().IsRowLevelChecksumEnabled(), cfg.IsRowLevelChecksumEnabled)
	// mutate buffers
	require.NotNil(t, ctx.GetMutateBuffers())
	// RowIDShardGenerator
	sctx.GetSessionVars().TxnCtx.StartTS = 123
	require.Same(t, sctx.GetSessionVars().GetRowIDShardGenerator(), ctx.GetRowIDShardGenerator())
	// ReservedRowIDAlloc
	reserved, ok := ctx.GetReservedRowIDAlloc()
	require.True(t, ok)
	require.Same(t, &sctx.GetSessionVars().StmtCtx.ReservedRowIDAlloc, reserved)
	// statistics support
	txnCtx := sctx.GetSessionVars().TxnCtx
	txnCtx.TableDeltaMap = make(map[int64]variable.TableDelta)
	sctx.GetSessionVars().TxnCtx = nil
	statisticsSupport, ok := ctx.GetStatisticsSupport()
	require.False(t, ok)
	require.Nil(t, statisticsSupport)
	sctx.GetSessionVars().TxnCtx = txnCtx
	statisticsSupport, ok = ctx.GetStatisticsSupport()
	require.True(t, ok)
	require.NotNil(t, statisticsSupport)
	require.Equal(t, 0, len(txnCtx.TableDeltaMap))
	statisticsSupport.UpdatePhysicalTableDelta(
		12, 1, 2, variable.DeltaColsMap(map[int64]int64{3: 4, 5: 6}),
	)
	require.Equal(t, 1, len(txnCtx.TableDeltaMap))
	deltaMap := txnCtx.TableDeltaMap[12]
	require.Equal(t, int64(12), deltaMap.TableID)
	require.Equal(t, int64(1), deltaMap.Delta)
	require.Equal(t, int64(2), deltaMap.Count)
	require.Equal(t, map[int64]int64{3: 4, 5: 6}, deltaMap.ColSize)
	// cached table support
	sctx.GetSessionVars().TxnCtx = nil
	cachedTableSupport, ok := ctx.GetCachedTableSupport()
	require.False(t, ok)
	require.Nil(t, cachedTableSupport)
	sctx.GetSessionVars().TxnCtx = txnCtx
	cachedTableSupport, ok = ctx.GetCachedTableSupport()
	require.True(t, ok)
	type mockCachedTable struct {
		table.CachedTable
	}
	handle := &mockCachedTable{}
	require.Nil(t, sctx.GetSessionVars().TxnCtx.CachedTables[123])
	cachedTableSupport.AddCachedTableHandleToTxn(123, handle)
	cached := sctx.GetSessionVars().TxnCtx.CachedTables[123]
	require.Same(t, handle, cached)
	// temporary table support
	sctx.GetSessionVars().TxnCtx = nil
	tempTableSupport, ok := ctx.GetTemporaryTableSupport()
	require.False(t, ok)
	require.Nil(t, tempTableSupport)
	sctx.GetSessionVars().TxnCtx = txnCtx
	mockTempData := &mockTemporaryData{}
	sctx.GetSessionVars().TemporaryTableData = mockTempData
	tempTableSupport, ok = ctx.GetTemporaryTableSupport()
	require.True(t, ok)
	require.Nil(t, txnCtx.TemporaryTables[456])
	tmpTblHandler, ok := tempTableSupport.AddTemporaryTableToTxn(&model.TableInfo{
		ID:            456,
		TempTableType: model.TempTableGlobal,
	})
	require.True(t, ok)
	require.NotNil(t, tmpTblHandler)
	tmpTblTable := txnCtx.TemporaryTables[456]
	require.NotNil(t, tmpTblTable)
	require.True(t, tmpTblTable.GetModified())
	require.Equal(t, int64(456000000), tmpTblHandler.GetCommittedSize())
	mockTempData.size = 111
	require.Equal(t, int64(456000111), tmpTblHandler.GetCommittedSize())
	require.Equal(t, int64(0), tmpTblHandler.GetDirtySize())
	tmpTblHandler.UpdateTxnDeltaSize(333)
	require.Equal(t, int64(333), tmpTblHandler.GetDirtySize())
	tmpTblHandler.UpdateTxnDeltaSize(-1)
	require.Equal(t, int64(332), tmpTblHandler.GetDirtySize())
	exchange, ok := ctx.GetExchangePartitionDMLSupport()
	require.True(t, ok)
	require.Same(t, ctx.GetDomainInfoSchema(), exchange.GetInfoSchemaToCheckExchangeConstraint())
}
