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

package contextimpl_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/binloginfo"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table/contextimpl"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/stretchr/testify/require"
)

func TestMutateContextImplFields(t *testing.T) {
	sctx := mock.NewContext()
	sctx.Mutations = make(map[int64]*binlog.TableMutation)
	ctx := contextimpl.NewTableContextImpl(sctx)
	// expression
	require.True(t, sctx.GetExprCtx() == ctx.GetExprCtx())
	// binlog
	sctx.GetSessionVars().BinlogClient = nil
	binlogSupport, ok := ctx.GetBinlogSupport()
	require.False(t, ok)
	require.Nil(t, binlogSupport)
	sctx.GetSessionVars().BinlogClient = binloginfo.MockPumpsClient(&testkit.MockPumpClient{})
	binlogSupport, ok = ctx.GetBinlogSupport()
	require.True(t, ok)
	require.NotNil(t, binlogSupport)
	binlogMutation := binlogSupport.GetBinlogMutation(1234)
	require.NotNil(t, binlogMutation)
	require.Same(t, sctx.StmtGetMutation(1234), binlogMutation)
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
}
