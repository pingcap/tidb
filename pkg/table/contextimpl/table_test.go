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
	require.False(t, ctx.BinlogEnabled())
	sctx.GetSessionVars().BinlogClient = binloginfo.MockPumpsClient(&testkit.MockPumpClient{})
	require.True(t, ctx.BinlogEnabled())
	binlogMutation := ctx.GetBinlogMutation(1234)
	require.NotNil(t, binlogMutation)
	require.Same(t, sctx.StmtGetMutation(1234), binlogMutation)
	// restricted SQL
	sctx.GetSessionVars().StmtCtx.InRestrictedSQL = false
	require.False(t, ctx.InRestrictedSQL())
	sctx.GetSessionVars().StmtCtx.InRestrictedSQL = true
	require.True(t, ctx.InRestrictedSQL())
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
}
