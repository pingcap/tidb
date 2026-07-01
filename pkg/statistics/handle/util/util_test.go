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

package util_test

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type recordingSessionPool struct {
	se        pools.Resource
	destroyed int
}

func (p recordingSessionPool) Get() (pools.Resource, error) { return p.se, nil }
func (recordingSessionPool) Put(pools.Resource)             {}
func (recordingSessionPool) Close()                         {}
func (p *recordingSessionPool) Destroy(pools.Resource)      { p.destroyed++ }

func TestIsSpecialGlobalIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(
		"create table t(" +
			"	a int," +
			"	b int," +
			"	c int," +
			"	d varchar(20)," +
			"	unique index b(b) global," +
			"	index c(c)," +
			"	unique index ub_s((b+1)) global," +
			"	unique index ud_s(d(3)) global," +
			"	index b_s((b+1))," +
			"	index d_s(d(3))" +
			") partition by hash(a) partitions 5")

	tblInfo := dom.MustGetTableInfo(t, "test", "t")
	cnt := 0
	for _, idx := range tblInfo.Indices {
		switch idx.Name.O {
		case "b", "c", "b_s", "d_s":
			cnt++
			require.False(t, util.IsSpecialGlobalIndex(idx, tblInfo))
		case "ub_s", "ud_s":
			cnt++
			require.True(t, util.IsSpecialGlobalIndex(idx, tblInfo))
		}
	}
	require.Equal(t, cnt, len(tblInfo.Indices))
}

func TestCallSCtxFailed(t *testing.T) {
	var sctxWithFailure sessionctx.Context
	pool := &recordingSessionPool{se: mock.NewContext()}
	err := util.CallWithSCtx(pool, func(sctx sessionctx.Context) error {
		sctxWithFailure = sctx
		return stderrors.New("simulated error")
	})
	require.Error(t, err)
	require.Equal(t, "simulated error", err.Error())
	require.NotNil(t, sctxWithFailure)
	require.Equal(t, 1, pool.destroyed)
}

func TestCallWithSCtxRestoresStatsSessionVars(t *testing.T) {
	pool := &recordingSessionPool{se: mock.NewContext()}
	sctx := pool.se.(sessionctx.Context)
	vars := sctx.GetSessionVars()
	vars.GlobalVarsAccessor = variable.NewMockGlobalAccessor4Tests()
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBAnalyzePartitionConcurrency, "3"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBAnalyzeVersion, "2"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBEnableAnalyzeSnapshot, "on"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBPartitionPruneMode, "dynamic"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBAnalyzeSkipColumnTypes, "blob"))
	vars.AnalyzePartitionConcurrency = 0
	vars.AnalyzeVersion = variable.DefTiDBAnalyzeVersion
	vars.EnableAnalyzeSnapshot = false
	vars.PartitionPruneMode.Store("static")
	vars.AnalyzeSkipColumnTypes = map[string]struct{}{"json": {}}
	oldAnalyzeSkipColumnTypes := vars.AnalyzeSkipColumnTypes

	restoreErr := util.CallWithSCtx(pool, func(sctx sessionctx.Context) error {
		vars := sctx.GetSessionVars()
		require.Equal(t, 3, vars.AnalyzePartitionConcurrency)
		require.Equal(t, 2, vars.AnalyzeVersion)
		require.True(t, vars.EnableAnalyzeSnapshot)
		require.Equal(t, string(variable.Dynamic), vars.PartitionPruneMode.Load())
		require.Equal(t, map[string]struct{}{"blob": {}}, vars.AnalyzeSkipColumnTypes)
		return nil
	})
	require.NoError(t, restoreErr)

	require.Equal(t, 0, vars.AnalyzePartitionConcurrency)
	require.Equal(t, variable.DefTiDBAnalyzeVersion, vars.AnalyzeVersion)
	require.False(t, vars.EnableAnalyzeSnapshot)
	require.Equal(t, "static", vars.PartitionPruneMode.Load())
	require.Equal(t, oldAnalyzeSkipColumnTypes, vars.AnalyzeSkipColumnTypes)
}
