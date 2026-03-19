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
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

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
	_, dom := testkit.CreateMockStoreAndDomain(t)

	var sctxWithFailure sessionctx.Context
	err := util.CallWithSCtx(dom.StatsHandle().SPool(), func(sctx sessionctx.Context) error {
		sctxWithFailure = sctx
		return errors.New("simulated error")
	})
	require.Error(t, err)
	require.Equal(t, "simulated error", err.Error())
	notReleased := infosync.ContainsInternalSession(sctxWithFailure)
	require.False(t, notReleased)
}

func TestCallWithSCtxSyncsStmtCtxTimeZone(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	pool := dom.StatsHandle().SPool()
	originTZ := fmt.Sprint(tk.MustQuery("select @@global.time_zone").Rows()[0][0])
	defer tk.MustExec("set @@global.time_zone='" + originTZ + "'")

	tk.MustExec("set @@global.time_zone='UTC'")
	var oldStmtTZ string
	err := util.CallWithSCtx(pool, func(sctx sessionctx.Context) error {
		// Execute a statement to make StmtCtx pick up the current session time zone (UTC).
		if _, _, err := util.ExecRows(sctx, "select 1"); err != nil {
			return err
		}
		oldStmtTZ = sctx.GetSessionVars().StmtCtx.TimeZone().String()
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, oldStmtTZ)

	tk.MustExec("set @@global.time_zone='Asia/Shanghai'")
	var varsTZ string
	var stmtTZ string
	err = util.CallWithSCtx(pool, func(sctx sessionctx.Context) error {
		// No SQL execution here; some stats paths read StmtCtx directly without SQL.
		// Example: AsyncMergePartitionStats2GlobalStats.MergePartitionStats2GlobalStats
		// reads sctx.GetSessionVars().StmtCtx.TimeZone() (global_stats_async.go:315)
		// before any SQL is executed.
		varsTZ = sctx.GetSessionVars().Location().String()
		stmtTZ = sctx.GetSessionVars().StmtCtx.TimeZone().String()
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, varsTZ)
	require.NotEmpty(t, stmtTZ)
	require.NotEqual(t, oldStmtTZ, varsTZ)
	require.Equal(t, varsTZ, stmtTZ)
}
