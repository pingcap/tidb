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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type partitionItemLookupForbiddenInfoSchema struct {
	infoschema.InfoSchema
	t *testing.T
}

func (is partitionItemLookupForbiddenInfoSchema) TableItemByPartitionID(partitionID int64) (infoschema.TableItem, bool) {
	is.t.Fatalf("TableItemByPartitionID should not be called for partition ID %d", partitionID)
	return infoschema.TableItem{}, false
}

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

func TestTableItemByIDForInitStatsAvoidsV1PartitionScan(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table normal (a int)")
	tk.MustExec("create table partitioned (a int) partition by hash(a) partitions 2")

	normalInfo := dom.MustGetTableInfo(t, "test", "normal")
	partitionedInfo := dom.MustGetTableInfo(t, "test", "partitioned")
	partitionInfo := partitionedInfo.GetPartitionInfo()
	require.NotNil(t, partitionInfo)
	require.NotEmpty(t, partitionInfo.Definitions)
	partitionID := partitionInfo.Definitions[0].ID

	is := partitionItemLookupForbiddenInfoSchema{
		InfoSchema: infoschema.MockInfoSchemaWithSchemaVer([]*model.TableInfo{normalInfo, partitionedInfo}, 1),
		t:          t,
	}
	getter := util.NewTableInfoGetter()

	item, ok := getter.TableItemByIDForInitStats(is, normalInfo.ID)
	require.True(t, ok)
	require.Equal(t, "normal", item.TableName.L)

	item, ok = getter.TableItemByIDForInitStats(is, partitionID)
	require.True(t, ok)
	require.Equal(t, "partitioned", item.TableName.L)

	_, ok = getter.TableItemByIDForInitStats(is, 1<<60)
	require.False(t, ok)
}

// failingStatsGlobalAccessor fails after the first statistics settings have been applied.
type failingStatsGlobalAccessor struct{ variable.GlobalVarAccessor }

func (a failingStatsGlobalAccessor) GetGlobalSysVar(name string) (string, error) {
	if name == vardef.TiDBAnalyzeVersion {
		return "", errors.New("statistics settings unavailable")
	}
	return a.GlobalVarAccessor.GetGlobalSysVar(name)
}

func TestUpdateSCtxVarsForStatsWithRestore(t *testing.T) {
	t.Run("unloaded session variables", func(t *testing.T) {
		sctx := mock.NewContext()
		vars := sctx.GetSessionVars()
		vars.GlobalVarsAccessor = variable.NewMockGlobalAccessor4Tests()
		_, loaded := vars.GetSystemVar(vardef.TiDBAnalyzeStoreBatchSize)
		require.False(t, loaded)
		restore, err := util.UpdateSCtxVarsForStatsWithRestore(sctx)
		require.NoError(t, err)
		restore()
		value, loaded := vars.GetSystemVar(vardef.TiDBAnalyzeStoreBatchSize)
		require.True(t, loaded)
		require.Equal(t, fmt.Sprint(vardef.DefTiDBAnalyzeStoreBatchSize), value)
		require.Equal(t, vardef.DefTiDBAnalyzeStoreBatchSize, vars.AnalyzeStoreBatchSize)
	})
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			sctx := mock.NewContext()
			vars := sctx.GetSessionVars()
			accessor := variable.NewMockGlobalAccessor4Tests()
			vars.GlobalVarsAccessor = accessor
			require.NoError(t, accessor.SetGlobalSysVar(context.Background(), vardef.TiDBAnalyzePartitionConcurrency, "3"))
			require.NoError(t, accessor.SetGlobalSysVar(context.Background(), vardef.TiDBAnalyzeVersion, "2"))
			require.NoError(t, accessor.SetGlobalSysVar(context.Background(), vardef.TiDBAnalyzeStoreBatchSize, "6"))
			require.NoError(t, accessor.SetGlobalSysVar(context.Background(), vardef.TimeZone, "+03:00"))
			require.NoError(t, vars.SetSystemVar(vardef.TimeZone, "+08:00"))
			require.NoError(t, vars.SetSystemVar(vardef.TiDBAnalyzeStoreBatchSize, "7"))
			vars.StmtCtx.SetTimeZone(time.UTC)
			vars.AnalyzePartitionConcurrency = 1
			vars.AnalyzeVersion = 1
			vars.EnableAsyncMergeGlobalStats = false
			vars.AnalyzeSkipColumnTypes = map[string]struct{}{"json": {}}
			originTZ := vars.TimeZone
			if fail {
				vars.GlobalVarsAccessor = failingStatsGlobalAccessor{accessor}
			}
			restore, err := util.UpdateSCtxVarsForStatsWithRestore(sctx)
			if fail {
				require.ErrorContains(t, err, "statistics settings unavailable")
				require.Nil(t, restore)
			} else {
				require.NoError(t, err)
				require.Equal(t, 3, vars.AnalyzePartitionConcurrency)
				require.Equal(t, 6, vars.AnalyzeStoreBatchSize)
				require.Equal(t, 3*3600, func() int { _, offset := time.Now().In(vars.TimeZone).Zone(); return offset }())
				restore()
			}
			require.Equal(t, 1, vars.AnalyzePartitionConcurrency)
			require.Equal(t, 1, vars.AnalyzeVersion)
			require.Equal(t, 7, vars.AnalyzeStoreBatchSize)
			require.False(t, vars.EnableAsyncMergeGlobalStats)
			require.Equal(t, map[string]struct{}{"json": {}}, vars.AnalyzeSkipColumnTypes)
			require.Same(t, originTZ, vars.TimeZone)
			require.Same(t, time.UTC, vars.StmtCtx.TimeZone())
		})
	}
}
