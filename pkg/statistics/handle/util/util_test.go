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
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session/syssession"
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
	// partitionMapBuilds counts the scans of all tables. On InfoSchema V1, only the build of
	// the partition map does such a scan through this wrapper.
	partitionMapBuilds *int
}

func (is partitionItemLookupForbiddenInfoSchema) ListTablesWithSpecialAttribute(filter infoschemacontext.SpecialAttributeFilter) []infoschemacontext.TableInfoResult {
	*is.partitionMapBuilds++
	return is.InfoSchema.ListTablesWithSpecialAttribute(filter)
}

func (is partitionItemLookupForbiddenInfoSchema) TableItemByPartitionID(partitionID int64) (infoschema.TableItem, bool) {
	is.t.Fatalf("TableItemByPartitionID should not be called for partition ID %d", partitionID)
	return infoschema.TableItem{}, false
}

func (is partitionItemLookupForbiddenInfoSchema) TableIDByPartitionID(partitionID int64) (int64, bool) {
	is.t.Fatalf("TableIDByPartitionID should not be called for partition ID %d", partitionID)
	return 0, false
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
	store, dom := testkit.CreateMockStoreAndDomain(t)

	var sctxWithFailure sessionctx.Context
	err := util.CallWithSCtx(dom.StatsHandle().SPool(), func(sctx sessionctx.Context) error {
		sctxWithFailure = sctx
		return errors.New("simulated error")
	})
	require.Error(t, err)
	require.Equal(t, "simulated error", err.Error())
	notReleased := infosync.ContainsInternalSession(sctxWithFailure)
	require.False(t, notReleased)

	// A panic in the function is recovered and returned as an error.
	err = util.CallWithSCtx(dom.StatsHandle().SPool(), func(sessionctx.Context) error {
		panic("simulated panic")
	})
	require.ErrorContains(t, err, "panic in CallWithSCtx")

	// A panic in the function of WrapTxn rolls back the writes made before the panic.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t (a int)")
	err = util.CallWithSCtx(dom.StatsHandle().SPool(), func(sctx sessionctx.Context) error {
		_, _, err := util.ExecRows(sctx, "insert into test.t values (1)")
		require.NoError(t, err)
		panic("simulated panic")
	}, util.FlagWrapTxn)
	require.ErrorContains(t, err, "panic in CallWithSCtx")
	tk.MustQuery("select count(*) from test.t").Check(testkit.Rows("0"))
}

func newStatsSessionContextForRestoreTest(t *testing.T) *mock.Context {
	t.Helper()
	sctx := mock.NewContext()
	vars := sctx.GetSessionVars()
	accessor := variable.NewMockGlobalAccessor4Tests()
	vars.GlobalVarsAccessor = accessor
	for _, setting := range []struct{ name, global, session string }{
		{vardef.TiDBEnableAsyncMergeGlobalStats, "ON", "OFF"},
		{vardef.TiDBAnalyzePartitionConcurrency, "3", "1"},
		{vardef.TiDBAnalyzeVersion, "2", "2"},
		{vardef.TiDBAnalyzeStoreBatchSize, "6", "7"},
		{vardef.TiDBPartitionPruneMode, "dynamic", "static"},
		{vardef.TiDBEnableAnalyzeSnapshot, "ON", "OFF"},
		{vardef.TiDBAnalyzeSkipColumnTypes, "blob", "json"},
		{vardef.TiDBSkipMissingPartitionStats, "ON", "OFF"},
		{vardef.InnodbLockWaitTimeout, "12", "45"},
		{vardef.TimeZone, "+03:00", "+08:00"},
	} {
		require.NoError(t, accessor.SetGlobalSysVar(context.Background(), setting.name, setting.global))
		if variable.GetSysVar(setting.name).HasSessionScope() {
			require.NoError(t, vars.SetSystemVar(setting.name, setting.session))
		}
	}
	vars.EnableAsyncMergeGlobalStats = false
	vars.AnalyzeVersion = 1
	vars.StmtCtx.SetTimeZone(time.UTC)
	return sctx
}

func checkRestoredStatsSessionVars(t *testing.T, sctx sessionctx.Context) {
	t.Helper()
	vars := sctx.GetSessionVars()
	require.False(t, vars.EnableAsyncMergeGlobalStats)
	require.Equal(t, 1, vars.AnalyzePartitionConcurrency)
	require.Equal(t, 1, vars.AnalyzeVersion)
	require.Equal(t, 7, vars.AnalyzeStoreBatchSize)
	require.Equal(t, "static", vars.PartitionPruneMode.Load())
	require.False(t, vars.EnableAnalyzeSnapshot)
	require.Equal(t, map[string]struct{}{"json": {}}, vars.AnalyzeSkipColumnTypes)
	require.False(t, vars.SkipMissingPartitionStats)
	require.Equal(t, int64(45000), vars.LockWaitTimeout)
	_, offset := time.Now().In(vars.TimeZone).Zone()
	require.Equal(t, 8*3600, offset)
	require.Same(t, time.UTC, vars.StmtCtx.TimeZone())
	for name, expected := range map[string]string{
		vardef.TiDBAnalyzeStoreBatchSize: "7",
		vardef.TimeZone:                  "+08:00",
	} {
		value, loaded := vars.GetSystemVar(name)
		require.True(t, loaded)
		require.Equal(t, expected, value)
	}
}

func TestCallWithSCtxRestoresStatsSessionVars(t *testing.T) {
	for _, failCallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("callback_error=%v", failCallback), func(t *testing.T) {
			sctx := newStatsSessionContextForRestoreTest(t)
			created := 0
			pool := syssession.NewAdvancedSessionPool(1, func() (syssession.SessionContext, error) {
				created++
				return sctx, nil
			})
			t.Cleanup(pool.Close)
			err := util.CallWithSCtx(pool, func(ctx sessionctx.Context) error {
				require.Same(t, sctx, ctx)
				vars := ctx.GetSessionVars()
				require.Equal(t, 3, vars.AnalyzePartitionConcurrency)
				require.Equal(t, 6, vars.AnalyzeStoreBatchSize)
				require.Equal(t, int64(12000), vars.LockWaitTimeout)
				_, offset := time.Now().In(vars.TimeZone).Zone()
				require.Equal(t, 3*3600, offset)
				require.Same(t, vars.TimeZone, vars.StmtCtx.TimeZone())
				if failCallback {
					return errors.New("statistics callback failed")
				}
				return nil
			})
			if failCallback {
				require.ErrorContains(t, err, "statistics callback failed")
			} else {
				require.NoError(t, err)
			}
			checkRestoredStatsSessionVars(t, sctx)
			if !failCallback {
				require.NoError(t, pool.WithSession(func(se *syssession.Session) error {
					return se.WithSessionContext(func(ctx sessionctx.Context) error {
						require.Same(t, sctx, ctx)
						checkRestoredStatsSessionVars(t, ctx)
						return nil
					})
				}))
				require.Equal(t, 1, created)
			}
		})
	}
	t.Run("setup_error", func(t *testing.T) {
		sctx := newStatsSessionContextForRestoreTest(t)
		vars := sctx.GetSessionVars()
		vars.GlobalVarsAccessor = failingStatsGlobalAccessor{
			GlobalVarAccessor: vars.GlobalVarsAccessor,
			failOn:            vardef.TimeZone,
		}
		pool := syssession.NewAdvancedSessionPool(1, func() (syssession.SessionContext, error) {
			return sctx, nil
		})
		t.Cleanup(pool.Close)
		called := false
		err := util.CallWithSCtx(pool, func(sessionctx.Context) error {
			called = true
			return nil
		})
		require.ErrorContains(t, err, "statistics settings unavailable")
		require.False(t, called)
		checkRestoredStatsSessionVars(t, sctx)
	})
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

func TestSnapshotTableInfoGetterAvoidsV1PartitionScan(t *testing.T) {
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

	partitionMapBuilds := 0
	is := partitionItemLookupForbiddenInfoSchema{
		InfoSchema:         infoschema.MockInfoSchemaWithSchemaVer([]*model.TableInfo{normalInfo, partitionedInfo}, 1),
		t:                  t,
		partitionMapBuilds: &partitionMapBuilds,
	}
	snapshotGetter := util.NewSnapshotTableInfoGetter(is)
	tbl, ok := snapshotGetter.TableInfoByID(normalInfo.ID)
	require.True(t, ok)
	require.Equal(t, normalInfo.ID, tbl.Meta().ID)
	item, ok := snapshotGetter.TableItemByID(normalInfo.ID)
	require.True(t, ok)
	require.Equal(t, "normal", item.TableName.L)
	// A table ID lookup does not need the partition map, so the map must not exist yet.
	require.Zero(t, partitionMapBuilds)
	tbl, ok = snapshotGetter.TableInfoByID(partitionID)
	require.True(t, ok)
	require.Equal(t, partitionedInfo.ID, tbl.Meta().ID)
	_, ok = snapshotGetter.TableInfoByID(1 << 60)
	require.False(t, ok)
	item, ok = snapshotGetter.TableItemByID(partitionID)
	require.True(t, ok)
	require.Equal(t, "partitioned", item.TableName.L)
	_, ok = snapshotGetter.TableItemByID(1 << 60)
	require.False(t, ok)
	_, ok = snapshotGetter.ParentTableID(normalInfo.ID)
	require.False(t, ok)
	tableID, ok := snapshotGetter.ParentTableID(partitionID)
	require.True(t, ok)
	require.Equal(t, partitionedInfo.ID, tableID)
	_, ok = snapshotGetter.ParentTableID(1 << 60)
	require.False(t, ok)
	// The partition ID and unknown ID lookups above all use one map.
	require.Equal(t, 1, partitionMapBuilds)
}

type failingStatsGlobalAccessor struct {
	variable.GlobalVarAccessor
	failOn string
}

func (a failingStatsGlobalAccessor) GetGlobalSysVar(name string) (string, error) {
	if name == a.failOn {
		return "", errors.New("statistics settings unavailable")
	}
	return a.GlobalVarAccessor.GetGlobalSysVar(name)
}

func TestUpdateSCtxVarsForStatsRestore(t *testing.T) {
	for _, failOn := range []string{
		"",
		vardef.TiDBEnableAsyncMergeGlobalStats,
		vardef.TiDBAnalyzePartitionConcurrency,
		vardef.TiDBAnalyzeVersion,
		vardef.TiDBAnalyzeStoreBatchSize,
		vardef.TiDBPartitionPruneMode,
		vardef.TiDBEnableAnalyzeSnapshot,
		vardef.TiDBAnalyzeSkipColumnTypes,
		vardef.TiDBSkipMissingPartitionStats,
		vardef.InnodbLockWaitTimeout,
		vardef.TimeZone,
	} {
		name := failOn
		if name == "" {
			name = "success"
		}
		t.Run(name, func(t *testing.T) {
			sctx := newStatsSessionContextForRestoreTest(t)
			vars := sctx.GetSessionVars()
			originTZ := vars.TimeZone
			if failOn != "" {
				vars.GlobalVarsAccessor = failingStatsGlobalAccessor{
					GlobalVarAccessor: vars.GlobalVarsAccessor,
					failOn:            failOn,
				}
			}
			restore, err := util.UpdateSCtxVarsForStats(sctx)
			if failOn != "" {
				require.ErrorContains(t, err, "statistics settings unavailable")
				require.Nil(t, restore)
			} else {
				require.NoError(t, err)
				require.True(t, vars.EnableAsyncMergeGlobalStats)
				require.Equal(t, 3, vars.AnalyzePartitionConcurrency)
				require.Equal(t, 2, vars.AnalyzeVersion)
				require.Equal(t, 6, vars.AnalyzeStoreBatchSize)
				require.Equal(t, "dynamic", vars.PartitionPruneMode.Load())
				require.True(t, vars.EnableAnalyzeSnapshot)
				require.Equal(t, map[string]struct{}{"blob": {}}, vars.AnalyzeSkipColumnTypes)
				require.True(t, vars.SkipMissingPartitionStats)
				require.Equal(t, int64(12000), vars.LockWaitTimeout)
				_, offset := time.Now().In(vars.TimeZone).Zone()
				require.Equal(t, 3*3600, offset)
				require.Same(t, vars.TimeZone, vars.StmtCtx.TimeZone())
				restore()
			}
			checkRestoredStatsSessionVars(t, sctx)
			require.Same(t, originTZ, vars.TimeZone)
		})
	}
	for _, name := range []string{
		vardef.TiDBAnalyzePartitionConcurrency,
		vardef.TiDBAnalyzeVersion,
		vardef.TiDBAnalyzeStoreBatchSize,
		vardef.InnodbLockWaitTimeout,
		vardef.TimeZone,
	} {
		t.Run("invalid_"+name, func(t *testing.T) {
			sctx := newStatsSessionContextForRestoreTest(t)
			vars := sctx.GetSessionVars()
			require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVarOnly(context.Background(), name, "invalid", false))
			restore, err := util.UpdateSCtxVarsForStats(sctx)
			require.Error(t, err)
			require.Nil(t, restore)
			checkRestoredStatsSessionVars(t, sctx)
		})
	}
	for _, tc := range []struct {
		name  string
		zone  *time.Location
		value string
	}{
		{"system", nil, "SYSTEM"},
		{"utc", time.UTC, "UTC"},
		{"fixed offset", time.FixedZone("", 8*3600), "+08:00"},
	} {
		t.Run("unloaded/"+tc.name, func(t *testing.T) {
			sctx := mock.NewContext()
			vars := sctx.GetSessionVars()
			vars.GlobalVarsAccessor = variable.NewMockGlobalAccessor4Tests()
			vars.TimeZone = tc.zone
			require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBAnalyzeStoreBatchSize, "6"))
			require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TimeZone, "+03:00"))
			for _, name := range []string{vardef.TiDBAnalyzeStoreBatchSize, vardef.TimeZone} {
				_, loaded := vars.GetSystemVar(name)
				require.False(t, loaded)
			}
			restore, err := util.UpdateSCtxVarsForStats(sctx)
			require.NoError(t, err)
			require.Equal(t, 6, vars.AnalyzeStoreBatchSize)
			restore()
			value, loaded := vars.GetSystemVar(vardef.TiDBAnalyzeStoreBatchSize)
			require.True(t, loaded)
			require.Equal(t, fmt.Sprint(vardef.DefTiDBAnalyzeStoreBatchSize), value)
			require.Equal(t, vardef.DefTiDBAnalyzeStoreBatchSize, vars.AnalyzeStoreBatchSize)
			value, loaded = vars.GetSystemVar(vardef.TimeZone)
			require.True(t, loaded)
			require.Equal(t, tc.value, value)
			if tc.zone == nil {
				require.Nil(t, vars.TimeZone)
			} else {
				require.Same(t, tc.zone, vars.TimeZone)
			}
			require.Same(t, time.UTC, vars.StmtCtx.TimeZone())
		})
	}
}
