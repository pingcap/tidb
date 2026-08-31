// Copyright 2026 PingCAP, Inc.
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

package plancache

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestTimeColumnTemporalComparisonPlanCacheToggle(t *testing.T) {
	var baseline [][][]any
	for _, enabled := range []string{"0", "1"} {
		t.Run("prepared_plan_cache_"+enabled, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("set tidb_enable_prepared_plan_cache = " + enabled)
			tk.MustExec("set sql_mode = ''")
			tk.MustExec("create table t_time_cache_toggle (id int primary key, trx_time time(6), key(trx_time))")
			tk.MustExec("insert into t_time_cache_toggle values (1, '19:10:06.123456'), (2, '18:00:00'), (3, '20:00:00'), (4, '00:00:00')")
			tk.MustExec("prepare time_cache_toggle from 'select id from t_time_cache_toggle where trx_time = ? order by id'")

			params := []string{
				"cast('1970-01-01 19:10:06.123456' as datetime(6))",
				"cast('1970-01-01' as date)",
				"'18:00:00'",
				"null",
				"cast('1970-00-01 19:10:06.123456' as datetime(6))",
			}
			got := make([][][]any, 0, len(params))
			for _, param := range params {
				tk.MustExec("set @p = " + param)
				result := tk.MustQuery("execute time_cache_toggle using @p")
				got = append(got, result.Rows())
			}
			if baseline == nil {
				baseline = got
			} else {
				require.Equal(t, baseline, got)
			}
			tk.MustExec("deallocate prepare time_cache_toggle")
		})
	}
}

func TestTimeColumnTemporalParameterPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache = 1")
	tk.MustExec("create table t_time_cache (id int primary key, trx_time time(0), key(trx_time))")
	tk.MustExec("insert into t_time_cache values (1, '19:10:06'), (2, '18:00:00'), (3, '20:00:00'), (4, '00:00:00')")

	tk.MustExec("prepare time_eq from 'select id from t_time_cache where trx_time = ? order by id'")
	tk.MustExec("set @p = cast('1970-01-01 19:10:05.600000' as datetime(6))")
	tk.MustQuery("execute time_eq using @p").Check(testkit.Rows("1"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec("set @p = cast('2030-12-31 18:00:00.400000' as datetime(6))")
	tk.MustQuery("execute time_eq using @p").Check(testkit.Rows("2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec("set @p = cast('1970-01-01' as date)")
	tk.MustQuery("execute time_eq using @p").Check(testkit.Rows("4"))

	tk.MustExec("prepare time_null_safe_eq from 'select id from t_time_cache where trx_time <=> ? order by id'")
	tk.MustExec("set @p = cast('1970-01-01 19:10:06' as datetime)")
	tk.MustQuery("execute time_null_safe_eq using @p").Check(testkit.Rows("1"))
	tk.MustExec("set @p = cast('2030-12-31 18:00:00' as datetime)")
	tk.MustQuery("execute time_null_safe_eq using @p").Check(testkit.Rows("2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("prepare time_reversed from 'select id from t_time_cache where ? <= trx_time order by id'")
	tk.MustExec("set @p = cast('1970-01-01 19:10:05.600000' as datetime(6))")
	tk.MustQuery("execute time_reversed using @p").Check(testkit.Rows("1", "3"))

	tk.MustExec("deallocate prepare time_eq")
	tk.MustExec("deallocate prepare time_null_safe_eq")
	tk.MustExec("deallocate prepare time_reversed")
}

func TestTimeColumnTemporalParameterPlanCacheUnderJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache = 1")
	tk.MustExec("create table t_time_join_cache (id int primary key, trx_time time, key(trx_time))")
	tk.MustExec("insert into t_time_join_cache values (1, '19:10:06'), (2, '20:00:00')")

	tk.MustExec("prepare time_join_eq from 'select t1.id from t_time_join_cache t1 join t_time_join_cache t2 on t1.id = t2.id where t1.trx_time = ? order by t1.id'")
	tk.MustExec("set @p = cast('1970-01-01 19:10:06' as datetime)")
	tk.MustQuery("execute time_join_eq using @p").Check(testkit.Rows("1"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("set @p = cast('2030-12-31 20:00:00' as datetime)")
	tk.MustQuery("execute time_join_eq using @p").Check(testkit.Rows("2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("deallocate prepare time_join_eq")
}

func TestTimeColumnTemporalComparisonFixControlPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache = 1")
	tk.MustExec("create table t_time_fix_control (id int primary key, trx_time time, key(trx_time))")
	tk.MustExec("insert into t_time_fix_control values (1, '19:10:06')")
	tk.MustExec("prepare time_fix_control from 'select id from t_time_fix_control where trx_time = ?'")
	tk.MustExec("set @p = cast('1000-01-01 19:10:06' as datetime)")

	tk.MustQuery("execute time_fix_control using @p").Check(testkit.Rows("1"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery("execute time_fix_control using @p").Check(testkit.Rows("1"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("set tidb_opt_fix_control = '70751:OFF'")
	tk.MustQuery("execute time_fix_control using @p").Check(testkit.Rows())
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery("execute time_fix_control using @p").Check(testkit.Rows())
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("set tidb_opt_fix_control = '70751:ON'")
	tk.MustQuery("execute time_fix_control using @p").Check(testkit.Rows("1"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("deallocate prepare time_fix_control")
}

func TestTimeColumnStringTemporalParameterPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache = 1")
	tk.MustExec("create table t_time_string_cache (id int primary key, trx_time time(0), key(trx_time))")
	tk.MustExec("insert into t_time_string_cache values (1, '19:10:06'), (2, '18:00:00'), (3, '-836:46:08')")

	tk.MustExec("prepare time_string_eq from 'select id from t_time_string_cache where trx_time = ? order by id'")
	tk.MustExec("set @p = '-836:46:08'")
	tk.MustQuery("execute time_string_eq using @p").Check(testkit.Rows("3"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("set @p = '18:00:00'")
	tk.MustQuery("execute time_string_eq using @p").Check(testkit.Rows("2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("set @p = 'not-a-time'")
	tk.MustQuery("execute time_string_eq using @p").Check(testkit.Rows())
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("deallocate prepare time_string_eq")
}

func TestTimeColumnTemporalParameterTypeChangesPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache = 1")
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("set timestamp = 69006")
	tk.MustExec("create table t_time_type_cache (id int primary key, trx_time time(6), key(trx_time))")
	tk.MustExec("insert into t_time_type_cache values (1, '19:10:06.123456'), (2, '18:00:00'), (3, '20:00:00'), (4, '00:00:00')")

	tk.MustExec("prepare time_type_changes from 'select id from t_time_type_cache where trx_time = ? order by id'")

	tk.MustExec("set @p = cast('1970-01-01 19:10:06.123456' as datetime(6))")
	tk.MustQuery("execute time_type_changes using @p").Check(testkit.Rows("1"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("set @p = cast('1970-01-01' as date)")
	tk.MustQuery("execute time_type_changes using @p").Check(testkit.Rows("4"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("set @p = timestamp '1970-01-01 18:00:00'")
	tk.MustQuery("execute time_type_changes using @p").Check(testkit.Rows("2"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("set @p = '18:00:00'")
	tk.MustQuery("execute time_type_changes using @p").Check(testkit.Rows("2"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("set @p = null")
	tk.MustQuery("execute time_type_changes using @p").Check(testkit.Rows())
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("set @p = cast('1970-00-01 19:10:06.123456' as datetime(6))")
	tk.MustQuery("execute time_type_changes using @p").Check(testkit.Rows())
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	require.Equal(t, "invalid-zero temporal parameter", tk.Session().GetSessionVars().StmtCtx.PlanCacheUnqualified())

	tk.MustExec("set @p = cast('1970-01-01 19:10:06.123456' as datetime(6))")
	tk.MustQuery("execute time_type_changes using @p").Check(testkit.Rows("1"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("deallocate prepare time_type_changes")
}

func TestInvalidZeroUnrelatedParameterSkipsPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache = 1")
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("create table t_time_unrelated_cache (id int primary key, trx_time time, dt datetime, key(trx_time))")
	tk.MustExec("insert into t_time_unrelated_cache values (1, '19:10:06', '1970-01-01 00:00:00')")

	tk.MustExec("prepare unrelated_invalid_zero from 'select id from t_time_unrelated_cache where trx_time = ? and dt = ?'")
	tk.MustExec("set @tm = '19:10:06'")
	tk.MustExec("set @dt = cast('1970-01-01 00:00:00' as datetime)")
	tk.MustQuery("execute unrelated_invalid_zero using @tm, @dt").Check(testkit.Rows("1"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	require.Empty(t, tk.Session().GetSessionVars().StmtCtx.PlanCacheUnqualified())

	tk.MustExec("set @dt = cast('1970-00-01 00:00:00' as datetime)")
	tk.MustQuery("execute unrelated_invalid_zero using @tm, @dt").Check(testkit.Rows())
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	require.Equal(t, "invalid-zero temporal parameter", tk.Session().GetSessionVars().StmtCtx.PlanCacheUnqualified())

	tk.MustQuery("execute unrelated_invalid_zero using @tm, @dt").Check(testkit.Rows())
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	require.Equal(t, "invalid-zero temporal parameter", tk.Session().GetSessionVars().StmtCtx.PlanCacheUnqualified())

	tk.MustExec("deallocate prepare unrelated_invalid_zero")
}

func TestCastTimeParameterTypesDoNotSharePlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache = 1")
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("create table t_cast_time_cache (id int primary key, trx_time time(6), key(trx_time))")
	tk.MustExec("insert into t_cast_time_cache values (1, '19:10:06.123456'), (2, '18:00:00')")

	tk.MustExec("prepare cast_time_types from 'select id from t_cast_time_cache where trx_time = cast(? as time(6)) order by id'")
	tk.MustExec("set @p = '19:10:06.123456'")
	tk.MustQuery("execute cast_time_types using @p").Check(testkit.Rows("1"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("set @p = cast('1970-01-01 18:00:00' as datetime(6))")
	tk.MustQuery("execute cast_time_types using @p").Check(testkit.Rows("2"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("set @p = '19:10:06.123456'")
	tk.MustQuery("execute cast_time_types using @p").Check(testkit.Rows("1"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("deallocate prepare cast_time_types")
}
