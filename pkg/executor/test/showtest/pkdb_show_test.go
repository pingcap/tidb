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

package showtest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPKDBShowStatsHealthyPrivileges(t *testing.T) {
	const privilegeWarning = "Warning|1105|SHOW STATS_HEALTHY only displays tables with database-level or table-level SELECT privileges, including active roles; column-level SELECT is insufficient."

	store := testkit.CreateMockStore(t)
	admin := testkit.NewTestKit(t, store)
	t.Cleanup(func() { admin.MustExec("set global pkdb_privilege = off") })

	t.Run("system_variable", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustQuery("select @@global.pkdb_privilege").Check(testkit.Rows("0"))
		tk.MustGetErrCode("set session pkdb_privilege = on", mysql.ErrGlobalVariable)
		tk.MustGetErrCode("set global pkdb_privilege = 'invalid'", mysql.ErrWrongValueForVar)
	})

	admin.MustExec("create database healthy_a")
	admin.MustExec("create database healthy_b")
	admin.MustExec("set tidb_partition_prune_mode = 'dynamic'")
	admin.MustExec("set tidb_analyze_version = 2")
	for _, table := range []string{"healthy_a.allowed", "healthy_a.denied", "healthy_a.columns_only", "healthy_b.db_allowed"} {
		admin.MustExec("create table " + table + " (a int, b int)")
		admin.MustExec("insert into " + table + " values (1, 1)")
		admin.MustExec("analyze table " + table)
	}
	admin.MustExec("create table healthy_a.no_stats (a int)")
	admin.MustExec("create table healthy_a.part (a int) partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	admin.MustExec("insert into healthy_a.part values (1), (11)")
	admin.MustExec("analyze table healthy_a.part")
	allRows := admin.MustQuery("show stats_healthy").Sort().Rows()

	for _, user := range []string{"healthy_scoped", "healthy_none", "healthy_columns", "healthy_meta", "healthy_legacy", "healthy_role"} {
		admin.MustExec("create user " + user)
	}
	admin.MustExec("grant select on healthy_a.allowed to healthy_scoped")
	admin.MustExec("grant select on healthy_a.no_stats to healthy_scoped")
	admin.MustExec("grant select on healthy_a.part to healthy_scoped")
	admin.MustExec("grant select on healthy_b.* to healthy_scoped")
	admin.MustExec("grant select(a, b) on healthy_a.columns_only to healthy_columns")
	admin.MustExec("grant select on mysql.stats_meta to healthy_meta")
	admin.MustExec("grant select on mysql.* to healthy_legacy")
	admin.MustExec("create role healthy_reader")
	admin.MustExec("grant select on healthy_a.allowed to healthy_reader")
	admin.MustExec("grant healthy_reader to healthy_role")

	login := func(t *testing.T, user string) *testkit.TestKit {
		t.Helper()
		tk := testkit.NewTestKit(t, store)
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: user, Hostname: "%"}, nil, nil, nil))
		tk.MustExec("set tidb_partition_prune_mode = 'dynamic'")
		return tk
	}

	t.Run("disabled_preserves_original_permissions", func(t *testing.T) {
		tk := login(t, "healthy_scoped")
		tk.MustGetErrCode("show stats_healthy", mysql.ErrDBaccessDenied)
		tk.MustGetErrCode("select * from mysql.stats_meta", mysql.ErrTableaccessDenied)

		legacy := login(t, "healthy_legacy")
		legacy.MustQuery("show stats_healthy").Sort().Check(allRows)
		legacy.MustQuery("show warnings").Check(testkit.Rows())
	})

	admin.MustExec("set global pkdb_privilege = on")
	admin.MustQuery("select @@global.pkdb_privilege").Check(testkit.Rows("1"))
	expected := []string{
		"healthy_a allowed  100",
		"healthy_a part global 100",
		"healthy_a part p0 100",
		"healthy_a part p1 100",
		"healthy_b db_allowed  100",
	}

	t.Run("table_and_database_select_includes_partitions", func(t *testing.T) {
		tk := login(t, "healthy_scoped")
		// No WHERE clause or current database is needed to filter tables across schemas.
		tk.MustQuery("show stats_healthy").Sort().Check(testkit.Rows(expected...))
		tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", privilegeWarning))
		tk.MustExec("use healthy_a")
		tk.MustQuery("show stats_healthy").Sort().Check(testkit.Rows(expected...))
		tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", privilegeWarning))
	})

	t.Run("where_and_like_filters", func(t *testing.T) {
		for _, test := range []struct {
			name string
			sql  string
			rows []string
		}{
			{"unauthorized_table", "show stats_healthy where Table_name = 'denied'", nil},
			{"authorized_table_without_stats", "show stats_healthy where Table_name = 'no_stats'", nil},
			{"constant_false_still_warns", "show stats_healthy where 1 = 0", nil},
			{"like_database", "show stats_healthy like 'healthy_b'", []string{"healthy_b db_allowed  100"}},
		} {
			t.Run(test.name, func(t *testing.T) {
				tk := login(t, "healthy_scoped")
				tk.MustQuery(test.sql).Sort().Check(testkit.Rows(test.rows...))
				tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", privilegeWarning))
			})
		}
	})

	t.Run("insufficient_permissions_return_empty_with_warning", func(t *testing.T) {
		for _, test := range []struct {
			name string
			user string
		}{
			{"no_select", "healthy_none"},
			{"all_columns_select_only", "healthy_columns"},
			{"mysql_stats_meta_select_only", "healthy_meta"},
		} {
			t.Run(test.name, func(t *testing.T) {
				tk := login(t, test.user)
				tk.MustQuery("show stats_healthy").Check(testkit.Rows())
				tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", privilegeWarning))
			})
		}
	})

	t.Run("only_active_roles_grant_visibility", func(t *testing.T) {
		tk := login(t, "healthy_role")
		tk.MustQuery("show stats_healthy").Check(testkit.Rows())
		tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", privilegeWarning))
		tk.MustExec("set role healthy_reader")
		tk.MustQuery("show stats_healthy").Check(testkit.Rows("healthy_a allowed  100"))
		tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", privilegeWarning))
		tk.MustExec("set role none")
		tk.MustQuery("show stats_healthy").Check(testkit.Rows())
		tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", privilegeWarning))
	})

	t.Run("legacy_mysql_select_preserves_results_without_warning", func(t *testing.T) {
		tk := login(t, "healthy_legacy")
		tk.MustQuery("show stats_healthy").Sort().Check(allRows)
		tk.MustQuery("show warnings").Check(testkit.Rows())
	})

	t.Run("other_permission_checks_unchanged", func(t *testing.T) {
		tk := login(t, "healthy_scoped")
		tk.MustGetErrCode("select * from mysql.stats_meta", mysql.ErrTableaccessDenied)
		for _, sql := range []string{"show stats_meta", "show stats_buckets", "show stats_histograms"} {
			tk.MustGetErrCode(sql, mysql.ErrTableaccessDenied)
		}
		tk.MustGetErrCode("show stats_healthy where Table_name = (select a from healthy_a.denied limit 1)", mysql.ErrColumnaccessDenied)
	})

	t.Run("prepared_statement_rechecks_permissions_and_switch", func(t *testing.T) {
		const planCacheWarning = "Warning|1105|skip prepared plan-cache: not a SELECT/UPDATE/INSERT/DELETE/SET statement"

		admin := testkit.NewTestKit(t, store)
		t.Cleanup(func() {
			admin.MustExec("set global pkdb_privilege = on")
			admin.MustExec("grant select on healthy_a.allowed to healthy_scoped")
		})
		tk := login(t, "healthy_scoped")
		tk.MustExec("prepare healthy_stmt from 'show stats_healthy'")
		tk.MustQuery("execute healthy_stmt").Sort().Check(testkit.Rows(expected...))
		require.Equal(t, "not a SELECT/UPDATE/INSERT/DELETE/SET statement", tk.Session().GetSessionVars().StmtCtx.PlanCacheUnqualified())
		tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", planCacheWarning, privilegeWarning))

		// Revoking a table grant must affect the next execution of the same statement.
		admin.MustExec("revoke select on healthy_a.allowed from healthy_scoped")
		expectedAfterRevoke := expected[1:]
		tk.MustQuery("execute healthy_stmt").Sort().Check(testkit.Rows(expectedAfterRevoke...))
		tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", planCacheWarning, privilegeWarning))

		// Disabling the switch restores the original permission error for both paths.
		admin.MustExec("set global pkdb_privilege = off")
		tk.MustGetErrCode("show stats_healthy", mysql.ErrDBaccessDenied)
		tk.MustGetErrCode("execute healthy_stmt", mysql.ErrDBaccessDenied)

		admin.MustExec("set global pkdb_privilege = on")
		tk.MustQuery("execute healthy_stmt").Sort().Check(testkit.Rows(expectedAfterRevoke...))
		tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", planCacheWarning, privilegeWarning))
		tk.MustExec("deallocate prepare healthy_stmt")
	})
}

func TestPKDBShowStatsHealthyForcedPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	admin := testkit.NewTestKit(t, store)
	t.Cleanup(func() { admin.MustExec("set global pkdb_privilege = off") })
	admin.MustExec("set global pkdb_privilege = on")
	admin.MustExec("create database healthy_cache")
	for _, table := range []string{"allowed", "denied"} {
		admin.MustExec("create table healthy_cache." + table + " (a int)")
		admin.MustExec("insert into healthy_cache." + table + " values (1)")
		admin.MustExec("analyze table healthy_cache." + table)
	}
	admin.MustExec("create user healthy_cache_user")
	admin.MustExec("grant select on healthy_cache.allowed to healthy_cache_user")

	login := func(t *testing.T) *testkit.TestKit {
		t.Helper()
		tk := testkit.NewTestKit(t, store)
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "healthy_cache_user", Hostname: "%"}, nil, nil, nil))
		tk.MustExec("set tidb_enable_prepared_plan_cache = on")
		tk.MustExec("set tidb_opt_fix_control = '49736:ON'")
		return tk
	}

	t.Run("prepared_statement", func(t *testing.T) {
		t.Cleanup(func() { admin.MustExec("set global pkdb_privilege = on") })
		admin.MustExec("grant select on mysql.* to healthy_cache_user")
		tk := login(t)
		tk.MustExec("prepare legacy_stmt from 'show stats_healthy'")
		tk.MustQuery("execute legacy_stmt").Sort().Check(testkit.Rows("healthy_cache allowed  100", "healthy_cache denied  100"))

		// The same SQL must not reuse an unfiltered plan after mysql.* SELECT is revoked.
		admin.MustExec("revoke select on mysql.* from healthy_cache_user")
		tk.MustExec("prepare scoped_stmt from 'show stats_healthy'")
		for range 2 {
			tk.MustQuery("execute scoped_stmt").Check(testkit.Rows("healthy_cache allowed  100"))
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		}
		admin.MustExec("set global pkdb_privilege = off")
		tk.MustGetErrCode("execute scoped_stmt", mysql.ErrDBaccessDenied)
		tk.MustExec("deallocate prepare scoped_stmt")
		tk.MustExec("deallocate prepare legacy_stmt")
	})

	t.Run("stored_function", func(t *testing.T) {
		t.Cleanup(func() {
			admin.MustExec("set global tidb_enable_procedure = off")
			admin.MustExec("set global tidb_enable_sp_param_substitute = off")
		})
		admin.InProcedure()
		for _, function := range []string{"legacy_health", "scoped_health"} {
			admin.MustExec("create function healthy_cache." + function + `() returns int sql security invoker
			begin
				declare x int;
				select a into x from healthy_cache.allowed limit 1;
				show stats_healthy where (@healthy_seen := @healthy_seen + 1) > 0;
				return @healthy_seen;
			end`)
			admin.MustExec("grant execute on function healthy_cache." + function + " to healthy_cache_user")
		}
		admin.MustExec("grant select on mysql.* to healthy_cache_user")
		tk := login(t)
		tk.Session().SetSessionExec(tk)
		tk.MustExec("set tidb_enable_sp_plan_cache = on")

		// SELECT INTO enables the shared cache state before the internal SHOW.
		tk.MustExec("set @healthy_seen = 0")
		tk.MustQuery("select healthy_cache.legacy_health()").Check(testkit.Rows("2"))

		admin.MustExec("revoke select on mysql.* from healthy_cache_user")
		// Another function prepares the same SHOW SQL after the privilege change.
		for range 2 {
			tk.MustExec("set @healthy_seen = 0")
			tk.MustQuery("select healthy_cache.scoped_health()").Check(testkit.Rows("1"))
		}
		require.True(t, tk.Session().GetSessionVars().StmtCtx.UseCache(), "SHOW must restore the caller's cache state")
	})
}
