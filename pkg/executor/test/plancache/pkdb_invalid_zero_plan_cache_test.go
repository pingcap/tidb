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

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPKDBPreparedInvalidZeroTemporalParamForTimeColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec(`set @@sql_mode=''`)
	tk.MustExec("create table rp (id int primary key, tm time unique)")
	tk.MustExec("insert into rp values (1, '19:10:06')")

	tk.MustExec("prepare time_comparison from 'select id from rp where tm = ?'")
	tk.MustExec("set @p = cast('1970-00-01 19:10:06' as datetime)")
	tk.MustQuery("execute time_comparison using @p").Check(testkit.Rows())
	require.Equal(t, "invalid-zero temporal parameter", tk.Session().GetSessionVars().StmtCtx.PlanCacheUnqualified())
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: invalid-zero temporal parameter"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute time_comparison using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec("set @p = cast('1970-01-01 19:10:06' as datetime)")
	tk.MustQuery("execute time_comparison using @p").Check(testkit.Rows("1"))
	require.Empty(t, tk.Session().GetSessionVars().StmtCtx.PlanCacheUnqualified())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute time_comparison using @p").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`set @@tidb_opt_fix_control = "49736:ON"`)
	tk.MustExec("set @p = cast('1970-00-01 19:10:06' as datetime)")
	tk.MustQuery("execute time_comparison using @p").Check(testkit.Rows())
	require.Equal(t, "invalid-zero temporal parameter", tk.Session().GetSessionVars().StmtCtx.PlanCacheUnqualified())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec(`set @@tidb_opt_fix_control = "49736:OFF"`)
	tk.MustExec("set @p = cast('1970-01-01 19:10:06' as datetime)")
	tk.MustQuery("execute time_comparison using @p").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`set @@tidb_opt_fix_control = "70751:OFF"`)
	tk.MustExec("admin flush session plan_cache")
	tk.MustExec("set @p = cast('1970-00-01 19:10:06' as datetime)")
	tk.MustQuery("execute time_comparison using @p").Check(testkit.Rows())
	require.Empty(t, tk.Session().GetSessionVars().StmtCtx.PlanCacheUnqualified())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute time_comparison using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("deallocate prepare time_comparison")
}

func TestPKDBPreparedInvalidZeroTemporalParamKeepsOriginalType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec(`set @@sql_mode=''`)
	tk.MustExec("create table rp_type (a int primary key)")
	tk.MustExec("insert into rp_type values (1)")
	tk.MustExec("prepare invalid_zero_type from 'select ? from rp_type where a = 1'")

	tk.MustExec("set @p = cast('1970-00-01' as date)")
	rs, err := tk.Exec("execute invalid_zero_type using @p")
	require.NoError(t, err)
	require.Len(t, rs.Fields(), 1)
	require.Equal(t, mysql.TypeDate, rs.Fields()[0].Column.FieldType.GetType())
	tk.ResultSetToResult(rs, "execute invalid_zero_type with date").Check(testkit.Rows("1970-00-01"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec("set @p = cast('1970-00-01 19:10:06' as datetime)")
	rs, err = tk.Exec("execute invalid_zero_type using @p")
	require.NoError(t, err)
	require.Len(t, rs.Fields(), 1)
	require.Equal(t, mysql.TypeDatetime, rs.Fields()[0].Column.FieldType.GetType())
	tk.ResultSetToResult(rs, "execute invalid_zero_type with datetime").Check(testkit.Rows("1970-00-01 19:10:06"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("deallocate prepare invalid_zero_type")
}
