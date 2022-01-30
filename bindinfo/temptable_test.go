// Copyright 2021 PingCAP, Inc.
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

package bindinfo_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/testkit"
)

// TestSelectBindingOnGlobalTempTableProhibited covers https://github.com/pingcap/tidb/issues/26377
func TestSelectBindingOnGlobalTempTableProhibited(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,tmp1")
	tk.MustExec("create table t1(a int(11))")
	tk.MustExec("create global temporary table tmp1(a int(11), key idx_a(a)) on commit delete rows;")
	tk.MustExec("create temporary table tmp2(a int(11), key idx_a(a));")

	queries := []string{
		"create global binding for with cte1 as (select a from tmp1) select * from cte1 using with cte1 as (select a from tmp1) select * from cte1",
		"create global binding for select * from t1 inner join tmp1 on t1.a=tmp1.a using select * from  t1 inner join tmp1 on t1.a=tmp1.a;",
		"create global binding for select * from t1 where t1.a in (select a from tmp1) using select * from t1 where t1.a in (select a from tmp1 use index (idx_a));",
		"create global binding for select a from t1 union select a from tmp1 using select a from t1 union select a from tmp1 use index (idx_a);",
		"create global binding for select t1.a, (select a from tmp1 where tmp1.a=1) as t2 from t1 using select t1.a, (select a from tmp1 where tmp1.a=1) as t2 from t1;",
		"create global binding for select * from (select * from tmp1) using select * from (select * from tmp1);",
		"create global binding for select * from t1 where t1.a = (select a from tmp1) using select * from t1 where t1.a = (select a from tmp1)",
	}
	genLocalTemporarySQL := func(sql string) string {
		return strings.Replace(sql, "tmp1", "tmp2", -1)
	}
	for _, query := range queries {
		localSQL := genLocalTemporarySQL(query)
		queries = append(queries, localSQL)
	}

	for _, q := range queries {
		tk.MustGetErrCode(q, errno.ErrOptOnTemporaryTable)
	}
}

// TestDMLBindingOnGlobalTempTableProhibited covers https://github.com/pingcap/tidb/issues/27422
func TestDMLBindingOnGlobalTempTableProhibited(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,tmp1,tmp2")
	tk.MustExec("create table t1(a int(11))")
	tk.MustExec("create global temporary table tmp1(a int(11), key idx_a(a)) on commit delete rows;")
	tk.MustExec("create temporary table tmp2(a int(11), key idx_a(a));")

	queries := []string{
		"create global binding for insert into t1 (select * from tmp1) using insert into t1 (select * from tmp1);",
		"create global binding for update t1 inner join tmp1 on t1.a=tmp1.a set t1.a=1 using update t1 inner join tmp1 on t1.a=tmp1.a set t1.a=1",
		"create global binding for update t1 set t1.a=(select a from tmp1) using update t1 set t1.a=(select a from tmp1)",
		"create global binding for update t1 set t1.a=1 where t1.a = (select a from tmp1) using update t1 set t1.a=1 where t1.a = (select a from tmp1)",
		"create global binding for with cte1 as (select a from tmp1) update t1 set t1.a=1 where t1.a in (select a from cte1) using with cte1 as (select a from tmp1) update t1 set t1.a=1 where t1.a in (select a from cte1)",
		"create global binding for delete from t1 where t1.a in (select a from tmp1) using delete from t1 where t1.a in (select a from tmp1)",
		"create global binding for delete from t1 where t1.a = (select a from tmp1) using delete from t1 where t1.a = (select a from tmp1)",
		"create global binding for delete t1 from t1,tmp1 using delete t1 from t1,tmp1",
	}
	genLocalTemporarySQL := func(sql string) string {
		return strings.Replace(sql, "tmp1", "tmp2", -1)
	}
	for _, query := range queries {
		localSQL := genLocalTemporarySQL(query)
		queries = append(queries, localSQL)
	}

	for _, q := range queries {
		tk.MustGetErrCode(q, errno.ErrOptOnTemporaryTable)
	}
}
