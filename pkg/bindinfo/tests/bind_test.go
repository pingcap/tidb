// Copyright 2023 PingCAP, Inc.
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

package tests

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/bindinfo/internal"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"github.com/stretchr/testify/require"
)

func TestBindingInListOperation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, c int, d int)`)

	// only 1 binding will be left
	tk.MustExec(`create binding for select * from t where a in(1) using select * from t where a in(1)`)
	tk.MustExec(`create binding for select * from t where a in(1,2) using select * from t where a in(1)`)
	tk.MustExec(`create binding for select * from t where a in(1) using select * from t where a in(1,2)`)
	tk.MustExec(`create binding for select * from t where a in(1,2) using select * from t where a in(1,2)`)
	tk.MustExec(`create binding for select * from t where a in(1,2,3) using select * from t where a in(1,2,3)`)
	require.Equal(t, 1, len(tk.MustQuery(`show bindings`).Rows()))
	tk.MustExec(`drop binding for select * from t where a in(1)`)
	require.Equal(t, 0, len(tk.MustQuery(`show bindings`).Rows()))

	// create and drop
	tk.MustExec(`create binding for select * from t where a in(1,2,3) using select * from t where a in(1)`)
	require.Equal(t, 1, len(tk.MustQuery(`show bindings`).Rows()))
	tk.MustExec(`drop binding for select * from t where a in(1)`)
	require.Equal(t, 0, len(tk.MustQuery(`show bindings`).Rows()))
	tk.MustExec(`create binding for select * from t where a in(1) using select * from t where a in(1)`)
	require.Equal(t, 1, len(tk.MustQuery(`show bindings`).Rows()))
	tk.MustExec(`drop binding for select * from t where a in(1,2,3)`)
	require.Equal(t, 0, len(tk.MustQuery(`show bindings`).Rows()))
	tk.MustExec(`create binding for select * from t where a in(1) using select * from t where a in(1)`)
	require.Equal(t, 1, len(tk.MustQuery(`show bindings`).Rows()))
	tk.MustExec(`drop binding for select * from t where a in(1,2,3,4,5,6,7,8,9,0,11,12)`)
	require.Equal(t, 0, len(tk.MustQuery(`show bindings`).Rows()))

	// create and set status
	tk.MustExec(`create global binding for select * from t where a in(1,2,3) using select * from t where a in(1)`)
	require.Equal(t, "enabled", tk.MustQuery(`show global bindings`).Rows()[0][3].(string))
	tk.MustExec(`set binding disabled for select * from t where a in(1)`)
	require.Equal(t, "disabled", tk.MustQuery(`show global bindings`).Rows()[0][3].(string))
	tk.MustExec(`set binding enabled for select * from t where a in(1,2,3,4,5)`)
	require.Equal(t, "enabled", tk.MustQuery(`show global bindings`).Rows()[0][3].(string))
}

func TestPrepareCacheWithBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, key idx_b(b), key idx_c(c))")
	tk.MustExec("create table t2(a int, b int, c int, key idx_b(b), key idx_c(c))")

	// TestDMLSQLBind
	tk.MustExec("prepare stmt1 from 'delete from t1 where b = 1 and c > 1';")
	tk.MustExec("execute stmt1;")
	require.Equal(t, "t1:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res := tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.MustUseIndex4ExplainFor(res, "idx_b(b)"), res.Rows())
	tk.MustExec("execute stmt1;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for delete from t1 where b = 1 and c > 1 using delete /*+ use_index(t1,idx_c) */ from t1 where b = 1 and c > 1")

	tk.MustExec("execute stmt1;")
	require.Equal(t, "t1:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.MustUseIndex4ExplainFor(res, "idx_c(c)"), res.Rows())

	tk.MustExec("prepare stmt2 from 'delete t1, t2 from t1 inner join t2 on t1.b = t2.b';")
	tk.MustExec("execute stmt2;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.HasPlan4ExplainFor(res, "HashJoin"), res.Rows())
	tk.MustExec("execute stmt2;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for delete t1, t2 from t1 inner join t2 on t1.b = t2.b using delete /*+ inl_join(t1) */ t1, t2 from t1 inner join t2 on t1.b = t2.b")

	tk.MustExec("execute stmt2;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.HasPlan4ExplainFor(res, "IndexJoin"), res.Rows())

	tk.MustExec("prepare stmt3 from 'update t1 set a = 1 where b = 1 and c > 1';")
	tk.MustExec("execute stmt3;")
	require.Equal(t, "t1:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.MustUseIndex4ExplainFor(res, "idx_b(b)"), res.Rows())
	tk.MustExec("execute stmt3;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for update t1 set a = 1 where b = 1 and c > 1 using update /*+ use_index(t1,idx_c) */ t1 set a = 1 where b = 1 and c > 1")

	tk.MustExec("execute stmt3;")
	require.Equal(t, "t1:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.MustUseIndex4ExplainFor(res, "idx_c(c)"), res.Rows())

	tk.MustExec("prepare stmt4 from 'update t1, t2 set t1.a = 1 where t1.b = t2.b';")
	tk.MustExec("execute stmt4;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.HasPlan4ExplainFor(res, "HashJoin"), res.Rows())
	tk.MustExec("execute stmt4;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for update t1, t2 set t1.a = 1 where t1.b = t2.b using update /*+ inl_join(t1) */ t1, t2 set t1.a = 1 where t1.b = t2.b")

	tk.MustExec("execute stmt4;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.HasPlan4ExplainFor(res, "IndexJoin"), res.Rows())

	tk.MustExec("prepare stmt5 from 'insert into t1 select * from t2 where t2.b = 2 and t2.c > 2';")
	tk.MustExec("execute stmt5;")
	require.Equal(t, "t2:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.MustUseIndex4ExplainFor(res, "idx_b(b)"), res.Rows())
	tk.MustExec("execute stmt5;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1 using insert /*+ use_index(t2,idx_c) */ into t1 select * from t2 where t2.b = 1 and t2.c > 1")

	tk.MustExec("execute stmt5;")
	require.Equal(t, "t2:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.MustUseIndex4ExplainFor(res, "idx_b(b)"), res.Rows())

	tk.MustExec("drop global binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("create global binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1 using insert into t1 select /*+ use_index(t2,idx_c) */ * from t2 where t2.b = 1 and t2.c > 1")

	tk.MustExec("execute stmt5;")
	require.Equal(t, "t2:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.MustUseIndex4ExplainFor(res, "idx_c(c)"), res.Rows())

	tk.MustExec("prepare stmt6 from 'replace into t1 select * from t2 where t2.b = 2 and t2.c > 2';")
	tk.MustExec("execute stmt6;")
	require.Equal(t, "t2:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.MustUseIndex4ExplainFor(res, "idx_b(b)"), res.Rows())
	tk.MustExec("execute stmt6;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for replace into t1 select * from t2 where t2.b = 1 and t2.c > 1 using replace into t1 select /*+ use_index(t2,idx_c) */ * from t2 where t2.b = 1 and t2.c > 1")

	tk.MustExec("execute stmt6;")
	require.Equal(t, "t2:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.MustUseIndex4ExplainFor(res, "idx_c(c)"), res.Rows())

	// TestExplain
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")

	tk.MustExec("prepare stmt1 from 'SELECT * from t1,t2 where t1.id = t2.id';")
	tk.MustExec("execute stmt1;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.HasPlan4ExplainFor(res, "HashJoin"))
	tk.MustExec("execute stmt1;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt2 from 'SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id';")
	tk.MustExec("execute stmt2;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.HasPlan4ExplainFor(res, "MergeJoin"))
	tk.MustExec("execute stmt2;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")

	tk.MustExec("execute stmt1;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.HasPlan4ExplainFor(res, "MergeJoin"))

	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")

	tk.MustExec("create index index_id on t1(id)")
	tk.MustExec("prepare stmt1 from 'SELECT * from t1 use index(index_id)';")
	tk.MustExec("execute stmt1;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.HasPlan4ExplainFor(res, "IndexReader"))
	tk.MustExec("execute stmt1;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for SELECT * from t1 using SELECT * from t1 ignore index(index_id)")
	tk.MustExec("execute stmt1;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.False(t, tk.HasPlan4ExplainFor(res, "IndexReader"))
	tk.MustExec("execute stmt1;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// Add test for SetOprStmt
	tk.MustExec("prepare stmt1 from 'SELECT * from t1 union SELECT * from t1';")
	tk.MustExec("execute stmt1;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.HasPlan4ExplainFor(res, "IndexReader"))
	tk.MustExec("execute stmt1;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt2 from 'SELECT * from t1 use index(index_id) union SELECT * from t1';")
	tk.MustExec("execute stmt2;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.HasPlan4ExplainFor(res, "IndexReader"))
	tk.MustExec("execute stmt2;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for SELECT * from t1 union SELECT * from t1 using SELECT * from t1 use index(index_id) union SELECT * from t1")

	tk.MustExec("execute stmt1;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.HasPlan4ExplainFor(res, "IndexReader"))

	tk.MustExec("drop global binding for SELECT * from t1 union SELECT * from t1")

	// TestBindingSymbolList
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, INDEX ia (a), INDEX ib (b));")
	tk.MustExec("insert into t value(1, 1);")
	tk.MustExec("prepare stmt1 from 'select a, b from t where a = 3 limit 1, 100';")
	tk.MustExec("execute stmt1;")
	require.Equal(t, "t:ia", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.MustUseIndex4ExplainFor(res, "ia(a)"), res.Rows())
	tk.MustExec("execute stmt1;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`create global binding for select a, b from t where a = 1 limit 0, 1 using select a, b from t use index (ib) where a = 1 limit 0, 1`)

	// after binding
	tk.MustExec("execute stmt1;")
	require.Equal(t, "t:ib", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.True(t, tk.MustUseIndex4ExplainFor(res, "ib(b)"), res.Rows())
}

// TestBindingSymbolList tests sql with "?, ?, ?, ?", fixes #13871
func TestBindingSymbolList(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, INDEX ia (a), INDEX ib (b));")
	tk.MustExec("insert into t value(1, 1);")

	// before binding
	tk.MustQuery("select a, b from t where a = 3 limit 1, 100")
	require.Equal(t, "t:ia", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("select a, b from t where a = 3 limit 1, 100", "ia(a)"))

	tk.MustExec(`create global binding for select a, b from t where a = 1 limit 0, 1 using select a, b from t use index (ib) where a = 1 limit 0, 1`)

	// after binding
	tk.MustQuery("select a, b from t where a = 3 limit 1, 100")
	require.Equal(t, "t:ib", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("select a, b from t where a = 3 limit 1, 100", "ib(b)"))

	// Normalize
	sql, hash := parser.NormalizeDigestForBinding("select a, b from test . t where a = 1 limit 0, 1")

	bindData := dom.BindHandle().GetBindRecord(hash.String(), sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select `a` , `b` from `test` . `t` where `a` = ? limit ...", bindData.OriginalSQL)
	bind := bindData.Bindings[0]
	require.Equal(t, "SELECT `a`,`b` FROM `test`.`t` USE INDEX (`ib`) WHERE `a` = 1 LIMIT 0,1", bind.BindSQL)
	require.Equal(t, "test", bindData.Db)
	require.Equal(t, bindinfo.Enabled, bind.Status)
	require.NotNil(t, bind.Charset)
	require.NotNil(t, bind.Collation)
	require.NotNil(t, bind.CreateTime)
	require.NotNil(t, bind.UpdateTime)
}

// TestBindingInListWithSingleLiteral tests sql with "IN (Lit)", fixes #44298
func TestBindingInListWithSingleLiteral(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, INDEX ia (a), INDEX ib (b));")
	tk.MustExec("insert into t value(1, 1);")

	// GIVEN
	sqlcmd := "select a, b from t where a in (1)"
	binding := `create global binding for select a, b from t where a in (1, 2, 3) using select a, b from t use index (ib) where a in (1, 2, 3)`

	// before binding
	tk.MustQuery(sqlcmd)
	require.Equal(t, "t:ia", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex(sqlcmd, "ia(a)"))

	tk.MustExec(binding)

	// after binding
	tk.MustQuery(sqlcmd)
	require.Equal(t, "t:ib", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex(sqlcmd, "ib(b)"))

	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))

	// Normalize
	sql, hash := parser.NormalizeDigestForBinding("select a, b from test . t where a in (1)")

	bindData := dom.BindHandle().GetBindRecord(hash.String(), sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select `a` , `b` from `test` . `t` where `a` in ( ... )", bindData.OriginalSQL)
	bind := bindData.Bindings[0]
	require.Equal(t, "SELECT `a`,`b` FROM `test`.`t` USE INDEX (`ib`) WHERE `a` IN (1,2,3)", bind.BindSQL)
	require.Equal(t, "test", bindData.Db)
	require.Equal(t, bindinfo.Enabled, bind.Status)
	require.NotNil(t, bind.Charset)
	require.NotNil(t, bind.Collation)
	require.NotNil(t, bind.CreateTime)
	require.NotNil(t, bind.UpdateTime)
}

func TestDMLSQLBind(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, key idx_b(b), key idx_c(c))")
	tk.MustExec("create table t2(a int, b int, c int, key idx_b(b), key idx_c(c))")

	tk.MustExec("delete from t1 where b = 1 and c > 1")
	require.Equal(t, "t1:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("delete from t1 where b = 1 and c > 1", "idx_b(b)"))
	tk.MustExec("create global binding for delete from t1 where b = 1 and c > 1 using delete /*+ use_index(t1,idx_c) */ from t1 where b = 1 and c > 1")
	tk.MustExec("delete from t1 where b = 1 and c > 1")
	require.Equal(t, "t1:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("delete from t1 where b = 1 and c > 1", "idx_c(c)"))

	tk.MustHavePlan("delete t1, t2 from t1 inner join t2 on t1.b = t2.b", "HashJoin")
	tk.MustExec("create global binding for delete t1, t2 from t1 inner join t2 on t1.b = t2.b using delete /*+ inl_join(t1) */ t1, t2 from t1 inner join t2 on t1.b = t2.b")
	tk.MustHavePlan("delete t1, t2 from t1 inner join t2 on t1.b = t2.b", "IndexJoin")

	tk.MustExec("update t1 set a = 1 where b = 1 and c > 1")
	require.Equal(t, "t1:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("update t1 set a = 1 where b = 1 and c > 1", "idx_b(b)"))
	tk.MustExec("create global binding for update t1 set a = 1 where b = 1 and c > 1 using update /*+ use_index(t1,idx_c) */ t1 set a = 1 where b = 1 and c > 1")
	tk.MustExec("delete from t1 where b = 1 and c > 1")
	require.Equal(t, "t1:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("update t1 set a = 1 where b = 1 and c > 1", "idx_c(c)"))

	tk.MustHavePlan("update t1, t2 set t1.a = 1 where t1.b = t2.b", "HashJoin")
	tk.MustExec("create global binding for update t1, t2 set t1.a = 1 where t1.b = t2.b using update /*+ inl_join(t1) */ t1, t2 set t1.a = 1 where t1.b = t2.b")
	tk.MustHavePlan("update t1, t2 set t1.a = 1 where t1.b = t2.b", "IndexJoin")

	tk.MustExec("insert into t1 select * from t2 where t2.b = 2 and t2.c > 2")
	require.Equal(t, "t2:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("insert into t1 select * from t2 where t2.b = 2 and t2.c > 2", "idx_b(b)"))
	tk.MustExec("create global binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1 using insert /*+ use_index(t2,idx_c) */ into t1 select * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("insert into t1 select * from t2 where t2.b = 2 and t2.c > 2")
	require.Equal(t, "t2:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("insert into t1 select * from t2 where t2.b = 2 and t2.c > 2", "idx_b(b)"))
	tk.MustExec("drop global binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("create global binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1 using insert into t1 select /*+ use_index(t2,idx_c) */ * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("insert into t1 select * from t2 where t2.b = 2 and t2.c > 2")
	require.Equal(t, "t2:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("insert into t1 select * from t2 where t2.b = 2 and t2.c > 2", "idx_c(c)"))

	tk.MustExec("replace into t1 select * from t2 where t2.b = 2 and t2.c > 2")
	require.Equal(t, "t2:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("replace into t1 select * from t2 where t2.b = 2 and t2.c > 2", "idx_b(b)"))
	tk.MustExec("create global binding for replace into t1 select * from t2 where t2.b = 1 and t2.c > 1 using replace into t1 select /*+ use_index(t2,idx_c) */ * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("replace into t1 select * from t2 where t2.b = 2 and t2.c > 2")
	require.Equal(t, "t2:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("replace into t1 select * from t2 where t2.b = 2 and t2.c > 2", "idx_c(c)"))
}

func TestBestPlanInBaselines(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, INDEX ia (a), INDEX ib (b));")
	tk.MustExec("insert into t value(1, 1);")

	// before binding
	tk.MustQuery("select a, b from t where a = 3 limit 1, 100")
	require.Equal(t, "t:ia", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("select a, b from t where a = 3 limit 1, 100", "ia(a)"))

	tk.MustQuery("select a, b from t where b = 3 limit 1, 100")
	require.Equal(t, "t:ib", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("select a, b from t where b = 3 limit 1, 100", "ib(b)"))

	tk.MustExec(`create global binding for select a, b from t where a = 1 limit 0, 1 using select /*+ use_index(@sel_1 test.t ia) */ a, b from t where a = 1 limit 0, 1`)
	tk.MustExec(`create global binding for select a, b from t where b = 1 limit 0, 1 using select /*+ use_index(@sel_1 test.t ib) */ a, b from t where b = 1 limit 0, 1`)

	sql, hash := internal.UtilNormalizeWithDefaultDB(t, "select a, b from t where a = 1 limit 0, 1")
	bindData := dom.BindHandle().GetBindRecord(hash, sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select `a` , `b` from `test` . `t` where `a` = ? limit ...", bindData.OriginalSQL)
	bind := bindData.Bindings[0]
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` `ia`)*/ `a`,`b` FROM `test`.`t` WHERE `a` = 1 LIMIT 0,1", bind.BindSQL)
	require.Equal(t, "test", bindData.Db)
	require.Equal(t, bindinfo.Enabled, bind.Status)

	tk.MustQuery("select a, b from t where a = 3 limit 1, 10")
	require.Equal(t, "t:ia", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("select a, b from t where a = 3 limit 1, 100", "ia(a)"))

	tk.MustQuery("select a, b from t where b = 3 limit 1, 100")
	require.Equal(t, "t:ib", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("select a, b from t where b = 3 limit 1, 100", "ib(b)"))
}

func TestErrorBind(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustGetErrMsg("create global binding for select * from t using select * from t", "[schema:1146]Table 'test.t' doesn't exist")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(i int, s varchar(20))")
	tk.MustExec("create table t1(i int, s varchar(20))")
	tk.MustExec("create index index_t on t(i,s)")

	_, err := tk.Exec("create global binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	require.NoError(t, err, "err %v", err)

	sql, hash := parser.NormalizeDigestForBinding("select * from test . t where i > ?")
	bindData := dom.BindHandle().GetBindRecord(hash.String(), sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select * from `test` . `t` where `i` > ?", bindData.OriginalSQL)
	bind := bindData.Bindings[0]
	require.Equal(t, "SELECT * FROM `test`.`t` USE INDEX (`index_t`) WHERE `i` > 100", bind.BindSQL)
	require.Equal(t, "test", bindData.Db)
	require.Equal(t, bindinfo.Enabled, bind.Status)
	require.NotNil(t, bind.Charset)
	require.NotNil(t, bind.Collation)
	require.NotNil(t, bind.CreateTime)
	require.NotNil(t, bind.UpdateTime)

	tk.MustExec("drop index index_t on t")
	rs, err := tk.Exec("select * from t where i > 10")
	require.NoError(t, err)
	rs.Close()

	dom.BindHandle().DropInvalidBindRecord()

	rs, err = tk.Exec("show global bindings")
	require.NoError(t, err)
	chk := rs.NewChunk(nil)
	err = rs.Next(context.TODO(), chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
}

func TestDMLEvolveBaselines(t *testing.T) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_b(b), index idx_c(c))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")
	tk.MustExec("analyze table t")
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")

	tk.MustExec("create global binding for delete from t where b = 1 and c > 1 using delete /*+ use_index(t,idx_c) */ from t where b = 1 and c > 1")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	tk.MustExec("delete /*+ use_index(t,idx_b) */ from t where b = 2 and c > 1")
	require.Equal(t, "t:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustExec("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)

	tk.MustExec("create global binding for update t set a = 1 where b = 1 and c > 1 using update /*+ use_index(t,idx_c) */ t set a = 1 where b = 1 and c > 1")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 2)
	tk.MustExec("update /*+ use_index(t,idx_b) */ t set a = 2 where b = 2 and c > 1")
	require.Equal(t, "t:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustExec("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 2)
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 2)

	tk.MustExec("create table t1 like t")
	tk.MustExec("create global binding for insert into t1 select * from t where t.b = 1 and t.c > 1 using insert into t1 select /*+ use_index(t,idx_c) */ * from t where t.b = 1 and t.c > 1")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 3)
	tk.MustExec("insert into t1 select /*+ use_index(t,idx_b) */ * from t where t.b = 2 and t.c > 2")
	require.Equal(t, "t:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustExec("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 3)
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 3)

	tk.MustExec("create global binding for replace into t1 select * from t where t.b = 1 and t.c > 1 using replace into t1 select /*+ use_index(t,idx_c) */ * from t where t.b = 1 and t.c > 1")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 4)
	tk.MustExec("replace into t1 select /*+ use_index(t,idx_b) */ * from t where t.b = 2 and t.c > 2")
	require.Equal(t, "t:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustExec("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 4)
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 4)
}

func TestAddEvolveTasks(t *testing.T) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a), index idx_b(b), index idx_c(c))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")
	tk.MustExec("analyze table t")
	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 and c = 0 using select * from t use index(idx_a) where a >= 1 and b >= 1 and c = 0")
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")
	// It cannot choose table path although it has lowest cost.
	tk.MustQuery("select * from t where a >= 4 and b >= 1 and c = 0")
	require.Equal(t, "t:idx_a", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustExec("admin flush bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t` WHERE `a` >= 4 AND `b` >= 1 AND `c` = 0", rows[0][1])
	require.Equal(t, "pending verify", rows[0][3])
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t` WHERE `a` >= 4 AND `b` >= 1 AND `c` = 0", rows[0][1])
	status := rows[0][3].(string)
	require.True(t, status == bindinfo.Enabled || status == bindinfo.Rejected)
}

func TestRuntimeHintsInEvolveTasks(t *testing.T) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a), index idx_b(b), index idx_c(c))")

	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 and c = 0 using select * from t use index(idx_a) where a >= 1 and b >= 1 and c = 0")
	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(5000), SET_VAR(TIKV_CLIENT_READ_TIMEOUT=20) */ * from t where a >= 4 and b >= 1 and c = 0")
	tk.MustExec("admin flush bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idx_c`), no_order_index(@`sel_1` `test`.`t` `idx_c`), max_execution_time(5000), set_var(tikv_client_read_timeout = '20')*/ * FROM `test`.`t` WHERE `a` >= 4 AND `b` >= 1 AND `c` = 0", rows[0][1])
}

func TestCaptureBaselinesScope(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	internal.UtilCleanBindingEnv(tk1, dom)
	tk1.MustQuery(`show session variables like "tidb_capture_plan_baselines"`).Check(testkit.Rows(
		"tidb_capture_plan_baselines OFF",
	))
	tk1.MustQuery(`show global variables like "tidb_capture_plan_baselines"`).Check(testkit.Rows(
		"tidb_capture_plan_baselines OFF",
	))
	tk1.MustQuery(`select @@global.tidb_capture_plan_baselines`).Check(testkit.Rows(
		"0",
	))

	tk1.MustExec("SET GLOBAL tidb_capture_plan_baselines = on")
	defer func() {
		tk1.MustExec(" set GLOBAL tidb_capture_plan_baselines = off")
	}()

	tk1.MustQuery(`show variables like "tidb_capture_plan_baselines"`).Check(testkit.Rows(
		"tidb_capture_plan_baselines ON",
	))
	tk1.MustQuery(`show global variables like "tidb_capture_plan_baselines"`).Check(testkit.Rows(
		"tidb_capture_plan_baselines ON",
	))
	tk2.MustQuery(`show global variables like "tidb_capture_plan_baselines"`).Check(testkit.Rows(
		"tidb_capture_plan_baselines ON",
	))
	tk2.MustQuery(`select @@global.tidb_capture_plan_baselines`).Check(testkit.Rows(
		"1",
	))
}

func TestStmtHints(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select /*+ MAX_EXECUTION_TIME(100), SET_VAR(TIKV_CLIENT_READ_TIMEOUT=20), MEMORY_QUOTA(2 GB) */ * from t use index(idx)")
	tk.MustQuery("select * from t")
	require.Equal(t, int64(2147483648), tk.Session().GetSessionVars().MemTracker.GetBytesLimit())
	require.Equal(t, uint64(100), tk.Session().GetSessionVars().StmtCtx.MaxExecutionTime)
	require.Equal(t, uint64(20), tk.Session().GetSessionVars().GetTiKVClientReadTimeout())
	tk.MustQuery("select a, b from t")
	require.Equal(t, int64(1073741824), tk.Session().GetSessionVars().MemTracker.GetBytesLimit())
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().StmtCtx.MaxExecutionTime)
	// TODO(crazycs520): Fix me.
	//require.Equal(t, uint64(0), tk.Session().GetSessionVars().GetTiKVClientReadTimeout())
}

func TestPrivileges(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx)")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	tk.MustExec("create user test@'%'")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "%"}, nil, nil, nil))
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)
}

func TestHintsSetEvolveTask(t *testing.T) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx_a(a))")
	tk.MustExec("create global binding for select * from t where a > 10 using select * from t ignore index(idx_a) where a > 10")
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")
	tk.MustQuery("select * from t use index(idx_a) where a > 0")
	bindHandle := dom.BindHandle()
	bindHandle.SaveEvolveTasksToStore()
	// Verify the added Binding for evolution contains valid ID and Hint, otherwise, panic may happen.
	sql, hash := internal.UtilNormalizeWithDefaultDB(t, "select * from t where a > ?")
	bindData := bindHandle.GetBindRecord(hash, sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", bindData.OriginalSQL)
	require.Len(t, bindData.Bindings, 2)
	bind := bindData.Bindings[1]
	require.Equal(t, bindinfo.PendingVerify, bind.Status)
	require.NotEqual(t, "", bind.ID)
	require.NotNil(t, bind.Hint)
}

func TestHintsSetID(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx_a(a))")
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ use_index(test.t, idx_a) */ * from t where a > 10")
	bindHandle := dom.BindHandle()
	// Verify the added Binding contains ID with restored query block.
	sql, hash := internal.UtilNormalizeWithDefaultDB(t, "select * from t where a > ?")
	bindData := bindHandle.GetBindRecord(hash, sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", bindData.OriginalSQL)
	require.Len(t, bindData.Bindings, 1)
	bind := bindData.Bindings[0]
	require.Equal(t, "use_index(@`sel_1` `test`.`t` `idx_a`)", bind.ID)

	internal.UtilCleanBindingEnv(tk, dom)
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ use_index(t, idx_a) */ * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", bindData.OriginalSQL)
	require.Len(t, bindData.Bindings, 1)
	bind = bindData.Bindings[0]
	require.Equal(t, "use_index(@`sel_1` `test`.`t` `idx_a`)", bind.ID)

	internal.UtilCleanBindingEnv(tk, dom)
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ use_index(@sel_1 t, idx_a) */ * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", bindData.OriginalSQL)
	require.Len(t, bindData.Bindings, 1)
	bind = bindData.Bindings[0]
	require.Equal(t, "use_index(@`sel_1` `test`.`t` `idx_a`)", bind.ID)

	internal.UtilCleanBindingEnv(tk, dom)
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ use_index(@qb1 t, idx_a) qb_name(qb1) */ * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", bindData.OriginalSQL)
	require.Len(t, bindData.Bindings, 1)
	bind = bindData.Bindings[0]
	require.Equal(t, "use_index(@`sel_1` `test`.`t` `idx_a`)", bind.ID)

	internal.UtilCleanBindingEnv(tk, dom)
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ use_index(T, IDX_A) */ * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", bindData.OriginalSQL)
	require.Len(t, bindData.Bindings, 1)
	bind = bindData.Bindings[0]
	require.Equal(t, "use_index(@`sel_1` `test`.`t` `idx_a`)", bind.ID)

	internal.UtilCleanBindingEnv(tk, dom)
	err := tk.ExecToErr("create global binding for select * from t using select /*+ non_exist_hint() */ * from t")
	require.True(t, terror.ErrorEqual(err, parser.ErrParse))
	tk.MustExec("create global binding for select * from t where a > 10 using select * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select * from `test` . `t` where `a` > ?", bindData.OriginalSQL)
	require.Len(t, bindData.Bindings, 1)
	bind = bindData.Bindings[0]
	require.Equal(t, "", bind.ID)
}

func TestNotEvolvePlanForReadStorageHint(t *testing.T) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a), index idx_b(b))")
	tk.MustExec("insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10,10)")
	tk.MustExec("analyze table t")
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	// Make sure the best plan of the SQL is use TiKV index.
	tk.MustExec("set @@session.tidb_executor_concurrency = 4; set @@tidb_allow_mpp=0;")
	rows := tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	require.Equal(t, "cop[tikv]", fmt.Sprintf("%v", rows[len(rows)-1][2]))
	tk.MustExec("set @@tidb_allow_mpp=1")

	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 using select /*+ read_from_storage(tiflash[t]) */ * from t where a >= 1 and b >= 1")
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")

	// Even if index of TiKV has lower cost, it chooses TiFlash.
	rows = tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	require.Equal(t, "mpp[tiflash]", fmt.Sprintf("%v", rows[len(rows)-1][2]))

	tk.MustExec("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	// None evolve task, because of the origin binding is a read_from_storage binding.
	require.Len(t, rows, 1)
	require.Equal(t, "SELECT /*+ read_from_storage(tiflash[`t`])*/ * FROM `test`.`t` WHERE `a` >= 1 AND `b` >= 1", rows[0][1])
	require.Equal(t, bindinfo.Enabled, rows[0][3])
}

func TestBindingWithIsolationRead(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a), index idx_b(b))")
	tk.MustExec("insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10,10)")
	tk.MustExec("analyze table t")
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 using select * from t use index(idx_a) where a >= 1 and b >= 1")
	tk.MustExec("set @@tidb_use_plan_baselines = 1")
	rows := tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	require.Equal(t, "cop[tikv]", rows[len(rows)-1][2])
	// Even if we build a binding use index for SQL, but after we set the isolation read for TiFlash, it choose TiFlash instead of index of TiKV.
	tk.MustExec("set @@tidb_isolation_read_engines = \"tiflash\"")
	rows = tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	require.Equal(t, "mpp[tiflash]", rows[len(rows)-1][2])
}

func TestReCreateBindAfterEvolvePlan(t *testing.T) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a), index idx_b(b), index idx_c(c))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")
	tk.MustExec("analyze table t")
	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 using select * from t use index(idx_a) where a >= 1 and b >= 1")
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")

	// It cannot choose table path although it has lowest cost.
	tk.MustQuery("select * from t where a >= 0 and b >= 0")
	require.Equal(t, "t:idx_a", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])

	tk.MustExec("admin flush bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t` WHERE `a` >= 0 AND `b` >= 0", rows[0][1])
	require.Equal(t, "pending verify", rows[0][3])

	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 using select * from t use index(idx_b) where a >= 1 and b >= 1")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	tk.MustQuery("select * from t where a >= 4 and b >= 1")
	require.Equal(t, "t:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
}

func TestInvisibleIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, unique idx_a(a), index idx_b(b) invisible)")
	tk.MustGetErrMsg(
		"create global binding for select * from t using select * from t use index(idx_b) ",
		"[planner:1176]Key 'idx_b' doesn't exist in table 't'")

	// Create bind using index
	tk.MustExec("create global binding for select * from t using select * from t use index(idx_a) ")

	tk.MustQuery("select * from t")
	require.Equal(t, "t:idx_a", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("select * from t", "idx_a(a)"))

	tk.MustExec(`prepare stmt1 from 'select * from t'`)
	tk.MustExec("execute stmt1")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx_a", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])

	// And then make this index invisible
	tk.MustExec("alter table t alter index idx_a invisible")
	tk.MustQuery("select * from t")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 0)

	tk.MustExec("execute stmt1")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 0)

	tk.MustExec("drop binding for select * from t")
}

func TestReCreateBind(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")

	tk.MustQuery("select * from mysql.bind_info where source != 'builtin'").Check(testkit.Rows())
	tk.MustQuery("show global bindings").Check(testkit.Rows())

	tk.MustExec("create global binding for select * from t using select * from t")
	tk.MustQuery("select original_sql, status from mysql.bind_info where source != 'builtin';").Check(testkit.Rows(
		"select * from `test` . `t` enabled",
	))
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t`", rows[0][0])
	require.Equal(t, bindinfo.Enabled, rows[0][3])

	tk.MustExec("create global binding for select * from t using select * from t")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t`", rows[0][0])
	require.Equal(t, bindinfo.Enabled, rows[0][3])

	rows = tk.MustQuery("select original_sql, status from mysql.bind_info where source != 'builtin';").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "deleted", rows[0][1])
	require.Equal(t, bindinfo.Enabled, rows[1][1])
}

func TestDMLIndexHintBind(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, key idx_b(b), key idx_c(c))")

	tk.MustExec("delete from t where b = 1 and c > 1")
	require.Equal(t, "t:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("delete from t where b = 1 and c > 1", "idx_b(b)"))
	tk.MustExec("create global binding for delete from t where b = 1 and c > 1 using delete from t use index(idx_c) where b = 1 and c > 1")
	tk.MustExec("delete from t where b = 1 and c > 1")
	require.Equal(t, "t:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	require.True(t, tk.MustUseIndex("delete from t where b = 1 and c > 1", "idx_c(c)"))
}

func TestForbidEvolvePlanBaseLinesBeforeGA(t *testing.T) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = false
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	err := tk.ExecToErr("set @@tidb_evolve_plan_baselines=0")
	require.Equal(t, nil, err)
	err = tk.ExecToErr("set @@TiDB_Evolve_pLan_baselines=1")
	require.EqualError(t, err, "Cannot enable baseline evolution feature, it is not generally available now")
	err = tk.ExecToErr("set @@TiDB_Evolve_pLan_baselines=oN")
	require.EqualError(t, err, "Cannot enable baseline evolution feature, it is not generally available now")
	err = tk.ExecToErr("admin evolve bindings")
	require.EqualError(t, err, "Cannot enable baseline evolution feature, it is not generally available now")
}

func TestSPMWithoutUseDatabase(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	internal.UtilCleanBindingEnv(tk, dom)
	internal.UtilCleanBindingEnv(tk1, dom)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create global binding for select * from t using select * from t force index(a)")

	err := tk1.ExecToErr("select * from t")
	require.Error(t, err)
	require.Regexp(t, "No database selected$", err)
	tk1.MustQuery(`select @@last_plan_from_binding;`).Check(testkit.Rows("0"))
	require.True(t, tk1.MustUseIndex("select * from test.t", "a"))
	tk1.MustExec("select * from test.t")
	tk1.MustQuery(`select @@last_plan_from_binding;`).Check(testkit.Rows("1"))
	tk1.MustExec("set binding disabled for select * from test.t")
	tk1.MustExec("select * from test.t")
	tk1.MustQuery(`select @@last_plan_from_binding;`).Check(testkit.Rows("0"))
}

func TestBindingWithoutCharset(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) CHARACTER SET utf8)")
	tk.MustExec("create global binding for select * from t where a = 'aa' using select * from t where a = 'aa'")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` = ?", rows[0][0])
	require.Equal(t, "SELECT * FROM `test`.`t` WHERE `a` = 'aa'", rows[0][1])
}

func TestBindingWithMultiParenthesis(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create global binding for select * from (select * from t where a = 1) tt using select * from (select * from t where a = 1) tt")
	tk.MustExec("create global binding for select * from ((select * from t where a = 1)) tt using select * from (select * from t where a = 1) tt")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from ( select * from `test` . `t` where `a` = ? ) as `tt`", rows[0][0])
	require.Equal(t, "SELECT * FROM (SELECT * FROM `test`.`t` WHERE `a` = 1) AS `tt`", rows[0][1])
}

func TestGCBindRecord(t *testing.T) {
	// set lease for gc tests
	originLease := bindinfo.Lease
	bindinfo.Lease = 0
	defer func() {
		bindinfo.Lease = originLease
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key(a))")

	tk.MustExec("create global binding for select * from t where a = 1 using select * from t use index(a) where a = 1")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` = ?", rows[0][0])
	require.Equal(t, bindinfo.Enabled, rows[0][3])
	tk.MustQuery("select status from mysql.bind_info where original_sql = 'select * from `test` . `t` where `a` = ?'").Check(testkit.Rows(
		bindinfo.Enabled,
	))

	h := dom.BindHandle()
	// bindinfo.Lease is set to 0 for test env in SetUpSuite.
	require.NoError(t, h.GCBindRecord())
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` = ?", rows[0][0])
	require.Equal(t, bindinfo.Enabled, rows[0][3])
	tk.MustQuery("select status from mysql.bind_info where original_sql = 'select * from `test` . `t` where `a` = ?'").Check(testkit.Rows(
		bindinfo.Enabled,
	))

	tk.MustExec("drop global binding for select * from t where a = 1")
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustQuery("select status from mysql.bind_info where original_sql = 'select * from `test` . `t` where `a` = ?'").Check(testkit.Rows(
		"deleted",
	))
	require.NoError(t, h.GCBindRecord())
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustQuery("select status from mysql.bind_info where original_sql = 'select * from `test` . `t` where `a` = ?'").Check(testkit.Rows())
}

func TestBindSQLDigest(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int primary key, a int, b int, key(a), key(b))")

	cases := []struct {
		origin string
		hint   string
	}{
		// agg hints
		{"select count(1) from t", "select /*+ hash_agg() */ count(1) from t"},
		{"select count(1) from t", "select /*+ stream_agg() */ count(1) from t"},
		// join hints
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ merge_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ tidb_smj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ hash_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ tidb_hj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ inl_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ tidb_inlj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ inl_hash_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		// index hints
		{"select * from t", "select * from t use index(primary)"},
		{"select * from t", "select /*+ use_index(primary) */ * from t"},
		{"select * from t", "select * from t use index(a)"},
		{"select * from t", "select /*+ use_index(a) */ * from t use index(a)"},
		{"select * from t", "select * from t use index(b)"},
		{"select * from t", "select /*+ use_index(b) */ * from t use index(b)"},
		{"select a, b from t where a=1 or b=1", "select /*+ use_index_merge(t, a, b) */ a, b from t where a=1 or b=1"},
		{"select * from t where a=1", "select /*+ ignore_index(t, a) */ * from t where a=1"},
		// push-down hints
		{"select * from t limit 10", "select /*+ limit_to_cop() */ * from t limit 10"},
		{"select a, count(*) from t group by a", "select /*+ agg_to_cop() */ a, count(*) from t group by a"},
		// index-merge hints
		{"select a, b from t where a>1 or b>1", "select /*+ no_index_merge() */ a, b from t where a>1 or b>1"},
		{"select a, b from t where a>1 or b>1", "select /*+ use_index_merge(t, a, b) */ a, b from t where a>1 or b>1"},
		// runtime hints
		{"select * from t", "select /*+ memory_quota(1024 MB) */ * from t"},
		{"select * from t", "select /*+ max_execution_time(1000) */ * from t"},
		{"select * from t", "select /*+ set_var(tikv_client_read_timeout=1000) */ * from t"},
		// storage hints
		{"select * from t", "select /*+ read_from_storage(tikv[t]) */ * from t"},
		// others
		{"select t1.a, t1.b from t t1 where t1.a in (select t2.a from t t2)", "select /*+ use_toja(true) */ t1.a, t1.b from t t1 where t1.a in (select t2.a from t t2)"},
	}
	for _, c := range cases {
		stmtsummary.StmtSummaryByDigestMap.Clear()
		internal.UtilCleanBindingEnv(tk, dom)
		sql := "create global binding for " + c.origin + " using " + c.hint
		tk.MustExec(sql)
		res := tk.MustQuery(`show global bindings`).Rows()
		require.Equal(t, len(res[0]), 11)

		parser4binding := parser.New()
		originNode, err := parser4binding.ParseOneStmt(c.origin, "utf8mb4", "utf8mb4_general_ci")
		require.NoError(t, err)
		_, sqlDigestWithDB := parser.NormalizeDigestForBinding(utilparser.RestoreWithDefaultDB(originNode, "test", c.origin))
		require.Equal(t, res[0][9], sqlDigestWithDB.String())
	}
}

func TestDropBindBySQLDigest(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int primary key, a int, b int, key(a), key(b))")

	cases := []struct {
		origin string
		hint   string
	}{
		// agg hints
		{"select count(1) from t", "select /*+ hash_agg() */ count(1) from t"},
		{"select count(1) from t", "select /*+ stream_agg() */ count(1) from t"},
		// join hints
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ merge_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ tidb_smj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ hash_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ tidb_hj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ inl_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ tidb_inlj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ inl_hash_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		// index hints
		{"select * from t", "select * from t use index(primary)"},
		{"select * from t", "select /*+ use_index(primary) */ * from t"},
		{"select * from t", "select * from t use index(a)"},
		{"select * from t", "select /*+ use_index(a) */ * from t use index(a)"},
		{"select * from t", "select * from t use index(b)"},
		{"select * from t", "select /*+ use_index(b) */ * from t use index(b)"},
		{"select a, b from t where a=1 or b=1", "select /*+ use_index_merge(t, a, b) */ a, b from t where a=1 or b=1"},
		{"select * from t where a=1", "select /*+ ignore_index(t, a) */ * from t where a=1"},
		// push-down hints
		{"select * from t limit 10", "select /*+ limit_to_cop() */ * from t limit 10"},
		{"select a, count(*) from t group by a", "select /*+ agg_to_cop() */ a, count(*) from t group by a"},
		// index-merge hints
		{"select a, b from t where a>1 or b>1", "select /*+ no_index_merge() */ a, b from t where a>1 or b>1"},
		{"select a, b from t where a>1 or b>1", "select /*+ use_index_merge(t, a, b) */ a, b from t where a>1 or b>1"},
		// runtime hints
		{"select * from t", "select /*+ memory_quota(1024 MB) */ * from t"},
		{"select * from t", "select /*+ max_execution_time(1000) */ * from t"},
		{"select * from t", "select /*+ set_var(tikv_client_read_timeout=1000) */ * from t"},
		// storage hints
		{"select * from t", "select /*+ read_from_storage(tikv[t]) */ * from t"},
		// others
		{"select t1.a, t1.b from t t1 where t1.a in (select t2.a from t t2)", "select /*+ use_toja(true) */ t1.a, t1.b from t t1 where t1.a in (select t2.a from t t2)"},
	}

	h := dom.BindHandle()
	// global scope
	for _, c := range cases {
		internal.UtilCleanBindingEnv(tk, dom)
		sql := "create global binding for " + c.origin + " using " + c.hint
		tk.MustExec(sql)
		h.ReloadBindings()
		res := tk.MustQuery(`show global bindings`).Rows()

		require.Equal(t, len(res), 1)
		require.Equal(t, len(res[0]), 11)
		drop := fmt.Sprintf("drop global binding for sql digest '%s'", res[0][9])
		tk.MustExec(drop)
		require.NoError(t, h.GCBindRecord())
		h.ReloadBindings()
		tk.MustQuery("show global bindings").Check(testkit.Rows())
	}

	// session scope
	for _, c := range cases {
		internal.UtilCleanBindingEnv(tk, dom)
		sql := "create binding for " + c.origin + " using " + c.hint
		tk.MustExec(sql)
		res := tk.MustQuery(`show bindings`).Rows()

		require.Equal(t, len(res), 1)
		require.Equal(t, len(res[0]), 11)
		drop := fmt.Sprintf("drop binding for sql digest '%s'", res[0][9])
		tk.MustExec(drop)
		require.NoError(t, h.GCBindRecord())
		tk.MustQuery("show bindings").Check(testkit.Rows())
	}

	// exception cases
	tk.MustGetErrMsg(fmt.Sprintf("drop binding for sql digest '%s'", "1"), "can't find any binding for '1'")
	tk.MustGetErrMsg(fmt.Sprintf("drop binding for sql digest '%s'", ""), "sql digest is empty")
}
<<<<<<< HEAD
=======

func TestJoinOrderHintWithBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2, t3;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")

	tk.MustExec("create global binding for select * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b using select /*+ leading(t2) */ * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustExec("select * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res := tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from ( `test` . `t1` join `test` . `t2` on `t1` . `a` = `t2` . `a` ) left join `test` . `t3` on `t2` . `b` = `t3` . `b`")

	tk.MustExec("drop global binding for select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
}

func TestNormalizeStmtForBinding(t *testing.T) {
	tests := []struct {
		sql        string
		normalized string
		digest     string
	}{
		{"select 1 from b where (x,y) in ((1, 3), ('3', 1))", "select ? from `b` where row ( `x` , `y` ) in ( ... )", "ab6c607d118c24030807f8d1c7c846ec23e3b752fd88ed763bb8e26fbfa56a83"},
		{"select 1 from b where (x,y) in ((1, 3), ('3', 1), (2, 3))", "select ? from `b` where row ( `x` , `y` ) in ( ... )", "ab6c607d118c24030807f8d1c7c846ec23e3b752fd88ed763bb8e26fbfa56a83"},
		{"select 1 from b where (x,y) in ((1, 3), ('3', 1), (2, 3),('x', 'y'))", "select ? from `b` where row ( `x` , `y` ) in ( ... )", "ab6c607d118c24030807f8d1c7c846ec23e3b752fd88ed763bb8e26fbfa56a83"},
		{"select 1 from b where (x,y) in ((1, 3), ('3', 1), (2, 3),('x', 'y'),('x', 'y'))", "select ? from `b` where row ( `x` , `y` ) in ( ... )", "ab6c607d118c24030807f8d1c7c846ec23e3b752fd88ed763bb8e26fbfa56a83"},
		{"select 1 from b where (x) in ((1), ('3'), (2),('x'),('x'))", "select ? from `b` where ( `x` ) in ( ( ... ) )", "03e6e1eb3d76b69363922ff269284b359ca73351001ba0e82d3221c740a6a14c"},
		{"select 1 from b where (x) in ((1), ('3'), (2),('x'))", "select ? from `b` where ( `x` ) in ( ( ... ) )", "03e6e1eb3d76b69363922ff269284b359ca73351001ba0e82d3221c740a6a14c"},
	}
	for _, test := range tests {
		stmt, _, _ := internal.UtilNormalizeWithDefaultDB(t, test.sql)
		n, digest := norm.NormalizeStmtForBinding(stmt, norm.WithFuzz(true))
		require.Equal(t, test.normalized, n)
		require.Equal(t, test.digest, digest)
	}
}
>>>>>>> 707b0a4e381 (parser: support (Row(..),Row(..))=>(..) in the binding mode (#51319))
