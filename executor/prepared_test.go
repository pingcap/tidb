// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestPreparedNameResolver(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, KEY id (id))")
	tk.MustExec("prepare stmt from 'select * from t limit ? offset ?'")
	_, err := tk.Exec("prepare stmt from 'select b from t'")
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'b' in 'field list'")

	_, err = tk.Exec("prepare stmt from '(select * FROM t) union all (select * FROM t) order by a limit ?'")
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'a' in 'order clause'")
}

// a 'create table' DDL statement should be accepted if it has no parameters.
func (s *testSuite1) TestPreparedDDL(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("prepare stmt from 'create table t (id int, KEY id (id))'")
}

func (s *testSuite1) TestIgnorePlanCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (id int primary key, num int)")
	tk.MustExec("insert into t values (1, 1)")
	tk.MustExec("insert into t values (2, 2)")
	tk.MustExec("insert into t values (3, 3)")
	tk.MustExec("prepare stmt from 'select /*+ IGNORE_PLAN_CACHE() */ * from t where id=?'")
	tk.MustExec("set @ignore_plan_doma = 1")
	tk.MustExec("execute stmt using @ignore_plan_doma")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.UseCache, IsFalse)
}

func (s *testSuite1) TestPrepareStmtAfterIsolationReadChange(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	// create virtual tiflash replica.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines='tikv'")
	tk.MustExec("prepare stmt from \"select * from t\"")
	tk.MustQuery("execute stmt")
	tkProcess := tk.Se.ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	c.Assert(rows[len(rows)-1][2], Equals, "cop[tikv]")

	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("execute stmt")
	tkProcess = tk.Se.ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	c.Assert(rows[len(rows)-1][2], Equals, "cop[tiflash]")

	c.Assert(len(tk.Se.GetSessionVars().PreparedStmts), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().PreparedStmts[1].(*plannercore.CachedPrepareStmt).NormalizedSQL, Equals, "select * from t")
	c.Assert(tk.Se.GetSessionVars().PreparedStmts[1].(*plannercore.CachedPrepareStmt).NormalizedPlan, Equals, "")
}
