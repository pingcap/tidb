// Copyright 2018 PingCAP, Inc.
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

package ddl_test

import (
	"fmt"

	gofail "github.com/etcd-io/gofail/runtime"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/testkit"
	"golang.org/x/net/context"
)

// TestInitializeOffsetAndState tests the case that the column's offset and state don't be initialized in the file of ddl_api.go when
// doing the operation of 'modify column'.
func (s *testStateChangeSuite) TestInitializeOffsetAndState(c *C) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create table t(a int, b int, c int)")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table t")

	gofail.Enable("github.com/pingcap/tidb/ddl/uninitializedOffsetAndState", `return(true)`)
	_, err = s.se.Execute(context.Background(), "ALTER TABLE t MODIFY COLUMN b int FIRST;")
	c.Assert(err, IsNil)
	gofail.Disable("github.com/pingcap/tidb/ddl/uninitializedOffsetAndState")
}

func (s *testDBSuite) TestUpdateHandleFailed(c *C) {
	gofail.Enable("github.com/pingcap/tidb/ddl/errorUpdateReorgHandle", `return(true)`)
	defer gofail.Disable("github.com/pingcap/tidb/ddl/errorUpdateReorgHandle")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_handle_failed")
	defer tk.MustExec("drop database test_handle_failed")
	tk.MustExec("use test_handle_failed")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values(-1, 1)")
	tk.MustExec("alter table t add index idx_b(b)")
	result := tk.MustQuery("select count(*) from t use index(idx_b)")
	result.Check(testkit.Rows("1"))
	tk.MustExec("admin check index t idx_b")
}

func (s *testDBSuite) TestAddIndexFailed(c *C) {
	gofail.Enable("github.com/pingcap/tidb/ddl/mockAddIndexErr", `return(true)`)
	defer gofail.Disable("github.com/pingcap/tidb/ddl/mockAddIndexErr")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_add_index_failed")
	defer tk.MustExec("drop database test_add_index_failed")
	tk.MustExec("use test_add_index_failed")

	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%v, %v)", i, i))
	}

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_add_index_failed"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	// Split the table.
	s.cluster.SplitTable(s.mvccStore, tblID, 100)

	tk.MustExec("alter table t add index idx_b(b)")
	tk.MustExec("admin check index t idx_b")
	tk.MustExec("admin check table t")
}
