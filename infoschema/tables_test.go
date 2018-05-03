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

package infoschema_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testSuite) TestDataForTableRowsCountField(c *C) {
	testleak.BeforeTest()
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	do, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer do.Close()

	h := do.StatsHandle()
	is := do.InfoSchema()
	tk := testkit.NewTestKit(c, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustQuery("select table_rows from information_schema.tables where table_name='t'").Check(
		testkit.Rows("0"))
	tk.MustExec("insert into t(c, d) values(1, 2), (2, 3), (3, 4)")
	h.DumpStatsDeltaToKV()
	h.Update(is)
	tk.MustQuery("select table_rows from information_schema.tables where table_name='t'").Check(
		testkit.Rows("3"))
	tk.MustExec("insert into t(c, d) values(4, 5)")
	h.DumpStatsDeltaToKV()
	h.Update(is)
	tk.MustQuery("select table_rows from information_schema.tables where table_name='t'").Check(
		testkit.Rows("4"))
	tk.MustExec("delete from t where c >= 3")
	h.DumpStatsDeltaToKV()
	h.Update(is)
	tk.MustQuery("select table_rows from information_schema.tables where table_name='t'").Check(
		testkit.Rows("2"))
	tk.MustExec("delete from t where c=3")
	h.DumpStatsDeltaToKV()
	h.Update(is)
	tk.MustQuery("select table_rows from information_schema.tables where table_name='t'").Check(
		testkit.Rows("2"))

	tk.MustExec("create user xxx")
	tk.MustExec("flush privileges")

	tk1 := testkit.NewTestKit(c, store)
	tk1.MustExec("use test")
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{
		Username: "xxx",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)

	tk1.MustQuery("select distinct(table_schema) from information_schema.tables").Check(testkit.Rows("INFORMATION_SCHEMA"))
}
