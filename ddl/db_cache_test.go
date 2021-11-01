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

package ddl_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/testkit"
)

// test alter table cache
func (s *testDBSuite2) TestAlterTableCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk2.MustExec("use test")
	/* Test of cache table */
	tk.MustExec("create table t1 ( n int auto_increment primary key)")
	tk.MustGetErrCode("alter table t1 ca", errno.ErrParse)
	tk.MustGetErrCode("alter table t2 cache", errno.ErrNoSuchTable)
	tk.MustExec("alter table t1 cache")
	checkTableCache(c, tk.Se, "test", "t1")
	tk.MustExec("drop table if exists t1")
	/*Test can't skip schema checker*/
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("CREATE TABLE t2 (a int)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tk2.MustExec("alter table t1 cache;")
	_, err := tk.Exec("commit")
	c.Assert(terror.ErrorEqual(domain.ErrInfoSchemaChanged, err), IsTrue)
	/* Test can skip schema checker */
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=2;")
	tk2.MustExec("alter table t2 cache")
	tk.MustExec("commit")
	// Test if a table is not exists
	tk.MustExec("drop table if exists t")
	tk.MustGetErrCode("alter table t cache", errno.ErrNoSuchTable)
	tk.MustExec("create table t (a int)")
	tk.MustExec("alter table t cache")
	// Multiple alter cache is okay
	tk.MustExec("alter table t cache")
	tk.MustExec("alter table t cache")
	// Test a temporary table
	tk.MustExec("drop table if exists t")
	tk.MustExec("create temporary table t (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("drop table if exists tmp1")
	// local temporary table alter is not supported
	tk.MustGetErrCode("alter table t cache", errno.ErrUnsupportedDDLOperation)
	// test global temporary table
	tk.MustExec("create global temporary table tmp1 " +
		"(id int not null primary key, code int not null, value int default null, unique key code(code))" +
		"on commit delete rows")
	tk.MustGetErrMsg("alter table tmp1 cache", ddl.ErrOptOnTemporaryTable.GenWithStackByArgs("alter temporary table cache").Error())

}

func (s *testDBSuite2) TestAlterPartitionCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists cache_partition_table;")
	tk.MustExec("create table cache_partition_table (a int, b int) partition by hash(a) partitions 3;")
	tk.MustGetErrCode("alter table cache_partition_table cache", errno.ErrOptOnCacheTable)
	defer tk.MustExec("drop table if exists cache_partition_table;")
	tk.MustExec("drop table if exists cache_partition_range_table;")
	tk.MustExec(`create table cache_partition_range_table (c1 smallint(6) not null, c2 char(5) default null) partition by range ( c1 ) (
			partition p0 values less than (10),
			partition p1 values less than (20),
			partition p2 values less than (30),
			partition p3 values less than (MAXVALUE)
	);`)
	tk.MustGetErrCode("alter table cache_partition_range_table cache;", errno.ErrOptOnCacheTable)
	defer tk.MustExec("drop table if exists cache_partition_range_table;")
	tk.MustExec("drop table if exists partition_list_table;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec(`create table cache_partition_list_table (id int) partition by list  (id) (
	    partition p0 values in (1,2),
	    partition p1 values in (3,4),
	    partition p3 values in (5,null)
	);`)
	tk.MustGetErrCode("alter table cache_partition_list_table cache", errno.ErrOptOnCacheTable)
	tk.MustExec("drop table if exists cache_partition_list_table;")
}

func (s *testDBSuite2) TestAlterViewTableCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists cache_view_t")
	tk.MustExec("create table cache_view_t (id int);")
	tk.MustExec("create view v as select * from cache_view_t")
	tk.MustGetErrCode("alter table v cache", errno.ErrWrongObject)
}
