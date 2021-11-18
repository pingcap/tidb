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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

func TestListColumnsPartitionWithGlobalIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	// Test generated column with global index
	restoreConfig := config.RestoreFunc()
	defer restoreConfig()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableGlobalIndex = true
	})
	tableDefs := []string{
		// Test for virtual generated column with global index
		`create table t (a varchar(10), b varchar(1) GENERATED ALWAYS AS (substr(a,1,1)) VIRTUAL) partition by list columns(b) (partition p0 values in ('a','c'), partition p1 values in ('b','d'));`,
		// Test for stored generated column with global index
		`create table t (a varchar(10), b varchar(1) GENERATED ALWAYS AS (substr(a,1,1)) STORED) partition by list columns(b) (partition p0 values in ('a','c'), partition p1 values in ('b','d'));`,
	}
	for _, tbl := range tableDefs {
		tk.MustExec("drop table if exists t")
		tk.MustExec(tbl)
		tk.MustExec("alter table t add unique index (a)")
		tk.MustExec("insert into t (a) values  ('aaa'),('abc'),('acd')")
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("aaa", "abc", "acd"))
		tk.MustQuery("select * from t where a = 'abc' order by a").Check(testkit.Rows("abc a"))
		tk.MustExec("update t set a='bbb' where a = 'aaa'")
		tk.MustExec("admin check table t")
		tk.MustQuery("select a from t order by a").Check(testkit.Rows("abc", "acd", "bbb"))
		// TODO: fix below test.
		//tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("abc", "acd"))
		//tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("bbb"))
		tk.MustQuery("select * from t where a = 'bbb' order by a").Check(testkit.Rows("bbb b"))
		// Test insert meet duplicate error.
		_, err := tk.Exec("insert into t (a) values  ('abc')")
		require.Error(t, err)
		// Test insert on duplicate update
		tk.MustExec("insert into t (a) values ('abc') on duplicate key update a='bbc'")
		tk.MustQuery("select a from t order by a").Check(testkit.Rows("acd", "bbb", "bbc"))
		tk.MustQuery("select * from t where a = 'bbc'").Check(testkit.Rows("bbc b"))
		// TODO: fix below test.
		//tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("acd"))
		//tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("bbb", "bbc"))
	}
}

func TestIssue20724(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a varchar(10) collate utf8mb4_general_ci)")
	tk.MustExec("insert into t1 values ('a')")
	tk.MustExec("update t1 set a = 'A'")
	tk.MustQuery("select * from t1").Check(testkit.Rows("A"))
	tk.MustExec("drop table t1")
}

func TestIssue20840(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table t1 (i varchar(20) unique key) collate=utf8mb4_general_ci")
	tk.MustExec("insert into t1 values ('a')")
	tk.MustExec("replace into t1 values ('A')")
	tk.MustQuery("select * from t1").Check(testkit.Rows("A"))
	tk.MustExec("drop table t1")
}

func TestIssueInsertPrefixIndexForNonUTF8Collation(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1 ( c_int int, c_str varchar(40) character set ascii collate ascii_bin, primary key(c_int, c_str(8)) clustered , unique key(c_str))")
	tk.MustExec("create table t2 ( c_int int, c_str varchar(40) character set latin1 collate latin1_bin, primary key(c_int, c_str(8)) clustered , unique key(c_str))")
	tk.MustExec("insert into t1 values (3, 'fervent brattain')")
	tk.MustExec("insert into t2 values (3, 'fervent brattain')")
	tk.MustExec("admin check table t1")
	tk.MustExec("admin check table t2")

	tk.MustExec("create table t3 (x varchar(40) CHARACTER SET ascii COLLATE ascii_bin, UNIQUE KEY uk(x(4)))")
	tk.MustExec("insert into t3 select 'abc '")
	tk.MustGetErrCode("insert into t3 select 'abc d'", 1062)
}
