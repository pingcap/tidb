// Copyright 2022 PingCAP, Inc.
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
	"fmt"
	"sync"
	"testing"

	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

var foreignKeyTestCase1 = []struct {
	prepareSQLs []string
	notNull     bool
}{
	// Case-1: test unique index only contain foreign key columns.
	{
		prepareSQLs: []string{
			"create table t1 (id int, a int, b int,  unique index(id), unique index(a, b));",
			"create table t2 (b int, name varchar(10), a int, id int, unique index(id), unique index (a,b), foreign key fk(a, b) references t1(a, b));",
		},
	},
	// Case-2: test unique index contain foreign key columns and other columns.
	{
		prepareSQLs: []string{
			"create table t1 (id int key, a int, b int, unique index(id), unique index(a, b, id));",
			"create table t2 (b int, a int, id int key, name varchar(10), unique index (a,b, id), foreign key fk(a, b) references t1(a, b));",
		},
	},
	// Case-3: test non-unique index only contain foreign key columns.
	{
		prepareSQLs: []string{
			"create table t1 (id int key,a int, b int, unique index(id), index(a, b));",
			"create table t2 (b int, a int, name varchar(10), id int key, index (a, b), foreign key fk(a, b) references t1(a, b));",
		},
	},
	// Case-4: test non-unique index contain foreign key columns and other columns.
	{
		prepareSQLs: []string{
			"create table t1 (id int key,a int, b int,  unique index(id), index(a, b, id));",
			"create table t2 (name varchar(10), b int, a int, id int key, index (a, b, id), foreign key fk(a, b) references t1(a, b));",
		},
	},
	//Case-5: test primary key only contain foreign key columns, and disable tidb_enable_clustered_index.
	{
		prepareSQLs: []string{
			"set @@tidb_enable_clustered_index=0;",
			"create table t1 (id int, a int, b int,  unique index(id), primary key (a, b));",
			"create table t2 (b int, name varchar(10), a int, id int, unique index(id), primary key (a, b), foreign key fk(a, b) references t1(a, b));",
		},
		notNull: true,
	},
	// Case-6: test primary key only contain foreign key columns, and enable tidb_enable_clustered_index.
	{
		prepareSQLs: []string{
			"set @@tidb_enable_clustered_index=1;",
			"create table t1 (id int, a int, b int,  unique index(id), primary key (a, b));",
			"create table t2 (b int,  a int, name varchar(10), id int, unique index(id), primary key (a, b), foreign key fk(a, b) references t1(a, b));",
		},
		notNull: true,
	},
	// Case-7: test primary key contain foreign key columns and other column, and disable tidb_enable_clustered_index.
	{
		prepareSQLs: []string{
			"set @@tidb_enable_clustered_index=0;",
			"create table t1 (id int, a int, b int,  unique index(id), primary key (a, b, id));",
			"create table t2 (b int,  a int, id int, name varchar(10), unique index(id), primary key (a, b, id), foreign key fk(a, b) references t1(a, b));",
		},
		notNull: true,
	},
	// Case-8: test primary key contain foreign key columns and other column, and enable tidb_enable_clustered_index.
	{
		prepareSQLs: []string{
			"set @@tidb_enable_clustered_index=1;",
			"create table t1 (id int, a int, b int,  unique index(id), primary key (a, b, id));",
			"create table t2 (name varchar(10), b int,  a int, id int, unique index(id), primary key (a, b, id), foreign key fk(a, b) references t1(a, b));",
		},
		notNull: true,
	},
}

func TestForeignKeyOnInsertChildTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	tk.MustExec("create table t_data (id int, a int, b int)")
	tk.MustExec("insert into t_data (id, a, b) values (1, 1, 1), (2, 2, 2);")
	for _, ca := range foreignKeyTestCase1 {
		tk.MustExec("drop table if exists t2;")
		tk.MustExec("drop table if exists t1;")
		for _, sql := range ca.prepareSQLs {
			tk.MustExec(sql)
		}
		tk.MustExec("insert into t1 (id, a, b) values (1, 1, 1);")
		tk.MustExec("insert into t2 (id, a, b) values (1, 1, 1)")
		if !ca.notNull {
			tk.MustExec("insert into t2 (id, a, b) values (2, null, 1)")
			tk.MustExec("insert into t2 (id, a, b) values (3, 1, null)")
			tk.MustExec("insert into t2 (id, a, b) values (4, null, null)")
		}
		tk.MustGetDBError("insert into t2 (id, a, b) values (5, 1, 0);", plannercore.ErrNoReferencedRow2)
		tk.MustGetDBError("insert into t2 (id, a, b) values (6, 0, 1);", plannercore.ErrNoReferencedRow2)
		tk.MustGetDBError("insert into t2 (id, a, b) values (7, 2, 2);", plannercore.ErrNoReferencedRow2)
		// Test insert from select.
		tk.MustExec("delete from t2")
		tk.MustExec("insert into t2 (id, a, b) select id, a, b from t_data where t_data.id=1")
		tk.MustGetDBError("insert into t2 (id, a, b) select id, a, b from t_data where t_data.id=2", plannercore.ErrNoReferencedRow2)

		// Test in txn
		tk.MustExec("delete from t2")
		tk.MustExec("begin")
		tk.MustExec("delete from t1 where a=1")
		tk.MustGetDBError("insert into t2 (id, a, b) values (1, 1, 1)", plannercore.ErrNoReferencedRow2)
		tk.MustExec("insert into t1 (id, a, b) values (2, 2, 2)")
		tk.MustExec("insert into t2 (id, a, b) values (2, 2, 2)")
		tk.MustExec("rollback")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 1 1"))
		tk.MustQuery("select id, a, b from t2 order by id").Check(testkit.Rows())
	}

	// Case-10: test primary key is handle and contain foreign key column, and foreign key column has default value.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("set @@tidb_enable_clustered_index=0;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (id int,a int, primary key(id));")
	tk.MustExec("create table t2 (id int key,a int not null default 0, index (a), foreign key fk(a) references t1(id));")
	tk.MustExec("insert into t1 values (1, 1);")
	tk.MustExec("insert into t2 values (1, 1);")
	tk.MustGetDBError("insert into t2 (id) values (10);", plannercore.ErrNoReferencedRow2)
	tk.MustGetDBError("insert into t2 values (3, 2);", plannercore.ErrNoReferencedRow2)

	// Case-11: test primary key is handle and contain foreign key column, and foreign key column doesn't have default value.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (id int key,a int, index (a), foreign key fk(a) references t1(id));")
	tk.MustExec("insert into t2 values (1, 1);")
	tk.MustExec("insert into t2 (id) values (10);")
	tk.MustGetDBError("insert into t2 values (3, 2);", plannercore.ErrNoReferencedRow2)
}

func TestForeignKeyOnInsertDuplicateUpdateChildTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	for _, ca := range foreignKeyTestCase1 {
		tk.MustExec("drop table if exists t2;")
		tk.MustExec("drop table if exists t1;")
		for _, sql := range ca.prepareSQLs {
			tk.MustExec(sql)
		}
		tk.MustExec("insert into t1 (id, a, b) values (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 11, 21, 'a')")

		sqls := []string{
			"insert into t2 (id, a, b, name) values (1, 12, 22, 'b') on duplicate key update a = 100",
			"insert into t2 (id, a, b, name) values (1, 13, 23, 'c') on duplicate key update a = a+10",
			"insert into t2 (id, a, b, name) values (1, 14, 24, 'd') on duplicate key update a = a + 100",
			"insert into t2 (id, a, b, name) values (1, 14, 24, 'd') on duplicate key update a = 12, b = 23",
		}
		for _, sqlStr := range sqls {
			tk.MustGetDBError(sqlStr, plannercore.ErrNoReferencedRow2)
		}
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 14, 26, 'b') on duplicate key update a = 12, b = 22, name = 'x'")
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 12 22 x"))
		if !ca.notNull {
			tk.MustExec("insert into t2 (id, a, b, name) values (1, 14, 26, 'b') on duplicate key update a = null, b = 22, name = 'y'")
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 <nil> 22 y"))
			tk.MustExec("insert into t2 (id, a, b, name) values (1, 15, 26, 'b') on duplicate key update b = null, name = 'z'")
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 <nil> <nil> z"))
		}
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 15, 26, 'b') on duplicate key update a=13,b=23, name = 'c'")
		tk.MustQuery("select id, a, b, name from t2").Check(testkit.Rows("1 13 23 c"))

		// Test In txn.
		tk.MustExec("delete from t2")
		tk.MustExec("delete from t1")
		tk.MustExec("insert into t1 (id, a, b) values (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)")
		tk.MustExec("insert into t2 (id, a, b, name) values (2, 11, 21, 'a')")
		tk.MustExec("begin")
		tk.MustExec("insert into t2 (id, a, b, name) values (2, 14, 26, 'b') on duplicate key update a = 12, b = 22, name = 'x'")
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("2 12 22 x"))
		tk.MustExec("rollback")
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("2 11 21 a"))

		tk.MustExec("begin")
		tk.MustExec("delete from t1 where id=3")
		tk.MustGetDBError("insert into t2 (id, a, b, name) values (2, 13, 23, 'y') on duplicate key update a = 13, b = 23, name = 'y'", plannercore.ErrNoReferencedRow2)
		tk.MustExec("insert into t2 (id, a, b, name) values (2, 14, 24, 'z') on duplicate key update a = 14, b = 24, name = 'z'")
		tk.MustExec("insert into t1 (id, a, b) values (5, 15, 25)")
		tk.MustExec("insert into t2 (id, a, b, name) values (2, 15, 25, 'o') on duplicate key update a = 15, b = 25, name = 'o'")
		tk.MustExec("delete from t1 where id=1")
		tk.MustGetDBError("insert into t2 (id, a, b, name) values (2, 11, 21, 'y') on duplicate key update a = 11, b = 21, name = 'p'", plannercore.ErrNoReferencedRow2)
		tk.MustExec("commit")
		tk.MustQuery("select id, a, b, name from t2").Check(testkit.Rows("2 15 25 o"))
	}

	// Case-9: test primary key is handle and contain foreign key column.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("set @@tidb_enable_clustered_index=0;")
	tk.MustExec("create table t1 (id int, a int, b int,  primary key (id));")
	tk.MustExec("create table t2 (b int,  a int, id int, name varchar(10), primary key (a), foreign key fk(a) references t1(id));")
	tk.MustExec("insert into t1 (id, a, b) values       (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)")
	tk.MustExec("insert into t2 (id, a, b, name) values (11, 1, 21, 'a')")

	tk.MustExec("insert into t2 (id, a) values (11, 1) on duplicate key update a = 2, name = 'b'")
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("11 2 21 b"))
	tk.MustExec("insert into t2 (id, a, b)    values (11, 2, 22) on duplicate key update a = 3, name = 'c'")
	tk.MustExec("insert into t2 (id, a, name) values (11, 3, 'b') on duplicate key update b = b+10, name = 'd'")
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("11 3 31 d"))
	tk.MustExec("insert into t2 (id, a, name) values (11, 3, 'b') on duplicate key update id = 1, name = 'f'")
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 3 31 f"))
	tk.MustGetDBError("insert into t2 (id, a, name) values (1, 3, 'b') on duplicate key update a = 10", plannercore.ErrNoReferencedRow2)
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 3 31 f"))

	// Test In txn.
	tk.MustExec("delete from t2")
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 (id, a, b) values       (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)")
	tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 21, 'a')")
	tk.MustExec("begin")
	tk.MustExec("insert into t2 (id, a) values (11, 1) on duplicate key update a = 2, name = 'b'")
	tk.MustExec("rollback")

	tk.MustExec("begin")
	tk.MustExec("delete from t1 where id=2")
	tk.MustGetDBError("insert into t2 (id, a) values (1, 1) on duplicate key update a = 2, name = 'b'", plannercore.ErrNoReferencedRow2)
	tk.MustExec("insert into t2 (id, a) values (1, 1) on duplicate key update a = 3, name = 'c'")
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 3 21 c"))
	tk.MustExec("insert into t1 (id, a, b) values (5, 15, 25)")
	tk.MustExec("insert into t2 (id, a) values (3, 3) on duplicate key update a = 5, name = 'd'")
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 5 21 d"))
	tk.MustExec("delete from t1 where id=1")
	tk.MustGetDBError("insert into t2 (id, a) values (1, 5) on duplicate key update a = 1, name = 'e'", plannercore.ErrNoReferencedRow2)
	tk.MustExec("commit")
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 5 21 d"))
}

func TestForeignKeyCheckAndLock(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("set @@foreign_key_checks=1")
	tk2.MustExec("use test")

	cases := []struct {
		prepareSQLs []string
	}{
		// Case-1: test unique index only contain foreign key columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int, name varchar(10), unique index (id))",
				"create table t2 (a int,  name varchar(10), unique index (a), foreign key fk(a) references t1(id))",
			},
		},
		//Case-2: test unique index contain foreign key columns and other columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int, name varchar(10), unique index (id, name))",
				"create table t2 (name varchar(10), a int,  unique index (a,  name), foreign key fk(a) references t1(id))",
			},
		},
		//Case-3: test non-unique index only contain foreign key columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int, name varchar(10), index (id))",
				"create table t2 (a int,  name varchar(10), index (a), foreign key fk(a) references t1(id))",
			},
		},
		//Case-4: test non-unique index contain foreign key columns and other columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int, name varchar(10), index (id, name))",
				"create table t2 (name varchar(10), a int,  index (a,  name), foreign key fk(a) references t1(id))",
			},
		},
		//Case-5: test primary key only contain foreign key columns, and disable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=0;",
				"create table t1 (id int, name varchar(10), primary key (id))",
				"create table t2 (a int,  name varchar(10), primary key (a), foreign key fk(a) references t1(id))",
			},
		},
		//Case-6: test primary key only contain foreign key columns, and enable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=1;",
				"create table t1 (id int, name varchar(10), primary key (id))",
				"create table t2 (a int,  name varchar(10), primary key (a), foreign key fk(a) references t1(id))",
			},
		},
		//Case-7: test primary key contain foreign key columns and other column, and disable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=0;",
				"create table t1 (id int, name varchar(10), primary key (id, name))",
				"create table t2 (a int,  name varchar(10), primary key (a , name), foreign key fk(a) references t1(id))",
			},
		},
		// Case-8: test primary key contain foreign key columns and other column, and enable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=1;",
				"create table t1 (id int, name varchar(10), primary key (id, name))",
				"create table t2 (a int,  name varchar(10), primary key (a , name), foreign key fk(a) references t1(id))",
			},
		},
	}

	for _, ca := range cases {
		tk.MustExec("drop table if exists t2;")
		tk.MustExec("drop table if exists t1;")
		for _, sql := range ca.prepareSQLs {
			tk.MustExec(sql)
		}
		// Test in optimistic txn
		tk.MustExec("insert into t1 (id, name) values (1, 'a');")
		// Test insert child table
		tk.MustExec("begin optimistic")
		tk.MustExec("insert into t2 (a, name) values (1, 'a');")
		tk2.MustExec("delete from t1 where id = 1")
		err := tk.ExecToErr("commit")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Write conflict")
	}
}

func TestForeignKeyOnInsertIgnore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	tk.MustExec("CREATE TABLE t1 (i INT PRIMARY KEY);")
	tk.MustExec("CREATE TABLE t2 (i INT, FOREIGN KEY (i) REFERENCES t1 (i));")
	tk.MustExec("INSERT INTO t1 VALUES (1),(3);")
	tk.MustExec("INSERT IGNORE INTO t2 VALUES (1),(2),(3),(4);")
	warning := "Warning 1452 Cannot add or update a child row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `fk_1` FOREIGN KEY (`i`) REFERENCES `t1` (`i`))"
	tk.MustQuery("show warnings;").Check(testkit.Rows(warning, warning))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1", "3"))
}

func TestForeignKeyOnInsertOnDuplicateParentTableCheck(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	for _, ca := range foreignKeyTestCase1 {
		tk.MustExec("drop table if exists t2;")
		tk.MustExec("drop table if exists t1;")
		for _, sql := range ca.prepareSQLs {
			tk.MustExec(sql)
		}
		if !ca.notNull {
			tk.MustExec("insert into t1 (id, a, b) values (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24), (5, 15, null), (6, null, 26), (7, null, null);")
			tk.MustExec("insert into t2 (id, a, b, name) values (1, 11, 21, 'a'), (5, 15, null, 'e'), (6, null, 26, 'f'), (7, null, null, 'g');")

			tk.MustExec("insert into t1 (id, a) values (2, 12) on duplicate key update a=a+100, b=b+200")
			tk.MustExec("insert into t1 (id, a) values (3, 13), (2, 12) on duplicate key update a=a+1000, b=b+2000")
			tk.MustExec("insert into t1 (id) values (5), (6), (7) on duplicate key update a=a+10000, b=b+20000")
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 11 21", "2 1112 2222", "3 1013 2023", "4 14 24", "5 10015 <nil>", "6 <nil> 20026", "7 <nil> <nil>"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 11 21 a", "5 15 <nil> e", "6 <nil> 26 f", "7 <nil> <nil> g"))

			tk.MustGetDBError("insert into t1 (id, a) values (1, 11) on duplicate key update a=a+10, b=b+20", plannercore.ErrRowIsReferenced2)
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 11 21", "2 1112 2222", "3 1013 2023", "4 14 24", "5 10015 <nil>", "6 <nil> 20026", "7 <nil> <nil>"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 11 21 a", "5 15 <nil> e", "6 <nil> 26 f", "7 <nil> <nil> g"))
		} else {
			tk.MustExec("insert into t1 (id, a, b) values (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)")
			tk.MustExec("insert into t2 (id, a, b, name) values (1, 11, 21, 'a');")

			tk.MustExec("insert into t1 (id, a, b) values (2, 12, 22) on duplicate key update a=a+100, b=b+200")
			tk.MustExec("insert into t1 (id, a, b) values (3, 13, 23), (2, 12, 22) on duplicate key update a=a+1000, b=b+2000")
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 11 21", "2 1112 2222", "3 1013 2023", "4 14 24"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 11 21 a"))

			tk.MustExec("insert into t1 (id, a, b) values (1, 11, 21) on duplicate key update id=11")
			tk.MustGetDBError("insert into t1 (id, a, b) values (1, 11, 21) on duplicate key update a=a+10, b=b+20", plannercore.ErrRowIsReferenced2)
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("2 1112 2222", "3 1013 2023", "4 14 24", "11 11 21"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 11 21 a"))
		}
	}

	// Case-9: test primary key is handle and contain foreign key column.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("set @@tidb_enable_clustered_index=0;")
	tk.MustExec("create table t1 (id int, a int, b int,  primary key (id));")
	tk.MustExec("create table t2 (b int,  a int, id int, name varchar(10), primary key (a), foreign key fk(a) references t1(id));")
	tk.MustExec("insert into t1 (id, a, b) values       (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)")
	tk.MustExec("insert into t2 (id, a, b, name) values (11, 1, 21, 'a')")

	tk.MustExec("insert into t1 (id, a, b) values (2, 0, 0), (3, 0, 0) on duplicate key update id=id+100")
	tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 11 21", "4 14 24", "102 12 22", "103 13 23"))

	tk.MustExec("insert into t1 (id, a, b) values (1, 0, 0) on duplicate key update a=a+100")
	tk.MustGetDBError("insert into t1 (id, a, b) values (1, 0, 0) on duplicate key update id=100+id", plannercore.ErrRowIsReferenced2)
	tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 111 21", "4 14 24", "102 12 22", "103 13 23"))
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("11 1 21 a"))
}

func TestForeignKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	// Test table has more than 1 foreign keys.
	tk.MustExec("create table t1 (id int, a int, b int,  primary key (id));")
	tk.MustExec("create table t2 (id int, a int, b int,  primary key (id));")
	tk.MustExec("create table t3 (b int,  a int, id int, primary key (a), foreign key (a) references t1(id),  foreign key (b) references t2(id));")
	tk.MustExec("insert into t1 (id, a, b) values (1, 11, 111), (2, 22, 222);")
	tk.MustExec("insert into t2 (id, a, b) values (2, 22, 222);")
	tk.MustGetDBError("insert into t3 (id, a, b) values (1, 1, 1)", plannercore.ErrNoReferencedRow2)
	tk.MustGetDBError("insert into t3 (id, a, b) values (2, 3, 2)", plannercore.ErrNoReferencedRow2)
}

func TestForeignKeyConcurrentInsertChildTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int, a int, primary key (id));")
	tk.MustExec("create table t2 (id int, a int, index(a),  foreign key fk(a) references t1(id));")
	tk.MustExec("insert into  t1 (id, a) values (1, 11),(2, 12), (3, 13), (4, 14)")
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("set @@global.tidb_enable_foreign_key=1")
			tk.MustExec("set @@foreign_key_checks=1")
			tk.MustExec("use test")
			for cnt := 0; cnt < 20; cnt++ {
				id := cnt%4 + 1
				sql := fmt.Sprintf("insert into t2 (id, a) values (%v, %v)", cnt, id)
				tk.MustExec(sql)
			}
		}()
	}
	wg.Wait()
}

func TestForeignKeyOnDeleteParentTableCheck(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	for _, ca := range foreignKeyTestCase1 {
		tk.MustExec("drop table if exists t2;")
		tk.MustExec("drop table if exists t1;")
		for _, sql := range ca.prepareSQLs {
			tk.MustExec(sql)
		}
		if !ca.notNull {
			tk.MustExec("insert into t1 (id, a, b) values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, null), (6, null, 6), (7, null, null);")
			tk.MustExec("insert into t2 (id, a, b) values (1, 1, 1), (5, 5, null), (6, null, 6), (7, null, null);;")

			tk.MustExec("delete from t1 where id = 2")
			tk.MustExec("delete from t1 where a = 3 or b = 4")
			tk.MustExec("delete from t1 where a = 5 or b = 6 or a is null or b is null;")
			tk.MustGetDBError("delete from t1 where id = 1", plannercore.ErrRowIsReferenced2)
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 1 1"))
		} else {
			tk.MustExec("insert into t1 (id, a, b) values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4);")
			tk.MustExec("insert into t2 (id, a, b) values (1, 1, 1);")

			tk.MustExec("delete from t1 where id = 2")
			tk.MustExec("delete from t1 where a = 3 or b = 4")
			tk.MustGetDBError("delete from t1 where id = 1", plannercore.ErrRowIsReferenced2)
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 1 1"))
		}
	}

	// Case-9: test primary key is handle and contain foreign key column.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (id int,a int, primary key(id));")
	tk.MustExec("create table t2 (id int,a int, primary key(a), foreign key fk(a) references t1(id));")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4);")
	tk.MustExec("insert into t2 values (1, 1);")
	tk.MustExec("delete from t1 where id = 2;")
	tk.MustExec("delete from t1 where a = 3 or a = 4;")
	tk.MustGetDBError("delete from t1 where id = 1", plannercore.ErrRowIsReferenced2)
	tk.MustQuery("select id, a from t1 order by id").Check(testkit.Rows("1 1"))
}
