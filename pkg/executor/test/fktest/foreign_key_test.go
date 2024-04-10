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

package fk_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/tests/realtikvtest"
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
		tk.MustGetDBError("insert into t2 (id, a, b) values (5, 1, 0);", plannererrors.ErrNoReferencedRow2)
		tk.MustGetDBError("insert into t2 (id, a, b) values (6, 0, 1);", plannererrors.ErrNoReferencedRow2)
		tk.MustGetDBError("insert into t2 (id, a, b) values (7, 2, 2);", plannererrors.ErrNoReferencedRow2)
		// Test insert from select.
		tk.MustExec("delete from t2")
		tk.MustExec("insert into t2 (id, a, b) select id, a, b from t_data where t_data.id=1")
		tk.MustGetDBError("insert into t2 (id, a, b) select id, a, b from t_data where t_data.id=2", plannererrors.ErrNoReferencedRow2)

		// Test in txn
		tk.MustExec("delete from t2")
		tk.MustExec("begin")
		tk.MustExec("delete from t1 where a=1")
		tk.MustGetDBError("insert into t2 (id, a, b) values (1, 1, 1)", plannererrors.ErrNoReferencedRow2)
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
	tk.MustGetDBError("insert into t2 (id) values (10);", plannererrors.ErrNoReferencedRow2)
	tk.MustGetDBError("insert into t2 values (3, 2);", plannererrors.ErrNoReferencedRow2)

	// Case-11: test primary key is handle and contain foreign key column, and foreign key column doesn't have default value.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (id int key,a int, index (a), foreign key fk(a) references t1(id));")
	tk.MustExec("insert into t2 values (1, 1);")
	tk.MustExec("insert into t2 (id) values (10);")
	tk.MustGetDBError("insert into t2 values (3, 2);", plannererrors.ErrNoReferencedRow2)
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
			tk.MustGetDBError(sqlStr, plannererrors.ErrNoReferencedRow2)
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
		tk.MustGetDBError("insert into t2 (id, a, b, name) values (2, 13, 23, 'y') on duplicate key update a = 13, b = 23, name = 'y'", plannererrors.ErrNoReferencedRow2)
		tk.MustExec("insert into t2 (id, a, b, name) values (2, 14, 24, 'z') on duplicate key update a = 14, b = 24, name = 'z'")
		tk.MustExec("insert into t1 (id, a, b) values (5, 15, 25)")
		tk.MustExec("insert into t2 (id, a, b, name) values (2, 15, 25, 'o') on duplicate key update a = 15, b = 25, name = 'o'")
		tk.MustExec("delete from t1 where id=1")
		tk.MustGetDBError("insert into t2 (id, a, b, name) values (2, 11, 21, 'y') on duplicate key update a = 11, b = 21, name = 'p'", plannererrors.ErrNoReferencedRow2)
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
	tk.MustGetDBError("insert into t2 (id, a, name) values (1, 3, 'b') on duplicate key update a = 10", plannererrors.ErrNoReferencedRow2)
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
	tk.MustGetDBError("insert into t2 (id, a) values (1, 1) on duplicate key update a = 2, name = 'b'", plannererrors.ErrNoReferencedRow2)
	tk.MustExec("insert into t2 (id, a) values (1, 1) on duplicate key update a = 3, name = 'c'")
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 3 21 c"))
	tk.MustExec("insert into t1 (id, a, b) values (5, 15, 25)")
	tk.MustExec("insert into t2 (id, a) values (3, 3) on duplicate key update a = 5, name = 'd'")
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 5 21 d"))
	tk.MustExec("delete from t1 where id=1")
	tk.MustGetDBError("insert into t2 (id, a) values (1, 5) on duplicate key update a = 1, name = 'e'", plannererrors.ErrNoReferencedRow2)
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

	if !*realtikvtest.WithRealTiKV {
		// Unistore doesn't write lock records on secondary keys with value unchanged, causing it incorrectly ignores
		// conflicts between transactions on these kinds of keys. This may make the test fail if fair locking is
		// enabled. So disable it if it's not running with real tikv.
		tk.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
		tk2.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
	}

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
		// Test delete in optimistic txn
		tk.MustExec("insert into t1 (id, name) values (1, 'a');")
		// Test insert child table
		tk.MustExec("begin optimistic")
		tk.MustExec("insert into t2 (a, name) values (1, 'a');")
		tk2.MustExec("delete from t1 where id = 1")
		err := tk.ExecToErr("commit")
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "Write conflict")
		tk.MustQuery("select id, name from t1 order by name").Check(testkit.Rows())
		tk.MustQuery("select a,  name from t2 order by name").Check(testkit.Rows())

		// Test update in optimistic txn
		tk.MustExec("insert into t1 (id, name) values (1, 'a');")
		tk.MustExec("begin optimistic")
		tk.MustExec("insert into t2 (a, name) values (1, 'a');")
		tk2.MustExec("update t1 set id=2 where id = 1")
		err = tk.ExecToErr("commit")
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "Write conflict")
		tk.MustQuery("select id, name from t1 order by name").Check(testkit.Rows("2 a"))
		tk.MustQuery("select a,  name from t2 order by name").Check(testkit.Rows())

		// Test update child table
		tk.MustExec("delete from t1")
		tk.MustExec("delete from t2")
		tk.MustExec("insert into t1 (id, name) values (1, 'a'), (2, 'b');")
		tk.MustExec("insert into t2 (a, name) values (1, 'a');")
		tk.MustExec("begin optimistic")
		tk.MustExec("update t2 set a=2 where a = 1")
		tk2.MustExec("delete from t1 where id = 2")
		err = tk.ExecToErr("commit")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Write conflict")
		tk.MustQuery("select id, name from t1 order by name").Check(testkit.Rows("1 a"))
		tk.MustQuery("select a,  name from t2 order by name").Check(testkit.Rows("1 a"))

		// Test in pessimistic txn
		tk.MustExec("delete from t2")
		// Test insert child table
		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into t2 (a, name) values (1, 'a');")
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk2.MustExec("begin pessimistic")
			err := tk2.ExecToErr("update t1 set id = 2 where id = 1")
			require.NotNil(t, err)
			require.Equal(t, "[planner:1451]Cannot delete or update a parent row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `fk` FOREIGN KEY (`a`) REFERENCES `t1` (`id`))", err.Error())
			tk2.MustExec("commit")
		}()
		time.Sleep(time.Millisecond * 50)
		tk.MustExec("commit")
		wg.Wait()
		tk.MustQuery("select id, name from t1 order by name").Check(testkit.Rows("1 a"))
		tk.MustQuery("select a,  name from t2 order by name").Check(testkit.Rows("1 a"))

		// Test update child table
		tk.MustExec("insert into t1 (id, name) values (2, 'b');")
		tk.MustExec("begin pessimistic")
		tk.MustExec("update t2 set a=2 where a = 1")
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk2.MustExec("begin pessimistic")
			err := tk2.ExecToErr("update t1 set id = 3 where id = 2")
			require.NotNil(t, err)
			require.Equal(t, "[planner:1451]Cannot delete or update a parent row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `fk` FOREIGN KEY (`a`) REFERENCES `t1` (`id`))", err.Error())
			tk2.MustExec("commit")
		}()
		time.Sleep(time.Millisecond * 50)
		tk.MustExec("commit")
		wg.Wait()
		tk.MustQuery("select id, name from t1 order by name").Check(testkit.Rows("1 a", "2 b"))
		tk.MustQuery("select a,  name from t2 order by name").Check(testkit.Rows("2 a"))

		// Test delete parent table in pessimistic txn
		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into t2 (a, name) values (1, 'a');")
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk2.MustExec("begin pessimistic")
			err := tk2.ExecToErr("delete from t1 where id = 1")
			require.NotNil(t, err)
			require.Equal(t, "[planner:1451]Cannot delete or update a parent row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `fk` FOREIGN KEY (`a`) REFERENCES `t1` (`id`))", err.Error())
			tk2.MustExec("commit")
		}()
		time.Sleep(time.Millisecond * 50)
		tk.MustExec("commit")
		wg.Wait()
		tk.MustQuery("select id, name from t1 order by name").Check(testkit.Rows("1 a", "2 b"))
		tk.MustQuery("select a,  name from t2 order by a").Check(testkit.Rows("1 a", "2 a"))

		tk.MustExec("delete from t2")
		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into t2 (a, name) values (1, 'a');")
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk2.MustExec("begin pessimistic")
			err := tk2.ExecToErr("delete from t1 where id < 5") // Also test the non-fast path
			require.NotNil(t, err)
			require.Equal(t, "[planner:1451]Cannot delete or update a parent row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `fk` FOREIGN KEY (`a`) REFERENCES `t1` (`id`))", err.Error())
			tk2.MustExec("commit")
		}()
		time.Sleep(time.Millisecond * 50)
		tk.MustExec("commit")
		wg.Wait()
		tk.MustQuery("select id, name from t1 order by name").Check(testkit.Rows("1 a", "2 b"))
		tk.MustQuery("select a,  name from t2 order by a").Check(testkit.Rows("1 a"))

		// Test delete parent table in auto-commit txn
		// TODO(crazycs520): fix following test.
		/*
			tk.MustExec("delete from t2")
			tk.MustExec("begin pessimistic")
			tk.MustExec("delete from t2;") // active txn
			tk.MustExec("insert into t2 (a, name) values (1, 'a');")
			wg.Add(1)
			go func() {
				defer wg.Done()
				tk2.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
			}()
			time.Sleep(time.Millisecond * 50)
			tk.MustExec("commit")
			wg.Wait()
		*/
	}
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

			tk.MustGetDBError("insert into t1 (id, a) values (1, 11) on duplicate key update a=a+10, b=b+20", plannererrors.ErrRowIsReferenced2)
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
			tk.MustGetDBError("insert into t1 (id, a, b) values (11, 11, 21) on duplicate key update a=a+10, b=b+20", plannererrors.ErrRowIsReferenced2)
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
	tk.MustGetDBError("insert into t1 (id, a, b) values (1, 0, 0) on duplicate key update id=100+id", plannererrors.ErrRowIsReferenced2)
	tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 111 21", "4 14 24", "102 12 22", "103 13 23"))
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("11 1 21 a"))

	// Case-10: Test insert into parent table failed cause by foreign key check, see https://github.com/pingcap/tidb/issues/39200.
	tk.MustExec("drop table if exists t1,t2;")
	tk.MustExec("create table t1 (id int key);")
	tk.MustExec("create table t2 (id int, foreign key fk(id) references t1(id));")
	tk.MustExec("set @@foreign_key_checks=0")
	tk.MustExec("insert into t2 values (1)")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("insert into t1 values (1) on duplicate key update id=2")
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

func TestForeignKeyOnUpdateChildTable(t *testing.T) {
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
			"update t2 set a=100, b = 200 where id = 1",
			"update t2 set a=a+10, b = b+20 where a = 11",
			"update t2 set a=a+100, b = b+200",
			"update t2 set a=12, b = 23 where id = 1",
		}
		for _, sqlStr := range sqls {
			tk.MustGetDBError(sqlStr, plannererrors.ErrNoReferencedRow2)
		}
		tk.MustExec("update t2 set a=12, b = 22 where id = 1")
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 12 22 a"))
		if !ca.notNull {
			tk.MustExec("update t2 set a=null, b = 22 where a = 12 ")
			tk.MustExec("update t2 set b = null where b = 22 ")
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 <nil> <nil> a"))
		}
		tk.MustExec("update t2 set a=13, b=23 where id = 1")
		tk.MustQuery("select id, a, b, name from t2").Check(testkit.Rows("1 13 23 a"))

		// Test In txn.
		tk.MustExec("delete from t2")
		tk.MustExec("delete from t1")
		tk.MustExec("insert into t1 (id, a, b) values (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 11, 21, 'a')")
		tk.MustExec("begin")
		tk.MustExec("update t2 set a=12, b=22 where id=1")
		tk.MustExec("rollback")

		tk.MustExec("begin")
		tk.MustExec("delete from t1 where id=2")
		tk.MustGetDBError("update t2 set a=12, b=22 where id=1", plannererrors.ErrNoReferencedRow2)
		tk.MustExec("update t2 set a=13, b=23 where id=1")
		tk.MustExec("insert into t1 (id, a, b) values (5, 15, 25)")
		tk.MustExec("update t2 set a=15, b=25 where id=1")
		tk.MustExec("delete from t1 where id=1")
		tk.MustGetDBError("update t2 set a=11, b=21 where id=1", plannererrors.ErrNoReferencedRow2)
		tk.MustExec("commit")
		tk.MustQuery("select id, a, b, name from t2").Check(testkit.Rows("1 15 25 a"))
	}

	// Case-9: test primary key is handle and contain foreign key column.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("set @@tidb_enable_clustered_index=0;")
	tk.MustExec("create table t1 (id int, a int, b int,  primary key (id));")
	tk.MustExec("create table t2 (b int,  a int, id int, name varchar(10), primary key (a), foreign key fk(a) references t1(id));")
	tk.MustExec("insert into t1 (id, a, b) values       (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)")
	tk.MustExec("insert into t2 (id, a, b, name) values (11, 1, 21, 'a')")
	tk.MustExec("update t2 set a = 2 where id = 11")
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("11 2 21 a"))
	tk.MustExec("update t2 set a = 3 where id = 11")
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("11 3 21 a"))
	tk.MustExec("update t2 set b=b+1 where id = 11")
	tk.MustQuery("select id, a, b , name from t2 order by id").Check(testkit.Rows("11 3 22 a"))
	tk.MustExec("update t2 set id = 1 where id = 11")
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 3 22 a"))
	tk.MustGetDBError("update t2 set a = 10 where id = 1", plannererrors.ErrNoReferencedRow2)
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 3 22 a"))

	// Test In txn.
	tk.MustExec("delete from t2")
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 (id, a, b) values       (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)")
	tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 21, 'a')")
	tk.MustExec("begin")
	tk.MustExec("update t2 set a=2, b=22 where id=1")
	tk.MustExec("rollback")

	tk.MustExec("begin")
	tk.MustExec("delete from t1 where id=2")
	tk.MustGetDBError("update t2 set a=2, b=22 where id=1", plannererrors.ErrNoReferencedRow2)
	tk.MustExec("update t2 set a=3, b=23 where id=1")
	tk.MustExec("insert into t1 (id, a, b) values (5, 15, 25)")
	tk.MustExec("update t2 set a=5, b=25 where id=1")
	tk.MustExec("delete from t1 where id=1")
	tk.MustGetDBError("update t2 set a=1, b=21 where id=1", plannererrors.ErrNoReferencedRow2)
	tk.MustExec("commit")
	tk.MustQuery("select id, a, b, name from t2").Check(testkit.Rows("1 5 25 a"))
}

func TestForeignKeyOnUpdateParentTableCheck(t *testing.T) {
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

			tk.MustExec("update t1 set a=a+100, b = b+200 where id = 2")
			tk.MustExec("update t1 set a=a+1000, b = b+2000 where a = 13 or b=222")
			tk.MustExec("update t1 set a=a+10000, b = b+20000 where id = 5 or a is null or b is null")
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 11 21", "2 1112 2222", "3 1013 2023", "4 14 24", "5 10015 <nil>", "6 <nil> 20026", "7 <nil> <nil>"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 11 21 a", "5 15 <nil> e", "6 <nil> 26 f", "7 <nil> <nil> g"))
			tk.MustGetDBError("update t1 set a=a+10, b = b+20 where id = 1 or a = 1112 or b = 24", plannererrors.ErrRowIsReferenced2)
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 11 21", "2 1112 2222", "3 1013 2023", "4 14 24", "5 10015 <nil>", "6 <nil> 20026", "7 <nil> <nil>"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 11 21 a", "5 15 <nil> e", "6 <nil> 26 f", "7 <nil> <nil> g"))
		} else {
			tk.MustExec("insert into t1 (id, a, b) values (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)")
			tk.MustExec("insert into t2 (id, a, b, name) values (1, 11, 21, 'a');")
			tk.MustExec("update t1 set a=a+100, b = b+200 where id = 2")
			tk.MustExec("update t1 set a=a+1000, b = b+2000 where a = 13 or b=222")
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 11 21", "2 1112 2222", "3 1013 2023", "4 14 24"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 11 21 a"))
			tk.MustGetDBError("update t1 set a=a+10, b = b+20 where id = 1 or a = 1112 or b = 24", plannererrors.ErrRowIsReferenced2)
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 11 21", "2 1112 2222", "3 1013 2023", "4 14 24"))
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
	tk.MustExec("update t1 set id = id + 100 where id =2 or a = 13")
	tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 11 21", "4 14 24", "102 12 22", "103 13 23"))
	tk.MustGetDBError("update t1 set id = id+10 where id = 1 or b = 24", plannererrors.ErrRowIsReferenced2)
	tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 11 21", "4 14 24", "102 12 22", "103 13 23"))
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("11 1 21 a"))
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
			tk.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 1 1"))
		} else {
			tk.MustExec("insert into t1 (id, a, b) values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4);")
			tk.MustExec("insert into t2 (id, a, b) values (1, 1, 1);")

			tk.MustExec("delete from t1 where id = 2")
			tk.MustExec("delete from t1 where a = 3 or b = 4")
			tk.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 1 1"))
		}
		models := []string{"pessimistic", "optimistic"}
		for _, model := range models {
			// Test in transaction.
			tk.MustExec("delete from t2")
			tk.MustExec("delete from t1")
			tk.MustExec("begin " + model)
			tk.MustExec("insert into t1 (id, a, b) values (1, 1, 1), (2, 2, 2);")
			tk.MustExec("insert into t2 (id, a, b) values (1, 1, 1);")
			tk.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
			tk.MustExec("delete from t1 where id = 2")
			tk.MustExec("delete from t2 where id = 1")
			tk.MustExec("delete from t1 where id = 1")
			tk.MustExec("commit")
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows())
			tk.MustQuery("select id, a, b from t2 order by id").Check(testkit.Rows())
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
	tk.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
	tk.MustQuery("select id, a from t1 order by id").Check(testkit.Rows("1 1"))
}

func TestForeignKeyOnDeleteCascade(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")
	cases := []struct {
		prepareSQLs []string
	}{
		// Case-1: test unique index only contain foreign key columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int, a int, b int,  unique index(a, b));",
				"create table t2 (b int, name varchar(10), a int, id int, unique index (a,b), foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);",
			},
		},
		// Case-2: test unique index contain foreign key columns and other columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int key, a int, b int, unique index(a, b, id));",
				"create table t2 (b int, a int, id int key, name varchar(10), unique index (a,b, id), foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);",
			},
		},
		// Case-3: test non-unique index only contain foreign key columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int key,a int, b int, index(a, b));",
				"create table t2 (b int, a int, name varchar(10), id int key, index (a, b), foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);",
			},
		},
		// Case-4: test non-unique index contain foreign key columns and other columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int key,a int, b int,  index(a, b, id));",
				"create table t2 (name varchar(10), b int, a int, id int key, index (a, b, id), foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);",
			},
		},
	}

	for idx, ca := range cases {
		tk.MustExec("drop table if exists t1, t2;")
		for _, sql := range ca.prepareSQLs {
			tk.MustExec(sql)
		}
		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, null), (6, null, 6), (7, null, null);")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b'), (3, 3, 3, 'c'), (4, 4, 4, 'd'), (5, 5, null, 'e'), (6, null, 6, 'f'), (7, null, null, 'g');")
		tk.MustExec("delete from t1 where id = 1")
		tk.MustExec("delete from t1 where id = 2 or a = 2")
		tk.MustExec("delete from t1 where a in (2,3,4) or b in (5,6,7) or id=7")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("5 5 <nil>"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("5 5 <nil> e", "6 <nil> 6 f", "7 <nil> <nil> g"))

		// Test in transaction.
		tk.MustExec("delete from t2")
		tk.MustExec("delete from t1")
		tk.MustExec("begin")
		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, null), (6, null, 6), (7, null, null);")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b'), (3, 3, 3, 'c'), (4, 4, 4, 'd'), (5, 5, null, 'e'), (6, null, 6, 'f'), (7, null, null, 'g');")
		tk.MustExec("delete from t1 where id = 1 or a = 2")
		tk.MustExec("delete from t1 where a in (2,3,4) or b in (5,6,7)")
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("5 5 <nil> e", "6 <nil> 6 f", "7 <nil> <nil> g"))
		tk.MustExec("rollback")
		tk.MustQuery("select * from t1").Check(testkit.Rows())
		tk.MustQuery("select * from t2").Check(testkit.Rows())

		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2);")
		tk.MustExec("begin")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b')")
		tk.MustExec("delete from t1 where id = 1")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("2 2 2"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("2 2 2 b"))
		err := tk.ExecToErr("insert into t2 (id, a, b, name) values (1, 1, 1, 'a')")
		require.Error(t, err)
		require.True(t, plannererrors.ErrNoReferencedRow2.Equal(err), err.Error())
		tk.MustExec("insert into t1 values (1, 1, 1);")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'c')")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 1 1", "2 2 2"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 1 1 c", "2 2 2 b"))
		tk.MustExec("delete from t1")
		tk.MustExec("commit")
		tk.MustQuery("select * from t1").Check(testkit.Rows())
		tk.MustQuery("select * from t2").Check(testkit.Rows())

		// only test in non-unique index
		if idx >= 2 {
			tk.MustExec("insert into t1 values (1, 1, 1),(2, 1, 1);")
			tk.MustExec("begin")
			tk.MustExec("delete from t1 where id = 1")
			tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a')")
			tk.MustExec("delete from t1 where id = 2")
			tk.MustQuery("select * from t1").Check(testkit.Rows())
			tk.MustQuery("select * from t2").Check(testkit.Rows())
			err := tk.ExecToErr("insert into t2 (id, a, b, name) values (1, 1, 1, 'a')")
			require.Error(t, err)
			require.True(t, plannererrors.ErrNoReferencedRow2.Equal(err), err.Error())
			tk.MustExec("insert into t1 values (3, 1, 1);")
			tk.MustExec("insert into t2 (id, a, b, name) values (3, 1, 1, 'e')")
			tk.MustExec("commit")
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("3 1 1"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("3 1 1 e"))

			tk.MustExec("delete from t2")
			tk.MustExec("delete from t1")
			tk.MustExec("begin")
			tk.MustExec("insert into t1 values (1, 1, 1),(2, 1, 1);")
			tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'), (2, 1, 1, 'b')")
			tk.MustExec("delete from t1 where id = 1")
			tk.MustExec("commit")
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("2 1 1"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows())
		}
	}

	cases = []struct {
		prepareSQLs []string
	}{
		// Case-5: test primary key only contain foreign key columns, and disable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=0;",
				"create table t1 (id int, a int, b int,  primary key (a, b));",
				"create table t2 (b int, name varchar(10),  a int, id int, primary key (a, b), foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);",
			},
		},
		// Case-6: test primary key only contain foreign key columns, and enable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=1;",
				"create table t1 (id int, a int, b int,  primary key (a, b));",
				"create table t2 (name varchar(10), b int,  a int, id int, primary key (a, b), foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);",
			},
		},
		// Case-7: test primary key contain foreign key columns and other column, and disable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=0;",
				"create table t1 (id int, a int, b int,  primary key (a, b, id));",
				"create table t2 (b int,  a int, name varchar(10), id int, primary key (a, b, id), foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);",
			},
		},
		// Case-8: test primary key contain foreign key columns and other column, and enable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=1;",
				"create table t1 (id int, a int, b int,  primary key (a, b, id));",
				"create table t2 (b int, name varchar(10),  a int, id int, primary key (a, b, id), foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);",
			},
		},
		// Case-9: test primary key is handle and contain foreign key column.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=0;",
				"create table t1 (id int, a int, b int,  primary key (id));",
				"create table t2 (b int,  a int, id int, name varchar(10), primary key (a), foreign key fk(a) references t1(id) ON DELETE CASCADE);",
			},
		},
	}
	for _, ca := range cases {
		tk.MustExec("drop table if exists t1, t2;")
		for _, sql := range ca.prepareSQLs {
			tk.MustExec(sql)
		}
		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2), (3, 3, 3), (4, 4, 4);")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b'), (3, 3, 3, 'c'), (4, 4, 4, 'd');")
		tk.MustExec("delete from t1 where id = 1 or a = 2")
		tk.MustQuery("select id, a, b from t2 order by id").Check(testkit.Rows("3 3 3", "4 4 4"))
		tk.MustExec("delete from t1 where a in (2,3) or b < 5")
		tk.MustQuery("select * from t1").Check(testkit.Rows())
		tk.MustQuery("select * from t2").Check(testkit.Rows())

		// test in transaction.
		tk.MustExec("begin")
		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2), (3, 3, 3), (4, 4, 4);")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b'), (3, 3, 3, 'c'), (4, 4, 4, 'd');")
		tk.MustExec("delete from t1 where id = 1 or a = 2")
		tk.MustExec("delete from t1 where a in (2,3,4) or b in (5,6,7)")
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows())
		tk.MustExec("rollback")
		tk.MustQuery("select * from t1").Check(testkit.Rows())
		tk.MustQuery("select * from t2").Check(testkit.Rows())

		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2);")
		tk.MustExec("begin")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b')")
		tk.MustExec("delete from t1 where id = 1")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("2 2 2"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("2 2 2 b"))
		err := tk.ExecToErr("insert into t2 (id, a, b, name) values (1, 1, 1, 'a')")
		require.Error(t, err)
		require.True(t, plannererrors.ErrNoReferencedRow2.Equal(err), err.Error())
		tk.MustExec("insert into t1 values (1, 1, 1);")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'c')")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 1 1", "2 2 2"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 1 1 c", "2 2 2 b"))
		tk.MustExec("delete from t1")
		tk.MustExec("commit")
		tk.MustQuery("select * from t1").Check(testkit.Rows())
		tk.MustQuery("select * from t2").Check(testkit.Rows())
	}
}

func TestForeignKeyOnDeleteCascade2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	// Test cascade delete in self table.
	tk.MustExec("create table t1 (id int key, name varchar(10), leader int,  index(leader), foreign key (leader) references t1(id) ON DELETE CASCADE);")
	tk.MustExec("insert into t1 values (1, 'boss', null), (10, 'l1_a', 1), (11, 'l1_b', 1), (12, 'l1_c', 1)")
	tk.MustExec("insert into t1 values (100, 'l2_a1', 10), (101, 'l2_a2', 10), (102, 'l2_a3', 10)")
	tk.MustExec("insert into t1 values (110, 'l2_b1', 11), (111, 'l2_b2', 11), (112, 'l2_b3', 11)")
	tk.MustExec("insert into t1 values (120, 'l2_c1', 12), (121, 'l2_c2', 12), (122, 'l2_c3', 12)")
	tk.MustExec("insert into t1 values (1000,'l3_a1', 100)")
	tk.MustExec("delete from t1 where id=11")
	tk.MustQuery("select id from t1 order by id").Check(testkit.Rows("1", "10", "12", "100", "101", "102", "120", "121", "122", "1000"))
	tk.MustExec("delete from t1 where id=1")
	// The affect rows doesn't contain the cascade deleted rows, the behavior is compatible with MySQL.
	require.Equal(t, uint64(1), tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	tk.MustQuery("select id from t1 order by id").Check(testkit.Rows())

	// Test explain analyze with foreign key cascade.
	tk.MustExec("insert into t1 values (1, 'boss', null), (10, 'l1_a', 1), (11, 'l1_b', 1), (12, 'l1_c', 1)")
	tk.MustExec("explain analyze delete from t1 where id=1")
	tk.MustQuery("select * from t1").Check(testkit.Rows())

	// Test string type foreign key.
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (id varchar(10) key, name varchar(10), leader varchar(10),  index(leader), foreign key (leader) references t1(id) ON DELETE CASCADE);")
	tk.MustExec("insert into t1 values (1, 'boss', null)")
	tk.MustExec("insert into t1 values (10, 'l1_a', 1), (11, 'l1_b', 1), (12, 'l1_c', 1)")
	tk.MustExec("insert into t1 values (100, 'l2_a1', 10), (101, 'l2_a2', 10), (102, 'l2_a3', 10)")
	tk.MustExec("insert into t1 values (110, 'l2_b1', 11), (111, 'l2_b2', 11), (112, 'l2_b3', 11)")
	tk.MustExec("insert into t1 values (120, 'l2_c1', 12), (121, 'l2_c2', 12), (122, 'l2_c3', 12)")
	tk.MustExec("insert into t1 values (1000,'l3_a1', 100)")
	tk.MustExec("delete from t1 where id=11")
	tk.MustQuery("select id from t1 order by id").Check(testkit.Rows("1", "10", "100", "1000", "101", "102", "12", "120", "121", "122"))
	tk.MustExec("delete from t1 where id=1")
	require.Equal(t, uint64(1), tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	tk.MustQuery("select id from t1 order by id").Check(testkit.Rows())

	// Test cascade delete depth.
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1(id int primary key, pid int, index(pid), foreign key(pid) references t1(id) on delete cascade);")
	tk.MustExec("insert into t1 values(0,0),(1,0),(2,1),(3,2),(4,3),(5,4),(6,5),(7,6),(8,7),(9,8),(10,9),(11,10),(12,11),(13,12),(14,13),(15,14);")
	tk.MustGetDBError("delete from t1 where id=0;", exeerrors.ErrForeignKeyCascadeDepthExceeded)
	tk.MustExec("delete from t1 where id=15;")
	tk.MustExec("delete from t1 where id=0;")
	tk.MustQuery("select * from t1").Check(testkit.Rows())
	tk.MustExec("insert into t1 values(0,0)")
	tk.MustExec("delete from t1 where id=0;")
	tk.MustQuery("select * from t1").Check(testkit.Rows())

	// Test for cascade delete failed.
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (id int key)")
	tk.MustExec("create table t2 (id int key, foreign key (id) references t1 (id) on delete cascade)")
	tk.MustExec("create table t3 (id int key, foreign key (id) references t2(id))")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	tk.MustExec("insert into t3 values (1)")
	// test in autocommit transaction
	tk.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("1"))
	// Test in transaction and commit transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (2),(3),(4)")
	tk.MustExec("insert into t2 values (2),(3)")
	tk.MustExec("insert into t3 values (3)")
	tk.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustExec("delete from t1 where id = 2")
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustQuery("select * from t1").Check(testkit.Rows("1", "3", "4"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1", "3"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("1", "3"))
	tk.MustExec("commit")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1", "3", "4"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1", "3"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("1", "3"))
	// Test in transaction and rollback transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (5), (6)")
	tk.MustExec("insert into t2 values (4), (5), (6)")
	tk.MustExec("insert into t3 values (5)")
	tk.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustExec("delete from t1 where id = 4")
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustQuery("select * from t1").Check(testkit.Rows("1", "3", "5", "6"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1", "3", "5", "6"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("1", "3", "5"))
	tk.MustExec("rollback")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1", "3", "4"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1", "3"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("1", "3"))
	tk.MustExec("delete from t3 where id = 1")
	tk.MustExec("delete from t1 where id = 1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("3", "4"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("3"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("3"))
	// Test in autocommit=0 transaction
	tk.MustExec("set autocommit=0")
	tk.MustExec("insert into t1 values (1), (2)")
	tk.MustExec("insert into t2 values (1), (2)")
	tk.MustExec("insert into t3 values (1)")
	tk.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustExec("delete from t1 where id = 2")
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustQuery("select * from t1").Check(testkit.Rows("1", "3", "4"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1", "3"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("1", "3"))
	tk.MustExec("set autocommit=1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1", "3", "4"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1", "3"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("1", "3"))

	// Test StmtCommit after fk cascade executor execute finish.
	tk.MustExec("drop table if exists t1,t2,t3")
	tk.MustExec("create table t0(id int primary key);")
	tk.MustExec("create table t1(id int primary key, pid int, index(pid), a int, foreign key(pid) references t1(id) on delete cascade, foreign key(a) references t0(id) on delete cascade);")
	tk.MustExec("insert into t0 values (0)")
	tk.MustExec("insert into t1 values (0, 0, 0)")
	tk.MustExec("insert into t1 (id, pid) values(1,0),(2,1),(3,2),(4,3),(5,4),(6,5),(7,6),(8,7),(9,8),(10,9),(11,10),(12,11),(13,12),(14,13);")
	tk.MustGetDBError("delete from t0 where id=0;", exeerrors.ErrForeignKeyCascadeDepthExceeded)
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustExec("delete from t1 where id=14;")
	tk.MustExec("delete from t0 where id=0;")
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustQuery("select * from t0").Check(testkit.Rows())
	tk.MustQuery("select * from t1").Check(testkit.Rows())

	// Test multi-foreign key cascade in one table.
	tk.MustExec("drop table if exists t1,t2,t3")
	tk.MustExec("create table t1 (id int key)")
	tk.MustExec("create table t2 (id int key)")
	tk.MustExec("create table t3 (id1 int, id2 int, constraint fk_id1 foreign key (id1) references t1 (id) on delete cascade, " +
		"constraint fk_id2 foreign key (id2) references t2 (id) on delete cascade)")
	tk.MustExec("insert into t1 values (1), (2), (3)")
	tk.MustExec("insert into t2 values (1), (2), (3)")
	tk.MustExec("insert into t3 values (1,1), (1, 2), (1, 3), (2, 1), (2, 2)")
	tk.MustExec("delete from t1 where id=1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select * from t3 order by id1").Check(testkit.Rows("2 1", "2 2"))
	tk.MustExec("create table t4 (id3 int key, constraint fk_id3 foreign key (id3) references t3 (id2))")
	tk.MustExec("insert into t4 values (2)")
	tk.MustGetDBError("delete from t1 where id = 2", plannererrors.ErrRowIsReferenced2)
	tk.MustGetDBError("delete from t2 where id = 2", plannererrors.ErrRowIsReferenced2)
	tk.MustExec("delete from t2 where id=1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select * from t3 order by id1").Check(testkit.Rows("2 2"))

	// Test multi-foreign key cascade in one table.
	tk.MustExec("drop table if exists t1,t2,t3, t4")
	tk.MustExec(`create table t1 (c0 int, index(c0))`)
	cnt := 20
	for i := 1; i < cnt; i++ {
		tk.MustExec(fmt.Sprintf("alter table t1 add column c%v int", i))
		tk.MustExec(fmt.Sprintf("alter table t1 add index idx_%v (c%v) ", i, i))
		tk.MustExec(fmt.Sprintf("alter table t1 add foreign key (c%v) references t1 (c%v) on delete cascade", i, i-1))
	}
	for i := 0; i < cnt; i++ {
		vals := strings.Repeat(strconv.Itoa(i)+",", 20)
		tk.MustExec(fmt.Sprintf("insert into t1 values (%v)", vals[:len(vals)-1]))
	}
	tk.MustExec("delete from t1 where c0 in (0, 1, 2, 3, 4)")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("15"))

	// Test foreign key cascade execution meet lock and do retry.
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk2.MustExec("set @@foreign_key_checks=1")
	tk2.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (id int key, name varchar(10), pid int, index(pid), constraint fk foreign key (pid) references t1 (id) on delete cascade)")
	tk.MustExec("insert into t1 values (1, 'boss', null), (2, 'a', 1), (3, 'b', 1), (4, 'c', '2')")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values (5, 'd', 3)")
	tk2.MustExec("begin pessimistic")
	tk2.MustExec("insert into t1 values (6, 'e', 4)")
	tk2.MustExec("delete from t1 where id=2")
	tk2.MustExec("commit")
	tk.MustExec("delete from t1 where id = 1")
	tk.MustExec("commit")
	tk.MustQuery("select * from t1").Check(testkit.Rows())

	// Test handle many foreign key value in one cascade.
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (id int auto_increment key, b int);")
	tk.MustExec("create table t2 (id int, b int, foreign key fk(id) references t1(id) on delete cascade)")
	tk.MustExec("insert into t1 (b) values (1),(1),(1),(1),(1),(1),(1),(1);")
	for i := 0; i < 12; i++ {
		tk.MustExec("insert into t1 (b) select b from t1")
	}
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("32768"))
	tk.MustExec("insert into t2 select * from t1")
	tk.MustExec("delete from t1")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t2").Check(testkit.Rows("0"))
}

func TestForeignKeyGenerateCascadeAST(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	fkValues := [][]types.Datum{
		{types.NewDatum(1), types.NewDatum("a")},
		{types.NewDatum(2), types.NewDatum("b")},
	}
	cols := []*model.ColumnInfo{
		{ID: 1, Name: model.NewCIStr("a"), FieldType: *types.NewFieldType(mysql.TypeLonglong)},
		{ID: 2, Name: model.NewCIStr("name"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
	}
	restoreFn := func(stmt ast.StmtNode) string {
		var sb strings.Builder
		fctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
		err := stmt.Restore(fctx)
		require.NoError(t, err)
		return sb.String()
	}
	checkStmtFn := func(stmt ast.StmtNode, sql string) {
		exec := tk.Session().GetRestrictedSQLExecutor()
		expectedStmt, err := exec.ParseWithParams(context.Background(), sql)
		require.NoError(t, err)
		require.Equal(t, restoreFn(expectedStmt), restoreFn(stmt))
	}
	var stmt ast.StmtNode
	stmt = executor.GenCascadeDeleteAST(model.NewCIStr("test"), model.NewCIStr("t2"), model.NewCIStr(""), cols, fkValues)
	checkStmtFn(stmt, "delete from test.t2 where (a,name) in ((1,'a'), (2,'b'))")
	stmt = executor.GenCascadeDeleteAST(model.NewCIStr("test"), model.NewCIStr("t2"), model.NewCIStr("idx"), cols, fkValues)
	checkStmtFn(stmt, "delete from test.t2 use index(idx) where (a,name) in ((1,'a'), (2,'b'))")
	stmt = executor.GenCascadeSetNullAST(model.NewCIStr("test"), model.NewCIStr("t2"), model.NewCIStr(""), cols, fkValues)
	checkStmtFn(stmt, "update test.t2 set a = null, name = null where (a,name) in ((1,'a'), (2,'b'))")
	stmt = executor.GenCascadeSetNullAST(model.NewCIStr("test"), model.NewCIStr("t2"), model.NewCIStr("idx"), cols, fkValues)
	checkStmtFn(stmt, "update test.t2 use index(idx) set a = null, name = null where (a,name) in ((1,'a'), (2,'b'))")
	newValue1 := []types.Datum{types.NewDatum(10), types.NewDatum("aa")}
	couple := &executor.UpdatedValuesCouple{
		NewValues:     newValue1,
		OldValuesList: fkValues,
	}
	stmt = executor.GenCascadeUpdateAST(model.NewCIStr("test"), model.NewCIStr("t2"), model.NewCIStr(""), cols, couple)
	checkStmtFn(stmt, "update test.t2 set a = 10, name = 'aa' where (a,name) in ((1,'a'), (2,'b'))")
	stmt = executor.GenCascadeUpdateAST(model.NewCIStr("test"), model.NewCIStr("t2"), model.NewCIStr("idx"), cols, couple)
	checkStmtFn(stmt, "update test.t2 use index(idx) set a = 10, name = 'aa' where (a,name) in ((1,'a'), (2,'b'))")
	// Test for 1 fk column.
	fkValues = [][]types.Datum{{types.NewDatum(1)}, {types.NewDatum(2)}}
	cols = []*model.ColumnInfo{{ID: 1, Name: model.NewCIStr("a"), FieldType: *types.NewFieldType(mysql.TypeLonglong)}}
	stmt = executor.GenCascadeDeleteAST(model.NewCIStr("test"), model.NewCIStr("t2"), model.NewCIStr(""), cols, fkValues)
	checkStmtFn(stmt, "delete from test.t2 where a in (1,2)")
	stmt = executor.GenCascadeDeleteAST(model.NewCIStr("test"), model.NewCIStr("t2"), model.NewCIStr("idx"), cols, fkValues)
	checkStmtFn(stmt, "delete from test.t2 use index(idx) where a in (1,2)")
}

func TestForeignKeyOnDeleteSetNull(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	cases := []struct {
		prepareSQLs []string
	}{
		// Case-1: test unique index only contain foreign key columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int, a int, b int,  unique index(a, b));",
				"create table t2 (b int, name varchar(10), a int, id int, unique index (a,b), foreign key fk(a, b) references t1(a, b) ON DELETE SET NULL);",
			},
		},
		// Case-2: test unique index contain foreign key columns and other columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int key, a int, b int, unique index(a, b, id));",
				"create table t2 (b int, a int, id int key, name varchar(10), unique index (a,b, id), foreign key fk(a, b) references t1(a, b) ON DELETE SET NULL);",
			},
		},
		// Case-3: test non-unique index only contain foreign key columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int key,a int, b int, index(a, b));",
				"create table t2 (b int, a int, name varchar(10), id int key, index (a, b), foreign key fk(a, b) references t1(a, b) ON DELETE SET NULL);",
			},
		},
		// Case-4: test non-unique index contain foreign key columns and other columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int key,a int, b int,  index(a, b, id));",
				"create table t2 (name varchar(10), b int, a int, id int key, index (a, b, id), foreign key fk(a, b) references t1(a, b) ON DELETE SET NULL);",
			},
		},
	}

	for idx, ca := range cases {
		tk.MustExec("drop table if exists t2;")
		tk.MustExec("drop table if exists t1;")
		for _, sql := range ca.prepareSQLs {
			tk.MustExec(sql)
		}
		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, null), (6, null, 6), (7, null, null);")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b'), (3, 3, 3, 'c'), (4, 4, 4, 'd'), (5, 5, null, 'e'), (6, null, 6, 'f'), (7, null, null, 'g');")
		tk.MustExec("delete from t1 where id = 1 or a = 2")
		tk.MustExec("delete from t1 where a in (2,3,4) or b in (5,6,7)")
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 <nil> <nil> a", "2 <nil> <nil> b", "3 <nil> <nil> c", "4 <nil> <nil> d", "5 5 <nil> e", "6 <nil> 6 f", "7 <nil> <nil> g"))

		// Test in transaction.
		tk.MustExec("delete from t2")
		tk.MustExec("delete from t1")
		tk.MustExec("begin")
		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, null), (6, null, 6), (7, null, null);")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b'), (3, 3, 3, 'c'), (4, 4, 4, 'd'), (5, 5, null, 'e'), (6, null, 6, 'f'), (7, null, null, 'g');")
		tk.MustExec("delete from t1 where id = 1 or a = 2")
		tk.MustExec("delete from t1 where a in (2,3,4) or b in (5,6,7)")
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 <nil> <nil> a", "2 <nil> <nil> b", "3 <nil> <nil> c", "4 <nil> <nil> d", "5 5 <nil> e", "6 <nil> 6 f", "7 <nil> <nil> g"))
		tk.MustExec("rollback")
		tk.MustQuery("select * from t1").Check(testkit.Rows())
		tk.MustQuery("select * from t2").Check(testkit.Rows())

		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2);")
		tk.MustExec("begin")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b')")
		tk.MustExec("delete from t1 where id = 1")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("2 2 2"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 <nil> <nil> a", "2 2 2 b"))
		err := tk.ExecToErr("insert into t2 (id, a, b, name) values (11, 1, 1, 'c')")
		require.Error(t, err)
		require.True(t, plannererrors.ErrNoReferencedRow2.Equal(err), err.Error())
		tk.MustExec("insert into t1 values (1, 1, 1);")
		tk.MustExec("insert into t2 (id, a, b, name) values (11, 1, 1, 'c')")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 1 1", "2 2 2"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 <nil> <nil> a", "2 2 2 b", "11 1 1 c"))
		tk.MustExec("delete from t1")
		tk.MustExec("commit")
		tk.MustQuery("select * from t1").Check(testkit.Rows())
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 <nil> <nil> a", "2 <nil> <nil> b", "11 <nil> <nil> c"))

		// only test in non-unique index
		if idx >= 2 {
			tk.MustExec("delete from t2")
			tk.MustExec("insert into t1 values (1, 1, 1),(2, 1, 1);")
			tk.MustExec("begin")
			tk.MustExec("delete from t1 where id = 1")
			tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a')")
			tk.MustExec("delete from t1 where id = 2")
			tk.MustQuery("select * from t1").Check(testkit.Rows())
			tk.MustQuery("select id, a, b, name from t2").Check(testkit.Rows("1 <nil> <nil> a"))
			err := tk.ExecToErr("insert into t2 (id, a, b, name) values (2, 1, 1, 'b')")
			require.Error(t, err)
			require.True(t, plannererrors.ErrNoReferencedRow2.Equal(err), err.Error())
			tk.MustExec("insert into t1 values (3, 1, 1);")
			tk.MustExec("insert into t2 (id, a, b, name) values (3, 1, 1, 'e')")
			tk.MustExec("commit")
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("3 1 1"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 <nil> <nil> a", "3 1 1 e"))

			tk.MustExec("delete from t2")
			tk.MustExec("delete from t1")
			tk.MustExec("begin")
			tk.MustExec("insert into t1 values (1, 1, 1),(2, 1, 1);")
			tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'), (2, 1, 1, 'b')")
			tk.MustExec("delete from t1 where id = 1")
			tk.MustExec("commit")
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("2 1 1"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 <nil> <nil> a", "2 <nil> <nil> b"))
		}
	}
}

func TestForeignKeyOnDeleteSetNull2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	// Test cascade delete in self table.
	tk.MustExec("create table t1 (id int key, name varchar(10), leader int,  index(leader), foreign key (leader) references t1(id) ON DELETE SET NULL);")
	tk.MustExec("insert into t1 values (1, 'boss', null), (10, 'l1_a', 1), (11, 'l1_b', 1), (12, 'l1_c', 1)")
	tk.MustExec("insert into t1 values (100, 'l2_a1', 10), (101, 'l2_a2', 10), (102, 'l2_a3', 10)")
	tk.MustExec("insert into t1 values (110, 'l2_b1', 11), (111, 'l2_b2', 11), (112, 'l2_b3', 11)")
	tk.MustExec("insert into t1 values (120, 'l2_c1', 12), (121, 'l2_c2', 12), (122, 'l2_c3', 12)")
	tk.MustExec("insert into t1 values (1000,'l3_a1', 100)")
	tk.MustExec("delete from t1 where id=11")
	tk.MustQuery("select id, name, leader from t1 order by id").Check(testkit.Rows("1 boss <nil>", "10 l1_a 1", "12 l1_c 1", "100 l2_a1 10", "101 l2_a2 10", "102 l2_a3 10", "110 l2_b1 <nil>", "111 l2_b2 <nil>", "112 l2_b3 <nil>", "120 l2_c1 12", "121 l2_c2 12", "122 l2_c3 12", "1000 l3_a1 100"))
	tk.MustExec("delete from t1 where id=1")
	// The affect rows doesn't contain the cascade deleted rows, the behavior is compatible with MySQL.
	require.Equal(t, uint64(1), tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	tk.MustQuery("select id, name, leader from t1 order by id").Check(testkit.Rows("10 l1_a <nil>", "12 l1_c <nil>", "100 l2_a1 10", "101 l2_a2 10", "102 l2_a3 10", "110 l2_b1 <nil>", "111 l2_b2 <nil>", "112 l2_b3 <nil>", "120 l2_c1 12", "121 l2_c2 12", "122 l2_c3 12", "1000 l3_a1 100"))

	// Test explain analyze with foreign key cascade.
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 values (1, 'boss', null), (10, 'l1_a', 1), (11, 'l1_b', 1), (12, 'l1_c', 1)")
	tk.MustExec("explain analyze delete from t1 where id=1")
	tk.MustQuery("select id, name, leader from t1 order by id").Check(testkit.Rows("10 l1_a <nil>", "11 l1_b <nil>", "12 l1_c <nil>"))

	// Test string type foreign key.
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (id varchar(10) key, name varchar(10), leader varchar(10),  index(leader), foreign key (leader) references t1(id) ON DELETE SET NULL);")
	tk.MustExec("insert into t1 values (1, 'boss', null)")
	tk.MustExec("insert into t1 values (10, 'l1_a', 1), (11, 'l1_b', 1), (12, 'l1_c', 1)")
	tk.MustExec("insert into t1 values (100, 'l2_a1', 10), (101, 'l2_a2', 10), (102, 'l2_a3', 10)")
	tk.MustExec("insert into t1 values (110, 'l2_b1', 11), (111, 'l2_b2', 11), (112, 'l2_b3', 11)")
	tk.MustExec("insert into t1 values (120, 'l2_c1', 12), (121, 'l2_c2', 12), (122, 'l2_c3', 12)")
	tk.MustExec("insert into t1 values (1000,'l3_a1', 100)")
	tk.MustExec("delete from t1 where id=11")
	tk.MustQuery("select id, name, leader from t1 order by name").Check(testkit.Rows("1 boss <nil>", "10 l1_a 1", "12 l1_c 1", "100 l2_a1 10", "101 l2_a2 10", "102 l2_a3 10", "110 l2_b1 <nil>", "111 l2_b2 <nil>", "112 l2_b3 <nil>", "120 l2_c1 12", "121 l2_c2 12", "122 l2_c3 12", "1000 l3_a1 100"))
	tk.MustExec("delete from t1 where id=1")
	require.Equal(t, uint64(1), tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	tk.MustQuery("select id, name, leader from t1 order by name").Check(testkit.Rows("10 l1_a <nil>", "12 l1_c <nil>", "100 l2_a1 10", "101 l2_a2 10", "102 l2_a3 10", "110 l2_b1 <nil>", "111 l2_b2 <nil>", "112 l2_b3 <nil>", "120 l2_c1 12", "121 l2_c2 12", "122 l2_c3 12", "1000 l3_a1 100"))

	// Test cascade set null depth.
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1(id int primary key, pid int, index(pid), foreign key(pid) references t1(id) on delete set null);")
	tk.MustExec("insert into t1 values(0,0),(1,0),(2,1),(3,2),(4,3),(5,4),(6,5),(7,6),(8,7),(9,8),(10,9),(11,10),(12,11),(13,12),(14,13),(15,14);")
	tk.MustExec("delete from t1 where id=0;")
	tk.MustQuery("select id, pid from t1").Check(testkit.Rows("1 <nil>", "2 1", "3 2", "4 3", "5 4", "6 5", "7 6", "8 7", "9 8", "10 9", "11 10", "12 11", "13 12", "14 13", "15 14"))

	// Test for cascade delete failed.
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (id int key)")
	tk.MustExec("create table t2 (id int, foreign key (id) references t1 (id) on delete set null)")
	tk.MustExec("create table t3 (id int, foreign key (id) references t2(id))")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	tk.MustExec("insert into t3 values (1)")
	// test in autocommit transaction
	tk.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("1"))
	// Test in transaction and commit transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (2),(3),(4)")
	tk.MustExec("insert into t2 values (2),(3)")
	tk.MustExec("insert into t3 values (3)")
	tk.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustExec("delete from t1 where id = 2")
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustQuery("select * from t1").Check(testkit.Rows("1", "3", "4"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("<nil>", "1", "3"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("1", "3"))
	tk.MustExec("commit")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("1", "3", "4"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("<nil>", "1", "3"))
	tk.MustQuery("select * from t3 order by id").Check(testkit.Rows("1", "3"))
	// Test in transaction and rollback transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (5), (6)")
	tk.MustExec("insert into t2 values (4), (5), (6)")
	tk.MustExec("insert into t3 values (5)")
	tk.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustExec("delete from t1 where id = 4")
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("1", "3", "5", "6"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("<nil>", "<nil>", "1", "3", "5", "6"))
	tk.MustQuery("select * from t3 order by id").Check(testkit.Rows("1", "3", "5"))
	tk.MustExec("rollback")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1", "3", "4"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("<nil>", "1", "3"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("1", "3"))
	tk.MustExec("delete from t3 where id = 1")
	tk.MustExec("delete from t1 where id = 1")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("3", "4"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("<nil>", "<nil>", "3"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("3"))

	// Test in autocommit=0 transaction
	tk.MustExec("set autocommit=0")
	tk.MustExec("insert into t1 values (1), (2)")
	tk.MustExec("insert into t2 values (1), (2)")
	tk.MustExec("insert into t3 values (1)")
	tk.MustGetDBError("delete from t1 where id = 1", plannererrors.ErrRowIsReferenced2)
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustExec("delete from t1 where id = 2")
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("1", "3", "4"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("<nil>", "<nil>", "<nil>", "1", "3"))
	tk.MustQuery("select * from t3 order by id").Check(testkit.Rows("1", "3"))
	tk.MustExec("set autocommit=1")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("1", "3", "4"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("<nil>", "<nil>", "<nil>", "1", "3"))
	tk.MustQuery("select * from t3 order by id").Check(testkit.Rows("1", "3"))

	// Test StmtCommit after fk cascade executor execute finish.
	tk.MustExec("drop table if exists t1,t2,t3")
	tk.MustExec("create table t0(id int primary key);")
	tk.MustExec("create table t1(id int primary key, pid int, index(pid), a int, foreign key(pid) references t1(id) on delete set null, foreign key(a) references t0(id) on delete set null);")
	tk.MustExec("insert into t0 values (0), (1)")
	tk.MustExec("insert into t1 values (0, 0, 0)")
	tk.MustExec("insert into t1 (id, pid) values(1,0),(2,1),(3,2),(4,3),(5,4),(6,5),(7,6),(8,7),(9,8),(10,9),(11,10),(12,11),(13,12),(14,13);")
	tk.MustExec("update t1 set a=1 where a is null")
	tk.MustExec("delete from t0 where id=0;")
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustQuery("select * from t0").Check(testkit.Rows("1"))
	tk.MustQuery("select id, pid, a from t1 order by id").Check(testkit.Rows("0 0 <nil>", "1 0 1", "2 1 1", "3 2 1", "4 3 1", "5 4 1", "6 5 1", "7 6 1", "8 7 1", "9 8 1", "10 9 1", "11 10 1", "12 11 1", "13 12 1", "14 13 1"))

	// Test multi-foreign key set null in one table.
	tk.MustExec("drop table if exists t1,t2,t3")
	tk.MustExec("create table t1 (id int key)")
	tk.MustExec("create table t2 (id int key)")
	tk.MustExec("create table t3 (id1 int, id2 int, constraint fk_id1 foreign key (id1) references t1 (id) on delete set null, " +
		"constraint fk_id2 foreign key (id2) references t2 (id) on delete set null)")
	tk.MustExec("insert into t1 values (1), (2), (3)")
	tk.MustExec("insert into t2 values (1), (2), (3)")
	tk.MustExec("insert into t3 values (1,1), (1, 2), (1, 3), (2, 1), (2, 2)")
	tk.MustExec("delete from t1 where id=1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select * from t3 order by id1").Check(testkit.Rows("<nil> 1", "<nil> 2", "<nil> 3", "2 1", "2 2"))
	tk.MustExec("create table t4 (id3 int key, constraint fk_id3 foreign key (id3) references t3 (id2))")
	tk.MustExec("insert into t4 values (2)")
	tk.MustExec("delete from t1 where id=2")
	tk.MustGetDBError("delete from t2 where id = 2", plannererrors.ErrRowIsReferenced2)
	tk.MustQuery("select * from t1").Check(testkit.Rows("3"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select * from t3 order by id1, id2").Check(testkit.Rows("<nil> 1", "<nil> 1", "<nil> 2", "<nil> 2", "<nil> 3"))

	// Test foreign key set null execution meet lock and do retry.
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk2.MustExec("set @@foreign_key_checks=1")
	tk2.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3, t4")
	tk.MustExec("create table t1 (id int key, name varchar(10), pid int, index(pid), constraint fk foreign key (pid) references t1 (id) on delete set null)")
	tk.MustExec("insert into t1 values (1, 'boss', null), (2, 'a', 1), (3, 'b', 1), (4, 'c', '2')")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values (5, 'd', 3)")
	tk2.MustExec("begin pessimistic")
	tk2.MustExec("insert into t1 values (6, 'e', 4)")
	tk2.MustExec("delete from t1 where id=2")
	tk2.MustExec("commit")
	tk.MustExec("delete from t1 where id = 1")
	tk.MustExec("commit")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("3 b <nil>", "4 c <nil>", "5 d 3", "6 e 4"))

	// Test foreign key cascade delete and set null in one row.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (id int key, name varchar(10), pid int, ppid int, index(pid), index(ppid) , constraint fk_pid foreign key (pid) references t1 (id) on delete cascade, " +
		"constraint fk_ppid foreign key (ppid) references t1 (id) on delete set null)")
	tk.MustExec("insert into t1 values (1, 'boss', null, null), (2, 'a', 1, 1), (3, 'b', 1, 1), (4, 'c', '2', 1)")
	tk.MustExec("delete from t1 where id = 1")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows())
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (id int key, name varchar(10), pid int, oid int, poid int, index(pid), index (oid), index(poid) , constraint fk_pid foreign key (pid) references t1 (id) on delete cascade, " +
		"constraint fk_poid foreign key (poid) references t1 (oid) on delete set null)")
	tk.MustExec("insert into t1 values (1, 'boss', null, 0, 0), (2, 'a', 1, 1, 0), (3, 'b', null, 2, 1), (4, 'c', 2, 3, 2)")
	tk.MustExec("delete from t1 where id = 1")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("3 b <nil> 2 <nil>"))

	// Test handle many foreign key value in one cascade.
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (id int auto_increment key, b int);")
	tk.MustExec("create table t2 (id int, b int, foreign key fk(id) references t1(id) on delete set null)")
	tk.MustExec("insert into t1 (b) values (1),(1),(1),(1),(1),(1),(1),(1);")
	for i := 0; i < 12; i++ {
		tk.MustExec("insert into t1 (b) select b from t1")
	}
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("32768"))
	tk.MustExec("insert into t2 select * from t1")
	tk.MustExec("delete from t1")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t2 where id is null").Check(testkit.Rows("32768"))
}

func TestForeignKeyOnUpdateCascade(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	cases := []struct {
		prepareSQLs []string
	}{
		// Case-1: test unique index only contain foreign key columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int, a int, b int,  unique index(a, b));",
				"create table t2 (b int, name varchar(10), a int, id int, unique index (a,b), foreign key fk(a, b) references t1(a, b) ON UPDATE CASCADE);",
			},
		},
		// Case-2: test unique index contain foreign key columns and other columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int key, a int, b int, unique index(a, b, id));",
				"create table t2 (b int, name varchar(10), a int, id int key, unique index (a,b, id), foreign key fk(a, b) references t1(a, b) ON UPDATE CASCADE);",
			},
		},
		// Case-3: test non-unique index only contain foreign key columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int key,a int, b int, index(a, b));",
				"create table t2 (b int, a int, name varchar(10), id int key, index (a, b), foreign key fk(a, b) references t1(a, b) ON UPDATE CASCADE);",
			},
		},
		// Case-4: test non-unique index contain foreign key columns and other columns.
		{
			prepareSQLs: []string{
				"create table t1 (id int key,a int, b int,  index(a, b, id));",
				"create table t2 (name varchar(10), b int, id int key, a int, index (a, b, id), foreign key fk(a, b) references t1(a, b) ON UPDATE CASCADE);",
			},
		},
	}

	for idx, ca := range cases {
		tk.MustExec("drop table if exists t2;")
		tk.MustExec("drop table if exists t1;")
		for _, sql := range ca.prepareSQLs {
			tk.MustExec(sql)
		}
		tk.MustExec("insert into t1 (id, a, b) values (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24), (5, 15, null), (6, null, 26), (7, null, null);")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 11, 21, 'a'),(2, 12, 22, 'b'), (3, 13, 23, 'c'), (4, 14, 24, 'd'), (5, 15, null, 'e'), (6, null, 26, 'f'), (7, null, null, 'g');")
		tk.MustExec("update t1 set a=a+100, b = b+200 where id in (1, 2)")
		tk.MustQuery("select id, a, b from t1 where id in (1,2) order by id").Check(testkit.Rows("1 111 221", "2 112 222"))
		tk.MustQuery("select id, a, b, name from t2 where id in (1,2,3) order by id").Check(testkit.Rows("1 111 221 a", "2 112 222 b", "3 13 23 c"))
		// Test update fk column to null
		tk.MustExec("update t1 set a=101, b=null where id = 1 or b = 222")
		tk.MustQuery("select id, a, b from t1 where id in (1,2) order by id").Check(testkit.Rows("1 101 <nil>", "2 101 <nil>"))
		tk.MustQuery("select id, a, b, name from t2 where id in (1,2,3) order by id").Check(testkit.Rows("1 101 <nil> a", "2 101 <nil> b", "3 13 23 c"))
		tk.MustExec("update t1 set a=null where b is null")
		tk.MustQuery("select id, a, b from t1 where b is null order by id").Check(testkit.Rows("1 <nil> <nil>", "2 <nil> <nil>", "5 <nil> <nil>", "7 <nil> <nil>"))
		tk.MustQuery("select id, a, b, name from t2 where b is null order by id").Check(testkit.Rows("1 101 <nil> a", "2 101 <nil> b", "5 15 <nil> e", "7 <nil> <nil> g"))
		// Test update fk column from null to not-null value
		tk.MustExec("update t1 set a=0, b = 0 where id = 7")
		tk.MustQuery("select id, a, b from t1 where a=0 and b=0 order by id").Check(testkit.Rows("7 0 0"))
		tk.MustQuery("select id, a, b from t2 where a=0 and b=0 order by id").Check(testkit.Rows())

		// Test in transaction.
		tk.MustExec("delete from t2")
		tk.MustExec("delete from t1")
		tk.MustExec("begin")
		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, null), (6, null, 6), (7, null, null);")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b'), (3, 3, 3, 'c'), (4, 4, 4, 'd'), (5, 5, null, 'e'), (6, null, 6, 'f'), (7, null, null, 'g');")
		tk.MustExec("update t1 set a=a+100, b = b+200 where id in (1, 2)")
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 101 201 a", "2 102 202 b", "3 3 3 c", "4 4 4 d", "5 5 <nil> e", "6 <nil> 6 f", "7 <nil> <nil> g"))
		tk.MustExec("rollback")
		tk.MustQuery("select * from t1").Check(testkit.Rows())
		tk.MustQuery("select * from t2").Check(testkit.Rows())

		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2);")
		tk.MustExec("begin")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b')")
		tk.MustExec("update t1 set a=101 where a = 1")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 101 1", "2 2 2"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 101 1 a", "2 2 2 b"))
		err := tk.ExecToErr("insert into t2 (id, a, b, name) values (3, 1, 1, 'c')")
		require.Error(t, err)
		require.True(t, plannererrors.ErrNoReferencedRow2.Equal(err), err.Error())
		tk.MustExec("insert into t1 values (3, 1, 1);")
		tk.MustExec("insert into t2 (id, a, b, name) values (3, 1, 1, 'c')")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 101 1", "2 2 2", "3 1 1"))
		tk.MustQuery("select id, a, b, name from t2 order by id, a").Check(testkit.Rows("1 101 1 a", "2 2 2 b", "3 1 1 c"))
		tk.MustExec("update t1 set a=null, b=2000 where id in (1, 2)")
		tk.MustExec("commit")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 <nil> 2000", "2 <nil> 2000", "3 1 1"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 <nil> 2000 a", "2 <nil> 2000 b", "3 1 1 c"))

		// only test in non-unique index
		if idx >= 2 {
			tk.MustExec("delete from t2")
			tk.MustExec("delete from t1")
			tk.MustExec("insert into t1 values (1, 1, 1),(2, 1, 1);")
			tk.MustExec("begin")
			tk.MustExec("update t1 set a=101 where id = 1")
			tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a')")
			tk.MustExec("update t1 set b=102 where id = 2")
			tk.MustQuery("select * from t1").Check(testkit.Rows("1 101 1", "2 1 102"))
			tk.MustQuery("select id, a, b, name from t2").Check(testkit.Rows("1 1 102 a"))
			err := tk.ExecToErr("insert into t2 (id, a, b, name) values (3, 1, 1, 'e')")
			require.Error(t, err)
			require.True(t, plannererrors.ErrNoReferencedRow2.Equal(err), err.Error())
			tk.MustExec("insert into t1 values (3, 1, 1);")
			tk.MustExec("insert into t2 (id, a, b, name) values (3, 1, 1, 'e')")
			tk.MustExec("commit")
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 101 1", "2 1 102", "3 1 1"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 1 102 a", "3 1 1 e"))

			tk.MustExec("delete from t2")
			tk.MustExec("delete from t1")
			tk.MustExec("begin")
			tk.MustExec("insert into t1 values (1, 1, 1),(2, 1, 1);")
			tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'), (2, 1, 1, 'b')")
			tk.MustExec("update t1 set a=101, b=102 where id = 1")
			tk.MustExec("commit")
			tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 101 102", "2 1 1"))
			tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 101 102 a", "2 101 102 b"))
		}
	}

	cases = []struct {
		prepareSQLs []string
	}{
		// Case-5: test primary key only contain foreign key columns, and disable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=0;",
				"create table t1 (id int, a int, b int,  primary key (a, b));",
				"create table t2 (b int,  a int, name varchar(10), id int, primary key (a, b), foreign key fk(a, b) references t1(a, b) ON UPDATE CASCADE);",
			},
		},
		// Case-6: test primary key only contain foreign key columns, and enable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=1;",
				"create table t1 (id int, a int, b int,  primary key (a, b));",
				"create table t2 (name varchar(10), b int,  a int, id int, primary key (a, b), foreign key fk(a, b) references t1(a, b) ON UPDATE CASCADE);",
			},
		},
		// Case-7: test primary key contain foreign key columns and other column, and disable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=0;",
				"create table t1 (id int, a int, b int,  primary key (a, b, id));",
				"create table t2 (b int, name varchar(10),  a int, id int, primary key (a, b, id), foreign key fk(a, b) references t1(a, b) ON UPDATE CASCADE);",
			},
		},
		// Case-8: test primary key contain foreign key columns and other column, and enable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=1;",
				"create table t1 (id int, a int, b int,  primary key (a, b, id));",
				"create table t2 (b int,  a int, id int, name varchar(10), primary key (a, b, id), foreign key fk(a, b) references t1(a, b) ON UPDATE CASCADE);",
			},
		},
	}
	for idx, ca := range cases {
		tk.MustExec("drop table if exists t2;")
		tk.MustExec("drop table if exists t1;")
		for _, sql := range ca.prepareSQLs {
			tk.MustExec(sql)
		}
		tk.MustExec("insert into t1 (id, a, b) values (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 11, 21, 'a'),(2, 12, 22, 'b'), (3, 13, 23, 'c'), (4, 14, 24, 'd')")
		tk.MustExec("update t1 set a=a+100, b = b+200 where id in (1, 2)")
		tk.MustQuery("select id, a, b from t1 where id in (1,2) order by id").Check(testkit.Rows("1 111 221", "2 112 222"))
		tk.MustQuery("select id, a, b, name from t2 where id in (1,2,3) order by id").Check(testkit.Rows("1 111 221 a", "2 112 222 b", "3 13 23 c"))
		tk.MustExec("update t1 set a=101 where id = 1 or b = 222")
		tk.MustQuery("select id, a, b from t1 where id in (1,2) order by id").Check(testkit.Rows("1 101 221", "2 101 222"))
		tk.MustQuery("select id, a, b, name from t2 where id in (1,2,3) order by id").Check(testkit.Rows("1 101 221 a", "2 101 222 b", "3 13 23 c"))

		if idx < 2 {
			tk.MustGetDBError("update t1 set b=200 where id in (1,2);", kv.ErrKeyExists)
		}

		// test in transaction.
		tk.MustExec("delete from t2")
		tk.MustExec("delete from t1")
		tk.MustExec("begin")
		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2), (3, 3, 3), (4, 4, 4);")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b'), (3, 3, 3, 'c'), (4, 4, 4, 'd');")
		tk.MustExec("update t1 set a=a+100, b=b+200 where id = 1 or a = 2")
		tk.MustExec("update t1 set a=a+1000, b=b+2000 where a in (2,3,4) or b in (5,6,7) or id=2")
		tk.MustQuery("select id, a, b from t2 order by id").Check(testkit.Rows("1 101 201", "2 1102 2202", "3 1003 2003", "4 1004 2004"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 101 201 a", "2 1102 2202 b", "3 1003 2003 c", "4 1004 2004 d"))
		tk.MustExec("commit")
		tk.MustQuery("select id, a, b from t2 order by id").Check(testkit.Rows("1 101 201", "2 1102 2202", "3 1003 2003", "4 1004 2004"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 101 201 a", "2 1102 2202 b", "3 1003 2003 c", "4 1004 2004 d"))

		tk.MustExec("delete from t2")
		tk.MustExec("delete from t1")
		tk.MustExec("insert into t1 values (1, 1, 1),(2, 2, 2);")
		tk.MustExec("begin")
		tk.MustExec("insert into t2 (id, a, b, name) values (1, 1, 1, 'a'),(2, 2, 2, 'b')")
		tk.MustExec("update t1 set a=a+100, b=b+200 where id = 1")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 101 201", "2 2 2"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 101 201 a", "2 2 2 b"))
		err := tk.ExecToErr("insert into t2 (id, a, b, name) values (3, 1, 1, 'e')")
		require.Error(t, err)
		require.True(t, plannererrors.ErrNoReferencedRow2.Equal(err), err.Error())
		tk.MustExec("insert into t1 values (3, 1, 1);")
		tk.MustExec("insert into t2 (id, a, b, name) values (3, 1, 1, 'c')")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 101 201", "2 2 2", "3 1 1"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 101 201 a", "2 2 2 b", "3 1 1 c"))
		tk.MustExec("update t1 set a=a+1000, b=b+2000 where a>1")
		tk.MustExec("commit")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 1101 2201", "2 1002 2002", "3 1 1"))
		tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("1 1101 2201 a", "2 1002 2002 b", "3 1 1 c"))
	}

	// Case-9: test primary key is handle and contain foreign key column.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("set @@tidb_enable_clustered_index=0;")
	tk.MustExec("create table t1 (id int, a int, b int,  primary key (id));")
	tk.MustExec("create table t2 (b int,  a int, id int, name varchar(10), primary key (a), foreign key fk(a) references t1(id) ON UPDATE CASCADE);")
	tk.MustExec("insert into t1 (id, a, b) values       (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)")
	tk.MustExec("insert into t2 (id, a, b, name) values (11, 1, 21, 'a'),(12, 2, 22, 'b'), (13, 3, 23, 'c'), (14, 4, 24, 'd')")
	tk.MustExec("update t1 set id = id + 100 where id in (1, 2, 3)")
	tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("4 14 24", "101 11 21", "102 12 22", "103 13 23"))
	tk.MustQuery("select id, a, b, name from t2 order by id").Check(testkit.Rows("11 101 21 a", "12 102 22 b", "13 103 23 c", "14 4 24 d"))
}

func TestForeignKeyOnUpdateCascade2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	// Test update same old row in parent, but only the first old row do cascade update
	tk.MustExec("create table t1 (id int key, a int,  index (a));")
	tk.MustExec("create table t2 (id int key, pid int, constraint fk_pid foreign key (pid) references t1(a) ON UPDATE CASCADE);")
	tk.MustExec("insert into t1 (id, a) values   (1,1), (2, 1)")
	tk.MustExec("insert into t2 (id, pid) values (1,1), (2, 1)")
	tk.MustExec("update t1 set a=id+1")
	tk.MustQuery("select id, a from t1 order by id").Check(testkit.Rows("1 2", "2 3"))
	tk.MustQuery("select id, pid from t2 order by id").Check(testkit.Rows("1 2", "2 2"))

	// Test cascade delete in self table.
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (id int key, name varchar(10), leader int,  index(leader), foreign key (leader) references t1(id) ON UPDATE CASCADE);")
	tk.MustExec("insert into t1 values (1, 'boss', null), (10, 'l1_a', 1), (11, 'l1_b', 1), (12, 'l1_c', 1)")
	tk.MustExec("insert into t1 values (100, 'l2_a1', 10)")
	tk.MustExec("insert into t1 values (110, 'l2_b1', 11)")
	tk.MustExec("insert into t1 values (1000,'l3_a1', 100)")
	tk.MustExec("update t1 set id=id+10000 where id=11")
	tk.MustQuery("select id, name, leader from t1 order by id").Check(testkit.Rows("1 boss <nil>", "10 l1_a 1", "12 l1_c 1", "100 l2_a1 10", "110 l2_b1 10011", "1000 l3_a1 100", "10011 l1_b 1"))
	tk.MustExec("update t1 set id=0 where id=1")
	tk.MustQuery("select id, name, leader from t1 order by id").Check(testkit.Rows("0 boss <nil>", "10 l1_a 0", "12 l1_c 0", "100 l2_a1 10", "110 l2_b1 10011", "1000 l3_a1 100", "10011 l1_b 0"))

	// Test explain analyze with foreign key cascade.
	tk.MustExec("explain analyze update t1 set id=1 where id=10")
	tk.MustQuery("select id, name, leader from t1 order by id").Check(testkit.Rows("0 boss <nil>", "1 l1_a 0", "12 l1_c 0", "100 l2_a1 1", "110 l2_b1 10011", "1000 l3_a1 100", "10011 l1_b 0"))

	// Test cascade delete in self table with string type foreign key.
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (id varchar(100) key, name varchar(10), leader varchar(100),  index(leader), foreign key (leader) references t1(id) ON UPDATE CASCADE);")
	tk.MustExec("insert into t1 values (1, 'boss', null), (10, 'l1_a', 1), (11, 'l1_b', 1), (12, 'l1_c', 1)")
	tk.MustExec("insert into t1 values (100, 'l2_a1', 10)")
	tk.MustExec("insert into t1 values (110, 'l2_b1', 11)")
	tk.MustExec("insert into t1 values (1000,'l3_a1', 100)")
	tk.MustExec("update t1 set id=id+10000 where id=11")
	tk.MustQuery("select id, name, leader from t1 order by name").Check(testkit.Rows("1 boss <nil>", "10 l1_a 1", "10011 l1_b 1", "12 l1_c 1", "100 l2_a1 10", "110 l2_b1 10011", "1000 l3_a1 100"))
	tk.MustExec("update t1 set id=0 where id=1")
	tk.MustQuery("select id, name, leader from t1 order by name").Check(testkit.Rows("0 boss <nil>", "10 l1_a 0", "10011 l1_b 0", "12 l1_c 0", "100 l2_a1 10", "110 l2_b1 10011", "1000 l3_a1 100"))

	// Test cascade delete depth error.
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t0 (id int, unique index(id))")
	tk.MustExec("insert into t0 values (1)")
	for i := 1; i < 17; i++ {
		tk.MustExec(fmt.Sprintf("create table t%v (id int, unique index(id), foreign key (id) references t%v(id) on update cascade)", i, i-1))
		tk.MustExec(fmt.Sprintf("insert into t%v values (1)", i))
	}
	tk.MustGetDBError("update t0 set id=10 where id=1;", exeerrors.ErrForeignKeyCascadeDepthExceeded)
	tk.MustQuery("select id from t0").Check(testkit.Rows("1"))
	tk.MustQuery("select id from t15").Check(testkit.Rows("1"))
	tk.MustExec("drop table if exists t16")
	tk.MustExec("update t0 set id=10 where id=1;")
	tk.MustQuery("select id from t0").Check(testkit.Rows("10"))
	tk.MustQuery("select id from t15").Check(testkit.Rows("10"))
	for i := 16; i > -1; i-- {
		tk.MustExec("drop table if exists t" + strconv.Itoa(i))
	}

	// Test handle many foreign key value in one cascade.
	tk.MustExec("create table t1 (id int auto_increment key, b int, index(b));")
	tk.MustExec("create table t2 (id int, b int, foreign key fk(b) references t1(b) on update cascade)")
	tk.MustExec("insert into t1 (b) values (1),(2),(3),(4),(5),(6),(7),(8);")
	for i := 0; i < 12; i++ {
		tk.MustExec("insert into t1 (b) select id from t1")
	}
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("32768"))
	tk.MustExec("insert into t2 select * from t1")
	tk.MustExec("update t1 set b=2")
	tk.MustQuery("select count(*) from t1 join t2 where t1.id=t2.id and t1.b=t2.b").Check(testkit.Rows("32768"))
}

func TestDMLExplainAnalyzeFKInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	// Test for Insert ignore foreign check runtime stats.
	tk.MustExec("drop table if exists t1,t2,t3")
	tk.MustExec("create table t1 (id int key)")
	tk.MustExec("create table t2 (id int key)")
	tk.MustExec("create table t3 (id int key, id1 int, id2 int, constraint fk_id1 foreign key (id1) references t1 (id) on delete cascade, " +
		"constraint fk_id2 foreign key (id2) references t2 (id) on delete cascade)")
	tk.MustExec("insert into t1 values (1), (2)")
	tk.MustExec("insert into t2 values (1)")
	res := tk.MustQuery("explain analyze insert ignore into t3 values (1, 1, 1), (2, 1, 1), (3, 2, 1), (4, 1, 1), (5, 2, 1), (6, 2, 1)")
	explain := getExplainResult(res)
	require.Regexpf(t, "time:.* loops:.* prepare:.* check_insert: {total_time:.* mem_insert_time:.* prefetch:.* fk_check:.*", explain, "")
	res = tk.MustQuery("explain analyze insert ignore into t3 values (7, null, null), (8, null, null)")
	explain = getExplainResult(res)
	require.Regexpf(t, "time:.* loops:.* prepare:.* check_insert: {total_time:.* mem_insert_time:.* prefetch:.* fk_check:.*", explain, "")
}

func getExplainResult(res *testkit.Result) string {
	resBuff := bytes.NewBufferString("")
	for _, row := range res.Rows() {
		_, _ = fmt.Fprintf(resBuff, "%s\t", row)
	}
	return resBuff.String()
}

func TestForeignKeyOnInsertOnDuplicateUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, name varchar(10));")
	tk.MustExec("create table t2 (id int key, pid int, foreign key fk(pid) references t1(id) ON UPDATE CASCADE ON DELETE CASCADE);")
	tk.MustExec("insert into t1 values (1, 'a'), (2, 'b')")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 1), (4, 2), (5, null)")
	tk.MustExec("insert into t1 values (1, 'aa') on duplicate key update name = 'aa'")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("1 aa", "2 b"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("1 1", "2 2", "3 1", "4 2", "5 <nil>"))
	tk.MustExec("insert into t1 values (1, 'aaa') on duplicate key update id = 10")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("2 b", "10 aa"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("1 10", "2 2", "3 10", "4 2", "5 <nil>"))
	// Test in transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (3, 'c')")
	tk.MustExec("insert into t2 values (6, 3)")
	tk.MustExec("insert into t1 values (2, 'bb'), (3, 'cc') on duplicate key update id =id*10")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("10 aa", "20 b", "30 c"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("1 10", "2 20", "3 10", "4 20", "5 <nil>", "6 30"))
	tk.MustExec("commit")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("10 aa", "20 b", "30 c"))
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("1 10", "2 20", "3 10", "4 20", "5 <nil>", "6 30"))
	tk.MustExec("delete from t1")
	tk.MustQuery("select * from t2").Check(testkit.Rows("5 <nil>"))
	// Test for cascade update failed.
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1 (id int key)")
	tk.MustExec("create table t2 (id int key, foreign key (id) references t1 (id) on update cascade)")
	tk.MustExec("create table t3 (id int key, foreign key (id) references t2(id))")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	tk.MustExec("insert into t3 values (1)")
	tk.MustGetDBError("insert into t1 values (1) on duplicate key update id = 2", plannererrors.ErrRowIsReferenced2)
	require.Equal(t, 0, len(tk.Session().GetSessionVars().TxnCtx.Savepoints))
	tk.MustExec("commit")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t3").Check(testkit.Rows("1"))
}

func TestExplainAnalyzeDMLWithFKInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key);")
	tk.MustExec("create table t2 (id int key, foreign key fk(id) references t1(id) ON UPDATE CASCADE ON DELETE CASCADE);")
	tk.MustExec("create table t3 (id int, unique index idx(id));")
	tk.MustExec("create table t4 (id int, index idx_id(id),foreign key fk(id) references t3(id));")
	tk.MustExec("create table t5 (id int key, id2 int, id3 int, unique index idx2(id2), index idx3(id3));")
	tk.MustExec("create table t6 (id int,     id2 int, id3 int, index idx_id(id), index idx_id2(id2), " +
		"foreign key fk_1 (id) references t5(id) ON UPDATE CASCADE ON DELETE SET NULL, " +
		"foreign key fk_2 (id2) references t5(id2) ON UPDATE CASCADE, " +
		"foreign key fk_3 (id3) references t5(id3) ON DELETE CASCADE);")
	tk.MustExec("create table t7(id int primary key, pid int, index(pid), foreign key(pid) references t7(id) on delete cascade);")

	cases := []struct {
		prepare []string
		sql     string
		plan    string
	}{
		// Test foreign key use primary key.
		{
			prepare: []string{
				"insert into t1 values (1),(2),(3),(4),(5)",
			},
			sql: "explain analyze insert into t2 values (1),(2),(3);",
			plan: "Insert_. N/A 0 root  time:.*, loops:1, prepare:.*, insert:.*" +
				"└─Foreign_Key_Check_. 0.00 0 root table:t1 total:.*, check:.*, lock:.*, foreign_keys:3 foreign_key:fk, check_exist N/A N/A",
		},
		{
			sql: "explain analyze insert ignore into t2 values (10),(11),(12);",
			plan: "Insert_.* fk_check.*" +
				"└─Foreign_Key_Check_.* 0 root table:t1 total:0s, foreign_keys:3 foreign_key:fk, check_exist N/A N/A",
		},
		{
			sql: "explain analyze update t2 set id=id+2 where id >1",
			plan: "Update_.* 0 root  time:.*, loops:1.*" +
				"├─TableReader_.*" +
				"│ └─TableRangeScan.*" +
				"└─Foreign_Key_Check_.* 0 root table:t1 total:.*, check:.*, lock:.*, foreign_keys:2 foreign_key:fk, check_exist N/A N/A",
		},
		{
			sql: "explain analyze delete from t1 where id>1",
			plan: "Delete_.*" +
				"├─TableReader_.*" +
				"│ └─TableRangeScan_.*" +
				"└─Foreign_Key_Cascade_.* 0 root table:t2 total:.*, foreign_keys:4 foreign_key:fk, on_delete:CASCADE N/A N/A.*" +
				"  └─Delete_.*" +
				"    └─Batch_Point_Get_.*",
		},
		{
			sql: "explain analyze update t1 set id=id+1 where id = 1",
			plan: "Update_.*" +
				"├─Point_Get_.*" +
				"└─Foreign_Key_Cascade_.* 0 root table:t2 total:.*, foreign_keys:1 foreign_key:fk, on_update:CASCADE N/A N/A.*" +
				"  └─Update_.*" +
				"    ├─Point_Get_.*" +
				"    └─Foreign_Key_Check_.*",
		},
		{
			sql: "explain analyze insert into t1 values (1) on duplicate key update id = 100",
			plan: "Insert_.*" +
				"└─Foreign_Key_Cascade_.* 0 root table:t2 total:0s foreign_key:fk, on_update:CASCADE N/A N/A",
		},
		{
			sql: "explain analyze insert into t1 values (2) on duplicate key update id = 100",
			plan: "Insert_.*" +
				"└─Foreign_Key_Cascade_.* 0 root table:t2 total:.*, foreign_keys:1 foreign_key:fk, on_update:CASCADE N/A N/A.*" +
				"  └─Update_.*" +
				"    ├─Point_Get_.*" +
				"    └─Foreign_Key_Check_.* 0 root table:t1 total:.*, check:.*, lock:.*, foreign_keys:1 foreign_key:fk, check_exist N/A N/A",
		},
		// Test foreign key use index.
		{
			prepare: []string{
				"insert into t3 values (1),(2),(3),(4),(5)",
			},
			sql: "explain analyze insert into t4 values (1),(2),(3);",
			plan: "Insert_.*" +
				"└─Foreign_Key_Check_.* 0 root table:t3, index:idx total:.*, check:.*, lock:.*, foreign_keys:3 foreign_key:fk, check_exist N/A N/A",
		},
		{
			sql: "explain analyze update t4 set id=id+2 where id >1",
			plan: "Update_.*" +
				"├─IndexReader_.*" +
				"│ └─IndexRangeScan_.*" +
				"└─Foreign_Key_Check_.* 0 root table:t3, index:idx total:.*, check:.*, lock:.*, foreign_keys:2 foreign_key:fk, check_exist N/A N/A",
		},
		{
			sql: "explain analyze delete from t3 where id in (2,3)",
			plan: "Delete_.*" +
				"├─Batch_Point_Get_.*" +
				"└─Foreign_Key_Check_.* 0 root table:t4, index:idx_id total:.*, check:.*, foreign_keys:2 foreign_key:fk, check_not_exist N/A N/A",
		},
		{
			prepare: []string{
				"insert into t3 values (2)",
			},
			sql: "explain analyze update t3 set id=id+1 where id = 2",
			plan: "Update_.*" +
				"├─Point_Get_.*" +
				"└─Foreign_Key_Check_.* 0 root table:t4, index:idx_id total:.*, check:.*, foreign_keys:1 foreign_key:fk, check_not_exist N/A N/A",
		},

		{
			sql: "explain analyze insert into t3 values (2) on duplicate key update id = 100",
			plan: "Insert_.*" +
				"└─Foreign_Key_Check_.* 0 root table:t4, index:idx_id total:0s foreign_key:fk, check_not_exist N/A N/A",
		},
		{
			sql: "explain analyze insert into t3 values (3) on duplicate key update id = 100",
			plan: "Insert_.*" +
				"└─Foreign_Key_Check_.* 0 root table:t4, index:idx_id total:.*, check:.*, foreign_keys:1 foreign_key:fk, check_not_exist N/A N/A",
		},
		// Test multi-foreign keys in on table.
		{
			prepare: []string{
				"insert into t5 values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)",
			},
			sql: "explain analyze insert into t6 values (1,1,1)",
			plan: "Insert_.*" +
				"├─Foreign_Key_Check_.* 0 root table:t5 total:.*, check:.*, lock:.*, foreign_keys:1 foreign_key:fk_1, check_exist N/A N/A.*" +
				"├─Foreign_Key_Check_.* 0 root table:t5, index:idx2 total:.*, check:.*, lock:.*, foreign_keys:1 foreign_key:fk_2, check_exist N/A N/A.*" +
				"└─Foreign_Key_Check_.* 0 root table:t5, index:idx3 total:.*, check:.*, lock:.*, foreign_keys:1 foreign_key:fk_3, check_exist N/A N/A",
		},
		{
			sql: "explain analyze insert ignore into t6 values (1,1,10)",
			plan: "Insert_.* root  time:.* loops:.* prepare:.* check_insert.* fk_check:.*" +
				"├─Foreign_Key_Check.* 0 root table:t5 total:0s, foreign_keys:1 foreign_key:fk_1, check_exist N/A N/A.*" +
				"├─Foreign_Key_Check.* 0 root table:t5, index:idx2 total:0s, foreign_keys:1 foreign_key:fk_2, check_exist N/A N/A.*" +
				"└─Foreign_Key_Check.* 0 root table:t5, index:idx3 total:0s, foreign_keys:1 foreign_key:fk_3, check_exist N/A N/A",
		},
		{
			sql: "explain analyze update t6 set id=id+1, id3=id2+1 where id = 1",
			plan: "Update_.*" +
				"├─IndexLookUp_.*" +
				"│ ├─IndexRangeScan_.*" +
				"│ └─TableRowIDScan_.*" +
				"├─Foreign_Key_Check_.* 0 root table:t5 total:.*, check:.*, lock:.*, foreign_keys:1 foreign_key:fk_1, check_exist N/A N/A.*" +
				"└─Foreign_Key_Check_.* 0 root table:t5, index:idx3 total:.*, check:.*, lock:.*, foreign_keys:1 foreign_key:fk_3, check_exist N/A N/A",
		},
		{
			sql: "explain analyze delete from t5 where id in (4,5)",
			plan: "Delete_.*" +
				"├─Batch_Point_Get_.*" +
				"├─Foreign_Key_Check_.* 0 root table:t6, index:idx_id2 total:.*, check:.*, foreign_keys:2 foreign_key:fk_2, check_not_exist N/A N/A.*" +
				"├─Foreign_Key_Cascade_.* 0 root table:t6, index:idx_id total:.*, foreign_keys:2 foreign_key:fk_1, on_delete:SET NULL N/A N/A.*" +
				"│ └─Update_.*" +
				"│   │ ├─IndexRangeScan_.*" +
				"│   │ └─TableRowIDScan_.*" +
				"│   └─Foreign_Key_Check_.* 0 root table:t5 total:0s foreign_key:fk_1, check_exist N/A N/A.*" +
				"└─Foreign_Key_Cascade_.* 0 root table:t6, index:fk_3 total:.*, foreign_keys:2 foreign_key:fk_3, on_delete:CASCADE N/A N/A.*" +
				"  └─Delete_.*" +
				"    └─IndexLookUp_.*" +
				"      ├─IndexRangeScan_.*" +
				"      └─TableRowIDScan_.*",
		},
		{
			sql: "explain analyze update t5 set id=id+1, id2=id2+1 where id = 3",
			plan: "Update_.*" +
				"├─Point_Get_.*" +
				"├─Foreign_Key_Cascade_.* 0 root table:t6, index:idx_id total:.*, foreign_keys:1 foreign_key:fk_1, on_update:CASCADE N/A N/A.*" +
				"│ └─Update_.*" +
				"│   ├─IndexLookUp_.*" +
				"│   │ ├─IndexRangeScan_.*" +
				"│   │ └─TableRowIDScan_.*" +
				"│   └─Foreign_Key_Check_.* 0 root table:t5 total:0s foreign_key:fk_1, check_exist N/A N/A.*" +
				"└─Foreign_Key_Cascade_.* 0 root table:t6, index:idx_id2 total:.*, foreign_keys:1 foreign_key:fk_2, on_update:CASCADE N/A N/A.*" +
				"  └─Update_.*" +
				"    ├─IndexLookUp_.*" +
				"    │ ├─IndexRangeScan_.*" +
				"    │ └─TableRowIDScan_.*" +
				"    └─Foreign_Key_Check_.* 0 root table:t5, index:idx2 total:0s foreign_key:fk_2, check_exist N/A N/A",
		},
		{
			prepare: []string{
				"insert into t5 values (10,10,10)",
			},
			sql: "explain analyze update t5 set id=id+1, id2=id2+1, id3=id3+1 where id = 10",
			plan: "Update_.*" +
				"├─Point_Get_.*" +
				"├─Foreign_Key_Check_.* 0 root table:t6, index:fk_3 total:.*, check:.*, foreign_keys:1 foreign_key:.*, check_not_exist N/A N/A.*" +
				"├─Foreign_Key_Cascade_.* 0 root table:t6, index:idx_id total:.*, foreign_keys:1 foreign_key:fk_1, on_update:CASCADE N/A N/A.*" +
				"│ └─Update_.*" +
				"│   ├─IndexLookUp_.*" +
				"│   │ ├─IndexRangeScan_.*" +
				"│   │ └─TableRowIDScan_.*" +
				"│   └─Foreign_Key_Check_.*" +
				"└─Foreign_Key_Cascade_.* 0 root table:t6, index:idx_id2 total:.*, foreign_keys:1 foreign_key:fk_2, on_update:CASCADE N/A N/A.*" +
				"  └─Update_.*" +
				"    ├─IndexLookUp_.*" +
				"    │ ├─IndexRangeScan_.*" +
				"    │ └─TableRowIDScan_.*" +
				"    └─Foreign_Key_Check_.* 0 root table:t5, index:idx2 total:0s foreign_key:fk_2, check_exist N/A N/A",
		},
		{
			sql: "explain analyze insert into t5 values (1,1,1) on duplicate key update id = 100, id3=100",
			plan: "Insert_.*" +
				"├─Foreign_Key_Check_.* 0 root table:t6, index:fk_3 total:.*, check:.*, foreign_keys:1 foreign_key:fk_3, check_not_exist N/A N/A.*" +
				"└─Foreign_Key_Cascade_.* 0 root table:t6, index:idx_id total:.*, foreign_keys:1 foreign_key:fk_1, on_update:CASCADE N/A N/A.*" +
				"  └─Update_.*" +
				"    ├─IndexLookUp_.*" +
				"    │ ├─IndexRangeScan_.*" +
				"    │ └─TableRowIDScan_.*" +
				"    └─Foreign_Key_Check_.* 0 root table:t5 total:0s foreign_key:fk_1, check_exist N/A N/A",
		},
		{
			prepare: []string{
				"insert into t7 values(0,0),(1,0),(2,1),(3,2),(4,3),(5,4),(6,5),(7,6),(8,7),(9,8),(10,9),(11,10),(12,11),(13,12),(14,13);",
			},
			sql: "explain analyze delete from t7 where id = 0;",
			plan: "Delete_.*" +
				"├─Point_Get_.*" +
				"└─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.* foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"  └─Delete_.*" +
				"    ├─UnionScan_.*" +
				"    │ └─IndexReader_.*" +
				"    │   └─IndexRangeScan_.*" +
				"    └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.* foreign_keys:2 foreign_key:fk_1, on_delete:CASCADE.*" +
				"      └─Delete_.*" +
				"        ├─UnionScan_.*" +
				"        │ └─IndexReader_.*" +
				"        │   └─IndexRangeScan_.*" +
				"        └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"          └─Delete_.*" +
				"            ├─UnionScan_.*" +
				"            │ └─IndexReader_.*" +
				"            │   └─IndexRangeScan_.*" +
				"            └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"              └─Delete_.*" +
				"                ├─UnionScan_.*" +
				"                │ └─IndexReader_.*" +
				"                │   └─IndexRangeScan_.*" +
				"                └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"                  └─Delete_.*" +
				"                    ├─UnionScan_.*" +
				"                    │ └─IndexReader_.*" +
				"                    │   └─IndexRangeScan_.*" +
				"                    └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"                      └─Delete_.*" +
				"                        ├─UnionScan_.*" +
				"                        │ └─IndexReader_.*" +
				"                        │   └─IndexRangeScan_.*" +
				"                        └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"                          └─Delete_.*" +
				"                            ├─UnionScan_.*" +
				"                            │ └─IndexReader_.*" +
				"                            │   └─IndexRangeScan_.*" +
				"                            └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"                              └─Delete_.*" +
				"                                ├─UnionScan_.*" +
				"                                │ └─IndexReader_.*" +
				"                                │   └─IndexRangeScan_.*" +
				"                                └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"                                  └─Delete_.*" +
				"                                    ├─UnionScan_.*" +
				"                                    │ └─IndexReader_.*" +
				"                                    │   └─IndexRangeScan_.*" +
				"                                    └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"                                      └─Delete_.*" +
				"                                        ├─UnionScan_.*" +
				"                                        │ └─IndexReader_.*" +
				"                                        │   └─IndexRangeScan_.*" +
				"                                        └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"                                          └─Delete_.*" +
				"                                            ├─UnionScan_.*" +
				"                                            │ └─IndexReader_.*" +
				"                                            │   └─IndexRangeScan_.*" +
				"                                            └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"                                              └─Delete_.*" +
				"                                                ├─UnionScan_.*" +
				"                                                │ └─IndexReader_.*" +
				"                                                │   └─IndexRangeScan_.*" +
				"                                                └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"                                                  └─Delete_.*" +
				"                                                    ├─UnionScan_.*" +
				"                                                    │ └─IndexReader_.*" +
				"                                                    │   └─IndexRangeScan_.*" +
				"                                                    └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"                                                      └─Delete_.*" +
				"                                                        ├─UnionScan_.*" +
				"                                                        │ └─IndexReader_.*" +
				"                                                        │   └─IndexRangeScan_.*" +
				"                                                        └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:.*, foreign_keys:1 foreign_key:fk_1, on_delete:CASCADE.*" +
				"                                                          └─Delete_.*" +
				"                                                            ├─UnionScan_.*" +
				"                                                            │ └─IndexReader_.*" +
				"                                                            │   └─IndexRangeScan_.*" +
				"                                                            └─Foreign_Key_Cascade_.* 0 root table:t7, index:pid total:0s foreign_key:fk_1, on_delete:CASCADE.*",
		},
	}
	for _, ca := range cases {
		for _, sql := range ca.prepare {
			tk.MustExec(sql)
		}
		res := tk.MustQuery(ca.sql)
		explain := getExplainResult(res)
		require.Regexp(t, ca.plan, explain)
	}
}

func TestForeignKeyRuntimeStats(t *testing.T) {
	checkStats := executor.FKCheckRuntimeStats{
		Total: time.Second * 3,
		Check: time.Second * 2,
		Lock:  time.Second,
		Keys:  10,
	}
	require.Equal(t, "total:3s, check:2s, lock:1s, foreign_keys:10", checkStats.String())
	checkStats.Merge(checkStats.Clone())
	require.Equal(t, "total:6s, check:4s, lock:2s, foreign_keys:20", checkStats.String())
	cascadeStats := executor.FKCascadeRuntimeStats{
		Total: time.Second,
		Keys:  10,
	}
	require.Equal(t, "total:1s, foreign_keys:10", cascadeStats.String())
	cascadeStats.Merge(cascadeStats.Clone())
	require.Equal(t, "total:2s, foreign_keys:20", cascadeStats.String())
}

func TestPrivilegeCheckInForeignKeyCascade(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key);")
	tk.MustExec("create table t2 (id int key, foreign key fk (id) references t1(id) ON DELETE CASCADE ON UPDATE CASCADE);")
	tk.MustExec("insert into t1 values (1), (2), (3);")
	cases := []struct {
		prepares []string
		sql      string
		err      error
		t1Rows   []string
		t2Rows   []string
	}{
		{
			prepares: []string{"grant insert on test.t2 to 'u1'@'%';"},
			sql:      "insert into t2 values (1), (2), (3);",
			t1Rows:   []string{"1", "2", "3"},
			t2Rows:   []string{"1", "2", "3"},
		},
		{
			prepares: []string{"grant select, delete on test.t1 to 'u1'@'%';"},
			sql:      "delete from t1 where id=1;",
			t1Rows:   []string{"2", "3"},
			t2Rows:   []string{"2", "3"},
		},
		{
			prepares: []string{"grant select, update on test.t1 to 'u1'@'%';"},
			sql:      "update t1 set id=id+10 where id=2;",
			t1Rows:   []string{"3", "12"},
			t2Rows:   []string{"3", "12"},
		},
	}
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("set @@foreign_key_checks=1")
	for _, ca := range cases {
		tk.MustExec("drop user if exists 'u1'@'%'")
		tk.MustExec("create user 'u1'@'%' identified by '';")
		for _, sql := range ca.prepares {
			tk.MustExec(sql)
		}
		err := tk2.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost", CurrentUser: true, AuthUsername: "u1", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)
		require.NoError(t, err)
		if ca.err == nil {
			tk2.MustExec(ca.sql)
		} else {
			err = tk2.ExecToErr(ca.sql)
			require.Error(t, err)
		}
		tk.MustQuery("select * from t1 order by id").Check(testkit.Rows(ca.t1Rows...))
		tk.MustQuery("select * from t2 order by id").Check(testkit.Rows(ca.t2Rows...))
	}
}

func TestForeignKeyIssue39732(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_stmt_summary=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")
	tk.MustExec("create user 'u1'@'%' identified by '';")
	tk.MustExec("GRANT ALL PRIVILEGES ON *.* TO 'u1'@'%'")
	err := tk.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost", CurrentUser: true, AuthUsername: "u1", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)
	require.NoError(t, err)
	tk.MustExec("create table t1 (id int key, leader int,  index(leader), foreign key (leader) references t1(id) ON DELETE CASCADE);")
	tk.MustExec("insert into t1 values (1, null), (10, 1), (11, 1), (20, 10)")
	tk.MustExec(`prepare stmt1 from 'delete from t1 where id = ?';`)
	tk.MustExec(`set @a = 1;`)
	tk.MustExec("execute stmt1 using @a;")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows())
}

func TestForeignKeyOnReplaceIntoChildTable(t *testing.T) {
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
		tk.MustExec("replace into t1 (id, a, b) values (1, 1, 1);")
		tk.MustExec("replace into t2 (id, a, b) values (1, 1, 1)")
		tk.MustGetDBError("replace into t1 (id, a, b) values (1, 2, 3);", plannererrors.ErrRowIsReferenced2)
		if !ca.notNull {
			tk.MustExec("replace into t2 (id, a, b) values (2, null, 1)")
			tk.MustExec("replace into t2 (id, a, b) values (3, 1, null)")
			tk.MustExec("replace into t2 (id, a, b) values (4, null, null)")
		}
		tk.MustGetDBError("replace into t2 (id, a, b) values (5, 1, 0);", plannererrors.ErrNoReferencedRow2)
		tk.MustGetDBError("replace into t2 (id, a, b) values (6, 0, 1);", plannererrors.ErrNoReferencedRow2)
		tk.MustGetDBError("replace into t2 (id, a, b) values (7, 2, 2);", plannererrors.ErrNoReferencedRow2)
		// Test replace into from select.
		tk.MustExec("delete from t2")
		tk.MustExec("replace into t2 (id, a, b) select id, a, b from t_data where t_data.id=1")
		tk.MustGetDBError("replace into t2 (id, a, b) select id, a, b from t_data where t_data.id=2", plannererrors.ErrNoReferencedRow2)

		// Test in txn
		tk.MustExec("delete from t2")
		tk.MustExec("begin")
		tk.MustExec("delete from t1 where a=1")
		tk.MustGetDBError("replace into t2 (id, a, b) values (1, 1, 1)", plannererrors.ErrNoReferencedRow2)
		tk.MustExec("replace into t1 (id, a, b) values (2, 2, 2)")
		tk.MustExec("replace into t2 (id, a, b) values (2, 2, 2)")
		tk.MustGetDBError("replace into t1 (id, a, b) values (2, 2, 3);", plannererrors.ErrRowIsReferenced2)
		tk.MustExec("rollback")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("1 1 1"))
		tk.MustQuery("select id, a, b from t2 order by id").Check(testkit.Rows())
	}

	// Case-10: test primary key is handle and contain foreign key column, and foreign key column has default value.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("set @@tidb_enable_clustered_index=0;")
	tk.MustExec("create table t1 (id int,a int, primary key(id));")
	tk.MustExec("create table t2 (id int key,a int not null default 0, index (a), foreign key fk(a) references t1(id));")
	tk.MustExec("replace into t1 values (1, 1);")
	tk.MustExec("replace into t2 values (1, 1);")
	tk.MustGetDBError("replace into t2 (id) values (10);", plannererrors.ErrNoReferencedRow2)
	tk.MustGetDBError("replace into t2 values (3, 2);", plannererrors.ErrNoReferencedRow2)

	// Case-11: test primary key is handle and contain foreign key column, and foreign key column doesn't have default value.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (id int key,a int, index (a), foreign key fk(a) references t1(id));")
	tk.MustExec("replace into t2 values (1, 1);")
	tk.MustExec("replace into t2 (id) values (10);")
	tk.MustGetDBError("replace into t2 values (3, 2);", plannererrors.ErrNoReferencedRow2)
}

func TestForeignKeyLargeTxnErr(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int auto_increment key, pid int, name varchar(200), index(pid));")
	tk.MustExec("insert into t1 (name) values ('abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890');")
	for i := 0; i < 8; i++ {
		tk.MustExec("insert into t1 (name) select name from t1;")
	}
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("256"))
	tk.MustExec("update t1 set pid=1 where id>1")
	tk.MustExec("alter table t1 add foreign key (pid) references t1 (id) on update cascade")
	originLimit := kv.TxnTotalSizeLimit.Load()
	defer func() {
		kv.TxnTotalSizeLimit.Store(originLimit)
	}()
	// Set the limitation to a small value, make it easier to reach the limitation.
	kv.TxnTotalSizeLimit.Store(10240)
	tk.MustQuery("select sum(id) from t1").Check(testkit.Rows("32896"))
	// foreign key cascade behaviour will cause ErrTxnTooLarge.
	tk.MustGetDBError("update t1 set id=id+100000 where id=1", kv.ErrTxnTooLarge)
	tk.MustQuery("select sum(id) from t1").Check(testkit.Rows("32896"))
	tk.MustGetDBError("update t1 set id=id+100000 where id=1", kv.ErrTxnTooLarge)
	tk.MustQuery("select id,pid from t1 where id<3 order by id").Check(testkit.Rows("1 <nil>", "2 1"))
	tk.MustExec("set @@foreign_key_checks=0")
	tk.MustExec("update t1 set id=id+100000 where id=1")
	tk.MustQuery("select id,pid from t1 where id<3 or pid is null order by id").Check(testkit.Rows("2 1", "100001 <nil>"))
}

func TestForeignKeyAndLockView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key)")
	tk.MustExec("create table t2 (id int key, foreign key (id) references t1(id) ON DELETE CASCADE ON UPDATE CASCADE)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("set @@foreign_key_checks=0")
	tk.MustExec("update t2 set id=2")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("set @@foreign_key_checks=1")
	tk2.MustExec("use test")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk2.MustExec("begin pessimistic")
		tk2.MustExec("update t1 set id=2 where id=1")
		tk2.MustExec("commit")
	}()
	time.Sleep(time.Millisecond * 200)
	_, digest := parser.NormalizeDigest("update t1 set id=2 where id=1")
	tk.MustQuery("select CURRENT_SQL_DIGEST from information_schema.tidb_trx where state='LockWaiting' and db='test'").Check(testkit.Rows(digest.String()))
	tk.MustGetErrMsg("update t1 set id=2", "[executor:1213]Deadlock found when trying to get lock; try restarting transaction")
	wg.Wait()
}
