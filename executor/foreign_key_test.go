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
	"testing"

	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestForeignKeyOnInsertChildTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")

	// Case-1: test unique index only contain foreign key columns.
	cases := []struct {
		sql string
		err *terror.Error
	}{
		{sql: "create table t1 (id int key,a int, b int, unique index(a, b));"},
		{sql: "create table t2 (id int key,a int, b int, index (a,b), foreign key fk(a, b) references t1(a, b));"},
		{sql: "insert into t1 values (-1, 1, 1);"},
		{sql: "insert into t2 values (1, 1, 1);"},
		{sql: "insert into t2 values (2, null, 1);"},
		{sql: "insert into t2 values (3, 1, null);"},
		{sql: "insert into t2 values (4, null, null);"},
		{sql: "insert into t2 (id, a) values (10, 1);"},
		{sql: "insert into t2 values (5, 1, 2);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (6, 0, 1);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (7, 2, 2);", err: plannercore.ErrNoReferencedRow2},
	}
	checkCaseFn := func() {
		for _, ca := range cases {
			if ca.err == nil {
				tk.MustExec(ca.sql)
			} else {
				err := tk.ExecToErr(ca.sql)
				msg := fmt.Sprintf("sql: %v, err: %v, expected_err: %v", ca.sql, err, ca.err)
				require.NotNil(t, err, msg)
				require.True(t, ca.err.Equal(err), msg)
			}
		}
	}
	checkCaseFn()

	// Case-2: test unique index contain foreign key columns and other columns.
	cases = []struct {
		sql string
		err *terror.Error
	}{
		{sql: "drop table if exists t2;"},
		{sql: "drop table if exists t1;"},
		{sql: "create table t1 (id int key,a int, b int, unique index(a, b, id));"},
		{sql: "create table t2 (id int key,a int, b int, index (a,b,id), foreign key fk(a, b) references t1(a, b));"},
		{sql: "insert into t1 values (-1, 1, 1);"},
		{sql: "insert into t2 values (1, 1, 1);"},
		{sql: "insert into t2 values (2, null, 1);"},
		{sql: "insert into t2 values (3, 1, null);"},
		{sql: "insert into t2 values (4, null, null);"},
		{sql: "insert into t2 (id, a) values (10, 1);"},
		{sql: "insert into t2 values (5, 1, 2);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (6, 0, 1);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (7, 2, 2);", err: plannercore.ErrNoReferencedRow2},
	}
	checkCaseFn()

	// Case-3: test non-unique index only contain foreign key columns.
	cases = []struct {
		sql string
		err *terror.Error
	}{
		{sql: "drop table if exists t2;"},
		{sql: "drop table if exists t1;"},
		{sql: "create table t1 (id int key,a int, b int, index(a, b));"},
		{sql: "create table t2 (id int key,a int, b int, index (a,b), foreign key fk(a, b) references t1(a, b));"},
		{sql: "insert into t1 values (-1, 1, 1);"},
		{sql: "insert into t2 values (1, 1, 1);"},
		{sql: "insert into t2 values (2, null, 1);"},
		{sql: "insert into t2 values (3, 1, null);"},
		{sql: "insert into t2 values (4, null, null);"},
		{sql: "insert into t2 (id, a) values (10, 1);"},
		{sql: "insert into t2 values (5, 1, 2);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (6, 0, 1);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (7, 2, 2);", err: plannercore.ErrNoReferencedRow2},
	}
	checkCaseFn()

	// Case-4: test non-unique index contain foreign key columns and other columns.
	cases = []struct {
		sql string
		err *terror.Error
	}{
		{sql: "drop table if exists t2;"},
		{sql: "drop table if exists t1;"},
		{sql: "create table t1 (id int key,a int, b int, index(a, b, id));"},
		{sql: "create table t2 (id int key,a int, b int, index (a,b,id), foreign key fk(a, b) references t1(a, b));"},
		{sql: "insert into t1 values (-1, 1, 1);"},
		{sql: "insert into t2 values (1, 1, 1);"},
		{sql: "insert into t2 values (2, null, 1);"},
		{sql: "insert into t2 values (3, 1, null);"},
		{sql: "insert into t2 values (4, null, null);"},
		{sql: "insert into t2 (id, a) values (10, 1);"},
		{sql: "insert into t2 values (5, 1, 2);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (6, 0, 1);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (7, 2, 2);", err: plannercore.ErrNoReferencedRow2},
	}
	checkCaseFn()

	// Case-5: test primary key only contain foreign key columns, and disable tidb_enable_clustered_index.
	cases = []struct {
		sql string
		err *terror.Error
	}{
		{sql: "set @@tidb_enable_clustered_index=0;"},
		{sql: "drop table if exists t2;"},
		{sql: "drop table if exists t1;"},
		{sql: "create table t1 (id int,a int, b int, primary key(a, b));"},
		{sql: "create table t2 (id int key,a int, b int, index (a,b), foreign key fk(a, b) references t1(a, b));"},
		{sql: "insert into t1 values (-1, 1, 1);"},
		{sql: "insert into t2 values (1, 1, 1);"},
		{sql: "insert into t2 values (2, null, 1);"},
		{sql: "insert into t2 values (3, 1, null);"},
		{sql: "insert into t2 values (4, null, null);"},
		{sql: "insert into t2 (id, a) values (10, 1);"},
		{sql: "insert into t2 values (5, 1, 2);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (6, 0, 1);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (7, 2, 2);", err: plannercore.ErrNoReferencedRow2},
	}
	checkCaseFn()

	// Case-6: test primary key only contain foreign key columns, and enable tidb_enable_clustered_index.
	cases = []struct {
		sql string
		err *terror.Error
	}{
		{sql: "set @@tidb_enable_clustered_index=1;"},
		{sql: "drop table if exists t2;"},
		{sql: "drop table if exists t1;"},
		{sql: "create table t1 (id int,a int, b int, primary key(a, b));"},
		{sql: "create table t2 (id int key,a int, b int, index (a,b), foreign key fk(a, b) references t1(a, b));"},
		{sql: "insert into t1 values (-1, 1, 1);"},
		{sql: "insert into t2 values (1, 1, 1);"},
		{sql: "insert into t2 values (2, null, 1);"},
		{sql: "insert into t2 values (3, 1, null);"},
		{sql: "insert into t2 values (4, null, null);"},
		{sql: "insert into t2 (id, a) values (10, 1);"},
		{sql: "insert into t2 values (5, 1, 2);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (6, 0, 1);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (7, 2, 2);", err: plannercore.ErrNoReferencedRow2},
	}
	checkCaseFn()

	// Case-7: test primary key contain foreign key columns and other column, and disable tidb_enable_clustered_index.
	cases = []struct {
		sql string
		err *terror.Error
	}{
		{sql: "set @@tidb_enable_clustered_index=0;"},
		{sql: "drop table if exists t2;"},
		{sql: "drop table if exists t1;"},
		{sql: "create table t1 (id int,a int, b int, primary key(a, b, id));"},
		{sql: "create table t2 (id int key,a int, b int, index (a,b), foreign key fk(a, b) references t1(a, b));"},
		{sql: "insert into t1 values (-1, 1, 1);"},
		{sql: "insert into t2 values (1, 1, 1);"},
		{sql: "insert into t2 values (2, null, 1);"},
		{sql: "insert into t2 values (3, 1, null);"},
		{sql: "insert into t2 values (4, null, null);"},
		{sql: "insert into t2 (id, a) values (10, 1);"},
		{sql: "insert into t2 values (5, 1, 2);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (6, 0, 1);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (7, 2, 2);", err: plannercore.ErrNoReferencedRow2},
	}
	checkCaseFn()

	// Case-8: test primary key contain foreign key columns and other column, and enable tidb_enable_clustered_index.
	cases = []struct {
		sql string
		err *terror.Error
	}{
		{sql: "set @@tidb_enable_clustered_index=1;"},
		{sql: "drop table if exists t2;"},
		{sql: "drop table if exists t1;"},
		{sql: "create table t1 (id int,a int, b int, primary key(a, b, id));"},
		{sql: "create table t2 (id int key,a int, b int, index (a,b), foreign key fk(a, b) references t1(a, b));"},
		{sql: "insert into t1 values (-1, 1, 1);"},
		{sql: "insert into t2 values (1, 1, 1);"},
		{sql: "insert into t2 values (2, null, 1);"},
		{sql: "insert into t2 values (3, 1, null);"},
		{sql: "insert into t2 values (4, null, null);"},
		{sql: "insert into t2 (id, a) values (10, 1);"},
		{sql: "insert into t2 values (5, 1, 2);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (6, 0, 1);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (7, 2, 2);", err: plannercore.ErrNoReferencedRow2},
	}
	checkCaseFn()

	// Case-9: test primary key is handle and contain foreign key column.
	cases = []struct {
		sql string
		err *terror.Error
	}{
		{sql: "set @@tidb_enable_clustered_index=0;"},
		{sql: "drop table if exists t2;"},
		{sql: "drop table if exists t1;"},
		{sql: "create table t1 (id int,a int, primary key(id));"},
		{sql: "create table t2 (id int key,a int, index (a), foreign key fk(a) references t1(id));"},
		{sql: "insert into t1 values (1, 1);"},
		{sql: "insert into t2 values (1, 1);"},
		{sql: "insert into t2 values (2, null);"},
		{sql: "insert into t2 (id) values (10);"},
		{sql: "insert into t2 values (3, 2);", err: plannercore.ErrNoReferencedRow2},
	}
	checkCaseFn()

	// Case-10: test primary key is handle and contain foreign key column.
	cases = []struct {
		sql string
		err *terror.Error
	}{
		{sql: "set @@tidb_enable_clustered_index=0;"},
		{sql: "drop table if exists t2;"},
		{sql: "drop table if exists t1;"},
		{sql: "create table t1 (id int,a int, primary key(id));"},
		{sql: "create table t2 (id int key,a int not null default 0, index (a), foreign key fk(a) references t1(id));"},
		{sql: "insert into t1 values (1, 1);"},
		{sql: "insert into t2 values (1, 1);"},
		{sql: "insert into t2 (id) values (10);", err: plannercore.ErrNoReferencedRow2},
		{sql: "insert into t2 values (3, 2);", err: plannercore.ErrNoReferencedRow2},
	}
	checkCaseFn()
}

func TestForeignKeyOnInsertChildTableInTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@tidb_enable_foreign_key=1")
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

		// Case-5: test primary key only contain foreign key columns, and disable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=0;",
				"create table t1 (id int, a int, b int,  primary key (a, b));",
				"create table t2 (b int,  a int, id int, primary key (a, b), foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);",
			},
		},
		// Case-6: test primary key only contain foreign key columns, and enable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=1;",
				"create table t1 (id int, a int, b int,  primary key (a, b));",
				"create table t2 (b int,  a int, id int, primary key (a, b), foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);",
			},
		},
		// Case-7: test primary key contain foreign key columns and other column, and disable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=0;",
				"create table t1 (id int, a int, b int,  primary key (a, b, id));",
				"create table t2 (b int,  a int, id int, primary key (a, b, id), foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);",
			},
		},
		// Case-8: test primary key contain foreign key columns and other column, and enable tidb_enable_clustered_index.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=1;",
				"create table t1 (id int, a int, b int,  primary key (a, b, id));",
				"create table t2 (b int,  a int, id int, primary key (a, b, id), foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);",
			},
		},
		// Case-9: test primary key is handle and contain foreign key column.
		{
			prepareSQLs: []string{
				"set @@tidb_enable_clustered_index=0;",
				"create table t1 (id int, a int, b int,  primary key (id));",
				"create table t2 (b int,  a int, id int, primary key (a), foreign key fk(a) references t1(id) ON DELETE CASCADE);",
			},
		},
	}

	for _, ca := range cases {
		tk.MustExec("drop table if exists t2;")
		tk.MustExec("drop table if exists t1;")
		for _, sql := range ca.prepareSQLs {
			tk.MustExec(sql)
		}
		tk.MustExec("insert into t1 (id, a, b) values (-1, 1, 1);")
		tk.MustExec("begin")
		tk.MustExec("delete from t1 where a=1")
		err := tk.ExecToErr("insert into t2 (id, a, b) values (1, 1, 1)")
		require.NotNil(t, err)
		require.True(t, plannercore.ErrNoReferencedRow2.Equal(err), err.Error())
		tk.MustExec("insert into t1 (id, a, b) values (2, 2, 2)")
		tk.MustExec("insert into t2 (id, a, b) values (2, 2, 2)")
		tk.MustExec("rollback")
		tk.MustQuery("select id, a, b from t1 order by id").Check(testkit.Rows("-1 1 1"))
		tk.MustQuery("select id, a, b from t2 order by id").Check(testkit.Rows())
	}
}
