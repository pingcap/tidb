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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestDeleteLockKey(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t1, t2, t3, t4, t5, t6;`)

	cases := []struct {
		ddl     string
		pre     string
		tk1Stmt string
		tk2Stmt string
	}{
		{
			"create table t1(k int, kk int, val int, primary key(k, kk), unique key(val))",
			"insert into t1 values(1, 2, 3)",
			"delete from t1 where val = 3",
			"insert into t1 values(1, 3, 3)",
		},
		{
			"create table t2(k int, kk int, val int, primary key(k, kk))",
			"insert into t2 values(1, 1, 1)",
			"delete from t2 where k = 1",
			"insert into t2 values(1, 1, 2)",
		},
		{
			"create table t3(k int, kk int, val int, vv int, primary key(k, kk), unique key(val))",
			"insert into t3 values(1, 2, 3, 4)",
			"delete from t3 where vv = 4",
			"insert into t3 values(1, 2, 3, 5)",
		},
		{
			"create table t4(k int, kk int, val int, vv int, primary key(k, kk), unique key(val))",
			"insert into t4 values(1, 2, 3, 4)",
			"delete from t4 where 1",
			"insert into t4 values(1, 2, 3, 5)",
		},
		{
			"create table t5(k int, kk int, val int, vv int, primary key(k, kk), unique key(val))",
			"insert into t5 values(1, 2, 3, 4), (2, 3, 4, 5)",
			"delete from t5 where k in (1, 2, 3, 4)",
			"insert into t5 values(1, 2, 3, 5)",
		},
		{
			"create table t6(k int, kk int, val int, vv int, primary key(k, kk), unique key(val))",
			"insert into t6 values(1, 2, 3, 4), (2, 3, 4, 5)",
			"delete from t6 where kk between 0 and 10",
			"insert into t6 values(1, 2, 3, 5), (2, 3, 4, 6)",
		},
	}
	var wg sync.WaitGroup
	for _, testCase := range cases {
		wg.Add(1)
		go func(testCase struct {
			ddl     string
			pre     string
			tk1Stmt string
			tk2Stmt string
		}) {
			tk1, tk2 := testkit.NewTestKit(t, store), testkit.NewTestKit(t, store)
			tk1.MustExec("use test")
			tk2.MustExec("use test")
			tk1.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
			tk1.MustExec(testCase.ddl)
			tk1.MustExec(testCase.pre)
			tk1.MustExec("begin pessimistic")
			tk2.MustExec("begin pessimistic")
			tk1.MustExec(testCase.tk1Stmt)
			doneCh := make(chan struct{}, 1)
			go func() {
				tk2.MustExec(testCase.tk2Stmt)
				doneCh <- struct{}{}
			}()
			time.Sleep(50 * time.Millisecond)
			tk1.MustExec("commit")
			<-doneCh
			tk2.MustExec("commit")
			wg.Done()
		}(testCase)
	}
	wg.Wait()
}

func TestDeleteIgnoreWithFK(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table parent (a int primary key)")
	tk.MustExec("create table child (a int, foreign key (a) references parent(a))")

	tk.MustExec("insert into parent values (1), (2)")
	tk.MustExec("insert into child values (1)")

	// Delete the row in parent table will fail
	require.NotNil(t, tk.ExecToErr("delete from parent where a = 1"))

	// Delete ignore will return no error
	tk.MustExec("delete ignore from parent where a = 1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1451 Cannot delete or update a parent row: a foreign key constraint fails (`test`.`child`, CONSTRAINT `fk_1` FOREIGN KEY (`a`) REFERENCES `parent` (`a`))"))

	// Other rows will be deleted successfully
	tk.MustExec("delete ignore from parent")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1451 Cannot delete or update a parent row: a foreign key constraint fails (`test`.`child`, CONSTRAINT `fk_1` FOREIGN KEY (`a`) REFERENCES `parent` (`a`))"))
	tk.MustQuery("select * from parent").Check(testkit.Rows("1"))

	tk.MustExec("insert into parent values (2)")
	// Delete multiple tables
	tk.MustExec("create table parent2 (a int primary key)")
	tk.MustExec("create table child2 (a int, foreign key (a) references parent2(a))")
	tk.MustExec("insert into parent2 values (1), (2)")
	tk.MustExec("insert into child2 values (1)")
	require.NotNil(t, tk.ExecToErr("delete from parent, parent2 using parent inner join parent2 where parent.a = parent2.a"))
	tk.MustExec("delete ignore from parent, parent2 using parent inner join parent2 where parent.a = parent2.a")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Warning 1451 Cannot delete or update a parent row: a foreign key constraint fails (`test`.`child2`, CONSTRAINT `fk_1` FOREIGN KEY (`a`) REFERENCES `parent2` (`a`))",
		"Warning 1451 Cannot delete or update a parent row: a foreign key constraint fails (`test`.`child`, CONSTRAINT `fk_1` FOREIGN KEY (`a`) REFERENCES `parent` (`a`))"))
	tk.MustQuery("select * from parent").Check(testkit.Rows("1"))
	tk.MustQuery("select * from parent2").Check(testkit.Rows("1"))

	// Test batch on delete
	require.NotNil(t, tk.ExecToErr("batch on `a` limit 1000 delete from parent where a = 1"))
	tk.MustExec("batch on `a` limit 1000 delete ignore from parent where a = 1")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1451 Cannot delete or update a parent row: a foreign key constraint fails (`test`.`child`, CONSTRAINT `fk_1` FOREIGN KEY (`a`) REFERENCES `parent` (`a`))"))

	cases := []struct {
		name         string
		ddl          string
		seed         string
		sql          string
		rows         []string
		handleRows   []string
		affectedRows uint64
		warningCount int
		multiTable   bool
	}{
		{
			name:         "normal self delete",
			ddl:          "create table t_self_delete (id int primary key clustered, pid int, foreign key (pid) references t_self_delete(id))",
			seed:         "insert into t_self_delete values (1, 1)",
			sql:          "delete from t_self_delete where id=1",
			affectedRows: 1,
		},
		{
			name:         "ignore self delete",
			ddl:          "create table t_self_delete (id int primary key clustered, pid int, foreign key (pid) references t_self_delete(id))",
			seed:         "insert into t_self_delete values (1, 1)",
			sql:          "delete ignore from t_self_delete where id=1",
			affectedRows: 1,
		},
		{
			name:         "retains another child",
			ddl:          "create table t_self_delete (id int primary key clustered, pid int, foreign key (pid) references t_self_delete(id))",
			seed:         "insert into t_self_delete values (1, 1), (3, 1)",
			sql:          "delete ignore from t_self_delete where id=1",
			rows:         []string{"1 1", "3 1"},
			warningCount: 1,
		},
		{
			name:         "multi table self delete",
			ddl:          "create table t_self_delete (id int primary key clustered, pid int, foreign key (pid) references t_self_delete(id))",
			seed:         "insert into t_self_delete values (1, 1)",
			sql:          "delete ignore d, s from t_self_delete d inner join t_delete_selector s on d.id=s.id",
			affectedRows: 2,
			multiTable:   true,
		},
		{
			name:         "multi table retains another child",
			ddl:          "create table t_self_delete (id int primary key clustered, pid int, foreign key (pid) references t_self_delete(id))",
			seed:         "insert into t_self_delete values (1, 1), (3, 1)",
			sql:          "delete ignore d, s from t_self_delete d inner join t_delete_selector s on d.id=s.id",
			rows:         []string{"1 1", "3 1"},
			affectedRows: 1,
			warningCount: 1,
			multiTable:   true,
		},
		{
			name:         "hidden handle self delete",
			ddl:          "create table t_self_delete (id int unique, pid int unique, foreign key (pid) references t_self_delete(id))",
			seed:         "insert into t_self_delete values (100, 100)",
			sql:          "delete ignore from t_self_delete where id=100",
			handleRows:   []string{"1 100"},
			affectedRows: 1,
		},
		{
			name:         "hidden handle retains different row",
			ddl:          "create table t_self_delete (id int unique, pid int unique, foreign key (pid) references t_self_delete(id))",
			seed:         "insert into t_self_delete values (2, null), (100, 2)",
			sql:          "delete ignore from t_self_delete where id=2",
			rows:         []string{"100 2", "2 <nil>"},
			handleRows:   []string{"1 2", "2 100"},
			warningCount: 1,
		},
		{
			name:         "common handle self delete",
			ddl:          "create table t_self_delete (id varchar(10), seq int, pid varchar(10), pseq int, primary key (id, seq) clustered, foreign key (pid, pseq) references t_self_delete(id, seq))",
			seed:         "insert into t_self_delete values ('self', 10, 'self', 10)",
			sql:          "delete ignore from t_self_delete where id='self' and seq=10",
			affectedRows: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			t.Cleanup(func() {
				tk.MustExec("rollback")
				tk.MustExec("set foreign_key_checks=0")
				tk.MustExec("drop table if exists t_self_delete, t_delete_selector")
			})
			tk.MustExec(tc.ddl)
			tk.MustExec(tc.seed)
			if tc.multiTable {
				tk.MustExec("create table t_delete_selector (id int primary key)")
				tk.MustExec("insert into t_delete_selector values (1)")
			}
			if len(tc.handleRows) > 0 {
				tk.MustQuery("select _tidb_rowid, id from t_self_delete order by _tidb_rowid").Check(testkit.Rows(tc.handleRows...))
			}
			tk.MustExec(tc.sql)
			require.Equal(t, tc.affectedRows, tk.Session().GetSessionVars().StmtCtx.AffectedRows())
			warnings := tk.MustQuery("show warnings").Rows()
			require.Len(t, warnings, tc.warningCount)
			for _, warning := range warnings {
				require.Equal(t, "Warning", warning[0])
				require.Equal(t, "1451", warning[1])
			}
			tk.MustQuery("select * from t_self_delete").Sort().Check(testkit.Rows(tc.rows...))
			tk.MustExec("admin check table t_self_delete")
			if tc.multiTable {
				tk.MustQuery("select * from t_delete_selector").Check(testkit.Rows())
				tk.MustExec("admin check table t_delete_selector")
			}
		})
	}
}
