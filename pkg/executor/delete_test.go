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

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
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
			tk1.Session().GetSessionVars().EnableClusteredIndex = vardef.ClusteredIndexDefModeIntOnly
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
}

// TestDeleteWithExistsSubquerySameTableIssue67019 reproduces issue #67019: when a
// subquery in DELETE's WHERE EXISTS references the same table as the delete target,
// that reference must be correlated (current row), not a full table scan. Otherwise
// expanding the subquery (e.g. adding UNION SELECT t3.c5 FROM t3) wrongly changes
// the number of rows deleted.
func TestDeleteWithExistsSubquerySameTableIssue67019(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("DROP DATABASE IF EXISTS repro41_delete")
	tk.MustExec("CREATE DATABASE repro41_delete")
	tk.MustExec("USE repro41_delete")

	tk.MustExec(`CREATE TABLE t1 (
  c1 INT PRIMARY KEY,
  c5 DATE NOT NULL
);`)
	tk.MustExec(`CREATE TABLE t3 (
  c1 INT PRIMARY KEY,
  c2 INT NOT NULL,
  c5 DATETIME NULL
);`)

	tk.MustExec("INSERT INTO t1 VALUES (1, '2022-04-30')")
	tk.MustExec("INSERT INTO t3 VALUES (3, 1, '2019-05-07 15:50:16')")

	// Original: right side of EXCEPT is only t1, so EXCEPT is empty, EXISTS false, delete 0 rows.
	tk.MustExec(`DELETE FROM t3
WHERE c1 = 3
  AND c2 = 1
  AND EXISTS (
    SELECT t1.c5, COALESCE(t1.c5, '2017-10-20 02:16:45'), COALESCE(t1.c5, '2020-08-28 22:54:55')
    FROM t1
    EXCEPT
    SELECT d.k, COALESCE(d.k, '2011-11-27 14:17:47'), COALESCE(d.k, '2010-03-17 16:03:58')
    FROM (
      SELECT t1.c5 AS k FROM t1
    ) AS d
  );`)
	tk.MustQuery("SELECT 'after original' AS tag, COUNT(*) AS t3_rows FROM t3").Check(testkit.Rows("after original 1"))

	tk.MustExec("TRUNCATE TABLE t3")
	tk.MustExec("INSERT INTO t3 VALUES (3, 1, '2019-05-07 15:50:16')")

	// Mutated: same query but d now includes t3.c5. Inner t3 must be correlated (current row).
	// So EXCEPT result is still empty, EXISTS false, delete 0 rows (same as original).
	tk.MustExec(`DELETE FROM t3
WHERE c1 = 3
  AND c2 = 1
  AND EXISTS (
    SELECT t1.c5, COALESCE(t1.c5, '2017-10-20 02:16:45'), COALESCE(t1.c5, '2020-08-28 22:54:55')
    FROM t1
    EXCEPT
    SELECT d.k, COALESCE(d.k, '2011-11-27 14:17:47'), COALESCE(d.k, '2010-03-17 16:03:58')
    FROM (
      SELECT t1.c5 AS k FROM t1
      UNION
      SELECT t3.c5 AS k FROM t3
    ) AS d
  );`)
	tk.MustQuery("SELECT 'after mutated' AS tag, COUNT(*) AS t3_rows FROM t3").Check(testkit.Rows("after mutated 1"))

	tk.MustExec("DROP DATABASE IF EXISTS repro41_delete")
}

// TestDeleteWithExistsSubquerySameTableSimple verifies that DELETE FROM t WHERE EXISTS (SELECT 1 FROM t)
// treats the inner t as the current row, so each row satisfies EXISTS and all rows are deleted.
func TestDeleteWithExistsSubquerySameTableSimple(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values (1, 10), (2, 20), (3, 30)")

	tk.MustExec("DELETE FROM t WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.a = t.a)")
	tk.MustQuery("SELECT COUNT(*) FROM t").Check(testkit.Rows("0"))
}

// TestDeleteWithExistsSubqueryDifferentTable ensures that when the subquery references
// a different table, behavior is unchanged (no regression).
func TestDeleteWithExistsSubqueryDifferentTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int primary key)")
	tk.MustExec("create table t2(a int primary key)")
	tk.MustExec("insert into t1 values (1), (2)")
	tk.MustExec("insert into t2 values (1)")

	tk.MustExec("DELETE FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.a = t1.a)")
	tk.MustQuery("SELECT * FROM t1").Check(testkit.Rows("2"))
}
