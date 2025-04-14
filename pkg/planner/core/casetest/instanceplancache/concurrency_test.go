// Copyright 2024 PingCAP, Inc.
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

package instanceplancache

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

/*
The methodology of these testing cases::
1. Generate some SQLs including DQL and DML;
2. Convert these SQLs to prepared-format, for example,
    convert 'select * from t where a='1 to "prepare st from 'select a from t where a=?'; set @a=1; execute st using @a"
3. Prepare N sessions to run these SQLs to simulate the same SQL plan being shared across different sessions at the same time;
4. Prepare 2 databases, one is used to run the original SQLs, one is used to run all prepared SQLs;
5. For each session and each SQL:
  1. Start a transaction;
  2. Run the original SQL on DB1;
  3. Run the prepared SQL on DB2;
  4. Check whether it can get the same result on DB1 and DB2;
  5. Commit the transaction;
6. Do step 1~5 again.
*/

type testStmt struct {
	normalStmt string
	prepStmt   string
	setStmt    string
	execStmt   string
}

func isDQL(stmt string) bool {
	return strings.HasPrefix(stmt, "select")
}

func isDML(stmt string) bool {
	return strings.HasPrefix(stmt, "insert") ||
		strings.HasPrefix(stmt, "update") ||
		strings.HasPrefix(stmt, "delete")
}

func isTxn(stmt string) bool {
	return strings.HasPrefix(stmt, "begin") ||
		strings.HasPrefix(stmt, "commit") ||
		strings.HasPrefix(stmt, "rollback")
}

type worker struct {
	tk    *testkit.TestKit
	stmts []*testStmt
	wg    *sync.WaitGroup
}

func (w *worker) run() {
	defer w.wg.Done()
	for _, stmt := range w.stmts {
		if isTxn(stmt.normalStmt) {
			w.tk.MustExec(stmt.normalStmt) // txn stmt
		} else if isDQL(stmt.normalStmt) { // DQL
			normalResult := w.tk.MustQuery(stmt.normalStmt)
			w.tk.MustExec(stmt.prepStmt)
			w.tk.MustExec(stmt.setStmt)
			preparedResult := w.tk.MustQuery(stmt.execStmt)
			normalResult.Sort().Check(preparedResult.Sort().Rows())
		} else if isDML(stmt.normalStmt) { // DML
			w.tk.MustExec(stmt.normalStmt)
			w.tk.MustExec(stmt.prepStmt)
			w.tk.MustExec(stmt.setStmt)
			w.tk.MustExec(stmt.execStmt)
		}
	}
}

func testWithWorkers(TKs []*testkit.TestKit, stmts []*testStmt) {
	nStmts := make([][]*testStmt, len(TKs))
	for _, stmt := range stmts {
		if isDML(stmt.normalStmt) { // avoid duplicate DML
			x := rand.Intn(len(TKs))
			nStmts[x] = append(nStmts[x], stmt)
		} else {
			for i := range TKs {
				nStmts[i] = append(nStmts[i], stmt)
			}
		}
	}

	var wg sync.WaitGroup
	for i, tk := range TKs {
		w := worker{tk: tk, stmts: nStmts[i], wg: &wg}
		wg.Add(1)
		go w.run()
	}
	wg.Wait()
}

func TestInstancePlanCacheConcurrencySysbench(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database normal`)
	tk.MustExec(`create database prepared`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	for _, db := range []string{"normal", "prepared"} {
		tk.MustExec("use " + db)
		tk.MustExec(`CREATE TABLE sbtest (
	 id int(10) unsigned NOT NULL auto_increment,
	 k int(10) unsigned NOT NULL default '0',
	 c char(120) NOT NULL default '',
	 PRIMARY KEY (id), KEY k (k))`)
	}

	maxID := 1
	genSelect := func() *testStmt {
		switch rand.Intn(5) {
		case 0: //select c from sbtest where id=?;
			id := rand.Intn(maxID)
			return &testStmt{
				normalStmt: fmt.Sprintf("select c from normal.sbtest where id=%v", id),
				prepStmt:   "prepare st from 'select c from prepared.sbtest where id=?'",
				setStmt:    fmt.Sprintf("set @id = %v", id),
				execStmt:   "execute st using @id",
			}
		case 1: // select c from sbtest where id between ? and ?
			id1 := rand.Intn(maxID)
			id2 := rand.Intn(maxID)
			if id1 > id2 {
				id1, id2 = id2, id1
			}
			return &testStmt{
				normalStmt: fmt.Sprintf("select c from normal.sbtest where id between %v and %v", id1, id2),
				prepStmt:   "prepare st from 'select c from prepared.sbtest where id between ? and ?'",
				setStmt:    fmt.Sprintf("set @id1 = %v, @id2 = %v", id1, id2),
				execStmt:   "execute st using @id1, @id2",
			}
		case 2: // select sum(k) from sbtest where id between ? and ?
			id1 := rand.Intn(maxID)
			id2 := rand.Intn(maxID)
			if id1 > id2 {
				id1, id2 = id2, id1
			}
			return &testStmt{
				normalStmt: fmt.Sprintf("select sum(k) from normal.sbtest where id between %v and %v", id1, id2),
				prepStmt:   "prepare st from 'select sum(k) from prepared.sbtest where id between ? and ?'",
				setStmt:    fmt.Sprintf("set @id1 = %v, @id2 = %v", id1, id2),
				execStmt:   "execute st using @id1, @id2",
			}
		case 3: // select c from sbtest where id between ? and ? order by c
			id1 := rand.Intn(maxID)
			id2 := rand.Intn(maxID)
			if id1 > id2 {
				id1, id2 = id2, id1
			}
			return &testStmt{
				normalStmt: fmt.Sprintf("select c from normal.sbtest where id between %v and %v order by c", id1, id2),
				prepStmt:   "prepare st from 'select c from prepared.sbtest where id between ? and ? order by c'",
				setStmt:    fmt.Sprintf("set @id1 = %v, @id2 = %v", id1, id2),
				execStmt:   "execute st using @id1, @id2",
			}
		default: // select distinct c from sbtest where id between ? and ? order by c
			id1 := rand.Intn(maxID)
			id2 := rand.Intn(maxID)
			if id1 > id2 {
				id1, id2 = id2, id1
			}
			return &testStmt{
				normalStmt: fmt.Sprintf("select distinct c from normal.sbtest where id between %v and %v order by c", id1, id2),
				prepStmt:   "prepare st from 'select distinct c from prepared.sbtest where id between ? and ? order by c'",
				setStmt:    fmt.Sprintf("set @id1 = %v, @id2 = %v", id1, id2),
				execStmt:   "execute st using @id1, @id2",
			}
		}
	}

	txnLeastID := 1 // used to let update/delete id keep increasing in a txn to avoid deadlock.
	genUpdate := func() *testStmt {
		switch rand.Intn(2) {
		case 0: // update sbtest set k=k+1 where id=?
			id := txnLeastID + rand.Intn(maxID-txnLeastID+1)
			txnLeastID = id
			return &testStmt{
				normalStmt: fmt.Sprintf("update normal.sbtest set k=k+1 where id=%v", id),
				prepStmt:   "prepare st from 'update prepared.sbtest set k=k+1 where id=?'",
				setStmt:    fmt.Sprintf("set @id = %v", id),
				execStmt:   "execute st using @id",
			}
		default: // update sbtest set c=? where id=?
			id := txnLeastID + rand.Intn(maxID-txnLeastID+1)
			txnLeastID = id
			c := fmt.Sprintf("%v", rand.Intn(10000))
			return &testStmt{
				normalStmt: fmt.Sprintf("update normal.sbtest set c='%v' where id=%v", c, id),
				prepStmt:   "prepare st from 'update prepared.sbtest set c=? where id=?'",
				setStmt:    fmt.Sprintf("set @c = '%v', @id = %v", c, id),
				execStmt:   "execute st using @c, @id",
			}
		}
	}
	genInsert := func() *testStmt {
		id := maxID
		maxID += 1
		k := rand.Intn(10000)
		c := fmt.Sprintf("%v", rand.Intn(10000))
		return &testStmt{
			normalStmt: fmt.Sprintf("insert into normal.sbtest values (%v, %v, '%v')", id, k, c),
			prepStmt:   "prepare st from 'insert into prepared.sbtest values (?, ?, ?)'",
			setStmt:    fmt.Sprintf("set @id = %v, @k = %v, @c = '%v'", id, k, c),
			execStmt:   "execute st using @id, @k, @c",
		}
	}
	genDelete := func() *testStmt {
		id := txnLeastID + rand.Intn(maxID-txnLeastID+1)
		txnLeastID = id
		return &testStmt{
			normalStmt: fmt.Sprintf("delete from normal.sbtest where id=%v", id),
			prepStmt:   "prepare st from 'delete from prepared.sbtest where id=?'",
			setStmt:    fmt.Sprintf("set @id = %v", id),
			execStmt:   "execute st using @id",
		}
	}

	nStmt := 2000
	nInitialRecords := 100
	stmts := make([]*testStmt, 0, nStmt)
	stmts = append(stmts, &testStmt{normalStmt: "begin"})
	for len(stmts) < nStmt {
		if rand.Intn(15) == 0 { // start a new txn
			stmts = append(stmts, &testStmt{normalStmt: "commit"})
			stmts = append(stmts, &testStmt{normalStmt: "begin"})
			txnLeastID = 1
			continue
		}
		if len(stmts) < nInitialRecords {
			stmts = append(stmts, genInsert())
			continue
		}
		x := rand.Intn(100)
		if x < 50 { // 50% DQL
			stmts = append(stmts, genSelect())
		} else if x < 75 { // 25% Update
			stmts = append(stmts, genUpdate())
		} else if x < 90 { // 15% Insert
			stmts = append(stmts, genInsert())
		} else {
			stmts = append(stmts, genDelete())
		}
	}
	stmts = append(stmts, &testStmt{normalStmt: "commit"})

	nConcurrency := 10
	TKs := make([]*testkit.TestKit, nConcurrency)
	for i := range TKs {
		TKs[i] = testkit.NewTestKit(t, store)
	}

	testWithWorkers(TKs, stmts)
}

func TestInstancePlanCacheIndexJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int)`)
	tk.MustExec(`create table t2 (a int, key(a))`)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%v, %v)", i, i))
		tk.MustExec(fmt.Sprintf("insert into t2 values (%v)", i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tki := testkit.NewTestKit(t, store)
			tki.MustExec(`use test`)
			tki.MustExec(`prepare st from 'select /*+ tidb_inlj(t2) */ t2.a from t1, t2 where t1.a=t2.a and t1.b=?'`)
			for k := 0; k < 100; k++ {
				v := rand.Intn(100)
				tki.MustExec("set @v = ?", v)
				tki.MustQuery("execute st using @v").Check(testkit.Rows(fmt.Sprintf("%v", v)))
			}
		}()
	}
	wg.Wait()
}

func TestInstancePlanCacheTableIndexScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, primary key(a), key(b))`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%v, %v)", i, i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tki := testkit.NewTestKit(t, store)
			tki.MustExec(`use test`)

			for k := 0; k < 100; k++ {
				if rand.Intn(2) == 0 { // table scan
					tki.MustExec(`prepare st from 'select a from t use index(primary) where a>=? and a<=?'`)
				} else { // index scan
					tki.MustExec(`prepare st from 'select b from t use index(b) where b>=? and b<=?'`)
				}
				v1, v2 := rand.Intn(50), rand.Intn(50)+50
				expected := make([]string, 0, v2-v1)
				for v := v1; v <= v2; v++ {
					expected = append(expected, fmt.Sprintf("%v", v))
				}
				sort.Strings(expected)

				tki.MustExec(`set @v1 = ?, @v2 = ?`, v1, v2)
				tki.MustQuery("execute st using @v1, @v2").Sort().Check(testkit.Rows(expected...))
			}
		}()
	}
	wg.Wait()
}

func TestInstancePlanCacheConcurrencyPointPartitioning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, primary key(a)) partition by hash(a) partitions 10`)
	tk.MustExec(`create table t2 (a int, primary key(a)) partition by range(a) (
    		partition p0 values less than (10),
    		partition p1 values less than (20),
    		partition p2 values less than (30),
    		partition p3 values less than (40),
    		partition p4 values less than (50),
    		partition p5 values less than (60),
    		partition p6 values less than (70),
    		partition p7 values less than (80),
    		partition p8 values less than (90),
    		partition p9 values less than (100))`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%v)", i))
		tk.MustExec(fmt.Sprintf("insert into t2 values (%v)", i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tki := testkit.NewTestKit(t, store)
			tki.MustExec(`use test`)
			for k := 0; k < 100; k++ {
				tName := fmt.Sprintf("t%v", rand.Intn(2)+1)
				tki.MustExec(fmt.Sprintf("prepare st from 'select * from %v where a=?'", tName))
				a := rand.Intn(100)
				tki.MustExec("set @a = ?", a)
				tki.MustQuery("execute st using @a").Check(testkit.Rows(fmt.Sprintf("%v", a)))
			}
		}()
	}
	wg.Wait()
}

func TestInstancePlanCacheConcurrencyPointMultipleColPKNoTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, primary key(a, b))`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%v, %v)", i, i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tki := testkit.NewTestKit(t, store)
			tki.MustExec(`use test`)
			tki.MustExec(`prepare st from 'select * from t where a=? and b=?'`)
			for k := 0; k < 100; k++ {
				a := rand.Intn(100)
				tki.MustExec("set @a = ?, @b = ?", a, a)
				tki.MustQuery("execute st using @a, @b").Check(testkit.Rows(fmt.Sprintf("%v %v", a, a)))
			}
		}()
	}
	wg.Wait()
}

func TestInstancePlanCacheConcurrencyPointNoTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, primary key(a))`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%v, %v)", i, i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tki := testkit.NewTestKit(t, store)
			tki.MustExec(`use test`)
			tki.MustExec(`prepare st from 'select * from t where a=?'`)
			for k := 0; k < 100; k++ {
				a := rand.Intn(100)
				tki.MustExec("set @a = ?", a)
				tki.MustQuery("execute st using @a").Check(testkit.Rows(fmt.Sprintf("%v %v", a, a)))
			}
		}()
	}
	wg.Wait()
}

func TestInstancePlanCacheBatchPointMultiColIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, primary key(a, b), unique key(c, d))`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%v, %v, %v, %v)", i, i, i, i))
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tki := testkit.NewTestKit(t, store)
			tki.MustExec(`use test`)
			if rand.Intn(2) == 0 {
				tki.MustExec(`prepare st from 'select a from t where (a, b) in ((?, ?), (?, ?))'`)
			} else {
				tki.MustExec(`prepare st from 'select a from t where (c, d) in ((?, ?), (?, ?))'`)
			}
			for k := 0; k < 100; k++ {
				a1, a2 := rand.Intn(50), 50+rand.Intn(50)
				tki.MustExec("set @a1 = ?, @a2 = ?", a1, a2)
				v1, v2 := fmt.Sprintf("%v", a1), fmt.Sprintf("%v", a2)
				if v1 > v2 {
					v1, v2 = v2, v1
				}
				tki.MustQuery("execute st using @a1, @a1, @a2, @a2").Sort().Check(
					testkit.Rows(v1, v2))
			}
		}()
	}
	wg.Wait()
}

func TestInstancePlanCacheConcurrencyBatchPointNoTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, primary key(a))`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%v, %v)", i, i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tki := testkit.NewTestKit(t, store)
			tki.MustExec(`use test`)
			tki.MustExec(`prepare st from 'select a from t where a in (?, ?)'`)
			for k := 0; k < 100; k++ {
				a1, a2 := rand.Intn(50), 50+rand.Intn(50)
				tki.MustExec("set @a1 = ?, @a2 = ?", a1, a2)
				v1, v2 := fmt.Sprintf("%v", a1), fmt.Sprintf("%v", a2)
				if v1 > v2 {
					v1, v2 = v2, v1
				}
				tki.MustQuery("execute st using @a1, @a2").Sort().Check(
					testkit.Rows(v1, v2))
			}
		}()
	}
	wg.Wait()
}

func TestInstancePlanCacheConcurrencyPoint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database normal`)
	tk.MustExec(`create database prepared`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	for _, db := range []string{"normal", "prepared"} {
		tk.MustExec("use " + db)
		tk.MustExec(`create table t1 (col1 int, col2 int, primary key(col1), unique key(col2))`)
		for i := 0; i < 100; i++ {
			tk.MustExec(fmt.Sprintf("insert into t1 values (%v, %v)", i, i))
		}
	}

	genPointSelect := func() *testStmt {
		switch rand.Intn(2) {
		case 0: // select col1 from t1 where col1=?
			v1 := rand.Intn(100)
			return &testStmt{
				normalStmt: fmt.Sprintf("select col1 from normal.t1 where col1=%v", v1),
				prepStmt:   "prepare st from 'select col1 from prepared.t1 where col1=?'",
				setStmt:    fmt.Sprintf("set @v1 = %v", v1),
				execStmt:   "execute st using @v1",
			}
		default: // select col1 from t1 where col1=? and col2=?
			v1 := rand.Intn(100)
			v2 := rand.Intn(100)
			return &testStmt{
				normalStmt: fmt.Sprintf("select col1 from normal.t1 where col1=%v and col2=%v", v1, v2),
				prepStmt:   "prepare st from 'select col1 from prepared.t1 where col1=? and col2=?'",
				setStmt:    fmt.Sprintf("set @v1 = %v, @v2 = %v", v1, v2),
				execStmt:   "execute st using @v1, @v2",
			}
		}
	}

	nStmt := 400
	stmts := make([]*testStmt, 0, nStmt)
	stmts = append(stmts, &testStmt{normalStmt: "begin"})
	for len(stmts) < nStmt {
		if rand.Intn(15) == 0 { // start a new txn
			stmts = append(stmts, &testStmt{normalStmt: "commit"})
			stmts = append(stmts, &testStmt{normalStmt: "begin"})
			continue
		}
		stmts = append(stmts, genPointSelect())
	}
	stmts = append(stmts, &testStmt{normalStmt: "commit"})

	nConcurrency := 10
	TKs := make([]*testkit.TestKit, nConcurrency)
	for i := range TKs {
		TKs[i] = testkit.NewTestKit(t, store)
	}

	testWithWorkers(TKs, stmts)
}

func TestInstancePlanCacheConcurrencyPartitioning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec(`create table t (a int) partition by range (a) (
    			partition p0 values less than (10),
    			partition p1 values less than (20),
    			partition p2 values less than (30),
    			partition p3 values less than (40),
    			partition p4 values less than (50),
    			partition p5 values less than (60),
    			partition p6 values less than (70),
    			partition p7 values less than (80),
    			partition p8 values less than (90),
    			partition p9 values less than (100))`)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%v)", i))
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tki := testkit.NewTestKit(t, store)
			tki.MustExec(`use test`)

			for k := 0; k < 100; k++ {
				switch rand.Intn(3) {
				case 0: // point get
					tki.MustExec(`prepare st from 'select * from t where a=?'`)
					v := rand.Intn(100)
					tki.MustExec("set @v = ?", v)
					tki.MustQuery("execute st using @v").Check(testkit.Rows(fmt.Sprintf("%v", v)))
				case 1: // batch get
					tki.MustExec(`prepare st from 'select * from t where a in (?, ?)'`)
					v1, v2 := rand.Intn(50), 50+rand.Intn(50)
					tki.MustExec("set @v1 = ?, @v2 = ?", v1, v2)
					v1s, v2s := fmt.Sprintf("%v", v1), fmt.Sprintf("%v", v2)
					if v1s > v2s {
						v1s, v2s = v2s, v1s
					}
					tki.MustQuery("execute st using @v1, @v2").Sort().Check(testkit.Rows(v1s, v2s))
				case 2: // range scan
					tki.MustExec(`prepare st from 'select * from t where a between ? and ?'`)
					v1, v2 := rand.Intn(50), 50+rand.Intn(50)
					tki.MustExec("set @v1 = ?, @v2 = ?", v1, v2)
					expected := make([]string, 0, v2-v1+1)
					for v := v1; v <= v2; v++ {
						expected = append(expected, fmt.Sprintf("%v", v))
					}
					sort.Strings(expected)
					tki.MustQuery("execute st using @v1, @v2").Sort().Check(testkit.Rows(expected...))
				}
			}
		}()
	}
	wg.Wait()
}

func TestInstancePlanCacheConcurrencyComp(t *testing.T) {
	// cases from https://github.com/PingCAP-QE/qa/tree/master/comp/yy/plan-cache
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database normal`)
	tk.MustExec(`create database prepared`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	for _, db := range []string{"normal", "prepared"} {
		tk.MustExec("use " + db)
		tk.MustExec(`create table t1 (col1 int, col2 int, key(col1, col2))`)
	}

	genInsert := func() *testStmt {
		col1 := rand.Intn(1000)
		col2 := rand.Intn(1000)
		return &testStmt{
			normalStmt: fmt.Sprintf("insert into normal.t1 values (%v, %v)", col1, col2),
			prepStmt:   "prepare st from 'insert into prepared.t1 values (?, ?)'",
			setStmt:    fmt.Sprintf("set @col1 = %v, @col2 = %v", col1, col2),
			execStmt:   "execute st using @col1, @col2",
		}
	}
	genBasicSelect := func() *testStmt {
		switch rand.Intn(2) {
		case 0: // point
			switch rand.Intn(3) {
			case 0: // select * from t1 where col1=?
				col1 := rand.Intn(1000)
				return &testStmt{
					normalStmt: fmt.Sprintf("select * from normal.t1 where col1=%v", col1),
					prepStmt:   "prepare st from 'select * from prepared.t1 where col1=?'",
					setStmt:    fmt.Sprintf("set @col1 = %v", col1),
					execStmt:   "execute st using @col1",
				}
			case 1: // select * from t1 where col1 is null
				return &testStmt{
					normalStmt: "select * from normal.t1 where col1 is null",
					prepStmt:   "prepare st from 'select * from prepared.t1 where col1 is null'",
					setStmt:    "",
					execStmt:   "execute st",
				}
			case 2: // select * from t1 where col1 in (?, ?, ?)
				v1 := rand.Intn(1000)
				v2 := rand.Intn(1000)
				v3 := rand.Intn(1000)
				return &testStmt{
					normalStmt: fmt.Sprintf("select * from normal.t1 where col1 in (%v, %v, %v)", v1, v2, v3),
					prepStmt:   "prepare st from 'select * from prepared.t1 where col1 in (?, ?, ?)'",
					setStmt:    fmt.Sprintf("set @v1 = %v, @v2 = %v, @v3 = %v", v1, v2, v3),
					execStmt:   "execute st using @v1, @v2, @v3",
				}
			}
		default: // range
			switch rand.Intn(4) {
			case 0: // select * from t1 where col1 between ? and ?
				v1 := rand.Intn(1000)
				v2 := rand.Intn(1000)
				return &testStmt{
					normalStmt: fmt.Sprintf("select * from normal.t1 where col1 between %v and %v", v1, v2),
					prepStmt:   "prepare st from 'select * from prepared.t1 where col1 between ? and ?'",
					setStmt:    fmt.Sprintf("set @v1 = %v, @v2 = %v", v1, v2),
					execStmt:   "execute st using @v1, @v2",
				}
			case 1: // select * from t1 where col1 > ?
				v1 := rand.Intn(1000)
				return &testStmt{
					normalStmt: fmt.Sprintf("select * from normal.t1 where col1 > %v", v1),
					prepStmt:   "prepare st from 'select * from prepared.t1 where col1 > ?'",
					setStmt:    fmt.Sprintf("set @v1 = %v", v1),
					execStmt:   "execute st using @v1",
				}
			case 2: // select * from t1 where col1 <= ?
				v1 := rand.Intn(1000)
				return &testStmt{
					normalStmt: fmt.Sprintf("select * from normal.t1 where col1 <= %v", v1),
					prepStmt:   "prepare st from 'select * from prepared.t1 where col1 <= ?'",
					setStmt:    fmt.Sprintf("set @v1 = %v", v1),
					execStmt:   "execute st using @v1",
				}
			default: // select * from t1 where col1 != ?
				v1 := rand.Intn(1000)
				return &testStmt{
					normalStmt: fmt.Sprintf("select * from normal.t1 where col1 != %v", v1),
					prepStmt:   "prepare st from 'select * from prepared.t1 where col1 != ?'",
					setStmt:    fmt.Sprintf("set @v1 = %v", v1),
					execStmt:   "execute st using @v1",
				}
			}
		}
		return nil
	}
	genAggSelect := func() *testStmt {
		switch rand.Intn(5) {
		case 0: // select sum(col1), col2 from t1 where col1=? group by col2
			v1 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select sum(col1), col2 from normal.t1 where col1=%v group by col2", v1),
				prepStmt:   "prepare st from 'select sum(col1), col2 from prepared.t1 where col1=? group by col2'",
				setStmt:    fmt.Sprintf("set @v1 = %v", v1),
				execStmt:   "execute st using @v1",
			}
		case 1: // select sum(col1), col2 from t1 where col1 between ? and ? group by col2
			v1 := rand.Intn(1000)
			v2 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select sum(col1), col2 from normal.t1 where col1 between %v and %v group by col2", v1, v2),
				prepStmt:   "prepare st from 'select sum(col1), col2 from prepared.t1 where col1 between ? and ? group by col2'",
				setStmt:    fmt.Sprintf("set @v1 = %v, @v2 = %v", v1, v2),
				execStmt:   "execute st using @v1, @v2",
			}
		case 2: // select sum(col1), col2 from t1 where col1 > ? group by col2
			v1 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select sum(col1), col2 from normal.t1 where col1 > %v group by col2", v1),
				prepStmt:   "prepare st from 'select sum(col1), col2 from prepared.t1 where col1 > ? group by col2'",
				setStmt:    fmt.Sprintf("set @v1 = %v", v1),
				execStmt:   "execute st using @v1",
			}
		case 3: // select sum(col1), col2 from t1 where col1 <= ? group by col2
			v1 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select sum(col1), col2 from normal.t1 where col1 <= %v group by col2", v1),
				prepStmt:   "prepare st from 'select sum(col1), col2 from prepared.t1 where col1 <= ? group by col2'",
				setStmt:    fmt.Sprintf("set @v1 = %v", v1),
				execStmt:   "execute st using @v1",
			}
		default: // select sum(col1), col2 from t1 where col1 = ? group by col2
			v1 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select sum(col1), col2 from normal.t1 where col1 = %v group by col2", v1),
				prepStmt:   "prepare st from 'select sum(col1), col2 from prepared.t1 where col1 = ? group by col2'",
				setStmt:    fmt.Sprintf("set @v1 = %v", v1),
				execStmt:   "execute st using @v1",
			}
		}
	}
	genJoinSelect := func() *testStmt {
		switch rand.Intn(4) {
		case 0: // select * from t1 t1 join t1 t2 on t1.col1=t2.col1 where t1.col1=?
			v1 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select * from normal.t1 t1 join normal.t1 t2 on t1.col1=t2.col1 where t1.col1=%v", v1),
				prepStmt:   "prepare st from 'select * from prepared.t1 t1 join prepared.t1 t2 on t1.col1=t2.col1 where t1.col1=?'",
				setStmt:    fmt.Sprintf("set @v1 = %v", v1),
				execStmt:   "execute st using @v1",
			}
		case 1: // select * from t1 t1 join t1 t2 on t1.col1=t2.col1 where t1.col1 between ? and ?
			v1 := rand.Intn(1000)
			v2 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select * from normal.t1 t1 join normal.t1 t2 on t1.col1=t2.col1 where t1.col1 between %v and %v", v1, v2),
				prepStmt:   "prepare st from 'select * from prepared.t1 t1 join prepared.t1 t2 on t1.col1=t2.col1 where t1.col1 between ? and ?'",
				setStmt:    fmt.Sprintf("set @v1 = %v, @v2 = %v", v1, v2),
				execStmt:   "execute st using @v1, @v2",
			}
		case 2: // select * from t1 t1 left join t1 t2 on t1.col1=t2.col1 where t1.col1 > ?
			v1 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select * from normal.t1 t1 left join normal.t1 t2 on t1.col1=t2.col1 where t1.col1 > %v", v1),
				prepStmt:   "prepare st from 'select * from prepared.t1 t1 left join prepared.t1 t2 on t1.col1=t2.col1 where t1.col1 > ?'",
				setStmt:    fmt.Sprintf("set @v1 = %v", v1),
				execStmt:   "execute st using @v1",
			}
		case 3: // select * from t1 t1 join t1 t2 on t1.col1>t2.col1 where t1.col1 <= ?
			v1 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select * from normal.t1 t1 join normal.t1 t2 on t1.col1>t2.col1 where t1.col1 <= %v", v1),
				prepStmt:   "prepare st from 'select * from prepared.t1 t1 join prepared.t1 t2 on t1.col1>t2.col1 where t1.col1 <= ?'",
				setStmt:    fmt.Sprintf("set @v1 = %v", v1),
				execStmt:   "execute st using @v1",
			}
		default: // select * from t1 t1 join t1 t2 on t1.col1<t2.col1 where t1.col1 between ? and ?
			v1 := rand.Intn(1000)
			v2 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select * from normal.t1 t1 join normal.t1 t2 on t1.col1<t2.col1 where t1.col1 between %v and %v", v1, v2),
				prepStmt:   "prepare st from 'select * from prepared.t1 t1 join prepared.t1 t2 on t1.col1<t2.col1 where t1.col1 between ? and ?'",
				setStmt:    fmt.Sprintf("set @v1 = %v, @v2 = %v", v1, v2),
				execStmt:   "execute st using @v1, @v2",
			}
		}
	}
	genPointSelect := func() *testStmt {
		switch rand.Intn(5) {
		case 0: // select col1 from t1 where col1=?
			v1 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select col1 from normal.t1 where col1=%v", v1),
				prepStmt:   "prepare st from 'select col1 from prepared.t1 where col1=?'",
				setStmt:    fmt.Sprintf("set @v1 = %v", v1),
				execStmt:   "execute st using @v1",
			}
		case 1: // select col1 from t1 where col1=? and col2=?
			v1 := rand.Intn(1000)
			v2 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select col1 from normal.t1 where col1=%v and col2=%v", v1, v2),
				prepStmt:   "prepare st from 'select col1 from prepared.t1 where col1=? and col2=?'",
				setStmt:    fmt.Sprintf("set @v1 = %v, @v2 = %v", v1, v2),
				execStmt:   "execute st using @v1, @v2",
			}
		case 2: // select col1 from t1 where col1=? or col2=?
			v1 := rand.Intn(1000)
			v2 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select col1 from normal.t1 where col1=%v or col2=%v", v1, v2),
				prepStmt:   "prepare st from 'select col1 from prepared.t1 where col1=? or col2=?'",
				setStmt:    fmt.Sprintf("set @v1 = %v, @v2 = %v", v1, v2),
				execStmt:   "execute st using @v1, @v2",
			}
		case 3: // select col1 from t1 where col1 in (?,?,?) and col2=?
			v1 := rand.Intn(1000)
			v2 := rand.Intn(1000)
			v3 := rand.Intn(1000)
			v4 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select col1 from normal.t1 where col1 in (%v, %v, %v) and col2=%v", v1, v2, v3, v4),
				prepStmt:   "prepare st from 'select col1 from prepared.t1 where col1 in (?, ?, ?) and col2=?'",
				setStmt:    fmt.Sprintf("set @v1 = %v, @v2 = %v, @v3 = %v, @v4 = %v", v1, v2, v3, v4),
				execStmt:   "execute st using @v1, @v2, @v3, @v4",
			}
		default: // select col1 from t1 where col1 in (?,?,?) or col2=?
			v1 := rand.Intn(1000)
			v2 := rand.Intn(1000)
			v3 := rand.Intn(1000)
			v4 := rand.Intn(1000)
			return &testStmt{
				normalStmt: fmt.Sprintf("select col1 from normal.t1 where col1 in (%v, %v, %v) or col2=%v", v1, v2, v3, v4),
				prepStmt:   "prepare st from 'select col1 from prepared.t1 where col1 in (?, ?, ?) or col2=?'",
				setStmt:    fmt.Sprintf("set @v1 = %v, @v2 = %v, @v3 = %v, @v4 = %v", v1, v2, v3, v4),
				execStmt:   "execute st using @v1, @v2, @v3, @v4",
			}
		}
	}

	nStmt := 2000
	nInitialRecords := 100
	stmts := make([]*testStmt, 0, nStmt)
	stmts = append(stmts, &testStmt{normalStmt: "begin"})
	for len(stmts) < nStmt {
		if rand.Intn(15) == 0 { // start a new txn
			stmts = append(stmts, &testStmt{normalStmt: "commit"})
			stmts = append(stmts, &testStmt{normalStmt: "begin"})
			continue
		}
		if len(stmts) < nInitialRecords {
			stmts = append(stmts, genInsert())
			continue
		}
		x := rand.Intn(100)
		if x < 10 { // 10% insert
			stmts = append(stmts, genInsert())
		} else if x < 50 { // 40% basic
			stmts = append(stmts, genBasicSelect())
		} else if x < 60 {
			stmts = append(stmts, genAggSelect())
		} else if x < 70 {
			stmts = append(stmts, genJoinSelect())
		} else {
			stmts = append(stmts, genPointSelect())
		}
	}
	stmts = append(stmts, &testStmt{normalStmt: "commit"})

	nConcurrency := 10
	TKs := make([]*testkit.TestKit, nConcurrency)
	for i := range TKs {
		TKs[i] = testkit.NewTestKit(t, store)
	}

	testWithWorkers(TKs, stmts)
}
