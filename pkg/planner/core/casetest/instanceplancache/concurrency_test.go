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
	genUpdate := func() *testStmt {
		switch rand.Intn(2) {
		case 0: // update sbtest set k=k+1 where id=?
			id := rand.Intn(maxID)
			return &testStmt{
				normalStmt: fmt.Sprintf("update normal.sbtest set k=k+1 where id=%v", id),
				prepStmt:   "prepare st from 'update prepared.sbtest set k=k+1 where id=?'",
				setStmt:    fmt.Sprintf("set @id = %v", id),
				execStmt:   "execute st using @id",
			}
		default: // update sbtest set c=? where id=?
			id := rand.Intn(maxID)
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
		id := rand.Intn(maxID)
		return &testStmt{
			normalStmt: fmt.Sprintf("delete from normal.sbtest where id=%v", id),
			prepStmt:   "prepare st from 'delete from prepared.sbtest where id=?'",
			setStmt:    fmt.Sprintf("set @id = %v", id),
			execStmt:   "execute st using @id",
		}
	}

	nStmt := 7000
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
