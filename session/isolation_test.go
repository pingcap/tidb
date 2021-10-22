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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session_test

import (
	"sync"
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestIsolation(t *testing.T) {
	t.Parallel()

	s := createTestSuite(func(err error) { require.NoError(t, err) }, nil)
	defer s.cleanSuite(func(err error) { require.NoError(t, err) })

	t.Run("P0DirtyWrite", SubTestP0DirtyWrite(s))
	t.Run("P1DirtyRead", SubTestP1DirtyRead(s))
	t.Run("P2NonRepeatableRead", SubTestP2NonRepeatableRead(s))
	t.Run("P3Phantom", SubTestP3Phantom(s))
	t.Run("P4LostUpdate", SubTestP4LostUpdate(s))
	t.Run("A3Phantom", SubTestA3Phantom(s))
	t.Run("A5AReadSkew", SubTestA5AReadSkew(s))
	t.Run("A5BWriteSkew", SubTestA5BWriteSkew(s))
	t.Run("ReadAfterWrite", SubTestReadAfterWrite(s))
	t.Run("PhantomReadInInnodb", SubTestPhantomReadInInnodb(s))
}

/*
These test cases come from the paper <A Critique of ANSI SQL Isolation Levels>.
The sign 'P0', 'P1'.... can be found in the paper. These cases will run under snapshot isolation.
*/
func SubTestP0DirtyWrite(s *testSessionSuiteBase) func(*testing.T) {
	return func(t *testing.T) {
		session1 := testkit.NewTestKit(t, s.store)
		session2 := testkit.NewTestKit(t, s.store)

		session1.MustExec("use test")
		session2.MustExec("use test")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("begin;")
		session2.MustExec("update x set c = c+1 where id = 1;")
		session1.MustExec("commit;")
		_, err := session2.Exec("commit;")
		require.Error(t, err)

		session1.MustExec("set tidb_txn_mode = 'pessimistic'")
		session2.MustExec("set tidb_txn_mode = 'pessimistic'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("begin;")
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			session2.MustExec("update x set c = c+1 where id = 1;")
			wg.Done()
		}()
		session1.MustExec("commit;")
		wg.Wait()
		session2.MustExec("commit;")

		session1.MustExec("set tx_isolation = 'READ-COMMITTED'")
		session2.MustExec("set tx_isolation = 'READ-COMMITTED'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("begin;")
		wg.Add(1)
		go func() {
			session2.MustExec("update x set c = c+1 where id = 1;")
			wg.Done()
		}()
		session1.MustExec("commit;")
		wg.Wait()
		session2.MustExec("commit;")
		session2.MustQuery("select * from x").Check(testkit.Rows("1 3"))
	}
}

func SubTestP1DirtyRead(s *testSessionSuiteBase) func(*testing.T) {
	return func(t *testing.T) {
		session1 := testkit.NewTestKit(t, s.store)
		session2 := testkit.NewTestKit(t, s.store)

		session1.MustExec("use test")
		session2.MustExec("use test")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("begin;")
		session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session1.MustExec("commit;")
		session2.MustExec("commit;")

		session1.MustExec("set tidb_txn_mode = 'pessimistic'")
		session2.MustExec("set tidb_txn_mode = 'pessimistic'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("begin;")
		session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session1.MustExec("commit;")
		session2.MustExec("commit;")

		session1.MustExec("set tx_isolation = 'READ-COMMITTED'")
		session2.MustExec("set tx_isolation = 'READ-COMMITTED'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("begin;")
		session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session1.MustExec("commit;")
		session2.MustExec("commit;")
	}
}

func SubTestP2NonRepeatableRead(s *testSessionSuiteBase) func(*testing.T) {
	return func(t *testing.T) {
		session1 := testkit.NewTestKit(t, s.store)
		session2 := testkit.NewTestKit(t, s.store)

		session1.MustExec("use test")
		session2.MustExec("use test")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("drop table if exists y;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")
		session1.MustExec("create table y (id int primary key, c int);")
		session1.MustExec("insert into y values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update x set c = c+1 where id = 1;")
		session2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update y set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
		session1.MustExec("commit;")

		session1.MustExec("set tidb_txn_mode = 'pessimistic'")
		session2.MustExec("set tidb_txn_mode = 'pessimistic'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("drop table if exists y;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")
		session1.MustExec("create table y (id int primary key, c int);")
		session1.MustExec("insert into y values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update x set c = c+1 where id = 1;")
		session2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update y set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
		session1.MustExec("commit;")

		session1.MustExec("set tx_isolation = 'READ-COMMITTED'")
		session2.MustExec("set tx_isolation = 'READ-COMMITTED'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("drop table if exists y;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")
		session1.MustExec("create table y (id int primary key, c int);")
		session1.MustExec("insert into y values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update x set c = c+1 where id = 1;")
		session2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update y set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("2"))
		session1.MustExec("commit;")
	}
}

func SubTestP3Phantom(s *testSessionSuiteBase) func(*testing.T) {
	return func(t *testing.T) {
		session1 := testkit.NewTestKit(t, s.store)
		session2 := testkit.NewTestKit(t, s.store)

		session1.MustExec("use test")
		session2.MustExec("use test")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("drop table if exists z;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")
		session1.MustExec("create table z (id int primary key, c int);")
		session1.MustExec("insert into z values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
		session2.MustExec("insert into x values(2, 1);")
		session2.MustQuery("select c from z where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update z set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustQuery("select c from z where id = 1;").Check(testkit.Rows("1"))
		session1.MustExec("commit;")

		session1.MustExec("set tidb_txn_mode = 'pessimistic'")
		session2.MustExec("set tidb_txn_mode = 'pessimistic'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("drop table if exists z;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")
		session1.MustExec("create table z (id int primary key, c int);")
		session1.MustExec("insert into z values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
		session2.MustExec("insert into x values(2, 1);")
		session2.MustQuery("select c from z where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update z set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustQuery("select c from z where id = 1;").Check(testkit.Rows("1"))
		session1.MustExec("commit;")

		session1.MustExec("set tx_isolation = 'READ-COMMITTED'")
		session2.MustExec("set tx_isolation = 'READ-COMMITTED'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("drop table if exists z;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")
		session1.MustExec("create table z (id int primary key, c int);")
		session1.MustExec("insert into z values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
		session2.MustExec("insert into x values(2, 1);")
		session2.MustQuery("select c from z where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update z set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustQuery("select c from z where id = 1;").Check(testkit.Rows("2"))
		session1.MustExec("commit;")
	}
}

func SubTestP4LostUpdate(s *testSessionSuiteBase) func(*testing.T) {
	return func(t *testing.T) {
		session1 := testkit.NewTestKit(t, s.store)
		session2 := testkit.NewTestKit(t, s.store)

		session1.MustExec("use test")
		session2.MustExec("use test")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("begin;")
		session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustExec("update x set c = c+1 where id = 1;")
		_, err := session1.Exec("commit;")
		require.Error(t, err)

		session1.MustExec("set tidb_txn_mode = 'pessimistic'")
		session2.MustExec("set tidb_txn_mode = 'pessimistic'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("begin;")
		session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustExec("update x set c = c+1 where id = 1;")
		session1.MustExec("commit;")

		session1.MustExec("set tx_isolation = 'READ-COMMITTED'")
		session2.MustExec("set tx_isolation = 'READ-COMMITTED'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("begin;")
		session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustExec("update x set c = c+1 where id = 1;")
		session1.MustExec("commit;")
		session1.MustQuery("select * from x").Check(testkit.Rows("1 3"))
	}
}

func SubTestA3Phantom(s *testSessionSuiteBase) func(*testing.T) {
	return func(t *testing.T) {
		session1 := testkit.NewTestKit(t, s.store)
		session2 := testkit.NewTestKit(t, s.store)

		session1.MustExec("use test")
		session2.MustExec("use test")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
		session1.MustExec("insert into x values(2, 1);")
		session1.MustExec("commit;")
		session2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
		session2.MustExec("commit;")

		session1.MustExec("set tidb_txn_mode = 'pessimistic'")
		session2.MustExec("set tidb_txn_mode = 'pessimistic'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
		session1.MustExec("insert into x values(2, 1);")
		session1.MustExec("commit;")
		session2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
		session2.MustExec("commit;")

		session1.MustExec("set tx_isolation = 'READ-COMMITTED'")
		session2.MustExec("set tx_isolation = 'READ-COMMITTED'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
		session1.MustExec("insert into x values(2, 1);")
		session1.MustExec("commit;")
		session2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1", "1"))
		session2.MustExec("commit;")
	}
}

func SubTestA5AReadSkew(s *testSessionSuiteBase) func(*testing.T) {
	return func(t *testing.T) {
		session1 := testkit.NewTestKit(t, s.store)
		session2 := testkit.NewTestKit(t, s.store)

		session1.MustExec("use test")
		session2.MustExec("use test")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("drop table if exists y;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")
		session1.MustExec("create table y (id int primary key, c int);")
		session1.MustExec("insert into y values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("update y set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
		session1.MustExec("commit;")

		session1.MustExec("set tidb_txn_mode = 'pessimistic'")
		session2.MustExec("set tidb_txn_mode = 'pessimistic'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("drop table if exists y;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")
		session1.MustExec("create table y (id int primary key, c int);")
		session1.MustExec("insert into y values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("update y set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
		session1.MustExec("commit;")

		session1.MustExec("set tx_isolation = 'READ-COMMITTED'")
		session2.MustExec("set tx_isolation = 'READ-COMMITTED'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("drop table if exists y;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")
		session1.MustExec("create table y (id int primary key, c int);")
		session1.MustExec("insert into y values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("update y set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("2"))
		session1.MustExec("commit;")
	}
}

func SubTestA5BWriteSkew(s *testSessionSuiteBase) func(*testing.T) {
	return func(t *testing.T) {
		session1 := testkit.NewTestKit(t, s.store)
		session2 := testkit.NewTestKit(t, s.store)

		session1.MustExec("use test")
		session2.MustExec("use test")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("drop table if exists y;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")
		session1.MustExec("create table y (id int primary key, c int);")
		session1.MustExec("insert into y values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
		session1.MustExec("update y set c = c+1 where id = 1;")
		session2.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustExec("commit;")

		session1.MustExec("set tidb_txn_mode = 'pessimistic'")
		session2.MustExec("set tidb_txn_mode = 'pessimistic'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("drop table if exists y;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")
		session1.MustExec("create table y (id int primary key, c int);")
		session1.MustExec("insert into y values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
		session1.MustExec("update y set c = c+1 where id = 1;")
		session2.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustExec("commit;")

		session1.MustExec("update y set id = 2 where id = 1;")
		session1.MustQuery("select id from x").Check(testkit.Rows("1"))
		session1.MustQuery("select id from y").Check(testkit.Rows("2"))
		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select id from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustQuery("select id from y where id = 2;").Check(testkit.Rows("2"))
		session1.MustExec("update y set id = 1 where id = 2;")
		session2.MustExec("update x set id = 2 where id = 1;")
		session2.MustExec("commit;")
		session1.MustExec("commit;")
		session1.MustQuery("select id from x").Check(testkit.Rows("2"))
		session1.MustQuery("select id from y").Check(testkit.Rows("1"))

		session1.MustExec("set tx_isolation = 'READ-COMMITTED'")
		session2.MustExec("set tx_isolation = 'READ-COMMITTED'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("drop table if exists y;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")
		session1.MustExec("create table y (id int primary key, c int);")
		session1.MustExec("insert into y values(1, 1);")

		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
		session1.MustExec("update y set c = c+1 where id = 1;")
		session2.MustExec("update x set c = c+1 where id = 1;")
		session2.MustExec("commit;")
		session1.MustExec("commit;")

		session1.MustExec("update y set id = 2 where id = 1;")
		session1.MustQuery("select id from x").Check(testkit.Rows("1"))
		session1.MustQuery("select id from y").Check(testkit.Rows("2"))
		session1.MustExec("begin;")
		session2.MustExec("begin;")
		session1.MustQuery("select id from x where id = 1;").Check(testkit.Rows("1"))
		session2.MustQuery("select id from y where id = 2;").Check(testkit.Rows("2"))
		session1.MustExec("update y set id = 1 where id = 2;")
		session2.MustExec("update x set id = 2 where id = 1;")
		session2.MustExec("commit;")
		session1.MustExec("commit;")
		session1.MustQuery("select id from x").Check(testkit.Rows("2"))
		session1.MustQuery("select id from y").Check(testkit.Rows("1"))
	}
}

/*
These test cases come from the paper <Highly Available Transactions: Virtues and Limitations>
for tidb, we support read-after-write on cluster level.
*/
func SubTestReadAfterWrite(s *testSessionSuiteBase) func(*testing.T) {
	return func(t *testing.T) {
		session1 := testkit.NewTestKit(t, s.store)
		session2 := testkit.NewTestKit(t, s.store)

		session1.MustExec("use test")
		session2.MustExec("use test")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustExec("update x set c = c+1 where id = 1;")
		session1.MustExec("commit;")
		session2.MustExec("begin;")
		session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("2"))
		session2.MustExec("commit;")

		session1.MustExec("set tidb_txn_mode = 'pessimistic'")
		session2.MustExec("set tidb_txn_mode = 'pessimistic'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustExec("update x set c = c+1 where id = 1;")
		session1.MustExec("commit;")
		session2.MustExec("begin;")
		session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("2"))
		session2.MustExec("commit;")

		session1.MustExec("set tx_isolation = 'READ-COMMITTED'")
		session2.MustExec("set tx_isolation = 'READ-COMMITTED'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustExec("update x set c = c+1 where id = 1;")
		session1.MustExec("commit;")
		session2.MustExec("begin;")
		session2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("2"))
		session2.MustExec("commit;")
	}
}

/*
This case will do harm in Innodb, even if in snapshot isolation, but harmless in tidb.
*/
func SubTestPhantomReadInInnodb(s *testSessionSuiteBase) func(*testing.T) {
	return func(t *testing.T) {
		session1 := testkit.NewTestKit(t, s.store)
		session2 := testkit.NewTestKit(t, s.store)

		session1.MustExec("use test")
		session2.MustExec("use test")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
		session2.MustExec("begin;")
		session2.MustExec("insert into x values(2, 1);")
		session2.MustExec("commit;")
		session1.MustExec("update x set c = c+1 where id < 5;")
		session1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("2"))
		session1.MustExec("commit;")

		session1.MustExec("set tidb_txn_mode = 'pessimistic'")
		session2.MustExec("set tidb_txn_mode = 'pessimistic'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
		session2.MustExec("begin;")
		session2.MustExec("insert into x values(2, 1);")
		session2.MustExec("commit;")
		session1.MustExec("update x set c = c+1 where id < 5;")
		session1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("2", "2"))
		session1.MustExec("commit;")

		session1.MustExec("set tx_isolation = 'READ-COMMITTED'")
		session2.MustExec("set tx_isolation = 'READ-COMMITTED'")

		session1.MustExec("drop table if exists x;")
		session1.MustExec("create table x (id int primary key, c int);")
		session1.MustExec("insert into x values(1, 1);")

		session1.MustExec("begin;")
		session1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
		session2.MustExec("begin;")
		session2.MustExec("insert into x values(2, 1);")
		session2.MustExec("commit;")
		session1.MustExec("update x set c = c+1 where id < 5;")
		session1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("2", "2"))
		session1.MustExec("commit;")
	}
}
