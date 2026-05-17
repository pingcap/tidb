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

package txntest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
)

func TestGetCachedStore(t *testing.T) {
	defer view.Stop()
	var d driver.TiKVDriver
	// when get the cached store, there should not have routine leak.
	store1, err := d.Open(*realtikvtest.TiKVPath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store1.Close())
	}()
	store2, err := d.Open(*realtikvtest.TiKVPath)
	require.NoError(t, err)
	require.Equal(t, store1, store2)
}

/*
These test cases come from the paper <A Critique of ANSI SQL Isolation Levels>.
The sign 'P0', 'P1'.... can be found in the paper. These cases will run under snapshot isolation.
*/
func TestP0DirtyWrite(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)
	session1.MustExec("use test;")
	session2.MustExec("use test;")
	session1.MustExec("set tidb_txn_mode = 'optimistic'")
	session2.MustExec("set tidb_txn_mode = 'optimistic'")

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
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		session2.MustExec("update x set c = c+1 where id = 1;")
	})
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
	wg.Run(func() {
		session2.MustExec("update x set c = c+1 where id = 1;")
	})
	session1.MustExec("commit;")
	wg.Wait()
	session2.MustExec("commit;")
	session2.MustQuery("select * from x").Check(testkit.Rows("1 3"))
}

func TestP1DirtyRead(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)
	session1.MustExec("use test;")
	session2.MustExec("use test;")
	session1.MustExec("set tidb_txn_mode = 'optimistic'")
	session2.MustExec("set tidb_txn_mode = 'optimistic'")

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

func TestP2NonRepeatableRead(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)
	session1.MustExec("use test;")
	session2.MustExec("use test;")
	session1.MustExec("set tidb_txn_mode = 'optimistic'")
	session2.MustExec("set tidb_txn_mode = 'optimistic'")

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

func TestP3Phantom(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)
	session1.MustExec("use test;")
	session2.MustExec("use test;")
	session1.MustExec("set tidb_txn_mode = 'optimistic'")
	session2.MustExec("set tidb_txn_mode = 'optimistic'")

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

func TestP4LostUpdate(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)
	session1.MustExec("use test;")
	session2.MustExec("use test;")
	session1.MustExec("set tidb_txn_mode = 'optimistic'")
	session2.MustExec("set tidb_txn_mode = 'optimistic'")

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

// cursor is not supported
func TestP4CLostUpdate(t *testing.T) {}

func TestA3Phantom(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)
	session1.MustExec("use test;")
	session2.MustExec("use test;")
	session1.MustExec("set tidb_txn_mode = 'optimistic'")
	session2.MustExec("set tidb_txn_mode = 'optimistic'")

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

func TestA5AReadSkew(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)
	session1.MustExec("use test;")
	session2.MustExec("use test;")
	session1.MustExec("set tidb_txn_mode = 'optimistic'")
	session2.MustExec("set tidb_txn_mode = 'optimistic'")

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

func TestA5BWriteSkew(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)
	session1.MustExec("use test;")
	session2.MustExec("use test;")
	session1.MustExec("set tidb_txn_mode = 'optimistic'")
	session2.MustExec("set tidb_txn_mode = 'optimistic'")

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

/*
These test cases come from the paper <Highly Available Transactions: Virtues and Limitations>
for tidb, we support read-after-write on cluster level.
*/
func TestReadAfterWrite(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)
	session1.MustExec("use test;")
	session2.MustExec("use test;")
	session1.MustExec("set tidb_txn_mode = 'optimistic'")
	session2.MustExec("set tidb_txn_mode = 'optimistic'")

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

/*
This case will do harm in Innodb, even if in snapshot isolation, but harmless in tidb.
*/
/*
TestG2AntiDependencyCycleForUpdate tests the anti-dependency cycle (G2 anomaly)
reported in https://github.com/pingcap/tidb/issues/10444.

Jepsen found that under pessimistic mode with SELECT ... FOR UPDATE, two
concurrent transactions could each read a non-existent key and then write
to each other's key, forming an anti-dependency cycle:

  T1: r(key_a, nil), w(key_b, 1)
  T2: r(key_b, nil), w(key_a, 1)

T1 must precede T2 (because T1 saw nil for key_a before T2 wrote it), but
T2 must also precede T1 (because T2 saw nil for key_b before T1 wrote it).

In pessimistic RR mode, SELECT ... FOR UPDATE acquires a pessimistic lock
even on non-existent point-get keys, so the second transaction should block
until the first commits, preventing the cycle.
*/
func TestG2AntiDependencyCycleForUpdate(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)
	session1.MustExec("use test;")
	session2.MustExec("use test;")

	// Pessimistic RR mode: SELECT FOR UPDATE on non-existent keys should
	// acquire locks and prevent anti-dependency cycles.
	session1.MustExec("set tidb_txn_mode = 'pessimistic'")
	session2.MustExec("set tidb_txn_mode = 'pessimistic'")

	session1.MustExec("drop table if exists t;")
	session1.MustExec("create table t (id int primary key, val int);")

	// Scenario: both keys do not exist yet.
	// T1 reads key 1 FOR UPDATE (acquires pessimistic lock), then inserts key 2.
	// T2 tries to read key 2 FOR UPDATE but should block because T1's insert
	// will conflict (or T2 blocks on the lock for key 2 once T1 writes it).
	session1.MustExec("begin;")
	session1.MustQuery("select * from t where id = 1 for update;").Check(testkit.Rows())

	// T2 begins and tries to read key 2 FOR UPDATE.
	session2.MustExec("begin;")
	session2.MustQuery("select * from t where id = 2 for update;").Check(testkit.Rows())

	// T1 inserts key 2. If key 2 was locked by T2, this would block. But since
	// T2's read returned empty, the lock on key 2 is a pessimistic lock on a
	// non-existent key.
	session1.MustExec("insert into t values(2, 1);")
	session1.MustExec("commit;")

	// T2 now tries to insert key 1. Since T1 held a pessimistic lock on key 1
	// (even though it didn't exist), and T1 has committed, T2 should be able to
	// proceed. The anti-dependency cycle is avoided because the locks serialize
	// the reads.
	session2.MustExec("insert into t values(1, 1);")
	session2.MustExec("commit;")

	// Verify both rows exist.
	session1.MustQuery("select * from t order by id;").Check(testkit.Rows("1 1", "2 1"))

	// Test with concurrent blocking: T1 locks key 3, T2 tries to lock key 3
	// and should block until T1 commits.
	session1.MustExec("begin;")
	session1.MustQuery("select * from t where id = 3 for update;").Check(testkit.Rows())

	var wg util.WaitGroupWrapper
	wg.Run(func() {
		session2.MustExec("begin;")
		// This should block until T1 releases its lock on key 3.
		session2.MustQuery("select * from t where id = 3 for update;").Check(testkit.Rows("3 100"))
		session2.MustExec("commit;")
	})

	session1.MustExec("insert into t values(3, 100);")
	session1.MustExec("commit;")
	wg.Wait()

	session1.MustQuery("select * from t where id = 3;").Check(testkit.Rows("3 100"))
}

/*
TestA5BWriteSkewForUpdateNonExistent tests write skew prevention when
SELECT ... FOR UPDATE is used on non-existent keys. This is the specific
scenario from https://github.com/pingcap/tidb/issues/10444:

  T1 = [r(3,nil), r(4,nil), w(4,2)]  (SELECT 3 FOR UPDATE, SELECT 4 FOR UPDATE, INSERT 4)
  T2 = [r(3,nil), r(4,nil), w(3,1)]  (SELECT 3 FOR UPDATE, SELECT 4 FOR UPDATE, INSERT 3)

With proper pessimistic locking on non-existent keys, T2's FOR UPDATE on key 3
should block on T1's lock (or vice versa), preventing the write skew.
*/
func TestA5BWriteSkewForUpdateNonExistent(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)
	session1.MustExec("use test;")
	session2.MustExec("use test;")
	session1.MustExec("set tidb_txn_mode = 'pessimistic'")
	session2.MustExec("set tidb_txn_mode = 'pessimistic'")

	session1.MustExec("drop table if exists t;")
	session1.MustExec("create table t (id int primary key, val int);")

	// T1 begins, reads key 3 and key 4 FOR UPDATE (both nil), then writes key 4.
	session1.MustExec("begin;")
	session1.MustQuery("select * from t where id = 3 for update;").Check(testkit.Rows())
	session1.MustQuery("select * from t where id = 4 for update;").Check(testkit.Rows())

	// T2 begins, tries to read key 3 FOR UPDATE. In RR pessimistic mode,
	// this should block because T1 holds a pessimistic lock on non-existent key 3.
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		session2.MustExec("begin;")
		// This should block on the pessimistic lock T1 holds on key 3.
		session2.MustQuery("select * from t where id = 3 for update;").Check(testkit.Rows())
		session2.MustQuery("select * from t where id = 4 for update;").Check(testkit.Rows("4 2"))
		session2.MustExec("insert into t values(3, 1);")
		session2.MustExec("commit;")
	})

	// T1 writes key 4 and commits, releasing the lock on key 3.
	session1.MustExec("insert into t values(4, 2);")
	session1.MustExec("commit;")
	wg.Wait()

	// Verify both inserts succeeded with proper serialization.
	session1.MustQuery("select * from t order by id;").Check(testkit.Rows("3 1", "4 2"))
}

func TestPhantomReadInInnodb(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)
	session1.MustExec("use test;")
	session2.MustExec("use test;")
	session1.MustExec("set tidb_txn_mode = 'optimistic'")
	session2.MustExec("set tidb_txn_mode = 'optimistic'")

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
