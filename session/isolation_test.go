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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/driver"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
	pdconfig "github.com/tikv/client-go/v2/config"
)

func TestIsolationWithTikv(t *testing.T) {
	if !*withTiKV {
		t.Skip(" without tikv")
	}
	var store kv.Storage
	var pdAddr string
	initPdAddrs()
	pdAddr = <-pdAddrChan
	var d driver.TiKVDriver
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches.Enabled = false
	})
	store, err := d.OpenWithOptions(fmt.Sprintf("tikv://%s?disableGC=true", pdAddr), driver.WithPDClientConfig(pdconfig.PDClient{
		PDServerTimeout: 10,
	}))
	require.NoError(t, err)
	err = clearStorage(store)
	require.NoError(t, err)
	err = clearETCD(store.(kv.EtcdBackend))
	require.NoError(t, err)
	session.ResetStoreForWithTiKVTest(store)
	defer func() {
		require.NoError(t, store.Close())
		pdAddrChan <- pdAddr
	}()
	time.Sleep(10 * time.Second)
	t.Run("TestP0DirtyWrite", func(t *testing.T) {
		p0DirtyWrite(t, store)
	})
	t.Run("TestP1DirtyRead", func(t *testing.T) {
		p1DirtyRead(t, store)
	})
	t.Run("TestP3Phantom", func(t *testing.T) {
		p3Phantom(t, store)
	})
	t.Run("TestP2NonRepeatableRead", func(t *testing.T) {
		p2NonRepeatableRead(t, store)
	})
	t.Run("TestP4LostUpdate", func(t *testing.T) {
		p4LostUpdate(t, store)
	})
	t.Run("TestA3Phantom", func(t *testing.T) {
		a3Phantom(t, store)
	})
	t.Run("TestA5AReadSkew", func(t *testing.T) {
		a5AReadSkew(t, store)
	})
	t.Run("TestA5BWriteSkew", func(t *testing.T) {
		a5BWriteSkew(t, store)
	})
	t.Run("TestReadAfterWrite", func(t *testing.T) {
		readAfterWrite(t, store)
	})
	t.Run("TestPhantomReadInInnodb", func(t *testing.T) {
		phantomReadInInnodb(t, store)
	})
}

/*
These test cases come from the paper <A Critique of ANSI SQL Isolation Levels>.
The sign 'P0', 'P1'.... can be found in the paper. These cases will run under snapshot isolation.
*/
func TestP0DirtyWrite(t *testing.T) {
	if *withTiKV {
		t.Skip(" with tikv")
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	p0DirtyWrite(t, store)
}

func p0DirtyWrite(t *testing.T, store kv.Storage) {
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)

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
	if *withTiKV {
		t.Skip(" with tikv")
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	p1DirtyRead(t, store)
}

func p1DirtyRead(t *testing.T, store kv.Storage) {
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)

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

func TestP2NonRepeatableRead(t *testing.T) {
	if *withTiKV {
		t.Skip(" with tikv")
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	p2NonRepeatableRead(t, store)
}

func p2NonRepeatableRead(t *testing.T, store kv.Storage) {
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)

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

func TestP3Phantom(t *testing.T) {
	if *withTiKV {
		t.Skip(" with tikv")
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	p3Phantom(t, store)
}

func p3Phantom(t *testing.T, store kv.Storage) {
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)

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

func TestP4LostUpdate(t *testing.T) {
	if *withTiKV {
		t.Skip(" with tikv")
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	p4LostUpdate(t, store)
}

func p4LostUpdate(t *testing.T, store kv.Storage) {
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)

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

// cursor is not supported
func TestP4CLostUpdate(t *testing.T) {}

func TestA3Phantom(t *testing.T) {
	if *withTiKV {
		t.Skip(" with tikv")
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	a3Phantom(t, store)
}

func a3Phantom(t *testing.T, store kv.Storage) {
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)

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

func TestA5AReadSkew(t *testing.T) {
	if *withTiKV {
		t.Skip(" with tikv")
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	a5AReadSkew(t, store)
}

func a5AReadSkew(t *testing.T, store kv.Storage) {

	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)

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

func TestA5BWriteSkew(t *testing.T) {
	if *withTiKV {
		t.Skip(" with tikv")
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	a5BWriteSkew(t, store)
}

func a5BWriteSkew(t *testing.T, store kv.Storage) {
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)

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

/*
These test cases come from the paper <Highly Available Transactions: Virtues and Limitations>
for tidb, we support read-after-write on cluster level.
*/
func TestReadAfterWrite(t *testing.T) {
	if *withTiKV {
		t.Skip(" with tikv")
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	readAfterWrite(t, store)
}

func readAfterWrite(t *testing.T, store kv.Storage) {
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)

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

/*
This case will do harm in Innodb, even if in snapshot isolation, but harmless in tidb.
*/
func TestPhantomReadInInnodb(t *testing.T) {
	if *withTiKV {
		t.Skip(" with tikv")
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	phantomReadInInnodb(t, store)

}

func phantomReadInInnodb(t *testing.T, store kv.Storage) {
	session1 := testkit.NewTestKit(t, store)
	session2 := testkit.NewTestKit(t, store)

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
