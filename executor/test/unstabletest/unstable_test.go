// Copyright 2023 PingCAP, Inc.
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

package unstabletest

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/skip"
	"github.com/stretchr/testify/require"
)

func TestCartesianJoinPanic(t *testing.T) {
	skip.UnderShort(t)
	// original position at join_test.go
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("set tidb_mem_quota_query = 1 << 28")
	tk.MustExec("set global tidb_mem_oom_action = 'CANCEL'")
	tk.MustExec("set global tidb_enable_tmp_storage_on_oom = off;")
	for i := 0; i < 14; i++ {
		tk.MustExec("insert into t select * from t")
	}
	err := tk.QueryToErr("desc analyze select * from t t1, t t2, t t3, t t4, t t5, t t6;")
	require.ErrorContains(t, err, memory.PanicMemoryExceedWarnMsg+memory.WarnMsgSuffixForSingleQuery)
}

func TestGlobalMemoryControl(t *testing.T) {
	skip.UnderShort(t)
	// original position at executor_test.go
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk0 := testkit.NewTestKit(t, store)
	tk0.MustExec("set global tidb_mem_oom_action = 'cancel'")
	tk0.MustExec("set global tidb_server_memory_limit = 512 << 20")
	tk0.MustExec("set global tidb_server_memory_limit_sess_min_size = 128")

	tk1 := testkit.NewTestKit(t, store)
	tracker1 := tk1.Session().GetSessionVars().MemTracker
	tracker1.FallbackOldAndSetNewAction(&memory.PanicOnExceed{})

	tk2 := testkit.NewTestKit(t, store)
	tracker2 := tk2.Session().GetSessionVars().MemTracker
	tracker2.FallbackOldAndSetNewAction(&memory.PanicOnExceed{})

	tk3 := testkit.NewTestKit(t, store)
	tracker3 := tk3.Session().GetSessionVars().MemTracker
	tracker3.FallbackOldAndSetNewAction(&memory.PanicOnExceed{})

	sm := &testkit.MockSessionManager{
		PS: []*util.ProcessInfo{tk1.Session().ShowProcess(), tk2.Session().ShowProcess(), tk3.Session().ShowProcess()},
	}
	dom.ServerMemoryLimitHandle().SetSessionManager(sm)
	go dom.ServerMemoryLimitHandle().Run()

	tracker1.Consume(100 << 20) // 100 MB
	tracker2.Consume(200 << 20) // 200 MB
	tracker3.Consume(300 << 20) // 300 MB

	test := make([]int, 128<<20)       // Keep 1GB HeapInUse
	time.Sleep(500 * time.Millisecond) // The check goroutine checks the memory usage every 100ms. The Sleep() make sure that Top1Tracker can be Canceled.

	// Kill Top1
	require.False(t, tracker1.NeedKill.Load())
	require.False(t, tracker2.NeedKill.Load())
	require.True(t, tracker3.NeedKill.Load())
	require.Equal(t, memory.MemUsageTop1Tracker.Load(), tracker3)
	util.WithRecovery( // Next Consume() will panic and cancel the SQL
		func() {
			tracker3.Consume(1)
		}, func(r interface{}) {
			require.True(t, strings.Contains(r.(string), memory.PanicMemoryExceedWarnMsg+memory.WarnMsgSuffixForInstance))
		})
	tracker2.Consume(300 << 20) // Sum 500MB, Not Panic, Waiting t3 cancel finish.
	time.Sleep(500 * time.Millisecond)
	require.False(t, tracker2.NeedKill.Load())
	// Kill Finished
	tracker3.Consume(-(300 << 20))
	// Simulated SQL is Canceled and the time is updated
	sm.PSMu.Lock()
	ps := *sm.PS[2]
	ps.Time = time.Now()
	sm.PS[2] = &ps
	sm.PSMu.Unlock()
	time.Sleep(500 * time.Millisecond)
	// Kill the Next SQL
	util.WithRecovery( // Next Consume() will panic and cancel the SQL
		func() {
			tracker2.Consume(1)
		}, func(r interface{}) {
			require.True(t, strings.Contains(r.(string), memory.PanicMemoryExceedWarnMsg+memory.WarnMsgSuffixForInstance))
		})
	require.Equal(t, test[0], 0) // Keep 1GB HeapInUse
}

func TestAdminCheckTable(t *testing.T) {
	skip.UnderShort(t)
	// test NULL value.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_null (
		a int(11) NOT NULL,
		c int(11) NOT NULL,
		PRIMARY KEY (a, c),
		KEY idx_a (a)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin`)

	tk.MustExec(`insert into test_null(a, c) values(2, 2);`)
	tk.MustExec(`ALTER TABLE test_null ADD COLUMN b int NULL DEFAULT '1795454803' AFTER a;`)
	tk.MustExec(`ALTER TABLE test_null add index b(b);`)
	tk.MustExec("ADMIN CHECK TABLE test_null")

	// Fix unflatten issue in CheckExec.
	tk.MustExec(`drop table if exists test`)
	tk.MustExec(`create table test (
		a time,
		PRIMARY KEY (a)
		);`)

	tk.MustExec(`insert into test set a='12:10:36';`)
	tk.MustExec(`admin check table test`)

	// Test decimal
	tk.MustExec(`drop table if exists test`)
	tk.MustExec("CREATE TABLE test (  a decimal, PRIMARY KEY (a));")
	tk.MustExec("insert into test set a=10;")
	tk.MustExec("admin check table test;")

	// Test timestamp type check table.
	tk.MustExec(`drop table if exists test`)
	tk.MustExec(`create table test ( a  TIMESTAMP, primary key(a) );`)
	tk.MustExec(`insert into test set a='2015-08-10 04:18:49';`)
	tk.MustExec(`admin check table test;`)

	// Test partitioned table.
	tk.MustExec(`drop table if exists test`)
	tk.MustExec(`create table test (
		      a int not null,
		      c int not null,
		      primary key (a, c),
		      key idx_a (a)) partition by range (c) (
		      partition p1 values less than (1),
		      partition p2 values less than (4),
		      partition p3 values less than (7),
		      partition p4 values less than (11))`)
	for i := 1; i <= 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into test values (%d, %d);", i, i))
	}
	tk.MustExec(`admin check table test;`)

	// Test index in virtual generated column.
	tk.MustExec(`drop table if exists test`)
	tk.MustExec(`create table test ( b json , c int as (JSON_EXTRACT(b,'$.d')), index idxc(c));`)
	tk.MustExec(`INSERT INTO test set b='{"d": 100}';`)
	tk.MustExec(`admin check table test;`)
	// Test prefix index.
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`CREATE TABLE t (
	  			ID CHAR(32) NOT NULL,
	  			name CHAR(32) NOT NULL,
	  			value CHAR(255),
	  			INDEX indexIDname (ID(8),name(8)));`)
	tk.MustExec(`INSERT INTO t VALUES ('keyword','urlprefix','text/ /text');`)
	tk.MustExec(`admin check table t;`)

	tk.MustExec("use mysql")
	tk.MustExec(`admin check table test.t;`)
	err := tk.ExecToErr("admin check table t")
	require.Error(t, err)

	// test add index on time type column which have default value
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`CREATE TABLE t1 (c2 YEAR, PRIMARY KEY (c2))`)
	tk.MustExec(`INSERT INTO t1 SET c2 = '1912'`)
	tk.MustExec(`ALTER TABLE t1 ADD COLUMN c3 TIMESTAMP NULL DEFAULT '1976-08-29 16:28:11'`)
	tk.MustExec(`ALTER TABLE t1 ADD COLUMN c4 DATE      NULL DEFAULT '1976-08-29'`)
	tk.MustExec(`ALTER TABLE t1 ADD COLUMN c5 TIME      NULL DEFAULT '16:28:11'`)
	tk.MustExec(`ALTER TABLE t1 ADD COLUMN c6 YEAR      NULL DEFAULT '1976'`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx1 (c2, c3,c4,c5,c6)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx2 (c2)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx3 (c3)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx4 (c4)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx5 (c5)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx6 (c6)`)
	tk.MustExec(`admin check table t1`)

	// Test add index on decimal column.
	tk.MustExec(`drop table if exists td1;`)
	tk.MustExec(`CREATE TABLE td1 (c2 INT NULL DEFAULT '70');`)
	tk.MustExec(`INSERT INTO td1 SET c2 = '5';`)
	tk.MustExec(`ALTER TABLE td1 ADD COLUMN c4 DECIMAL(12,8) NULL DEFAULT '213.41598062';`)
	tk.MustExec(`ALTER TABLE td1 ADD INDEX id2 (c4) ;`)
	tk.MustExec(`ADMIN CHECK TABLE td1;`)

	// Test add not null column, then add index.
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1 (a int);`)
	tk.MustExec(`insert into t1 set a=2;`)
	tk.MustExec(`alter table t1 add column b timestamp not null;`)
	tk.MustExec(`alter table t1 add index(b);`)
	tk.MustExec(`admin check table t1;`)

	// Test for index with change decimal precision.
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1 (a decimal(2,1), index(a))`)
	tk.MustExec(`insert into t1 set a='1.9'`)
	err = tk.ExecToErr(`alter table t1 modify column a decimal(3,2);`)
	require.NoError(t, err)
	tk.MustExec(`delete from t1;`)
	tk.MustExec(`admin check table t1;`)
}
