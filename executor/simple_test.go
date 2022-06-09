// Copyright 2016 PingCAP, Inc.
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
	"strconv"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
)

func TestKillStmt(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.EnableGlobalKill = false
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	sm := &mockSessionManager{
		serverID: 0,
	}
	tk.Session().SetSessionManager(sm)
	tk.MustExec("kill 1")
	result := tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] connectionID' instead"))

	newCfg2 := *originCfg
	newCfg2.EnableGlobalKill = true
	config.StoreGlobalConfig(&newCfg2)

	// ZERO serverID, treated as truncated.
	tk.MustExec("kill 1")
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID."))

	// truncated
	sm.SetServerID(1)
	tk.MustExec("kill 101")
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID."))

	// excceed int64
	tk.MustExec("kill 9223372036854775808") // 9223372036854775808 == 2^63
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Parse ConnectionID failed: Unexpected connectionID excceeds int64"))

	// local kill
	connID := util.NewGlobalConnID(1, true)
	tk.MustExec("kill " + strconv.FormatUint(connID.ID(), 10))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows())

	// remote kill is tested in `tests/globalkilltest`
}

func TestFlushPrivileges(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`CREATE USER 'testflush'@'localhost' IDENTIFIED BY '';`)
	tk.MustExec(`UPDATE mysql.User SET Select_priv='Y' WHERE User="testflush" and Host="localhost"`)

	// Create a new session.
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.True(t, se.Auth(&auth.UserIdentity{Username: "testflush", Hostname: "localhost"}, nil, nil))

	ctx := context.Background()
	// Before flush.
	_, err = se.Execute(ctx, `SELECT authentication_string FROM mysql.User WHERE User="testflush" and Host="localhost"`)
	require.Error(t, err)

	tk.MustExec("FLUSH PRIVILEGES")

	// After flush.
	_, err = se.Execute(ctx, `SELECT authentication_string FROM mysql.User WHERE User="testflush" and Host="localhost"`)
	require.NoError(t, err)

}

func TestFlushPrivilegesPanic(t *testing.T) {
	// Run in a separate suite because this test need to set SkipGrantTable config.
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Security.SkipGrantTable = true
	})

	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("FLUSH PRIVILEGES")
}

func TestDropPartitionStats(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	// Use the testSerialSuite to fix the unstable test
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database if not exists test_drop_gstats`)
	tk.MustExec("use test_drop_gstats")
	tk.MustExec("drop table if exists test_drop_gstats;")
	tk.MustExec(`create table test_drop_gstats (
	a int,
	key(a)
)
partition by range (a) (
	partition p0 values less than (10),
	partition p1 values less than (20),
	partition global values less than (30)
)`)
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("insert into test_drop_gstats values (1), (5), (11), (15), (21), (25)")
	require.Nil(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))

	checkPartitionStats := func(names ...string) {
		rs := tk.MustQuery("show stats_meta").Rows()
		require.Equal(t, len(names), len(rs))
		for i := range names {
			require.Equal(t, names[i], rs[i][2].(string))
		}
	}

	tk.MustExec("analyze table test_drop_gstats")
	checkPartitionStats("global", "p0", "p1", "global")

	tk.MustExec("drop stats test_drop_gstats partition p0")
	checkPartitionStats("global", "p1", "global")

	err := tk.ExecToErr("drop stats test_drop_gstats partition abcde")
	require.Error(t, err)
	require.Equal(t, "can not found the specified partition name abcde in the table definition", err.Error())

	tk.MustExec("drop stats test_drop_gstats partition global")
	checkPartitionStats("global", "p1")

	tk.MustExec("drop stats test_drop_gstats global")
	checkPartitionStats("p1")

	tk.MustExec("analyze table test_drop_gstats")
	checkPartitionStats("global", "p0", "p1", "global")

	tk.MustExec("drop stats test_drop_gstats partition p0, p1, global")
	checkPartitionStats("global")

	tk.MustExec("analyze table test_drop_gstats")
	checkPartitionStats("global", "p0", "p1", "global")

	tk.MustExec("drop stats test_drop_gstats")
	checkPartitionStats()
}

func TestDropStats(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := dom.StatsHandle()
	h.Clear()
	testKit.MustExec("analyze table t")
	statsTbl := h.GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)

	testKit.MustExec("drop stats t")
	require.Nil(t, h.Update(is))
	statsTbl = h.GetTableStats(tableInfo)
	require.True(t, statsTbl.Pseudo)

	testKit.MustExec("analyze table t")
	statsTbl = h.GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)

	h.SetLease(1)
	testKit.MustExec("drop stats t")
	require.Nil(t, h.Update(is))
	statsTbl = h.GetTableStats(tableInfo)
	require.True(t, statsTbl.Pseudo)
	h.SetLease(0)
}

func TestUseDB(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	err := tk.ExecToErr("USE ``")
	require.Truef(t, terror.ErrorEqual(core.ErrNoDB, err), "err %v", err)
}

func TestStmtAutoNewTxn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// Some statements are like DDL, they commit the previous txn automically.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Fix issue https://github.com/pingcap/tidb/issues/10705
	tk.MustExec("begin")
	tk.MustExec("create user 'xxx'@'%';")
	tk.MustExec("grant all privileges on *.* to 'xxx'@'%';")

	tk.MustExec("create table auto_new (id int)")
	tk.MustExec("begin")
	tk.MustExec("insert into auto_new values (1)")
	tk.MustExec("revoke all privileges on *.* from 'xxx'@'%'")
	tk.MustExec("rollback") // insert statement has already committed
	tk.MustQuery("select * from auto_new").Check(testkit.Rows("1"))

	// Test the behavior when autocommit is false.
	tk.MustExec("set autocommit = 0")
	tk.MustExec("insert into auto_new values (2)")
	tk.MustExec("create user 'yyy'@'%'")
	tk.MustExec("rollback")
	tk.MustQuery("select * from auto_new").Check(testkit.Rows("1", "2"))

	tk.MustExec("drop user 'yyy'@'%'")
	tk.MustExec("insert into auto_new values (3)")
	tk.MustExec("rollback")
	tk.MustQuery("select * from auto_new").Check(testkit.Rows("1", "2"))
}

func TestIssue9111(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// CREATE USER / DROP USER fails if admin doesn't have insert privilege on `mysql.user` table.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user 'user_admin'@'localhost';")
	tk.MustExec("grant create user on *.* to 'user_admin'@'localhost';")

	// Create a new session.
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.True(t, se.Auth(&auth.UserIdentity{Username: "user_admin", Hostname: "localhost"}, nil, nil))

	ctx := context.Background()
	_, err = se.Execute(ctx, `create user test_create_user`)
	require.NoError(t, err)
	_, err = se.Execute(ctx, `drop user test_create_user`)
	require.NoError(t, err)

	tk.MustExec("revoke create user on *.* from 'user_admin'@'localhost';")
	tk.MustExec("grant insert, delete on mysql.user to 'user_admin'@'localhost';")

	_, err = se.Execute(ctx, `create user test_create_user`)
	require.NoError(t, err)
	_, err = se.Execute(ctx, `drop user test_create_user`)
	require.NoError(t, err)

	_, err = se.Execute(ctx, `create role test_create_user`)
	require.NoError(t, err)
	_, err = se.Execute(ctx, `drop role test_create_user`)
	require.NoError(t, err)

	tk.MustExec("drop user 'user_admin'@'localhost';")
}

func TestRoleAtomic(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create role r2;")
	err := tk.ExecToErr("create role r1, r2, r3")
	require.Error(t, err)
	// Check atomic create role.
	result := tk.MustQuery(`SELECT user FROM mysql.User WHERE user in ('r1', 'r2', 'r3')`)
	result.Check(testkit.Rows("r2"))
	// Check atomic drop role.
	err = tk.ExecToErr("drop role r1, r2, r3")
	require.Error(t, err)
	result = tk.MustQuery(`SELECT user FROM mysql.User WHERE user in ('r1', 'r2', 'r3')`)
	result.Check(testkit.Rows("r2"))
	tk.MustExec("drop role r2;")
}

func TestExtendedStatsPrivileges(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create user 'u1'@'%'")
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.True(t, se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil))
	ctx := context.Background()
	_, err = se.Execute(ctx, "set session tidb_enable_extended_stats = on")
	require.NoError(t, err)
	_, err = se.Execute(ctx, "alter table test.t add stats_extended s1 correlation(a,b)")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]ALTER command denied to user 'u1'@'%' for table 't'", err.Error())
	tk.MustExec("grant alter on test.* to 'u1'@'%'")
	_, err = se.Execute(ctx, "alter table test.t add stats_extended s1 correlation(a,b)")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]ADD STATS_EXTENDED command denied to user 'u1'@'%' for table 't'", err.Error())
	tk.MustExec("grant select on test.* to 'u1'@'%'")
	_, err = se.Execute(ctx, "alter table test.t add stats_extended s1 correlation(a,b)")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]ADD STATS_EXTENDED command denied to user 'u1'@'%' for table 'stats_extended'", err.Error())
	tk.MustExec("grant insert on mysql.stats_extended to 'u1'@'%'")
	_, err = se.Execute(ctx, "alter table test.t add stats_extended s1 correlation(a,b)")
	require.NoError(t, err)

	_, err = se.Execute(ctx, "use test")
	require.NoError(t, err)
	_, err = se.Execute(ctx, "alter table t drop stats_extended s1")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]DROP STATS_EXTENDED command denied to user 'u1'@'%' for table 'stats_extended'", err.Error())
	tk.MustExec("grant update on mysql.stats_extended to 'u1'@'%'")
	_, err = se.Execute(ctx, "alter table t drop stats_extended s1")
	require.NoError(t, err)
	tk.MustExec("drop user 'u1'@'%'")
}

func TestIssue17247(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user 'issue17247'")
	tk.MustExec("grant CREATE USER on *.* to 'issue17247'")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "issue17247", Hostname: "%"}, nil, nil))
	tk1.MustExec("ALTER USER USER() IDENTIFIED BY 'xxx'")
	tk1.MustExec("ALTER USER CURRENT_USER() IDENTIFIED BY 'yyy'")
	tk1.MustExec("ALTER USER CURRENT_USER IDENTIFIED BY 'zzz'")
	tk.MustExec("ALTER USER 'issue17247'@'%' IDENTIFIED BY 'kkk'")
	tk.MustExec("ALTER USER 'issue17247'@'%' IDENTIFIED BY PASSWORD '*B50FBDB37F1256824274912F2A1CE648082C3F1F'")
	// Wrong grammar
	_, err := tk1.Exec("ALTER USER USER() IDENTIFIED BY PASSWORD '*B50FBDB37F1256824274912F2A1CE648082C3F1F'")
	require.Error(t, err)
}

// Close issue #23649.
// See https://github.com/pingcap/tidb/issues/23649
func TestIssue23649(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("DROP USER IF EXISTS issue23649;")
	tk.MustExec("CREATE USER issue23649;")
	err := tk.ExecToErr("GRANT bogusrole to issue23649;")
	require.Equal(t, "[executor:3523]Unknown authorization ID `bogusrole`@`%`", err.Error())
	err = tk.ExecToErr("GRANT bogusrole to nonexisting;")
	require.Equal(t, "[executor:3523]Unknown authorization ID `bogusrole`@`%`", err.Error())
}

func TestSetCurrentUserPwd(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER issue28534;")
	defer func() {
		tk.MustExec("DROP USER IF EXISTS issue28534;")
	}()

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "issue28534", Hostname: "localhost", CurrentUser: true, AuthUsername: "issue28534", AuthHostname: "%"}, nil, nil))
	tk.MustExec(`SET PASSWORD FOR CURRENT_USER() = "43582eussi"`)

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	result := tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="issue28534"`)
	result.Check(testkit.Rows(auth.EncodePassword("43582eussi")))
}

func TestShowGrantsAfterDropRole(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER u29473")
	defer tk.MustExec("DROP USER IF EXISTS u29473")

	tk.MustExec("CREATE ROLE r29473")
	tk.MustExec("GRANT r29473 TO u29473")
	tk.MustExec("GRANT CREATE USER ON *.* TO u29473")

	tk.Session().Auth(&auth.UserIdentity{Username: "u29473", Hostname: "%"}, nil, nil)
	tk.MustExec("SET ROLE r29473")
	tk.MustExec("DROP ROLE r29473")
	tk.MustQuery("SHOW GRANTS").Check(testkit.Rows("GRANT CREATE USER ON *.* TO 'u29473'@'%'"))
}

func TestPrivilegesAfterDropUser(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int, v int)")
	defer tk.MustExec("drop table t1")

	tk.MustExec("CREATE USER u1 require ssl")
	defer tk.MustExec("DROP USER IF EXISTS u1")

	tk.MustExec("GRANT CREATE ON test.* TO u1")
	tk.MustExec("GRANT UPDATE ON test.t1 TO u1")
	tk.MustExec("GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO u1")
	tk.MustExec("GRANT SELECT(v), UPDATE(v) on test.t1 TO u1")

	tk.MustQuery("SELECT COUNT(1) FROM mysql.global_grants WHERE USER='u1' AND HOST='%'").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT COUNT(1) FROM mysql.global_priv WHERE USER='u1' AND HOST='%'").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT COUNT(1) FROM mysql.tables_priv WHERE USER='u1' AND HOST='%'").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT COUNT(1) FROM mysql.columns_priv WHERE USER='u1' AND HOST='%'").Check(testkit.Rows("1"))
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil)
	tk.MustQuery("SHOW GRANTS FOR u1").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'u1'@'%'",
		"GRANT CREATE ON test.* TO 'u1'@'%'",
		"GRANT UPDATE ON test.t1 TO 'u1'@'%'",
		"GRANT SELECT(v), UPDATE(v) ON test.t1 TO 'u1'@'%'",
		"GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'u1'@'%'",
	))

	tk.MustExec("DROP USER u1")
	err := tk.QueryToErr("SHOW GRANTS FOR u1")
	require.Equal(t, "[privilege:1141]There is no such grant defined for user 'u1' on host '%'", err.Error())
	tk.MustQuery("SELECT * FROM mysql.global_grants WHERE USER='u1' AND HOST='%'").Check(testkit.Rows())
	tk.MustQuery("SELECT * FROM mysql.global_priv WHERE USER='u1' AND HOST='%'").Check(testkit.Rows())
	tk.MustQuery("SELECT * FROM mysql.tables_priv WHERE USER='u1' AND HOST='%'").Check(testkit.Rows())
	tk.MustQuery("SELECT * FROM mysql.columns_priv WHERE USER='u1' AND HOST='%'").Check(testkit.Rows())
}

func TestDropRoleAfterRevoke(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// issue 29781
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil)

	tk.MustExec("create role r1, r2, r3;")
	defer tk.MustExec("drop role if exists r1, r2, r3;")
	tk.MustExec("grant r1,r2,r3 to current_user();")
	tk.MustExec("set role all;")
	tk.MustExec("revoke r1, r3 from root;")
	tk.MustExec("drop role r1;")
}

func TestUserWithSetNames(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set names gbk;")

	tk.MustExec("drop user if exists '\xd2\xbb'@'localhost';")
	tk.MustExec("create user '\xd2\xbb'@'localhost' IDENTIFIED BY '\xd2\xbb';")

	result := tk.MustQuery("SELECT authentication_string FROM mysql.User WHERE User='\xd2\xbb' and Host='localhost';")
	result.Check(testkit.Rows(auth.EncodePassword("一")))

	tk.MustExec("ALTER USER '\xd2\xbb'@'localhost' IDENTIFIED BY '\xd2\xbb\xd2\xbb';")
	result = tk.MustQuery("SELECT authentication_string FROM mysql.User WHERE User='\xd2\xbb' and Host='localhost';")
	result.Check(testkit.Rows(auth.EncodePassword("一一")))

	tk.MustExec("RENAME USER '\xd2\xbb'@'localhost' to '\xd2\xbb'")

	tk.MustExec("drop user '\xd2\xbb';")
}

func TestStatementsCauseImplicitCommit(t *testing.T) {
	// Test some of the implicit commit statements.
	// See https://dev.mysql.com/doc/refman/5.7/en/implicit-commit.html
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table ic (id int primary key)")

	cases := []string{
		"create table xx (id int)",
		"create user 'xx'@'127.0.0.1'",
		"grant SELECT on test.ic to 'xx'@'127.0.0.1'",
		"flush privileges",
		"analyze table ic",
	}
	for i, sql := range cases {
		tk.MustExec("begin")
		tk.MustExec("insert into ic values (?)", i)
		tk.MustExec(sql)
		tk.MustQuery("select * from ic where id = ?", i).Check(testkit.Rows(strconv.FormatInt(int64(i), 10)))
		// Clean up data
		tk.MustExec("delete from ic")
	}
}
