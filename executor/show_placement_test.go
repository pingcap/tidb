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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestShowPlacement(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2, t3, t4, db2.t2")
	tk.MustExec("drop database if exists db2")
	tk.MustExec("drop database if exists db3")
	tk.MustExec("drop placement policy if exists pa1")
	tk.MustExec("drop placement policy if exists pa2")
	tk.MustExec("drop placement policy if exists pb1")

	// prepare policies
	tk.MustExec("create placement policy pa2 " +
		"LEADER_CONSTRAINTS=\"[+region=us-east-1]\" " +
		"FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" " +
		"FOLLOWERS=3")
	defer tk.MustExec("drop placement policy pa2")

	tk.MustExec("create placement policy pa1 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1,cn-east-2\"" +
		"SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy pa1")

	tk.MustExec("create placement policy pb1 " +
		"VOTER_CONSTRAINTS=\"[+region=bj]\" " +
		"LEARNER_CONSTRAINTS=\"[+region=sh]\" " +
		"CONSTRAINTS=\"[+disk=ssd]\"" +
		"VOTERS=5 " +
		"LEARNERS=3")
	defer tk.MustExec("drop placement policy pb1")

	// prepare database
	tk.MustExec("create database db2 PLACEMENT POLICY pa2")
	defer tk.MustExec("drop database if exists db2")

	// prepare tables
	tk.MustExec("create table t1 (id int) placement policy pa1")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec("create table t2 (id int)")
	defer tk.MustExec("drop table if exists t2")

	tk.MustExec("CREATE TABLE t3 (id INT) placement policy pa1 PARTITION BY RANGE (id) (" +
		"PARTITION p0 VALUES LESS THAN (100) placement policy pa2," +
		"PARTITION p1 VALUES LESS THAN (1000)," +
		"PARTITION p2 VALUES LESS THAN (10000)" +
		")")
	defer tk.MustExec("drop table if exists t3")

	tk.MustExec("create table db2.t2 (id int) PLACEMENT POLICY pa2")
	defer tk.MustExec("drop table if exists db2.t2")

	tk.MustQuery("show placement").Check(testkit.Rows(
		"POLICY pa1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" NULL",
		"POLICY pa2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" NULL",
		"POLICY pb1 CONSTRAINTS=\"[+disk=ssd]\" VOTERS=5 VOTER_CONSTRAINTS=\"[+region=bj]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=sh]\" NULL",
		"DATABASE db2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" PENDING",
		"TABLE db2.t2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" PENDING",
		"TABLE test.t1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" PENDING",
		"TABLE test.t3 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" PENDING",
		"TABLE test.t3 PARTITION p0 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" PENDING",
		"TABLE test.t3 PARTITION p1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" PENDING",
		"TABLE test.t3 PARTITION p2 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" PENDING",
	))

	tk.MustQuery("show placement like 'POLICY%'").Check(testkit.Rows(
		"POLICY pa1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" NULL",
		"POLICY pa2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" NULL",
		"POLICY pb1 CONSTRAINTS=\"[+disk=ssd]\" VOTERS=5 VOTER_CONSTRAINTS=\"[+region=bj]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=sh]\" NULL",
	))

	tk.MustQuery("show placement like 'POLICY pa%'").Check(testkit.Rows(
		"POLICY pa1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" NULL",
		"POLICY pa2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" NULL",
	))

	tk.MustQuery("show placement where Target='POLICY pb1'").Check(testkit.Rows(
		"POLICY pb1 CONSTRAINTS=\"[+disk=ssd]\" VOTERS=5 VOTER_CONSTRAINTS=\"[+region=bj]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=sh]\" NULL",
	))
}

func TestShowPlacementPrivilege(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2,t3, db2.t1, db2.t3")
	tk.MustExec("drop database if exists db2")
	tk.MustExec("drop database if exists db3")
	tk.MustExec("drop user if exists user1")
	tk.MustExec("drop placement policy if exists p1")

	// prepare user
	tk.MustExec("create user user1")
	defer tk.MustExec("drop user user1")

	// prepare policy
	tk.MustExec("create placement policy p1 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1,cn-east-2\"" +
		"SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy p1")

	// prepare database
	tk.MustExec("create database db3 PLACEMENT POLICY p1")
	defer tk.MustExec("drop database if exists db3")

	tk.MustExec("create database db2 PLACEMENT POLICY p1")
	defer tk.MustExec("drop database if exists db2")

	// prepare tables
	tk.MustExec("create table t1 (id int) placement policy p1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t2 (id int) PLACEMENT POLICY p1")
	defer tk.MustExec("drop table if exists t2")
	tk.MustExec("CREATE TABLE t3 (id INT) PARTITION BY RANGE (id) (" +
		"PARTITION p1 VALUES LESS THAN (100) PLACEMENT POLICY p1" +
		")")
	defer tk.MustExec("drop table if exists t3")
	tk.MustExec("create table db2.t1 (id int) PLACEMENT POLICY p1")
	defer tk.MustExec("drop table if exists db2.t1")
	tk.MustExec("create table db2.t3 (id int) PLACEMENT POLICY p1")
	defer tk.MustExec("drop table if exists db2.t3")

	tk1 := testkit.NewTestKit(t, store)
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "user1", Hostname: "%"}, nil, nil))

	// before grant
	tk1.MustQuery("show placement").Check(testkit.Rows(
		"POLICY p1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" NULL",
	))

	// do some grant
	tk.MustExec(`grant select on test.t1 to 'user1'@'%'`)
	tk.MustExec(`grant select on test.t3 to 'user1'@'%'`)
	tk.MustExec(`grant select on db2.t1 to 'user1'@'%'`)

	// after grant
	tk1.MustQuery("show placement").Check(testkit.Rows(
		"POLICY p1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" NULL",
		"DATABASE db2 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" PENDING",
		"TABLE db2.t1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" PENDING",
		"TABLE test.t1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" PENDING",
		"TABLE test.t3 PARTITION p1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" PENDING",
	))
}

func TestShowPlacementForDB(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop database if exists db2")
	tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p1 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1,cn-east-2\"" +
		"SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy p1")

	tk.MustExec("create database db2 placement policy p1")
	defer tk.MustExec("drop database if exists db2")

	err := tk.QueryToErr("show placement for database dbnoexist")
	require.EqualError(t, err, "[schema:1049]Unknown database 'dbnoexist'")

	tk.MustQuery("show placement for database test").Check(testkit.Rows())
	tk.MustQuery("show placement for database db2").Check(testkit.Rows(
		"DATABASE db2 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" SCHEDULED",
	))
}

func TestShowPlacementForTableAndPartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("drop table if exists t1,t2,t3,t4,db2.t1")
	tk.MustExec("drop database if exists db2")

	tk.MustExec("create placement policy p1 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1,cn-east-2\"" +
		"SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy p1")

	tk.MustExec("create placement policy p2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=2")
	defer tk.MustExec("drop placement policy p2")

	// table ref a policy
	tk.MustExec("create table t1 (id int) placement policy p1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustQuery("show placement for table t1").Check(testkit.Rows(
		"TABLE test.t1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" PENDING",
	))

	// table no placement
	tk.MustExec("create table t3 (id int)")
	defer tk.MustExec("drop table if exists t3")
	tk.MustQuery("show placement for table t3").Check(testkit.Rows())

	// table do not display partition placement
	tk.MustExec("create table t4 (id int) PLACEMENT POLICY p2 PARTITION BY RANGE (id) (" +
		"PARTITION p0 VALUES LESS THAN (100)," +
		"PARTITION p1 VALUES LESS THAN (1000)," +
		"PARTITION p2 VALUES LESS THAN (10000) PLACEMENT POLICY p1" +
		")")
	defer tk.MustExec("drop table if exists t4")
	tk.MustQuery("show placement for table t4").Check(testkit.Rows(
		"TABLE test.t4 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=2 PENDING",
	))

	// partition inherent table
	tk.MustQuery("show placement for table t4 partition p0").Check(testkit.Rows(
		"TABLE test.t4 PARTITION p0 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=2 PENDING",
	))

	// partition custom placement
	tk.MustQuery("show placement for table t4 partition p2").Check(testkit.Rows(
		"TABLE test.t4 PARTITION p2 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" PENDING",
	))

	// partition without placement
	tk.MustExec("create table t5 (id int) PARTITION BY RANGE (id) (" +
		"PARTITION p0 VALUES LESS THAN (100)" +
		")")
	tk.MustQuery("show placement for table t5 partition p0").Check(testkit.Rows())

	// table name with format db.table
	tk.MustExec("create database db2")
	defer tk.MustExec("drop database if exists db2")
	tk.MustExec("create table db2.t1 (id int) PLACEMENT POLICY p2")
	defer tk.MustExec("drop table if exists db2.t1")
	tk.MustQuery("show placement for table db2.t1").Check(testkit.Rows(
		"TABLE db2.t1 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=2 PENDING",
	))

	// not exists
	err := tk.ExecToErr("show placement for table tn")
	require.EqualError(t, err, "[schema:1146]Table 'test.tn' doesn't exist")
	err = tk.ExecToErr("show placement for table dbn.t1")
	require.EqualError(t, err, "[schema:1146]Table 'dbn.t1' doesn't exist")
	err = tk.ExecToErr("show placement for table tn partition pn")
	require.EqualError(t, err, "[schema:1146]Table 'test.tn' doesn't exist")
	err = tk.QueryToErr("show placement for table t1 partition pn")
	require.EqualError(t, err, "[table:1735]Unknown partition 'pn' in table 't1'")
	err = tk.QueryToErr("show placement for table t4 partition pn")
	require.EqualError(t, err, "[table:1735]Unknown partition 'pn' in table 't4'")
}

func TestShowPlacementForDBPrivilege(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists db2.t1")
	tk.MustExec("drop database if exists db2")
	tk.MustExec("drop user if exists user1")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	// prepare policy
	tk.MustExec("create placement policy p1 PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("create placement policy p2 PRIMARY_REGION=\"r1\" REGIONS=\"r1,r3\" SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy if exists p2")

	// prepare user
	tk.MustExec("create user user1")
	defer tk.MustExec("drop user user1")

	// prepare database
	tk.MustExec("create database db2 PLACEMENT POLICY p1")
	defer tk.MustExec("drop database db2")

	// prepare table
	tk.MustExec("create table db2.t1 (id int) PLACEMENT POLICY p2")
	defer tk.MustExec("drop table db2.t1")

	tk1 := testkit.NewTestKit(t, store)
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "user1", Hostname: "%"}, nil, nil))

	privs := []string{
		"all privileges on db2.*",
		"create on db2.*",
		"alter on db2.*",
		"drop on db2.*",
		"select on db2.t1",
		"insert on db2.t1",
		"create on db2.t1",
		"delete on db2.t1",
	}

	// before grant
	err := tk1.QueryToErr("show placement for database db2")
	require.EqualError(t, err, executor.ErrDBaccessDenied.GenWithStackByArgs("user1", "%", "db2").Error())

	tk1.MustQuery("show placement").Check(testkit.Rows(
		"POLICY p1 PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" SCHEDULE=\"EVEN\" NULL",
		"POLICY p2 PRIMARY_REGION=\"r1\" REGIONS=\"r1,r3\" SCHEDULE=\"EVEN\" NULL",
	))

	for _, priv := range privs {
		// do grant
		tk.MustExec(fmt.Sprintf("grant %s to 'user1'@'%%'", priv))
		tk1.MustQuery("show placement for database db2").Check(testkit.Rows(
			"DATABASE db2 PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" SCHEDULE=\"EVEN\" PENDING",
		))

		tk1.MustQuery("show placement").Check(testkit.Rows(
			"POLICY p1 PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" SCHEDULE=\"EVEN\" NULL",
			"POLICY p2 PRIMARY_REGION=\"r1\" REGIONS=\"r1,r3\" SCHEDULE=\"EVEN\" NULL",
			"DATABASE db2 PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" SCHEDULE=\"EVEN\" PENDING",
			"TABLE db2.t1 PRIMARY_REGION=\"r1\" REGIONS=\"r1,r3\" SCHEDULE=\"EVEN\" PENDING",
		))

		err = tk1.QueryToErr("show placement for database test")
		require.EqualError(t, err, executor.ErrDBaccessDenied.GenWithStackByArgs("user1", "%", "test").Error())

		// do revoke
		tk.MustExec(fmt.Sprintf("revoke %s from 'user1'@'%%'", priv))
		err = tk1.QueryToErr("show placement for database db2")
		require.EqualError(t, err, executor.ErrDBaccessDenied.GenWithStackByArgs("user1", "%", "db2").Error())

		tk1.MustQuery("show placement").Check(testkit.Rows(
			"POLICY p1 PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" SCHEDULE=\"EVEN\" NULL",
			"POLICY p2 PRIMARY_REGION=\"r1\" REGIONS=\"r1,r3\" SCHEDULE=\"EVEN\" NULL",
		))
	}
}

func TestShowPlacementForTableAndPartitionPrivilege(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2,t3,t4,db2.t1")
	tk.MustExec("drop database if exists db2")
	tk.MustExec("drop user if exists user1")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	// prepare user
	tk.MustExec("create user user1")
	defer tk.MustExec("drop user user1")

	// prepare database
	tk.MustExec("create database db2")
	defer tk.MustExec("drop database db2")

	// prepare policy
	tk.MustExec("create placement policy p1 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1,cn-east-2\"" +
		"SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy p1")

	tk.MustExec("create placement policy p2 LEADER_CONSTRAINTS=\"[+region=bj]\" FOLLOWER_CONSTRAINTS=\"[+region=sh]\" FOLLOWERS=4")
	defer tk.MustExec("drop placement policy p2")

	// prepare tables
	tk.MustExec("create table t1 (id int) placement policy p1 PARTITION BY RANGE (id) (" +
		"PARTITION p1 VALUES LESS THAN (1000) PLACEMENT POLICY p2" +
		")")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t2 (id int) PLACEMENT POLICY p2")
	defer tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t3 (id int)")
	defer tk.MustExec("drop table if exists t3")
	tk.MustExec("create table db2.t1 (id int) PLACEMENT POLICY p2")
	defer tk.MustExec("drop table if exists db2.t1")

	tk1 := testkit.NewTestKit(t, store)
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "user1", Hostname: "%"}, nil, nil))

	// before grant
	err := tk1.ExecToErr("show placement for table test.t1")
	require.EqualError(t, err, core.ErrTableaccessDenied.GenWithStackByArgs("SHOW", "user1", "%", "t1").Error())

	err = tk1.ExecToErr("show placement for table test.t1 partition p1")
	require.EqualError(t, err, core.ErrTableaccessDenied.GenWithStackByArgs("SHOW", "user1", "%", "t1").Error())

	err = tk1.ExecToErr("show placement for table test.t2")
	require.EqualError(t, err, core.ErrTableaccessDenied.GenWithStackByArgs("SHOW", "user1", "%", "t2").Error())

	err = tk1.ExecToErr("show placement for table test.t3")
	require.EqualError(t, err, core.ErrTableaccessDenied.GenWithStackByArgs("SHOW", "user1", "%", "t3").Error())

	err = tk1.ExecToErr("show placement for table db2.t1")
	require.EqualError(t, err, core.ErrTableaccessDenied.GenWithStackByArgs("SHOW", "user1", "%", "t1").Error())

	tk1.MustQuery("show placement").Check(testkit.Rows(
		"POLICY p1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" NULL",
		"POLICY p2 LEADER_CONSTRAINTS=\"[+region=bj]\" FOLLOWERS=4 FOLLOWER_CONSTRAINTS=\"[+region=sh]\" NULL",
	))

	privs := []string{
		"create on test.t1",
		"alter on test.t1",
		"drop on test.t1",
		"select on test.t1",
		"insert on test.t1",
		"create on test.t1",
		"delete on test.t1",
	}

	for _, priv := range privs {
		// do grant
		tk.MustExec(fmt.Sprintf("grant %s to 'user1'@'%%'", priv))
		tk1.MustQuery("show placement").Check(testkit.Rows(
			"POLICY p1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" NULL",
			"POLICY p2 LEADER_CONSTRAINTS=\"[+region=bj]\" FOLLOWERS=4 FOLLOWER_CONSTRAINTS=\"[+region=sh]\" NULL",
			"TABLE test.t1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" PENDING",
			"TABLE test.t1 PARTITION p1 LEADER_CONSTRAINTS=\"[+region=bj]\" FOLLOWERS=4 FOLLOWER_CONSTRAINTS=\"[+region=sh]\" PENDING",
		))

		tk1.MustQuery("show placement for table test.t1").Check(testkit.Rows(
			"TABLE test.t1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" PENDING",
		))

		tk1.MustQuery("show placement for table test.t1 partition p1").Check(testkit.Rows(
			"TABLE test.t1 PARTITION p1 LEADER_CONSTRAINTS=\"[+region=bj]\" FOLLOWERS=4 FOLLOWER_CONSTRAINTS=\"[+region=sh]\" PENDING",
		))

		err = tk1.ExecToErr("show placement for table test.t2")
		require.EqualError(t, err, core.ErrTableaccessDenied.GenWithStackByArgs("SHOW", "user1", "%", "t2").Error())

		err = tk1.ExecToErr("show placement for table test.t3")
		require.EqualError(t, err, core.ErrTableaccessDenied.GenWithStackByArgs("SHOW", "user1", "%", "t3").Error())

		err = tk1.ExecToErr("show placement for table db2.t1")
		require.EqualError(t, err, core.ErrTableaccessDenied.GenWithStackByArgs("SHOW", "user1", "%", "t1").Error())

		// do revoke
		tk.MustExec(fmt.Sprintf("revoke %s from 'user1'@'%%'", priv))
		err = tk1.ExecToErr("show placement for table test.t1")
		require.EqualError(t, err, core.ErrTableaccessDenied.GenWithStackByArgs("SHOW", "user1", "%", "t1").Error())

		tk1.MustQuery("show placement").Check(testkit.Rows(
			"POLICY p1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" NULL",
			"POLICY p2 LEADER_CONSTRAINTS=\"[+region=bj]\" FOLLOWERS=4 FOLLOWER_CONSTRAINTS=\"[+region=sh]\" NULL",
		))
	}
}
