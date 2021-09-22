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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite5) TestShowPlacement(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists pa1")
	tk.MustExec("drop placement policy if exists pa2")
	tk.MustExec("drop placement policy if exists pb1")
	tk.MustExec("drop table if exists t1, t2, db2.t2")
	tk.MustExec("drop database if exists db2")

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
	tk.MustExec("create database db2")
	defer tk.MustExec("drop database if exists db2")

	// prepare tables
	tk.MustExec("create table t2 (id int) LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=2")
	defer tk.MustExec("drop table if exists t2")

	tk.MustExec("create table t1 (id int) placement policy pa1")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec("create table db2.t2 (id int) LEADER_CONSTRAINTS=\"[+region=bj]\" FOLLOWERS=2")
	defer tk.MustExec("drop table if exists db2.t2")

	tk.MustQuery("show placement").Check(testkit.Rows(
		"POLICY pa1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\"",
		"POLICY pa2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\"",
		"POLICY pb1 CONSTRAINTS=\"[+disk=ssd]\" VOTERS=5 VOTER_CONSTRAINTS=\"[+region=bj]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=sh]\"",
		"Table db2.t2 LEADER_CONSTRAINTS=\"[+region=bj]\" FOLLOWERS=2",
		"Table test.t1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\"",
		"Table test.t2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=2",
	))

	tk.MustQuery("show placement like 'POLICY%'").Check(testkit.Rows(
		"POLICY pa1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\"",
		"POLICY pa2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\"",
		"POLICY pb1 CONSTRAINTS=\"[+disk=ssd]\" VOTERS=5 VOTER_CONSTRAINTS=\"[+region=bj]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=sh]\"",
	))

	tk.MustQuery("show placement like 'POLICY pa%'").Check(testkit.Rows(
		"POLICY pa1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\"",
		"POLICY pa2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\"",
	))

	tk.MustQuery("show placement where Target='POLICY pb1'").Check(testkit.Rows(
		"POLICY pb1 CONSTRAINTS=\"[+disk=ssd]\" VOTERS=5 VOTER_CONSTRAINTS=\"[+region=bj]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=sh]\"",
	))
}

func (s *testSuite5) TestShowPlacementForTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop table if exists t1,t2, db2.t1")
	tk.MustExec("drop database if exists db2")

	tk.MustExec("create placement policy p1 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1,cn-east-2\"" +
		"SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy p1")

	// ref a policy
	tk.MustExec("create table t1 (id int) placement policy p1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustQuery("show placement for table t1").Check(testkit.Rows(
		"Table test.t1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\"",
	))

	// direct setting
	tk.MustExec("create table t2 (id int) LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=2")
	defer tk.MustExec("drop table if exists t2")
	tk.MustQuery("show placement for table t2").Check(testkit.Rows(
		"Table test.t2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=2",
	))

	// no placement
	tk.MustExec("create table t3 (id int)")
	defer tk.MustExec("drop table if exists t3")
	tk.MustQuery("show placement for table t3").Check(testkit.Rows())

	// table name with format db.table
	tk.MustExec("create database db2")
	defer tk.MustExec("drop database if exists db2")
	tk.MustExec("create table db2.t1 (id int) LEADER_CONSTRAINTS=\"[+region=bj]\" FOLLOWERS=2")
	defer tk.MustExec("drop table if exists db2.t1")
	tk.MustQuery("show placement for table db2.t1").Check(testkit.Rows(
		"Table db2.t1 LEADER_CONSTRAINTS=\"[+region=bj]\" FOLLOWERS=2",
	))
}
