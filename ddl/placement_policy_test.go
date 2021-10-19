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

package ddl_test

import (
	"context"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testDBSuite6) TestPlacementPolicy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists x")

	originalHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)

	hook := &ddl.TestDDLCallback{}
	var policyID int64
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if policyID != 0 {
			return
		}
		// job.SchemaID will be assigned when the policy is created.
		if job.SchemaName == "x" && job.Type == model.ActionCreatePlacementPolicy && job.SchemaID != 0 {
			policyID = job.SchemaID
			return
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	tk.MustExec("create placement policy x " +
		"LEARNERS=1 " +
		"LEARNER_CONSTRAINTS=\"[+region=cn-west-1]\" " +
		"VOTERS=3 " +
		"VOTER_CONSTRAINTS=\"[+disk=ssd]\"")

	checkFunc := func(policyInfo *model.PolicyInfo) {
		c.Assert(policyInfo.ID != 0, Equals, true)
		c.Assert(policyInfo.Name.L, Equals, "x")
		c.Assert(policyInfo.Followers, Equals, uint64(0))
		c.Assert(policyInfo.FollowerConstraints, Equals, "")
		c.Assert(policyInfo.Voters, Equals, uint64(3))
		c.Assert(policyInfo.VoterConstraints, Equals, "[+disk=ssd]")
		c.Assert(policyInfo.Learners, Equals, uint64(1))
		c.Assert(policyInfo.LearnerConstraints, Equals, "[+region=cn-west-1]")
		c.Assert(policyInfo.State, Equals, model.StatePublic)
		c.Assert(policyInfo.Schedule, Equals, "")
	}

	// Check the policy is correctly reloaded in the information schema.
	po := testGetPolicyByNameFromIS(c, tk.Se, "x")
	checkFunc(po)

	// Check the policy is correctly written in the kv meta.
	po = testGetPolicyByIDFromMeta(c, s.store, policyID)
	checkFunc(po)

	tk.MustGetErrCode("create placement policy x "+
		"PRIMARY_REGION=\"cn-east-1\" "+
		"REGIONS=\"cn-east-1,cn-east-2\" ", mysql.ErrPlacementPolicyExists)

	tk.MustGetErrCode("create placement policy X "+
		"PRIMARY_REGION=\"cn-east-1\" "+
		"REGIONS=\"cn-east-1,cn-east-2\" ", mysql.ErrPlacementPolicyExists)

	tk.MustGetErrCode("create placement policy `X` "+
		"PRIMARY_REGION=\"cn-east-1\" "+
		"REGIONS=\"cn-east-1,cn-east-2\" ", mysql.ErrPlacementPolicyExists)

	tk.MustExec("create placement policy if not exists X " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1,cn-east-2\" ")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 8238 Placement policy 'X' already exists"))

	tk.MustExec("drop placement policy x")
	tk.MustGetErrCode("drop placement policy x", mysql.ErrPlacementPolicyNotExists)
	tk.MustExec("drop placement policy if exists x")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 8239 Unknown placement policy 'x'"))

	// TODO: privilege check & constraint syntax check.
}

func testGetPolicyByIDFromMeta(c *C, store kv.Storage, policyID int64) *model.PolicyInfo {
	var (
		policyInfo *model.PolicyInfo
		err        error
	)
	err1 := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		policyInfo, err = t.GetPolicy(policyID)
		if err != nil {
			return err
		}
		return nil
	})
	c.Assert(err1, IsNil)
	c.Assert(policyInfo, NotNil)
	return policyInfo
}

func testGetPolicyByNameFromIS(c *C, ctx sessionctx.Context, policy string) *model.PolicyInfo {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	po, ok := dom.InfoSchema().PolicyByName(model.NewCIStr(policy))
	c.Assert(ok, Equals, true)
	return po
}

func (s *testDBSuite6) TestPlacementValidation(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists x")

	cases := []struct {
		name     string
		settings string
		success  bool
		errmsg   string
	}{
		{
			name: "Dict is not allowed for common constraint",
			settings: "LEARNERS=1 " +
				"LEARNER_CONSTRAINTS=\"[+zone=cn-west-1]\" " +
				"CONSTRAINTS=\"{'+disk=ssd':2}\"",
			errmsg: "invalid label constraints format: 'Constraints' should be [constraint1, ...] or any yaml compatible array representation",
		},
		{
			name: "constraints may be incompatible with itself",
			settings: "LEARNERS=1 " +
				"LEARNER_CONSTRAINTS=\"[+zone=cn-west-1, +zone=cn-west-2]\"",
			errmsg: "invalid label constraints format: should be [constraint1, ...] (error conflicting label constraints: '+zone=cn-west-2' and '+zone=cn-west-1'), {constraint1: cnt1, ...} (error yaml: unmarshal errors:\n" +
				"  line 1: cannot unmarshal !!seq into map[string]int), or any yaml compatible representation: invalid LearnerConstraints",
		},
		{
			settings: "PRIMARY_REGION=\"cn-east-1\" " +
				"REGIONS=\"cn-east-1,cn-east-2\" ",
			success: true,
		},
	}

	// test for create
	for _, ca := range cases {
		sql := fmt.Sprintf("%s %s", "create placement policy x", ca.settings)
		if ca.success {
			tk.MustExec(sql)
			tk.MustExec("drop placement policy if exists x")
		} else {
			err := tk.ExecToErr(sql)
			c.Assert(err, NotNil, Commentf(ca.name))
			c.Assert(err.Error(), Equals, ca.errmsg, Commentf(ca.name))
		}
	}

	// test for alter
	tk.MustExec("create placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1,cn-east\"")
	for _, ca := range cases {
		sql := fmt.Sprintf("%s %s", "alter placement policy x", ca.settings)
		if ca.success {
			tk.MustExec(sql)
			tk.MustExec("alter placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1,cn-east\"")
		} else {
			err := tk.ExecToErr(sql)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, ca.errmsg)
			tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east\""))
		}
	}
	tk.MustExec("drop placement policy x")
}

func (s *testDBSuite6) TestResetSchemaPlacement(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists TestResetPlacementDB;")
	tk.MustExec("create placement policy `TestReset` followers=4;")
	tk.MustGetErrCode("create placement policy `default` followers=4;", mysql.ErrReservedSyntax)
	tk.MustGetErrCode("create placement policy default followers=4;", mysql.ErrParse)

	tk.MustExec("create database TestResetPlacementDB placement policy `TestReset`;")
	tk.MustExec("use TestResetPlacementDB")
	// Test for `=default`
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testutil.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PLACEMENT POLICY=`TestReset` */",
	))
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY=default;")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testutil.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))
	// Test for `SET DEFAULT`
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY=`TestReset`;")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testutil.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PLACEMENT POLICY=`TestReset` */",
	))
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY SET DEFAULT")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testutil.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))
	// Test for `= 'DEFAULT'`
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY=`TestReset`;")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testutil.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PLACEMENT POLICY=`TestReset` */",
	))
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY = 'DEFAULT'")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testutil.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))
	// Test for "= `DEFAULT`"
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY=`TestReset`;")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testutil.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PLACEMENT POLICY=`TestReset` */",
	))
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY = `DEFAULT`")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testutil.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))

	tk.MustExec("drop placement policy `TestReset`;")
	tk.MustExec("drop database TestResetPlacementDB;")
}

func (s *testDBSuite6) TestAlterPlacementPolicy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists x")
	tk.MustExec("create placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1,cn-east\"")
	defer tk.MustExec("drop placement policy if exists x")

	// test for normal cases
	tk.MustExec("alter placement policy x PRIMARY_REGION=\"bj\" REGIONS=\"bj,sh\"")
	tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"bj\" REGIONS=\"bj,sh\""))
	tk.MustQuery("select * from information_schema.placement_rules where policy_name = 'x'").Check(testkit.Rows("3 def x <nil> <nil> <nil> bj bj,sh      0 0"))

	tk.MustExec("alter placement policy x " +
		"PRIMARY_REGION=\"bj\" " +
		"REGIONS=\"bj\" " +
		"SCHEDULE=\"EVEN\"")
	tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"bj\" REGIONS=\"bj\" SCHEDULE=\"EVEN\""))
	tk.MustQuery("select * from INFORMATION_SCHEMA.PLACEMENT_RULES WHERE POLICY_NAME='x'").Check(testkit.Rows("3 def x <nil> <nil> <nil> bj bj     EVEN 0 0"))

	tk.MustExec("alter placement policy x " +
		"LEADER_CONSTRAINTS=\"[+region=us-east-1]\" " +
		"FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" " +
		"FOLLOWERS=3")
	tk.MustQuery("show placement where target='POLICY x'").Check(
		testkit.Rows("POLICY x LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\""),
	)
	tk.MustQuery("SELECT POLICY_NAME,LEADER_CONSTRAINTS,FOLLOWER_CONSTRAINTS,FOLLOWERS FROM information_schema.PLACEMENT_RULES WHERE POLICY_NAME = 'x'").Check(
		testkit.Rows("x [+region=us-east-1] [+region=us-east-2] 3"),
	)

	tk.MustExec("alter placement policy x " +
		"VOTER_CONSTRAINTS=\"[+region=bj]\" " +
		"LEARNER_CONSTRAINTS=\"[+region=sh]\" " +
		"CONSTRAINTS=\"[+disk=ssd]\"" +
		"VOTERS=5 " +
		"LEARNERS=3")
	tk.MustQuery("show placement where target='POLICY x'").Check(
		testkit.Rows("POLICY x CONSTRAINTS=\"[+disk=ssd]\" VOTERS=5 VOTER_CONSTRAINTS=\"[+region=bj]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=sh]\""),
	)
	tk.MustQuery("SELECT " +
		"CATALOG_NAME,POLICY_NAME,SCHEMA_NAME,TABLE_NAME,PARTITION_NAME," +
		"PRIMARY_REGION,REGIONS,CONSTRAINTS,LEADER_CONSTRAINTS,FOLLOWER_CONSTRAINTS,LEARNER_CONSTRAINTS," +
		"SCHEDULE,FOLLOWERS,LEARNERS FROM INFORMATION_SCHEMA.placement_rules WHERE POLICY_NAME='x'").Check(
		testkit.Rows("def x <nil> <nil> <nil>   [+disk=ssd]   [+region=sh]  0 3"),
	)

	// test alter not exist policies
	tk.MustExec("drop placement policy x")
	tk.MustGetErrCode("alter placement policy x REGIONS=\"bj,sh\"", mysql.ErrPlacementPolicyNotExists)
	tk.MustGetErrCode("alter placement policy x2 REGIONS=\"bj,sh\"", mysql.ErrPlacementPolicyNotExists)
	tk.MustQuery("select * from INFORMATION_SCHEMA.PLACEMENT_RULES WHERE POLICY_NAME='x'").Check(testkit.Rows())
}

func (s *testDBSuite6) TestCreateTableWithPlacementPolicy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t,t_range_p,t_hash_p,t_list_p")

	// Direct placement option: special constraints may be incompatible with common constraint.
	_, err := tk.Exec("create table t(a int) " +
		"FOLLOWERS=2 " +
		"FOLLOWER_CONSTRAINTS=\"[+zone=cn-east-1]\" " +
		"CONSTRAINTS=\"[+disk=ssd,-zone=cn-east-1]\"")
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*conflicting label constraints.*")

	tk.MustExec("create table t(a int) " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 ")
	defer tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't'").Check(testkit.Rows(`def test t <nil> PRIMARY_REGION="cn-east-1" REGIONS="cn-east-1, cn-east-2" FOLLOWERS=2`))

	tbl := testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().PlacementPolicyRef, IsNil)
	c.Assert(tbl.Meta().DirectPlacementOpts, NotNil)

	checkFunc := func(policySetting *model.PlacementSettings) {
		c.Assert(policySetting.PrimaryRegion, Equals, "cn-east-1")
		c.Assert(policySetting.Regions, Equals, "cn-east-1, cn-east-2")
		c.Assert(policySetting.Followers, Equals, uint64(2))
		c.Assert(policySetting.FollowerConstraints, Equals, "")
		c.Assert(policySetting.Voters, Equals, uint64(0))
		c.Assert(policySetting.VoterConstraints, Equals, "")
		c.Assert(policySetting.Learners, Equals, uint64(0))
		c.Assert(policySetting.LearnerConstraints, Equals, "")
		c.Assert(policySetting.Constraints, Equals, "")
		c.Assert(policySetting.Schedule, Equals, "")
	}
	checkFunc(tbl.Meta().DirectPlacementOpts)
	tk.MustQuery("SELECT * FROM information_schema.placement_rules WHERE TABLE_NAME = 't'").Check(testkit.Rows("<nil> def <nil> test t <nil> cn-east-1 cn-east-1, cn-east-2      2 0"))
	tk.MustExec("drop table if exists t")

	// Direct placement option and placement policy can't co-exist.
	_, err = tk.Exec("create table t(a int) " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 " +
		"CONSTRAINTS=\"[+disk=ssd]\" " +
		"PLACEMENT POLICY=\"x\"")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8240]Placement policy 'x' can't co-exist with direct placement options")

	// Only placement policy should check the policy existence.
	tk.MustGetErrCode("create table t(a int)"+
		"PLACEMENT POLICY=\"x\"", mysql.ErrPlacementPolicyNotExists)
	tk.MustExec("create placement policy x " +
		"FOLLOWERS=2 " +
		"CONSTRAINTS=\"[+disk=ssd]\" ")
	tk.MustExec("create placement policy y " +
		"FOLLOWERS=3 " +
		"CONSTRAINTS=\"[+region=bj]\" ")
	tk.MustExec("create table t(a int)" +
		"PLACEMENT POLICY=\"x\"")
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't'").Check(testkit.Rows(`def test t x <nil>`))
	tk.MustExec("create table t_range_p(id int) placement policy x partition by range(id) (" +
		"PARTITION p0 VALUES LESS THAN (100)," +
		"PARTITION p1 VALUES LESS THAN (1000) placement policy y," +
		"PARTITION p2 VALUES LESS THAN (10000) PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1, cn-east-2\" FOLLOWERS=2 )",
	)
	tk.MustExec("set tidb_enable_list_partition=1")
	tk.MustExec("create table t_list_p(name varchar(10)) placement policy x partition by list columns(name) (" +
		"PARTITION p0 VALUES IN ('a', 'b')," +
		"PARTITION p1 VALUES IN ('c', 'd') placement policy y," +
		"PARTITION p2 VALUES IN ('e', 'f') PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1, cn-east-2\" FOLLOWERS=2 )",
	)
	tk.MustExec("create table t_hash_p(id int) placement policy x partition by HASH(id) PARTITIONS 4")

	policyX := testGetPolicyByName(c, tk.Se, "x", true)
	c.Assert(policyX.Name.L, Equals, "x")
	c.Assert(policyX.ID != 0, Equals, true)

	policyY := testGetPolicyByName(c, tk.Se, "y", true)
	c.Assert(policyY.Name.L, Equals, "y")
	c.Assert(policyY.ID != 0, Equals, true)

	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().PlacementPolicyRef, NotNil)
	c.Assert(tbl.Meta().PlacementPolicyRef.Name.L, Equals, "x")
	c.Assert(tbl.Meta().PlacementPolicyRef.ID, Equals, policyX.ID)
	tk.MustQuery("SELECT * FROM information_schema.placement_rules WHERE TABLE_NAME = 't'").Check(testkit.Rows())
	tk.MustExec("drop table if exists t")

	checkPartitionTableFunc := func(tblName string) {
		tbl = testGetTableByName(c, tk.Se, "test", tblName)
		c.Assert(tbl, NotNil)
		c.Assert(tbl.Meta().PlacementPolicyRef, NotNil)
		c.Assert(tbl.Meta().PlacementPolicyRef.Name.L, Equals, "x")
		c.Assert(tbl.Meta().PlacementPolicyRef.ID, Equals, policyX.ID)
		c.Assert(tbl.Meta().DirectPlacementOpts, IsNil)

		c.Assert(tbl.Meta().Partition, NotNil)
		c.Assert(len(tbl.Meta().Partition.Definitions), Equals, 3)

		p0 := tbl.Meta().Partition.Definitions[0]
		c.Assert(p0.PlacementPolicyRef, IsNil)
		c.Assert(p0.DirectPlacementOpts, IsNil)

		p1 := tbl.Meta().Partition.Definitions[1]
		c.Assert(p1.PlacementPolicyRef, NotNil)
		c.Assert(p1.PlacementPolicyRef.Name.L, Equals, "y")
		c.Assert(p1.PlacementPolicyRef.ID, Equals, policyY.ID)
		c.Assert(p1.DirectPlacementOpts, IsNil)

		p2 := tbl.Meta().Partition.Definitions[2]
		c.Assert(p0.PlacementPolicyRef, IsNil)
		checkFunc(p2.DirectPlacementOpts)
	}

	checkPartitionTableFunc("t_range_p")
	tk.MustExec("drop table if exists t_range_p")

	checkPartitionTableFunc("t_list_p")
	tk.MustExec("drop table if exists t_list_p")

	tbl = testGetTableByName(c, tk.Se, "test", "t_hash_p")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().PlacementPolicyRef, NotNil)
	c.Assert(tbl.Meta().PlacementPolicyRef.Name.L, Equals, "x")
	c.Assert(tbl.Meta().PlacementPolicyRef.ID, Equals, policyX.ID)
	c.Assert(tbl.Meta().DirectPlacementOpts, IsNil)
	for _, p := range tbl.Meta().Partition.Definitions {
		c.Assert(p.PlacementPolicyRef, IsNil)
		c.Assert(p.DirectPlacementOpts, IsNil)
	}
	tk.MustExec("drop table if exists t_hash_p")

	tk.MustExec("create table t(a int)" +
		"FOLLOWERS=2 " +
		"CONSTRAINTS=\"[+disk=ssd]\" ")
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't'").Check(testkit.Rows(`def test t <nil> CONSTRAINTS="[+disk=ssd]" FOLLOWERS=2`))

	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().DirectPlacementOpts, NotNil)

	checkFunc = func(policySetting *model.PlacementSettings) {
		c.Assert(policySetting.PrimaryRegion, Equals, "")
		c.Assert(policySetting.Regions, Equals, "")
		c.Assert(policySetting.Followers, Equals, uint64(2))
		c.Assert(policySetting.FollowerConstraints, Equals, "")
		c.Assert(policySetting.Voters, Equals, uint64(0))
		c.Assert(policySetting.VoterConstraints, Equals, "")
		c.Assert(policySetting.Learners, Equals, uint64(0))
		c.Assert(policySetting.LearnerConstraints, Equals, "")
		c.Assert(policySetting.Constraints, Equals, "[+disk=ssd]")
		c.Assert(policySetting.Schedule, Equals, "")
	}
	checkFunc(tbl.Meta().DirectPlacementOpts)
	tk.MustQuery("SELECT * FROM information_schema.placement_rules WHERE TABLE_NAME = 't'").Check(testkit.Rows("<nil> def <nil> test t <nil>   [+disk=ssd]     2 0"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop placement policy if exists x")
	tk.MustExec("drop placement policy if exists y")
}

func (s *testDBSuite6) TestDropPlacementPolicyInUse(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create database if not exists test2")
	tk.MustExec("drop table if exists test.t11, test.t12, test2.t21, test2.t21, test2.t22")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("drop placement policy if exists p3")
	tk.MustExec("drop placement policy if exists p4")

	// p1 is used by test.t11 and test2.t21
	tk.MustExec("create placement policy p1 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("create table test.t11 (id int) placement policy 'p1'")
	defer tk.MustExec("drop table if exists test.t11")
	tk.MustExec("create table test2.t21 (id int) placement policy 'p1'")
	defer tk.MustExec("drop table if exists test2.t21")

	// p1 is used by test.t12
	tk.MustExec("create placement policy p2 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("create table test.t12 (id int) placement policy 'p2'")
	defer tk.MustExec("drop table if exists test.t12")
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't12'").Check(testkit.Rows(`def test t12 p2 <nil>`))

	// p3 is used by test2.t22
	tk.MustExec("create placement policy p3 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy if exists p3")
	tk.MustExec("create table test.t21 (id int) placement policy 'p3'")
	defer tk.MustExec("drop table if exists test.t21")

	// p4 is used by test_p
	tk.MustExec("create placement policy p4 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy if exists p4")
	tk.MustExec("create database test_p placement policy 'p4'")
	defer tk.MustExec("drop database if exists test_p")

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(txn.Rollback(), IsNil)
	}()
	for _, policyName := range []string{"p1", "p2", "p3", "p4"} {
		err := tk.ExecToErr(fmt.Sprintf("drop placement policy %s", policyName))
		c.Assert(err.Error(), Equals, fmt.Sprintf("[ddl:8241]Placement policy '%s' is still in use", policyName))

		err = tk.ExecToErr(fmt.Sprintf("drop placement policy if exists %s", policyName))
		c.Assert(err.Error(), Equals, fmt.Sprintf("[ddl:8241]Placement policy '%s' is still in use", policyName))
	}
}

func testGetPolicyByName(c *C, ctx sessionctx.Context, name string, mustExist bool) *model.PolicyInfo {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	po, ok := dom.InfoSchema().PolicyByName(model.NewCIStr(name))
	if mustExist {
		c.Assert(ok, Equals, true)
	}
	return po
}

func testGetPolicyDependency(storage kv.Storage, name string) []int64 {
	ids := make([]int64, 0, 32)
	err1 := kv.RunInNewTxn(context.Background(), storage, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		dbs, err := t.ListDatabases()
		if err != nil {
			return err
		}
		for _, db := range dbs {
			tbls, err := t.ListTables(db.ID)
			if err != nil {
				return err
			}
			for _, tbl := range tbls {
				if tbl.PlacementPolicyRef != nil && tbl.PlacementPolicyRef.Name.L == name {
					ids = append(ids, tbl.ID)
				}
			}
		}
		return nil
	})
	if err1 != nil {
		return []int64{}
	}
	return ids
}

func (s *testDBSuite6) TestPolicyCacheAndPolicyDependency(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists x")

	// Test policy cache.
	tk.MustExec("create placement policy x primary_region=\"r1\" regions=\"r1,r2\" schedule=\"EVEN\";")
	po := testGetPolicyByName(c, tk.Se, "x", true)
	c.Assert(po, NotNil)
	tk.MustQuery("show placement").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" SCHEDULE=\"EVEN\""))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int) placement policy \"x\"")
	defer tk.MustExec("drop table if exists t")
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't'").Check(testkit.Rows(`def test t BASE TABLE x <nil>`))
	tbl := testGetTableByName(c, tk.Se, "test", "t")

	// Test policy dependency cache.
	dependencies := testGetPolicyDependency(s.store, "x")
	c.Assert(dependencies, NotNil)
	c.Assert(len(dependencies), Equals, 1)
	c.Assert(dependencies[0], Equals, tbl.Meta().ID)

	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (a int) placement policy \"x\"")
	defer tk.MustExec("drop table if exists t2")
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't'").Check(testkit.Rows(`def test t BASE TABLE x <nil>`))
	tbl2 := testGetTableByName(c, tk.Se, "test", "t2")

	dependencies = testGetPolicyDependency(s.store, "x")
	c.Assert(dependencies, NotNil)
	c.Assert(len(dependencies), Equals, 2)
	in := func() bool {
		for _, one := range dependencies {
			if one == tbl2.Meta().ID {
				return true
			}
		}
		return false
	}
	c.Assert(in(), Equals, true)

	// Test drop policy can't succeed cause there are still some table depend on them.
	_, err := tk.Exec("drop placement policy x")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8241]Placement policy 'x' is still in use")

	// Drop depended table t firstly.
	tk.MustExec("drop table if exists t")
	dependencies = testGetPolicyDependency(s.store, "x")
	c.Assert(dependencies, NotNil)
	c.Assert(len(dependencies), Equals, 1)
	c.Assert(dependencies[0], Equals, tbl2.Meta().ID)

	_, err = tk.Exec("drop placement policy x")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8241]Placement policy 'x' is still in use")

	// Drop depended table t2 secondly.
	tk.MustExec("drop table if exists t2")
	dependencies = testGetPolicyDependency(s.store, "x")
	c.Assert(dependencies, NotNil)
	c.Assert(len(dependencies), Equals, 0)

	po = testGetPolicyByName(c, tk.Se, "x", true)
	c.Assert(po, NotNil)

	tk.MustExec("drop placement policy x")

	po = testGetPolicyByName(c, tk.Se, "x", false)
	c.Assert(po, IsNil)
	dependencies = testGetPolicyDependency(s.store, "x")
	c.Assert(dependencies, NotNil)
	c.Assert(len(dependencies), Equals, 0)
}

func (s *testDBSuite6) TestAlterTablePartitionWithPlacementPolicy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer func() {
		tk.MustExec("drop table if exists t1")
		tk.MustExec("drop placement policy if exists x")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	// Direct placement option: special constraints may be incompatible with common constraint.
	tk.MustExec("create table t1 (c int) PARTITION BY RANGE (c) " +
		"(PARTITION p0 VALUES LESS THAN (6)," +
		"PARTITION p1 VALUES LESS THAN (11)," +
		"PARTITION p2 VALUES LESS THAN (16)," +
		"PARTITION p3 VALUES LESS THAN (21));")
	defer tk.MustExec("drop table if exists t1")
	tk.MustQuery("SELECT " +
		"CATALOG_NAME,POLICY_NAME,SCHEMA_NAME,TABLE_NAME,PARTITION_NAME," +
		"PRIMARY_REGION,REGIONS,CONSTRAINTS,LEADER_CONSTRAINTS,FOLLOWER_CONSTRAINTS,LEARNER_CONSTRAINTS," +
		"SCHEDULE,FOLLOWERS,LEARNERS FROM INFORMATION_SCHEMA.placement_rules WHERE table_NAME='t1'").Check(
		testkit.Rows())

	tk.MustExec("alter table t1 partition p0 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 ")

	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Partitions WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't1' AND PARTITION_NAME = 'p0'").Check(testkit.Rows(`def test t1 p0  PRIMARY_REGION="cn-east-1" REGIONS="cn-east-1, cn-east-2" FOLLOWERS=2`))
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Partitions WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't1' AND PARTITION_NAME = 'p1'").Check(testkit.Rows(`def test t1 p1 <nil> <nil>`))
	tbl := testGetTableByName(c, tk.Se, "test", "t1")
	c.Assert(tbl, NotNil)
	ptDef := testGetPartitionDefinitionsByName(c, tk.Se, "test", "t1", "p0")
	c.Assert(ptDef.PlacementPolicyRef.Name.L, Equals, "")
	c.Assert(ptDef.DirectPlacementOpts, NotNil)

	checkFunc := func(policySetting *model.PlacementSettings) {
		c.Assert(policySetting.PrimaryRegion, Equals, "cn-east-1")
		c.Assert(policySetting.Regions, Equals, "cn-east-1, cn-east-2")
		c.Assert(policySetting.Followers, Equals, uint64(2))
		c.Assert(policySetting.FollowerConstraints, Equals, "")
		c.Assert(policySetting.Voters, Equals, uint64(0))
		c.Assert(policySetting.VoterConstraints, Equals, "")
		c.Assert(policySetting.Learners, Equals, uint64(0))
		c.Assert(policySetting.LearnerConstraints, Equals, "")
		c.Assert(policySetting.Constraints, Equals, "")
		c.Assert(policySetting.Schedule, Equals, "")
	}
	checkFunc(ptDef.DirectPlacementOpts)
	tk.MustQuery("SELECT " +
		"CATALOG_NAME,POLICY_NAME,SCHEMA_NAME,TABLE_NAME,PARTITION_NAME," +
		"PRIMARY_REGION,REGIONS,CONSTRAINTS,LEADER_CONSTRAINTS,FOLLOWER_CONSTRAINTS,LEARNER_CONSTRAINTS," +
		"SCHEDULE,FOLLOWERS,LEARNERS FROM INFORMATION_SCHEMA.placement_rules WHERE TABLE_NAME='t1'").Check(
		testkit.Rows("def <nil> test t1 p0 cn-east-1 cn-east-1, cn-east-2      2 0"))

	//Direct placement option and placement policy can't co-exist.
	_, err := tk.Exec("alter table t1 partition p0 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 " +
		"PLACEMENT POLICY=\"x\"")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8240]Placement policy 'x' can't co-exist with direct placement options")

	// Only placement policy should check the policy existence.
	tk.MustGetErrCode("alter table t1 partition p0 "+
		"PLACEMENT POLICY=\"x\"", mysql.ErrPlacementPolicyNotExists)
	tk.MustExec("create placement policy x " +
		"FOLLOWERS=2 ")
	tk.MustExec("alter table t1 partition p0 " +
		"PLACEMENT POLICY=\"x\"")
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Partitions WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't1' AND PARTITION_NAME = 'p0'").Check(testkit.Rows(`def test t1 p0 x `))

	ptDef = testGetPartitionDefinitionsByName(c, tk.Se, "test", "t1", "p0")
	c.Assert(ptDef, NotNil)
	c.Assert(ptDef.PlacementPolicyRef, NotNil)
	c.Assert(ptDef.PlacementPolicyRef.Name.L, Equals, "x")
	c.Assert(ptDef.PlacementPolicyRef.ID != 0, Equals, true)

	tk.MustExec("alter table t1 partition p0 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 ")
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Partitions WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't1' AND PARTITION_NAME = 'p0'").Check(testkit.Rows("def test t1 p0  PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1, cn-east-2\" FOLLOWERS=2"))

	ptDef = testGetPartitionDefinitionsByName(c, tk.Se, "test", "t1", "p0")
	c.Assert(ptDef, NotNil)
	c.Assert(ptDef.DirectPlacementOpts, NotNil)

	checkFunc = func(policySetting *model.PlacementSettings) {
		c.Assert(policySetting.PrimaryRegion, Equals, "cn-east-1")
		c.Assert(policySetting.Regions, Equals, "cn-east-1, cn-east-2")
		c.Assert(policySetting.Followers, Equals, uint64(2))
		c.Assert(policySetting.FollowerConstraints, Equals, "")
		c.Assert(policySetting.Voters, Equals, uint64(0))
		c.Assert(policySetting.VoterConstraints, Equals, "")
		c.Assert(policySetting.Learners, Equals, uint64(0))
		c.Assert(policySetting.LearnerConstraints, Equals, "")
		c.Assert(policySetting.Constraints, Equals, "")
		c.Assert(policySetting.Schedule, Equals, "")
	}
	checkFunc(ptDef.DirectPlacementOpts)
}

func testGetPartitionDefinitionsByName(c *C, ctx sessionctx.Context, db string, table string, ptName string) model.PartitionDefinition {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	c.Assert(err, IsNil)
	c.Assert(tbl, NotNil)
	var ptDef model.PartitionDefinition
	for _, def := range tbl.Meta().Partition.Definitions {
		if ptName == def.Name.L {
			ptDef = def
			break
		}
	}
	return ptDef
}

func (s *testDBSuite6) TestPolicyInheritance(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop placement policy if exists x")

	// test table inherit database's placement rules.
	tk.MustExec("create database mydb constraints=\"[+zone=hangzhou]\"")
	tk.MustQuery("show create database mydb").Check(testkit.Rows("mydb CREATE DATABASE `mydb` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] CONSTRAINTS=\"[+zone=hangzhou]\" */"))

	tk.MustExec("use mydb")
	tk.MustExec("create table t(a int)")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] CONSTRAINTS=\"[+zone=hangzhou]\" */"))
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t(a int) constraints=\"[+zone=suzhou]\"")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] CONSTRAINTS=\"[+zone=suzhou]\" */"))
	tk.MustExec("drop table if exists t")

	// table will inherit db's placement rules, which is shared by all partition as default one.
	tk.MustExec("create table t(a int) partition by range(a) (partition p0 values less than (100), partition p1 values less than (200))")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] CONSTRAINTS=\"[+zone=hangzhou]\" */\n" +
		"PARTITION BY RANGE ( `a` ) (\n" +
		"  PARTITION `p0` VALUES LESS THAN (100),\n" +
		"  PARTITION `p1` VALUES LESS THAN (200)\n" +
		")"))
	tk.MustExec("drop table if exists t")

	// partition's specified placement rules will override the default one.
	tk.MustExec("create table t(a int) partition by range(a) (partition p0 values less than (100) constraints=\"[+zone=suzhou]\", partition p1 values less than (200))")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] CONSTRAINTS=\"[+zone=hangzhou]\" */\n" +
		"PARTITION BY RANGE ( `a` ) (\n" +
		"  PARTITION `p0` VALUES LESS THAN (100) /*T![placement] CONSTRAINTS=\"[+zone=suzhou]\" */,\n" +
		"  PARTITION `p1` VALUES LESS THAN (200)\n" +
		")"))
	tk.MustExec("drop table if exists t")

	// test partition override table's placement rules.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int) CONSTRAINTS=\"[+zone=suzhou]\" partition by range(a) (partition p0 values less than (100) CONSTRAINTS=\"[+zone=changzhou]\", partition p1 values less than (200))")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] CONSTRAINTS=\"[+zone=suzhou]\" */\n" +
		"PARTITION BY RANGE ( `a` ) (\n" +
		"  PARTITION `p0` VALUES LESS THAN (100) /*T![placement] CONSTRAINTS=\"[+zone=changzhou]\" */,\n" +
		"  PARTITION `p1` VALUES LESS THAN (200)\n" +
		")"))
}
