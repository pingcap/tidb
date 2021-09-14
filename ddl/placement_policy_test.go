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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testkit"
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
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1,cn-east-2\" " +
		"LEARNERS=1 " +
		"LEARNER_CONSTRAINTS=\"[+region=cn-west-1]\" " +
		"VOTERS=3 " +
		"VOTER_CONSTRAINTS=\"[+disk=ssd]\"")

	checkFunc := func(policyInfo *model.PolicyInfo) {
		c.Assert(policyInfo.ID != 0, Equals, true)
		c.Assert(policyInfo.Name.L, Equals, "x")
		c.Assert(policyInfo.PrimaryRegion, Equals, "cn-east-1")
		c.Assert(policyInfo.Regions, Equals, "cn-east-1,cn-east-2")
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

func (s *testDBSuite6) TestConstraintCompatibility(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists x")

	cases := []struct {
		settings string
		success  bool
		errmsg   string
	}{
		// Dict is not allowed for common constraint.
		{
			settings: "PRIMARY_REGION=\"cn-east-1\" " +
				"REGIONS=\"cn-east-1,cn-east-2\" " +
				"LEARNERS=1 " +
				"LEARNER_CONSTRAINTS=\"[+zone=cn-west-1]\" " +
				"CONSTRAINTS=\"{'+disk=ssd':2}\"",
			errmsg: "invalid label constraints format: should be [constraint1, ...] (error yaml: unmarshal errors:\n  line 1: cannot unmarshal !!map into []string)",
		},
		// Special constraints may be incompatible with itself.
		{
			settings: "PRIMARY_REGION=\"cn-east-1\" " +
				"REGIONS=\"cn-east-1,cn-east-2\" " +
				"LEARNERS=1 " +
				"LEARNER_CONSTRAINTS=\"[+zone=cn-west-1, +zone=cn-west-2]\"",
			errmsg: "conflicting label constraints: '+zone=cn-west-2' and '+zone=cn-west-1'",
		},
		{
			settings: "PRIMARY_REGION=\"cn-east-1\" " +
				"REGIONS=\"cn-east-1,cn-east-2\" " +
				"LEARNERS=1 " +
				"LEARNER_CONSTRAINTS=\"[+zone=cn-west-1, -zone=cn-west-1]\"",
			errmsg: "conflicting label constraints: '-zone=cn-west-1' and '+zone=cn-west-1'",
		},
		{
			settings: "PRIMARY_REGION=\"cn-east-1\" " +
				"REGIONS=\"cn-east-1,cn-east-2\" " +
				"LEARNERS=1 " +
				"LEARNER_CONSTRAINTS=\"[+zone=cn-west-1, +zone=cn-west-1]\"",
			success: true,
		},
		// Special constraints may be incompatible with common constraint.
		{
			settings: "PRIMARY_REGION=\"cn-east-1\" " +
				"REGIONS=\"cn-east-1, cn-east-2\" " +
				"FOLLOWERS=2 " +
				"FOLLOWER_CONSTRAINTS=\"[+zone=cn-east-1]\" " +
				"CONSTRAINTS=\"[+zone=cn-east-2]\"",
			errmsg: "conflicting label constraints: '+zone=cn-east-2' and '+zone=cn-east-1'",
		},
		{
			settings: "PRIMARY_REGION=\"cn-east-1\" " +
				"REGIONS=\"cn-east-1, cn-east-2\" " +
				"FOLLOWERS=2 " +
				"FOLLOWER_CONSTRAINTS=\"[+zone=cn-east-1]\" " +
				"CONSTRAINTS=\"[+disk=ssd,-zone=cn-east-1]\"",
			errmsg: "conflicting label constraints: '-zone=cn-east-1' and '+zone=cn-east-1'",
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
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, ca.errmsg)
		}
	}

	// test for alter
	tk.MustExec("create placement policy x regions=\"cn-east1,cn-east\"")
	for _, ca := range cases {
		sql := fmt.Sprintf("%s %s", "alter placement policy x", ca.settings)
		if ca.success {
			tk.MustExec(sql)
			tk.MustExec("alter placement policy x regions=\"cn-east1,cn-east\"")
		} else {
			err := tk.ExecToErr(sql)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, ca.errmsg)
			tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x REGIONS=\"cn-east1,cn-east\" SCHEDULED"))
		}
	}
	tk.MustExec("drop placement policy x")
}

func (s *testDBSuite6) TestAlterPlacementPolicy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists x")
	tk.MustExec("create placement policy x primary_region=\"cn-east-1\" regions=\"cn-east1,cn-east\"")
	defer tk.MustExec("drop placement policy if exists x")

	// test for normal cases
	tk.MustExec("alter placement policy x REGIONS=\"bj,sh\"")
	tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x REGIONS=\"bj,sh\" SCHEDULED"))

	tk.MustExec("alter placement policy x " +
		"PRIMARY_REGION=\"bj\" " +
		"REGIONS=\"sh\" " +
		"SCHEDULE=\"EVEN\"")
	tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"bj\" REGIONS=\"sh\" SCHEDULE=\"EVEN\" SCHEDULED"))

	tk.MustExec("alter placement policy x " +
		"LEADER_CONSTRAINTS=\"[+region=us-east-1]\" " +
		"FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" " +
		"FOLLOWERS=3")
	tk.MustQuery("show placement where target='POLICY x'").Check(
		testkit.Rows("POLICY x LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" SCHEDULED"),
	)

	tk.MustExec("alter placement policy x " +
		"VOTER_CONSTRAINTS=\"[+region=bj]\" " +
		"LEARNER_CONSTRAINTS=\"[+region=sh]\" " +
		"CONSTRAINTS=\"[+disk=ssd]\"" +
		"VOTERS=5 " +
		"LEARNERS=3")
	tk.MustQuery("show placement where target='POLICY x'").Check(
		testkit.Rows("POLICY x CONSTRAINTS=\"[+disk=ssd]\" VOTERS=5 VOTER_CONSTRAINTS=\"[+region=bj]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=sh]\" SCHEDULED"),
	)

	// test alter not exist policies
	tk.MustExec("drop placement policy x")
	tk.MustGetErrCode("alter placement policy x REGIONS=\"bj,sh\"", mysql.ErrPlacementPolicyNotExists)
	tk.MustGetErrCode("alter placement policy x2 REGIONS=\"bj,sh\"", mysql.ErrPlacementPolicyNotExists)
}

func (s *testDBSuite6) TestCreateTableWithPlacementPolicy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	// Direct placement option: special constraints may be incompatible with common constraint.
	_, err := tk.Exec("create table t(a int) " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 " +
		"FOLLOWER_CONSTRAINTS=\"[+zone=cn-east-1]\" " +
		"CONSTRAINTS=\"[+disk=ssd,-zone=cn-east-1]\"")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "conflicting label constraints: '-zone=cn-east-1' and '+zone=cn-east-1'")

	tk.MustExec("create table t(a int) " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 " +
		"FOLLOWER_CONSTRAINTS=\"[+zone=cn-east-1]\" " +
		"CONSTRAINTS=\"[+disk=ssd]\"")

	tbl := testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().PlacementPolicyRef, IsNil)
	c.Assert(tbl.Meta().DirectPlacementOpts, NotNil)

	checkFunc := func(policySetting *model.PlacementSettings) {
		c.Assert(policySetting.PrimaryRegion, Equals, "cn-east-1")
		c.Assert(policySetting.Regions, Equals, "cn-east-1, cn-east-2")
		c.Assert(policySetting.Followers, Equals, uint64(2))
		c.Assert(policySetting.FollowerConstraints, Equals, "[+zone=cn-east-1]")
		c.Assert(policySetting.Voters, Equals, uint64(0))
		c.Assert(policySetting.VoterConstraints, Equals, "")
		c.Assert(policySetting.Learners, Equals, uint64(0))
		c.Assert(policySetting.LearnerConstraints, Equals, "")
		c.Assert(policySetting.Constraints, Equals, "[+disk=ssd]")
		c.Assert(policySetting.Schedule, Equals, "")
	}
	checkFunc(tbl.Meta().DirectPlacementOpts)
	tk.MustExec("drop table if exists t")

	// Direct placement option and placement policy can't co-exist.
	_, err = tk.Exec("create table t(a int) " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 " +
		"FOLLOWER_CONSTRAINTS=\"[+zone=cn-east-1]\" " +
		"CONSTRAINTS=\"[+disk=ssd]\" " +
		"PLACEMENT POLICY=\"x\"")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8240]Placement policy 'x' can't co-exist with direct placement options")

	// Only placement policy should check the policy existence.
	tk.MustGetErrCode("create table t(a int)"+
		"PLACEMENT POLICY=\"x\"", mysql.ErrPlacementPolicyNotExists)
	tk.MustExec("create placement policy x " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 " +
		"FOLLOWER_CONSTRAINTS=\"[+zone=cn-east-1]\" " +
		"CONSTRAINTS=\"[+disk=ssd]\" ")
	tk.MustExec("create table t(a int)" +
		"PLACEMENT POLICY=\"x\"")

	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().PlacementPolicyRef, NotNil)
	c.Assert(tbl.Meta().PlacementPolicyRef.Name.L, Equals, "x")
	c.Assert(tbl.Meta().PlacementPolicyRef.ID != 0, Equals, true)
	tk.MustExec("drop table if exists t")

	// Only direct placement options should check the compatibility itself.
	_, err = tk.Exec("create table t(a int)" +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 " +
		"FOLLOWER_CONSTRAINTS=\"[+zone=cn-east-1]\" " +
		"CONSTRAINTS=\"[+disk=ssd, -zone=cn-east-1]\" ")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "conflicting label constraints: '-zone=cn-east-1' and '+zone=cn-east-1'")

	tk.MustExec("create table t(a int)" +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 " +
		"FOLLOWER_CONSTRAINTS=\"[+zone=cn-east-1]\" " +
		"CONSTRAINTS=\"[+disk=ssd]\" ")

	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().DirectPlacementOpts, NotNil)

	checkFunc = func(policySetting *model.PlacementSettings) {
		c.Assert(policySetting.PrimaryRegion, Equals, "cn-east-1")
		c.Assert(policySetting.Regions, Equals, "cn-east-1, cn-east-2")
		c.Assert(policySetting.Followers, Equals, uint64(2))
		c.Assert(policySetting.FollowerConstraints, Equals, "[+zone=cn-east-1]")
		c.Assert(policySetting.Voters, Equals, uint64(0))
		c.Assert(policySetting.VoterConstraints, Equals, "")
		c.Assert(policySetting.Learners, Equals, uint64(0))
		c.Assert(policySetting.LearnerConstraints, Equals, "")
		c.Assert(policySetting.Constraints, Equals, "[+disk=ssd]")
		c.Assert(policySetting.Schedule, Equals, "")
	}
	checkFunc(tbl.Meta().DirectPlacementOpts)
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop placement policy if exists x")
}
