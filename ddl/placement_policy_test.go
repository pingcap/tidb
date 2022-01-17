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
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/gcworker"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

func clearAllBundles(c *C) {
	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)
	clearBundles := make([]*placement.Bundle, 0, len(bundles))
	for _, bundle := range bundles {
		clearBundles = append(clearBundles, &placement.Bundle{ID: bundle.ID})
	}
	err = infosync.PutRuleBundles(context.TODO(), clearBundles)
	c.Assert(err, IsNil)
}

func checkExistTableBundlesInPD(c *C, do *domain.Domain, dbName string, tbName string) {
	tblInfo, err := do.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tbName))
	c.Assert(err, IsNil)

	c.Assert(kv.RunInNewTxn(context.TODO(), do.Store(), false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		checkTableBundlesInPD(c, t, tblInfo.Meta())
		return nil
	}), IsNil)
}

func checkAllBundlesNotChange(c *C, bundles []*placement.Bundle) {
	currentBundles, err := infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)

	bundlesMap := make(map[string]*placement.Bundle)
	for _, bundle := range currentBundles {
		bundlesMap[bundle.ID] = bundle
	}
	c.Assert(len(bundlesMap), Equals, len(currentBundles))
	c.Assert(len(currentBundles), Equals, len(bundles))

	for _, bundle := range bundles {
		got, ok := bundlesMap[bundle.ID]
		c.Assert(ok, IsTrue)

		expectedJSON, err := json.Marshal(bundle)
		c.Assert(err, IsNil)

		gotJSON, err := json.Marshal(got)
		c.Assert(err, IsNil)
		c.Assert(string(gotJSON), Equals, string(expectedJSON))
	}
}

func checkTableBundlesInPD(c *C, t *meta.Meta, tblInfo *model.TableInfo) {
	checks := make([]*struct {
		ID     string
		bundle *placement.Bundle
	}, 0)

	bundle, err := placement.NewTableBundle(t, tblInfo)
	c.Assert(err, IsNil)
	checks = append(checks, &struct {
		ID     string
		bundle *placement.Bundle
	}{ID: placement.GroupID(tblInfo.ID), bundle: bundle})

	if tblInfo.Partition != nil {
		for _, def := range tblInfo.Partition.Definitions {
			bundle, err := placement.NewPartitionBundle(t, def)
			c.Assert(err, IsNil)
			checks = append(checks, &struct {
				ID     string
				bundle *placement.Bundle
			}{ID: placement.GroupID(def.ID), bundle: bundle})
		}
	}

	for _, check := range checks {
		got, err := infosync.GetRuleBundle(context.TODO(), check.ID)
		c.Assert(err, IsNil)
		if check.bundle == nil {
			c.Assert(got.IsEmpty(), IsTrue)
		} else {
			expectedJSON, err := json.Marshal(check.bundle)
			c.Assert(err, IsNil)

			gotJSON, err := json.Marshal(got)
			c.Assert(err, IsNil)
			c.Assert(string(gotJSON), Equals, string(expectedJSON))
		}
	}
}

func (s *testDBSuite6) TestPlacementPolicy(c *C) {
	clearAllBundles(c)
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
		"FOLLOWERS=3 " +
		"FOLLOWER_CONSTRAINTS=\"[+disk=ssd]\"")

	checkFunc := func(policyInfo *model.PolicyInfo) {
		c.Assert(policyInfo.ID != 0, Equals, true)
		c.Assert(policyInfo.Name.L, Equals, "x")
		c.Assert(policyInfo.Followers, Equals, uint64(3))
		c.Assert(policyInfo.FollowerConstraints, Equals, "[+disk=ssd]")
		c.Assert(policyInfo.Voters, Equals, uint64(0))
		c.Assert(policyInfo.VoterConstraints, Equals, "")
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

	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(0, Equals, len(bundles))

	tk.MustExec("drop placement policy x")
	tk.MustGetErrCode("drop placement policy x", mysql.ErrPlacementPolicyNotExists)
	tk.MustExec("drop placement policy if exists x")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 8239 Unknown placement policy 'x'"))

	// TODO: privilege check & constraint syntax check.
}

func (s *testDBSuite6) TestPlacementFollowers(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")

	defer tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop placement policy if exists x")

	tk.MustExec("drop placement policy if exists x")
	tk.MustGetErrMsg("create placement policy x FOLLOWERS=99", "invalid placement option: followers should be less than or equal to 8: 99")

	tk.MustExec("drop placement policy if exists x")
	tk.MustExec("create placement policy x FOLLOWERS=4")
	tk.MustGetErrMsg("alter placement policy x FOLLOWERS=99", "invalid placement option: followers should be less than or equal to 8: 99")

	tk.MustExec("drop table if exists t1")
	tk.MustGetErrMsg("create table t1 (a int) followers=99;", "invalid placement option: followers should be less than or equal to 8: 99")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int) followers=4;")
	tk.MustGetErrMsg("alter table t1 followers=99;", "invalid placement option: followers should be less than or equal to 8: 99")
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
			settings: "FOLLOWERS=3 LEARNERS=1 " +
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
			tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east\" NULL"))
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

func (s *testDBSuite6) TestCreateOrReplacePlacementPolicy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists x")

	// If the policy does not exist, CREATE OR REPLACE PLACEMENT POLICY is the same as CREATE PLACEMENT POLICY
	tk.MustExec("create or replace placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1,cn-east\"")
	defer tk.MustExec("drop placement policy if exists x")
	tk.MustQuery("show create placement policy x").Check(testkit.Rows("x CREATE PLACEMENT POLICY `x` PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east\""))

	// If the policy does exist, CREATE OR REPLACE PLACEMENT_POLICY is the same as ALTER PLACEMENT POLICY.
	tk.MustExec("create or replace placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1\"")
	tk.MustQuery("show create placement policy x").Check(testkit.Rows("x CREATE PLACEMENT POLICY `x` PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1\""))

	// Cannot be used together with the if not exists clause. Ref: https://mariadb.com/kb/en/create-view
	tk.MustGetErrMsg("create or replace placement policy if not exists x primary_region=\"cn-east-1\" regions=\"cn-east-1\"", "[ddl:1221]Incorrect usage of OR REPLACE and IF NOT EXISTS")
}

func (s *testDBSuite6) TestAlterPlacementPolicy(c *C) {
	clearAllBundles(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists x")
	tk.MustExec("drop table if exists tp")
	tk.MustExec("create placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1,cn-east\"")
	defer tk.MustExec("drop placement policy if exists x")

	// create a table ref to policy x, testing for alter policy will update PD bundles
	tk.MustExec(`CREATE TABLE tp (id INT) placement policy x PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000) placement policy x
	);`)
	defer tk.MustExec("drop table if exists tp")

	policy, ok := s.dom.InfoSchema().PolicyByName(model.NewCIStr("x"))
	c.Assert(ok, IsTrue)

	// test for normal cases
	tk.MustExec("alter placement policy x PRIMARY_REGION=\"bj\" REGIONS=\"bj,sh\"")
	tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"bj\" REGIONS=\"bj,sh\" NULL"))
	tk.MustQuery("select * from information_schema.placement_policies where policy_name = 'x'").Check(testkit.Rows(strconv.FormatInt(policy.ID, 10) + " def x bj bj,sh      0 0"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	tk.MustExec("alter placement policy x " +
		"PRIMARY_REGION=\"bj\" " +
		"REGIONS=\"bj\" " +
		"SCHEDULE=\"EVEN\"")
	tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"bj\" REGIONS=\"bj\" SCHEDULE=\"EVEN\" NULL"))
	tk.MustQuery("select * from INFORMATION_SCHEMA.PLACEMENT_POLICIES WHERE POLICY_NAME='x'").Check(testkit.Rows(strconv.FormatInt(policy.ID, 10) + " def x bj bj     EVEN 0 0"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	tk.MustExec("alter placement policy x " +
		"LEADER_CONSTRAINTS=\"[+region=us-east-1]\" " +
		"FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" " +
		"FOLLOWERS=3")
	tk.MustQuery("show placement where target='POLICY x'").Check(
		testkit.Rows("POLICY x LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" NULL"),
	)
	tk.MustQuery("SELECT POLICY_NAME,LEADER_CONSTRAINTS,FOLLOWER_CONSTRAINTS,FOLLOWERS FROM information_schema.PLACEMENT_POLICIES WHERE POLICY_NAME = 'x'").Check(
		testkit.Rows("x [+region=us-east-1] [+region=us-east-2] 3"),
	)
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	tk.MustExec("alter placement policy x " +
		"VOTER_CONSTRAINTS=\"[+region=bj]\" " +
		"LEARNER_CONSTRAINTS=\"[+region=sh]\" " +
		"CONSTRAINTS=\"[+disk=ssd]\"" +
		"VOTERS=5 " +
		"LEARNERS=3")
	tk.MustQuery("show placement where target='POLICY x'").Check(
		testkit.Rows("POLICY x CONSTRAINTS=\"[+disk=ssd]\" VOTERS=5 VOTER_CONSTRAINTS=\"[+region=bj]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=sh]\" NULL"),
	)
	tk.MustQuery("SELECT " +
		"CATALOG_NAME,POLICY_NAME," +
		"PRIMARY_REGION,REGIONS,CONSTRAINTS,LEADER_CONSTRAINTS,FOLLOWER_CONSTRAINTS,LEARNER_CONSTRAINTS," +
		"SCHEDULE,FOLLOWERS,LEARNERS FROM INFORMATION_SCHEMA.placement_policies WHERE POLICY_NAME='x'").Check(
		testkit.Rows("def x   [+disk=ssd]   [+region=sh]  0 3"),
	)
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// test alter not exist policies
	tk.MustExec("drop table tp")
	tk.MustExec("drop placement policy x")
	tk.MustGetErrCode("alter placement policy x REGIONS=\"bj,sh\"", mysql.ErrPlacementPolicyNotExists)
	tk.MustGetErrCode("alter placement policy x2 REGIONS=\"bj,sh\"", mysql.ErrPlacementPolicyNotExists)
	tk.MustQuery("select * from INFORMATION_SCHEMA.PLACEMENT_POLICIES WHERE POLICY_NAME='x'").Check(testkit.Rows())
}

func (s *testDBSuite6) TestCreateTableWithPlacementPolicy(c *C) {
	clearAllBundles(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")
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
	checkExistTableBundlesInPD(c, s.dom, "test", "t")

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
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop placement policy if exists x")
	tk.MustExec("drop placement policy if exists y")
}

func (s *testDBSuite6) getClonedTable(dbName string, tableName string) (*model.TableInfo, error) {
	tbl, err := s.dom.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		return nil, err
	}

	tblMeta := tbl.Meta()
	tblMeta = tblMeta.Clone()
	policyRef := *tblMeta.PlacementPolicyRef
	tblMeta.PlacementPolicyRef = &policyRef
	return tblMeta, nil
}

func (s *testDBSuite6) getClonedDatabase(dbName string) (*model.DBInfo, bool) {
	db, ok := s.dom.InfoSchema().SchemaByName(model.NewCIStr(dbName))
	if !ok {
		return nil, ok
	}

	db = db.Clone()
	policyRef := *db.PlacementPolicyRef
	db.PlacementPolicyRef = &policyRef
	return db, true
}

func (s *testDBSuite6) TestCreateTableWithInfoPlacement(c *C) {
	clearAllBundles(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop database if exists test2")
	tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p1 followers=1")
	defer tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("create table t1(a int) placement policy p1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create database test2")
	defer tk.MustExec("drop database if exists test2")

	tbl, err := s.getClonedTable("test", "t1")
	c.Assert(err, IsNil)
	policy, ok := s.dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	c.Assert(ok, IsTrue)
	c.Assert(tbl.PlacementPolicyRef.ID, Equals, policy.ID)

	tk.MustExec("alter table t1 placement policy='default'")
	tk.MustExec("drop placement policy p1")
	tk.MustExec("create placement policy p1 followers=2")
	c.Assert(s.dom.DDL().CreateTableWithInfo(tk.Se, model.NewCIStr("test2"), tbl, ddl.OnExistError), IsNil)
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table test2.t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */"))
	tk.MustQuery("show placement where target='TABLE test2.t1'").Check(testkit.Rows("TABLE test2.t1 FOLLOWERS=2 PENDING"))

	// The ref id for new table should be the new policy id
	tbl2, err := s.getClonedTable("test2", "t1")
	c.Assert(err, IsNil)
	policy2, ok := s.dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	c.Assert(ok, IsTrue)
	c.Assert(tbl2.PlacementPolicyRef.ID, Equals, policy2.ID)
	c.Assert(policy2.ID != policy.ID, IsTrue)

	// Test policy not exists
	tbl2.Name = model.NewCIStr("t3")
	tbl2.PlacementPolicyRef.Name = model.NewCIStr("pxx")
	err = s.dom.DDL().CreateTableWithInfo(tk.Se, model.NewCIStr("test2"), tbl2, ddl.OnExistError)
	c.Assert(err.Error(), Equals, "[schema:8239]Unknown placement policy 'pxx'")
}

func (s *testDBSuite6) TestCreateSchemaWithInfoPlacement(c *C) {
	clearAllBundles(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists test2")
	tk.MustExec("drop database if exists test3")
	tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p1 followers=1")
	defer tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("create database test2 placement policy p1")
	defer tk.MustExec("drop database if exists test2")
	defer tk.MustExec("drop database if exists test3")

	db, ok := s.getClonedDatabase("test2")
	c.Assert(ok, IsTrue)
	policy, ok := s.dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	c.Assert(ok, IsTrue)
	c.Assert(db.PlacementPolicyRef.ID, Equals, policy.ID)

	db2 := db.Clone()
	db2.Name = model.NewCIStr("test3")
	tk.MustExec("alter database test2 placement policy='default'")
	tk.MustExec("drop placement policy p1")
	tk.MustExec("create placement policy p1 followers=2")
	c.Assert(s.dom.DDL().CreateSchemaWithInfo(tk.Se, db2, ddl.OnExistError), IsNil)
	tk.MustQuery("show create database test2").Check(testkit.Rows("test2 CREATE DATABASE `test2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	tk.MustQuery("show create database test3").Check(testkit.Rows("test3 CREATE DATABASE `test3` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] PLACEMENT POLICY=`p1` */"))
	tk.MustQuery("show placement where target='DATABASE test3'").Check(testkit.Rows("DATABASE test3 FOLLOWERS=2 SCHEDULED"))

	// The ref id for new table should be the new policy id
	db2, ok = s.getClonedDatabase("test3")
	c.Assert(ok, IsTrue)
	policy2, ok := s.dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	c.Assert(ok, IsTrue)
	c.Assert(db2.PlacementPolicyRef.ID, Equals, policy2.ID)
	c.Assert(policy2.ID != policy.ID, IsTrue)

	// Test policy not exists
	db2.Name = model.NewCIStr("test4")
	db2.PlacementPolicyRef.Name = model.NewCIStr("p2")
	err := s.dom.DDL().CreateSchemaWithInfo(tk.Se, db2, ddl.OnExistError)
	c.Assert(err.Error(), Equals, "[schema:8239]Unknown placement policy 'p2'")
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
	tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" SCHEDULE=\"EVEN\" NULL"))

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
	clearAllBundles(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_enable_direct_placement=1")
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
	checkExistTableBundlesInPD(c, s.dom, "test", "t1")

	tk.MustExec("alter table t1 partition p0 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 ")

	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Partitions WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't1' AND PARTITION_NAME = 'p0'").Check(testkit.Rows(`def test t1 p0 <nil> PRIMARY_REGION="cn-east-1" REGIONS="cn-east-1, cn-east-2" FOLLOWERS=2`))
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Partitions WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't1' AND PARTITION_NAME = 'p1'").Check(testkit.Rows(`def test t1 p1 <nil> <nil>`))
	tbl := testGetTableByName(c, tk.Se, "test", "t1")
	c.Assert(tbl, NotNil)
	ptDef := testGetPartitionDefinitionsByName(c, tk.Se, "test", "t1", "p0")
	c.Assert(ptDef.PlacementPolicyRef, IsNil)
	c.Assert(ptDef.DirectPlacementOpts, NotNil)
	checkExistTableBundlesInPD(c, s.dom, "test", "t1")

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
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Partitions WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't1' AND PARTITION_NAME = 'p0'").Check(testkit.Rows(`def test t1 p0 x <nil>`))
	checkExistTableBundlesInPD(c, s.dom, "test", "t1")

	ptDef = testGetPartitionDefinitionsByName(c, tk.Se, "test", "t1", "p0")
	c.Assert(ptDef, NotNil)
	c.Assert(ptDef.PlacementPolicyRef, NotNil)
	c.Assert(ptDef.PlacementPolicyRef.Name.L, Equals, "x")
	c.Assert(ptDef.PlacementPolicyRef.ID != 0, Equals, true)

	tk.MustExec("alter table t1 partition p0 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2 ")
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Partitions WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't1' AND PARTITION_NAME = 'p0'").Check(testkit.Rows("def test t1 p0 <nil> PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1, cn-east-2\" FOLLOWERS=2"))

	ptDef = testGetPartitionDefinitionsByName(c, tk.Se, "test", "t1", "p0")
	c.Assert(ptDef, NotNil)
	c.Assert(ptDef.DirectPlacementOpts, NotNil)
	checkExistTableBundlesInPD(c, s.dom, "test", "t1")

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
	clearAllBundles(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")

	tk.MustExec("drop table if exists t, t0")
	tk.MustExec("drop placement policy if exists x")

	// test table inherit database's placement rules.
	tk.MustExec("create database mydb constraints=\"[+zone=hangzhou]\"")
	tk.MustQuery("show create database mydb").Check(testkit.Rows("mydb CREATE DATABASE `mydb` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] CONSTRAINTS=\"[+zone=hangzhou]\" */"))

	tk.MustExec("use mydb")
	tk.MustExec("create table t(a int)")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] CONSTRAINTS=\"[+zone=hangzhou]\" */"))
	checkExistTableBundlesInPD(c, s.dom, "mydb", "t")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t(a int) constraints=\"[+zone=suzhou]\"")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] CONSTRAINTS=\"[+zone=suzhou]\" */"))
	checkExistTableBundlesInPD(c, s.dom, "mydb", "t")
	tk.MustExec("drop table if exists t")

	// test create table like should not inherit database's placement rules.
	tk.MustExec("create table t0 (a int) placement policy 'default'")
	tk.MustQuery("show create table t0").Check(testkit.Rows("t0 CREATE TABLE `t0` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	checkExistTableBundlesInPD(c, s.dom, "mydb", "t0")
	tk.MustExec("create table t1 like t0")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	checkExistTableBundlesInPD(c, s.dom, "mydb", "t1")
	tk.MustExec("drop table if exists t0, t")

	// table will inherit db's placement rules, which is shared by all partition as default one.
	tk.MustExec("create table t(a int) partition by range(a) (partition p0 values less than (100), partition p1 values less than (200))")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] CONSTRAINTS=\"[+zone=hangzhou]\" */\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (200))"))
	checkExistTableBundlesInPD(c, s.dom, "mydb", "t")
	tk.MustExec("drop table if exists t")

	// partition's specified placement rules will override the default one.
	tk.MustExec("create table t(a int) partition by range(a) (partition p0 values less than (100) constraints=\"[+zone=suzhou]\", partition p1 values less than (200))")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] CONSTRAINTS=\"[+zone=hangzhou]\" */\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] CONSTRAINTS=\"[+zone=suzhou]\" */,\n" +
		" PARTITION `p1` VALUES LESS THAN (200))"))
	checkExistTableBundlesInPD(c, s.dom, "mydb", "t")
	tk.MustExec("drop table if exists t")

	// test partition override table's placement rules.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int) CONSTRAINTS=\"[+zone=suzhou]\" partition by range(a) (partition p0 values less than (100) CONSTRAINTS=\"[+zone=changzhou]\", partition p1 values less than (200))")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] CONSTRAINTS=\"[+zone=suzhou]\" */\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] CONSTRAINTS=\"[+zone=changzhou]\" */,\n" +
		" PARTITION `p1` VALUES LESS THAN (200))"))
	checkExistTableBundlesInPD(c, s.dom, "mydb", "t")
}

func (s *testDBSuite6) TestDatabasePlacement(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists db2")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("set @@tidb_enable_direct_placement=1")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	policy, ok := tk.Se.GetInfoSchema().(infoschema.InfoSchema).PolicyByName(model.NewCIStr("p1"))
	c.Assert(ok, IsTrue)

	tk.MustExec(`create database db2`)
	defer tk.MustExec("drop database db2")
	tk.MustQuery("show create database db2").Check(testkit.Rows(
		"db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))

	// alter with policy
	tk.MustExec("alter database db2 placement policy p1")
	tk.MustQuery("show create database db2").Check(testkit.Rows(
		"db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] PLACEMENT POLICY=`p1` */",
	))

	db, ok := tk.Se.GetInfoSchema().(infoschema.InfoSchema).SchemaByName(model.NewCIStr("db2"))
	c.Assert(ok, IsTrue)
	c.Assert(db.PlacementPolicyRef.ID, Equals, policy.ID)
	c.Assert(db.DirectPlacementOpts, IsNil)

	// alter with direct placement
	tk.MustExec("alter database db2 primary_region='r2' regions='r1,r2'")
	tk.MustQuery("show create database db2").Check(testkit.Rows(
		"db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] PRIMARY_REGION=\"r2\" REGIONS=\"r1,r2\" */",
	))

	// reset with placement policy 'default'
	tk.MustExec("alter database db2 placement policy default")
	tk.MustQuery("show create database db2").Check(testkit.Rows(
		"db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))

	// error invalid policy
	err := tk.ExecToErr("alter database db2 placement policy px")
	c.Assert(err.Error(), Equals, "[schema:8239]Unknown placement policy 'px'")

	// error when policy and direct options both set
	err = tk.ExecToErr("alter database db2 placement policy p1 primary_region='r2' regions='r2'")
	c.Assert(err.Error(), Equals, "[ddl:8240]Placement policy 'p1' can't co-exist with direct placement options")

	// error for invalid placement opt
	err = tk.ExecToErr("alter database db2 primary_region='r2' regions='r2' leader_constraints='[+region=bj]'")
	c.Assert(err.Error(), Equals, "invalid placement option: should be [LEADER/VOTER/LEARNER/FOLLOWER]_CONSTRAINTS=.. [VOTERS/FOLLOWERS/LEARNERS]=.., mixed other sugar options PRIMARY_REGION=\"r2\" REGIONS=\"r2\" LEADER_CONSTRAINTS=\"[+region=bj]\"")

	// failed alter has no effect
	tk.MustQuery("show create database db2").Check(testkit.Rows(
		"db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))
}

func (s *testDBSuite6) TestDropDatabaseGCPlacement(c *C) {
	clearAllBundles(c)
	failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`)
	defer func(originGC bool) {
		failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed")
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())
	ddl.EmulatorGCDisable()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists db2")

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")

	tk.MustExec("create table t (id int) primary_region='r0' regions='r0'")
	defer tk.MustExec("drop table if exists t")

	tk.MustExec("create database db2")
	tk.MustExec("create table db2.t0 (id int)")
	tk.MustExec("create table db2.t1 (id int) primary_region='r1' regions='r1,r2'")
	tk.MustExec(`create table db2.t2 (id int) primary_region='r1' regions='r1,r2' PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) primary_region='r2' regions='r2',
        PARTITION p1 VALUES LESS THAN (1000)
	)`)

	is := tk.Se.GetInfoSchema().(infoschema.InfoSchema)
	t, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	tk.MustExec("drop database db2")

	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bundles), Equals, 4)

	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	c.Assert(err, IsNil)
	c.Assert(gcWorker.DeleteRanges(context.TODO(), math.MaxInt64), IsNil)

	bundles, err = infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bundles), Equals, 1)
	c.Assert(bundles[0].ID, Equals, placement.GroupID(t.Meta().ID))
}

func (s *testDBSuite6) TestDropTableGCPlacement(c *C) {
	clearAllBundles(c)
	failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`)
	defer func(originGC bool) {
		failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed")
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())
	ddl.EmulatorGCDisable()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")

	tk.MustExec("create table t0 (id int)")
	defer tk.MustExec("drop table if exists t0")

	tk.MustExec("create table t1 (id int) primary_region='r1' regions='r1,r2'")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec(`create table t2 (id int) primary_region='r1' regions='r1,r2' PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) primary_region='r2' regions='r2',
        PARTITION p1 VALUES LESS THAN (1000)
	)`)
	defer tk.MustExec("drop table if exists t2")

	is := tk.Se.GetInfoSchema().(infoschema.InfoSchema)
	t1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)

	tk.MustExec("drop table t2")

	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bundles), Equals, 3)

	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	c.Assert(err, IsNil)
	c.Assert(gcWorker.DeleteRanges(context.TODO(), math.MaxInt64), IsNil)

	bundles, err = infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bundles), Equals, 1)
	c.Assert(bundles[0].ID, Equals, placement.GroupID(t1.Meta().ID))
}

func (s *testDBSuite6) TestAlterTablePlacement(c *C) {
	clearAllBundles(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")
	tk.MustExec("drop table if exists tp")
	tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	policy, ok := tk.Se.GetInfoSchema().(infoschema.InfoSchema).PolicyByName(model.NewCIStr("p1"))
	c.Assert(ok, IsTrue)

	tk.MustExec(`CREATE TABLE tp (id INT) PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000)
	);`)
	defer tk.MustExec("drop table tp")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// alter with policy
	tk.MustExec("alter table tp placement policy p1")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))

	tb, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(tb.Meta().PlacementPolicyRef.ID, Equals, policy.ID)
	c.Assert(tb.Meta().DirectPlacementOpts, IsNil)
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// alter with direct placement
	tk.MustExec("alter table tp primary_region='r2' regions='r1,r2'")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PRIMARY_REGION=\"r2\" REGIONS=\"r1,r2\" */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// reset with placement policy 'default'
	tk.MustExec("alter table tp placement policy default")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// error invalid policy
	err = tk.ExecToErr("alter table tp placement policy px")
	c.Assert(err.Error(), Equals, "[schema:8239]Unknown placement policy 'px'")

	// error when policy and direct options both set
	err = tk.ExecToErr("alter table tp placement policy p1 primary_region='r2' regions='r2'")
	c.Assert(err.Error(), Equals, "[ddl:8240]Placement policy 'p1' can't co-exist with direct placement options")

	// error for invalid placement opt
	err = tk.ExecToErr("alter table tp primary_region='r2' regions='r2' leader_constraints='[+region=bj]'")
	c.Assert(err.Error(), Equals, "invalid placement option: should be [LEADER/VOTER/LEARNER/FOLLOWER]_CONSTRAINTS=.. [VOTERS/FOLLOWERS/LEARNERS]=.., mixed other sugar options PRIMARY_REGION=\"r2\" REGIONS=\"r2\" LEADER_CONSTRAINTS=\"[+region=bj]\"")

	// failed alter has no effect
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")
}

func (s *testDBSuite6) TestDropTablePartitionGCPlacement(c *C) {
	clearAllBundles(c)
	failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`)
	defer func(originGC bool) {
		failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed")
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())
	ddl.EmulatorGCDisable()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")

	tk.MustExec("create table t0 (id int)")
	defer tk.MustExec("drop table if exists t0")

	tk.MustExec("create table t1 (id int) primary_region='r1' regions='r1,r2'")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec(`create table t2 (id int) primary_region='r1' regions='r1,r2' PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) primary_region='r2' regions='r2',
        PARTITION p1 VALUES LESS THAN (1000) primary_region='r3' regions='r3'
	)`)
	defer tk.MustExec("drop table if exists t2")

	is := tk.Se.GetInfoSchema().(infoschema.InfoSchema)
	t1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	t2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)

	tk.MustExec("alter table t2 drop partition p0")

	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bundles), Equals, 4)

	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	c.Assert(err, IsNil)
	c.Assert(gcWorker.DeleteRanges(context.TODO(), math.MaxInt64), IsNil)

	bundles, err = infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bundles), Equals, 3)
	bundlesMap := make(map[string]*placement.Bundle)
	for _, bundle := range bundles {
		bundlesMap[bundle.ID] = bundle
	}
	_, ok := bundlesMap[placement.GroupID(t1.Meta().ID)]
	c.Assert(ok, IsTrue)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().ID)]
	c.Assert(ok, IsTrue)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().Partition.Definitions[1].ID)]
	c.Assert(ok, IsTrue)
}

func (s *testDBSuite6) TestAlterTablePartitionPlacement(c *C) {
	clearAllBundles(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")
	tk.MustExec("drop table if exists tp")
	tk.MustExec("drop placement policy if exists p0")
	tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p0 primary_region='r0' regions='r0'")
	defer tk.MustExec("drop placement policy p0")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	policy, ok := tk.Se.GetInfoSchema().(infoschema.InfoSchema).PolicyByName(model.NewCIStr("p1"))
	c.Assert(ok, IsTrue)

	tk.MustExec(`CREATE TABLE tp (id INT) placement policy p0 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000)
	);`)
	defer tk.MustExec("drop table tp")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p0` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// alter with policy
	tk.MustExec("alter table tp partition p0 placement policy p1")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p0` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))

	tb, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(tb.Meta().Partition.Definitions[0].PlacementPolicyRef.ID, Equals, policy.ID)
	c.Assert(tb.Meta().Partition.Definitions[0].DirectPlacementOpts, IsNil)
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// alter with direct placement
	tk.MustExec("alter table tp partition p1 primary_region='r2' regions='r1,r2'")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p0` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PRIMARY_REGION=\"r2\" REGIONS=\"r1,r2\" */)"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	tk.MustExec("alter table tp partition p1 primary_region='r3' regions='r3,r4'")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p0` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PRIMARY_REGION=\"r3\" REGIONS=\"r3,r4\" */)"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// reset with placement policy 'default'
	tk.MustExec("alter table tp partition p1 placement policy default")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p0` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	tk.MustExec("alter table tp partition p0 placement policy default")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p0` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// error invalid policy
	err = tk.ExecToErr("alter table tp partition p1 placement policy px")
	c.Assert(err.Error(), Equals, "[schema:8239]Unknown placement policy 'px'")

	// error when policy and direct options both set
	err = tk.ExecToErr("alter table tp partition p0 placement policy p1 primary_region='r2' regions='r2'")
	c.Assert(err.Error(), Equals, "[ddl:8240]Placement policy 'p1' can't co-exist with direct placement options")

	// error for invalid placement opt
	err = tk.ExecToErr("alter table tp partition p1 primary_region='r2' regions='r2' leader_constraints='[+region=bj]'")
	c.Assert(err.Error(), Equals, "invalid placement option: should be [LEADER/VOTER/LEARNER/FOLLOWER]_CONSTRAINTS=.. [VOTERS/FOLLOWERS/LEARNERS]=.., mixed other sugar options PRIMARY_REGION=\"r2\" REGIONS=\"r2\" LEADER_CONSTRAINTS=\"[+region=bj]\"")

	// error invalid partition name
	err = tk.ExecToErr("alter table tp partition p2 primary_region='r2' regions='r2'")
	c.Assert(err.Error(), Equals, "[table:1735]Unknown partition 'p2' in table 'tp'")

	// failed alter has no effect
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p0` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")
}

func (s *testDBSuite6) TestAddPartitionWithPlacement(c *C) {
	clearAllBundles(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")
	tk.MustExec("drop table if exists tp")
	tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	policy, ok := tk.Se.GetInfoSchema().(infoschema.InfoSchema).PolicyByName(model.NewCIStr("p1"))
	c.Assert(ok, IsTrue)

	tk.MustExec(`CREATE TABLE tp (id INT) FOLLOWERS=1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000)
	);`)
	defer tk.MustExec("drop table tp")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] FOLLOWERS=1 */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// Add partitions
	tk.MustExec(`alter table tp add partition (
		partition p2 values less than (10000) placement policy p1,
		partition p3 values less than (100000) primary_region="r1" regions="r1,r2",
		partition p4 values less than (1000000) placement policy default
	)`)
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] FOLLOWERS=1 */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p3` VALUES LESS THAN (100000) /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" */,\n" +
		" PARTITION `p4` VALUES LESS THAN (1000000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	tb, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(tb.Meta().Partition.Definitions[2].PlacementPolicyRef.ID, Equals, policy.ID)

	// error invalid policy
	err = tk.ExecToErr("alter table tp add partition (partition p5 values less than (10000000) placement policy px)")
	c.Assert(err.Error(), Equals, "[schema:8239]Unknown placement policy 'px'")

	// error when policy and direct options both set
	err = tk.ExecToErr("alter table tp add partition (partition p5 values less than (10000000) placement policy p1 primary_region='r2' regions='r2')")
	c.Assert(err.Error(), Equals, "[ddl:8240]Placement policy 'p1' can't co-exist with direct placement options")

	// error for invalid placement opt
	err = tk.ExecToErr("alter table tp add partition (partition p5 values less than (10000000) primary_region='r2' regions='r2' leader_constraints='[+region=bj]')")
	c.Assert(err.Error(), Equals, "invalid placement option: should be [LEADER/VOTER/LEARNER/FOLLOWER]_CONSTRAINTS=.. [VOTERS/FOLLOWERS/LEARNERS]=.., mixed other sugar options PRIMARY_REGION=\"r2\" REGIONS=\"r2\" LEADER_CONSTRAINTS=\"[+region=bj]\"")

	// failed alter has no effect
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] FOLLOWERS=1 */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p3` VALUES LESS THAN (100000) /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" */,\n" +
		" PARTITION `p4` VALUES LESS THAN (1000000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")
}

func (s *testDBSuite6) TestTruncateTableWithPlacement(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")

	tk.MustExec("drop table if exists t1, tp")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	tk.MustExec("create placement policy p2 primary_region='r2' regions='r2'")
	defer tk.MustExec("drop placement policy p2")

	policy1, ok := tk.Se.GetInfoSchema().(infoschema.InfoSchema).PolicyByName(model.NewCIStr("p1"))
	c.Assert(ok, IsTrue)

	policy2, ok := tk.Se.GetInfoSchema().(infoschema.InfoSchema).PolicyByName(model.NewCIStr("p2"))
	c.Assert(ok, IsTrue)

	tk.MustExec(`CREATE TABLE t1 (id INT) primary_region="r1" regions="r1"`)
	defer tk.MustExec("drop table t1")

	// test for normal table
	tk.MustQuery("show create table t1").Check(testkit.Rows("" +
		"t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1\" */"))

	t1, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tk.MustExec("TRUNCATE TABLE t1")
	tk.MustQuery("show create table t1").Check(testkit.Rows("" +
		"t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1\" */"))
	newT1, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(newT1.Meta().ID != t1.Meta().ID, IsTrue)

	// test for partitioned table
	tk.MustExec(`CREATE TABLE tp (id INT) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000) placement policy p2,
        PARTITION p2 VALUES LESS THAN (10000) primary_region="r1" regions="r1,r2"
	);`)
	defer tk.MustExec("drop table tp")

	tp, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(tp.Meta().PlacementPolicyRef.ID, Equals, policy1.ID)
	c.Assert(tp.Meta().Partition.Definitions[1].PlacementPolicyRef.ID, Equals, policy2.ID)
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p2` VALUES LESS THAN (10000) /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" */)"))

	tk.MustExec("TRUNCATE TABLE tp")
	newTp, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(newTp.Meta().ID != tp.Meta().ID, IsTrue)
	c.Assert(newTp.Meta().PlacementPolicyRef.ID, Equals, policy1.ID)
	c.Assert(newTp.Meta().Partition.Definitions[1].PlacementPolicyRef.ID, Equals, policy2.ID)
	for i := range []int{0, 1, 2} {
		c.Assert(newTp.Meta().Partition.Definitions[i].ID != tp.Meta().Partition.Definitions[i].ID, IsTrue)
	}
}

func (s *testDBSuite6) TestTruncateTableGCWithPlacement(c *C) {
	clearAllBundles(c)
	failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`)
	defer func(originGC bool) {
		failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed")
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())
	ddl.EmulatorGCDisable()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")

	tk.MustExec("create table t0 (id int)")
	defer tk.MustExec("drop table if exists t0")

	tk.MustExec("create table t1 (id int) primary_region='r1' regions='r1,r2'")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec(`create table t2 (id int) primary_region='r1' regions='r1,r2' PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) primary_region='r2' regions='r2',
        PARTITION p1 VALUES LESS THAN (1000)
	)`)
	defer tk.MustExec("drop table if exists t2")

	tk.MustExec("truncate table t2")

	is := tk.Se.GetInfoSchema().(infoschema.InfoSchema)
	t1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	t2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)

	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bundles), Equals, 5)

	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	c.Assert(err, IsNil)
	c.Assert(gcWorker.DeleteRanges(context.TODO(), math.MaxInt64), IsNil)

	bundles, err = infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bundles), Equals, 3)
	bundlesMap := make(map[string]*placement.Bundle)
	for _, bundle := range bundles {
		bundlesMap[bundle.ID] = bundle
	}
	_, ok := bundlesMap[placement.GroupID(t1.Meta().ID)]
	c.Assert(ok, IsTrue)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().ID)]
	c.Assert(ok, IsTrue)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().Partition.Definitions[0].ID)]
	c.Assert(ok, IsTrue)
}

func (s *testDBSuite6) TestTruncateTablePartitionWithPlacement(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")

	tk.MustExec("drop table if exists t1, tp")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("drop placement policy if exists p3")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	tk.MustExec("create placement policy p2 primary_region='r2' regions='r2'")
	defer tk.MustExec("drop placement policy p2")

	tk.MustExec("create placement policy p3 primary_region='r3' regions='r3'")
	defer tk.MustExec("drop placement policy p3")

	policy1, ok := tk.Se.GetInfoSchema().(infoschema.InfoSchema).PolicyByName(model.NewCIStr("p1"))
	c.Assert(ok, IsTrue)

	policy2, ok := tk.Se.GetInfoSchema().(infoschema.InfoSchema).PolicyByName(model.NewCIStr("p2"))
	c.Assert(ok, IsTrue)

	policy3, ok := tk.Se.GetInfoSchema().(infoschema.InfoSchema).PolicyByName(model.NewCIStr("p3"))
	c.Assert(ok, IsTrue)

	tk.MustExec(`CREATE TABLE t1 (id INT) primary_region="r1" regions="r1"`)
	defer tk.MustExec("drop table t1")

	// test for partitioned table
	tk.MustExec(`CREATE TABLE tp (id INT) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000) placement policy p2,
        PARTITION p2 VALUES LESS THAN (10000) placement policy p3,
        PARTITION p3 VALUES LESS THAN (100000) primary_region="r2" regions="r2"
	);`)
	defer tk.MustExec("drop table tp")

	tp, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)

	tk.MustExec("ALTER TABLE tp TRUNCATE partition p1,p3")
	newTp, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(newTp.Meta().ID, Equals, tp.Meta().ID)
	c.Assert(newTp.Meta().PlacementPolicyRef.ID, Equals, policy1.ID)
	c.Assert(newTp.Meta().Partition.Definitions[1].PlacementPolicyRef.ID, Equals, policy2.ID)
	c.Assert(newTp.Meta().Partition.Definitions[2].PlacementPolicyRef.ID, Equals, policy3.ID)
	c.Assert(newTp.Meta().Partition.Definitions[0].ID, Equals, tp.Meta().Partition.Definitions[0].ID)
	c.Assert(newTp.Meta().Partition.Definitions[1].ID != tp.Meta().Partition.Definitions[1].ID, IsTrue)
	c.Assert(newTp.Meta().Partition.Definitions[2].ID, Equals, tp.Meta().Partition.Definitions[2].ID)
	c.Assert(newTp.Meta().Partition.Definitions[3].ID != tp.Meta().Partition.Definitions[3].ID, IsTrue)

	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p2` VALUES LESS THAN (10000) /*T![placement] PLACEMENT POLICY=`p3` */,\n" +
		" PARTITION `p3` VALUES LESS THAN (100000) /*T![placement] PRIMARY_REGION=\"r2\" REGIONS=\"r2\" */)"))
}

func (s *testDBSuite6) TestTruncatePartitionGCWithPlacement(c *C) {
	clearAllBundles(c)
	failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`)
	defer func(originGC bool) {
		failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed")
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())
	ddl.EmulatorGCDisable()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")

	tk.MustExec("create table t0 (id int)")
	defer tk.MustExec("drop table if exists t0")

	tk.MustExec("create table t1 (id int) primary_region='r1' regions='r1,r2'")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec(`create table t2 (id int) primary_region='r1' regions='r1,r2' PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) primary_region='r2' regions='r2',
        PARTITION p1 VALUES LESS THAN (1000)
	)`)
	defer tk.MustExec("drop table if exists t2")

	tk.MustExec("alter table t2 truncate partition p0")

	is := tk.Se.GetInfoSchema().(infoschema.InfoSchema)
	t1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	t2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)

	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bundles), Equals, 4)

	gcWorker, err := gcworker.NewMockGCWorker(s.store)
	c.Assert(err, IsNil)
	c.Assert(gcWorker.DeleteRanges(context.TODO(), math.MaxInt64), IsNil)

	bundles, err = infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(len(bundles), Equals, 3)
	bundlesMap := make(map[string]*placement.Bundle)
	for _, bundle := range bundles {
		bundlesMap[bundle.ID] = bundle
	}
	_, ok := bundlesMap[placement.GroupID(t1.Meta().ID)]
	c.Assert(ok, IsTrue)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().ID)]
	c.Assert(ok, IsTrue)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().Partition.Definitions[0].ID)]
	c.Assert(ok, IsTrue)
}

func (s *testDBSuite6) TestExchangePartitionWithPlacement(c *C) {
	clearAllBundles(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_enable_exchange_partition=1")
	tk.MustExec("set @@tidb_enable_direct_placement=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, tp")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	tk.MustExec("create placement policy p2 primary_region='r2' regions='r2'")
	defer tk.MustExec("drop placement policy p2")

	policy1, ok := tk.Se.GetInfoSchema().(infoschema.InfoSchema).PolicyByName(model.NewCIStr("p1"))
	c.Assert(ok, IsTrue)

	policy2, ok := tk.Se.GetInfoSchema().(infoschema.InfoSchema).PolicyByName(model.NewCIStr("p2"))
	c.Assert(ok, IsTrue)

	tk.MustExec(`CREATE TABLE t1 (id INT) placement policy p1`)
	defer tk.MustExec("drop table t1")

	tk.MustExec(`CREATE TABLE t2 (id INT)`)
	defer tk.MustExec("drop table t2")

	t1, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	t1ID := t1.Meta().ID

	t2, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	t2ID := t2.Meta().ID

	tk.MustExec(`CREATE TABLE tp (id INT) primary_region="r1" regions="r1" PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000) placement policy p2,
        PARTITION p2 VALUES LESS THAN (10000) primary_region="r1" regions="r1,r2"
	);`)
	defer tk.MustExec("drop table tp")

	tp, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	tpID := tp.Meta().ID
	par0ID := tp.Meta().Partition.Definitions[0].ID
	par1ID := tp.Meta().Partition.Definitions[1].ID
	par2ID := tp.Meta().Partition.Definitions[2].ID

	// exchange par0, t1
	tk.MustExec("alter table tp exchange partition p0 with table t1")
	tk.MustQuery("show create table t1").Check(testkit.Rows("" +
		"t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */"))
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1\" */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p2` VALUES LESS THAN (10000) /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" */)"))
	tp, err = tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(tp.Meta().ID, Equals, tpID)
	c.Assert(tp.Meta().Partition.Definitions[0].ID, Equals, t1ID)
	c.Assert(tp.Meta().Partition.Definitions[0].DirectPlacementOpts, IsNil)
	c.Assert(tp.Meta().Partition.Definitions[0].PlacementPolicyRef, IsNil)
	t1, err = tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(t1.Meta().ID, Equals, par0ID)
	c.Assert(t1.Meta().DirectPlacementOpts, IsNil)
	c.Assert(t1.Meta().PlacementPolicyRef.ID, Equals, policy1.ID)
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// exchange par0, t2
	tk.MustExec("alter table tp exchange partition p0 with table t2")
	tk.MustQuery("show create table t2").Check(testkit.Rows("" +
		"t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1\" */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p2` VALUES LESS THAN (10000) /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" */)"))
	tp, err = tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(tp.Meta().ID, Equals, tpID)
	c.Assert(tp.Meta().Partition.Definitions[0].ID, Equals, t2ID)
	c.Assert(tp.Meta().Partition.Definitions[0].DirectPlacementOpts, IsNil)
	c.Assert(tp.Meta().Partition.Definitions[0].PlacementPolicyRef, IsNil)
	t2, err = tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	c.Assert(t2.Meta().ID, Equals, t1ID)
	c.Assert(t2.Meta().DirectPlacementOpts, IsNil)
	c.Assert(t2.Meta().PlacementPolicyRef, IsNil)
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// exchange par1, t1
	tk.MustExec("alter table tp exchange partition p1 with table t1")
	tk.MustQuery("show create table t1").Check(testkit.Rows("" +
		"t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */"))
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1\" */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p2` VALUES LESS THAN (10000) /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" */)"))
	tp, err = tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(tp.Meta().ID, Equals, tpID)
	c.Assert(tp.Meta().Partition.Definitions[1].ID, Equals, par0ID)
	c.Assert(tp.Meta().Partition.Definitions[1].DirectPlacementOpts, IsNil)
	c.Assert(tp.Meta().Partition.Definitions[1].PlacementPolicyRef.ID, Equals, policy2.ID)
	t1, err = tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(t1.Meta().ID, Equals, par1ID)
	c.Assert(t1.Meta().DirectPlacementOpts, IsNil)
	c.Assert(t1.Meta().PlacementPolicyRef.ID, Equals, policy1.ID)
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")

	// exchange par2, t2
	tk.MustExec("alter table tp exchange partition p2 with table t2")
	tk.MustQuery("show create table t2").Check(testkit.Rows("" +
		"t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1\" */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p2` VALUES LESS THAN (10000) /*T![placement] PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" */)"))
	tp, err = tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(tp.Meta().ID, Equals, tpID)
	c.Assert(tp.Meta().Partition.Definitions[2].ID, Equals, t1ID)
	c.Assert(tp.Meta().Partition.Definitions[2].DirectPlacementOpts.PrimaryRegion, Equals, "r1")
	c.Assert(tp.Meta().Partition.Definitions[2].DirectPlacementOpts.Regions, Equals, "r1,r2")
	c.Assert(tp.Meta().Partition.Definitions[2].PlacementPolicyRef, IsNil)
	t2, err = tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	c.Assert(t2.Meta().ID, Equals, par2ID)
	c.Assert(t2.Meta().DirectPlacementOpts, IsNil)
	c.Assert(t2.Meta().PlacementPolicyRef, IsNil)
	checkExistTableBundlesInPD(c, s.dom, "test", "tp")
}

func (s *testDBSuite6) TestPDFail(c *C) {
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/domain/infosync/putRuleBundlesError"), IsNil)
	}()

	clearAllBundles(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop table if exists t1, t2, tp")

	tk.MustExec("create placement policy p1 primary_region=\"cn-east-1\" regions=\"cn-east-1,cn-east\"")
	defer tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create table t1(id int)")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec(`CREATE TABLE tp (id INT) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000) placement policy p1
	);`)
	defer tk.MustExec("drop table if exists tp")
	existBundles, err := infosync.GetAllRuleBundles(context.TODO())
	c.Assert(err, IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/domain/infosync/putRuleBundlesError", "return(true)"), IsNil)

	// alter policy
	err = tk.ExecToErr("alter placement policy p1 primary_region='rx' regions='rx'")
	c.Assert(infosync.ErrHTTPServiceError.Equal(err), IsTrue)
	tk.MustQuery("show create placement policy p1").Check(testkit.Rows("p1 CREATE PLACEMENT POLICY `p1` PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east\""))
	checkAllBundlesNotChange(c, existBundles)

	// create table
	err = tk.ExecToErr("create table t2 (id int) placement policy p1")
	c.Assert(infosync.ErrHTTPServiceError.Equal(err), IsTrue)
	err = tk.ExecToErr("show create table t2")
	c.Assert(infoschema.ErrTableNotExists.Equal(err), IsTrue)
	checkAllBundlesNotChange(c, existBundles)

	// alter table
	err = tk.ExecToErr("alter table t1 placement policy p1")
	c.Assert(infosync.ErrHTTPServiceError.Equal(err), IsTrue)
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	checkAllBundlesNotChange(c, existBundles)

	// add partition
	err = tk.ExecToErr("alter table tp add partition (" +
		"partition p2 values less than (10000) placement policy p1," +
		"partition p3 values less than (100000) primary_region=\"r1\" regions=\"r1,r2\"" +
		")")
	c.Assert(infosync.ErrHTTPServiceError.Equal(err), IsTrue)
	tk.MustQuery("show create table tp").Check(testkit.Rows("tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p1` */)"))
	checkAllBundlesNotChange(c, existBundles)

	// alter partition
	err = tk.ExecToErr(`alter table tp PARTITION p1 primary_region="r2" regions="r2,r3"`)
	c.Assert(infosync.ErrHTTPServiceError.Equal(err), IsTrue)
	tk.MustQuery("show create table tp").Check(testkit.Rows("tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p1` */)"))
	checkAllBundlesNotChange(c, existBundles)

	// exchange partition
	tk.MustExec("alter table tp exchange partition p1 with table t1")
	c.Assert(infosync.ErrHTTPServiceError.Equal(err), IsTrue)
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table tp").Check(testkit.Rows("tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p1` */)"))
	checkAllBundlesNotChange(c, existBundles)
}

func (s *testDBSuite6) TestRecoverTableWithPlacementPolicy(c *C) {
	clearAllBundles(c)
	failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`)
	defer func(originGC bool) {
		failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed")
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())
	ddl.EmulatorGCDisable()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("drop placement policy if exists p3")
	tk.MustExec("drop table if exists tp1, tp2")

	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk.MustExec(fmt.Sprintf(safePointSQL, "20060102-15:04:05 -0700 MST"))

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1,r2'")
	defer tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p2 primary_region='r2' regions='r2,r3'")
	defer tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create placement policy p3 primary_region='r3' regions='r3,r4'")
	defer tk.MustExec("drop placement policy if exists p3")

	// test recover
	tk.MustExec(`CREATE TABLE tp1 (id INT) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) placement policy p2,
        PARTITION p1 VALUES LESS THAN (1000),
        PARTITION p2 VALUES LESS THAN (10000) placement policy p3
	);`)
	defer tk.MustExec("drop table if exists tp1")

	tk.MustExec("drop table tp1")
	tk.MustExec("recover table tp1")
	tk.MustQuery("show create table tp1").Check(testkit.Rows("tp1 CREATE TABLE `tp1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp1")

	// test flashback
	tk.MustExec(`CREATE TABLE tp2 (id INT) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) placement policy p2,
        PARTITION p1 VALUES LESS THAN (1000),
        PARTITION p2 VALUES LESS THAN (10000) placement policy p3
	);`)
	defer tk.MustExec("drop table if exists tp2")

	tk.MustExec("drop table tp1")
	tk.MustExec("drop table tp2")
	tk.MustExec("flashback table tp2")
	tk.MustQuery("show create table tp2").Check(testkit.Rows("tp2 CREATE TABLE `tp2` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp2")

	// test recover after police drop
	tk.MustExec("drop table tp2")
	tk.MustExec("drop placement policy p1")
	tk.MustExec("drop placement policy p2")
	tk.MustExec("drop placement policy p3")

	tk.MustExec("flashback table tp2 to tp3")
	tk.MustQuery("show create table tp3").Check(testkit.Rows("tp3 CREATE TABLE `tp3` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000))"))
	checkExistTableBundlesInPD(c, s.dom, "test", "tp3")
}
