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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/gcworker"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/stretchr/testify/require"
)

func checkExistTableBundlesInPD(t *testing.T, do *domain.Domain, dbName string, tbName string) {
	tblInfo, err := do.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tbName))
	require.NoError(t, err)

	require.NoError(t, kv.RunInNewTxn(context.TODO(), do.Store(), false, func(ctx context.Context, txn kv.Transaction) error {
		tt := meta.NewMeta(txn)
		checkTableBundlesInPD(t, do, tt, tblInfo.Meta())
		return nil
	}))
}

func checkAllBundlesNotChange(t *testing.T, bundles []*placement.Bundle) {
	currentBundles, err := infosync.GetAllRuleBundles(context.TODO())
	require.NoError(t, err)

	bundlesMap := make(map[string]*placement.Bundle)
	for _, bundle := range currentBundles {
		bundlesMap[bundle.ID] = bundle
	}
	require.Equal(t, len(currentBundles), len(bundlesMap))
	require.Equal(t, len(bundles), len(currentBundles))

	for _, bundle := range bundles {
		got, ok := bundlesMap[bundle.ID]
		require.True(t, ok)

		expectedJSON, err := json.Marshal(bundle)
		require.NoError(t, err)

		gotJSON, err := json.Marshal(got)
		require.NoError(t, err)
		require.Equal(t, string(expectedJSON), string(gotJSON))
	}
}

func checkTableBundlesInPD(t *testing.T, do *domain.Domain, tt *meta.Meta, tblInfo *model.TableInfo) {
	checks := make([]*struct {
		ID      string
		tableID int64
		bundle  *placement.Bundle
	}, 0)

	bundle, err := placement.NewTableBundle(tt, tblInfo)
	require.NoError(t, err)
	checks = append(checks, &struct {
		ID      string
		tableID int64
		bundle  *placement.Bundle
	}{ID: placement.GroupID(tblInfo.ID), tableID: tblInfo.ID, bundle: bundle})

	if tblInfo.Partition != nil {
		for _, def := range tblInfo.Partition.Definitions {
			bundle, err := placement.NewPartitionBundle(tt, def)
			require.NoError(t, err)
			checks = append(checks, &struct {
				ID      string
				tableID int64
				bundle  *placement.Bundle
			}{ID: placement.GroupID(def.ID), tableID: def.ID, bundle: bundle})
		}
	}

	is := do.InfoSchema()
	for _, check := range checks {
		pdGot, err := infosync.GetRuleBundle(context.TODO(), check.ID)
		require.NoError(t, err)
		isGot, ok := is.PlacementBundleByPhysicalTableID(check.tableID)
		if check.bundle == nil {
			require.True(t, pdGot.IsEmpty(), "bundle should be nil for table: %d", check.tableID)
			require.False(t, ok, "bundle should be nil for table: %d", check.tableID)
		} else {
			expectedJSON, err := json.Marshal(check.bundle)
			require.NoError(t, err)

			pdGotJSON, err := json.Marshal(pdGot)
			require.NoError(t, err)
			require.NotNil(t, pdGot)
			require.Equal(t, string(expectedJSON), string(pdGotJSON))

			isGotJSON, err := json.Marshal(isGot)
			require.NoError(t, err)
			require.NotNil(t, isGot)
			require.Equal(t, string(expectedJSON), string(isGotJSON))
		}
	}
}

func TestPlacementPolicy(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	// clearAllBundles(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists x")

	hook := &ddl.TestDDLCallback{Do: dom}
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
	dom.DDL().SetHook(hook)

	tk.MustExec("create placement policy x " +
		"LEARNERS=1 " +
		"LEARNER_CONSTRAINTS=\"[+region=cn-west-1]\" " +
		"FOLLOWERS=3 " +
		"FOLLOWER_CONSTRAINTS=\"[+disk=ssd]\"")

	checkFunc := func(policyInfo *model.PolicyInfo) {
		require.Equal(t, true, policyInfo.ID != 0)
		require.Equal(t, "x", policyInfo.Name.L)
		require.Equal(t, uint64(3), policyInfo.Followers)
		require.Equal(t, "[+disk=ssd]", policyInfo.FollowerConstraints)
		require.Equal(t, uint64(0), policyInfo.Voters)
		require.Equal(t, "", policyInfo.VoterConstraints)
		require.Equal(t, uint64(1), policyInfo.Learners)
		require.Equal(t, "[+region=cn-west-1]", policyInfo.LearnerConstraints)
		require.Equal(t, model.StatePublic, policyInfo.State)
		require.Equal(t, "", policyInfo.Schedule)
	}

	// Check the policy is correctly reloaded in the information schema.
	po := testGetPolicyByNameFromIS(t, tk.Session(), "x")
	checkFunc(po)

	// Check the policy is correctly written in the kv meta.
	po = testGetPolicyByIDFromMeta(t, store, policyID)
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
	require.NoError(t, err)
	require.Equal(t, len(bundles), 0)

	tk.MustExec("drop placement policy x")
	tk.MustGetErrCode("drop placement policy x", mysql.ErrPlacementPolicyNotExists)
	tk.MustExec("drop placement policy if exists x")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 8239 Unknown placement policy 'x'"))

	// TODO: privilege check & constraint syntax check.
}

func TestCreatePlacementPolicyWithInfo(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	// clearAllBundles(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tp")
	tk.MustExec("drop placement policy if exists p")
	tk.MustExec("create placement policy p " +
		"LEARNERS=1 " +
		"LEARNER_CONSTRAINTS=\"[+region=cn-west-1]\" " +
		"FOLLOWERS=3 " +
		"FOLLOWER_CONSTRAINTS=\"[+disk=ssd]\"")
	defer tk.MustExec("drop placement policy if exists p")
	defer tk.MustExec("drop placement policy if exists p2")
	tk.MustExec(`CREATE TABLE tp(id int) placement policy p PARTITION BY RANGE (id) (
PARTITION p0 VALUES LESS THAN (100) PLACEMENT POLICY p,
PARTITION p1 VALUES LESS THAN (1000))
`)
	defer tk.MustExec("drop table if exists tp")

	oldPolicy, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p"))
	oldPolicy = oldPolicy.Clone()
	require.True(t, ok)

	// create a non exist policy
	for _, onExist := range []ddl.OnExist{ddl.OnExistReplace, ddl.OnExistIgnore, ddl.OnExistError} {
		newPolicy := oldPolicy.Clone()
		newPolicy.Name = model.NewCIStr("p2")
		newPolicy.Followers = 2
		newPolicy.LearnerConstraints = "[+zone=z2]"
		tk.Session().SetValue(sessionctx.QueryString, "skip")
		err := dom.DDL().CreatePlacementPolicyWithInfo(tk.Session(), newPolicy.Clone(), onExist)
		require.NoError(t, err)
		// old policy should not be changed
		found, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p"))
		require.True(t, ok)
		checkPolicyEquals(t, oldPolicy, found)
		checkExistTableBundlesInPD(t, dom, "test", "tp")

		// new created policy
		found, ok = dom.InfoSchema().PolicyByName(model.NewCIStr("p2"))
		require.True(t, ok)
		// ID of the created policy should be reassigned
		require.NotEqual(t, newPolicy.ID, found.ID)
		newPolicy.ID = found.ID
		checkPolicyEquals(t, newPolicy, found)
		tk.MustExec("drop placement policy if exists p2")
	}

	// create same name policy with on exists error
	newPolicy := oldPolicy.Clone()
	newPolicy.ID = oldPolicy.ID + 1
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	err := dom.DDL().CreatePlacementPolicyWithInfo(tk.Session(), newPolicy.Clone(), ddl.OnExistError)
	require.Error(t, err)
	require.True(t, infoschema.ErrPlacementPolicyExists.Equal(err))
	found, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p"))
	require.True(t, ok)
	checkPolicyEquals(t, oldPolicy, found)
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// create same name policy with on exist ignore
	newPolicy = oldPolicy.Clone()
	newPolicy.ID = oldPolicy.ID + 1
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	err = dom.DDL().CreatePlacementPolicyWithInfo(tk.Session(), newPolicy.Clone(), ddl.OnExistIgnore)
	require.NoError(t, err)
	found, ok = dom.InfoSchema().PolicyByName(model.NewCIStr("p"))
	require.True(t, ok)
	checkPolicyEquals(t, oldPolicy, found)
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// create same name policy with on exist replace
	newPolicy = oldPolicy.Clone()
	newPolicy.ID = oldPolicy.ID + 1
	newPolicy.Followers = 1
	newPolicy.LearnerConstraints = "[+zone=z1]"
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	err = dom.DDL().CreatePlacementPolicyWithInfo(tk.Session(), newPolicy.Clone(), ddl.OnExistReplace)
	require.NoError(t, err)
	found, ok = dom.InfoSchema().PolicyByName(model.NewCIStr("p"))
	require.True(t, ok)
	// when replace a policy the old policy's id should not be changed
	newPolicy.ID = oldPolicy.ID
	checkPolicyEquals(t, newPolicy, found)
	checkExistTableBundlesInPD(t, dom, "test", "tp")
}

func checkPolicyEquals(t *testing.T, expected *model.PolicyInfo, actual *model.PolicyInfo) {
	require.Equal(t, expected.ID, actual.ID)
	require.Equal(t, expected.Name, actual.Name)
	require.Equal(t, *expected.PlacementSettings, *actual.PlacementSettings)
	require.Equal(t, expected.State, actual.State)
}

func TestPlacementFollowers(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	defer tk.MustExec("drop placement policy if exists x")

	tk.MustExec("drop placement policy if exists x")
	tk.MustGetErrMsg("create placement policy x FOLLOWERS=99", "invalid placement option: followers should be less than or equal to 8: 99")

	tk.MustExec("drop placement policy if exists x")
	tk.MustExec("create placement policy x FOLLOWERS=4")
	tk.MustGetErrMsg("alter placement policy x FOLLOWERS=99", "invalid placement option: followers should be less than or equal to 8: 99")
}

func testGetPolicyByIDFromMeta(t *testing.T, store kv.Storage, policyID int64) *model.PolicyInfo {
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
	require.Nil(t, err1)
	require.NotNil(t, policyInfo)
	return policyInfo
}

func testGetPolicyByNameFromIS(t *testing.T, ctx sessionctx.Context, policy string) *model.PolicyInfo {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	require.NoError(t, err)
	po, ok := dom.InfoSchema().PolicyByName(model.NewCIStr(policy))
	require.Equal(t, true, ok)
	return po
}

func TestPlacementValidation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
			require.NotNil(t, err)
			require.EqualErrorf(t, err, ca.errmsg, ca.name)
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
			require.Error(t, err)
			require.Equal(t, ca.errmsg, err.Error())
			tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east\" NULL"))
		}
	}
	tk.MustExec("drop placement policy x")
}

func TestResetSchemaPlacement(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists TestResetPlacementDB;")
	tk.MustExec("create placement policy `TestReset` followers=4;")
	tk.MustGetErrCode("create placement policy `default` followers=4;", mysql.ErrReservedSyntax)
	tk.MustGetErrCode("create placement policy default followers=4;", mysql.ErrParse)

	tk.MustExec("create database TestResetPlacementDB placement policy `TestReset`;")
	tk.MustExec("use TestResetPlacementDB")
	// Test for `=default`
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testkit.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PLACEMENT POLICY=`TestReset` */",
	))
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY=default;")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testkit.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))
	// Test for `SET DEFAULT`
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY=`TestReset`;")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testkit.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PLACEMENT POLICY=`TestReset` */",
	))
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY SET DEFAULT")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testkit.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))
	// Test for `= 'DEFAULT'`
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY=`TestReset`;")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testkit.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PLACEMENT POLICY=`TestReset` */",
	))
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY = 'DEFAULT'")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testkit.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))
	// Test for "= `DEFAULT`"
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY=`TestReset`;")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testkit.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PLACEMENT POLICY=`TestReset` */",
	))
	tk.MustExec("ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY = `DEFAULT`")
	tk.MustQuery(`show create database TestResetPlacementDB`).Check(testkit.RowsWithSep("|",
		"TestResetPlacementDB CREATE DATABASE `TestResetPlacementDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))

	tk.MustExec("drop placement policy `TestReset`;")
	tk.MustExec("drop database TestResetPlacementDB;")
}

func TestCreateOrReplacePlacementPolicy(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists x")
	tk.MustExec("drop table if exists tp")

	// If the policy does not exist, CREATE OR REPLACE PLACEMENT POLICY is the same as CREATE PLACEMENT POLICY
	tk.MustExec("create or replace placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1,cn-east\"")
	defer tk.MustExec("drop placement policy if exists x")
	tk.MustQuery("show create placement policy x").Check(testkit.Rows("x CREATE PLACEMENT POLICY `x` PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east\""))

	// create a table refers the policy
	tk.MustExec(`CREATE TABLE tp(id int) placement policy x PARTITION BY RANGE (id) (
PARTITION p0 VALUES LESS THAN (100) PLACEMENT POLICY x,
PARTITION p1 VALUES LESS THAN (1000))
`)
	defer tk.MustExec("drop table if exists tp")

	// If the policy does exist, CREATE OR REPLACE PLACEMENT_POLICY is the same as ALTER PLACEMENT POLICY.
	tk.MustExec("create or replace placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1\"")
	tk.MustQuery("show create placement policy x").Check(testkit.Rows("x CREATE PLACEMENT POLICY `x` PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1\""))
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// Cannot be used together with the if not exists clause. Ref: https://mariadb.com/kb/en/create-view
	tk.MustGetErrMsg("create or replace placement policy if not exists x primary_region=\"cn-east-1\" regions=\"cn-east-1\"", "[ddl:1221]Incorrect usage of OR REPLACE and IF NOT EXISTS")
}

func TestAlterPlacementPolicy(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	// clearAllBundles(t)
	tk := testkit.NewTestKit(t, store)
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

	policy, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("x"))
	require.True(t, ok)

	// test for normal cases
	tk.MustExec("alter placement policy x PRIMARY_REGION=\"bj\" REGIONS=\"bj,sh\"")
	tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"bj\" REGIONS=\"bj,sh\" NULL"))
	tk.MustQuery("select * from information_schema.placement_policies where policy_name = 'x'").Check(testkit.Rows(strconv.FormatInt(policy.ID, 10) + " def x bj bj,sh      2 0"))
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	tk.MustExec("alter placement policy x " +
		"PRIMARY_REGION=\"bj\" " +
		"REGIONS=\"bj\" " +
		"SCHEDULE=\"EVEN\"")
	tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"bj\" REGIONS=\"bj\" SCHEDULE=\"EVEN\" NULL"))
	tk.MustQuery("select * from INFORMATION_SCHEMA.PLACEMENT_POLICIES WHERE POLICY_NAME='x'").Check(testkit.Rows(strconv.FormatInt(policy.ID, 10) + " def x bj bj     EVEN 2 0"))
	checkExistTableBundlesInPD(t, dom, "test", "tp")

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
	checkExistTableBundlesInPD(t, dom, "test", "tp")

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
		testkit.Rows("def x   [+disk=ssd]   [+region=sh]  2 3"),
	)
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// test alter not exist policies
	tk.MustExec("drop table tp")
	tk.MustExec("drop placement policy x")
	tk.MustGetErrCode("alter placement policy x REGIONS=\"bj,sh\"", mysql.ErrPlacementPolicyNotExists)
	tk.MustGetErrCode("alter placement policy x2 REGIONS=\"bj,sh\"", mysql.ErrPlacementPolicyNotExists)
	tk.MustQuery("select * from INFORMATION_SCHEMA.PLACEMENT_POLICIES WHERE POLICY_NAME='x'").Check(testkit.Rows())
}

func TestCreateTableWithPlacementPolicy(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// clearAllBundles(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t,t_range_p,t_hash_p,t_list_p")
	tk.MustExec("drop placement policy if exists x")
	tk.MustExec("drop placement policy if exists y")
	defer func() {
		tk.MustExec("drop table if exists t,t_range_p,t_hash_p,t_list_p")
		tk.MustExec("drop placement policy if exists x")
		tk.MustExec("drop placement policy if exists y")
	}()

	// special constraints may be incompatible with common constraint.
	_, err := tk.Exec("create placement policy pn " +
		"FOLLOWERS=2 " +
		"FOLLOWER_CONSTRAINTS=\"[+zone=cn-east-1]\" " +
		"CONSTRAINTS=\"[+disk=ssd,-zone=cn-east-1]\"")
	require.Error(t, err)
	require.Regexp(t, ".*conflicting label constraints.*", err.Error())

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
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME FROM information_schema.Tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't'").Check(testkit.Rows(`def test t x`))
	tk.MustExec("create table t_range_p(id int) placement policy x partition by range(id) (" +
		"PARTITION p0 VALUES LESS THAN (100)," +
		"PARTITION p1 VALUES LESS THAN (1000) placement policy y," +
		"PARTITION p2 VALUES LESS THAN (10000))",
	)
	tk.MustExec("set tidb_enable_list_partition=1")
	tk.MustExec("create table t_list_p(name varchar(10)) placement policy x partition by list columns(name) (" +
		"PARTITION p0 VALUES IN ('a', 'b')," +
		"PARTITION p1 VALUES IN ('c', 'd') placement policy y," +
		"PARTITION p2 VALUES IN ('e', 'f'))",
	)
	tk.MustExec("create table t_hash_p(id int) placement policy x partition by HASH(id) PARTITIONS 4")

	policyX := testGetPolicyByName(t, tk.Session(), "x", true)
	require.Equal(t, "x", policyX.Name.L)
	require.Equal(t, true, policyX.ID != 0)

	policyY := testGetPolicyByName(t, tk.Session(), "y", true)
	require.Equal(t, "y", policyY.Name.L)
	require.Equal(t, true, policyY.ID != 0)

	tbl := external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.NotNil(t, tbl.Meta().PlacementPolicyRef)
	require.Equal(t, "x", tbl.Meta().PlacementPolicyRef.Name.L)
	require.Equal(t, policyX.ID, tbl.Meta().PlacementPolicyRef.ID)
	tk.MustExec("drop table if exists t")

	checkPartitionTableFunc := func(tblName string) {
		tbl = external.GetTableByName(t, tk, "test", tblName)
		require.NotNil(t, tbl)
		require.NotNil(t, tbl.Meta().PlacementPolicyRef)
		require.Equal(t, "x", tbl.Meta().PlacementPolicyRef.Name.L)
		require.Equal(t, policyX.ID, tbl.Meta().PlacementPolicyRef.ID)

		require.NotNil(t, tbl.Meta().Partition)
		require.Equal(t, 3, len(tbl.Meta().Partition.Definitions))

		p0 := tbl.Meta().Partition.Definitions[0]
		require.Nil(t, p0.PlacementPolicyRef)

		p1 := tbl.Meta().Partition.Definitions[1]
		require.NotNil(t, p1.PlacementPolicyRef)
		require.Equal(t, "y", p1.PlacementPolicyRef.Name.L)
		require.Equal(t, policyY.ID, p1.PlacementPolicyRef.ID)

		p2 := tbl.Meta().Partition.Definitions[2]
		require.Nil(t, p2.PlacementPolicyRef)
	}

	checkPartitionTableFunc("t_range_p")
	tk.MustExec("drop table if exists t_range_p")

	checkPartitionTableFunc("t_list_p")
	tk.MustExec("drop table if exists t_list_p")

	tbl = external.GetTableByName(t, tk, "test", "t_hash_p")
	require.NotNil(t, tbl)
	require.NotNil(t, tbl.Meta().PlacementPolicyRef)
	require.Equal(t, "x", tbl.Meta().PlacementPolicyRef.Name.L)
	require.Equal(t, policyX.ID, tbl.Meta().PlacementPolicyRef.ID)
	for _, p := range tbl.Meta().Partition.Definitions {
		require.Nil(t, p.PlacementPolicyRef)
	}
}

func getClonedTable(dom *domain.Domain, dbName string, tableName string) (*model.TableInfo, error) {
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		return nil, err
	}

	tblMeta := tbl.Meta()
	tblMeta = tblMeta.Clone()
	policyRef := *tblMeta.PlacementPolicyRef
	tblMeta.PlacementPolicyRef = &policyRef
	return tblMeta, nil
}

func getClonedDatabase(dom *domain.Domain, dbName string) (*model.DBInfo, bool) {
	db, ok := dom.InfoSchema().SchemaByName(model.NewCIStr(dbName))
	if !ok {
		return nil, ok
	}

	db = db.Clone()
	policyRef := *db.PlacementPolicyRef
	db.PlacementPolicyRef = &policyRef
	return db, true
}

func TestCreateTableWithInfoPlacement(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	// clearAllBundles(t)
	tk := testkit.NewTestKit(t, store)
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

	tbl, err := getClonedTable(dom, "test", "t1")
	require.NoError(t, err)
	policy, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	require.True(t, ok)
	require.Equal(t, policy.ID, tbl.PlacementPolicyRef.ID)

	tk.MustExec("alter table t1 placement policy='default'")
	tk.MustExec("drop placement policy p1")
	tk.MustExec("create placement policy p1 followers=2")
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	require.Nil(t, dom.DDL().CreateTableWithInfo(tk.Session(), model.NewCIStr("test2"), tbl, ddl.OnExistError))
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table test2.t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */"))
	tk.MustQuery("show placement where target='TABLE test2.t1'").Check(testkit.Rows("TABLE test2.t1 FOLLOWERS=2 PENDING"))

	// The ref id for new table should be the new policy id
	tbl2, err := getClonedTable(dom, "test2", "t1")
	require.NoError(t, err)
	policy2, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	require.True(t, ok)
	require.Equal(t, policy2.ID, tbl2.PlacementPolicyRef.ID)
	require.True(t, policy2.ID != policy.ID)

	// Test policy not exists
	tbl2.Name = model.NewCIStr("t3")
	tbl2.PlacementPolicyRef.Name = model.NewCIStr("pxx")
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	err = dom.DDL().CreateTableWithInfo(tk.Session(), model.NewCIStr("test2"), tbl2, ddl.OnExistError)
	require.Equal(t, "[schema:8239]Unknown placement policy 'pxx'", err.Error())
}

func TestCreateSchemaWithInfoPlacement(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	// clearAllBundles(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists test2")
	tk.MustExec("drop database if exists test3")
	tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p1 followers=1")
	defer tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("create database test2 placement policy p1")
	defer tk.MustExec("drop database if exists test2")
	defer tk.MustExec("drop database if exists test3")

	db, ok := getClonedDatabase(dom, "test2")
	require.True(t, ok)
	policy, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	require.True(t, ok)
	require.Equal(t, policy.ID, db.PlacementPolicyRef.ID)

	db2 := db.Clone()
	db2.Name = model.NewCIStr("test3")
	tk.MustExec("alter database test2 placement policy='default'")
	tk.MustExec("drop placement policy p1")
	tk.MustExec("create placement policy p1 followers=2")
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	require.Nil(t, dom.DDL().CreateSchemaWithInfo(tk.Session(), db2, ddl.OnExistError))
	tk.MustQuery("show create database test2").Check(testkit.Rows("test2 CREATE DATABASE `test2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	tk.MustQuery("show create database test3").Check(testkit.Rows("test3 CREATE DATABASE `test3` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] PLACEMENT POLICY=`p1` */"))
	tk.MustQuery("show placement where target='DATABASE test3'").Check(testkit.Rows("DATABASE test3 FOLLOWERS=2 SCHEDULED"))

	// The ref id for new table should be the new policy id
	db2, ok = getClonedDatabase(dom, "test3")
	require.True(t, ok)
	policy2, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	require.True(t, ok)
	require.Equal(t, policy2.ID, db2.PlacementPolicyRef.ID)
	require.True(t, policy2.ID != policy.ID)

	// Test policy not exists
	db2.Name = model.NewCIStr("test4")
	db2.PlacementPolicyRef.Name = model.NewCIStr("p2")
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	err := dom.DDL().CreateSchemaWithInfo(tk.Session(), db2, ddl.OnExistError)
	require.Equal(t, "[schema:8239]Unknown placement policy 'p2'", err.Error())
}

func TestDropPlacementPolicyInUse(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME FROM information_schema.Tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't12'").Check(testkit.Rows(`def test t12 p2`))

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

	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() {
		require.Nil(t, txn.Rollback())
	}()
	for _, policyName := range []string{"p1", "p2", "p3", "p4"} {
		err := tk.ExecToErr(fmt.Sprintf("drop placement policy %s", policyName))
		require.Equal(t, fmt.Sprintf("[ddl:8241]Placement policy '%s' is still in use", policyName), err.Error())

		err = tk.ExecToErr(fmt.Sprintf("drop placement policy if exists %s", policyName))
		require.Equal(t, fmt.Sprintf("[ddl:8241]Placement policy '%s' is still in use", policyName), err.Error())
	}
}

func testGetPolicyByName(t *testing.T, ctx sessionctx.Context, name string, mustExist bool) *model.PolicyInfo {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	require.NoError(t, err)
	po, ok := dom.InfoSchema().PolicyByName(model.NewCIStr(name))
	if mustExist {
		require.Equal(t, true, ok)
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

func TestPolicyCacheAndPolicyDependency(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists x")

	// Test policy cache.
	tk.MustExec("create placement policy x primary_region=\"r1\" regions=\"r1,r2\" schedule=\"EVEN\";")
	po := testGetPolicyByName(t, tk.Session(), "x", true)
	require.NotNil(t, po)
	tk.MustQuery("show placement where target='POLICY x'").Check(testkit.Rows("POLICY x PRIMARY_REGION=\"r1\" REGIONS=\"r1,r2\" SCHEDULE=\"EVEN\" NULL"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int) placement policy \"x\"")
	defer tk.MustExec("drop table if exists t")
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, TIDB_PLACEMENT_POLICY_NAME FROM information_schema.Tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't'").Check(testkit.Rows(`def test t BASE TABLE x`))
	tbl := external.GetTableByName(t, tk, "test", "t")

	// Test policy dependency cache.
	dependencies := testGetPolicyDependency(store, "x")
	require.NotNil(t, dependencies)
	require.Equal(t, 1, len(dependencies))
	require.Equal(t, tbl.Meta().ID, dependencies[0])

	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (a int) placement policy \"x\"")
	defer tk.MustExec("drop table if exists t2")
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, TIDB_PLACEMENT_POLICY_NAME FROM information_schema.Tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't'").Check(testkit.Rows(`def test t BASE TABLE x`))
	tbl2 := external.GetTableByName(t, tk, "test", "t2")

	dependencies = testGetPolicyDependency(store, "x")
	require.NotNil(t, dependencies)
	require.Equal(t, 2, len(dependencies))
	in := func() bool {
		for _, one := range dependencies {
			if one == tbl2.Meta().ID {
				return true
			}
		}
		return false
	}
	require.Equal(t, true, in())

	// Test drop policy can't succeed cause there are still some table depend on them.
	_, err := tk.Exec("drop placement policy x")
	require.Error(t, err)
	require.Equal(t, "[ddl:8241]Placement policy 'x' is still in use", err.Error())

	// Drop depended table t firstly.
	tk.MustExec("drop table if exists t")
	dependencies = testGetPolicyDependency(store, "x")
	require.NotNil(t, dependencies)
	require.Equal(t, 1, len(dependencies))
	require.Equal(t, tbl2.Meta().ID, dependencies[0])

	_, err = tk.Exec("drop placement policy x")
	require.Error(t, err)
	require.Equal(t, "[ddl:8241]Placement policy 'x' is still in use", err.Error())

	// Drop depended table t2 secondly.
	tk.MustExec("drop table if exists t2")
	dependencies = testGetPolicyDependency(store, "x")
	require.NotNil(t, dependencies)
	require.Equal(t, 0, len(dependencies))

	po = testGetPolicyByName(t, tk.Session(), "x", true)
	require.NotNil(t, po)

	tk.MustExec("drop placement policy x")

	po = testGetPolicyByName(t, tk.Session(), "x", false)
	require.Nil(t, po)
	dependencies = testGetPolicyDependency(store, "x")
	require.NotNil(t, dependencies)
	require.Equal(t, 0, len(dependencies))
}

func TestAlterTablePartitionWithPlacementPolicy(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	// clearAllBundles(t)
	tk := testkit.NewTestKit(t, store)
	defer func() {
		tk.MustExec("drop table if exists t1")
		tk.MustExec("drop placement policy if exists x")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop placement policy if exists x")

	// Direct placement option: special constraints may be incompatible with common constraint.
	tk.MustExec("create table t1 (c int) PARTITION BY RANGE (c) " +
		"(PARTITION p0 VALUES LESS THAN (6)," +
		"PARTITION p1 VALUES LESS THAN (11)," +
		"PARTITION p2 VALUES LESS THAN (16)," +
		"PARTITION p3 VALUES LESS THAN (21));")
	defer tk.MustExec("drop table if exists t1")
	checkExistTableBundlesInPD(t, dom, "test", "t1")

	// Only placement policy should check the policy existence.
	tk.MustGetErrCode("alter table t1 partition p0 "+
		"PLACEMENT POLICY=\"x\"", mysql.ErrPlacementPolicyNotExists)
	tk.MustExec("create placement policy x " +
		"FOLLOWERS=2 ")
	tk.MustExec("alter table t1 partition p0 " +
		"PLACEMENT POLICY=\"x\"")
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, TIDB_PLACEMENT_POLICY_NAME FROM information_schema.Partitions WHERE TABLE_SCHEMA='test' AND TABLE_NAME = 't1' AND PARTITION_NAME = 'p0'").Check(testkit.Rows(`def test t1 p0 x`))
	checkExistTableBundlesInPD(t, dom, "test", "t1")

	policyX, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("x"))
	require.True(t, ok)
	ptDef := testGetPartitionDefinitionsByName(t, tk.Session(), "test", "t1", "p0")
	require.NotNil(t, ptDef)
	require.NotNil(t, ptDef.PlacementPolicyRef)
	require.Equal(t, "x", ptDef.PlacementPolicyRef.Name.L)
	require.Equal(t, policyX.ID, ptDef.PlacementPolicyRef.ID)
}

func testGetPartitionDefinitionsByName(t *testing.T, ctx sessionctx.Context, db string, table string, ptName string) model.PartitionDefinition {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	require.NoError(t, err)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	require.NoError(t, err)
	require.NotNil(t, tbl)
	var ptDef model.PartitionDefinition
	for _, def := range tbl.Meta().Partition.Definitions {
		if ptName == def.Name.L {
			ptDef = def
			break
		}
	}
	return ptDef
}

func TestPolicyInheritance(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	// clearAllBundles(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop database if exists mydb")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	defer func() {
		tk.MustExec("drop database if exists mydb")
		tk.MustExec("drop placement policy if exists p1")
		tk.MustExec("drop placement policy if exists p2")
	}()

	// test table inherit database's placement rules.
	tk.MustExec("create placement policy p1 constraints=\"[+zone=hangzhou]\"")
	tk.MustExec("create database mydb placement policy p1")
	tk.MustQuery("show create database mydb").Check(testkit.Rows("mydb CREATE DATABASE `mydb` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] PLACEMENT POLICY=`p1` */"))

	tk.MustExec("use mydb")
	tk.MustExec("create table t(a int)")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */"))
	checkExistTableBundlesInPD(t, dom, "mydb", "t")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create placement policy p2 constraints=\"[+zone=suzhou]\"")
	tk.MustExec("create table t(a int) placement policy p2")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p2` */"))
	checkExistTableBundlesInPD(t, dom, "mydb", "t")
	tk.MustExec("drop table if exists t")

	// test create table like should not inherit database's placement rules.
	tk.MustExec("create table t0 (a int) placement policy 'default'")
	tk.MustQuery("show create table t0").Check(testkit.Rows("t0 CREATE TABLE `t0` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	checkExistTableBundlesInPD(t, dom, "mydb", "t0")
	tk.MustExec("create table t1 like t0")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	checkExistTableBundlesInPD(t, dom, "mydb", "t1")
	tk.MustExec("drop table if exists t0, t")

	// table will inherit db's placement rules, which is shared by all partition as default one.
	tk.MustExec("create table t(a int) partition by range(a) (partition p0 values less than (100), partition p1 values less than (200))")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (200))"))
	checkExistTableBundlesInPD(t, dom, "mydb", "t")
	tk.MustExec("drop table if exists t")

	// partition's specified placement rules will override the default one.
	tk.MustExec("create table t(a int) partition by range(a) (partition p0 values less than (100) placement policy p2, partition p1 values less than (200))")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (200))"))
	checkExistTableBundlesInPD(t, dom, "mydb", "t")
	tk.MustExec("drop table if exists t")

	// test partition override table's placement rules.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int) placement policy p2 partition by range(a) (partition p0 values less than (100) placement policy p1, partition p1 values less than (200))")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p2` */\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (200))"))
	checkExistTableBundlesInPD(t, dom, "mydb", "t")
}

func TestDatabasePlacement(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists db2")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	tk.MustExec("create placement policy p2 primary_region='r2' regions='r1,r2'")
	defer tk.MustExec("drop placement policy p2")

	policy1, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	require.True(t, ok)

	tk.MustExec(`create database db2`)
	defer tk.MustExec("drop database db2")
	tk.MustQuery("show create database db2").Check(testkit.Rows(
		"db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))

	policy2, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p2"))
	require.True(t, ok)

	// alter with policy
	tk.MustExec("alter database db2 placement policy p1")
	tk.MustQuery("show create database db2").Check(testkit.Rows(
		"db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] PLACEMENT POLICY=`p1` */",
	))

	db, ok := dom.InfoSchema().SchemaByName(model.NewCIStr("db2"))
	require.True(t, ok)
	require.Equal(t, policy1.ID, db.PlacementPolicyRef.ID)

	tk.MustExec("alter database db2 placement policy p2")
	tk.MustQuery("show create database db2").Check(testkit.Rows(
		"db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] PLACEMENT POLICY=`p2` */",
	))

	db, ok = dom.InfoSchema().SchemaByName(model.NewCIStr("db2"))
	require.True(t, ok)
	require.Equal(t, policy2.ID, db.PlacementPolicyRef.ID)

	// reset with placement policy 'default'
	tk.MustExec("alter database db2 placement policy default")
	tk.MustQuery("show create database db2").Check(testkit.Rows(
		"db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))

	db, ok = dom.InfoSchema().SchemaByName(model.NewCIStr("db2"))
	require.True(t, ok)
	require.Nil(t, db.PlacementPolicyRef)

	// error invalid policy
	err := tk.ExecToErr("alter database db2 placement policy px")
	require.Equal(t, "[schema:8239]Unknown placement policy 'px'", err.Error())

	// failed alter has no effect
	tk.MustQuery("show create database db2").Check(testkit.Rows(
		"db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))
}

func TestDropDatabaseGCPlacement(t *testing.T) {
	// clearAllBundles(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`))
	defer func(originGC bool) {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed"))
		if originGC {
			util.EmulatorGCEnable()
		} else {
			util.EmulatorGCDisable()
		}
	}(util.IsEmulatorGCEnable())
	util.EmulatorGCDisable()
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists db2")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("use test")

	tk.MustExec("create placement policy p1 primary_region='r0' regions='r0'")
	defer tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p2 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create table t (id int) placement policy p1")
	defer tk.MustExec("drop table if exists t")

	tk.MustExec("create database db2")
	defer tk.MustExec("drop database if exists db2")

	tk.MustExec("create table db2.t0 (id int)")
	tk.MustExec("create table db2.t1 (id int) placement policy p1")
	tk.MustExec(`create table db2.t2 (id int) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) placement policy p2,
        PARTITION p1 VALUES LESS THAN (1000)
	)`)

	is := dom.InfoSchema()
	tt, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	tk.MustExec("drop database db2")

	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 4, len(bundles))

	gcWorker, err := gcworker.NewMockGCWorker(store)
	require.NoError(t, err)
	require.Nil(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))

	bundles, err = infosync.GetAllRuleBundles(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 1, len(bundles))
	require.Equal(t, placement.GroupID(tt.Meta().ID), bundles[0].ID)
}

func TestDropTableGCPlacement(t *testing.T) {
	// clearAllBundles(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`))
	defer func(originGC bool) {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed"))
		if originGC {
			util.EmulatorGCEnable()
		} else {
			util.EmulatorGCDisable()
		}
	}(util.IsEmulatorGCEnable())
	util.EmulatorGCDisable()
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0,t1")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create placement policy p1 primary_region='r0' regions='r0'")
	defer tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p2 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create table t0 (id int)")
	defer tk.MustExec("drop table if exists t0")

	tk.MustExec("create table t1 (id int) placement policy p1")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec(`create table t2 (id int) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) placement policy p2,
        PARTITION p1 VALUES LESS THAN (1000)
	)`)
	defer tk.MustExec("drop table if exists t2")

	is := dom.InfoSchema()
	t1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)

	tk.MustExec("drop table t2")

	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 3, len(bundles))

	gcWorker, err := gcworker.NewMockGCWorker(store)
	require.NoError(t, err)
	require.Nil(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))

	bundles, err = infosync.GetAllRuleBundles(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 1, len(bundles))
	require.Equal(t, placement.GroupID(t1.Meta().ID), bundles[0].ID)

	bundles = dom.InfoSchema().AllPlacementBundles()
	require.NoError(t, err)
	require.Equal(t, 1, len(bundles))
	require.Equal(t, placement.GroupID(t1.Meta().ID), bundles[0].ID)
}

func TestAlterTablePlacement(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	// clearAllBundles(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tp")
	tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	policy, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	require.True(t, ok)

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
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// alter with policy
	tk.MustExec("alter table tp placement policy p1")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))

	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	require.NoError(t, err)
	require.Equal(t, policy.ID, tb.Meta().PlacementPolicyRef.ID)
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// reset with placement policy 'default'
	tk.MustExec("alter table tp placement policy default")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// error invalid policy
	err = tk.ExecToErr("alter table tp placement policy px")
	require.Equal(t, "[schema:8239]Unknown placement policy 'px'", err.Error())

	// failed alter has no effect
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(t, dom, "test", "tp")
}

func TestDropTablePartitionGCPlacement(t *testing.T) {
	// clearAllBundles(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`))
	defer func(originGC bool) {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed"))
		if originGC {
			util.EmulatorGCEnable()
		} else {
			util.EmulatorGCDisable()
		}
	}(util.IsEmulatorGCEnable())
	util.EmulatorGCDisable()
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0,t1")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("drop placement policy if exists p3")

	tk.MustExec("create placement policy p1 primary_region='r0' regions='r0'")
	defer tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p2 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create placement policy p3 primary_region='r2' regions='r2'")
	defer tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create table t0 (id int)")
	defer tk.MustExec("drop table if exists t0")

	tk.MustExec("create table t1 (id int) placement policy p1")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec(`create table t2 (id int) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) placement policy p2,
        PARTITION p1 VALUES LESS THAN (1000) placement policy p3
	)`)
	defer tk.MustExec("drop table if exists t2")

	is := dom.InfoSchema()
	t1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	t2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)

	tk.MustExec("alter table t2 drop partition p0")

	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 4, len(bundles))

	gcWorker, err := gcworker.NewMockGCWorker(store)
	require.NoError(t, err)
	require.Nil(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))

	bundles, err = infosync.GetAllRuleBundles(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 3, len(bundles))
	bundlesMap := make(map[string]*placement.Bundle)
	for _, bundle := range bundles {
		bundlesMap[bundle.ID] = bundle
	}
	_, ok := bundlesMap[placement.GroupID(t1.Meta().ID)]
	require.True(t, ok)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().ID)]
	require.True(t, ok)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().Partition.Definitions[1].ID)]
	require.True(t, ok)

	bundles = dom.InfoSchema().AllPlacementBundles()
	require.NoError(t, err)
	require.Equal(t, 3, len(bundles))
	bundlesMap = make(map[string]*placement.Bundle)
	for _, bundle := range bundles {
		bundlesMap[bundle.ID] = bundle
	}
	_, ok = bundlesMap[placement.GroupID(t1.Meta().ID)]
	require.True(t, ok)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().ID)]
	require.True(t, ok)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().Partition.Definitions[1].ID)]
	require.True(t, ok)
}

func TestAlterTablePartitionPlacement(t *testing.T) {
	// clearAllBundles(t)
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tp")
	tk.MustExec("drop placement policy if exists p0")
	tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p0 primary_region='r0' regions='r0'")
	defer tk.MustExec("drop placement policy p0")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	policy, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	require.True(t, ok)

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
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// alter with policy
	tk.MustExec("alter table tp partition p0 placement policy p1")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p0` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))

	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	require.NoError(t, err)
	require.Equal(t, policy.ID, tb.Meta().Partition.Definitions[0].PlacementPolicyRef.ID)
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// reset with placement policy 'default'
	tk.MustExec("alter table tp partition p1 placement policy default")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p0` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	tk.MustExec("alter table tp partition p0 placement policy default")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p0` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// error invalid policy
	err = tk.ExecToErr("alter table tp partition p1 placement policy px")
	require.Equal(t, "[schema:8239]Unknown placement policy 'px'", err.Error())

	// error invalid partition name
	err = tk.ExecToErr("alter table tp partition p2 placement policy p1")
	require.Equal(t, "[table:1735]Unknown partition 'p2' in table 'tp'", err.Error())

	// failed alter has no effect
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p0` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(t, dom, "test", "tp")
}

func TestAddPartitionWithPlacement(t *testing.T) {
	// clearAllBundles(t)
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tp")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	tk.MustExec("create placement policy p2 primary_region='r2' regions='r2'")
	defer tk.MustExec("drop placement policy p2")

	policy2, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p2"))
	require.True(t, ok)

	tk.MustExec(`CREATE TABLE tp (id INT) PLACEMENT POLICY p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000)
	);`)
	defer tk.MustExec("drop table tp")
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// Add partitions
	tk.MustExec(`alter table tp add partition (
		partition p2 values less than (10000) placement policy p2,
		partition p3 values less than (100000),
		partition p4 values less than (1000000) placement policy default
	)`)
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p3` VALUES LESS THAN (100000),\n" +
		" PARTITION `p4` VALUES LESS THAN (1000000))"))
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	require.NoError(t, err)
	require.Equal(t, policy2.ID, tb.Meta().Partition.Definitions[2].PlacementPolicyRef.ID)

	// error invalid policy
	err = tk.ExecToErr("alter table tp add partition (partition p5 values less than (10000000) placement policy px)")
	require.Equal(t, "[schema:8239]Unknown placement policy 'px'", err.Error())

	// failed alter has no effect
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p3` VALUES LESS THAN (100000),\n" +
		" PARTITION `p4` VALUES LESS THAN (1000000))"))
	checkExistTableBundlesInPD(t, dom, "test", "tp")
}

func TestTruncateTableWithPlacement(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, tp")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	tk.MustExec("create placement policy p2 primary_region='r2' regions='r2'")
	defer tk.MustExec("drop placement policy p2")

	policy1, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	require.True(t, ok)

	policy2, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p2"))
	require.True(t, ok)

	tk.MustExec(`CREATE TABLE t1 (id INT) placement policy p1`)
	defer tk.MustExec("drop table t1")

	// test for normal table
	tk.MustQuery("show create table t1").Check(testkit.Rows("" +
		"t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */"))

	t1, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tk.MustExec("TRUNCATE TABLE t1")
	tk.MustQuery("show create table t1").Check(testkit.Rows("" +
		"t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */"))
	newT1, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.True(t, newT1.Meta().ID != t1.Meta().ID)

	// test for partitioned table
	tk.MustExec(`CREATE TABLE tp (id INT) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000) placement policy p2,
        PARTITION p2 VALUES LESS THAN (10000)
	);`)
	defer tk.MustExec("drop table tp")

	tp, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	require.NoError(t, err)
	require.Equal(t, policy1.ID, tp.Meta().PlacementPolicyRef.ID)
	require.Equal(t, policy2.ID, tp.Meta().Partition.Definitions[1].PlacementPolicyRef.ID)
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p2` VALUES LESS THAN (10000))"))

	tk.MustExec("TRUNCATE TABLE tp")
	newTp, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	require.NoError(t, err)
	require.True(t, newTp.Meta().ID != tp.Meta().ID)
	require.Equal(t, policy1.ID, newTp.Meta().PlacementPolicyRef.ID)
	require.Equal(t, policy2.ID, newTp.Meta().Partition.Definitions[1].PlacementPolicyRef.ID)
	for i := range []int{0, 1, 2} {
		require.True(t, newTp.Meta().Partition.Definitions[i].ID != tp.Meta().Partition.Definitions[i].ID)
	}
}

func TestTruncateTableGCWithPlacement(t *testing.T) {
	// clearAllBundles(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`))
	defer func(originGC bool) {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed"))
		if originGC {
			util.EmulatorGCEnable()
		} else {
			util.EmulatorGCDisable()
		}
	}(util.IsEmulatorGCEnable())
	util.EmulatorGCDisable()
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0,t1")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create placement policy p1 primary_region='r0' regions='r0'")
	defer tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p2 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create table t0 (id int)")
	defer tk.MustExec("drop table if exists t0")

	tk.MustExec("create table t1 (id int) placement policy p1")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec(`create table t2 (id int) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) placement policy p2,
        PARTITION p1 VALUES LESS THAN (1000)
	)`)
	defer tk.MustExec("drop table if exists t2")

	tk.MustExec("truncate table t2")

	is := dom.InfoSchema()
	t1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	t2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)

	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 5, len(bundles))

	gcWorker, err := gcworker.NewMockGCWorker(store)
	require.NoError(t, err)
	require.Nil(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))

	bundles, err = infosync.GetAllRuleBundles(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 3, len(bundles))
	bundlesMap := make(map[string]*placement.Bundle)
	for _, bundle := range bundles {
		bundlesMap[bundle.ID] = bundle
	}
	_, ok := bundlesMap[placement.GroupID(t1.Meta().ID)]
	require.True(t, ok)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().ID)]
	require.True(t, ok)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().Partition.Definitions[0].ID)]
	require.True(t, ok)
}

func TestTruncateTablePartitionWithPlacement(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists tp")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("drop placement policy if exists p3")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	tk.MustExec("create placement policy p2 primary_region='r2' regions='r2'")
	defer tk.MustExec("drop placement policy p2")

	tk.MustExec("create placement policy p3 primary_region='r3' regions='r3'")
	defer tk.MustExec("drop placement policy p3")

	policy1, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	require.True(t, ok)

	policy2, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p2"))
	require.True(t, ok)

	policy3, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p3"))
	require.True(t, ok)

	// test for partitioned table
	tk.MustExec(`CREATE TABLE tp (id INT) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000) placement policy p2,
        PARTITION p2 VALUES LESS THAN (10000) placement policy p3,
        PARTITION p3 VALUES LESS THAN (100000)
	);`)
	defer tk.MustExec("drop table tp")

	tp, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	require.NoError(t, err)

	tk.MustExec("ALTER TABLE tp TRUNCATE partition p1,p3")
	newTp, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	require.NoError(t, err)
	require.Equal(t, tp.Meta().ID, newTp.Meta().ID)
	require.Equal(t, policy1.ID, newTp.Meta().PlacementPolicyRef.ID)
	require.Equal(t, policy2.ID, newTp.Meta().Partition.Definitions[1].PlacementPolicyRef.ID)
	require.Equal(t, policy3.ID, newTp.Meta().Partition.Definitions[2].PlacementPolicyRef.ID)
	require.Equal(t, tp.Meta().Partition.Definitions[0].ID, newTp.Meta().Partition.Definitions[0].ID)
	require.True(t, newTp.Meta().Partition.Definitions[1].ID != tp.Meta().Partition.Definitions[1].ID)
	require.Equal(t, tp.Meta().Partition.Definitions[2].ID, newTp.Meta().Partition.Definitions[2].ID)
	require.True(t, newTp.Meta().Partition.Definitions[3].ID != tp.Meta().Partition.Definitions[3].ID)

	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p2` VALUES LESS THAN (10000) /*T![placement] PLACEMENT POLICY=`p3` */,\n" +
		" PARTITION `p3` VALUES LESS THAN (100000))"))
}

func TestTruncatePartitionGCWithPlacement(t *testing.T) {
	// clearAllBundles(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`))
	defer func(originGC bool) {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed"))
		if originGC {
			util.EmulatorGCEnable()
		} else {
			util.EmulatorGCDisable()
		}
	}(util.IsEmulatorGCEnable())
	util.EmulatorGCDisable()
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create placement policy p1 primary_region='r0' regions='r0'")
	defer tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p2 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create table t0 (id int)")
	defer tk.MustExec("drop table if exists t0")

	tk.MustExec("create table t1 (id int) placement policy p1")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec(`create table t2 (id int) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) placement policy p2,
        PARTITION p1 VALUES LESS THAN (1000)
	)`)
	defer tk.MustExec("drop table if exists t2")

	tk.MustExec("alter table t2 truncate partition p0")

	is := dom.InfoSchema()
	t1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	t2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)

	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 4, len(bundles))

	gcWorker, err := gcworker.NewMockGCWorker(store)
	require.NoError(t, err)
	require.Nil(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))

	bundles, err = infosync.GetAllRuleBundles(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 3, len(bundles))
	bundlesMap := make(map[string]*placement.Bundle)
	for _, bundle := range bundles {
		bundlesMap[bundle.ID] = bundle
	}
	_, ok := bundlesMap[placement.GroupID(t1.Meta().ID)]
	require.True(t, ok)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().ID)]
	require.True(t, ok)

	_, ok = bundlesMap[placement.GroupID(t2.Meta().Partition.Definitions[0].ID)]
	require.True(t, ok)
}

func TestExchangePartitionWithPlacement(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	// clearAllBundles(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_enable_exchange_partition=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, tp")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("drop placement policy if exists p3")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy p1")

	tk.MustExec("create placement policy p2 primary_region='r2' regions='r2'")
	defer tk.MustExec("drop placement policy p2")

	tk.MustExec("create placement policy p3 primary_region='r3' regions='r3'")
	defer tk.MustExec("drop placement policy p3")

	policy1, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p1"))
	require.True(t, ok)

	policy2, ok := dom.InfoSchema().PolicyByName(model.NewCIStr("p2"))
	require.True(t, ok)

	tk.MustExec(`CREATE TABLE t1 (id INT) placement policy p1`)
	defer tk.MustExec("drop table t1")

	tk.MustExec(`CREATE TABLE t2 (id INT)`)
	defer tk.MustExec("drop table t2")

	t1, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	t1ID := t1.Meta().ID

	t2, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	t2ID := t2.Meta().ID

	tk.MustExec(`CREATE TABLE tp (id INT) placement policy p3 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000) placement policy p2,
        PARTITION p2 VALUES LESS THAN (10000)
	);`)
	defer tk.MustExec("drop table tp")

	tp, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	require.NoError(t, err)
	tpID := tp.Meta().ID
	par0ID := tp.Meta().Partition.Definitions[0].ID
	par1ID := tp.Meta().Partition.Definitions[1].ID

	// exchange par0, t1
	tk.MustExec("alter table tp exchange partition p0 with table t1")
	tk.MustQuery("show create table t1").Check(testkit.Rows("" +
		"t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */"))
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p3` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p2` VALUES LESS THAN (10000))"))
	tp, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	require.NoError(t, err)
	require.Equal(t, tpID, tp.Meta().ID)
	require.Equal(t, t1ID, tp.Meta().Partition.Definitions[0].ID)
	require.Nil(t, tp.Meta().Partition.Definitions[0].PlacementPolicyRef)
	t1, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.Equal(t, par0ID, t1.Meta().ID)
	require.Equal(t, policy1.ID, t1.Meta().PlacementPolicyRef.ID)
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// exchange par0, t2
	tk.MustExec("alter table tp exchange partition p0 with table t2")
	tk.MustQuery("show create table t2").Check(testkit.Rows("" +
		"t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p3` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p2` VALUES LESS THAN (10000))"))
	tp, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	require.NoError(t, err)
	require.Equal(t, tpID, tp.Meta().ID)
	require.Equal(t, t2ID, tp.Meta().Partition.Definitions[0].ID)
	require.Nil(t, tp.Meta().Partition.Definitions[0].PlacementPolicyRef)
	t2, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	require.Equal(t, t1ID, t2.Meta().ID)
	require.Nil(t, t2.Meta().PlacementPolicyRef)
	checkExistTableBundlesInPD(t, dom, "test", "tp")

	// exchange par1, t1
	tk.MustExec("alter table tp exchange partition p1 with table t1")
	tk.MustQuery("show create table t1").Check(testkit.Rows("" +
		"t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */"))
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p3` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p2` */,\n" +
		" PARTITION `p2` VALUES LESS THAN (10000))"))
	tp, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	require.NoError(t, err)
	require.Equal(t, tpID, tp.Meta().ID)
	require.Equal(t, par0ID, tp.Meta().Partition.Definitions[1].ID)
	require.Equal(t, policy2.ID, tp.Meta().Partition.Definitions[1].PlacementPolicyRef.ID)
	t1, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.Equal(t, par1ID, t1.Meta().ID)
	require.Equal(t, policy1.ID, t1.Meta().PlacementPolicyRef.ID)
	checkExistTableBundlesInPD(t, dom, "test", "tp")
}

func TestPDFail(t *testing.T) {
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/infosync/putRuleBundlesError"))
	}()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// clearAllBundles(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, tp")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create placement policy p1 primary_region=\"cn-east-1\" regions=\"cn-east-1,cn-east\"")
	defer tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy p2 followers=1")
	defer tk.MustExec("drop placement policy if exists p2")

	tk.MustExec("create table t1(id int)")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec(`CREATE TABLE tp (id INT) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000) placement policy p1
	);`)
	defer tk.MustExec("drop table if exists tp")
	existBundles, err := infosync.GetAllRuleBundles(context.TODO())
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/putRuleBundlesError", "return(true)"))

	// alter policy
	err = tk.ExecToErr("alter placement policy p1 primary_region='rx' regions='rx'")
	require.True(t, infosync.ErrHTTPServiceError.Equal(err))
	tk.MustQuery("show create placement policy p1").Check(testkit.Rows("p1 CREATE PLACEMENT POLICY `p1` PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east\""))
	checkAllBundlesNotChange(t, existBundles)

	// create table
	err = tk.ExecToErr("create table t2 (id int) placement policy p1")
	require.True(t, infosync.ErrHTTPServiceError.Equal(err))
	err = tk.ExecToErr("show create table t2")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
	checkAllBundlesNotChange(t, existBundles)

	// alter table
	err = tk.ExecToErr("alter table t1 placement policy p1")
	require.True(t, infosync.ErrHTTPServiceError.Equal(err))
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	checkAllBundlesNotChange(t, existBundles)

	// add partition
	err = tk.ExecToErr("alter table tp add partition (" +
		"partition p2 values less than (10000) placement policy p1," +
		"partition p3 values less than (100000)" +
		")")
	require.True(t, infosync.ErrHTTPServiceError.Equal(err))
	tk.MustQuery("show create table tp").Check(testkit.Rows("tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p1` */)"))
	checkAllBundlesNotChange(t, existBundles)

	// alter partition
	err = tk.ExecToErr(`alter table tp PARTITION p1 placement policy p2`)
	require.True(t, infosync.ErrHTTPServiceError.Equal(err))
	tk.MustQuery("show create table tp").Check(testkit.Rows("tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p1` */)"))
	checkAllBundlesNotChange(t, existBundles)

	// exchange partition
	tk.MustExec("alter table tp exchange partition p1 with table t1")
	require.True(t, infosync.ErrHTTPServiceError.Equal(err))
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table tp").Check(testkit.Rows("tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000) /*T![placement] PLACEMENT POLICY=`p1` */)"))
	checkAllBundlesNotChange(t, existBundles)
}

func TestRecoverTableWithPlacementPolicy(t *testing.T) {
	// clearAllBundles(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed", `return`))
	defer func(originGC bool) {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/gcworker/ignoreDeleteRangeFailed"))
		if originGC {
			util.EmulatorGCEnable()
		} else {
			util.EmulatorGCDisable()
		}
	}(util.IsEmulatorGCEnable())
	util.EmulatorGCDisable()
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	checkExistTableBundlesInPD(t, dom, "test", "tp1")

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
	checkExistTableBundlesInPD(t, dom, "test", "tp2")

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
	checkExistTableBundlesInPD(t, dom, "test", "tp3")
}
