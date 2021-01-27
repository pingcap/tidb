// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"fmt"
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testDBSuite1) TestAlterTableAlterPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")

	tk.Se.GetSessionVars().EnableAlterPlacement = true
	defer func() {
		tk.MustExec("drop table if exists t1")
		tk.Se.GetSessionVars().EnableAlterPlacement = false
	}()

	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11),
	PARTITION p2 VALUES LESS THAN (16),
	PARTITION p3 VALUES LESS THAN (21)
);`)

	is := s.dom.InfoSchema()

	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	partDefs := tb.Meta().GetPartitionInfo().Definitions
	p0ID := placement.GroupID(partDefs[0].ID)
	is.SetBundle(&placement.Bundle{
		ID:    p0ID,
		Rules: []*placement.Rule{{Role: placement.Leader, Count: 1}},
	})

	// normal cases
	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+zone=sh"]'
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+   zone   =   sh  ",     "- zone = bj    "]'
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{"+   zone   =   sh  ": 1}'
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints="{'+zone=sh': 1}"
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints="['+zone=sh']"
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{"+   zone   =   sh, -zone =   bj ": 1}'
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{"+   zone   =   sh  ": 1, "- zone = bj": 2}'
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
alter placement policy
	constraints='{"+   zone   =   sh, -zone =   bj ": 1}'
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
drop placement policy
	role=leader`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
drop placement policy
	role=follower`)
	c.Assert(err, NotNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	role=xxx
	constraints='{"+   zone   =   sh, -zone =   bj ": 1}'
	replicas=3`)
	c.Assert(err, NotNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{"+   zone   =   sh, -zone =   bj ": 1}'
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*ROLE is not specified.*")

	// multiple statements
	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+   zone   =   sh  "]'
	role=follower
	replicas=3,
add placement policy
	constraints='{"+   zone   =   sh, -zone =   bj ": 1}'
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+   zone   =   sh  "]'
	role=follower
	replicas=3,
add placement policy
	constraints='{"+zone=sh,-zone=bj":1,"+zone=sh,-zone=nj":1}'
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{"+   zone   =   sh  ": 1, "- zone = bj,+zone=sh": 2}'
	role=follower
	replicas=3,
alter placement policy
	constraints='{"+   zone   =   sh, -zone =   bj ": 1}'
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+zone=sh", "-zone=bj"]'
	role=follower
	replicas=3,
add placement policy
	constraints='{"+zone=sh": 1}'
	role=follower
	replicas=3,
add placement policy
	constraints='{"+zone=sh,-zone=bj":1,"+zone=sh,-zone=nj":1}'
	role=follower
	replicas=3,
alter placement policy
	constraints='{"+zone=sh": 1, "-zon =bj,+zone=sh": 1}'
	role=follower
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
drop placement policy
	role=leader,
drop placement policy
	role=leader`)
	c.Assert(err, NotNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{"+zone=sh,-zone=bj":1,"+zone=sh,-zone=nj":1}'
	role=voter
	replicas=3,
drop placement policy
	role=leader`)
	c.Assert(err, IsNil)

	// list/dict detection
	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	role=follower
	constraints='[]'`)
	c.Assert(err, ErrorMatches, ".*array CONSTRAINTS should be with a positive REPLICAS.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints=',,,'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, "(?s).*constraint is neither an array of string, nor a string-to-number map.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	role=voter
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='[,,,'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, "(?s).*constraint is neither an array of string, nor a string-to-number map.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{,,,'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, "(?s).*constraint is neither an array of string, nor a string-to-number map.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{"+   zone   =   sh  ": 1, "- zone = bj": 2}'
	role=follower
	replicas=2`)
	c.Assert(err, ErrorMatches, ".*should be larger or equal to the number of total replicas.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{"+   zone   =   sh  ": 1, "- zone = bj": 2}'
	role=leader`)
	c.Assert(err, ErrorMatches, ".*should be larger or equal to the number of total replicas.*")

	// checkPlacementSpecConstraint
	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='[",,,"]'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+    "]'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	// unknown operation
	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["0000"]'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	// without =
	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+000"]'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	// empty key
	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+ =zone1"]'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+  =   z"]'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	// empty value
	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+zone="]'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+z  =   "]'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	_, err = tk.Exec(`alter table t1 alter partition p
add placement policy
	constraints='["+zone=sh"]'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*Unknown partition.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{"+   zone   =   sh, -zone =   bj ": -1}'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*count should be positive.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+   zone   =   sh"]'
	role=leader
	replicas=0`)
	c.Assert(err, ErrorMatches, ".*Invalid placement option REPLICAS, it is not allowed to be 0.*")

	// ban tiflash
	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='["+zone=sh", "+engine=tiflash"]'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*unsupported label.*")

	// invalid partition
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c int)")
	_, err = tk.Exec(`alter table t1 alter partition p
add placement policy
	constraints='["+zone=sh"]'
	role=follower
	replicas=3`)
	c.Assert(ddl.ErrPartitionMgmtOnNonpartitioned.Equal(err), IsTrue)

	// issue 20751
	tk.MustExec("drop table if exists t_part_pk_id")
	tk.MustExec("create TABLE t_part_pk_id (c1 int,c2 int) partition by range(c1 div c2 ) (partition p0 values less than (2))")
	_, err = tk.Exec(`alter table t_part_pk_id alter partition p0 add placement policy constraints='["+host=store1"]' role=leader;`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t_part_pk_id alter partition p0 add placement policy constraints='["+host=store1"]' role=leader replicas=3;`)
	c.Assert(err, ErrorMatches, ".*replicas can only be 1 when the role is leader")
	tk.MustExec("drop table t_part_pk_id")
}

func (s *testDBSuite1) TestPlacementPolicyCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.Se.GetSessionVars().EnableAlterPlacement = true
	defer func() {
		tk.MustExec("drop table if exists t1")
		tk.Se.GetSessionVars().EnableAlterPlacement = false
	}()

	initTable := func() []string {
		tk.MustExec("drop table if exists t1")
		tk.MustExec(`create table t1(id int) partition by range(id)
(partition p0 values less than (100), partition p1 values less than (200))`)

		is := s.dom.InfoSchema()

		tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
		c.Assert(err, IsNil)
		partDefs := tb.Meta().GetPartitionInfo().Definitions

		sort.Slice(partDefs, func(i, j int) bool { return partDefs[i].Name.L < partDefs[j].Name.L })

		rows := []string{}
		for k, v := range partDefs {
			ptID := placement.GroupID(v.ID)
			is.SetBundle(&placement.Bundle{
				ID:    ptID,
				Rules: []*placement.Rule{{Count: k}},
			})
			rows = append(rows, fmt.Sprintf("%s 0  test t1 %s <nil>  %d ", ptID, v.Name.L, k))
		}
		return rows
	}

	// test drop
	rows := initTable()
	tk.MustQuery("select * from information_schema.placement_policy order by REPLICAS").Check(testkit.Rows(rows...))
	tk.MustExec("alter table t1 drop partition p0")
	tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows(rows[1:]...))

	rows = initTable()
	tk.MustQuery("select * from information_schema.placement_policy order by REPLICAS").Check(testkit.Rows(rows...))
	tk.MustExec("drop table t1")
	tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows())

	// test truncate
	rows = initTable()
	tk.MustQuery("select * from information_schema.placement_policy order by REPLICAS").Check(testkit.Rows(rows...))
	tk.MustExec("alter table t1 truncate partition p0")
	tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows(rows[1:]...))

	rows = initTable()
	tk.MustQuery("select * from information_schema.placement_policy order by REPLICAS").Check(testkit.Rows(rows...))
	tk.MustExec("truncate table t1")
	tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows())
}

func (s *testSerialDBSuite) TestTxnScopeConstraint(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.Se.GetSessionVars().EnableAlterPlacement = true
	defer func() {
		tk.MustExec("drop table if exists t1")
		tk.Se.GetSessionVars().EnableAlterPlacement = false
	}()

	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11),
	PARTITION p2 VALUES LESS THAN (16),
	PARTITION p3 VALUES LESS THAN (21)
);`)

	is := s.dom.InfoSchema()

	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	partDefs := tb.Meta().GetPartitionInfo().Definitions

	for _, def := range partDefs {
		if def.Name.String() == "p0" {
			groupID := placement.GroupID(def.ID)
			is.SetBundle(&placement.Bundle{
				ID: groupID,
				Rules: []*placement.Rule{
					{
						GroupID: groupID,
						Role:    placement.Leader,
						Count:   1,
						LabelConstraints: []placement.LabelConstraint{
							{
								Key:    placement.DCLabelKey,
								Op:     placement.In,
								Values: []string{"sh"},
							},
						},
					},
				},
			})
		} else if def.Name.String() == "p2" {
			groupID := placement.GroupID(def.ID)
			is.SetBundle(&placement.Bundle{
				ID: groupID,
				Rules: []*placement.Rule{
					{
						GroupID: groupID,
						Role:    placement.Follower,
						Count:   3,
						LabelConstraints: []placement.LabelConstraint{
							{
								Key:    placement.DCLabelKey,
								Op:     placement.In,
								Values: []string{"sh"},
							},
						},
					},
				},
			})
		}
	}

	testCases := []struct {
		name              string
		sql               string
		txnScope          string
		disableAutoCommit bool
		err               error
	}{
		{
			name:     "Insert into PARTITION p0 with global txnScope",
			sql:      "insert into t1 (c) values (1)",
			txnScope: "global",
			err:      nil,
		},
		{
			name:     "insert into PARTITION p0 with wrong txnScope",
			sql:      "insert into t1 (c) values (1)",
			txnScope: "bj",
			err:      fmt.Errorf(".*out of txn_scope.*"),
		},
		{
			name:     "insert into PARTITION p1 with local txnScope",
			sql:      "insert into t1 (c) values (10)",
			txnScope: "bj",
			err:      fmt.Errorf(".*doesn't have placement policies with txn_scope.*"),
		},
		{
			name:     "insert into PARTITION p1 with global txnScope",
			sql:      "insert into t1 (c) values (10)",
			txnScope: "global",
			err:      nil,
		},
		{
			name:     "insert into PARTITION p2 with local txnScope",
			sql:      "insert into t1 (c) values (15)",
			txnScope: "bj",
			err:      fmt.Errorf(".*leader placement policy is not defined.*"),
		},
		{
			name:     "insert into PARTITION p2 with global txnScope",
			sql:      "insert into t1 (c) values (15)",
			txnScope: "global",
			err:      nil,
		},
		{
			name:              "insert into PARTITION p0 with wrong txnScope and autocommit off",
			sql:               "insert into t1 (c) values (1)",
			txnScope:          "bj",
			disableAutoCommit: true,
			err:               fmt.Errorf(".*out of txn_scope.*"),
		},
	}

	for _, testcase := range testCases {
		c.Log(testcase.name)
		se, err := session.CreateSession4Test(s.store)
		c.Check(err, IsNil)
		tk.Se = se
		tk.MustExec("use test")
		tk.MustExec(fmt.Sprintf("set @@txn_scope = %v", testcase.txnScope))
		if testcase.disableAutoCommit {
			tk.MustExec("set @@autocommit = 0")
			tk.MustExec("begin")
			tk.MustExec(testcase.sql)
			_, err = tk.Exec("commit")
		} else {
			_, err = tk.Exec(testcase.sql)
		}
		if testcase.err == nil {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Matches, testcase.err.Error())
		}
	}
}

func (s *testDBSuite1) TestAbortTxnIfPlacementChanged(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tp1")
	tk.Se.GetSessionVars().EnableAlterPlacement = true
	defer func() {
		tk.MustExec("drop table if exists tp1")
		tk.Se.GetSessionVars().EnableAlterPlacement = false
	}()

	tk.MustExec(`create table tp1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)
	se1, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	tk1 := testkit.NewTestKitWithSession(c, s.store, se1)
	tk1.MustExec("use test")

	tk1.Se.GetSessionVars().EnableAlterPlacement = true
	defer func() {
		tk1.Se.GetSessionVars().EnableAlterPlacement = false
	}()
	_, err = tk.Exec(`alter table tp1 alter partition p0
add placement policy
	constraints='["+   zone   =   sh  "]'
	role=leader
	replicas=1;`)
	c.Assert(err, IsNil)
	// modify p0 when alter p0 placement policy, the txn should be failed
	_, err = tk.Exec("begin;")
	c.Assert(err, IsNil)
	_, err = tk1.Exec(`alter table tp1 alter partition p0
add placement policy
	constraints='["+   zone   =   sh  "]'
	role=follower
	replicas=3;`)
	c.Assert(err, IsNil)
	_, err = tk.Exec("insert into tp1 (c) values (1);")
	c.Assert(err, IsNil)
	_, err = tk.Exec("commit")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "*.[domain:8028]*.")

	_, err = tk.Exec(`alter table tp1 alter partition p1
add placement policy
	constraints='["+   zone   =   sh  "]'
	role=leader
	replicas=1;`)
	// modify p0 when alter p1 placement policy, the txn should be success.
	_, err = tk.Exec("begin;")
	c.Assert(err, IsNil)
	_, err = tk1.Exec(`alter table tp1 alter partition p1
add placement policy
	constraints='["+   zone   =   sh  "]'
	role=follower
	replicas=3;`)
	_, err = tk.Exec("insert into tp1 (c) values (1);")
	c.Assert(err, IsNil)
	_, err = tk.Exec("commit")
	c.Assert(err, IsNil)
}

func (s *testDBSuite1) TestGlobalTxnState(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")

	tk.Se.GetSessionVars().EnableAlterPlacement = true
	defer func() {
		tk.Se.GetSessionVars().EnableAlterPlacement = false
	}()

	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11)
);`)

	is := s.dom.InfoSchema()

	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	pid, err := tables.FindPartitionByName(tb.Meta(), "p0")
	c.Assert(err, IsNil)
	groupID := placement.GroupID(pid)
	is.SetBundle(&placement.Bundle{
		ID: groupID,
		Rules: []*placement.Rule{
			{
				GroupID: groupID,
				Role:    placement.Leader,
				Count:   1,
				LabelConstraints: []placement.LabelConstraint{
					{
						Key:    placement.DCLabelKey,
						Op:     placement.In,
						Values: []string{"bj"},
					},
				},
			},
		},
	})
	dbInfo := testGetSchemaByName(c, tk.Se, "test")
	tk2 := testkit.NewTestKit(c, s.store)
	var chkErr error
	done := false
	testcases := []struct {
		name      string
		hook      *ddl.TestDDLCallback
		expectErr error
	}{
		{
			name: "write partition p0 during StateGlobalTxnOnly",
			hook: func() *ddl.TestDDLCallback {
				hook := &ddl.TestDDLCallback{}
				hook.OnJobUpdatedExported = func(job *model.Job) {
					if job.Type == model.ActionAlterTableAlterPartition && job.State == model.JobStateRunning &&
						job.SchemaState == model.StateGlobalTxnOnly && job.SchemaID == dbInfo.ID && done == false {
						done = true
						tk2.MustExec("use test")
						tk2.MustExec("set @@txn_scope=bj")
						_, chkErr = tk2.Exec("insert into t1 (c) values (1);")
					}
				}
				return hook
			}(),
			expectErr: fmt.Errorf(".*can not be written by local transactions when its placement policy is being altered.*"),
		},
		// FIXME: support abort read txn during StateGlobalTxnOnly
		// {
		//	name: "read partition p0 during middle state",
		//	hook: func() *ddl.TestDDLCallback {
		//		hook := &ddl.TestDDLCallback{}
		//		hook.OnJobUpdatedExported = func(job *model.Job) {
		//			if job.Type == model.ActionAlterTableAlterPartition && job.State == model.JobStateRunning &&
		//				job.SchemaState == model.StateGlobalTxnOnly && job.SchemaID == dbInfo.ID && done == false {
		//				done = true
		//				tk2.MustExec("use test")
		//				tk2.MustExec("set @@txn_scope=bj")
		//				tk2.MustExec("begin;")
		//				tk2.MustExec("select * from t1 where c < 6;")
		//				_, chkErr = tk2.Exec("commit")
		//			}
		//		}
		//		return hook
		//	}(),
		//	expectErr: fmt.Errorf(".*can not be written by local transactions when its placement policy is being altered.*"),
		// },
	}
	originalHook := s.dom.DDL().GetHook()
	testFunc := func(name string, hook *ddl.TestDDLCallback, expectErr error) {
		c.Log(name)
		done = false
		s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
		defer func() {
			s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
		}()
		_, err = tk.Exec(`alter table t1 alter partition p0
alter placement policy
	constraints='["+zone=bj"]'
	role=leader
	replicas=1`)
		c.Assert(err, IsNil)
		c.Assert(done, Equals, true)
		if expectErr != nil {
			c.Assert(chkErr, NotNil)
			c.Assert(chkErr.Error(), Matches, expectErr.Error())
		} else {
			c.Assert(chkErr, IsNil)
		}
	}

	for _, testcase := range testcases {
		testFunc(testcase.name, testcase.hook, testcase.expectErr)
	}
}
