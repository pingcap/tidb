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

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testDBSuite1) TestAlterTableAlterPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11),
	PARTITION p2 VALUES LESS THAN (16),
	PARTITION p3 VALUES LESS THAN (21)
);`)

	// normal cases
	_, err := tk.Exec(`alter table t1 alter partition p0
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
	constraints='{"+zone=sh,+zone=bj":1,"+zone=sh,+zone=bj":1}'
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
	constraints='{"+zone=sh,+zone=bj":1,"+zone=sh,+zone=bj":1}'
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
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{"+zone=sh,+zone=bj":1,"+zone=sh,+zone=bj":1}'
	role=voter
	replicas=3,
drop placement policy
	role=leader`)
	c.Assert(err, IsNil)

	// list/dict detection
	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints=',,,'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*array or object.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='[,,,'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*invalid character.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{,,,'
	role=follower
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*invalid character.*")

	_, err = tk.Exec(`alter table t1 alter partition p0
add placement policy
	constraints='{"+   zone   =   sh  ": 1, "- zone = bj": 2}'
	role=follower
	replicas=2`)
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
	defer tk.MustExec("drop table if exists t1")

	initTable := func() []string {
		bundles := make(map[string]*placement.Bundle)
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t1(id int) partition by hash(id) partitions 2")

		is := s.dom.InfoSchema()
		is.MockBundles(bundles)

		tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
		c.Assert(err, IsNil)
		partDefs := tb.Meta().GetPartitionInfo().Definitions

		rows := []string{}
		for k, v := range partDefs {
			ptID := placement.GroupID(v.ID)
			bundles[ptID] = &placement.Bundle{
				ID:    ptID,
				Rules: []*placement.Rule{{Count: k}},
			}
			rows = append(rows, fmt.Sprintf("%s 0  test t1 %s <nil>  %d ", ptID, v.Name.L, k))
		}
		return rows
	}

	// test drop
	rows := initTable()
	tk.MustQuery("select * from information_schema.placement_policy order by REPLICAS").Check(testkit.Rows(rows...))
	tk.MustExec("drop table t1")
	tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows())

	// test truncate
	rows = initTable()
	tk.MustQuery("select * from information_schema.placement_policy order by REPLICAS").Check(testkit.Rows(rows...))
	tk.MustExec("truncate table t1")
	tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows())
}
