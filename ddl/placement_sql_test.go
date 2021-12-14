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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"context"
	"fmt"
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/placement"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

// TODO: Remove in https://github.com/pingcap/tidb/issues/27971 or change to use SQL PLACEMENT POLICY
func (s *testDBSuite1) TestPlacementPolicyCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.Se.GetSessionVars().EnableAlterPlacement = true
	tk.MustExec("set @@tidb_enable_exchange_partition = 1")
	defer func() {
		tk.MustExec("set @@tidb_enable_exchange_partition = 0")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("drop table if exists t2")
		tk.Se.GetSessionVars().EnableAlterPlacement = false
	}()

	initTable := func() []string {
		tk.MustExec("drop table if exists t2")
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
	_ = initTable()
	//tk.MustQuery("select * from information_schema.placement_policy order by REPLICAS").Check(testkit.Rows(rows...))
	tk.MustExec("alter table t1 drop partition p0")
	//tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows(rows[1:]...))

	_ = initTable()
	//tk.MustQuery("select * from information_schema.placement_policy order by REPLICAS").Check(testkit.Rows(rows...))
	tk.MustExec("drop table t1")
	//tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows())

	// test truncate
	_ = initTable()
	//tk.MustQuery("select * from information_schema.placement_policy order by REPLICAS").Check(testkit.Rows(rows...))
	tk.MustExec("alter table t1 truncate partition p0")
	//tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows(rows[1:]...))

	_ = initTable()
	//tk.MustQuery("select * from information_schema.placement_policy order by REPLICAS").Check(testkit.Rows(rows...))
	tk.MustExec("truncate table t1")
	//tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows())

	// test exchange
	_ = initTable()
	//tk.MustQuery("select * from information_schema.placement_policy order by REPLICAS").Check(testkit.Rows(rows...))
	tk.MustExec("create table t2(id int)")
	tk.MustExec("alter table t1 exchange partition p0 with table t2")
	//tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows())
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
						Constraints: []placement.Constraint{
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
						Constraints: []placement.Constraint{
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
		zone              string
		disableAutoCommit bool
		err               error
	}{
		{
			name:     "Insert into PARTITION p0 with global txnScope",
			sql:      "insert into t1 (c) values (1)",
			txnScope: "global",
			zone:     "",
			err:      nil,
		},
		{
			name:     "insert into PARTITION p0 with wrong txnScope",
			sql:      "insert into t1 (c) values (1)",
			txnScope: "local",
			zone:     "bj",
			err:      fmt.Errorf(".*out of txn_scope.*"),
		},
		{
			name:     "insert into PARTITION p1 with local txnScope",
			sql:      "insert into t1 (c) values (10)",
			txnScope: "local",
			zone:     "bj",
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
			txnScope: "local",
			zone:     "bj",
			err:      fmt.Errorf(".*leader placement policy is not defined.*"),
		},
		{
			name:     "insert into PARTITION p2 with global txnScope",
			sql:      "insert into t1 (c) values (15)",
			txnScope: "global",
			zone:     "",
			err:      nil,
		},
		{
			name:              "insert into PARTITION p0 with wrong txnScope and autocommit off",
			sql:               "insert into t1 (c) values (1)",
			txnScope:          "local",
			zone:              "bj",
			disableAutoCommit: true,
			err:               fmt.Errorf(".*out of txn_scope.*"),
		},
	}

	for _, testcase := range testCases {
		c.Log(testcase.name)
		failpoint.Enable("tikvclient/injectTxnScope",
			fmt.Sprintf(`return("%v")`, testcase.zone))
		se, err := session.CreateSession4Test(s.store)
		c.Check(err, IsNil)
		tk.Se = se
		tk.MustExec("use test")
		tk.MustExec("set global tidb_enable_local_txn = on;")
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
		tk.MustExec("set global tidb_enable_local_txn = off;")
		failpoint.Disable("tikvclient/injectTxnScope")
	}
}

func (s *testDBSuite6) TestCreateSchemaWithPlacement(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop schema if exists SchemaDirectPlacementTest")
	tk.MustExec("drop schema if exists SchemaPolicyPlacementTest")
	tk.Se.GetSessionVars().EnableAlterPlacement = true
	defer func() {
		tk.MustExec("drop schema if exists SchemaDirectPlacementTest")
		tk.MustExec("drop schema if exists SchemaPolicyPlacementTest")
		tk.MustExec("drop placement policy if exists PolicySchemaTest")
		tk.MustExec("drop placement policy if exists PolicyTableTest")
		tk.Se.GetSessionVars().EnableAlterPlacement = false
	}()

	tk.MustExec(`CREATE SCHEMA SchemaDirectPlacementTest PRIMARY_REGION='nl' REGIONS = "se,nz,nl" FOLLOWERS=3`)
	tk.MustQuery("SHOW CREATE SCHEMA schemadirectplacementtest").Check(testkit.Rows("SchemaDirectPlacementTest CREATE DATABASE `SchemaDirectPlacementTest` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] PRIMARY_REGION=\"nl\" REGIONS=\"se,nz,nl\" FOLLOWERS=3 */"))
	tk.MustQuery("SELECT * FROM information_schema.placement_rules WHERE SCHEMA_NAME='SchemaDirectPlacementTest'").Check(testkit.Rows("<nil> def <nil> SchemaDirectPlacementTest <nil> <nil> nl se,nz,nl      3 0"))

	tk.MustExec(`CREATE PLACEMENT POLICY PolicySchemaTest LEADER_CONSTRAINTS = "[+region=nl]" FOLLOWER_CONSTRAINTS="[+region=se]" FOLLOWERS=4 LEARNER_CONSTRAINTS="[+region=be]" LEARNERS=4`)
	tk.MustExec(`CREATE PLACEMENT POLICY PolicyTableTest LEADER_CONSTRAINTS = "[+region=tl]" FOLLOWER_CONSTRAINTS="[+region=tf]" FOLLOWERS=2 LEARNER_CONSTRAINTS="[+region=tle]" LEARNERS=1`)
	tk.MustQuery("SHOW PLACEMENT like 'POLICY %PolicySchemaTest%'").Check(testkit.Rows("POLICY PolicySchemaTest LEADER_CONSTRAINTS=\"[+region=nl]\" FOLLOWERS=4 FOLLOWER_CONSTRAINTS=\"[+region=se]\" LEARNERS=4 LEARNER_CONSTRAINTS=\"[+region=be]\" NULL"))
	tk.MustQuery("SHOW PLACEMENT like 'POLICY %PolicyTableTest%'").Check(testkit.Rows("POLICY PolicyTableTest LEADER_CONSTRAINTS=\"[+region=tl]\" FOLLOWERS=2 FOLLOWER_CONSTRAINTS=\"[+region=tf]\" LEARNERS=1 LEARNER_CONSTRAINTS=\"[+region=tle]\" NULL"))
	tk.MustExec("CREATE SCHEMA SchemaPolicyPlacementTest PLACEMENT POLICY = `PolicySchemaTest`")
	tk.MustQuery("SHOW CREATE SCHEMA SCHEMAPOLICYPLACEMENTTEST").Check(testkit.Rows("SchemaPolicyPlacementTest CREATE DATABASE `SchemaPolicyPlacementTest` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] PLACEMENT POLICY=`PolicySchemaTest` */"))

	tk.MustExec(`CREATE TABLE SchemaDirectPlacementTest.UseSchemaDefault (a int unsigned primary key, b varchar(255))`)
	tk.MustQuery(`SHOW CREATE TABLE SchemaDirectPlacementTest.UseSchemaDefault`).Check(testkit.Rows(
		"UseSchemaDefault CREATE TABLE `UseSchemaDefault` (\n" +
			"  `a` int(10) unsigned NOT NULL,\n" +
			"  `b` varchar(255) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PRIMARY_REGION=\"nl\" REGIONS=\"se,nz,nl\" FOLLOWERS=3 */"))
	tk.MustQuery("SELECT CATALOG_NAME, SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.schemata WHERE SCHEMA_NAME='SchemaDirectPlacementTest'").Check(testkit.Rows(`def SchemaDirectPlacementTest utf8mb4 utf8mb4_bin <nil> PRIMARY_REGION="nl" REGIONS="se,nz,nl" FOLLOWERS=3`))
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='SchemaDirectPlacementTest' AND TABLE_NAME = 'UseSchemaDefault'").Check(testkit.Rows(`def SchemaDirectPlacementTest UseSchemaDefault <nil> PRIMARY_REGION="nl" REGIONS="se,nz,nl" FOLLOWERS=3`))

	tk.MustExec(`CREATE TABLE SchemaDirectPlacementTest.UseDirectPlacement (a int unsigned primary key, b varchar(255)) PRIMARY_REGION="se" REGIONS="se"`)

	tk.MustQuery(`SHOW CREATE TABLE SchemaDirectPlacementTest.UseDirectPlacement`).Check(testkit.Rows(
		"UseDirectPlacement CREATE TABLE `UseDirectPlacement` (\n" +
			"  `a` int(10) unsigned NOT NULL,\n" +
			"  `b` varchar(255) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PRIMARY_REGION=\"se\" REGIONS=\"se\" */"))
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='SchemaDirectPlacementTest' AND TABLE_NAME = 'UseDirectPlacement'").Check(testkit.Rows("def SchemaDirectPlacementTest UseDirectPlacement <nil> PRIMARY_REGION=\"se\" REGIONS=\"se\""))

	tk.MustExec(`CREATE TABLE SchemaPolicyPlacementTest.UseSchemaDefault (a int unsigned primary key, b varchar(255))`)
	tk.MustQuery(`SHOW CREATE TABLE SchemaPolicyPlacementTest.UseSchemaDefault`).Check(testkit.Rows(
		"UseSchemaDefault CREATE TABLE `UseSchemaDefault` (\n" +
			"  `a` int(10) unsigned NOT NULL,\n" +
			"  `b` varchar(255) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`PolicySchemaTest` */"))
	tk.MustQuery("SELECT CATALOG_NAME, SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.schemata WHERE SCHEMA_NAME='SchemaPolicyPlacementTest'").Check(testkit.Rows(`def SchemaPolicyPlacementTest utf8mb4 utf8mb4_bin PolicySchemaTest <nil>`))
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='SchemaPolicyPlacementTest' AND TABLE_NAME = 'UseSchemaDefault'").Check(testkit.Rows(`def SchemaPolicyPlacementTest UseSchemaDefault PolicySchemaTest <nil>`))

	tk.MustExec(`CREATE TABLE SchemaPolicyPlacementTest.UsePolicy (a int unsigned primary key, b varchar(255)) PLACEMENT POLICY = "PolicyTableTest"`)
	tk.MustQuery(`SHOW CREATE TABLE SchemaPolicyPlacementTest.UsePolicy`).Check(testkit.Rows(
		"UsePolicy CREATE TABLE `UsePolicy` (\n" +
			"  `a` int(10) unsigned NOT NULL,\n" +
			"  `b` varchar(255) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`PolicyTableTest` */"))
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='SchemaPolicyPlacementTest' AND TABLE_NAME = 'UsePolicy'").Check(testkit.Rows(`def SchemaPolicyPlacementTest UsePolicy PolicyTableTest <nil>`))

	is := s.dom.InfoSchema()

	db, ok := is.SchemaByName(model.NewCIStr("SchemaDirectPlacementTest"))
	c.Assert(ok, IsTrue)
	c.Assert(db.PlacementPolicyRef, IsNil)
	c.Assert(db.DirectPlacementOpts, NotNil)
	c.Assert(db.DirectPlacementOpts.PrimaryRegion, Matches, "nl")
	c.Assert(db.DirectPlacementOpts.Regions, Matches, "se,nz,nl")
	c.Assert(db.DirectPlacementOpts.Followers, Equals, uint64(3))
	c.Assert(db.DirectPlacementOpts.Learners, Equals, uint64(0))

	db, ok = is.SchemaByName(model.NewCIStr("SchemaPolicyPlacementTest"))
	c.Assert(ok, IsTrue)
	c.Assert(db.PlacementPolicyRef, NotNil)
	c.Assert(db.DirectPlacementOpts, IsNil)
	c.Assert(db.PlacementPolicyRef.Name.O, Equals, "PolicySchemaTest")
}

func (s *testDBSuite6) TestAlterDBPlacement(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists TestAlterDB;")
	tk.MustExec("create database TestAlterDB;")
	tk.MustExec("use TestAlterDB")
	tk.MustExec("drop placement policy if exists alter_x")
	tk.MustExec("drop placement policy if exists alter_y")
	tk.MustExec("create placement policy alter_x PRIMARY_REGION=\"cn-east-1\", REGIONS=\"cn-east-1\";")
	defer tk.MustExec("drop placement policy if exists alter_x")
	tk.MustExec("create placement policy alter_y PRIMARY_REGION=\"cn-east-2\", REGIONS=\"cn-east-2\";")
	defer tk.MustExec("drop placement policy if exists alter_y")
	defer tk.MustExec(`DROP DATABASE IF EXISTS TestAlterDB;`) // Must drop tables before policy

	// Policy Test
	// Test for Non-Exist policy
	tk.MustGetErrCode("ALTER DATABASE TestAlterDB PLACEMENT POLICY=`alter_z`;", mysql.ErrPlacementPolicyNotExists)
	tk.MustQuery("SELECT CATALOG_NAME, SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.schemata WHERE SCHEMA_NAME='TestAlterDB'").Check(testkit.Rows(`def TestAlterDB utf8mb4 utf8mb4_bin <nil> <nil>`))

	tk.MustExec("ALTER DATABASE TestAlterDB PLACEMENT POLICY=`alter_x`;")
	// Test for information_schema.schemata
	tk.MustQuery("SELECT CATALOG_NAME, SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.schemata WHERE SCHEMA_NAME='TestAlterDB'").Check(testkit.Rows(`def TestAlterDB utf8mb4 utf8mb4_bin alter_x <nil>`))
	// Test for Show Create Database
	tk.MustQuery(`show create database TestAlterDB`).Check(testutil.RowsWithSep("|",
		"TestAlterDB CREATE DATABASE `TestAlterDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PLACEMENT POLICY=`alter_x` */",
	))
	// Test for Alter Placement Policy affect table created.
	tk.MustExec("create table t(a int);")
	tk.MustQuery(`show create table t`).Check(testutil.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin "+
			"/*T![placement] PLACEMENT POLICY=`alter_x` */",
	))
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='TestAlterDB' AND TABLE_NAME = 't'").Check(testkit.Rows(`def TestAlterDB t alter_x <nil>`))
	// Test for Alter Default Placement Policy, And will not update the old table options.
	tk.MustExec("ALTER DATABASE TestAlterDB DEFAULT PLACEMENT POLICY=`alter_y`;")
	tk.MustExec("create table t2(a int);")
	tk.MustQuery(`show create table t`).Check(testutil.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin "+
			"/*T![placement] PLACEMENT POLICY=`alter_x` */",
	))
	tk.MustQuery(`show create table t2`).Check(testutil.RowsWithSep("|",
		"t2 CREATE TABLE `t2` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin "+
			"/*T![placement] PLACEMENT POLICY=`alter_y` */",
	))
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='TestAlterDB' AND TABLE_NAME = 't2'").Check(testkit.Rows(`def TestAlterDB t2 alter_y <nil>`))

	// Reset Test
	tk.MustExec("drop database if exists TestAlterDB;")
	tk.MustExec("create database TestAlterDB;")
	tk.MustExec("use TestAlterDB")

	// DirectOption Test
	tk.MustExec("ALTER DATABASE TestAlterDB PRIMARY_REGION=\"se\" FOLLOWERS=2 REGIONS=\"se\";")
	// Test for information_schema.schemata
	tk.MustQuery("SELECT CATALOG_NAME, SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.schemata WHERE SCHEMA_NAME='TestAlterDB'").Check(testkit.Rows(`def TestAlterDB utf8mb4 utf8mb4_bin <nil> PRIMARY_REGION="se" REGIONS="se" FOLLOWERS=2`))
	// Test for Show Create Database
	tk.MustQuery(`show create database TestAlterDB`).Check(testutil.RowsWithSep("|",
		"TestAlterDB CREATE DATABASE `TestAlterDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PRIMARY_REGION=\"se\" REGIONS=\"se\" FOLLOWERS=2 */",
	))
	// Test for Alter Placement Rule affect table created.
	tk.MustExec("create table t3(a int);")
	tk.MustQuery(`show create table t3`).Check(testutil.RowsWithSep("|",
		"t3 CREATE TABLE `t3` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin "+
			"/*T![placement] PRIMARY_REGION=\"se\" REGIONS=\"se\" FOLLOWERS=2 */",
	))
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='TestAlterDB' AND TABLE_NAME = 't3'").Check(testkit.Rows("def TestAlterDB t3 <nil> PRIMARY_REGION=\"se\" REGIONS=\"se\" FOLLOWERS=2"))
	// Test for override default option
	tk.MustExec("create table t4(a int) PLACEMENT POLICY=\"alter_x\";")
	tk.MustQuery(`show create table t4`).Check(testutil.RowsWithSep("|",
		"t4 CREATE TABLE `t4` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin "+
			"/*T![placement] PLACEMENT POLICY=`alter_x` */",
	))
	tk.MustQuery("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TIDB_PLACEMENT_POLICY_NAME, TIDB_DIRECT_PLACEMENT FROM information_schema.Tables WHERE TABLE_SCHEMA='TestAlterDB' AND TABLE_NAME = 't4'").Check(testkit.Rows(`def TestAlterDB t4 alter_x <nil>`))

	// Hybrid Test
	// Test for alter both policy and  direct options.
	tk.MustQuery(`show create database TestAlterDB`).Check(testutil.RowsWithSep("|",
		"TestAlterDB CREATE DATABASE `TestAlterDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PRIMARY_REGION=\"se\" REGIONS=\"se\" FOLLOWERS=2 */",
	))
	tk.MustGetErrCode("ALTER DATABASE TestAlterDB PLACEMENT POLICY=`alter_x` FOLLOWERS=2;", mysql.ErrPlacementPolicyWithDirectOption)
	tk.MustGetErrCode("ALTER DATABASE TestAlterDB DEFAULT PLACEMENT POLICY=`alter_y` PRIMARY_REGION=\"se\" FOLLOWERS=2;", mysql.ErrPlacementPolicyWithDirectOption)
	tk.MustQuery(`show create database TestAlterDB`).Check(testutil.RowsWithSep("|",
		"TestAlterDB CREATE DATABASE `TestAlterDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PRIMARY_REGION=\"se\" REGIONS=\"se\" FOLLOWERS=2 */",
	))
	// Test for change direct options to policy.
	tk.MustExec("ALTER DATABASE TestAlterDB PLACEMENT POLICY=`alter_x`;")
	tk.MustQuery(`show create database TestAlterDB`).Check(testutil.RowsWithSep("|",
		"TestAlterDB CREATE DATABASE `TestAlterDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PLACEMENT POLICY=`alter_x` */",
	))
	// Test for change policy to direct options.
	tk.MustExec("ALTER DATABASE TestAlterDB PRIMARY_REGION=\"se\" FOLLOWERS=2 REGIONS=\"se\" ;")
	tk.MustQuery(`show create database TestAlterDB`).Check(testutil.RowsWithSep("|",
		"TestAlterDB CREATE DATABASE `TestAlterDB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ "+
			"/*T![placement] PRIMARY_REGION=\"se\" REGIONS=\"se\" FOLLOWERS=2 */",
	))
}

func (s *testDBSuite6) TestEnablePlacementCheck(c *C) {

	tk := testkit.NewTestKit(c, s.store)
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "set @@global.tidb_enable_alter_placement=1")
	c.Assert(err, IsNil)

	tk.MustExec("drop database if exists TestPlacementDB;")
	tk.MustExec("create database TestPlacementDB;")
	tk.MustExec("use TestPlacementDB;")
	tk.MustExec("drop placement policy if exists placement_x;")
	tk.MustExec("create placement policy placement_x PRIMARY_REGION=\"cn-east-1\", REGIONS=\"cn-east-1\";")
	se.GetSessionVars().EnableAlterPlacement = true
	tk.MustExec("create table t(c int) partition by range (c) (partition p1 values less than (200) followers=2);")
	defer func() {
		tk.MustExec("drop database if exists TestPlacementDB;")
		tk.MustExec("drop placement policy if exists placement_x;")
	}()

	tk.Se.GetSessionVars().EnableAlterPlacement = false
	tk.MustGetErrCode("create database TestPlacementDB2 followers=2;", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter database TestPlacementDB placement policy=placement_x", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("create table t (c int) FOLLOWERS=2;", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t voters=2;", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("create table m (c int) partition by range (c) (partition p1 values less than (200) followers=2);", mysql.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t partition p1 placement policy=\"placement_x\";", mysql.ErrUnsupportedDDLOperation)
}
