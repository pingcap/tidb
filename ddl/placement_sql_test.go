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
	"fmt"
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
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
	tk.MustExec("set @@tidb_enable_exchange_partition = 1")
	defer func() {
		tk.MustExec("set @@tidb_enable_exchange_partition = 0")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("drop table if exists t2")
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
	defer func() {
		tk.MustExec("drop table if exists t1")
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
	tk.MustExec("set @@tidb_enable_direct_placement=1")
	tk.MustExec("drop schema if exists SchemaDirectPlacementTest")
	tk.MustExec("drop schema if exists SchemaPolicyPlacementTest")
	defer func() {
		tk.MustExec("drop schema if exists SchemaDirectPlacementTest")
		tk.MustExec("drop schema if exists SchemaPolicyPlacementTest")
		tk.MustExec("drop placement policy if exists PolicySchemaTest")
		tk.MustExec("drop placement policy if exists PolicyTableTest")
	}()

	tk.MustExec(`CREATE SCHEMA SchemaDirectPlacementTest PRIMARY_REGION='nl' REGIONS = "se,nz,nl" FOLLOWERS=3`)
	tk.MustQuery("SHOW CREATE SCHEMA schemadirectplacementtest").Check(testkit.Rows("SchemaDirectPlacementTest CREATE DATABASE `SchemaDirectPlacementTest` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] PRIMARY_REGION=\"nl\" REGIONS=\"se,nz,nl\" FOLLOWERS=3 */"))

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
	tk.MustExec("set @@tidb_enable_direct_placement=1")
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

func (s *testDBSuite6) TestPlacementMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists db1")
	tk.MustExec("drop database if exists db2")
	tk.MustExec("drop table if exists t1,t2,t3,t4")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")

	// default should be strict
	tk.MustQuery("select @@tidb_placement_mode").Check(testkit.Rows("STRICT"))

	// prepare policy, db and tables
	tk.MustExec("create placement policy p1 followers=4")
	defer tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("create database db1 placement policy p1")
	defer tk.MustExec("drop database if exists db1")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("create placement policy p2 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy if exists p2")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("create table t1(id int) placement policy p1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("create table t2(id int) PARTITION BY RANGE (id) (" +
		"PARTITION p0 VALUES LESS THAN (100) placement policy p1," +
		"PARTITION p1 VALUES LESS THAN (1000))")
	defer tk.MustExec("drop table if exists t2")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	// invalid values
	err := tk.ExecToErr("set tidb_placement_mode='aaa'")
	c.Assert(err.Error(), Equals, "[variable:1231]Variable 'tidb_placement_mode' can't be set to the value of 'aaa'")

	// ignore mode
	tk.MustExec("set tidb_placement_mode='ignore'")
	tk.MustQuery("select @@tidb_placement_mode").Check(testkit.Rows("IGNORE"))

	// create placement policy in ignore mode (policy name exists)
	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show placement where target='POLICY p1'").Check(testkit.Rows("POLICY p1 FOLLOWERS=4 NULL"))

	// create placement policy in ignore mode (policy name not exists)
	tk.MustExec("create placement policy p3 primary_region='r1' regions='r1'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show placement where target='POLICY p3'").Check(testkit.Rows())

	// alter placement policy in ignore mode (policy exists)
	tk.MustExec("alter placement policy p1 primary_region='r1' regions='r1'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show placement where target='POLICY p1'").Check(testkit.Rows("POLICY p1 FOLLOWERS=4 NULL"))

	// alter placement policy in ignore mode (policy not exists)
	tk.MustExec("alter placement policy p3 primary_region='r1' regions='r1'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show placement where target='POLICY p3'").Check(testkit.Rows())

	// drop placement policy in ignore mode (policy exists)
	tk.MustExec("drop placement policy p1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show placement where target='POLICY p1'").Check(testkit.Rows("POLICY p1 FOLLOWERS=4 NULL"))

	// drop placement policy in ignore mode (policy not exists)
	tk.MustExec("drop placement policy p3")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show placement where target='POLICY p3'").Check(testkit.Rows())

	// create database in ignore mode (ref exist policy)
	tk.MustExec("create database db2 placement policy p1")
	defer tk.MustExec("drop database if exists db2")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create database db2").Check(testkit.Rows("db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	tk.MustExec("drop database db2")

	// create database in ignore mode (ref non exist policy)
	tk.MustExec("create database db2 placement policy pxxx")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create database db2").Check(testkit.Rows("db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))

	// alter database in ignore mode (policy exists)
	tk.MustExec("alter database db2 placement policy p1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create database db2").Check(testkit.Rows("db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))

	// alter database in ignore mode (policy not exists)
	tk.MustExec("alter database db2 placement policy px")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create database db2").Check(testkit.Rows("db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))

	// alter database in ignore mode  (other alter should succeed)
	tk.MustExec("alter database db2 placement policy px DEFAULT CHARACTER SET 'ascii'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create database db2").Check(testkit.Rows("db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET ascii */"))

	// create table in ignore mode (ref exist policy)
	tk.MustExec("create table t3(id int) placement policy p1")
	defer tk.MustExec("drop table if exists t3")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create table t3").Check(testkit.Rows("t3 CREATE TABLE `t3` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// create table like in ignore mode
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3 like t1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create table t3").Check(testkit.Rows("t3 CREATE TABLE `t3` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// create table in ignore mode (db has placement)
	tk.MustExec("create table db1.t1(id int)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create table db1.t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// create partitioned table in ignore mode (ref exist policy)
	tk.MustExec("CREATE TABLE t4 (id INT) PLACEMENT POLICY p1 PARTITION BY RANGE (id) (" +
		"PARTITION p0 VALUES LESS THAN (100) PLACEMENT POLICY p3," +
		"PARTITION p1 VALUES LESS THAN (1000) PLACEMENT POLICY p1," +
		"PARTITION p2 VALUES LESS THAN (10000)," +
		"PARTITION p3 VALUES LESS THAN (100000) PLACEMENT POLICY p2)")
	defer tk.MustExec("drop table if exists t4")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create table t4").Check(testkit.Rows("t4 CREATE TABLE `t4` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000),\n" +
		" PARTITION `p3` VALUES LESS THAN (100000))"))

	// create partitioned table in ignore mode (ref non exist policy)
	tk.MustExec("drop table t4")
	tk.MustExec("CREATE TABLE t4 (id INT) PLACEMENT POLICY pxxx PARTITION BY RANGE (id) (" +
		"PARTITION p0 VALUES LESS THAN (100) PLACEMENT POLICY pyyy," +
		"PARTITION p1 VALUES LESS THAN (1000) PLACEMENT POLICY pzzz," +
		"PARTITION p2 VALUES LESS THAN (10000)," +
		"PARTITION p3 VALUES LESS THAN (100000) PLACEMENT POLICY pttt)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create table t4").Check(testkit.Rows("t4 CREATE TABLE `t4` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000),\n" +
		" PARTITION `p3` VALUES LESS THAN (100000))"))

	// alter table placement in ignore mode (policy exists)
	tk.MustExec("alter table t1 placement policy p2")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */"))

	// alter table placement in ignore mode (policy not exists)
	tk.MustExec("alter table t1 placement policy p3")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */"))

	// alter table in ignore mode (other alter should succeed)
	tk.MustExec("alter table t1 placement policy p2 comment='aaa'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='aaa' /*T![placement] PLACEMENT POLICY=`p1` */"))

	// add partition in ignore mode (policy exists)
	tk.MustExec("alter table t2 add partition (partition p2 values less than(10000) placement policy p1)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create table t2").Check(testkit.Rows("t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000))"))

	// add partition in ignore mode (policy not exists)
	tk.MustExec("alter table t2 add partition (partition p3 values less than(100000) placement policy p3)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create table t2").Check(testkit.Rows("t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000),\n" +
		" PARTITION `p3` VALUES LESS THAN (100000))"))

	// alter partition placement in ignore mode (policy exists)
	tk.MustExec("alter table t2 partition p0 placement policy p1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create table t2").Check(testkit.Rows("t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000),\n" +
		" PARTITION `p3` VALUES LESS THAN (100000))"))

	// alter partition placement in ignore mode (policy not exists)
	tk.MustExec("alter table t2 partition p0 placement policy p3")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"))
	tk.MustQuery("show create table t2").Check(testkit.Rows("t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (1000),\n" +
		" PARTITION `p2` VALUES LESS THAN (10000),\n" +
		" PARTITION `p3` VALUES LESS THAN (100000))"))

	// create tableWithInfo in ignore mode
	tk.MustExec("drop table if exists t2")
	tbl, err := s.getClonedTable("test", "t1")
	c.Assert(err, IsNil)
	c.Assert(tbl.PlacementPolicyRef, NotNil)
	tbl.Name = model.NewCIStr("t2")
	err = s.dom.DDL().CreateTableWithInfo(tk.Se, model.NewCIStr("test"), tbl, ddl.OnExistError)
	c.Assert(err, IsNil)
	tk.MustQuery("show create table t2").Check(testkit.Rows("t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='aaa'"))

	// createTableWithInfo in ignore mode (policy not exists)
	tk.MustExec("drop table if exists t2")
	tbl, err = s.getClonedTable("test", "t1")
	c.Assert(err, IsNil)
	c.Assert(tbl.PlacementPolicyRef, NotNil)
	tbl.Name = model.NewCIStr("t2")
	tbl.PlacementPolicyRef.Name = model.NewCIStr("pxx")
	err = s.dom.DDL().CreateTableWithInfo(tk.Se, model.NewCIStr("test"), tbl, ddl.OnExistError)
	c.Assert(err, IsNil)
	tk.MustQuery("show create table t2").Check(testkit.Rows("t2 CREATE TABLE `t2` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='aaa'"))

	// createSchemaWithInfo in ignore mode
	tk.MustExec("drop database if exists db2")
	db1, ok := s.getClonedDatabase("db1")
	c.Assert(ok, IsTrue)
	c.Assert(db1.PlacementPolicyRef, NotNil)
	db1.Name = model.NewCIStr("db2")
	err = s.dom.DDL().CreateSchemaWithInfo(tk.Se, db1, ddl.OnExistError)
	c.Assert(err, IsNil)
	tk.MustQuery("show create database db2").Check(testkit.Rows("db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))

	// createSchemaWithInfo in ignore mode (policy not exists)
	tk.MustExec("drop database if exists db2")
	db1, ok = s.getClonedDatabase("db1")
	c.Assert(ok, IsTrue)
	c.Assert(db1.PlacementPolicyRef, NotNil)
	db1.Name = model.NewCIStr("db2")
	db1.PlacementPolicyRef.Name = model.NewCIStr("pxx")
	err = s.dom.DDL().CreateSchemaWithInfo(tk.Se, db1, ddl.OnExistError)
	c.Assert(err, IsNil)
	tk.MustQuery("show create database db2").Check(testkit.Rows("db2 CREATE DATABASE `db2` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))

}

func (s *testDBSuite6) TestPlacementTiflashCheck(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
		c.Assert(err, IsNil)
	}()

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_direct_placement=1")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop table if exists tp")

	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1'")
	defer tk.MustExec("drop placement policy if exists p1")

	tk.MustExec(`CREATE TABLE tp (id INT) PARTITION BY RANGE (id) (
	   PARTITION p0 VALUES LESS THAN (100),
	   PARTITION p1 VALUES LESS THAN (1000)
	)`)
	defer tk.MustExec("drop table if exists tp")
	tk.MustExec("alter table tp set tiflash replica 1")

	err := tk.ExecToErr("alter table tp placement policy p1")
	c.Assert(ddl.ErrIncompatibleTiFlashAndPlacement.Equal(err), IsTrue)
	err = tk.ExecToErr("alter table tp primary_region='r2' regions='r2'")
	c.Assert(ddl.ErrIncompatibleTiFlashAndPlacement.Equal(err), IsTrue)
	err = tk.ExecToErr("alter table tp partition p0 placement policy p1")
	c.Assert(ddl.ErrIncompatibleTiFlashAndPlacement.Equal(err), IsTrue)
	err = tk.ExecToErr("alter table tp partition p0 primary_region='r2' regions='r2'")
	c.Assert(ddl.ErrIncompatibleTiFlashAndPlacement.Equal(err), IsTrue)
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))

	tk.MustExec("drop table tp")
	tk.MustExec(`CREATE TABLE tp (id INT) placement policy p1 PARTITION BY RANGE (id) (
	   PARTITION p0 VALUES LESS THAN (100),
	   PARTITION p1 VALUES LESS THAN (1000)
	)`)
	err = tk.ExecToErr("alter table tp set tiflash replica 1")
	c.Assert(ddl.ErrIncompatibleTiFlashAndPlacement.Equal(err), IsTrue)
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`p1` */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))

	tk.MustExec("drop table tp")
	tk.MustExec(`CREATE TABLE tp (id INT) PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) placement policy p1 ,
        PARTITION p1 VALUES LESS THAN (1000)
	)`)
	err = tk.ExecToErr("alter table tp set tiflash replica 1")
	c.Assert(ddl.ErrIncompatibleTiFlashAndPlacement.Equal(err), IsTrue)
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p1` */,\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))

	tk.MustExec("drop table tp")
	tk.MustExec(`CREATE TABLE tp (id INT) primary_region='r2' regions='r2' PARTITION BY RANGE (id) (
	   PARTITION p0 VALUES LESS THAN (100),
	   PARTITION p1 VALUES LESS THAN (1000)
	)`)
	err = tk.ExecToErr("alter table tp set tiflash replica 1")
	c.Assert(ddl.ErrIncompatibleTiFlashAndPlacement.Equal(err), IsTrue)
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PRIMARY_REGION=\"r2\" REGIONS=\"r2\" */\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100),\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))

	tk.MustExec("drop table tp")
	tk.MustExec(`CREATE TABLE tp (id INT) PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100)  primary_region='r3' regions='r3',
        PARTITION p1 VALUES LESS THAN (1000)
	)`)
	err = tk.ExecToErr("alter table tp set tiflash replica 1")
	c.Assert(ddl.ErrIncompatibleTiFlashAndPlacement.Equal(err), IsTrue)
	tk.MustQuery("show create table tp").Check(testkit.Rows("" +
		"tp CREATE TABLE `tp` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`id`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PRIMARY_REGION=\"r3\" REGIONS=\"r3\" */,\n" +
		" PARTITION `p1` VALUES LESS THAN (1000))"))
}

func (s *testDBSuite6) TestEnableDirectPlacement(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop database if exists db1")
	tk.MustExec("drop table if exists tp")
	tk.MustExec("drop placement policy if exists p1")
	defer func() {
		tk.MustExec("drop database if exists db1")
		tk.MustExec("drop table if exists tp")
		tk.MustExec("drop placement policy if exists p1")
	}()
	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1,r2'")
	tk.MustExec(`CREATE TABLE tp (id INT) placement policy p1 PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100) placement policy p1,
        PARTITION p1 VALUES LESS THAN (1000)
	)`)
	tk.MustExec("create database db1 placement policy p1")

	errorSQLs := []string{
		"create database db2 followers=1",
		"alter database db1 followers=1",
		"create table t(a int) followers=1",
		"alter table tp followers=1",
		"alter table tp partition p0 followers=1",
		"alter table tp add partition(partition p2 values less than(10000) followers=1)",
	}

	for _, sql := range errorSQLs {
		err := tk.ExecToErr(sql)
		c.Assert(err.Error(), Equals, "Direct placement is disabled")
	}
}
