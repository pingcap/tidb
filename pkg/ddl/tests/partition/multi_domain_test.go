// Copyright 2024 PingCAP, Inc.
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

package partition

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	parserModel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)


func TestMultiSchemaDropRangePartition(t *testing.T) {
	createSQL := `create table t (a int primary key, b varchar(255)) partition by range (a) (partition p0 values less than (100), partition p1 values less than (200))`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1),(2,2),(101,101),(102,102)`)
	}
	alterSQL := `alter table t drop partition p0`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		// TODO: Test both static and dynamic partition pruning!
		switch schemaState {
		case "write only":
			// tkNO are unaware of the DDL
			// tkO see non-readable/non-writable p0 partition, and should try to read from p1
			// in case there is something written to overlapping p1
			tkO.MustContainErrMsg(`insert into t values (1,1)`, "[table:1526]Table has no partition for value matching a partition being dropped, 'p0'")
			tkNO.MustContainErrMsg(`insert into t values (1,1)`, "[kv:1062]Duplicate entry '1' for key 't.PRIMARY'")
			tkO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.PRIMARY'")
			tkNO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.PRIMARY'")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "101 101", "102 102", "2 2"))
			tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("101 101", "102 102"))
		case "delete only":
			// tkNO see non-readable/non-writable p0 partition, and should try to read from p1
			// in case there is something written to overlapping p1
			// tkO is not aware of p0.
			tkO.MustExec(`insert into t values (1,2)`)
			tkNO.MustContainErrMsg(`insert into t values (1,2)`, "[table:1526]Table has no partition for value matching a partition being dropped, 'p0'")
			tkO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.PRIMARY'")
			tkNO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.PRIMARY'")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 2", "101 101", "102 102"))
			// Original row should not be seen in StateWriteOnly
			tkNO.MustQuery(`select * from t partition (p0)`).Sort().Check(testkit.Rows())
			tkNO.MustContainErrMsg(`select * from t partition (pNonExisting)`, "[table:1735]Unknown partition 'pnonexisting' in table 't'")
			tkNO.MustQuery(`select * from t partition (p1)`).Sort().Check(testkit.Rows("1 2", "101 101", "102 102"))
			tkNO.MustQuery(`select * from t where a < 1000`).Sort().Check(testkit.Rows("1 2", "101 101", "102 102"))
			tkNO.MustQuery(`select * from t where a > 0`).Sort().Check(testkit.Rows("1 2", "101 101", "102 102"))
			tkNO.MustQuery(`select * from t where a = 1`).Sort().Check(testkit.Rows("1 2"))
			tkNO.MustQuery(`select * from t where a = 1 or a = 2 or a = 3`).Sort().Check(testkit.Rows("1 2"))
			tkNO.MustQuery(`select * from t where a in (1,2,3)`).Sort().Check(testkit.Rows("1 2"))
			tkNO.MustQuery(`select * from t where a < 100`).Sort().Check(testkit.Rows("1 2"))

			tkNO.MustQuery(`select * from t where b = 2`).Sort().Check(testkit.Rows("1 2"))
			// TODO: Test update and delete!
			// TODO: test key, hash and list partition without default partition :)
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY RANGE (`a`)\n" +
				"(PARTITION `p0` VALUES LESS THAN (100),\n" +
				" PARTITION `p1` VALUES LESS THAN (200))"))
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY RANGE (`a`)\n" +
				"(PARTITION `p1` VALUES LESS THAN (200))"))
		case "delete reorganization":
			// just to not fail :)
		case "none":
			// just to not fail :)
		default:
			require.Failf(t, "unhandled schema state '%s'", schemaState)
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, func(kit *testkit.TestKit) {}, loopFn)
}

func TestMultiSchemaDropListDefaultPartition(t *testing.T) {
	createSQL := `create table t (a int primary key, b varchar(255)) partition by list (a) (partition p0 values in (1,2,3), partition p1 values in (100,101,102,DEFAULT))`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1),(2,2),(101,101),(102,102)`)
	}
	alterSQL := `alter table t drop partition p0`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		// TODO: Test both static and dynamic partition pruning!
		switch schemaState {
		case "write only":
			// tkNO are unaware of the DDL
			// tkO see non-readable/non-writable p0 partition, and should try to read from p1
			// in case there is something written to overlapping p1
			tkO.MustContainErrMsg(`insert into t values (1,1)`, "[table:1526]Table has no partition for value matching a partition being dropped, 'p0'")
			tkNO.MustContainErrMsg(`insert into t values (1,1)`, "[kv:1062]Duplicate entry '1' for key 't.PRIMARY'")
			tkO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.PRIMARY'")
			tkNO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.PRIMARY'")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "101 101", "102 102", "2 2"))
			tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("101 101", "102 102"))
		case "delete only":
			// tkNO see non-readable/non-writable p0 partition, and should try to read from p1
			// in case there is something written to overlapping p1
			// tkO is not aware of p0.
			tkO.MustExec(`insert into t values (1,2)`)
			tkNO.MustContainErrMsg(`insert into t values (1,2)`, "[table:1526]Table has no partition for value matching a partition being dropped, 'p0'")
			tkO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.PRIMARY'")
			tkNO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.PRIMARY'")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 2", "101 101", "102 102"))
			// Original row should not be seen in StateWriteOnly
			tkNO.MustQuery(`select * from t partition (p0)`).Sort().Check(testkit.Rows())
			tkNO.MustContainErrMsg(`select * from t partition (pNonExisting)`, "[table:1735]Unknown partition 'pnonexisting' in table 't'")
			tkNO.MustQuery(`select * from t partition (p1)`).Sort().Check(testkit.Rows("1 2", "101 101", "102 102"))
			tkNO.MustQuery(`select * from t where a < 1000`).Sort().Check(testkit.Rows("1 2", "101 101", "102 102"))
			tkNO.MustQuery(`select * from t where a > 0`).Sort().Check(testkit.Rows("1 2", "101 101", "102 102"))
			tkNO.MustQuery(`select * from t where a = 1`).Sort().Check(testkit.Rows("1 2"))
			tkNO.MustQuery(`select * from t where a = 1 or a = 2 or a = 3`).Sort().Check(testkit.Rows("1 2"))
			tkNO.MustQuery(`select * from t where a in (1,2,3)`).Sort().Check(testkit.Rows("1 2"))
			tkNO.MustQuery(`select * from t where a < 100`).Sort().Check(testkit.Rows("1 2"))

			tkNO.MustQuery(`select * from t where b = 2`).Sort().Check(testkit.Rows("1 2"))
			// TODO: Test update and delete!
			// TODO: test key, hash and list partition without default partition :)
			// Should we see the partition or not?!?
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY LIST (`a`)\n" +
				"(PARTITION `p0` VALUES IN (1,2,3),\n" +
				" PARTITION `p1` VALUES IN (100,101,102,DEFAULT))"))
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY LIST (`a`)\n" +
				"(PARTITION `p1` VALUES IN (100,101,102,DEFAULT))"))
		case "delete reorganization":
			// just to not fail :)
		case "none":
			// just to not fail :)
		default:
			require.Failf(t, "unhandled schema state '%s'", schemaState)
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, func(kit *testkit.TestKit) {}, loopFn)
}

func TestMultiSchemaDropListColumnsDefaultPartition(t *testing.T) {
	createSQL := `create table t (a int, b varchar(255), c varchar (255), primary key (a,b)) partition by list columns (a,b) (partition p0 values in ((1,"1"),(2,"2"),(3,"3")), partition p1 values in ((100,"100"),(101,"101"),(102,"102"),DEFAULT))`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1,1),(2,2,2),(101,101,101),(102,102,102)`)
	}
	alterSQL := `alter table t drop partition p0`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		// TODO: Test both static and dynamic partition pruning!
		switch schemaState {
		case "write only":
			// tkNO are unaware of the DDL
			// tkO see non-readable/non-writable p0 partition, and should try to read from p1
			// in case there is something written to overlapping p1
			tkO.MustContainErrMsg(`insert into t values (1,1,1)`, "[table:1526]Table has no partition for value matching a partition being dropped, 'p0'")
			tkNO.MustContainErrMsg(`insert into t values (1,1,1)`, "[kv:1062]Duplicate entry '1-1' for key 't.PRIMARY'")
			tkO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101-101' for key 't.PRIMARY'")
			tkNO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101-101' for key 't.PRIMARY'")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1 1", "101 101 101", "102 102 102", "2 2 2"))
			tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("101 101 101", "102 102 102"))
		case "delete only":
			// tkNO see non-readable/non-writable p0 partition, and should try to read from p1
			// in case there is something written to overlapping p1
			// tkO is not aware of p0.
			tkO.MustExec(`insert into t values (1,1,2)`)
			tkNO.MustContainErrMsg(`insert into t values (1,1,2)`, "[table:1526]Table has no partition for value matching a partition being dropped, 'p0'")
			tkO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101-101' for key 't.PRIMARY'")
			tkNO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101-101' for key 't.PRIMARY'")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1 2", "101 101 101", "102 102 102"))
			// Original row should not be seen in StateWriteOnly
			tkNO.MustQuery(`select * from t partition (p0)`).Sort().Check(testkit.Rows())
			tkNO.MustContainErrMsg(`select * from t partition (pNonExisting)`, "[table:1735]Unknown partition 'pnonexisting' in table 't'")
			tkNO.MustQuery(`select * from t partition (p1)`).Sort().Check(testkit.Rows("1 1 2", "101 101 101", "102 102 102"))
			tkNO.MustQuery(`select * from t where a < 1000`).Sort().Check(testkit.Rows("1 1 2", "101 101 101", "102 102 102"))
			tkNO.MustQuery(`select * from t where a > 0`).Sort().Check(testkit.Rows("1 1 2", "101 101 101", "102 102 102"))
			tkNO.MustQuery(`select * from t where a = 1`).Sort().Check(testkit.Rows("1 1 2"))
			tkNO.MustQuery(`select * from t where a = 1 or a = 2 or a = 3`).Sort().Check(testkit.Rows("1 1 2"))
			tkNO.MustQuery(`select * from t where a in (1,2,3) or b in ("1","2")`).Sort().Check(testkit.Rows("1 1 2"))
			tkNO.MustQuery(`select * from t where a in (1,2,3)`).Sort().Check(testkit.Rows("1 1 2"))
			tkNO.MustQuery(`select * from t where a < 100`).Sort().Check(testkit.Rows("1 1 2"))

			tkNO.MustQuery(`select * from t where c = "2"`).Sort().Check(testkit.Rows("1 1 2"))
			tkNO.MustQuery(`select * from t where b = "1"`).Sort().Check(testkit.Rows("1 1 2"))
			// TODO: Test update and delete!
			// TODO: test key, hash and list partition without default partition :)
			// Should we see the partition or not?!?
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) NOT NULL,\n" +
				"  `c` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`,`b`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY LIST COLUMNS(`a`,`b`)\n" +
				"(PARTITION `p0` VALUES IN ((1,'1'),(2,'2'),(3,'3')),\n" +
				" PARTITION `p1` VALUES IN ((100,'100'),(101,'101'),(102,'102'),DEFAULT))"))
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) NOT NULL,\n" +
				"  `c` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`,`b`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY LIST COLUMNS(`a`,`b`)\n" +
				"(PARTITION `p1` VALUES IN ((100,'100'),(101,'101'),(102,'102'),DEFAULT))"))
		case "delete reorganization":
			// just to not fail :)
		case "none":
			// just to not fail :)
		default:
			require.Failf(t, "unhandled schema state '%s'", schemaState)
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, func(kit *testkit.TestKit) {}, loopFn)
}



// TestMultiSchemaDropUniqueIndex to show behavior when
// dropping a unique index
func TestMultiSchemaDropUniqueIndex(t *testing.T) {
	createSQL := `create table t (a int primary key, b varchar(255), unique key uk_b (b))`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9)`)
	}
	alterSQL := `alter table t drop index uk_b`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		switch schemaState {
		case "write only":
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `uk_b` (`b`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
			tkO.MustContainErrMsg(`insert into t values (10,1)`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			tkNO.MustContainErrMsg(`insert into t values (10,1)`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
		case "delete only":
			// Delete only from the uk_b unique index, cannot have errors
			tkO.MustExec(`insert into t values (10,1)`)
			tkO.MustExec(`insert into t values (11,11)`)
			tkO.MustExec(`delete from t where a = 2`)
			// Write only for uk_b, we cannot find anything through the index or read from the index, but still gives duplicate keys on insert/updates
			// So we already have two duplicates of b = 1, but only one in the unique index uk_a, so here we cannot insert any.
			tkNO.MustContainErrMsg(`insert into t values (12,1)`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			tkNO.MustContainErrMsg(`update t set b = 1 where a = 9`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			// Deleted from the index!
			tkNO.MustExec(`insert into t values (13,2)`)
			tkNO.MustContainErrMsg(`insert into t values (14,3)`, "[kv:1062]Duplicate entry '3' for key 't.uk_b'")
			// b = 11 never written to the index!
			tkNO.MustExec(`insert into t values (15,11)`)
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		case "delete reorganization":
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		case "none":
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		default:
			require.Failf(t, "unhandled schema state '%s'", schemaState)
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, func(kit *testkit.TestKit) {}, loopFn)
}

func runMultiSchemaTest(t *testing.T, createSQL, alterSQL string, initFn, postFn func(*testkit.TestKit), loopFn func(tO, tNO *testkit.TestKit)) {
	// During manual debug, increase this time a lot so it does not reload the tables in the domain
	// due to lease time :)
	//distCtx := testkit.NewDistExecutionContextWithLease(t, 2, 15*time.Second)
	distCtx := testkit.NewDistExecutionContextWithLease(t, 2, 15*time.Hour)
	store := distCtx.Store
	domOwner := distCtx.GetDomain(0)
	domNonOwner := distCtx.GetDomain(1)
	defer func() {
		domOwner.Close()
		domNonOwner.Close()
		store.Close()
	}()

	if !domOwner.DDL().OwnerManager().IsOwner() {
		domOwner, domNonOwner = domNonOwner, domOwner
	}

	seOwner, err := session.CreateSessionWithDomain(store, domOwner)
	require.NoError(t, err)
	seNonOwner, err := session.CreateSessionWithDomain(store, domNonOwner)
	require.NoError(t, err)

	tkDDLOwner := testkit.NewTestKitWithSession(t, store, seOwner)
	tkDDLOwner.MustExec(`use test`)
	tkDDLOwner.MustExec(`set @@global.tidb_enable_global_index = 1`)
	tkDDLOwner.MustExec(`set @@session.tidb_enable_global_index = 1`)
	tkO := testkit.NewTestKitWithSession(t, store, seOwner)
	tkO.MustExec(`use test`)
	tkNO := testkit.NewTestKitWithSession(t, store, seNonOwner)
	tkNO.MustExec(`use test`)

	tkDDLOwner.MustExec(createSQL)
	domOwner.Reload()
	domNonOwner.Reload()
	initFn(tkO)
	verStart := domNonOwner.InfoSchema().SchemaMetaVersion()
	hookChan := make(chan struct{})
	hookFunc := func(job *model.Job) {
		hookChan <- struct{}{}
		logutil.BgLogger().Info("XXXXXXXXXXX Hook now waiting", zap.String("job.State", job.State.String()), zap.String("job.SchemaStage", job.SchemaState.String()))
		<-hookChan
		logutil.BgLogger().Info("XXXXXXXXXXX Hook released", zap.String("job.State", job.State.String()), zap.String("job.SchemaStage", job.SchemaState.String()))
	}
	failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", hookFunc)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/onJobRunAfter")
	alterChan := make(chan error)
	go func() {
		err := tkDDLOwner.ExecToErr(alterSQL)
		logutil.BgLogger().Info("XXXXXXXXXXX DDL done!", zap.String("alterSQL", alterSQL))
		alterChan <- err
	}()
	// Skip the first state, since we want to compare before vs after in the loop
	<-hookChan
	hookChan <- struct{}{}
	verCurr := verStart + 1
	i := 0
	for {
		// Waiting for the next State change to be done (i.e. blocking the state after)
		releaseHook := true
		for {
			select {
			case <-hookChan:
			case err := <-alterChan:
				require.NoError(t, err)
				releaseHook = false
				logutil.BgLogger().Info("XXXXXXXXXXX release hook")
				break
			}
			domOwner.Reload()
			if domNonOwner.InfoSchema().SchemaMetaVersion() == domOwner.InfoSchema().SchemaMetaVersion() {
				// looping over reorganize data/indexes
				hookChan <- struct{}{}
				continue
			}
			break
		}
		logutil.BgLogger().Info("XXXXXXXXXXX states loop", zap.Int64("verCurr", verCurr), zap.Int64("NonOwner ver", domNonOwner.InfoSchema().SchemaMetaVersion()), zap.Int64("Owner ver", domOwner.InfoSchema().SchemaMetaVersion()))
		domOwner.Reload()
		require.Equal(t, verCurr-1, domNonOwner.InfoSchema().SchemaMetaVersion())
		require.Equal(t, verCurr, domOwner.InfoSchema().SchemaMetaVersion())
		loopFn(tkO, tkNO)
		domNonOwner.Reload()
		verCurr++
		i++
		if !releaseHook {
			// Alter done!
			break
		}
		// Continue to next state
		hookChan <- struct{}{}
	}
	logutil.BgLogger().Info("XXXXXXXXXXX states loop done")
	postFn(tkO)
}

// TestMultiSchemaTruncatePartitionWithGlobalIndex to show behavior when
// truncating a partition with a global index
func TestMultiSchemaTruncatePartitionWithGlobalIndex(t *testing.T) {
	// TODO: Also test non-int PK, multi-column PK
	createSQL := `create table t (a int primary key, b varchar(255), c varchar(255) default 'Filler', unique key uk_b (b) global) partition by hash (a) partitions 2`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t (a,b) values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7)`)
	}
	alterSQL := `alter table t truncate partition p1`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		logutil.BgLogger().Info("XXXXXXXXXXX loopFn", zap.String("schemaState", schemaState))
		switch schemaState {
		case "write only":
			// tkNO is seeing state None, so unaware of DDL
			// tkO is seeing state write only, so using the old partition,
			// but are aware of new ids, so should filter them or global index reads
			// Duplicate keys (from delete only state) are allowed on insert/update,
			// even if it cannot read them from the global index, due to filtering.
			rows := tkNO.MustQuery(`select * from t`).Sort().Rows()
			tkO.MustQuery(`select * from t`).Sort().Check(rows)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateNone, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateWriteOnly, tblO.Partition.DDLState)
		case "delete only":
			// tkNO is seeing state write only, so still can access the dropped partition
			// tkO is seeing state delete only, so cannot see the dropped partition,
			// but must still double write to the global indexes.

			tkNO.MustContainErrMsg(`insert into t values (1,1,"Duplicate key")`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			tkO.MustContainErrMsg(`insert into t values (1,1,"Duplicate key")`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateWriteOnly, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteOnly, tblO.Partition.DDLState)

			tkNO.MustExec(`insert into t values (21,21,"OK")`)
			tkNO.MustExec(`insert into t values (23,23,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (21,21,"Duplicate key")`, "[kv:1062]Duplicate entry '21' for key 't.uk_b'")
			tkO.MustContainErrMsg(`insert into t values (6,23,"Duplicate key")`, "[kv:1062]Duplicate entry '23' for key 't.uk_b'")
			// Primary is not global, so here we can insert into the new partition, without
			// conflicting to the old one
			tkO.MustExec(`insert into t values (21,25,"OK")`)
			tkNO.MustContainErrMsg(`insert into t values (8,25,"Duplicate key")`, "[kv:1062]Duplicate entry '25' for key 't.uk_b'")
			// type differences, cannot use index
			tkNO.MustQuery(`select count(*) from t where b = 25`).Check(testkit.Rows("0"))
			tkNO.MustQuery(`select b from t where b = 25`).Check(testkit.Rows())
			// PointGet should not find new partitions for StateWriteOnly
			tkNO.MustQuery(`select count(*) from t where b = "25"`).Check(testkit.Rows("0"))
			tkNO.MustQuery(`select b from t where b = "25"`).Check(testkit.Rows())
			// TODO: Also set column c to something useful
			tkNO.MustExec(`update t set a = 2 where b = "25"`)
			require.Equal(t, uint64(0), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
			// TODO: Somehow tkNO gets the updated tableInfo :(
			// track it down some how...
			// WASHERE ^^^^
			// But does not happen every time :(
			// So first try to make it more reproducible...
			// Probably due to some internal or lease timeout reloading the domain. Could be checked
			// with schema version?
			// TODO: Also set column c to something useful
			tkNO.MustExec(`update t set a = 2 where b = "25"`)
			require.Equal(t, uint64(0), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
			//tkNO.MustContainErrMsg(`update t set a = 2 where b = "25"`, "[kv:1062]Duplicate entry '2' for key 't.PRIMARY'")
			// Primary is not global, so here we can insert into the old partition, without
			// conflicting to the new one
			tkNO.MustExec(`insert into t values (25,27,"OK")`)

			tkO.MustQuery(`select count(*) from t where b = "23"`).Check(testkit.Rows("0"))
			// TODO: Also set column c to something useful
			tkO.MustExec(`update t set a = 2 where b = "23"`)
			require.Equal(t, uint64(0), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
			tkNO.MustQuery(`select count(*) from t where a = 23`).Check(testkit.Rows("1"))
			tkNO.MustQuery(`select * from t where a = 23`).Check(testkit.Rows("23 23 OK"))
			tkNO.MustExec(`update t set b = 10 where a = 23`)
			require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
			tkNO.MustExec(`update t set b = 23 where a = 23`)
			require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
			tkNO.MustContainErrMsg(`update t set b = 25 where a = 23`, "[kv:1062]Duplicate entry '25' for key 't.uk_b'")
			tkO.MustExec(`update t set b = 23 where a = 25`)
			require.Equal(t, uint64(0), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
			tkO.MustContainErrMsg(`update t set b = 21 where a = 21`, "[kv:1062]Duplicate entry '21' for key 't.uk_b'")
			tkO.MustContainErrMsg(`update t set b = 23 where b = "25"`, "[kv:1062]Duplicate entry '23' for key 't.uk_b'")

			tkO.MustExec(`update t set b = 29 where a = 21`)
			require.Equal(t, uint64(1), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
			tkNO.MustExec(`update t set b = 25 where b = "27"`)
			require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
			tkO.MustExec(`update t set b = 27, a = 27 where b = "29"`)
			require.Equal(t, uint64(1), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())

			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows(""+
				"1 1 Filler",
				"2 2 Filler",
				"21 21 OK",
				"23 23 OK",
				"25 25 OK",
				"3 3 Filler",
				"4 4 Filler",
				"5 5 Filler",
				"6 6 Filler",
				"7 7 Filler"))

			tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows(""+
				"2 2 Filler",
				"27 27 OK",
				"4 4 Filler",
				"6 6 Filler"))
			// TODO: Add tests for delete as well as index lookup and table scan!
		case "delete reorganization":
			// tkNO is seeing state delete only, so cannot see the dropped partition,
			// but must still double write to the global indexes.
			// tkO is seeing state delete reorganization, so cannot see the dropped partition,
			// and should ignore the dropped partitions entries in the Global Indexes!
			rows := tkO.MustQuery(`select * from t`).Sort().Rows()
			tkNO.MustQuery(`select * from t`).Sort().Check(rows)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteOnly, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteReorganization, tblO.Partition.DDLState)
			tkO.MustQuery(`select b from t where b = "1"`).Check(testkit.Rows())
			tkO.MustExec(`insert into t values (1,1,"OK")`)
			tkO.MustQuery(`select b from t where b = "1"`).Check(testkit.Rows("1"))
			tkO.MustQuery(`select b from t where b = 1`).Check(testkit.Rows("1"))
			tkO.MustContainErrMsg(`insert into t values (3,1,"Duplicate")`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			// b = 23 was inserted into the dropped partition, OK to delete
			// TODO: WASHERE!!! Seems like the index thing has blocked the insert fix...
			tkO.MustExec(`insert into t values (10,23,"OK")`)
			tkNO.MustExec(`insert into t values (41,41,"OK")`)
			tkNO.MustContainErrMsg(`insert into t values (12,25,"Duplicate key")`, "[kv:1062]Duplicate entry '25' for key 't.uk_b'")
			tkNO.MustContainErrMsg(`insert into t values (25,25,"Duplicate key")`, "[kv:1062]Duplicate entry '25' for key 't.uk_b'")
			tkNO.MustContainErrMsg(`insert into t values (41,27,"Duplicate key")`, "[kv:1062]Duplicate entry '27' for key 't.uk_b'")
			tkO.MustExec(`insert into t values (43,43,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (44,43,"Duplicate key")`, "[kv:1062]Duplicate entry '43' for key 't.uk_b'")
			tkNO.MustContainErrMsg(`update t set b = 5 where a = 41`, "[kv:1062]Duplicate entry '5' for key 't.uk_b'")
			tkNO.MustExec(`update t set a = 5 where b = "41"`)
			require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
			tkO.MustExec(`update t set a = 7 where b = "43"`)
			require.Equal(t, uint64(1), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
			// This should be silently deleted / overwritten
			tkO.MustExec(`update t set b = 5 where b = "43"`)
			require.Equal(t, uint64(1), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
			tkO.MustExec(`update t set b = 3 where b = 41`)
			require.Equal(t, uint64(1), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
			rows = tkNO.MustQuery(`select * from t`).Sort().Rows()
			tkO.MustQuery(`select * from t`).Sort().Check(rows)
		case "none":
			tkNO.MustExec(`insert into t values (81,81,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (81,81,"Duplicate key")`, "[kv:1062]Duplicate entry '81' for key 't.uk_b'")
			tkNO.MustExec(`insert into t values (85,85,"OK")`)
			tkO.MustExec(`insert into t values (87,87,"OK")`)
			rows := tkNO.MustQuery(`select * from t`).Sort().Rows()
			tkO.MustQuery(`select * from t`).Sort().Check(rows)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteReorganization, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateNone, tblO.Partition.DDLState)
		default:
			require.Failf(t, "unhandled schema state '%s'", schemaState)
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, func(kit *testkit.TestKit) {}, loopFn)
}

func TestMultiSchemaTruncatePartitionWithPKGlobal(t *testing.T) {
	// TODO: Also test non-int PK, multi-column PK
	createSQL := `create table t (a int primary key nonclustered global, b int, c varchar(255) default 'Filler', unique key uk_b (b)) partition by hash (b) partitions 2`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t (a,b) values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7)`)
	}
	alterSQL := `alter table t truncate partition p0`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		switch schemaState {
		case "write only":
			// tkNO is seeing state None, so unaware of DDL
			// tkO is seeing state write only, so using the old partition,
			// but are aware of new ids, so should filter them or global index reads
			// Duplicate keys (from delete only state) are allowed on insert/update,
			// even if it cannot read them from the global index, due to filtering.
			rows := tkNO.MustQuery(`select * from t`).Sort().Rows()
			tkO.MustQuery(`select * from t`).Sort().Check(rows)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateNone, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateWriteOnly, tblO.Partition.DDLState)
		case "delete only":
			// tkNO is seeing state write only, so still can access the dropped partition
			// tkO is seeing state delete only, so cannot see the dropped partition,
			// but must still double write to the global indexes.

			// What do we want to test?
			// tkNO, tkO should not be able to insert or update any PK that conflicts with any previous
			// transactions

			tkNO.MustContainErrMsg(`insert into t values (2,2,"Duplicate key")`, "[kv:1062]Duplicate entry '2' for key 't.PRIMARY'")
			tkNO.MustContainErrMsg(`insert into t values (10,2,"Duplicate key")`, "[kv:1062]Duplicate entry '2' for key 't.uk_b'")

			tkO.MustQuery(`select a from t where a = 2`).Check(testkit.Rows())
			// OK! PK violation due to old partition is still accessible!!!
			// Similar to when dropping a unique index, see TestMultiSchemaDropUniqueIndex
			tkO.MustContainErrMsg(`insert into t values (2,2,"Duplicate key")`, "[kv:1062]Duplicate entry '2' for key 't.PRIMARY'")

			// Note that PK (global) is not violated! and Unique key (b) is not global!
			tkO.MustExec(`insert into t values (10,2,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (10,10,"Duplicate key")`, "[kv:1062]Duplicate entry '10' for key 't.PRIMARY'")
			tkNO.MustContainErrMsg(`insert into t values (10,10,"Duplicate key")`, "[kv:1062]Duplicate entry '10' for key 't.PRIMARY'")

			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateWriteOnly, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteOnly, tblO.Partition.DDLState)

			/*
				tkNO.MustExec(`insert into t values (21,21,"OK")`)
				tkNO.MustExec(`insert into t values (23,23,"OK")`)
				tkO.MustContainErrMsg(`insert into t values (21,21,"Duplicate key")`, "[kv:1062]Duplicate entry '21' for key 't.uk_b'")
				tkO.MustContainErrMsg(`insert into t values (6,23,"Duplicate key")`, "[kv:1062]Duplicate entry '23' for key 't.uk_b'")
				// Primary is not global, so here we can insert into the new partition, without
				// conflicting to the old one
				tkO.MustExec(`insert into t values (21,25,"OK")`)
				tkNO.MustContainErrMsg(`insert into t values (8,25,"Duplicate key")`, "[kv:1062]Duplicate entry '25' for key 't.uk_b'")
				// PointGet should not find new partitions for StateWriteOnly
				tkNO.MustQuery(`select count(*) from t where b = "25"`).Check(testkit.Rows("0"))
				tkNO.MustQuery(`select b from t where b = "25"`).Check(testkit.Rows())
				// TODO: Also set column c to something useful
				tkNO.MustExec(`update t set a = 2 where b = "25"`)
				require.Equal(t, uint64(0), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
				// TODO: Somehow tkNO gets the updated tableInfo :(
				// track it down some how...
				// WASHERE ^^^^
				// But does not happen every time :(
				// So first try to make it more reproducible...
				// Probably due to some internal or lease timeout reloading the domain. Could be checked
				// with schema version?
				// TODO: Also set column c to something useful
				tkNO.MustExec(`update t set a = 2 where b = "25"`)
				require.Equal(t, uint64(0), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
				//tkNO.MustContainErrMsg(`update t set a = 2 where b = "25"`, "[kv:1062]Duplicate entry '2' for key 't.PRIMARY'")
				// Primary is not global, so here we can insert into the old partition, without
				// conflicting to the new one
				tkNO.MustExec(`insert into t values (25,27,"OK")`)

				tkO.MustQuery(`select count(*) from t where b = "23"`).Check(testkit.Rows("0"))
				// TODO: Also set column c to something useful
				tkO.MustExec(`update t set a = 2 where b = "23"`)
				require.Equal(t, uint64(0), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
				tkNO.MustQuery(`select count(*) from t where a = 23`).Check(testkit.Rows("1"))
				tkNO.MustQuery(`select * from t where a = 23`).Check(testkit.Rows("23 23 OK"))
				tkNO.MustExec(`update t set b = 10 where a = 23`)
				require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
				tkNO.MustExec(`update t set b = 23 where a = 23`)
				require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
				tkNO.MustContainErrMsg(`update t set b = 25 where a = 23`, "[kv:1062]Duplicate entry '25' for key 't.uk_b'")
				tkO.MustExec(`update t set b = 23 where a = 25`)
				require.Equal(t, uint64(0), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
				tkO.MustContainErrMsg(`update t set b = 21 where a = 21`, "[kv:1062]Duplicate entry '21' for key 't.uk_b'")
				tkO.MustContainErrMsg(`update t set b = 23 where b = "25"`, "[kv:1062]Duplicate entry '23' for key 't.uk_b'")

				tkO.MustExec(`update t set b = 29 where a = 21`)
				require.Equal(t, uint64(1), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
				tkNO.MustExec(`update t set b = 25 where b = "27"`)
				require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
				tkO.MustExec(`update t set b = 27, a = 27 where b = "29"`)
				require.Equal(t, uint64(1), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())

				tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows(""+
					"1 1 Filler",
					"2 2 Filler",
					"21 21 OK",
					"23 23 OK",
					"25 25 OK",
					"3 3 Filler",
					"4 4 Filler",
					"5 5 Filler",
					"6 6 Filler",
					"7 7 Filler"))

				tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows(""+
					"2 2 Filler",
					"27 27 OK",
					"4 4 Filler",
					"6 6 Filler"))
				// TODO: Add tests for delete as well as index lookup and table scan!

			*/
		case "delete reorganization":
			// tkNO is seeing state delete only, so cannot see the dropped partition,
			// but must still double write to the global indexes.
			// tkO is seeing state delete reorganization, so cannot see the dropped partition,
			// and should ignore the dropped partitions entries in the Global Indexes!
			rows := tkO.MustQuery(`select * from t`).Sort().Rows()
			tkNO.MustQuery(`select * from t`).Sort().Check(rows)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteOnly, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteReorganization, tblO.Partition.DDLState)
			/*
				tkO.MustQuery(`select b from t where b = "1"`).Check(testkit.Rows())
				tkO.MustExec(`insert into t values (1,1,"OK")`)
				tkO.MustQuery(`select b from t where b = "1"`).Check(testkit.Rows("1"))
				tkO.MustQuery(`select b from t where b = 1`).Check(testkit.Rows("1"))
				tkO.MustContainErrMsg(`insert into t values (3,1,"Duplicate")`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
				// b = 23 was inserted into the dropped partition, OK to delete
				// TODO: WASHERE!!! Seems like the index thing has blocked the insert fix...
				tkO.MustExec(`insert into t values (10,23,"OK")`)
				tkNO.MustExec(`insert into t values (41,41,"OK")`)
				tkNO.MustContainErrMsg(`insert into t values (12,25,"Duplicate key")`, "[kv:1062]Duplicate entry '25' for key 't.uk_b'")
				tkNO.MustContainErrMsg(`insert into t values (25,25,"Duplicate key")`, "[kv:1062]Duplicate entry '25' for key 't.uk_b'")
				tkNO.MustContainErrMsg(`insert into t values (41,27,"Duplicate key")`, "[kv:1062]Duplicate entry '27' for key 't.uk_b'")
				tkO.MustExec(`insert into t values (43,43,"OK")`)
				tkO.MustContainErrMsg(`insert into t values (44,43,"Duplicate key")`, "[kv:1062]Duplicate entry '43' for key 't.uk_b'")
				tkNO.MustContainErrMsg(`update t set b = 5 where a = 41`, "[kv:1062]Duplicate entry '5' for key 't.uk_b'")
				tkNO.MustExec(`update t set a = 5 where b = "41"`)
				require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
				tkO.MustExec(`update t set a = 7 where b = "43"`)
				require.Equal(t, uint64(1), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
				// This should be silently deleted / overwritten
				tkO.MustExec(`update t set b = 5 where b = "43"`)
				require.Equal(t, uint64(1), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
				tkO.MustExec(`update t set b = 3 where b = 41`)
				require.Equal(t, uint64(1), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
				rows = tkNO.MustQuery(`select * from t`).Sort().Rows()
				tkO.MustQuery(`select * from t`).Sort().Check(rows)
			*/
		case "none":
		/*
			tkNO.MustExec(`insert into t values (81,81,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (81,81,"Duplicate key")`, "[kv:1062]Duplicate entry '81' for key 't.uk_b'")
			tkNO.MustExec(`insert into t values (85,85,"OK")`)
			tkO.MustExec(`insert into t values (87,87,"OK")`)
			rows := tkNO.MustQuery(`select * from t`).Sort().Rows()
			tkO.MustQuery(`select * from t`).Sort().Check(rows)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteReorganization, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(parserModel.NewCIStr("test"), parserModel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateNone, tblO.Partition.DDLState)

		*/
		default:
			require.Fail(t, "Unhandled schema state", "State: '%s'", schemaState)
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, func(kit *testkit.TestKit) {}, loopFn)
}
