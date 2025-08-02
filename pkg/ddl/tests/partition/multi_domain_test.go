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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

<<<<<<< HEAD
=======
func TestMultiSchemaReorganizePartitionIssue56819(t *testing.T) {
	createSQL := `create table t (a int primary key, b varchar(255), unique index idx_b_global (b) global) partition by range (a) (partition p1 values less than (200), partition pMax values less than (maxvalue))`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1),(2,2)`)
	}
	alterSQL := `alter table t reorganize partition p1 into (partition p0 values less than (100), partition p1 values less than (200))`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		switch schemaState {
		case model.StateDeleteOnly.String():
			tkNO.MustExec(`insert into t values (4,4)`)
			tkNO.MustQuery(`select * from t where b = "4"`).Sort().Check(testkit.Rows("4 4"))
			tkO.MustQuery(`select * from t where b = "4"`).Sort().Check(testkit.Rows("4 4"))
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn, false)
}

func TestMultiSchemaDropRangePartition(t *testing.T) {
	createSQL := `create table t (a int primary key, b varchar(255), unique key (b) global, unique key (b,a) global, unique key (b,a)) partition by range (a) (partition p0 values less than (100), partition p1 values less than (200))`
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
			tkNO.MustContainErrMsg(`insert into t values (1,1)`, "[kv:1062]Duplicate entry '1' for key 't.")
			tkO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.")
			tkNO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "101 101", "102 102", "2 2"))
			tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("101 101", "102 102"))
		case "delete only":
			// tkNO see non-readable/non-writable p0 partition, and should try to read from p1
			// in case there is something written to overlapping p1
			// tkO is not aware of p0.
			tkO.MustExec(`insert into t values (1,20)`)
			tkNO.MustContainErrMsg(`insert into t values (1,20)`, "[table:1526]Table has no partition for value matching a partition being dropped, 'p0'")
			tkO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.")
			tkNO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 20", "101 101", "102 102"))
			// Original row should not be seen in StateWriteOnly
			tkNO.MustQuery(`select * from t partition (p0)`).Sort().Check(testkit.Rows())
			tkNO.MustContainErrMsg(`select * from t partition (pNonExisting)`, "[table:1735]Unknown partition 'pnonexisting' in table 't'")
			tkNO.MustQuery(`select * from t partition (p1)`).Sort().Check(testkit.Rows("1 20", "101 101", "102 102"))
			tkNO.MustQuery(`select * from t where a < 1000`).Sort().Check(testkit.Rows("1 20", "101 101", "102 102"))
			tkNO.MustQuery(`select * from t where a > 0`).Sort().Check(testkit.Rows("1 20", "101 101", "102 102"))
			tkNO.MustQuery(`select * from t where a = 1`).Sort().Check(testkit.Rows("1 20"))
			tkNO.MustQuery(`select * from t where a = 1 or a = 2 or a = 3`).Sort().Check(testkit.Rows("1 20"))
			tkNO.MustQuery(`select * from t where a in (1,2,3)`).Sort().Check(testkit.Rows("1 20"))
			tkNO.MustQuery(`select * from t where a < 100`).Sort().Check(testkit.Rows("1 20"))

			tkNO.MustQuery(`select * from t where b = 20`).Sort().Check(testkit.Rows("1 20"))
			// TODO: Test update and delete!
			// TODO: test key, hash and list partition without default partition :)
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `b` (`b`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `b_2` (`b`,`a`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `b_3` (`b`,`a`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY RANGE (`a`)\n" +
				"(PARTITION `p0` VALUES LESS THAN (100),\n" +
				" PARTITION `p1` VALUES LESS THAN (200))"))
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `b` (`b`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `b_2` (`b`,`a`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `b_3` (`b`,`a`)\n" +
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
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn, false)
}

func TestMultiSchemaDropListDefaultPartition(t *testing.T) {
	createSQL := `create table t (a int primary key, b varchar(255), unique key (b) global, unique key (b,a) global, unique key (b,a)) partition by list (a) (partition p0 values in (1,2,3), partition p1 values in (100,101,102,DEFAULT))`
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
			tkNO.MustContainErrMsg(`insert into t values (1,1)`, "[kv:1062]Duplicate entry '1' for key 't.")
			tkO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.")
			tkNO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "101 101", "102 102", "2 2"))
			tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("101 101", "102 102"))
		case "delete only":
			// tkNO see non-readable/non-writable p0 partition, and should try to read from p1
			// in case there is something written to overlapping p1
			// tkO is not aware of p0.
			tkO.MustExec(`insert into t values (1,20)`)
			tkNO.MustContainErrMsg(`insert into t values (1,20)`, "[table:1526]Table has no partition for value matching a partition being dropped, 'p0'")
			tkO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.")
			tkNO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 20", "101 101", "102 102"))
			// Original row should not be seen in StateWriteOnly
			tkNO.MustQuery(`select * from t partition (p0)`).Sort().Check(testkit.Rows())
			tkNO.MustContainErrMsg(`select * from t partition (pNonExisting)`, "[table:1735]Unknown partition 'pnonexisting' in table 't'")
			tkNO.MustQuery(`select * from t partition (p1)`).Sort().Check(testkit.Rows("1 20", "101 101", "102 102"))
			tkNO.MustQuery(`select * from t where a < 1000`).Sort().Check(testkit.Rows("1 20", "101 101", "102 102"))
			tkNO.MustQuery(`select * from t where a > 0`).Sort().Check(testkit.Rows("1 20", "101 101", "102 102"))
			tkNO.MustQuery(`select * from t where a = 1`).Sort().Check(testkit.Rows("1 20"))
			tkNO.MustQuery(`select * from t where a = 1 or a = 2 or a = 3`).Sort().Check(testkit.Rows("1 20"))
			tkNO.MustQuery(`select * from t where a in (1,2,3)`).Sort().Check(testkit.Rows("1 20"))
			tkNO.MustQuery(`select * from t where a < 100`).Sort().Check(testkit.Rows("1 20"))

			tkNO.MustQuery(`select * from t where b = 20`).Sort().Check(testkit.Rows("1 20"))
			// TODO: Test update and delete!
			// TODO: test key, hash and list partition without default partition :)
			// Should we see the partition or not?!?
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `b` (`b`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `b_2` (`b`,`a`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `b_3` (`b`,`a`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY LIST (`a`)\n" +
				"(PARTITION `p0` VALUES IN (1,2,3),\n" +
				" PARTITION `p1` VALUES IN (100,101,102,DEFAULT))"))
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `b` (`b`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `b_2` (`b`,`a`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `b_3` (`b`,`a`)\n" +
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
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn, false)
}

func TestMultiSchemaDropListColumnsDefaultPartition(t *testing.T) {
	createSQL := `create table t (a int, b varchar(255), c varchar (255), primary key (a,b), unique key (a) global, unique key (b,a) global, unique key (c) global, unique key (b,a)) partition by list columns (a,b) (partition p0 values in ((1,"1"),(2,"2"),(3,"3")), partition p1 values in ((100,"100"),(101,"101"),(102,"102"),DEFAULT))`
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
			tkNO.MustContainErrMsg(`insert into t values (1,1,1)`, "[kv:1062]Duplicate entry '1' for key")
			tkO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101")
			tkNO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101' for key 't.a_2'")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1 1", "101 101 101", "102 102 102", "2 2 2"))
			tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("101 101 101", "102 102 102"))
			tkO.MustQuery(`select a from t where c = "2"`).Sort().Check(testkit.Rows())
			tkNO.MustQuery(`select a from t where c = "2"`).Sort().Check(testkit.Rows("2"))
		case "delete only":
			// tkNO see non-readable/non-writable p0 partition, and should try to read from p1
			// in case there is something written to overlapping p1
			// tkO is not aware of p0.
			tkO.MustExec(`insert into t values (3,3,3)`)
			tkO.MustContainErrMsg(`insert into t values (1,1,2)`, "[kv:1062]Duplicate entry '1' for key 't.a_2")
			tkNO.MustContainErrMsg(`insert into t values (3,3,3)`, "[table:1526]Table has no partition for value matching a partition being dropped, 'p0'")
			tkO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101' for key 't.a_2'")
			tkNO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101' for key 't.a_2'")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("101 101 101", "102 102 102", "3 3 3"))
			tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("101 101 101", "102 102 102", "3 3 3"))
			// Original row should not be seen in StateWriteOnly
			tkNO.MustQuery(`select * from t partition (p0)`).Sort().Check(testkit.Rows())
			tkNO.MustContainErrMsg(`select * from t partition (pNonExisting)`, "[table:1735]Unknown partition 'pnonexisting' in table 't'")
			tkNO.MustQuery(`select * from t partition (p1)`).Sort().Check(testkit.Rows("101 101 101", "102 102 102", "3 3 3"))
			tkNO.MustQuery(`select * from t where a < 1000`).Sort().Check(testkit.Rows("101 101 101", "102 102 102", "3 3 3"))
			tkNO.MustQuery(`select * from t where a > 0`).Sort().Check(testkit.Rows("101 101 101", "102 102 102", "3 3 3"))
			tkNO.MustQuery(`select * from t where a = 3`).Sort().Check(testkit.Rows("3 3 3"))
			tkNO.MustQuery(`select * from t where a = 1 or a = 2 or a = 3`).Sort().Check(testkit.Rows("3 3 3"))
			tkNO.MustQuery(`select * from t where a in (1,2,3) or b in ("1","2")`).Sort().Check(testkit.Rows("3 3 3"))
			tkNO.MustQuery(`select * from t where a in (1,2,3)`).Sort().Check(testkit.Rows("3 3 3"))
			tkNO.MustQuery(`select * from t where a < 100`).Sort().Check(testkit.Rows("3 3 3"))

			tkNO.MustQuery(`select * from t where c = "2"`).Sort().Check(testkit.Rows())
			tkNO.MustQuery(`select a from t where c = "2"`).Sort().Check(testkit.Rows())
			tkNO.MustQuery(`select * from t where b = "3"`).Sort().Check(testkit.Rows("3 3 3"))
			tkO.MustQuery(`select * from t where c = "2"`).Sort().Check(testkit.Rows())
			// TODO: Test update and delete!
			// TODO: test key, hash and list partition without default partition :)
			// Should we see the partition or not?!?
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) NOT NULL,\n" +
				"  `c` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`,`b`) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `a_2` (`a`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `b` (`b`,`a`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `c` (`c`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `b_2` (`b`,`a`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY LIST COLUMNS(`a`,`b`)\n" +
				"(PARTITION `p0` VALUES IN ((1,'1'),(2,'2'),(3,'3')),\n" +
				" PARTITION `p1` VALUES IN ((100,'100'),(101,'101'),(102,'102'),DEFAULT))"))
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) NOT NULL,\n" +
				"  `c` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`,`b`) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `a_2` (`a`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `b` (`b`,`a`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `c` (`c`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `b_2` (`b`,`a`)\n" +
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
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn, false)
}

func TestMultiSchemaReorganizePartition(t *testing.T) {
	createSQL := `create table t (a int primary key, b varchar(255), unique index idx_b_global (b) global) partition by range (a) (partition p1 values less than (200), partition pMax values less than (maxvalue))`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1),(2,2),(101,101),(102,102),(998,998),(999,999)`)
	}
	alterSQL := `alter table t reorganize partition p1 into (partition p0 values less than (100), partition p1 values less than (200))`

	testID := 4
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		dbgStr := ` /* ` + schemaState + ` */`

		// Check for every state
		tkO.MustContainErrMsg(`insert into t values (1,2)`+dbgStr, "[kv:1062]Duplicate entry")
		tkNO.MustContainErrMsg(`insert into t values (1,2)`+dbgStr, "[kv:1062]Duplicate entry")
		tkO.MustContainErrMsg(`insert into t values (101,101)`+dbgStr, "[kv:1062]Duplicate entry")
		tkNO.MustContainErrMsg(`insert into t values (101,101)`+dbgStr, "[kv:1062]Duplicate entry")
		tkO.MustContainErrMsg(`insert into t values (999,999)`+dbgStr, "[kv:1062]Duplicate entry '999' for key 't.")
		tkNO.MustContainErrMsg(`insert into t values (999,999)`+dbgStr, "[kv:1062]Duplicate entry '999' for key 't.")
		tkNO.MustQuery(`select * from t where a = 1` + dbgStr).Sort().Check(testkit.Rows("1 1"))
		tkNO.MustQuery(`select * from t where a = 1 or a = 2 or a = 3` + dbgStr).Sort().Check(testkit.Rows("1 1", "2 2"))
		tkNO.MustQuery(`select * from t where a in (1,2,3)` + dbgStr).Sort().Check(testkit.Rows("1 1", "2 2"))
		tkNO.MustQuery(`select * from t where b = "2"` + dbgStr).Sort().Check(testkit.Rows("2 2"))

		highID := testID + 980
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,%d)`+dbgStr, highID, highID))
		res = tkNO.MustQuery(fmt.Sprintf(`select * from t where b = "%d"`+dbgStr, highID))
		if len(res.Rows()) != 1 {
			tkNO.MustQuery(fmt.Sprintf(`explain select * from t where b = "%d"`+dbgStr, highID)).Check(testkit.Rows(fmt.Sprintf("%d %d", highID, highID)))
		}
		res.Check(testkit.Rows(fmt.Sprintf("%d %d", highID, highID)))

		highID++
		tkNO.MustExec(fmt.Sprintf(`insert into t values (%d,%d)`+dbgStr, highID, highID))
		tkO.MustQuery(fmt.Sprintf(`select * from t where b = "%d"`+dbgStr, highID)).Check(testkit.Rows(fmt.Sprintf("%d %d", highID, highID)))

		testID++
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,%d)`+dbgStr, testID, testID))
		tkNO.MustQuery(fmt.Sprintf(`select * from t where b = "%d"`+dbgStr, testID)).Check(testkit.Rows(fmt.Sprintf("%d %d", testID, testID)))

		logutil.BgLogger().Info("inserting rows", zap.Int("testID", testID), zap.String("state", schemaState))

		testID++
		tkNO.MustExec(fmt.Sprintf(`insert into t values (%d,%d)`+dbgStr, testID, testID))
		tkO.MustQuery(fmt.Sprintf(`select * from t where b = "%d"`+dbgStr, testID)).Check(testkit.Rows(fmt.Sprintf("%d %d", testID, testID)))

		// Test for Index, specially between WriteOnly and DeleteOnly, but better to test all states.
		// if tkNO (DeleteOnly) updates a row, the new index should be deleted, but not inserted.
		// It will be inserted by backfill in WriteReorganize.
		// If not deleted, then there would be an orphan entry in the index!
		tkO.MustExec(fmt.Sprintf(`update t set b = %d where a = %d`+dbgStr, testID+100, testID))
		tkNO.MustQuery(fmt.Sprintf(`select a, b from t where a = %d`+dbgStr, testID)).Check(testkit.Rows(fmt.Sprintf("%d %d", testID, testID+100)))
		tkNO.MustQuery(fmt.Sprintf(`select a, b from t where b = "%d"`+dbgStr, testID+100)).Check(testkit.Rows(fmt.Sprintf("%d %d", testID, testID+100)))
		tkNO.MustExec(fmt.Sprintf(`update t set b = %d where a = %d`+dbgStr, testID+99, testID-1))
		tkO.MustQuery(fmt.Sprintf(`select a, b from t where a = %d`+dbgStr, testID-1)).Check(testkit.Rows(fmt.Sprintf("%d %d", testID-1, testID+99)))
		tkO.MustQuery(fmt.Sprintf(`select a, b from t where b = "%d"`+dbgStr, testID+99)).Check(testkit.Rows(fmt.Sprintf("%d %d", testID-1, testID+99)))
		tkNO.MustExec(fmt.Sprintf(`update t set b = %d where a = %d`+dbgStr, testID, testID))
		tkO.MustExec(fmt.Sprintf(`update t set b = %d where a = %d`+dbgStr, testID-1, testID-1))

		switch schemaState {
		case model.StateDeleteOnly.String():
			// tkNO sees original table/partitions as before the DDL stated
			// tkO uses the original table/partitions, but should also delete from the newly created
			// Global Index, to replace the existing one.

			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "101 101", "102 102", "2 2", "5 5", "6 6", "984 984", "985 985", "998 998", "999 999"))
			tkNO.MustQuery(`select * from t where a < 1000`).Sort().Check(testkit.Rows("1 1", "101 101", "102 102", "2 2", "5 5", "6 6", "984 984", "985 985", "998 998", "999 999"))
			tkNO.MustQuery(`select * from t where a > 0`).Sort().Check(testkit.Rows("1 1", "101 101", "102 102", "2 2", "5 5", "6 6", "984 984", "985 985", "998 998", "999 999"))
			tkNO.MustQuery(`select * from t where a < 100`).Sort().Check(testkit.Rows("1 1", "2 2", "5 5", "6 6"))
		case model.StateWriteOnly.String():
			// Both tkO and tkNO uses the original table/partitions,
			// but tkO should also update the newly created
			// Global Index, and tkNO should only delete from it.
		case model.StateWriteReorganization.String():
			// Both tkO and tkNO uses the original table/partitions,
			// and should also update the newly created Global Index.
		case model.StateDeleteReorganization.String():
			// Both tkO now sees the new partitions, and should use the new Global Index,
			// plus double write to the old one.
			// tkNO uses the original table/partitions,
			// and should also update the newly created Global Index.
			// TODO: Test update and delete!
			// TODO: test key, hash and list partition without default partition :)
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `idx_b_global` (`b`) /*T![global_index] GLOBAL */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY RANGE (`a`)\n" +
				"(PARTITION `p1` VALUES LESS THAN (200),\n" +
				" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `idx_b_global` (`b`) /*T![global_index] GLOBAL */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY RANGE (`a`)\n" +
				"(PARTITION `p0` VALUES LESS THAN (100),\n" +
				" PARTITION `p1` VALUES LESS THAN (200),\n" +
				" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
		case model.StatePublic.String():
			// not tested, both tkO and tkNO sees same partitions
		case model.StateNone.String():
			// not tested, both tkO and tkNO sees same partitions
		default:
			require.Failf(t, "unhandled schema state", "State '%s'", schemaState)
		}
	}
	postFn := func(tkO *testkit.TestKit, store kv.Storage) {
		tkO.MustQuery(`select * from t where b = 5`).Sort().Check(testkit.Rows("5 5"))
		tkO.MustQuery(`select * from t where b = "5"`).Sort().Check(testkit.Rows("5 5"))
		tkO.MustExec(`admin check table t`)
		tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "10 10", "101 101", "102 102", "11 11", "12 12", "13 13", "14 14", "15 15", "16 16", "2 2", "5 5", "6 6", "7 7", "8 8", "9 9", "984 984", "985 985", "986 986", "987 987", "988 988", "989 989", "990 990", "991 991", "992 992", "993 993", "994 994", "995 995", "998 998", "999 999"))
		tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
			"t CREATE TABLE `t` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `b` varchar(255) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
			"  UNIQUE KEY `idx_b_global` (`b`) /*T![global_index] GLOBAL */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
			"PARTITION BY RANGE (`a`)\n" +
			"(PARTITION `p0` VALUES LESS THAN (100),\n" +
			" PARTITION `p1` VALUES LESS THAN (200),\n" +
			" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, false)
}

// Also tests for conversions of unique indexes
// 1 unique non-global - to become global
// 2 unique global - to become non-global
// 3 unique non-global - to stay non-global
// 4 unique global - to stay global
func TestMultiSchemaPartitionByGlobalIndex(t *testing.T) {
	createSQL := `create table t (a int primary key nonclustered global, b varchar(255), c bigint, unique index idx_b_global (b) global, unique key idx_ba (b,a), unique key idx_ab (a,b) global, unique key idx_c_global (c) global, unique key idx_cab (c,a,b)) partition by key (a,b) partitions 3`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1,1),(2,2,2),(101,101,101),(102,102,102)`)
	}
	alterSQL := `alter table t partition by key (b,a) partitions 5 update indexes (idx_ba global, idx_ab local)`
	doneStateWriteReorganize := false
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		switch schemaState {
		case model.StateDeleteOnly.String():
			// tkNO sees original table/partitions as before the DDL stated
			// tkO uses the original table/partitions, but should also delete from the newly created
			// Global Index, to replace the existing one.
			tkO.MustContainErrMsg(`insert into t values (1,2,3)`, "[kv:1062]Duplicate entry '2' for key 't.idx_b")
			tkNO.MustContainErrMsg(`insert into t values (1,2,3)`, "[kv:1062]Duplicate entry '2' for key 't.idx_b")
			tkO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101")
			tkNO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1 1", "101 101 101", "102 102 102", "2 2 2"))
			tkNO.MustQuery(`select * from t where a < 1000`).Sort().Check(testkit.Rows("1 1 1", "101 101 101", "102 102 102", "2 2 2"))
			tkNO.MustQuery(`select * from t where a > 0`).Sort().Check(testkit.Rows("1 1 1", "101 101 101", "102 102 102", "2 2 2"))
			tkNO.MustQuery(`select * from t where a = 1`).Sort().Check(testkit.Rows("1 1 1"))
			tkNO.MustQuery(`select * from t where a = 1 or a = 2 or a = 3`).Sort().Check(testkit.Rows("1 1 1", "2 2 2"))
			tkNO.MustQuery(`select * from t where a in (1,2,3)`).Sort().Check(testkit.Rows("1 1 1", "2 2 2"))
			tkNO.MustQuery(`select * from t where a < 100`).Sort().Check(testkit.Rows("1 1 1", "2 2 2"))

			tkNO.MustQuery(`select * from t where b = 2`).Sort().Check(testkit.Rows("2 2 2"))
			tkO.MustExec(`insert into t values (3,3,3)`)
			tkNO.MustExec(`insert into t values (4,4,4)`)
			tkNO.MustQuery(`select * from t where a = 3`).Sort().Check(testkit.Rows("3 3 3"))
			tkO.MustQuery(`select * from t where a = 4`).Sort().Check(testkit.Rows("4 4 4"))
		case model.StateWriteOnly.String():
			// Both tkO and tkNO uses the original table/partitions,
			// but tkO should also update the newly created
			// Global Index, and tkNO should only delete from it.
			tkO.MustContainErrMsg(`insert into t values (1,1,1)`, "[kv:1062]Duplicate entry '1")
			tkNO.MustContainErrMsg(`insert into t values (1,1,1)`, "[kv:1062]Duplicate entry '1")
			tkO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101")
			tkNO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101")
			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1 1", "101 101 101", "102 102 102", "2 2 2", "3 3 3", "4 4 4"))
			tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1 1", "101 101 101", "102 102 102", "2 2 2", "3 3 3", "4 4 4"))
			logutil.BgLogger().Info("insert into t values (5,5,5)")
			tkO.MustExec(`insert into t values (5,5,5)`)
			tkNO.MustExec(`insert into t values (6,6,6)`)
			tkNO.MustQuery(`select * from t where a = 5`).Sort().Check(testkit.Rows("5 5 5"))
			tkO.MustQuery(`select * from t where a = 6`).Sort().Check(testkit.Rows("6 6 6"))
		case model.StateWriteReorganization.String():
			// It will go through StateWriteReorg more than once.
			if doneStateWriteReorganize {
				break
			}
			doneStateWriteReorganize = true
			// Both tkO and tkNO uses the original table/partitions,
			// and should also update the newly created Global Index.
			tkO.MustExec(`insert into t values (7,7,7)`)
			tkNO.MustExec(`insert into t values (8,8,8)`)
			tkNO.MustQuery(`select * from t where b = 7`).Check(testkit.Rows("7 7 7"))
			tkO.MustQuery(`select * from t where b = 8`).Check(testkit.Rows("8 8 8"))
		case model.StateDeleteReorganization.String():
			// Both tkO now sees the new partitions, and should use the new Global Index,
			// plus double write to the old one.
			// tkNO uses the original table/partitions,
			// and should also update the newly created Global Index.
			tkO.MustExec(`insert into t values (9,9,9)`)
			tkNO.MustExec(`insert into t values (10,10,10)`)
			tkNO.MustQuery(`select * from t where b = 9`).Check(testkit.Rows("9 9 9"))
			tkO.MustQuery(`select * from t where b = 10`).Check(testkit.Rows("10 10 10"))
			// TODO: Test update and delete!
			// TODO: test key, hash and list partition without default partition :)
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  `c` bigint(20) DEFAULT NULL,\n" +
				"  UNIQUE KEY `idx_b_global` (`b`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `idx_ba` (`b`,`a`),\n" +
				"  UNIQUE KEY `idx_ab` (`a`,`b`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `idx_c_global` (`c`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `idx_cab` (`c`,`a`,`b`),\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */ /*T![global_index] GLOBAL */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY KEY (`a`,`b`) PARTITIONS 3"))
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  `c` bigint(20) DEFAULT NULL,\n" +
				"  UNIQUE KEY `idx_cab` (`c`,`a`,`b`),\n" +
				"  UNIQUE KEY `idx_b_global` (`b`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `idx_ba` (`b`,`a`) /*T![global_index] GLOBAL */,\n" +
				"  UNIQUE KEY `idx_ab` (`a`,`b`),\n" +
				"  UNIQUE KEY `idx_c_global` (`c`) /*T![global_index] GLOBAL */,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */ /*T![global_index] GLOBAL */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
				"PARTITION BY KEY (`b`,`a`) PARTITIONS 5"))
		case model.StatePublic.String():
			tkO.MustExec(`insert into t values (11,11,11)`)
			tkNO.MustExec(`insert into t values (12,12,12)`)
		case model.StateNone.String():
			tkO.MustExec(`insert into t values (13,13,13)`)
			tkNO.MustExec(`insert into t values (14,14,14)`)
			tkO.MustQuery(`select * from t where b = 11`).Check(testkit.Rows("11 11 11"))
		default:
			require.Failf(t, "unhandled schema state '%s'", schemaState)
		}
	}
	postFn := func(tkO *testkit.TestKit, _ kv.Storage) {
		tkO.MustQuery(`select * from t where b = 5`).Check(testkit.Rows("5 5 5"))
		tkO.MustExec(`admin check table t`)
		tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows(""+
			"1 1 1",
			"10 10 10",
			"101 101 101",
			"102 102 102",
			"11 11 11",
			"12 12 12",
			"13 13 13",
			"14 14 14",
			"2 2 2",
			"3 3 3",
			"4 4 4",
			"5 5 5",
			"6 6 6",
			"7 7 7",
			"8 8 8",
			"9 9 9"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, false)
}

// TestMultiSchemaModifyColumn to show behavior when changing a column
func TestMultiSchemaModifyColumn(t *testing.T) {
	createSQL := `create table t (a int primary key, b varchar(255), key k_b (b))`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1)`)
	}
	alterSQL := `alter table t modify column b int unsigned not null`
	checkFn := func(tkO, tkNO *testkit.TestKit, id int, schemaState string) {
		indexIDSQL := `select index_id from information_schema.tidb_indexes where table_schema = 'test' and table_name = 't' and key_name = 'k_b'`
		logutil.BgLogger().Info("check table", zap.Int("id", id), zap.String("schemaState", schemaState),
			zap.String("owner table", tkO.MustQuery("select * from t use index()").Sort().String()),
			zap.String("owner index", tkO.MustQuery("select * from t use index(k_b)").Sort().String()),
			zap.String("owner index id", tkO.MustQuery(indexIDSQL).String()),
			zap.Int64("owner txn schema version", sessiontxn.GetTxnManager(tkO.Session()).GetTxnInfoSchema().SchemaMetaVersion()),
			zap.Int64("owner domain schema version", tkO.Session().GetLatestInfoSchema().SchemaMetaVersion()),
			zap.String("non-owner table", tkNO.MustQuery("select * from t use index()").Sort().String()),
			zap.String("non-owner index", tkNO.MustQuery("select * from t use index(k_b)").Sort().String()),
			zap.String("non-owner index id", tkNO.MustQuery(indexIDSQL).String()),
			zap.Int64("non-owner txn schema version", sessiontxn.GetTxnManager(tkNO.Session()).GetTxnInfoSchema().SchemaMetaVersion()),
			zap.Int64("non-owner domain schema version", tkNO.Session().GetLatestInfoSchema().SchemaMetaVersion()),
		)
		tkO.MustExec("set @@sql_mode = default")
		tkNO.MustExec("set @@sql_mode = default")

		tkO.MustExec("set session tidb_enable_fast_table_check = off")
		tkNO.MustExec("set session tidb_enable_fast_table_check = off")
		defer func() {
			tkO.MustExec("set session tidb_enable_fast_table_check = default")
			tkNO.MustExec("set session tidb_enable_fast_table_check = default")
		}()
		err := tkO.ExecToErr(`admin check table t`)
		require.NoError(t, err, "owner admin check table failed", fmt.Sprintf("id, %d, schemaState: %s", id, schemaState))
		err = tkNO.ExecToErr(`admin check table t`)
		require.NoError(t, err, "non-owner admin check table failed", fmt.Sprintf("id, %d, schemaState: %s", id, schemaState))
	}
	firstTimeToPublic := true
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		switch schemaState {
		case model.StateDeleteOnly.String():
			// we are only interested in StateWriteReorganization
		case model.StateWriteOnly.String():
			// we are only interested in StateDeleteReorganization->StatePublic
		case model.StateWriteReorganization.String():
		case model.StatePublic.String():
			if !firstTimeToPublic {
				return
			}
			firstTimeToPublic = false
			// tkNO sees varchar column and tkO sees int column
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` int(10) unsigned NOT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
				"  KEY `k_b` (`b`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
				"  KEY `k_b` (`b`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
			checkFn(tkO, tkNO, 1, schemaState)

			tkO.MustExec(`insert into t values (10, " 09.60 ")`)

			checkFn(tkO, tkNO, 2, schemaState)

			// No warning!? Same in MySQL...
			tkNO.MustQuery(`show warnings`).Check(testkit.Rows())
			tkO.MustQuery(`select * from t where a = 10`).Check(testkit.Rows("10 10"))
			tkNO.MustQuery(`select * from t where a = 10`).Check(testkit.Rows("10 10"))
			// If the original b was defined as 'NOT NULL', then it would give an error:
			// [table:1364]Field 'b' doesn't have a default value

			tkNO.MustExec(`insert into t values (11, " 011.50 ")`)
			tkNO.MustQuery(`show warnings`).Check(testkit.Rows())
			// Anomaly, the different sessions sees different data.
			// So it should be acceptable for partitioning DDLs as well.
			// It may be possible to check that writes from StateWriteOnly convert 1:1
			// to the new type, and block writes otherwise. But then it would break the first tkO insert above...
			tkO.MustQuery(`select * from t where a = 11`).Check(testkit.Rows("11 12"))
			tkNO.MustQuery(`select * from t where a = 11`).Check(testkit.Rows("11  011.50 "))
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Greater(t, tblO.Columns[1].ID, tblNO.Columns[1].ID)
			// This also means that old copies of the columns will be left in the row, until the row is updated or deleted.
			// But I guess that is at least documented.
		default:
			require.Failf(t, "unhandled schema state '%s'", schemaState)
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn, false)
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
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn, false)
}

// getTablePartitionAndIndexIDs returns one array consisting of:
// table id + partition ids
func getTableAndPartitionIDs(t *testing.T, tk *testkit.TestKit) (parts []int64) {
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	if tbl.Meta().Partition == nil {
		return []int64{tbl.Meta().ID}
	}
	originalIDs := make([]int64, 0, 1+len(tbl.Meta().Partition.Definitions))
	originalIDs = append(originalIDs, tbl.Meta().ID)
	if tbl.Meta().Partition != nil {
		for _, def := range tbl.Meta().Partition.Definitions {
			originalIDs = append(originalIDs, def.ID)
		}
	}
	return originalIDs
}

func getAddingPartitionIDs(t *testing.T, tk *testkit.TestKit) (parts []int64) {
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	if tbl.Meta().Partition == nil {
		return nil
	}
	ids := make([]int64, 0, len(tbl.Meta().Partition.AddingDefinitions))
	if tbl.Meta().Partition != nil {
		for _, def := range tbl.Meta().Partition.AddingDefinitions {
			ids = append(ids, def.ID)
		}
	}
	return ids
}

func checkTableAndIndexEntries(t *testing.T, tk *testkit.TestKit, originalIDs []int64) {
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	currTableID := tbl.Meta().ID
	indexes := make([]int64, 0, len(tbl.Meta().Indices))
	globalIndexes := make([]int64, 0, 2)
	indexDebugInfo := make([]model.IndexInfo, 0, len(tbl.Meta().Indices))
	for _, idx := range tbl.Meta().Indices {
		if idx.Global {
			globalIndexes = append(globalIndexes, idx.ID)
		} else {
			indexes = append(indexes, idx.ID)
		}
		indexDebugInfo = append(indexDebugInfo, *idx)
	}
	// Only existing Global Indexes on table level
	if tbl.Meta().Partition == nil {
		require.Equal(t, 0, len(globalIndexes))
	} else {
		slices.Sort(globalIndexes)
		prev := int64(0)
		for _, idxID := range globalIndexes {
			if prev+1 == idxID {
				prev = idxID
				continue
			}
			require.False(t, HaveEntriesForTableIndex(t, tk, currTableID, prev+1, idxID), "Global index id range [%d,%d) for table id %d has still entries!\nTable: %#v\nIndexes: %#v\nPartitioning: %#v", prev+1, idxID, currTableID, tbl, indexDebugInfo, tbl.Meta().Partition)
			prev = idxID
		}
		require.False(t, HaveEntriesForTableIndex(t, tk, currTableID, prev+1, 0), "Global index id > %d for table id %d has still entries!", prev, currTableID)
	}

	// Only existing non-global indexes on partitions or non-partitioned tables
	slices.Sort(indexes)
	prev := int64(0)
	for _, idxID := range indexes {
		if prev+1 == idxID {
			prev = idxID
			continue
		}
		if tbl.Meta().Partition == nil {
			require.False(t, HaveEntriesForTableIndex(t, tk, currTableID, prev+1, idxID), "Index id range [%d,%d) for table id %d has still entries!\nTable: %#v\nIndex: %#v", prev+1, idxID, currTableID, tbl, indexDebugInfo)
		} else {
			for _, def := range tbl.Meta().Partition.Definitions {
				require.False(t, HaveEntriesForTableIndex(t, tk, def.ID, prev+1, idxID), "Index id %d for table id %d has still entries!", idxID, currTableID)
			}
		}
		prev = idxID
	}
	if tbl.Meta().Partition == nil {
		require.False(t, HaveEntriesForTableIndex(t, tk, currTableID, prev+1, 0), "Index id > %d for table id %d has still entries!", prev, currTableID)
	} else {
		for i, def := range tbl.Meta().Partition.Definitions {
			require.False(t, HaveEntriesForTableIndex(t, tk, def.ID, prev+1, 0), "Index id > %d for part (%d) id %d has still entries!\nTable: %#v\nPartitioning: %#v", prev, i, def.ID, tbl, tbl.Meta().Partition)
		}
	}
PartitionLoop:
	for _, id := range originalIDs {
		if tbl.Meta().Partition != nil {
			for _, def := range tbl.Meta().Partition.Definitions {
				if def.ID == id {
					continue PartitionLoop
				}
			}
		}
		if id == currTableID {
			continue
		}
		// old partitions removed
		require.False(t, HaveEntriesForTableIndex(t, tk, id, 0), "Reorganized table or partition id %d for table id %d has still entries!\nOrignal ids (table id, partition ids...): %#v\nTable: %#v\nPartitioning: %#v", id, currTableID, originalIDs, tbl, tbl.Meta().Partition)
	}
}

func runMultiSchemaTest(t *testing.T, createSQL, alterSQL string, initFn func(*testkit.TestKit), postFn func(*testkit.TestKit, kv.Storage), loopFn func(tO, tNO *testkit.TestKit), retestWithoutPartitions bool) {
	runMultiSchemaTestWithBackfillDML(t, createSQL, alterSQL, "", initFn, postFn, loopFn, retestWithoutPartitions)
}
func runMultiSchemaTestWithBackfillDML(t *testing.T, createSQL, alterSQL, backfillDML string, initFn func(*testkit.TestKit), postFn func(*testkit.TestKit, kv.Storage), loopFn func(tO, tNO *testkit.TestKit), retestWithoutPartitions bool) {
	// When debugging, increase the lease, so the schema does not auto reload :)
	distCtx := testkit.NewDistExecutionContextWithLease(t, 2, 15*time.Second)
	store := distCtx.Store
	domOwner := distCtx.GetDomain(0)
	domNonOwner := distCtx.GetDomain(1)

	if !domOwner.DDL().OwnerManager().IsOwner() {
		domOwner, domNonOwner = domNonOwner, domOwner
	}
	require.True(t, domOwner.DDL().OwnerManager().IsOwner())
	require.False(t, domNonOwner.DDL().OwnerManager().IsOwner())

	seDDLOwner, err := session.CreateSessionWithDomain(store, domOwner)
	require.NoError(t, err)
	tkDDLOwner := testkit.NewTestKitWithSession(t, store, seDDLOwner)
	tkDDLOwner.MustExec(`use test`)
	// Just to ensure we are not relying on the configurable assertions
	tkDDLOwner.MustExec(`set @@global.tidb_txn_assertion_level = off`)
	tkDDLOwner.MustExec(`set @@session.tidb_txn_assertion_level = off`)
	seTkOwner, err := session.CreateSessionWithDomain(store, domOwner)
	require.NoError(t, err)
	tkO := testkit.NewTestKitWithSession(t, store, seTkOwner)
	tkO.MustExec(`use test`)
	seTkNonOwner, err := session.CreateSessionWithDomain(store, domNonOwner)
	require.NoError(t, err)
	tkNO := testkit.NewTestKitWithSession(t, store, seTkNonOwner)
	tkNO.MustExec(`use test`)

	tkDDLOwner.MustExec(createSQL)
	domOwner.Reload()
	domNonOwner.Reload()

	originalPartitions := make([]int64, 0, 2)
	originalIndexIDs := make([]int64, 0, 1)
	originalGlobalIndexIDs := make([]int64, 0, 1)
	ctx := tkO.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableID := tbl.Meta().ID
	if tbl.Meta().Partition != nil {
		for _, def := range tbl.Meta().Partition.Definitions {
			originalPartitions = append(originalPartitions, def.ID)
		}
	}
	for _, idx := range tbl.Meta().Indices {
		if idx.Global {
			originalGlobalIndexIDs = append(originalGlobalIndexIDs, idx.ID)
			continue
		}
		originalIndexIDs = append(originalIndexIDs, idx.ID)
	}

	initFn(tkO)

	domOwner.Reload()
	domNonOwner.Reload()

	if !tbl.Meta().HasClusteredIndex() {
		// Debug prints, so it is possible to verify duplicate _tidb_rowid's
		res := tkO.MustQuery(`select *, _tidb_rowid from t`)
		logutil.BgLogger().Info("Query result before DDL", zap.String("result", res.String()))
	}

	verStart := domNonOwner.InfoSchema().SchemaMetaVersion()
	hookChan := make(chan *model.Job)
	// Notice that the job.SchemaState is not committed yet, so the table will still be in the previous state!
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
		hookChan <- job
		logutil.BgLogger().Info("XXXXXXXXXXX Hook now waiting", zap.String("job.State", job.State.String()), zap.String("job.SchemaState", job.SchemaState.String()))
		<-hookChan
		logutil.BgLogger().Info("XXXXXXXXXXX Hook released", zap.String("job.State", job.State.String()), zap.String("job.SchemaState", job.SchemaState.String()))
	})
	alterChan := make(chan error)
	go func() {
		if backfillDML != "" {
			// This can be used for testing concurrent writes during backfill.
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/PartitionBackfillData", func(b bool) {
				if b {
					seTk, err := session.CreateSessionWithDomain(store, domOwner)
					require.NoError(t, err)
					tk := testkit.NewTestKitWithSession(t, store, seTk)
					tk.MustExec(`use test`)
					logutil.BgLogger().Info("XXXXXXXXXXX Concurrent UPDATE!")
					tk.MustExec(backfillDML)
				}
			})
		}
		seDDL, err := session.CreateSessionWithDomain(store, domOwner)
		require.NoError(t, err)
		tkDDL := testkit.NewTestKitWithSession(t, store, seDDL)
		tkDDL.MustExec(`use test`)
		logutil.BgLogger().Info("XXXXXXXXXXX DDL starting!", zap.String("alterSQL", alterSQL))
		err = tkDDL.ExecToErr(alterSQL)
		logutil.BgLogger().Info("XXXXXXXXXXX DDL done!", zap.String("alterSQL", alterSQL))
		if backfillDML != "" {
			testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/PartitionBackfillData")
		}
		alterChan <- err
	}()
	// Skip the first state, since we want to compare before vs after in the loop
	<-hookChan
	hookChan <- nil
	verCurr := verStart + 1
	states := make([]model.SchemaState, 0, 5)
	for {
		// Waiting for the next State change to be done (i.e. blocking the state after)
		releaseHook := true
		var job *model.Job
		for {
			select {
			case job = <-hookChan:
			case err := <-alterChan:
				require.NoError(t, err)
				releaseHook = false
				logutil.BgLogger().Info("XXXXXXXXXXX release hook")
				break
			}
			domOwner.Reload()
			if domNonOwner.InfoSchema().SchemaMetaVersion() == domOwner.InfoSchema().SchemaMetaVersion() {
				// looping over reorganize data/indexes
				logutil.BgLogger().Info("XXXXXXXXXXX Schema Version has not changed")
				hookChan <- nil
				continue
			}
			break
		}
		logutil.BgLogger().Info("XXXXXXXXXXX states loop", zap.Int64("verCurr", verCurr), zap.Int64("NonOwner ver", domNonOwner.InfoSchema().SchemaMetaVersion()), zap.Int64("Owner ver", domOwner.InfoSchema().SchemaMetaVersion()))
		domOwner.Reload()
		require.Equal(t, verCurr-1, domNonOwner.InfoSchema().SchemaMetaVersion())
		require.Equal(t, verCurr, domOwner.InfoSchema().SchemaMetaVersion())
		// TODO: rewrite this to use the InjectCall failpoint instead
		state := model.StateNone
		if job != nil {
			state = job.SchemaState
		}
		states = append(states, state)
		loopFn(tkO, tkNO)
		domNonOwner.Reload()
		if !releaseHook {
			// Alter done!
			logutil.BgLogger().Info("XXXXXXXXXXX alter done, breaking states loop")
			break
		}
		// Continue to next state
		verCurr++
		hookChan <- nil
	}
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep")
	logutil.BgLogger().Info("XXXXXXXXXXX states loop done")
	if !tbl.Meta().HasClusteredIndex() {
		// Debug prints, so it is possible to verify possible newly generated _tidb_rowid's
		res := tkO.MustQuery(`select *, _tidb_rowid from t`)
		logutil.BgLogger().Info("Query result after DDL", zap.String("result", res.String()))
	}
	// Verify that there are no KV entries for old partitions or old indexes!!!
	gcWorker, err := gcworker.NewMockGCWorker(store)
	require.NoError(t, err)
	err = gcWorker.DeleteRanges(context.Background(), uint64(math.MaxInt64))
	require.NoError(t, err)
	tkO.MustQuery(`select * from mysql.gc_delete_range`).Check(testkit.Rows())
	ctx = tkO.Session()
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	newTableID := tbl.Meta().ID
	if tableID != newTableID {
		require.False(t, HaveEntriesForTableIndex(t, tkO, tableID, 0), "Old table id %d has still entries!", tableID)
	}
GlobalLoop:
	for _, globIdx := range originalGlobalIndexIDs {
		for _, idx := range tbl.Meta().Indices {
			if idx.ID == globIdx {
				continue GlobalLoop
			}
		}
		// Global index removed
		require.False(t, HaveEntriesForTableIndex(t, tkO, tableID, globIdx), "Global index id %d for table id %d has still entries!", globIdx, tableID)
	}
LocalLoop:
	for _, locIdx := range originalIndexIDs {
		for _, idx := range tbl.Meta().Indices {
			if idx.ID == locIdx {
				continue LocalLoop
			}
		}
		// local index removed
		if tbl.Meta().Partition != nil {
			for _, part := range tbl.Meta().Partition.Definitions {
				require.False(t, HaveEntriesForTableIndex(t, tkO, part.ID, locIdx), "Local index id %d for partition id %d has still entries!", locIdx, tableID)
			}
		}
	}
PartitionLoop:
	for _, partID := range originalPartitions {
		if tbl.Meta().Partition != nil {
			for _, def := range tbl.Meta().Partition.Definitions {
				if def.ID == partID {
					continue PartitionLoop
				}
			}
		}
		// old partitions removed
		require.False(t, HaveEntriesForTableIndex(t, tkO, partID, 0), "Reorganized partition id %d for table id %d has still entries!", partID, tableID)
	}
	// TODO: Use this instead of the above, which check for any row that should not exists
	// When the following issues are fixed:
	// TestMultiSchemaModifyColumn - https://github.com/pingcap/tidb/issues/60264
	// TestMultiSchemaPartitionByGlobalIndex -https://github.com/pingcap/tidb/issues/60263
	//checkTableAndIndexEntries(t, tkO, originalIDs)

	if postFn != nil {
		postFn(tkO, store)
	}
	if retestWithoutPartitions {
		// Check that all DMLs would have give the same result without ALTER and on a non-partitioned table!
		res := tkO.MustQuery(`select * from t`).Sort()
		tkO.MustExec("drop table t")
		tkO.MustExec(createSQL)
		// Will give error if not already partitioned, just ignore it.
		_, _ = tkO.Exec("alter table t remove partitioning")
		initFn(tkO)
		domOwner.Reload()
		domNonOwner.Reload()
		for range states {
			loopFn(tkO, tkNO)
		}
		if postFn != nil {
			postFn(tkO, store)
		}
		tkO.MustQuery(`select * from t`).Sort().Check(res.Rows())
	}
	// NOT deferring this, since it might hang on test failures...
	domOwner.Close()
	domNonOwner.Close()
	store.Close()
}

// HaveEntriesForTableIndex returns number of entries in the KV range of table+index or just the table if index is 0.
// Also checks with gc_delete_range
func HaveEntriesForTableIndex(t *testing.T, tk *testkit.TestKit, ids ...int64) bool {
	var start kv.Key
	var end kv.Key
	if len(ids) < 2 || len(ids) > 3 {
		require.Fail(t, "HaveEntriesForTableIndex requires 2 or 3 ids: tableID, indexID [, lastIndexID]")
	}
	tableID := ids[0]
	indexID := ids[1]
	if indexID == 0 {
		logutil.BgLogger().Info("HaveEntriesForTableIndex checking table", zap.Int64("tableID", tableID))
		start = tablecodec.EncodeTablePrefix(tableID)
		end = tablecodec.EncodeTablePrefix(tableID + 1)
	} else {
		start = tablecodec.EncodeTableIndexPrefix(tableID, indexID)
		if len(ids) == 3 {
			if ids[2] == 0 {
				logutil.BgLogger().Info("HaveEntriesForTableIndex indexes greater or equal than", zap.Int64("tableID", tableID), zap.Int64("indexID", indexID))
				end = tablecodec.EncodeTableIndexPrefix(tableID, math.MaxInt64)
				end = end.Next()
			} else {
				logutil.BgLogger().Info("HaveEntriesForTableIndex indexes between", zap.Int64("tableID", tableID), zap.Int64("indexID", indexID), zap.Int64("lastIndexID", ids[2]))
				end = tablecodec.EncodeTableIndexPrefix(tableID, ids[2])
			}
		} else {
			logutil.BgLogger().Info("HaveEntriesForTableIndex index", zap.Int64("tableID", tableID), zap.Int64("indexID", indexID))
			end = tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
		}
	}
	ctx := tk.Session()
	require.NoError(t, sessiontxn.NewTxn(context.Background(), ctx))
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	it, err := txn.Iter(start, end)
	require.NoError(t, err)
	defer it.Close()
	count := 0
	for it.Valid() {
		count++
		logutil.BgLogger().Info("HaveEntriesForTableIndex", zap.String("key", hex.EncodeToString(it.Key())), zap.String("value", hex.EncodeToString(it.Value())))
		err = it.Next()
		require.NoError(t, err)
	}
	if count > 0 {
		logutil.BgLogger().Info("HaveEntriesForTableIndex", zap.Int64("tableID", tableID), zap.Int64("indexID", indexID), zap.Int("count", count))
		return true
	}
	return false
}

>>>>>>> 567e139701f (ddl: add states to remove old objects during modifying column (#62549))
func TestMultiSchemaReorganizePK(t *testing.T) {
	createSQL := `create table t (c1 INT primary key, c2 CHAR(255), c3 CHAR(255), c4 CHAR(255), c5 CHAR(255)) partition by range (c1) (partition p1 values less than (200), partition pMax values less than (maxvalue))`
	i := 1
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',%d,%d)`, i, "init O", 4185725186-i, 7483634197-i))
		i++
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',%d,%d)`, i, "init O", 4185725186-i, 7483634197-i))
		i++
	}
	alterSQL := `alter table t reorganize partition p1 into (partition p0 values less than (100), partition p1 values less than (200))`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',%d,%d)`, i, schemaState+" O", 4185725186-i, 7483634197-i))
		i++
		tkNO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',%d,%d)`, i, schemaState+" NO", 4185725186-i, 7483634197-i))
		i++
	}
	postFn := func(tkO *testkit.TestKit) {
		require.Equal(t, int(6*2+1), i)
		tkO.MustQuery(`select c1,c2 from t`).Sort().Check(testkit.Rows(""+
			"1 init O",
			"10 delete reorganization NO",
			"11 none O",
			"12 none NO",
			"2 init O",
			"3 delete only O",
			"4 delete only NO",
			"5 write only O",
			"6 write only NO",
			"7 write reorganization O",
			"8 write reorganization NO",
			"9 delete reorganization O"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, false)
	i = 1
	postFn = func(tkO *testkit.TestKit) {
		tkO.MustQuery(`select c1,c2,c3 from t`).Sort().Check(testkit.Rows(""+
			"1 init O updated",
			"10 delete reorganization NO Original",
			"11 none O Original",
			"12 none NO Original",
			"2 init O updated",
			"3 delete only O updated",
			"4 delete only NO updated",
			"5 write only O updated",
			"6 write only NO updated",
			"7 write reorganization O Original",
			"8 write reorganization NO Original",
			"9 delete reorganization O Original"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, true)
}

func TestMultiSchemaReorganizeNoPK(t *testing.T) {
	createSQL := `create table t (c1 INT, c2 CHAR(255), c3 CHAR(255), c4 CHAR(255), c5 CHAR(255)) partition by range (c1) (partition p1 values less than (200), partition pMax values less than (maxvalue))`
	i := 1
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',repeat('%d', 25),repeat('%d', 25))`, i, "init O", 4185725186-i, 7483634197-i))
		i++
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',repeat('%d', 25),repeat('%d', 25))`, i, "init O", 4185725186-i, 7483634197-i))
		i++
	}
	alterSQL := `alter table t reorganize partition p1 into (partition p0 values less than (100), partition p1 values less than (200))`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		tkO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',repeat('%d', 25),repeat('%d', 25))`, i, schemaState+" O", 4185725186-i, 7483634197-i))
		i++
		tkNO.MustExec(fmt.Sprintf(`insert into t values (%d,'%s','Original',repeat('%d', 25),repeat('%d', 25))`, i, schemaState+" NO", 4185725186-i, 7483634197-i))
		i++
	}
	postFn := func(tkO *testkit.TestKit) {
		require.Equal(t, int(6*2+1), i)
		tkO.MustQuery(`select c1,_tidb_rowid,c2 from t`).Sort().Check(testkit.Rows(""+
			"1 60001 init O",
			"10 30004 delete reorganization NO",
			"11 7 none O",
			"12 30005 none NO",
			"2 60002 init O",
			"3 60003 delete only O",
			"4 60004 delete only NO",
			"5 4 write only O",
			// Before, there were a DUPLICATE ROW here!!!
			//"5 60004 write only O",
			"6 60005 write only NO",
			"7 5 write reorganization O",
			"8 30003 write reorganization NO",
			"9 6 delete reorganization O"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, false)
	i = 1
	postFn = func(tkO *testkit.TestKit) {
		require.Equal(t, int(6*2+1), i)
		tkO.MustQuery(`select c1,_tidb_rowid,c2,c3 from t`).Sort().Check(testkit.Rows(""+
			"1 1 init O updated",
			"10 30004 delete reorganization NO Original",
			"11 7 none O Original",
			"12 30005 none NO Original",
			"2 2 init O updated",
			"3 3 delete only O updated",
			"4 30001 delete only NO updated",
			"5 4 write only O updated",
			"6 30002 write only NO updated",
			"7 5 write reorganization O Original",
			"8 30003 write reorganization NO Original",
			"9 6 delete reorganization O Original"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, true)
}

func runMultiSchemaTest(t *testing.T, createSQL, alterSQL string, initFn, postFn func(*testkit.TestKit), loopFn func(tO, tNO *testkit.TestKit), injectUpdate bool) {
	distCtx := testkit.NewDistExecutionContextWithLease(t, 2, 15*time.Second)
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
	tkDDLOwner.MustExec(`set @@global.tidb_ddl_reorg_worker_cnt=1`)
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
	hook := &callback.TestDDLCallback{Do: domOwner}
	hook.OnJobRunAfterExported = hookFunc
	domOwner.DDL().SetHook(hook)
	alterChan := make(chan struct{})
	go func() {
		tkDDLOwner.MustExec(alterSQL)
		logutil.BgLogger().Info("XXXXXXXXXXX drop partition done!")
		alterChan <- struct{}{}
	}()
	if injectUpdate {
		// This can be used for testing concurrent writes during backfill.
		// It tested OK, since the backfill will fail and retry where it will get the fresh values and succeed.
		require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/PartitionBackfillData", func(b bool) {
			if b {
				tkO.MustExec(`update t set c3 = "updated"`)
			}
		}))
		defer failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/PartitionBackfillData")
	}

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
			case <-alterChan:
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
		//tk.t.Logf("RefreshSession rand seed: %d", seed)
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
