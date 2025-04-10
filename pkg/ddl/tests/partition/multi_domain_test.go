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
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/gcworker"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

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
			tkNO.MustContainErrMsg(`insert into t values (1,1,1)`, "[kv:1062]Duplicate entry '1' for key 't.a_2'")
			tkO.MustContainErrMsg(`insert into t values (101,101,101)`, "[kv:1062]Duplicate entry '101' for key 't.a_2'")
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
	createSQL := `create table t (a int primary key, b varchar(255), unique key uk_b (b))`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9)`)
	}
	alterSQL := `alter table t modify column b int unsigned not null`
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
			// tkNO sees varchar column and tkO sees int column
			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` int(10) unsigned NOT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `uk_b` (`b`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
				"t CREATE TABLE `t` (\n" +
				"  `a` int(11) NOT NULL,\n" +
				"  `b` varchar(255) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `uk_b` (`b`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

			tkO.MustExec(`insert into t values (10, " 09.60 ")`)
			// No warning!? Same in MySQL...
			tkNO.MustQuery(`show warnings`).Check(testkit.Rows())
			tkNO.MustContainErrMsg(`insert into t values (11, "09.60")`, "[kv:1062]Duplicate entry '10' for key 't._Idx$_uk_b_0'")
			tkO.MustQuery(`select * from t where a = 10`).Check(testkit.Rows("10 10"))
			// <nil> ?!?
			tkNO.MustQuery(`select * from t where a = 10`).Check(testkit.Rows("10 <nil>"))
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

// TODO: Also add test for REMOVE PARTITIONING!
///*
//// TODO: complete this test, so that we test all four changes:
//1 unique non-global - to become global
//2 unique global - to become non-global
//3 unique non-global - to stay non-global
//4 unique global - to stay global
//func TestMultiSchemaPartitionByGlobalIndex(t *testing.T) {
//	createSQL := `create table t (a int primary key, b varchar(255), c bigint, unique index idx_b_global (b) global, unique key idx_b (b), unique key idx_c_global (c), unique key idx_c (c)) partition by key (a,b) partitions 3`
//	initFn := func(tkO *testkit.TestKit) {
//		tkO.MustExec(`insert into t values (1,1),(2,2),(101,101),(102,102)`)
//	}
//	alterSQL := `alter table t partition by key (b,a) partitions 5`
//	loopFn := func(tkO, tkNO *testkit.TestKit) {
//		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
//		schemaState := res.Rows()[0][0].(string)
//		switch schemaState {
//		case model.StateDeleteOnly.String():
//			// tkNO sees original table/partitions as before the DDL stated
//			// tkO uses the original table/partitions, but should also delete from the newly created
//			// Global Index, to replace the existing one.
//			tkO.MustContainErrMsg(`insert into t values (1,2)`, "[kv:1062]Duplicate entry '2' for key 't.idx_b'")
//			tkNO.MustContainErrMsg(`insert into t values (1,2)`, "[kv:1062]Duplicate entry '2' for key 't.idx_b'")
//			tkO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.idx_b'")
//			tkNO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.idx_b'")
//			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "101 101", "102 102", "2 2"))
//			tkNO.MustQuery(`select * from t where a < 1000`).Sort().Check(testkit.Rows("1 1", "101 101", "102 102", "2 2"))
//			tkNO.MustQuery(`select * from t where a > 0`).Sort().Check(testkit.Rows("1 1", "101 101", "102 102", "2 2"))
//			tkNO.MustQuery(`select * from t where a = 1`).Sort().Check(testkit.Rows("1 1"))
//			tkNO.MustQuery(`select * from t where a = 1 or a = 2 or a = 3`).Sort().Check(testkit.Rows("1 1", "2 2"))
//			tkNO.MustQuery(`select * from t where a in (1,2,3)`).Sort().Check(testkit.Rows("1 1", "2 2"))
//			tkNO.MustQuery(`select * from t where a < 100`).Sort().Check(testkit.Rows("1 1", "2 2"))
//
//			tkNO.MustQuery(`select * from t where b = 2`).Sort().Check(testkit.Rows("2 2"))
//			tkO.MustExec(`insert into t values (3,3)`)
//			tkNO.MustExec(`insert into t values (4,4)`)
//			tkNO.MustQuery(`select * from t where a = 3`).Sort().Check(testkit.Rows("3 3"))
//			tkO.MustQuery(`select * from t where a = 4`).Sort().Check(testkit.Rows("4 4"))
//		case model.StateWriteOnly.String():
//			// Both tkO and tkNO uses the original table/partitions,
//			// but tkO should also update the newly created
//			// Global Index, and tkNO should only delete from it.
//			/*
//				tkO.MustContainErrMsg(`insert into t values (1,1)`, "[kv:1062]Duplicate entry '1' for key 't.idx_b'")
//				tkNO.MustContainErrMsg(`insert into t values (1,1)`, "[kv:1062]Duplicate entry '1' for key 't.idx_b'")
//				tkO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.idx_b'")
//				tkNO.MustContainErrMsg(`insert into t values (101,101)`, "[kv:1062]Duplicate entry '101' for key 't.idx_b'")
//				tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "101 101", "102 102", "2 2", "3 3", "4 4"))
//				tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "101 101", "102 102", "2 2", "3 3", "4 4"))
//
//			*/
//			logutil.BgLogger().Info("insert into t values (5,5)")
//			tkO.MustExec(`insert into t values (5,5)`)
//			tkNO.MustExec(`insert into t values (6,6)`)
//			tkNO.MustQuery(`select * from t where a = 5`).Sort().Check(testkit.Rows("5 5"))
//			tkO.MustQuery(`select * from t where a = 6`).Sort().Check(testkit.Rows("6 6"))
//		case model.StateWriteReorganization.String():
//			// Both tkO and tkNO uses the original table/partitions,
//			// and should also update the newly created Global Index.
//			tkO.MustExec(`insert into t values (7,7)`)
//			tkNO.MustExec(`insert into t values (8,8)`)
//			tkNO.MustQuery(`select * from t where b = 7`).Check(testkit.Rows("7 7"))
//			tkO.MustQuery(`select * from t where b = 8`).Check(testkit.Rows("8 8"))
//		case model.StateDeleteReorganization.String():
//			// Both tkO now sees the new partitions, and should use the new Global Index,
//			// plus double write to the old one.
//			// tkNO uses the original table/partitions,
//			// and should also update the newly created Global Index.
//			tkO.MustExec(`insert into t values (9,9)`)
//			tkNO.MustExec(`insert into t values (10,10)`)
//			tkNO.MustQuery(`select * from t where b = 9`).Check(testkit.Rows("9 9"))
//			tkO.MustQuery(`select * from t where b = 10`).Check(testkit.Rows("10 10"))
//			// TODO: Test update and delete!
//			// TODO: test key, hash and list partition without default partition :)
//			tkNO.MustQuery(`show create table t`).Check(testkit.Rows("" +
//				"t CREATE TABLE `t` (\n" +
//				"  `a` int(11) NOT NULL,\n" +
//				"  `b` varchar(255) DEFAULT NULL,\n" +
//				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
//				"  UNIQUE KEY idx_b (`b`) /*T![global_index] GLOBAL */\n" +
//				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
//				"PARTITION BY RANGE (`a`)\n" +
//				"(PARTITION `p1` VALUES LESS THAN (200))"))
//			tkO.MustQuery(`show create table t`).Check(testkit.Rows("" +
//				"t CREATE TABLE `t` (\n" +
//				"  `a` int(11) NOT NULL,\n" +
//				"  `b` varchar(255) DEFAULT NULL,\n" +
//				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
//				"  UNIQUE KEY idx_b (`b`) /*T![global_index] GLOBAL */\n" +
//				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
//				"PARTITION BY RANGE (`a`)\n" +
//				"(PARTITION `p0` VALUES LESS THAN (100),\n" +
//				" PARTITION `p1` VALUES LESS THAN (200))"))
//			tkO.MustExec(`insert into t values (3,3)`)
//		case model.StateNone.String():
//			// just to not fail :)
//		default:
//			require.Failf(t, "unhandled schema state '%s'", schemaState)
//		}
//	}
//	postFn := func(tkO *testkit.TestKit) {
//		tkO.MustQuery(`select * from t where b = 5`).Sort().Check(testkit.Rows("5 5"))
//		tkO.MustExec(`admin check table t`)
//	}
//	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn)
//}

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

	seOwner, err := session.CreateSessionWithDomain(store, domOwner)
	require.NoError(t, err)
	seNonOwner, err := session.CreateSessionWithDomain(store, domNonOwner)
	require.NoError(t, err)

	tkDDLOwner := testkit.NewTestKitWithSession(t, store, seOwner)
	tkDDLOwner.MustExec(`use test`)
	// Just to ensure we are not relying on the configurable assertions
	tkDDLOwner.MustExec(`set @@global.tidb_txn_assertion_level = off`)
	tkDDLOwner.MustExec(`set @@session.tidb_txn_assertion_level = off`)
	tkO := testkit.NewTestKitWithSession(t, store, seOwner)
	tkO.MustExec(`use test`)
	tkNO := testkit.NewTestKitWithSession(t, store, seNonOwner)
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

	if !tbl.Meta().IsCommonHandle && !tbl.Meta().PKIsHandle {
		// Debug prints, so it is possible to verify duplicate _tidb_rowid's
		res := tkO.MustQuery(`select *, _tidb_rowid from t`)
		logutil.BgLogger().Info("Query result before DDL", zap.String("result", res.String()))
	}

	verStart := domNonOwner.InfoSchema().SchemaMetaVersion()
	hookChan := make(chan struct{})
	hookFunc := func(job *model.Job) {
		hookChan <- struct{}{}
		logutil.BgLogger().Info("XXXXXXXXXXX Hook now waiting", zap.String("job.State", job.State.String()), zap.String("job.SchemaState", job.SchemaState.String()))
		<-hookChan
		logutil.BgLogger().Info("XXXXXXXXXXX Hook released", zap.String("job.State", job.State.String()), zap.String("job.SchemaState", job.SchemaState.String()))
	}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", hookFunc)
	alterChan := make(chan error)
	go func() {
		if backfillDML != "" {
			// This can be used for testing concurrent writes during backfill.
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/PartitionBackfillData", func(b bool) {
				if b {
					logutil.BgLogger().Info("XXXXXXXXXXX Concurrent UPDATE!")
					tkO.MustExec(backfillDML)
				}
			})
		}
		logutil.BgLogger().Info("XXXXXXXXXXX DDL starting!", zap.String("alterSQL", alterSQL))
		err := tkDDLOwner.ExecToErr(alterSQL)
		logutil.BgLogger().Info("XXXXXXXXXXX DDL done!", zap.String("alterSQL", alterSQL))
		if backfillDML != "" {
			testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/PartitionBackfillData")
		}
		alterChan <- err
	}()
	// Skip the first state, since we want to compare before vs after in the loop
	<-hookChan
	hookChan <- struct{}{}
	verCurr := verStart + 1
	state := 0
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
				logutil.BgLogger().Info("XXXXXXXXXXX Schema Version has not changed")
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
		if !releaseHook {
			// Alter done!
			break
		}
		// Continue to next state
		verCurr++
		state++
		hookChan <- struct{}{}
	}
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep")
	logutil.BgLogger().Info("XXXXXXXXXXX states loop done")
	if !tbl.Meta().IsCommonHandle && !tbl.Meta().PKIsHandle {
		// Debug prints, so it is possible to verify possible newly generated _tidb_rowid's
		res := tkO.MustQuery(`select *, _tidb_rowid from t`)
		logutil.BgLogger().Info("Query result after DDL", zap.String("result", res.String()))
	}
	// Verify that there are no KV entries for old partitions or old indexes!!!
	gcWorker, err := gcworker.NewMockGCWorker(store)
	require.NoError(t, err)
	err = gcWorker.DeleteRanges(context.Background(), uint64(math.MaxInt64))
	require.NoError(t, err)
	delRange := tkO.MustQuery(`select * from mysql.gc_delete_range_done`).Rows()
	s := ""
	for _, row := range delRange {
		if s != "" {
			s += "\n"
		}
		for i, col := range row {
			if i != 0 {
				s += " "
			}
			s += col.(string)
		}
	}
	logutil.BgLogger().Info("gc_delete_range_done", zap.String("rows", s))
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
	if postFn != nil {
		postFn(tkO, store)
	}
	// TODO: Enable this and fix the failures!
	disabled := domOwner == nil
	if disabled && retestWithoutPartitions {
		// Check that all DMLs would have give the same result without ALTER and on a non-partitioned table!
		res := tkO.MustQuery(`select * from t`).Sort()
		tkO.MustExec("drop table t")
		tkO.MustExec(createSQL)
		tkO.MustExec("alter table t remove partitioning")
		initFn(tkO)
		domOwner.Reload()
		domNonOwner.Reload()
		for i := 0; i <= state; i++ {
			loopFn(tkO, tkNO)
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
func HaveEntriesForTableIndex(t *testing.T, tk *testkit.TestKit, tableID, indexID int64) bool {
	var start kv.Key
	var end kv.Key
	if indexID == 0 {
		start = tablecodec.EncodeTablePrefix(tableID)
		end = tablecodec.EncodeTablePrefix(tableID + 1)
	} else {
		start = tablecodec.EncodeTableIndexPrefix(tableID, indexID)
		end = tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
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
	postFn := func(tkO *testkit.TestKit, _ kv.Storage) {
		require.Equal(t, int(7*2+1), i)
		tkO.MustQuery(`select c1,c2 from t`).Sort().Check(testkit.Rows(""+
			"1 init O",
			"10 delete reorganization NO",
			"11 public O",
			"12 public NO",
			"13 none O",
			"14 none NO",
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
}

func TestMultiSchemaReorganizePKBackfillDML(t *testing.T) {
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
	postFn := func(tkO *testkit.TestKit, _ kv.Storage) {
		tkO.MustQuery(`select c1,c2,c3 from t`).Sort().Check(testkit.Rows(""+
			"1 init O updated",
			"10 delete reorganization NO Original",
			"11 public O Original",
			"12 public NO Original",
			"13 none O Original",
			"14 none NO Original",
			"2 init O updated",
			"3 delete only O updated",
			"4 delete only NO updated",
			"5 write only O updated",
			"6 write only NO updated",
			"7 write reorganization O Original",
			"8 write reorganization NO Original",
			"9 delete reorganization O Original"))
	}
	runMultiSchemaTestWithBackfillDML(t, createSQL, alterSQL, "update t set c3 = 'updated'", initFn, postFn, loopFn, false)
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
	postFn := func(tkO *testkit.TestKit, _ kv.Storage) {
		require.Equal(t, int(7*2+1), i)
		tkO.MustQuery(`select c1,_tidb_rowid,c2 from t`).Sort().Check(testkit.Rows(""+
			"1 60001 init O",
			"10 30004 delete reorganization NO",
			"11 7 public O",
			"12 30005 public NO",
			"13 8 none O",
			"14 30006 none NO",
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
}

func TestMultiSchemaReorganizeNoPKBackfillDML(t *testing.T) {
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
	postFn := func(tkO *testkit.TestKit, _ kv.Storage) {
		require.Equal(t, int(7*2+1), i)
		tkO.MustQuery(`select c1,_tidb_rowid,c2,c3 from t`).Sort().Check(testkit.Rows(""+
			"1 1 init O updated",
			"10 30004 delete reorganization NO Original",
			"11 7 public O Original",
			"12 30005 public NO Original",
			"13 8 none O Original",
			"14 30006 none NO Original",
			"2 2 init O updated",
			"3 3 delete only O updated",
			"4 30001 delete only NO updated",
			"5 4 write only O updated",
			"6 30002 write only NO updated",
			"7 5 write reorganization O Original",
			"8 30003 write reorganization NO Original",
			"9 6 delete reorganization O Original"))
	}
	runMultiSchemaTestWithBackfillDML(t, createSQL, alterSQL, "update t set c3 = 'updated'", initFn, postFn, loopFn, false)
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
			// but are aware of new ids, so should filter them from global index reads.
			// Duplicate key errors (from delete only state) are allowed on insert/update,
			// even if it cannot read them from the global index, due to filtering.
			rows := tkNO.MustQuery(`select * from t`).Sort().Rows()
			tkO.MustQuery(`select * from t`).Sort().Check(rows)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateNone, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateWriteOnly, tblO.Partition.DDLState)
			require.Equal(t, tblNO.Partition.Definitions[1].ID, tblO.Partition.Definitions[1].ID)
			tkNO.MustContainErrMsg(`insert into t values (1,1,"Duplicate key")`, "[kv:1062]Duplicate entry '1' for key 't.")
			tkO.MustContainErrMsg(`insert into t values (1,1,"Duplicate key")`, "[kv:1062]Duplicate entry '1' for key 't.")
		case "delete only":
			// tkNO is seeing state write only, so still can access the dropped partition
			// tkO is seeing state delete only, so cannot see the dropped partition,
			// but must still write to the shared global indexes.
			// So they will get errors on the same entries in the global index.

			tkNO.MustContainErrMsg(`insert into t values (1,1,"Duplicate key")`, "[kv:1062]Duplicate entry '1' for key 't.")
			tkO.MustContainErrMsg(`insert into t values (1,1,"Duplicate key")`, "[kv:1062]Duplicate entry '1' for key 't.")
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateWriteOnly, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteOnly, tblO.Partition.DDLState)
			require.NotEqual(t, tblNO.Partition.Definitions[1].ID, tblO.Partition.Definitions[1].ID)

			tkNO.MustExec(`insert into t values (21,21,"OK")`)
			tkNO.MustExec(`insert into t values (23,23,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (21,21,"Duplicate key")`, "[kv:1062]Duplicate entry '21' for key 't.")
			tkO.MustContainErrMsg(`insert into t values (6,23,"Duplicate key")`, "[kv:1062]Duplicate entry '")
			// Primary is not global, so here we can insert into the new partition, without
			// conflicting to the old one
			tkO.MustExec(`insert into t values (21,25,"OK")`)
			tkO.MustExec(`insert into t values (99,99,"OK")`)
			tkNO.MustContainErrMsg(`insert into t values (8,25,"Duplicate key")`, "[kv:1062]Duplicate entry '25' for key 't.uk_b'")
			// type differences, cannot use index
			tkNO.MustQuery(`select count(*) from t where b = 25`).Check(testkit.Rows("0"))
			tkNO.MustQuery(`select b from t where b = 25`).Check(testkit.Rows())
			// PointGet should not find new partitions for StateWriteOnly
			tkNO.MustQuery(`select count(*) from t where b = "25"`).Check(testkit.Rows("0"))
			tkNO.MustQuery(`select b from t where b = "25"`).Check(testkit.Rows())
			tkNO.MustExec(`update t set a = 2, c = "'a' Updated" where b = "25"`)
			require.Equal(t, uint64(0), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
			tkNO.MustExec(`update t set a = 2, c = "'a' Updated" where b = "25"`)
			require.Equal(t, uint64(0), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
			// Primary is not global, so here we can insert into the old partition, without
			// conflicting to the new one
			tkO.MustQuery(`select count(*) from t where a = 99`).Check(testkit.Rows("1"))
			tkNO.MustExec(`insert into t values (99,27,"OK")`)

			tkO.MustQuery(`select count(*) from t where b = "23"`).Check(testkit.Rows("0"))
			tkO.MustExec(`update t set a = 2, c = "'a' Updated" where b = "23"`)
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
				"3 3 Filler",
				"4 4 Filler",
				"5 5 Filler",
				"6 6 Filler",
				"7 7 Filler",
				"99 25 OK"))
			tkNO.MustQuery(`select b from t order by b`).Check(testkit.Rows(""+
				"1",
				"2",
				"21",
				"23",
				"25",
				"3",
				"4",
				"5",
				"6",
				"7"))

			tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows(""+
				"2 2 Filler",
				"27 27 OK",
				"4 4 Filler",
				"6 6 Filler",
				"99 99 OK"))
			tkO.MustQuery(`select b from t order by b`).Check(testkit.Rows(""+
				"2",
				"27",
				"4",
				"6",
				"99"))
			// TODO: Add tests for delete
		case "delete reorganization":
			// tkNO is seeing state delete only, so cannot see the dropped partition,
			// but must still must give duplicate errors when writes to the global indexes collide
			// with the dropped partitions.
			// tkO is seeing state delete reorganization, so cannot see the dropped partition,
			// and can ignore the dropped partitions entries in the Global Indexes, i.e. overwrite them!
			rows := tkO.MustQuery(`select * from t`).Sort().Rows()
			tkNO.MustQuery(`select * from t`).Sort().Check(rows)
			rows = tkO.MustQuery(`select b from t order by b`).Rows()
			tkNO.MustQuery(`select b from t order by b`).Check(rows)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteOnly, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteReorganization, tblO.Partition.DDLState)
			require.Equal(t, tblNO.Partition.Definitions[1].ID, tblO.Partition.Definitions[1].ID)
			tkO.MustQuery(`select b from t where b = "1"`).Check(testkit.Rows())
			tkO.MustExec(`insert into t values (1,1,"OK")`)
			tkO.MustQuery(`select b from t where b = "1"`).Check(testkit.Rows("1"))
			tkO.MustQuery(`select b from t where b = 1`).Check(testkit.Rows("1"))
			tkO.MustContainErrMsg(`insert into t values (3,1,"Duplicate")`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			// b = 23 was inserted into the dropped partition, OK to delete
			tkO.MustExec(`insert into t values (10,23,"OK")`)
			tkNO.MustExec(`insert into t values (41,41,"OK")`)
			tkNO.MustContainErrMsg(`insert into t values (12,25,"Duplicate key")`, "[kv:1062]Duplicate entry '25' for key 't.uk_b'")
			tkNO.MustContainErrMsg(`insert into t values (25,25,"Duplicate key")`, "[kv:1062]Duplicate entry '25' for key 't.")
			tkNO.MustContainErrMsg(`insert into t values (41,27,"Duplicate key")`, "[kv:1062]Duplicate entry '")
			tkO.MustExec(`insert into t values (43,43,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (44,43,"Duplicate key")`, "[kv:1062]Duplicate entry '")
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
			tkO.MustContainErrMsg(`insert into t values (81,81,"Duplicate key")`, "[kv:1062]Duplicate entry '81' for key 't.")
			tkNO.MustExec(`insert into t values (85,85,"OK")`)
			tkO.MustExec(`insert into t values (87,87,"OK")`)
			rows := tkNO.MustQuery(`select * from t`).Sort().Rows()
			tkO.MustQuery(`select * from t`).Sort().Check(rows)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteReorganization, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateNone, tblO.Partition.DDLState)
		default:
			require.Failf(t, "unhandled schema state '%s'", schemaState)
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn, false)
}

func TestMultiSchemaTruncatePartitionWithPKGlobal(t *testing.T) {
	createSQL := `create table t (a int primary key nonclustered global, b int, c varchar(255) default 'Filler', unique key uk_b (b)) partition by hash (b) partitions 2`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t (a,b) values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7)`)
	}
	alterSQL := `alter table t truncate partition p1`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		switch schemaState {
		case "write only":
			// tkNO is seeing state None, so unaware of DDL
			// tkO is seeing state write only, so using the old partition,
			// but are aware of new ids, so should filter them from global index reads.
			// Duplicate key errors (from delete only state) are allowed on insert/update,
			// even if it cannot read them from the global index, due to filtering.
			rows := tkNO.MustQuery(`select * from t`).Sort().Rows()
			tkO.MustQuery(`select * from t`).Sort().Check(rows)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateNone, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateWriteOnly, tblO.Partition.DDLState)
			require.Equal(t, tblNO.Partition.Definitions[1].ID, tblO.Partition.Definitions[1].ID)
		case "delete only":
			// tkNO is seeing state write only, so still can access the dropped partition
			// tkO is seeing state delete only, so cannot see the dropped partition,
			// but must still write to the shared global indexes.
			// So they will get errors on the same entries in the global index.

			tkNO.MustContainErrMsg(`insert into t values (1,1,"Duplicate key")`, "[kv:1062]Duplicate entry '1' for key 't.")
			tkNO.MustContainErrMsg(`insert into t values (11,1,"Duplicate key")`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")

			tkO.MustQuery(`select a from t where a = 1`).Check(testkit.Rows())
			// OK! PK violation due to old partition is still accessible!!!
			// Similar to when dropping a unique index, see TestMultiSchemaDropUniqueIndex
			tkO.MustContainErrMsg(`insert into t values (1,1,"Duplicate key")`, "[kv:1062]Duplicate entry '1' for key 't.PRIMARY'")
			// Note that PK (global) is not violated! and Unique key (b) is not global,
			// and the partition is dropped, so OK to write.
			tkO.MustExec(`insert into t values (11,1,"OK, non global unique index")`)
			// The anomaly here is that tkNO and tkO sees different versions of the table,
			// and therefore different data!
			tkO.MustQuery(`select * from t where b = 1`).Check(testkit.Rows("11 1 OK, non global unique index"))
			tkNO.MustQuery(`select * from t where b = 1`).Check(testkit.Rows("1 1 Filler"))

			tkO.MustExec(`insert into t values (13,13,"OK")`)
			tkNO.MustExec(`insert into t values (15,13,"OK, non global unique index")`)
			tkO.MustQuery(`select * from t where b = 13`).Check(testkit.Rows("13 13 OK"))
			tkNO.MustQuery(`select * from t where b = 13`).Check(testkit.Rows("15 13 OK, non global unique index"))

			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateWriteOnly, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteOnly, tblO.Partition.DDLState)
			require.NotEqual(t, tblNO.Partition.Definitions[1].ID, tblO.Partition.Definitions[1].ID)

			tkNO.MustExec(`insert into t values (21,21,"OK")`)
			tkNO.MustExec(`insert into t values (23,23,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (21,21,"Duplicate key")`, "[kv:1062]Duplicate entry '21' for key 't.PRIMARY'")
			tkO.MustContainErrMsg(`insert into t values (6,23,"Duplicate key")`, "[kv:1062]Duplicate entry '6' for key 't.PRIMARY'")
			// Primary is global, so here we cannot insert into the new partition, without
			// conflicting to the old one
			tkO.MustContainErrMsg(`insert into t values (21,25,"Duplicate key")`, "[kv:1062]Duplicate entry '21' for key 't.PRIMARY'")
			tkO.MustExec(`insert into t values (25,25,"OK")`)
			// Should be able to insert to the new partition, with a duplicate of non-global key
			tkNO.MustExec(`insert into t values (95,25,"OK, non global unique key")`)
			tkNO.MustContainErrMsg(`insert into t values (25,95,"Duplicate key")`, "[kv:1062]Duplicate entry '25' for key 't.PRIMARY'")
			// PointGet should not find new partitions for StateWriteOnly
			tkNO.MustQuery(`select count(*) from t where a = 25`).Check(testkit.Rows("0"))
			tkNO.MustQuery(`select count(*) from t where b = 25`).Check(testkit.Rows("1"))
			tkNO.MustQuery(`select * from t where b = 25`).Check(testkit.Rows("95 25 OK, non global unique key"))
			tkNO.MustExec(`update t set a = 17, c = "Updated" where b = 25`)
			require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())

			tkO.MustQuery(`select count(*) from t where b = 23`).Check(testkit.Rows("0"))
			tkO.MustExec(`update t set a = 19, c = "Updated" where b = 23`)
			require.Equal(t, uint64(0), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
			tkNO.MustQuery(`select count(*) from t where a = 23`).Check(testkit.Rows("1"))
			tkNO.MustQuery(`select * from t where a = 23`).Check(testkit.Rows("23 23 OK"))
			tkNO.MustExec(`update t set b = 10 where a = 23`)
			require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
			tkNO.MustExec(`update t set b = 23 where a = 23`)
			require.Equal(t, uint64(1), tkNO.Session().GetSessionVars().StmtCtx.AffectedRows())
			tkNO.MustContainErrMsg(`update t set b = 25 where a = 23`, "[kv:1062]Duplicate entry '25' for key 't.uk_b'")
			tkO.MustExec(`update t set b = 23 where a = 25`)
			require.Equal(t, uint64(1), tkO.Session().GetSessionVars().StmtCtx.AffectedRows())
			// non-global unique index
			// Same state's partition:
			tkO.MustContainErrMsg(`update t set b = 23 where a = 13`, "[kv:1062]Duplicate entry '23' for key 't.uk_b'")
			tkNO.MustContainErrMsg(`update t set b = 23 where a = 21`, "[kv:1062]Duplicate entry '23' for key 't.uk_b'")
			// Others state's partition:
			tkO.MustExec(`update t set b = 21, c = "Updated" where a = 13`)
			tkO.MustExec(`insert into t values (19,19, "OK")`)
			tkNO.MustExec(`update t set b = 19, c = "Updated" where a = 21`)

			// PK
			// Same state's partition:
			tkO.MustContainErrMsg(`update t set a = 13 where b = 19`, "[kv:1062]Duplicate entry '13' for key 't.PRIMARY'")
			tkNO.MustContainErrMsg(`update t set a = 7 where b = 3`, "[kv:1062]Duplicate entry '7' for key 't.PRIMARY'")
			// Others state's partition:
			tkO.MustContainErrMsg(`update t set a = 7 where b = 19`, "[kv:1062]Duplicate entry '7' for key 't.PRIMARY'")
			tkNO.MustContainErrMsg(`update t set a = 13 where b = 13`, "[kv:1062]Duplicate entry '13' for key 't.PRIMARY'")

			tkNO.MustQuery(`select * from t`).Sort().Check(testkit.Rows(""+
				"0 0 Filler",
				"1 1 Filler",
				"15 13 OK, non global unique index",
				"17 25 Updated",
				"2 2 Filler",
				"21 19 Updated",
				"23 23 OK",
				"3 3 Filler",
				"4 4 Filler",
				"5 5 Filler",
				"6 6 Filler",
				"7 7 Filler"))

			tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows(""+
				"0 0 Filler",
				"11 1 OK, non global unique index",
				"13 21 Updated",
				"19 19 OK",
				"2 2 Filler",
				"25 23 OK",
				"4 4 Filler",
				"6 6 Filler"))
			tkO.MustExec(`admin check table t`)
			tkNO.MustExec(`admin check table t`)
			// TODO: Add tests for delete as well

		case "delete reorganization":
			// tkNO is seeing state delete only, so cannot see the dropped partition,
			// but must still must give duplicate errors when writes to the global indexes collide
			// with the dropped partitions.
			// tkO is seeing state delete reorganization, so cannot see the dropped partition,
			// and can ignore the dropped partitions entries in the Global Indexes, i.e. overwrite them!
			rows := tkO.MustQuery(`select * from t`).Sort().Rows()
			tkNO.MustQuery(`select * from t`).Sort().Check(rows)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteOnly, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteReorganization, tblO.Partition.DDLState)
			require.Equal(t, tblNO.Partition.Definitions[1].ID, tblO.Partition.Definitions[1].ID)
			tkO.MustQuery(`select a, b from t where b = 1`).Check(testkit.Rows("11 1"))
			tkO.MustQuery(`select b from t where a = 1`).Check(testkit.Rows())
			tkO.MustContainErrMsg(`insert into t values (3,1,"Duplicate")`, "[kv:1062]Duplicate entry '1' for key 't.uk_b'")
			// Old partition should be OK to overwrite for tkO, but not tkNO!
			tkO.MustExec(`insert into t values (3,3,"OK")`)
			tkNO.MustContainErrMsg(`insert into t values (5,5, "Duplicate pk")`, "[kv:1062]Duplicate entry '5' for key 't.PRIMARY'")
			tkO.MustExec(`update t set a = 5 where b = 3`)
			tkNO.MustContainErrMsg(`update t set a = 7 where b = 3`, "[kv:1062]Duplicate entry '7' for key 't.PRIMARY'")
			res := tkNO.MustQuery(`select * from t`).Sort()
			res.Check(testkit.Rows(""+
				"0 0 Filler",
				"11 1 OK, non global unique index",
				"13 21 Updated",
				"19 19 OK",
				"2 2 Filler",
				"25 23 OK",
				"4 4 Filler",
				"5 3 OK",
				"6 6 Filler"))
			tkO.MustQuery(`select * from t`).Sort().Check(res.Rows())

			tkO.MustExec(`admin check table t`)
			tkNO.MustExec(`admin check table t`)
		case "none":
			tkNO.MustExec(`insert into t values (81,81,"OK")`)
			tkO.MustContainErrMsg(`insert into t values (81,81,"Duplicate key")`, "[kv:1062]Duplicate entry '81' for key 't.")
			tkNO.MustExec(`insert into t values (85,85,"OK")`)
			tkO.MustExec(`insert into t values (87,87,"OK")`)
			rows := tkNO.MustQuery(`select * from t`).Sort().Rows()
			tkO.MustQuery(`select * from t`).Sort().Check(rows)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteReorganization, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateNone, tblO.Partition.DDLState)
			require.Equal(t, tblNO.Partition.Definitions[1].ID, tblO.Partition.Definitions[1].ID)
		default:
			require.Fail(t, "Unhandled schema state", "State: '%s'", schemaState)
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn, false)
}

func TestRemovePartitioningNoPKCovering(t *testing.T) {
	createSQL := `create table t (a int unsigned, b varchar(255), c int, d varchar(255), key (b), key (c,b), key(c), key(d))` +
		` partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition pMax values less than (MAXVALUE))`
	alterSQL := `alter table t remove partitioning`
	runCoveringTest(t, createSQL, alterSQL)
}

func TestReorganizePartitionNoPKCovering(t *testing.T) {
	createSQL := `create table t (a int unsigned, b varchar(255), c int, d varchar(255), key (b), key (c,b), key(c), key(d))` +
		` partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition pMax values less than (MAXVALUE))`
	alterSQL := `alter table t reorganize partition pMax into (partition p2 values less than (30), partition p3 values less than (40), partition p4 values less than (50), partition p5 values less than (60), partition p6 values less than (70), partition p7 values less than (80), partition p8 values less than (90), partition p9 values less than (100), partition p10 values less than (110), partition pMax values less than (MAXVALUE))`
	runCoveringTest(t, createSQL, alterSQL)
}

func TestRePartitionByKeyNoPKCovering(t *testing.T) {
	createSQL := `create table t (a int unsigned, b varchar(255), c int, d varchar(255), key (b), key (c,b), key(c), key(d))` +
		` partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition pMax values less than (MAXVALUE))`
	alterSQL := `alter table t partition by key(a) partitions 3`
	runCoveringTest(t, createSQL, alterSQL)
}

func TestPartitionByKeyNoPKCovering(t *testing.T) {
	createSQL := `create table t (a int unsigned, b varchar(255), c int, d varchar(255), key (b), key (c,b), key(c), key(d))`
	alterSQL := `alter table t partition by key(a) partitions 3`
	runCoveringTest(t, createSQL, alterSQL)
}

func TestAddKeyPartitionNoPKCovering(t *testing.T) {
	createSQL := `create table t (a int unsigned, b varchar(255), c int, d varchar(255), key (b), key (c,b), key(c), key(d)) partition by key (a) partitions 3`
	alterSQL := `alter table t add partition partitions 1`
	runCoveringTest(t, createSQL, alterSQL)
}

func TestCoalesceKeyPartitionNoPKCovering(t *testing.T) {
	createSQL := `create table t (a int unsigned, b varchar(255), c int, d varchar(255), key (b), key (c,b), key(c), key(d)) partition by key (a) partitions 3`
	alterSQL := `alter table t coalesce partition 1`
	runCoveringTest(t, createSQL, alterSQL)
}

func TestRemovePartitioningCovering(t *testing.T) {
	createSQL := `create table t (a int unsigned PRIMARY KEY NONCLUSTERED, b varchar(255), c int, d varchar(255), key (b), key (c,b), key(c), key(d))` +
		` partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition pMax values less than (MAXVALUE))`
	alterSQL := `alter table t remove partitioning`
	runCoveringTest(t, createSQL, alterSQL)
}

func TestReorganizePartitionCovering(t *testing.T) {
	createSQL := `create table t (a int unsigned PRIMARY KEY NONCLUSTERED, b varchar(255), c int, d varchar(255), key (b), key (c,b), key(c), key(d))` +
		` partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition pMax values less than (MAXVALUE))`
	alterSQL := `alter table t reorganize partition pMax into (partition p2 values less than (30), partition p3 values less than (40), partition p4 values less than (50), partition p5 values less than (60), partition p6 values less than (70), partition p7 values less than (80), partition p8 values less than (90), partition p9 values less than (100), partition p10 values less than (110), partition pMax values less than (MAXVALUE))`
	runCoveringTest(t, createSQL, alterSQL)
}

func TestRePartitionByKeyCovering(t *testing.T) {
	createSQL := `create table t (a int unsigned PRIMARY KEY NONCLUSTERED, b varchar(255), c int, d varchar(255), key (b), key (c,b), key(c), key(d))` +
		` partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition pMax values less than (MAXVALUE))`
	alterSQL := `alter table t partition by key(a) partitions 3`
	runCoveringTest(t, createSQL, alterSQL)
}

func TestPartitionByKeyCovering(t *testing.T) {
	createSQL := `create table t (a int unsigned PRIMARY KEY NONCLUSTERED, b varchar(255), c int, d varchar(255), key (b), key (c,b), key(c), key(d))`
	alterSQL := `alter table t partition by key(a) partitions 3`
	runCoveringTest(t, createSQL, alterSQL)
}

func TestAddKeyPartitionCovering(t *testing.T) {
	createSQL := `create table t (a int unsigned PRIMARY KEY NONCLUSTERED, b varchar(255), c int, d varchar(255), key (b), key (c,b), key(c), key(d)) partition by key (a) partitions 3`
	alterSQL := `alter table t add partition partitions 1`
	runCoveringTest(t, createSQL, alterSQL)
}

func TestCoalesceKeyPartitionCovering(t *testing.T) {
	createSQL := `create table t (a int unsigned PRIMARY KEY NONCLUSTERED, b varchar(255), c int, d varchar(255), key (b), key (c,b), key(c), key(d)) partition by key (a) partitions 3`
	alterSQL := `alter table t coalesce partition 1`
	runCoveringTest(t, createSQL, alterSQL)
}

func runCoveringTest(t *testing.T, createSQL, alterSQL string) {
	//insert a row
	//insert a row, on duplicate key update - no match
	//insert a row, on duplicate key update - match original table - io
	//insert a row, on duplicate key update - match inserted in this state - ic
	//insert a row, on duplicate key update - match inserted in previous state - ip
	//insert a row, on duplicate key update - match inserted in next state - in
	//update a row from just inserted - ic
	//update a row from inserted in original table - io
	//update a row from inserted in previous state ip
	//update a row from inserted in next state - in
	//update a row from just updated - uc
	//update a row from updated in original table - uo
	//update a row from updated in previous state - up
	//update a row from updated in next state - un
	//delete a row from just inserted - ic
	//delete a row from inserted in original table - io
	//delete a row from inserted in previous state - ip
	//delete a row from inserted in next state - in
	//delete a row from just updated - uc
	//delete a row from updated in original table - uo
	//delete a row from updated in previous state - up
	//delete a row from updated in next state - un

	currID := 1
	const (
		Insert     = 0
		Update     = 1
		Delete     = 2
		InsertODKU = 3
		Original   = 0
		Previous   = 1
		Current    = 2
	)
	// dimensions: execute state, from state, type, IDs
	states := 7
	fromStates := 3
	IDs := make([][][][]int, states)
	for s := 0; s < states; s++ {
		IDs[s] = make([][][]int, fromStates)
		for f := 0; f < fromStates; f++ {
			IDs[s][f] = make([][]int, 4)
		}
	}
	// Skip first state, since it is none, i.e. before the DDL started...
	for s := states - 1; s > 0; s-- {
		// Check operation against 'before DDL'
		IDs[s][Current][Delete] = append(IDs[s][Current][Delete], currID)
		IDs[s][Original][Insert] = append(IDs[s][Original][Insert], currID)
		currID++
		IDs[s][Current][Update] = append(IDs[s][Current][Update], currID)
		IDs[s][Original][Insert] = append(IDs[s][Original][Insert], currID)
		currID++
		IDs[s][Current][InsertODKU] = append(IDs[s][Current][InsertODKU], currID)
		IDs[s][Original][Insert] = append(IDs[s][Original][Insert], currID)
		currID++
		for _, from := range []int{Previous, Current} {
			// Check operation against previous and current state
			IDs[s][Current][Delete] = append(IDs[s][Current][Delete], currID)
			IDs[s][from][Update] = append(IDs[s][from][Update], currID)
			IDs[s][from][Insert] = append(IDs[s][from][Insert], currID)
			currID++
			IDs[s][Current][Delete] = append(IDs[s][Current][Delete], currID)
			IDs[s][from][Insert] = append(IDs[s][from][Insert], currID)
			currID++
			IDs[s][Current][Update] = append(IDs[s][Current][Update], currID)
			IDs[s][from][Insert] = append(IDs[s][from][Insert], currID)
			currID++
			IDs[s][Current][InsertODKU] = append(IDs[s][Current][InsertODKU], currID)
			IDs[s][from][Insert] = append(IDs[s][from][Insert], currID)
			currID++
		}
		// Check against Next state, use 'Previous' as current and 'Current' as Next.
		IDs[s][Previous][Delete] = append(IDs[s][Previous][Delete], currID)
		IDs[s][Current][Update] = append(IDs[s][Current][Update], currID)
		IDs[s][Current][Insert] = append(IDs[s][Current][Insert], currID)
		currID++
		IDs[s][Previous][Delete] = append(IDs[s][Previous][Delete], currID)
		IDs[s][Current][Insert] = append(IDs[s][Current][Insert], currID)
		currID++
		IDs[s][Previous][Update] = append(IDs[s][Previous][Update], currID)
		IDs[s][Current][Insert] = append(IDs[s][Current][Insert], currID)
		currID++
		IDs[s][Previous][InsertODKU] = append(IDs[s][Previous][InsertODKU], currID)
		IDs[s][Current][Insert] = append(IDs[s][Current][Insert], currID)
		currID++

		// Normal inserts to keep
		IDs[s][Current][Insert] = append(IDs[s][Current][Insert], currID)
		currID++
		IDs[s][Current][InsertODKU] = append(IDs[s][Current][InsertODKU], currID)
		currID++
	}
	require.Equal(t, 103, currID)

	// Run like this:
	// prepare in previous state + run in Current
	//   use tkNO for previous state
	//   use tkO for Current state
	//   for x in range IDs[s][Previous][Insert]
	//   for x in range IDs[s][Current][Insert]
	//   for x in range IDs[s][Previous][Update]
	//   for x in range IDs[s][Current][Update]
	//   for x in range IDs[s][Previous][Delete]
	//   for x in range IDs[s][Current][Delete]
	//   for x in range IDs[s][Previous][InsertODKU]
	//   for x in range IDs[s][Current][InsertODKU]
	hasUniqueKey := false
	initFn := func(tkO *testkit.TestKit) {
		logutil.BgLogger().Info("initFn start")
		ctx := tkO.Session()
		dom := domain.GetDomain(ctx)
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		hasUniqueKey = tbl.Meta().PKIsHandle || tbl.Meta().IsCommonHandle
		for _, idx := range tbl.Meta().Indices {
			if idx.Unique {
				hasUniqueKey = true
			}
		}
		for s := range IDs {
			for _, id := range IDs[s][Original][Insert] {
				//if id != 53 {
				//	continue
				//}
				sql := fmt.Sprintf(`insert into t values (%d,%d,%d,'Original s:%d')`, id, id, id, s)
				tkO.MustExec(sql)
				logutil.BgLogger().Info("run sql", zap.String("sql", sql))
			}
		}
		// make all partitions to be EXCHANGED, so they have duplicated _tidb_rowid's between
		// the partitions
		if tbl.Meta().Partition != nil &&
			!tbl.Meta().IsCommonHandle &&
			!tbl.Meta().PKIsHandle {
			for _, def := range tbl.Meta().Partition.Definitions {
				partName := def.Name.O
				tkO.MustExec(`create table tx like t`)
				tkO.MustExec(`alter table tx remove partitioning`)
				tkO.MustExec(fmt.Sprintf("insert into tx select * from t partition(`%s`)", partName))
				res := tkO.MustQuery(`select *, _tidb_rowid from tx`)
				logutil.BgLogger().Info("rows in Exchanged table", zap.Any("rows", res.Rows()))
				// Somehow there is an issue internally when using WITH VALIDATION,
				// giving test.tx does not exist error...
				tkO.MustExec(fmt.Sprintf("alter table t exchange partition `%s` with table tx without validation", partName))
				tkO.MustExec(`drop table tx`)
			}
		}
		tkO.MustQuery(`select count(*) from t`).Check(testkit.Rows("18"))
		logutil.BgLogger().Info("initFn Done")
	}

	state := 1
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		logutil.BgLogger().Info("loopFn start", zap.Int("state", state))
		if state >= len(IDs) {
			// Reset state for validation against non-partitioned table
			state = 1
		}
		for _, op := range []int{Insert, Update, Delete, InsertODKU} {
			for _, from := range []int{Previous, Current} {
				tk := tkO
				if from == Previous {
					tk = tkNO
				}
				for _, id := range IDs[state][from][op] {
					//if id != 53 {
					//	continue
					//}
					var sql string
					switch op {
					case Insert:
						sql = fmt.Sprintf(`insert into t values (%d,%d,%d,'Insert s:%d f:%d')`, id, id, id, state, from)
					case Update:
						sql = fmt.Sprintf(`update t set b = %d, d = concat(d, ' Update s:%d f:%d') where a = %d`, id+currID, state, from, id)
					case Delete:
						sql = fmt.Sprintf(`delete from t where a = %d /* s:%d f:%d */`, id, state, from)
					case InsertODKU:
						sql = fmt.Sprintf(`insert into t values (%d, %d, %d, 'InsertODKU s:%d f:%d') on duplicate key update b = %d, d = concat(d, ' ODKU s:%d f:%d')`, id, id, id, state, from, id+currID, state, from)
					default:
						require.Fail(t, "unknown op", "op: %d", op)
					}
					logutil.BgLogger().Info("run sql", zap.String("sql", sql))
					//tk.MustQuery(`select *, _tidb_rowid from t`).Check(testkit.Rows("53 53 53 Original s:3 1"))
					tk.MustExec(sql)
				}
			}
		}
		logutil.BgLogger().Info("loopFn done", zap.Int("state", state))
		state++
	}
	postFn := func(tkO *testkit.TestKit, _ kv.Storage) {
		// TODO: Enable this and fix issues!!!
		//tkO.MustExec(`admin check table t`)
		// Total number of rows after above operations.
		// Just to check for duplicates or missing rows
		// TODO: Fix this for non-PK tests
		//tkO.MustQuery(`select count(*) from t`).Check(testkit.Rows("61"))
		res := tkO.MustQuery(`select * from t`).Sort()
		if hasUniqueKey {
			tkO.MustQuery(`select a from t group by a having count(*) > 1`).Check(testkit.Rows())
			res.Check(testkit.Rows(""+
				"10 113 10 Insert s:6 f:2 Update s:6 f:2",
				"100 203 100 Insert s:1 f:2 ODKU s:1 f:1",
				"101 101 101 Insert s:1 f:2",
				"102 102 102 InsertODKU s:1 f:2",
				"11 114 11 Insert s:6 f:2 ODKU s:6 f:2",
				"14 117 14 Insert s:6 f:2 Update s:6 f:1",
				"15 118 15 Insert s:6 f:2 ODKU s:6 f:1",
				"16 16 16 Insert s:6 f:2",
				"17 17 17 InsertODKU s:6 f:2",
				"19 122 19 Original s:5 Update s:5 f:2",
				"2 105 2 Original s:6 Update s:6 f:2",
				"20 123 20 Original s:5 ODKU s:5 f:2",
				"23 126 23 Insert s:5 f:1 Update s:5 f:2",
				"24 127 24 Insert s:5 f:1 ODKU s:5 f:2",
				"27 130 27 Insert s:5 f:2 Update s:5 f:2",
				"28 131 28 Insert s:5 f:2 ODKU s:5 f:2",
				"3 106 3 Original s:6 ODKU s:6 f:2",
				"31 134 31 Insert s:5 f:2 Update s:5 f:1",
				"32 135 32 Insert s:5 f:2 ODKU s:5 f:1",
				"33 33 33 Insert s:5 f:2",
				"34 34 34 InsertODKU s:5 f:2",
				"36 139 36 Original s:4 Update s:4 f:2",
				"37 140 37 Original s:4 ODKU s:4 f:2",
				"40 143 40 Insert s:4 f:1 Update s:4 f:2",
				"41 144 41 Insert s:4 f:1 ODKU s:4 f:2",
				"44 147 44 Insert s:4 f:2 Update s:4 f:2",
				"45 148 45 Insert s:4 f:2 ODKU s:4 f:2",
				"48 151 48 Insert s:4 f:2 Update s:4 f:1",
				"49 152 49 Insert s:4 f:2 ODKU s:4 f:1",
				"50 50 50 Insert s:4 f:2",
				"51 51 51 InsertODKU s:4 f:2",
				"52 52 52 Original s:3",
				// TODO: FIXME: Why duplicate here?!?
				"53 156 53 Original s:3 Update s:3 f:2",
				"53 53 53 Original s:3",
				// TODO: FIXME: Why duplicate here?!?
				"54 157 54 Original s:3 ODKU s:3 f:2",
				"54 54 54 Original s:3",
				"57 160 57 Insert s:3 f:1 Update s:3 f:2",
				"58 161 58 Insert s:3 f:1 ODKU s:3 f:2",
				"6 109 6 Insert s:6 f:1 Update s:6 f:2",
				"61 164 61 Insert s:3 f:2 Update s:3 f:2",
				"62 165 62 Insert s:3 f:2 ODKU s:3 f:2",
				"65 168 65 Insert s:3 f:2 Update s:3 f:1",
				"66 169 66 Insert s:3 f:2 ODKU s:3 f:1",
				"67 67 67 Insert s:3 f:2",
				"68 68 68 InsertODKU s:3 f:2",
				"7 110 7 Insert s:6 f:1 ODKU s:6 f:2",
				"70 173 70 Original s:2 Update s:2 f:2",
				"71 174 71 Original s:2 ODKU s:2 f:2",
				"74 177 74 Insert s:2 f:1 Update s:2 f:2",
				"75 178 75 Insert s:2 f:1 ODKU s:2 f:2",
				"78 181 78 Insert s:2 f:2 Update s:2 f:2",
				"79 182 79 Insert s:2 f:2 ODKU s:2 f:2",
				"82 185 82 Insert s:2 f:2 Update s:2 f:1",
				"83 186 83 Insert s:2 f:2 ODKU s:2 f:1",
				"84 84 84 Insert s:2 f:2",
				"85 85 85 InsertODKU s:2 f:2",
				"87 190 87 Original s:1 Update s:1 f:2",
				"88 191 88 Original s:1 ODKU s:1 f:2",
				"91 194 91 Insert s:1 f:1 Update s:1 f:2",
				"92 195 92 Insert s:1 f:1 ODKU s:1 f:2",
				"95 198 95 Insert s:1 f:2 Update s:1 f:2",
				"96 199 96 Insert s:1 f:2 ODKU s:1 f:2",
				"99 202 99 Insert s:1 f:2 Update s:1 f:1"))
		} else {
			res.Sort().Check(testkit.Rows(""+
				"10 113 10 Insert s:6 f:2 Update s:6 f:2",
				"100 100 100 Insert s:1 f:2",
				"100 100 100 InsertODKU s:1 f:1",
				"101 101 101 Insert s:1 f:2",
				"102 102 102 InsertODKU s:1 f:2",
				"11 11 11 Insert s:6 f:2",
				"11 11 11 InsertODKU s:6 f:2",
				"14 117 14 Insert s:6 f:2 Update s:6 f:1",
				"15 15 15 Insert s:6 f:2",
				"15 15 15 InsertODKU s:6 f:1",
				"16 16 16 Insert s:6 f:2",
				"17 17 17 InsertODKU s:6 f:2",
				"19 122 19 Original s:5 Update s:5 f:2",
				"2 105 2 Original s:6 Update s:6 f:2",
				"20 20 20 InsertODKU s:5 f:2",
				"20 20 20 Original s:5",
				"23 126 23 Insert s:5 f:1 Update s:5 f:2",
				"24 24 24 Insert s:5 f:1",
				"24 24 24 InsertODKU s:5 f:2",
				"27 130 27 Insert s:5 f:2 Update s:5 f:2",
				"28 28 28 Insert s:5 f:2",
				"28 28 28 InsertODKU s:5 f:2",
				"3 3 3 InsertODKU s:6 f:2",
				"3 3 3 Original s:6",
				"31 134 31 Insert s:5 f:2 Update s:5 f:1",
				"32 32 32 Insert s:5 f:2",
				"32 32 32 InsertODKU s:5 f:1",
				"33 33 33 Insert s:5 f:2",
				"34 34 34 InsertODKU s:5 f:2",
				"36 139 36 Original s:4 Update s:4 f:2",
				"37 37 37 InsertODKU s:4 f:2",
				"37 37 37 Original s:4",
				"40 143 40 Insert s:4 f:1 Update s:4 f:2",
				"41 41 41 Insert s:4 f:1",
				"41 41 41 InsertODKU s:4 f:2",
				"44 147 44 Insert s:4 f:2 Update s:4 f:2",
				"45 45 45 Insert s:4 f:2",
				"45 45 45 InsertODKU s:4 f:2",
				"48 151 48 Insert s:4 f:2 Update s:4 f:1",
				"49 49 49 Insert s:4 f:2",
				"49 49 49 InsertODKU s:4 f:1",
				"50 50 50 Insert s:4 f:2",
				"51 51 51 InsertODKU s:4 f:2",
				"52 52 52 Original s:3",
				"53 156 53 Original s:3 Update s:3 f:2",
				"53 53 53 Original s:3",
				"54 54 54 InsertODKU s:3 f:2",
				"54 54 54 Original s:3",
				"57 160 57 Insert s:3 f:1 Update s:3 f:2",
				"58 58 58 Insert s:3 f:1",
				"58 58 58 InsertODKU s:3 f:2",
				"6 109 6 Insert s:6 f:1 Update s:6 f:2",
				"61 164 61 Insert s:3 f:2 Update s:3 f:2",
				"62 62 62 Insert s:3 f:2",
				"62 62 62 InsertODKU s:3 f:2",
				"65 168 65 Insert s:3 f:2 Update s:3 f:1",
				"66 66 66 Insert s:3 f:2",
				"66 66 66 InsertODKU s:3 f:1",
				"67 67 67 Insert s:3 f:2",
				"68 68 68 InsertODKU s:3 f:2",
				"7 7 7 Insert s:6 f:1",
				"7 7 7 InsertODKU s:6 f:2",
				"70 173 70 Original s:2 Update s:2 f:2",
				"71 71 71 InsertODKU s:2 f:2",
				"71 71 71 Original s:2",
				"74 177 74 Insert s:2 f:1 Update s:2 f:2",
				"75 75 75 Insert s:2 f:1",
				"75 75 75 InsertODKU s:2 f:2",
				"78 181 78 Insert s:2 f:2 Update s:2 f:2",
				"79 79 79 Insert s:2 f:2",
				"79 79 79 InsertODKU s:2 f:2",
				"82 185 82 Insert s:2 f:2 Update s:2 f:1",
				"83 83 83 Insert s:2 f:2",
				"83 83 83 InsertODKU s:2 f:1",
				"84 84 84 Insert s:2 f:2",
				"85 85 85 InsertODKU s:2 f:2",
				"87 190 87 Original s:1 Update s:1 f:2",
				"88 88 88 InsertODKU s:1 f:2",
				"88 88 88 Original s:1",
				"91 194 91 Insert s:1 f:1 Update s:1 f:2",
				"92 92 92 Insert s:1 f:1",
				"92 92 92 InsertODKU s:1 f:2",
				"95 198 95 Insert s:1 f:2 Update s:1 f:2",
				"96 96 96 Insert s:1 f:2",
				"96 96 96 InsertODKU s:1 f:2",
				"99 202 99 Insert s:1 f:2 Update s:1 f:1"))
		}
		logutil.BgLogger().Info("postFn done", zap.Int("state", state))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, true)
}

func TestIssue58692(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index idx(a)) partition by hash(a) partitions 5")
	tk.MustExec("insert into t (a, b) values (1, 1), (2, 2), (3, 3)")
	var i atomic.Int32
	i.Store(3)
	done := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		tk2 := testkit.NewTestKit(t, store)
		tmp := i.Add(1)
		tk2.MustExec(fmt.Sprintf("insert into test.t values (%d, %d)", tmp, tmp))
		tk2.MustExec(fmt.Sprintf("update test.t set b = b + 11, a = b where b = %d", tmp-1))
		if tmp == 10 {
			close(done)
		}
	})
	tk.MustExec("alter table t remove partitioning")
	// about 1% of the runs, the alter returns before the failpoint finishes,
	// which causes a failure with '9 20' from table and '9 9' from index.
	// So wait until done :)
	// See https://github.com/pingcap/tidb/pull/58902#discussion_r2001031467
	<-done
	rsIndex := tk.MustQuery("select *,_tidb_rowid from t use index(idx)").Sort()
	rsTable := tk.MustQuery("select *,_tidb_rowid from t use index()").Sort()
	tk.MustExec("admin check table t")
	tk.MustQuery("select * from t where b = 20").Check(testkit.Rows("9 20"))
	tk.MustQuery("select * from t use index(idx) where a = 9").Check(testkit.Rows("9 20"))
	require.Equal(t, rsIndex.String(), rsTable.String(), "Expected: from index, Actual: from table")
}

func TestDuplicateRowsNoPK(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, key idx (a)) partition by hash(a) partitions 2")
	tk.MustExec("insert into t (a, b) values (1, 1)")
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
		if job.SchemaState != model.StateDeleteReorganization {
			return
		}
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")

		tk2.MustExec("update t set b = 2 where b = 1")
	})
	tk.MustExec("alter table t remove partitioning")
	// TODO: FIXME!
	tk.MustContainErrMsg(`admin check table t`, "[admin:8223]data inconsistency in table: t, index: idx, handle: 1, index-values:")
}

func TestDuplicateRowsPK59680(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, primary key (a) nonclustered) partition by hash(a) partitions 2")
	tk.MustExec("insert into t (a, b) values (1, 1)")
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
		if job.SchemaState != model.StateDeleteReorganization {
			return
		}
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")

		tk2.MustExec("update t set b = 2 where b = 1")
	})
	tk.MustExec("alter table t remove partitioning")
	// TODO: FIXME!
	tk.MustContainErrMsg(`admin check table t`, "[admin:8223]data inconsistency in table: t, index: PRIMARY, handle: ")
}

func TestIssue58864(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, primary key (a) nonclustered) partition by hash(a) partitions 2")
	tk.MustExec("insert into t (a, b) values (1, 1)")
	var i atomic.Int32
	i.Store(1)

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
		if job.State != model.JobStateDone {
			return
		}
		val := int(i.Add(1))
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")

		tk2.MustExec("insert into t values (?, ?)", val, val)
		tk2.MustExec("update t set b = b + 1 where a = ?", val)
	})
	tk.MustExec("alter table t remove partitioning")
}
