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
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/gcworker"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
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

			tkNO.MustQuery(`select * from t where c = "2"`).Sort().Check(testkit.Rows("2 2 2"))
			tkNO.MustQuery(`select * from t where b = "3"`).Sort().Check(testkit.Rows("3 3 3"))
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
			zap.Int64("owner domain schema version", tkO.Session().GetDomainInfoSchema().SchemaMetaVersion()),
			zap.String("non-owner table", tkNO.MustQuery("select * from t use index()").Sort().String()),
			zap.String("non-owner index", tkNO.MustQuery("select * from t use index(k_b)").Sort().String()),
			zap.String("non-owner index id", tkNO.MustQuery(indexIDSQL).String()),
			zap.Int64("non-owner txn schema version", sessiontxn.GetTxnManager(tkNO.Session()).GetTxnInfoSchema().SchemaMetaVersion()),
			zap.Int64("non-owner domain schema version", tkNO.Session().GetDomainInfoSchema().SchemaMetaVersion()),
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
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
			require.NoError(t, err)
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
		sort.Slice(globalIndexes, func(i, j int) bool { return globalIndexes[i] < globalIndexes[j] })
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
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
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
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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

	tkO.MustExec("set session tidb_enable_fast_table_check = off")
	tkO.MustExec(`admin check table t`)

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
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", func(job *model.Job) {
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
		tkO.MustExec(fmt.Sprintf(`admin check table t /* state: %s */`, state.String()))
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
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter")
	logutil.BgLogger().Info("XXXXXXXXXXX states loop done")
	tkO.MustExec(`admin check table t`)
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
	tbl, err = is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	newTableID := tbl.Meta().ID
	if tableID != newTableID {
		require.False(t, HaveEntriesForTableIndex(t, tkO, tableID, 0), "Old table id %d has still entries!", tableID)
	}
	checkTableAndIndexEntries(t, tkO, originalPartitions)

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
			"1 1 init O",
			"10 30004 delete reorganization NO",
			"11 7 public O",
			"12 30005 public NO",
			"13 8 none O",
			"14 30006 none NO",
			"2 2 init O",
			"3 3 delete only O",
			"4 30001 delete only NO",
			"5 4 write only O",
			// Before, there were a DUPLICATE ROW here!!!
			//"5 60004 write only O",
			"6 30002 write only NO",
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
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateNone, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateWriteOnly, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteOnly, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteReorganization, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateNone, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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

			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateWriteOnly, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteOnly, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
			tblNO, err := tkNO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
			require.NoError(t, err)
			require.Equal(t, model.StateDeleteReorganization, tblNO.Partition.DDLState)
			tblO, err := tkO.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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

func exchangeAllPartitionsToGetDuplicateTiDBRowIDs(t *testing.T, tk *testkit.TestKit) {
	ctx := tk.Session()
	dom := domain.GetDomain(ctx)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	// make all partitions to be EXCHANGED, so they have duplicated _tidb_rowid's between
	// the partitions
	if tbl.Meta().Partition != nil &&
		!tbl.Meta().HasClusteredIndex() {
		for _, def := range tbl.Meta().Partition.Definitions {
			partName := def.Name.O
			tk.MustExec(`create table tx like t`)
			tk.MustExec(`alter table tx remove partitioning`)
			tk.MustExec(fmt.Sprintf("insert into tx select * from t partition(`%s`) order by a", partName))
			tk.MustExec(fmt.Sprintf("alter table t exchange partition `%s` with table tx without validation", partName))
			tk.MustExec(`drop table tx`)
		}
	}
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
		tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
		require.NoError(t, err)
		hasUniqueKey = tbl.Meta().HasClusteredIndex()
		if !hasUniqueKey {
			for _, idx := range tbl.Meta().Indices {
				if idx.Unique {
					hasUniqueKey = true
				}
			}
		}
		for s := range IDs {
			for _, id := range IDs[s][Original][Insert] {
				sql := fmt.Sprintf(`insert into t values (%d,%d,%d,'Original s:%d')`, id, id, id, s)
				tkO.MustExec(sql)
				logutil.BgLogger().Info("run sql", zap.String("sql", sql))
			}
		}
		// make all partitions to be EXCHANGED, so they have duplicated _tidb_rowid's between
		// the partitions
		exchangeAllPartitionsToGetDuplicateTiDBRowIDs(t, tkO)
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
					tk.MustExec(sql)
				}
			}
		}
		logutil.BgLogger().Info("loopFn done", zap.Int("state", state))
		state++
	}
	postFn := func(tkO *testkit.TestKit, _ kv.Storage) {
		tkO.MustExec(`admin check table t`)
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
				"53 156 53 Original s:3 Update s:3 f:2",
				"54 157 54 Original s:3 ODKU s:3 f:2",
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
				// There are duplicate of InsertODKU is because no unique index, so no Duplicate Key!
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
				"53 156 53 Original s:3 Update s:3 f:2",
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
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", func(job *model.Job) {
		tk2 := testkit.NewTestKit(t, store)
		tmp := i.Add(1)
		tk2.MustExec(fmt.Sprintf("insert into test.t values (%d, %d)", tmp, tmp))
		tk2.MustExec(fmt.Sprintf("update test.t set b = b + 11, a = b where b = %d", tmp-1))
		if tmp == 10 {
			close(done)
		}
	})
	tk.MustExec("alter table t remove partitioning")
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
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", func(job *model.Job) {
		if job.SchemaState != model.StateDeleteReorganization {
			return
		}
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")

		tk2.MustExec("update t set b = 2 where b = 1")
	})
	tk.MustExec("alter table t remove partitioning")
	tk.MustExec(`admin check table t`)
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1 2"))
	tk.MustQuery("select a from t").Check(testkit.Rows("1"))
	tk.MustQuery("select *, _tidb_rowid from t").Sort().Check(testkit.Rows("1 2 1"))
}

func TestDuplicateRowsPK59680(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, primary key (a) nonclustered) partition by hash(a) partitions 2")
	tk.MustExec("insert into t (a, b) values (1, 1)")
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", func(job *model.Job) {
		if job.SchemaState != model.StateDeleteReorganization {
			return
		}
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")

		tk2.MustExec("update t set b = 2 where b = 1")
	})
	tk.MustExec("alter table t remove partitioning")
	tk.MustExec(`admin check table t`)
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1 2"))
	tk.MustQuery("select a from t").Check(testkit.Rows("1"))
	tk.MustQuery("select *, _tidb_rowid from t").Sort().Check(testkit.Rows("1 2 1"))
}

func TestIssue58864(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, primary key (a) nonclustered) partition by hash(a) partitions 2")
	tk.MustExec("insert into t (a, b) values (1, 1)")
	var i atomic.Int32
	i.Store(1)

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", func(job *model.Job) {
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

func TestMultiSchemaNewTiDBRowID(t *testing.T) {
	createSQL := "CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  KEY `idx_a` (`a`),\n" +
		"  KEY `idx_b` (`b`),\n" +
		"  KEY `idx_ab` (`a`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY HASH (`a`) PARTITIONS 4"
	oldTableDef := "t " + createSQL
	newTableDef := oldTableDef
	newTableDef = newTableDef[:len(newTableDef)-1] + "3"
	var rows int
	initFn := func(tk *testkit.TestKit) {
		tk.MustExec("insert into t (a, b) values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)")
		tk.MustExec("insert into t (a,b) select a+10,b+10 from t order by a")
		tk.MustExec("create table tx0 (a int, b int, index idx_a (a), index idx_b (b), index idx_ab (a,b))")
		tk.MustExec("insert into tx0 select * from t partition(p0)")
		tk.MustExec("alter table t exchange partition p0 with table tx0 without validation")
		tk.MustExec("create table tx1 (a int, b int, index idx_a (a), index idx_b (b), index idx_ab (a,b))")
		tk.MustExec("insert into tx1 (a,b) select a,b from t where a % 4 = 1 order by a")
		tk.MustExec("create table tx2 (a int, b int, index idx_a (a), index idx_b (b), index idx_ab (a,b))")
		tk.MustExec("insert into tx2 (a,b) select a,b from t where a % 4 = 2 order by a")
		tk.MustExec("create table tx3 (a int, b int, index idx_a (a), index idx_b (b), index idx_ab (a,b))")
		tk.MustExec("insert into tx3 (a,b) select a,b from t where a % 4 = 3 order by a")
		tk.MustExec("alter table t exchange partition p1 with table tx1 without validation")
		tk.MustExec("alter table t exchange partition p2 with table tx2 without validation")
		tk.MustExec("alter table t exchange partition p3 with table tx3 without validation")
		tk.MustExec("drop table tx0, tx1, tx2, tx3")
		res := tk.MustQuery("select *, _tidb_rowid from t")
		rows = len(res.Rows())
		res.Sort().Check(testkit.Rows(
			"1 1 1",
			"10 10 3",
			"11 11 3",
			"12 12 3",
			"13 13 4",
			"14 14 4",
			"15 15 4",
			"16 16 4",
			"17 17 5",
			"18 18 5",
			"19 19 5",
			"2 2 1",
			"20 20 5",
			"3 3 1",
			"4 4 1",
			"5 5 2",
			"6 6 2",
			"7 7 2",
			"8 8 2",
			"9 9 3"))
	}
	alterSQL := `alter table t coalesce partition 3`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		/* For debugging only
		tableAndPartIDs := getTableAndPartitionIDs(t, tkO)
		for i := range tableAndPartIDs {
			logutil.BgLogger().Info("Have old entries?", zap.Int("i", i), zap.String("state", schemaState), zap.Bool("rows", HaveEntriesForTableIndex(t, tkO, tableAndPartIDs[i], 0)))
		}
		newPartIDs := getAddingPartitionIDs(t, tkO)
		for i := range newPartIDs {
			logutil.BgLogger().Info("Have new entries?", zap.Int("i", i), zap.String("state", schemaState), zap.Bool("rows", HaveEntriesForTableIndex(t, tkO, newPartIDs[i], 0)))
		}
		*/
		tkO.MustExec("admin check table t /* " + schemaState + " */")
		switch schemaState {
		case "delete only":
			// Cannot do any test for updated _tidb_rowid
		case "write only":
			// Test if WriteReorganize will update the right New Partitions or create duplicates?
			// Test two consecutive updates:
			// - First update creates a new _tidb_rowid in old partition, is the _tidb_rowid also written to the new ones?
			// - Second update will it update the correct _tidb_rowid in the new partition?
			// 24 % 4 = 0, 24 % 3 = 0
			tkO.MustExec("update t set a = 24 where a = 1")
			/* For debugging only
			logutil.BgLogger().Info("Have old before entries 1 -> 24?", zap.Bool("rows", HaveEntriesForTableIndex(t, tkO, tableAndPartIDs[1+1%(len(tableAndPartIDs)-1)], 0)))
			logutil.BgLogger().Info("Have old after entries 1 -> 24?", zap.Bool("rows", HaveEntriesForTableIndex(t, tkO, tableAndPartIDs[1+24%(len(tableAndPartIDs)-1)], 0)))
			logutil.BgLogger().Info("Have new before entries 1 -> 24?", zap.Bool("rows", HaveEntriesForTableIndex(t, tkO, newPartIDs[1%len(newPartIDs)], 0)))
			logutil.BgLogger().Info("Have new after entries 1 -> 24?", zap.Bool("rows", HaveEntriesForTableIndex(t, tkO, newPartIDs[24%len(newPartIDs)], 0)))
			*/
			tkO.MustQuery("select a,b,_tidb_rowid from t where a = 24 or a = 1").Sort().Check(testkit.Rows("24 1 21"))
			tkNO.MustQuery("select a,b,_tidb_rowid from t where a = 24 or a = 1").Sort().Check(testkit.Rows("24 1 21"))
			tkO.MustExec("admin check table t")
			// 25 % 4 = 1, 25 % 3 = 1
			tkO.MustExec("update t set a = 25 where a = 24")
			/* For debugging only
			logutil.BgLogger().Info("Have old before entries 24 -> 25?", zap.Bool("rows", HaveEntriesForTableIndex(t, tkO, tableAndPartIDs[1+24%(len(tableAndPartIDs)-1)], 0)))
			logutil.BgLogger().Info("Have old after entries 24 -> 25?", zap.Bool("rows", HaveEntriesForTableIndex(t, tkO, tableAndPartIDs[1+25%(len(tableAndPartIDs)-1)], 0)))
			logutil.BgLogger().Info("Have new before entries 24 -> 25?", zap.Bool("rows", HaveEntriesForTableIndex(t, tkO, newPartIDs[24%len(newPartIDs)], 0)))
			logutil.BgLogger().Info("Have new after entries 24 -> 25?", zap.Bool("rows", HaveEntriesForTableIndex(t, tkO, newPartIDs[25%len(newPartIDs)], 0)))
			*/
			tkO.MustQuery("select a,b,_tidb_rowid from t where a = 25 or a = 24 or a = 1").Sort().Check(testkit.Rows("25 1 22"))
			tkNO.MustQuery("select a,b,_tidb_rowid from t where a = 25 or a = 24 or a = 1").Sort().Check(testkit.Rows("25 1 22"))
			tkO.MustExec("admin check table t")
			// Test the same but with a delete instead of in second query
			// 26 % 4 = 2, 26 % 3 = 2
			tkO.MustExec("update t set a = 26 where a = 3")
			tkO.MustExec("delete from t where a = 26")
			rows--

			// - First update creates a new _tidb_rowid in new partition only
			// - Second update will it update the correct _tidb_rowid in the new partition?
			// 22 % 4 = 2, 22 % 3 = 1
			tkO.MustExec("update t set a = 22 where a = 2")
			// Will this create yet new duplicates or not?
			// 42 % 4 = 2, 42 % 3 = 0
			tkO.MustExec("update t set a = 42 where a = 22")
			// Test the same but with a delete instead of in second query
			// 12 % 4 = 0, 12 % 3 = 0, 23 % 4 = 3, 23 % 3 = 2
			tkO.MustExec("update t set a = 23 where a = 12")
			tkO.MustExec("delete from t where a = 23")
			rows--
			// Before backfill:
			tkO.MustQuery("select a, b, _tidb_rowid from t").Sort().Check(testkit.Rows(""+
				"10 10 3",
				"11 11 3",
				"13 13 4",
				"14 14 4",
				"15 15 4",
				"16 16 4",
				"17 17 5",
				"18 18 5",
				"19 19 5",
				"20 20 5",
				"25 1 22",
				"4 4 1",
				"42 2 1",
				"5 5 2",
				"6 6 2",
				"7 7 2",
				"8 8 2",
				"9 9 3"))
			// TODO: More variants?
		case "write reorganization":
			// Is this before, during or after backfill?
		case "delete reorganization":
			// 'new' different, 'old' different
			tkO.MustExec("admin check table t")
			tkNO.MustExec("admin check table t")
			// after backfill:
			tkO.MustQuery("select a, b, _tidb_rowid from t").Sort().Check(testkit.Rows(""+
				"10 10 30006",
				"11 11 30010",
				"13 13 30003",
				"14 14 30007",
				"15 15 30011",
				"16 16 4",
				"17 17 30004",
				"18 18 30008",
				"19 19 30012",
				"20 20 5",
				"25 1 22",
				"4 4 30001",
				"42 2 1",
				"5 5 30002",
				"6 6 30005",
				"7 7 30009",
				"8 8 2",
				"9 9 3"))
			tkNO.MustQuery("select a, b, _tidb_rowid from t").Sort().Check(testkit.Rows(""+
				"10 10 30006",
				"11 11 30010",
				"13 13 30003",
				"14 14 30007",
				"15 15 30011",
				"16 16 4",
				"17 17 30004",
				"18 18 30008",
				"19 19 30012",
				"20 20 5",
				"25 1 22",
				"4 4 30001",
				"42 2 1",
				"5 5 30002",
				"6 6 30005",
				"7 7 30009",
				"8 8 2",
				"9 9 3"))
			// 13 % 4 = 1, 13 % 3 = 1, 36 % 4 = 0, 36 % 3 = 0
			tkO.MustQuery("select *, _tidb_rowid from t where a = 13").Check(testkit.Rows("13 13 30003"))
			tkNO.MustQuery("select *, _tidb_rowid from t where a = 13").Check(testkit.Rows("13 13 30003"))
			tkO.MustExec("update t set a = 36 where a = 13")
			// 38 % 4 = 2, 38 % 3 = 2
			tkO.MustExec("update t set a = 38 where a = 36")
			// Test the same but with a delete instead of in second query
			// 14 % 4 = 2, 14 % 3 = 2, 37 % 4 = 1, 37 % 3 = 1
			tkO.MustExec("update t set a = 37 where a = 14")
			tkO.MustExec("delete from t where a = 37")
			rows--

			// 'new' same, 'old' different
			// 6 % 4 = 2, 6 % 3 = 0, 21 % 4 = 1, 21 % 3 = 0
			tkO.MustExec("update t set a = 21 where a = 6")
			// 41 % 4 = 1, 41 % 3 = 2
			// Will this create yet new duplicates or not?
			tkO.MustExec("update t set a = 41 where a = 21")
			// Test the same but with a delete instead of in second query
			// 7 % 4 = 3, 7 % 3 = 1, 28 % 4 = 0, 28 % 3 = 1
			tkO.MustExec("update t set a = 28 where a = 7")
			tkO.MustExec("delete from t where a = 28")
			rows--

			// TODO: Also do the opposite, i.e. change the 'old' and check the 'new' with tkNO (old) and tkO (new)
			// Check new _tidb_rowid's, i.e. anything apart from p0
		case "public":
		case "none":
		default:
			require.Fail(t, "unhandled schema state", "State: '%s'", schemaState)
		}
		tmpRows := tkO.MustQuery("select count(*) from t").Rows()[0][0].(string)
		tmpNr, err := strconv.Atoi(tmpRows)
		require.NoError(t, err)
		require.Equal(t, rows, tmpNr, "Number of rows not correct in table (%d!=%d) State: '%s'", rows, tmpNr, schemaState)
	}
	postFn := func(tkO *testkit.TestKit, _ kv.Storage) {
		tkO.MustExec("admin check table t /* postFn */")
		tkO.MustQuery(`select count(*) from t`).Check(testkit.Rows("16"))
		tkO.MustQuery(`select b, a,_tidb_rowid from t`).Sort().Check(testkit.Rows(""+
			"1 25 22",
			"10 10 30006",
			"11 11 30010",
			"13 38 30003",
			"15 15 30011",
			"16 16 4",
			"17 17 30004",
			"18 18 30008",
			"19 19 30012",
			"2 42 1",
			"20 20 5",
			"4 4 30001",
			"5 5 30002",
			"6 41 30005",
			"8 8 2",
			"9 9 3"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, false)
}

func TestBackfillConcurrentDML(t *testing.T) {
	// We want to test the scenario where one batch fails and one succeeds,
	// so it will retry both batches (since it never updated the reorgInfo).
	// Additionally, we want to test if it can cause duplication due to it may not know
	// the committed new record IDs.
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_ddl_reorg_batch_size = 32")
	tk.MustExec("set global tidb_ddl_reorg_worker_cnt = 2")
	tk.RefreshSession()

	tk.MustExec("use test")

	// TODO: adjust to 5 partitions, so that 3 values want to write to the same ID
	// since the first would succeed with the same, the second one would generate and add to the map
	// as well as the third must do the same!!!
	tk.MustExec("create table t (a int, b int, primary key (a) nonclustered) partition by hash(a) partitions 3")
	tk.MustExec("insert into t (a, b) values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12),(13,13),(14,14),(15,15),(16,16)")
	tk.MustExec("insert into t (a, b) select a+16, b+16 from t")
	tk.MustExec("insert into t (a, b) select a+32, b+32 from t")
	tk.MustExec("insert into t (a, b) select a+64, b+64 from t")
	// use EXCHANGE PARTITION to make both partitions having _tidb_rowids 1-128/3
	tk.MustExec("create table tx0 like t")
	tk.MustExec("alter table tx0 remove partitioning")
	tk.MustExec("insert into tx0 select a,b from t partition (p0)")
	tk.MustExec("alter table t exchange partition p0 with table tx0 without validation")
	tk.MustExec("create table tx1 like tx0")
	tk.MustExec("insert into tx1 select a,b from t partition (p1)")
	tk.MustExec("alter table t exchange partition p1 with table tx1 without validation")
	tk.MustExec("create table tx2 like tx0")
	tk.MustExec("insert into tx2 select a,b from t partition (p2)")
	tk.MustExec("alter table t exchange partition p2 with table tx2 without validation")
	tk.MustExec("drop table tx0, tx1, tx2")
	var i atomic.Int32
	i.Store(0)

	tbl, err := tk.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)

	columnFt := make(map[int64]*types.FieldType)
	for idx := range tbl.Columns {
		col := tbl.Columns[idx]
		columnFt[col.ID] = &col.FieldType
	}

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/PartitionBackfillNonClustered", func(vals []byte) {
		m, err := tablecodec.DecodeRowWithMapNew(vals, columnFt, time.UTC, nil)
		require.NoError(t, err)
		var col1 int64
		if d, ok := m[tbl.Columns[0].ID]; ok {
			col1 = d.GetInt64()
		}
		if col1%3 != 1 {
			// Let the p0/p2 complete, so there will be duplicates in the new p0/p1 partitions
			return
		}
		if col1 > 30 {
			// let the first batch succeed, and generate new record IDs
			return
		}

		round := i.Add(1)
		if round == 1 {
			// UPDATE the same row, so the backfill will fail and retry
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use test")
			tk2.MustExec(fmt.Sprintf("update t set b = b + 300 where a = %d", col1))
		}
		// TODO: Also start a transaction that will fail due to conflict with the backfill?
		// probably have to continue in another failpoint hook?
	})
	tk.MustExec("alter table t coalesce partition 1")
	tk.MustExec("admin check table t")
	tk.MustQuery("select a,b,_tidb_rowid from t").Sort().Check(testkit.Rows(
		"1 301 129",
		"10 10 30035",
		"100 100 30065",
		"101 101 34",
		"102 102 34",
		"103 103 30066",
		"104 104 35",
		"105 105 35",
		"106 106 30067",
		"107 107 36",
		"108 108 36",
		"109 109 30068",
		"11 11 4",
		"110 110 37",
		"111 111 37",
		"112 112 30069",
		"113 113 38",
		"114 114 38",
		"115 115 30070",
		"116 116 39",
		"117 117 39",
		"118 118 30071",
		"119 119 40",
		"12 12 4",
		"120 120 40",
		"121 121 30072",
		"122 122 41",
		"123 123 41",
		"124 124 30073",
		"125 125 42",
		"126 126 42",
		"127 127 43",
		"128 128 43",
		"13 13 30036",
		"14 14 5",
		"15 15 5",
		"16 16 30037",
		"17 17 6",
		"18 18 6",
		"19 19 30038",
		"2 2 1",
		"20 20 7",
		"21 21 7",
		"22 22 30039",
		"23 23 8",
		"24 24 8",
		"25 25 30040",
		"26 26 9",
		"27 27 9",
		"28 28 30041",
		"29 29 10",
		"3 3 1",
		"30 30 10",
		"31 31 30042",
		"32 32 11",
		"33 33 11",
		"34 34 30043",
		"35 35 12",
		"36 36 12",
		"37 37 30044",
		"38 38 13",
		"39 39 13",
		"4 4 30033",
		"40 40 30045",
		"41 41 14",
		"42 42 14",
		"43 43 30046",
		"44 44 15",
		"45 45 15",
		"46 46 30047",
		"47 47 16",
		"48 48 16",
		"49 49 30048",
		"5 5 2",
		"50 50 17",
		"51 51 17",
		"52 52 30049",
		"53 53 18",
		"54 54 18",
		"55 55 30050",
		"56 56 19",
		"57 57 19",
		"58 58 30051",
		"59 59 20",
		"6 6 2",
		"60 60 20",
		"61 61 30052",
		"62 62 21",
		"63 63 21",
		"64 64 30053",
		"65 65 22",
		"66 66 22",
		"67 67 30054",
		"68 68 23",
		"69 69 23",
		"7 7 30034",
		"70 70 30055",
		"71 71 24",
		"72 72 24",
		"73 73 30056",
		"74 74 25",
		"75 75 25",
		"76 76 30057",
		"77 77 26",
		"78 78 26",
		"79 79 30058",
		"8 8 3",
		"80 80 27",
		"81 81 27",
		"82 82 30059",
		"83 83 28",
		"84 84 28",
		"85 85 30060",
		"86 86 29",
		"87 87 29",
		"88 88 30061",
		"89 89 30",
		"9 9 3",
		"90 90 30",
		"91 91 30062",
		"92 92 31",
		"93 93 31",
		"94 94 30063",
		"95 95 32",
		"96 96 32",
		"97 97 30064",
		"98 98 33",
		"99 99 33"))
}

func TestBackfillConcurrentDMLRange(t *testing.T) {
	// We want to test the scenario where one batch fails and one succeeds,
	// so it will retry both batches (since it never updated the reorgInfo).
	// Additionally, we want to test if it can cause duplication due to it may not know
	// the committed new record IDs.
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_ddl_reorg_batch_size = 32")
	tk.MustExec("set global tidb_ddl_reorg_worker_cnt = 2")
	tk.RefreshSession()

	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int) partition by range (a) interval (100) first partition less than (100) last partition less than (900)")
	tk.MustExec("alter table t reorganize partition P_LT_500, P_LT_600, P_LT_700, P_LT_800 into (partition p8 values less than (800))")
	tk.MustExec("insert into t (a, b) values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12),(13,13),(14,14),(15,15),(16,16)")
	tk.MustExec("insert into t (a, b) select a+100, b+100 from t order by a")
	exchangeAllPartitionsToGetDuplicateTiDBRowIDs(t, tk)
	originalIDs := getTableAndPartitionIDs(t, tk)
	var i atomic.Int32
	i.Store(0)

	tbl, err := tk.Session().GetInfoSchema().TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)

	columnFt := make(map[int64]*types.FieldType)
	for idx := range tbl.Columns {
		col := tbl.Columns[idx]
		columnFt[col.ID] = &col.FieldType
	}

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		// TODO: start a transaction to be committed during backfill
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		res := tk2.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		switch schemaState {
		case model.StateNone.String():
		// start
		case model.StateDeleteOnly.String():
		case model.StateWriteOnly.String():
		case model.StateWriteReorganization.String():
			// Before backfill!
			// First write to new partition p0, so should keep its _tidb_rowid (1-5)
			// These should be skipped by backfill since _tidb_rowid AND row is the same.
			tk2.MustExec("update t set b = b + 1000 where a = 2")
			tk2.MustExec("update t set b = b + 1000 where a = 3")
			tk2.MustExec("update t set b = b + 1000 where a = 4")
			tk2.MustExec("update t set b = b + 1000 where a = 5")
			// These will get new _tidb_rowid's in the new partitions (since the above used the original)
			tk2.MustExec("update t set b = b + 1000 where a = 102")
			tk2.MustExec("update t set b = b + 1000 where a = 103")
			tk2.MustExec("update t set b = b + 1000 where a = 104")
			tk2.MustExec("update t set b = b + 1000 where a = 105")
			// These will cause conflicts in backfill, since these updates will keep the _tidb_rowid, (12-15)
			// and since P_LT_100 will be handled first, so _tidb_rowid will be the same, but not same row.
			// Will these find the same rows in the new partitions?
			tk2.MustExec("update t set b = b + 1000 where a = 108")
			tk2.MustExec("update t set b = b + 1000 where a = 109")
			tk2.MustExec("update t set b = b + 1000 where a = 110")
			tk2.MustExec("update t set b = b + 1000 where a = 111")
			// TODO: also test where the rows moves between partitions (both old and new, so 4 variants)
			// Will these also update/delete the new partitions?
			tk2.MustExec("update t set b = b + 100 where a = 2")
			tk2.MustExec("delete from t where a = 3")
			tk2.MustExec("update t set b = b + 100 where a = 102")
			tk2.MustExec("delete from t where a = 103")
			tk2.MustExec("update t set b = b + 100 where a = 108")
			tk2.MustExec("delete from t where a = 109")
		case model.StateDeleteReorganization.String():
			// Using the same rows, but now using the new partitions
			// Will it find the same rows in the old partitions?
			// TODO: How to check it?
			tk2.MustExec("update t set b = b + 100 where a = 4")
			tk2.MustExec("delete from t where a = 5")
			tk2.MustExec("update t set b = b + 100 where a = 104")
			tk2.MustExec("delete from t where a = 105")
			tk2.MustExec("update t set b = b + 100 where a = 110")
			tk2.MustExec("delete from t where a = 111")
		case model.StatePublic.String():
			// Last
		default:
			require.FailNow(t, "unexpected schema state: %v", schemaState)
		}
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/PartitionBackfillNonClustered", func(vals []byte) {
		// TODO: Also start a transaction that will fail due to conflict with the backfill?
		// TODO: Also use INSERT and DELETE and INSERT IGNORE and INSERT ON DUPLICATE KEY UPDATE
		// probably have to continue in another failpoint hook?
		// Tests:
		// - 2+ updates of same rows (different transactions) to see that an already updated row,
		//   that got a new _tidb_rowid, will update with the same _tidb_rowid
		// - Delete a row that have a newly generated _tidb_rowid from backfill
		// - Delete a row that have a newly generated _tidb_rowid from update
		// - Delete a row that have a newly generated _tidb_rowid from backfill after it was updated
		// combinations of update with 2 => 1 new partitions, 1 => 2 new partitions, same partition?
		// also test combinations where old to/from is same or different and new from/to is same or different
		// - test in DeleteOnly, WriteOnly, WriteReorganize (before, during and after backfill?), DeleteReorganize, public?
		//   At least once so we read the set 'old' and double write to the 'new' set,
		//   and once where we read the 'new' set and double write to the old set.
		m, err := tablecodec.DecodeRowWithMapNew(vals, columnFt, time.UTC, nil)
		require.NoError(t, err)
		var col1 int64
		if d, ok := m[tbl.Columns[0].ID]; ok {
			col1 = d.GetInt64()
		}
		if col1 != 1 {
			return
		}

		round := i.Add(1)
		if round == 1 {
			// UPDATE the same row, so the backfill will fail and retry
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use test")
			tk2.MustExec(fmt.Sprintf("update t set b = b + 300 where a = %d", col1))
		}
	})
	tk.MustQuery("select a,b,_tidb_rowid from t").Sort().Check(testkit.Rows(""+
		"1 1 1",
		"10 10 10",
		"101 101 1",
		"102 102 2",
		"103 103 3",
		"104 104 4",
		"105 105 5",
		"106 106 6",
		"107 107 7",
		"108 108 8",
		"109 109 9",
		"11 11 11",
		"110 110 10",
		"111 111 11",
		"112 112 12",
		"113 113 13",
		"114 114 14",
		"115 115 15",
		"116 116 16",
		"12 12 12",
		"13 13 13",
		"14 14 14",
		"15 15 15",
		"16 16 16",
		"2 2 2",
		"3 3 3",
		"4 4 4",
		"5 5 5",
		"6 6 6",
		"7 7 7",
		"8 8 8",
		"9 9 9"))
	// merge the first 4 into 1 and split the last into 4
	tk.MustExec("alter table t reorganize partition P_LT_100, P_LT_200, P_LT_300, P_LT_400, p8 into (partition p0 values less than (400), partition p4 values less than (500), partition p5 values less than (600), partition p6 values less than (700), partition p7 values less than (800))")
	tk.MustExec("admin check table t")
	require.Equal(t, int32(2), i.Load(), "backfill should have been retried once")

	deleteRanges := tk.MustQuery(`select * from mysql.gc_delete_range`).Rows()
	logutil.BgLogger().Info("deleteRanges", zap.Int("deleteRanges", len(deleteRanges)))
	for len(deleteRanges) > 0 {
		// EmulatorGC will handle unistore deletions asynchronously
		time.Sleep(time.Duration(len(deleteRanges)) * time.Millisecond)
		deleteRanges = tk.MustQuery(`select * from mysql.gc_delete_range`).Rows()
		logutil.BgLogger().Info("deleteRanges re-check", zap.Int("deleteRanges", len(deleteRanges)))
	}
	checkTableAndIndexEntries(t, tk, originalIDs)
	tk.MustQuery("select a,b from t").Sort().Check(testkit.Rows(""+
		"1 301",
		"10 10",
		"101 101",
		"102 1202",
		"104 1204",
		"106 106",
		"107 107",
		"108 1208",
		"11 11",
		"110 1210",
		"112 112",
		"113 113",
		"114 114",
		"115 115",
		"116 116",
		"12 12",
		"13 13",
		"14 14",
		"15 15",
		"16 16",
		"2 1102",
		"4 1104",
		"6 6",
		"7 7",
		"8 8",
		"9 9"))
}

func TestMultiSchemaReorgDeleteNonClusteredRange(t *testing.T) {
	createSQL := `create table t (a int, b char(255), c char(255), index idx_a (a), index idx_ba (b, a), index idx_cb (c,b), index idx_c (c)) partition by range (a) (partition p1 values less than (100), partition p2 values less than (200), partition p3 values less than (300))`
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec("insert into t (a) values (1),(2),(3),(4),(101),(102),(103),(104),(201),(202),(203),(204)")
		tkO.MustExec(`update t set b = "Original", c = a`)
		exchangeAllPartitionsToGetDuplicateTiDBRowIDs(t, tkO)
	}
	// TODO: test all variants of from/to and newFrom/newTo, i.e if a row moves between partitions, both new and old
	// So test should be:
	// to == from, newTo == newFrom
	// to == from, newTo != newFrom
	// to != from, newTo == newFrom
	// to != from, newTo != newFrom
	// AND where at least one of them causes a new _tidb_rowid to be generated
	//     AND that id is used for lookup on the other set (i.e. new for <= WriteReorg, and old for DeleteReorg)
	// Better to create one of these tests first to == from, newTo == newFrom and create alterative after, when this works.
	alterSQL := `alter table t reorganize partition p1,p2,p3 into (partition newP1 values less than (300))`
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		switch schemaState {
		case model.StateWriteOnly.String():
			tkO.MustQuery(`select *, _tidb_rowid from t`).Sort().Check(testkit.Rows(""+
				"1 Original 1 1",
				"101 Original 101 1",
				"102 Original 102 2",
				"103 Original 103 3",
				"104 Original 104 4",
				"2 Original 2 2",
				"201 Original 201 1",
				"202 Original 202 2",
				"203 Original 203 3",
				"204 Original 204 4",
				"3 Original 3 3",
				"4 Original 4 4"))
			tkO.MustExec(`delete from t where a = 1 -- CASE (1)`)
			tkO.MustExec(`update t set b = concat(b, " updated") where a = 2`)
			tkO.MustExec(`update t set b = concat(b, " updated") where a = 102`)
			tkO.MustExec(`update t set b = concat(b, " updated") where a = 202`)
			// TODO: extend test to see the new partition _tidb_rowid before delete?
			tkO.MustQuery(`select *, _tidb_rowid from t where a = 2`)
			tkO.MustExec(`delete from t where a = 2 -- CASE (5)`)
			tkO.MustExec(`delete from t where a = 102 -- CASE (2)`)
			tkO.MustExec(`update t set b = concat(b, " updated") where a = 3`)
			tkO.MustExec(`delete from t where a = 103 -- CASE (3)`)
			tkO.MustExec(`update t set b = concat(b, " updated") where a = 4`)
			tkO.MustExec(`update t set b = concat(b, " updated") where a = 104`)
			// TODO: extend test to see the new partition _tidb_rowid before delete?
			tkO.MustQuery(`select *, _tidb_rowid from t where a = 104`)
			tkO.MustExec(`delete from t where a = 104 -- CASE (4)`)
		}
		/*
			case model.StateWriteOnly.String():
				// < 100 will keep their original _tidb_rowid also in new partitions (unless updated to new partitions!)
				for i := range 4 {
					id := i * 100
					// WriteOnly state:
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+1))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+2))
					tkO.MustExec(fmt.Sprintf(`delete from t where c1 = %d`, id+2))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+2))
					tkO.MustExec(fmt.Sprintf(`delete from t where c1 = %d`, id+3))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+3))
					// WriteReorg state:
					// 4 is first updated in WriteReorg
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+5))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+6))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+7))
					// DeleteReorg state:
					// 8 is first updated in DeleteReorg
					// 9 is first updated in WriteReorg
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+10))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+11))
				}
				// TODO: We are testing if update is reflected in other partition set!!
				// After this:
				// old partitions _tidb_rowid
				//                new partitions _tidb_rowid
				// 1   1          1   1
				// ...
				// 101 1          101 30001 (at least NOT 1!)
				// ...
			case model.StateWriteReorganization.String():
				for i := range 4 {
					id := i * 100
					// WriteReorg state:
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+4))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+5))
					tkO.MustExec(fmt.Sprintf(`delete from t where c1 = %d`, id+6))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+6))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+7))
					tkO.MustExec(fmt.Sprintf(`delete from t where c1 = %d`, id+7))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+7))
					// DeleteReorg state:
					// 8 is first updated in DeleteReorg
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+9))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+10))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+11))
				}
			case model.StateDeleteReorganization.String():
				for i := range 4 {
					id := i * 100
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+8))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+9))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+10))
					tkO.MustExec(fmt.Sprintf(`update t set c2 = c2 + 1000, c3 = concat(c3, " u ", c2) where c1 = %d`, id+11))
				}

		*/
		// TODO: check the tkNO state of the OLD partitions!!!
		// What do we want to test?
		// old partition      new partitions
		// old ID             old ID    <= seeing old partitions; No issues
		// old ID             new ID    <= seeing old partitions; may miss update/delete new partition
		// new ID             old ID    <= seeing old partitions; may miss update/delete new partition
		// ^^^ THIS NEEDS tkNO in DeleteReorg ^^^
		// new ID1            new ID1   <= seeing old partitions; No issues
		// new ID1            new ID2   <= seeing old partitions; may miss update/delete new partition
		// old ID             old ID    <= seeing new partitions; No issues
		// old ID             new ID    <= seeing new partitions; may miss update/delete old partition
		// new ID             old ID    <= seeing new partitions; may miss update/delete old partition
		// new ID             new ID    <= seeing new partitions; No issues
		// new ID1            new ID2   <= seeing new partitions; may miss update/delete new partition
		//
		// AND each combination, where update sees a collision in both old(prev) ID AND with the map
		// which needs to create a new ID and add to the map...
		// So what really needs to be tested:
		// seeing old partitions:
		//   - update where old id already exists in the newFromMap
		//      - delete newFrom row with mapped id, delete map entry
		//      - if exists in newToMap:
		//
		//      (use the new ID from the map, delete+insert with same ID)
		//
		//   - update where old id already exists the table, but not in the map (generate a new ID and add to the map)
		//   - update where old id does not exists the table or in the map (generate a new ID and add to the map)
		//   - delete where old id already exists in the map (use the new ID from the map, delete both from table and map)
		//   - delete where old id already exists the table with different ID, but not in the map ()
		//   - delete where old id already exists the table with same ID, but not in the map (delete from table)
		//   - delete where old id does not exists the table or in the map (skip)
		//
		//   d = delete, i = insert, m = map (prevID, partID)=>(newID), r = tableRow, n = generate new ID
		//
		// newToKey = New set of partitions, t_<tableid>_r<rowID>, rowID is just a different name for _tidb_rowid
		// map is a map from the other set of partitions rowID+PartID => newID (prevID, partID)=>(newID)
		//  Note: if new ID is generated when updating in current partitions, then it is guaranteed to be unique
		//        So it is just inserted directly in newToKey (table)
		//              newFromMap   newToMap  newToKey-not-same-value newToKey-same-value
		//   - update   d m+r        d+i r     n+i m+r                 d+i r
		//
		// Hmm, too tired to go through all of the above and create a good covering test...
		// so just for now I will do the following (no variants for to/from, newTo/newFrom)
		//
		// === vvv This is enough for now vvv ===
		// delete where newFromKey exists and the row is the same (should not also have a matching newFromMap entry!) (5)
		// delete where newFromKey exists and the row is NOT the same
		//   + where newFromMap exists (4)
		//   + where newFromMap does NOT exist (3) -- nothing to delete!
		// delete where newFromKey does NOT exist
		//   + where newFromMap exists (2)
		//   + where newFromMap does NOT exist (1) -- nothing to delete!
		// === ^^^ This is enough for now ^^^ ===
		// Note: _tidb_rowid's are unique within the same partition
		//       AND duplicate rows cannot be in different partitions! (due to partitioning expression!)
		// ===>>> So we need to setup: only 5 rows:
		// 1) before backfill:
		//    - a simple delete in WriteOnly before any other delete. No row should exist!!!
		//      like delete from t where a = 1
		// 2) Two possibilities:
		//    - before backfill:
		//      - REORGANIZE oldP1, oldP2, oldP3 => newP1,
		//        - where oldP1,oldP2 and oldP3 has the same rowIDs (a=2,rowID=2),(a=102,rowID=2),(a=202,rowID=2)
		//        - newFrom == newTo, so newToMap == newFromMap, and newToKey == newFromKey, but to != from.
		//      - update a=2 oldP1r1 (reorganized to newP1) => newToKey does not exist, so same rowID=2!
		//      - update a=102 rowP2r1 => newP1 => same rowID already exists, but different row, creates a new and adds it to newToMap
		//      - delete a=2 oldP1r1, actually case 5, newFromKey exists and row is the same!
		//      - delete a=102 <= Case 2
		//    - Or after backfill, which already done the same as the two first updates!
		//      - delete a=2 oldP1r1, actually case 5, newFromKey exists and row is the same!
		//      - delete a=102 <= Case 2
		// 3) before backfill: (a=3,rowID=3), (a=113,rowID=3) in oldP2 and (a=213,rowID=3) in oldP3)
		//    - update a=3 oldP1r1 => newToKey/newToMap does not exist => keep rowID=1
		//    - delete a=103 oldP2r1 => newFromKey exists, but row is different, newFromMap does not exist <= case 3
		// 4) before backfill: (a=3,rowID=3), (a=113,rowID=3) in oldP2 and (a=213,rowID=3) in oldP3)
		//    - update a=4 oldP1r1 => newToKey/newToMap does not exist => keep rowID=1
		//    - update a=104 oldP2r1 => newFromKey exists, but row is different, creates newFromMap
		//    - delete a=104 oldP2r1 => newFromKey exists, but row is different, newFromMap exists <= case 4
		// Which all can be simplified before backfill!!!
		// - delete from t where a = 1 -- CASE (1)
		// - update t set b = concat(b, " updated") where a = 2
		// - update t set b = concat(b, " updated") where a = 102
		// - update t set b = concat(b, " updated") where a = 202
		// - delete from t where a = 2 -- CASE (5)
		// - delete from t where a = 102 -- CASE (2)
		// - update t set b = concat(b, " updated") where a = 3
		// - delete from t where a = 103 -- CASE (3)
		// - update t set b = concat(b, " updated") where a = 4
		// - update t set b = concat(b, " updated") where a = 104
		// - delete from t where a = 104 -- CASE (4)
		//
		//
		// TODO:
		// update where newFromKey exists and the row is the same (should not also have a matching newFromMap entry!)
		// update where newFromKey exists and the row is NOT the same
		//   + where newFromMap exists
		//   + where newFromMap does NOT exist
		// update where newFromKey does NOT exist
		//   + where newFromMap exists
		//   + where newFromMap does NOT exist
		// for all the following cases:
		// update where newToKey exists and the row is the same (should not also have a matching newToMap entry!)
		// update where newToKey exists and the row is NOT the same
		//   + where newToMap exists
		//   + where newToMap does NOT exist
		// update where newToKey does NOT exist
		//   + where newToMap exists
		//   + where newToMap does NOT exist
		//}
	}
	postFn := func(tkO *testkit.TestKit, _ kv.Storage) {
		//require.Equal(t, int(7*2+1), i)
		tkO.MustQuery(`select a,b,c,_tidb_rowid from t`).Sort().Check(testkit.Rows(""+
			// 1 deleted
			"101 Original 101 1",
			// 102 deleted
			// 103 deleted
			// 104 deleted
			"201 Original 201 30001",
			"202 Original updated 202 14",
			"203 Original 203 30002",
			"204 Original 204 30003",
			// 2 deleted
			"3 Original updated 3 3",
			"4 Original updated 4 4"))
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn, false)
}

func TestNonClusteredUpdateReorgUpdate(t *testing.T) {
	// Currently there is a case where:
	// update would remove the wrong row
	// happens if update x when there is no newFromMap, but there is a matching newFromKey, but not the same row!!!
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, primary key (a) nonclustered) partition by hash(a) partitions 2")
	tk.MustExec("insert into t (a, b) values (1,1),(2,2)")
	exchangeAllPartitionsToGetDuplicateTiDBRowIDs(t, tk)
	f := func(res []string, expected []any) bool {
		if len(res) == 3 && len(expected) == 3 && res[0] == expected[0].(string) && res[1] == expected[1].(string) {
			if res[2] == "30001" {
				return true
			}
			r, err := strconv.Atoi(res[2])
			if err != nil {
				return false
			}
			return r == expected[2].(int)
		}
		return false
	}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", func(job *model.Job) {
		// So the table is actually in WriteOnly, before backfill!
		if job.SchemaState != model.StateWriteReorganization {
			return
		}
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		tk2.MustExec("update t set b = b + 10 where a = 1")
		// Would delete newFrom 1, which would then be backfilled again!
		tk2.MustExec("update t set b = b + 10 where a = 2")
		tk2.MustQuery(`select a,b,_tidb_rowid from t`).Sort().CheckWithFunc([][]any{{"1", "11", 1}, {"2", "12", 3}}, f)
	})
	tk.MustExec("alter table t remove partitioning")
	tk.MustQuery(`select a,b,_tidb_rowid from t`).Sort().CheckWithFunc([][]any{{"1", "11", 1}, {"2", "12", 3}}, f)
}

func TestNonClusteredReorgUpdate(t *testing.T) {
	createSQL := "create table t (a int, b int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than (30))"
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1),(11,11),(22,22)`)
		exchangeAllPartitionsToGetDuplicateTiDBRowIDs(t, tkO)
	}
	alterSQL := "alter table t reorganize partition p0, p1 into (partition p0new values less than (20))"
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		switch schemaState {
		case model.StateDeleteReorganization.String():
			tkNO.MustQuery(`select a,b,_tidb_rowid from t`).Sort().Check(testkit.Rows("1 1 1", "11 11 30001", "22 22 1"))
			tkO.MustQuery(`select a,b,_tidb_rowid from t`).Sort().Check(testkit.Rows("1 1 1", "11 11 30001", "22 22 1"))
			tkNO.MustExec(`update t set a = 21 where a = 1`)
			tkNO.MustQuery(`select a,b,_tidb_rowid from t`).Sort().Check(testkit.Rows("11 11 30001", "21 1 60001", "22 22 1"))
			tkO.MustQuery(`select a,b,_tidb_rowid from t`).Sort().Check(testkit.Rows("11 11 30001", "21 1 60001", "22 22 1"))
			tkNO.MustExec(`update t set a = 2 where a = 22`)
			tkO.MustQuery(`select a,b,_tidb_rowid from t`).Sort().Check(testkit.Rows("11 11 30001", "2 22 60002", "21 1 60001"))
			tkNO.MustQuery(`select a,b,_tidb_rowid from t`).Sort().Check(testkit.Rows("11 11 30001", "2 22 60002", "21 1 60001"))
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn, false)
}

// TODO: Still managed to repeat the issue of update removing the wrong newFrom row
// Need something like:
// newFromKey != newToKey
// newFromKey set for a different row
// newFromMap not set for this value (so either was inserted as newFromKey and then deleted cannot happen, OR not inserted)
// So can it happen after backfill? No, since it needs to already be there, so either newFromKey is set OR newFromMap
// Hmm, what if the newFrom is 0?
// So an zero alter, then moving the row, Nope, if newFrom is 0, then nothing is deleted...

func TestNonClusteredReorgUpdateHash(t *testing.T) {
	createSQL := "create table t (a int, b int) partition by hash (a) partitions 2"
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (2,2),(3,3)`)
		exchangeAllPartitionsToGetDuplicateTiDBRowIDs(t, tkO)
	}
	// TODO: Allow "add partition 1" as syntax?
	alterSQL := "alter table t add partition partitions 1"
	loopFn := func(tkO, tkNO *testkit.TestKit) {
		res := tkO.MustQuery(`select schema_state from information_schema.DDL_JOBS where table_name = 't' order by job_id desc limit 1`)
		schemaState := res.Rows()[0][0].(string)
		switch schemaState {
		case model.StateDeleteReorganization.String():
			tkNO.MustQuery(`select a,b,_tidb_rowid from t`).Sort().Check(testkit.Rows("2 2 1", "3 3 1"))
			tkO.MustQuery(`select a,b,_tidb_rowid from t`).Sort().Check(testkit.Rows("2 2 1", "3 3 1"))
			tkNO.MustExec(`update t set a = 5 where a = 3`)
			tkNO.MustQuery(`select a,b,_tidb_rowid from t`).Sort().Check(testkit.Rows("2 2 1", "5 3 30001"))
			tkO.MustQuery(`select a,b,_tidb_rowid from t`).Sort().Check(testkit.Rows("2 2 1", "5 3 30001"))
		}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn, false)
}
