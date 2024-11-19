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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessiontxn"
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
	postFn := func(_ *testkit.TestKit, _ kv.Storage) {
		// nothing
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn)
}

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
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn)
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
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn)
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
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, nil, loopFn)
}

func TestMultiSchemaReorganizePartition(t *testing.T) {
	createSQL := `create table t (a int primary key, b varchar(255), unique index idx_b_global (b) global) partition by range (a) (partition p1 values less than (200), partition pMax values less than (maxvalue))`
	originalPartitions := make([]int64, 0, 2)
	originalIndexIDs := make([]int64, 0, 1)
	originalGlobalIndexIDs := make([]int64, 0, 1)
	tableID := int64(0)
	initFn := func(tkO *testkit.TestKit) {
		tkO.MustExec(`insert into t values (1,1),(2,2),(101,101),(102,102),(998,998),(999,999)`)
		ctx := tkO.Session()
		is := domain.GetDomain(ctx).InfoSchema()
		tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
		require.NoError(t, err)
		tableID = tbl.Meta().ID
		for _, def := range tbl.Meta().Partition.Definitions {
			originalPartitions = append(originalPartitions, def.ID)
		}
		for _, idx := range tbl.Meta().Indices {
			if idx.Global {
				originalGlobalIndexIDs = append(originalGlobalIndexIDs, idx.ID)
				continue
			}
			originalIndexIDs = append(originalIndexIDs, idx.ID)
		}
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
		tkO.MustContainErrMsg(`insert into t values (999,999)`+dbgStr, "[kv:1062]Duplicate entry '999' for key 't.idx_b_global'")
		tkNO.MustContainErrMsg(`insert into t values (999,999)`+dbgStr, "[kv:1062]Duplicate entry '999' for key 't.idx_b_global'")
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

		logutil.BgLogger().Info("inserting rows", zap.Int("testID", testID))

		testID++
		tkNO.MustExec(fmt.Sprintf(`insert into t values (%d,%d)`+dbgStr, testID, testID))
		tkO.MustQuery(fmt.Sprintf(`select * from t where b = "%d"`+dbgStr, testID)).Check(testkit.Rows(fmt.Sprintf("%d %d", testID, testID)))

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
		case model.StateNone.String():
		default:
			require.Failf(t, "unhandled schema state '%s'", schemaState)
		}
	}
	postFn := func(tkO *testkit.TestKit, store kv.Storage) {
		tkO.MustQuery(`select * from t where b = 5`).Sort().Check(testkit.Rows("5 5"))
		tkO.MustQuery(`select * from t where b = "5"`).Sort().Check(testkit.Rows("5 5"))
		tkO.MustExec(`admin check table t`)
		tkO.MustQuery(`select * from t`).Sort().Check(testkit.Rows("1 1", "10 10", "101 101", "102 102", "11 11", "12 12", "13 13", "14 14", "2 2", "5 5", "6 6", "7 7", "8 8", "9 9", "984 984", "985 985", "986 986", "987 987", "988 988", "989 989", "990 990", "991 991", "992 992", "993 993", "998 998", "999 999"))
		// TODO: Verify that there are no KV entries for old partitions or old indexes!!!
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
		ctx := tkO.Session()
		is := domain.GetDomain(ctx).InfoSchema()
		tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
		require.NoError(t, err)
		tableID = tbl.Meta().ID
		// Save this for the fix of https://github.com/pingcap/tidb/issues/56822
		//GlobalLoop:
		//	for _, globIdx := range originalGlobalIndexIDs {
		//		for _, idx := range tbl.Meta().Indices {
		//			if idx.ID == globIdx {
		//				continue GlobalLoop
		//			}
		//		}
		//		// Global index removed
		//		require.False(t, HaveEntriesForTableIndex(t, tkO, tableID, globIdx), "Global index id %d for table id %d has still entries!", globIdx, tableID)
		//	}
	LocalLoop:
		for _, locIdx := range originalIndexIDs {
			for _, idx := range tbl.Meta().Indices {
				if idx.ID == locIdx {
					continue LocalLoop
				}
			}
			// local index removed
			for _, part := range tbl.Meta().Partition.Definitions {
				require.False(t, HaveEntriesForTableIndex(t, tkO, part.ID, locIdx), "Local index id %d for partition id %d has still entries!", locIdx, tableID)
			}
		}
		// TODO: Fix cleanup issues, most likely it needs one more SchemaState in onReorganizePartition
		//PartitionLoop:
		//	for _, partID := range originalPartitions {
		//		for _, def := range tbl.Meta().Partition.Definitions {
		//			if def.ID == partID {
		//				continue PartitionLoop
		//			}
		//		}
		//		// old partitions removed
		//		require.False(t, HaveEntriesForTableIndex(t, tkO, partID, 0), "Reorganized partition id %d for table id %d has still entries!", partID, tableID)
		//	}
	}
	runMultiSchemaTest(t, createSQL, alterSQL, initFn, postFn, loopFn)
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

func runMultiSchemaTest(t *testing.T, createSQL, alterSQL string, initFn func(*testkit.TestKit), postFn func(*testkit.TestKit, kv.Storage), loopFn func(tO, tNO *testkit.TestKit)) {
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
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", hookFunc)
	alterChan := make(chan struct{})
	go func() {
		tkDDLOwner.MustExec(alterSQL)
		logutil.BgLogger().Info("XXXXXXXXXXX drop partition done!")
		alterChan <- struct{}{}
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
		i++
		hookChan <- struct{}{}
	}
	logutil.BgLogger().Info("XXXXXXXXXXX states loop done")
	if postFn != nil {
		postFn(tkO, store)
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
