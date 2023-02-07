// Copyright 2023 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/internal/callback"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type allTableData struct {
	keys [][]byte
	vals [][]byte
	tp   []string
}

// TODO: Create a more generic function that gets all accessible table ids
// from all schemas, and checks the full key space so that there are no
// keys for non-existing table IDs. Also figure out how to wait for deleteRange
// Checks that there are no accessible data after an existing table
// assumes that tableIDs are only increasing.
// To be used during failure testing of ALTER, to make sure cleanup is done.
func noNewTablesAfter(t *testing.T, tk *testkit.TestKit, ctx sessionctx.Context, tbl table.Table) {
	waitForGC := tk.MustQuery(`select start_key, end_key from mysql.gc_delete_range`).Rows()
	require.NoError(t, sessiontxn.NewTxn(context.Background(), ctx))
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	defer func() {
		err := txn.Rollback()
		require.NoError(t, err)
	}()
	// Get max tableID (if partitioned)
	tblID := tbl.Meta().ID
	if pt := tbl.GetPartitionedTable(); pt != nil {
		defs := pt.Meta().Partition.Definitions
		{
			for i := range defs {
				tblID = mathutil.Max[int64](tblID, defs[i].ID)
			}
		}
	}
	prefix := tablecodec.EncodeTablePrefix(tblID + 1)
	it, err := txn.Iter(prefix, nil)
	require.NoError(t, err)
ROW:
	for it.Valid() {
		for _, rowGC := range waitForGC {
			// OK if queued for range delete / GC
			hexString := fmt.Sprintf("%v", rowGC[0])
			start, err := hex.DecodeString(hexString)
			require.NoError(t, err)
			hexString = fmt.Sprintf("%v", rowGC[1])
			end, err := hex.DecodeString(hexString)
			require.NoError(t, err)
			if bytes.Compare(start, it.Key()) >= 0 && bytes.Compare(it.Key(), end) < 0 {
				it.Close()
				it, err = txn.Iter(end, nil)
				require.NoError(t, err)
				continue ROW
			}
		}
		foundTblID := tablecodec.DecodeTableID(it.Key())
		// There are internal table ids starting from MaxInt48 -1 and allocating decreasing ids
		// Allow 0xFF of them, See JobTableID, ReorgTableID, HistoryTableID, MDLTableID
		require.False(t, it.Key()[0] == 't' && foundTblID < 0xFFFFFFFFFF00, "Found table data after highest physical Table ID %d < %d", tblID, foundTblID)
		break
	}
}

func getAllDataForPhysicalTable(t *testing.T, ctx sessionctx.Context, physTable table.PhysicalTable) allTableData {
	require.NoError(t, sessiontxn.NewTxn(context.Background(), ctx))
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	defer func() {
		err := txn.Rollback()
		require.NoError(t, err)
	}()

	all := allTableData{
		keys: make([][]byte, 0),
		vals: make([][]byte, 0),
		tp:   make([]string, 0),
	}
	pid := physTable.GetPhysicalID()
	prefix := tablecodec.EncodeTablePrefix(pid)
	it, err := txn.Iter(prefix, nil)
	require.NoError(t, err)
	for it.Valid() {
		if !it.Key().HasPrefix(prefix) {
			break
		}
		all.keys = append(all.keys, it.Key())
		all.vals = append(all.vals, it.Value())
		if tablecodec.IsRecordKey(it.Key()) {
			all.tp = append(all.tp, "Record")
			tblID, kv, _ := tablecodec.DecodeRecordKey(it.Key())
			require.Equal(t, pid, tblID)
			vals, _ := tablecodec.DecodeValuesBytesToStrings(it.Value())
			logutil.BgLogger().Info("Record",
				zap.Int64("pid", tblID),
				zap.Stringer("key", kv),
				zap.Strings("values", vals))
		} else if tablecodec.IsIndexKey(it.Key()) {
			all.tp = append(all.tp, "Index")
		} else {
			all.tp = append(all.tp, "Other")
		}
		err = it.Next()
		require.NoError(t, err)
	}
	return all
}

func TestReorganizeRangePartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database ReorgPartition")
	tk.MustExec("use ReorgPartition")
	tk.MustExec(`create table t (a int unsigned PRIMARY KEY, b varchar(255), c int, key (b), key (c,b)) partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into t values (1,"1",1), (12,"12",21),(23,"23",32),(34,"34",43),(45,"45",54),(56,"56",65)`)
	tk.MustQuery(`select * from t where c < 40`).Sort().Check(testkit.Rows(""+
		"1 1 1",
		"12 12 21",
		"23 23 32"))
	tk.MustExec(`alter table t reorganize partition pMax into (partition p2 values less than (30), partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`admin check table t`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (10),\n" +
		" PARTITION `p1` VALUES LESS THAN (20),\n" +
		" PARTITION `p2` VALUES LESS THAN (30),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
	tk.MustQuery(`select * from t`).Sort().Check(testkit.Rows(""+
		"1 1 1",
		"12 12 21",
		"23 23 32",
		"34 34 43",
		"45 45 54",
		"56 56 65"))
	tk.MustQuery(`select * from t partition (p0)`).Sort().Check(testkit.Rows("" +
		"1 1 1"))
	tk.MustQuery(`select * from t partition (p1)`).Sort().Check(testkit.Rows("" +
		"12 12 21"))
	tk.MustQuery(`select * from t partition (p2)`).Sort().Check(testkit.Rows("" +
		"23 23 32"))
	tk.MustQuery(`select * from t partition (pMax)`).Sort().Check(testkit.Rows(""+
		"34 34 43",
		"45 45 54",
		"56 56 65"))
	tk.MustQuery(`select * from t where b > "1"`).Sort().Check(testkit.Rows(""+
		"12 12 21",
		"23 23 32",
		"34 34 43",
		"45 45 54",
		"56 56 65"))
	tk.MustQuery(`select * from t where c < 40`).Sort().Check(testkit.Rows(""+
		"1 1 1",
		"12 12 21",
		"23 23 32"))
	tk.MustExec(`alter table t reorganize partition p2,pMax into (partition p2 values less than (35),partition p3 values less than (47), partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`admin check table t`)
	tk.MustQuery(`select * from t`).Sort().Check(testkit.Rows(""+
		"1 1 1",
		"12 12 21",
		"23 23 32",
		"34 34 43",
		"45 45 54",
		"56 56 65"))
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (10),\n" +
		" PARTITION `p1` VALUES LESS THAN (20),\n" +
		" PARTITION `p2` VALUES LESS THAN (35),\n" +
		" PARTITION `p3` VALUES LESS THAN (47),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
	tk.MustQuery(`select * from t partition (p0)`).Sort().Check(testkit.Rows("" +
		"1 1 1"))
	tk.MustQuery(`select * from t partition (p1)`).Sort().Check(testkit.Rows("" +
		"12 12 21"))
	tk.MustQuery(`select * from t partition (p2)`).Sort().Check(testkit.Rows(""+
		"23 23 32",
		"34 34 43"))
	tk.MustQuery(`select * from t partition (p3)`).Sort().Check(testkit.Rows("" +
		"45 45 54"))
	tk.MustQuery(`select * from t partition (pMax)`).Sort().Check(testkit.Rows("" +
		"56 56 65"))
	tk.MustExec(`alter table t reorganize partition p0,p1 into (partition p1 values less than (20))`)
	tk.MustExec(`admin check table t`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p1` VALUES LESS THAN (20),\n" +
		" PARTITION `p2` VALUES LESS THAN (35),\n" +
		" PARTITION `p3` VALUES LESS THAN (47),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
	tk.MustQuery(`select * from t`).Sort().Check(testkit.Rows(""+
		"1 1 1",
		"12 12 21",
		"23 23 32",
		"34 34 43",
		"45 45 54",
		"56 56 65"))
	tk.MustExec(`alter table t drop index b`)
	tk.MustExec(`alter table t drop index c`)
	tk.MustExec(`admin check table t`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p1` VALUES LESS THAN (20),\n" +
		" PARTITION `p2` VALUES LESS THAN (35),\n" +
		" PARTITION `p3` VALUES LESS THAN (47),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
	tk.MustExec(`create table t2 (a int unsigned not null, b varchar(255), c int, key (b), key (c,b)) partition by range (a) ` +
		"(PARTITION `p1` VALUES LESS THAN (20),\n" +
		" PARTITION `p2` VALUES LESS THAN (35),\n" +
		" PARTITION `p3` VALUES LESS THAN (47),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))")
	tk.MustExec(`insert into t2 select * from t`)
	// Not allowed to change the start range!
	tk.MustGetErrCode(`alter table t2 reorganize partition p2 into (partition p2a values less than (20), partition p2b values less than (36))`,
		errno.ErrRangeNotIncreasing)
	// Not allowed to change the end range!
	tk.MustGetErrCode(`alter table t2 reorganize partition p2 into (partition p2a values less than (30), partition p2b values less than (36))`, errno.ErrRangeNotIncreasing)
	tk.MustGetErrCode(`alter table t2 reorganize partition p2 into (partition p2a values less than (30), partition p2b values less than (34))`, errno.ErrRangeNotIncreasing)
	// Also not allowed to change from MAXVALUE to something else IF there are values in the removed range!
	tk.MustContainErrMsg(`alter table t2 reorganize partition pMax into (partition p2b values less than (50))`, "[table:1526]Table has no partition for value 56")
	tk.MustQuery(`show create table t2`).Check(testkit.Rows("" +
		"t2 CREATE TABLE `t2` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p1` VALUES LESS THAN (20),\n" +
		" PARTITION `p2` VALUES LESS THAN (35),\n" +
		" PARTITION `p3` VALUES LESS THAN (47),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
	// But allowed to change from MAXVALUE if no existing values is outside the new range!
	tk.MustExec(`alter table t2 reorganize partition pMax into (partition p4 values less than (90))`)
	tk.MustExec(`admin check table t2`)
	tk.MustQuery(`show create table t2`).Check(testkit.Rows("" +
		"t2 CREATE TABLE `t2` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p1` VALUES LESS THAN (20),\n" +
		" PARTITION `p2` VALUES LESS THAN (35),\n" +
		" PARTITION `p3` VALUES LESS THAN (47),\n" +
		" PARTITION `p4` VALUES LESS THAN (90))"))
}

func TestReorganizeListPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database ReorgListPartition")
	tk.MustExec("use ReorgListPartition")
	tk.MustExec(`create table t (a int, b varchar(55), c int) partition by list (a)` +
		` (partition p1 values in (12,23,51,14), partition p2 values in (24,63), partition p3 values in (45))`)
	tk.MustExec(`insert into t values (12,"12",21), (24,"24",42),(51,"51",15),(23,"23",32),(63,"63",36),(45,"45",54)`)
	tk.MustExec(`alter table t reorganize partition p1 into (partition p0 values in (12,51,13), partition p1 values in (23))`)
	tk.MustExec(`admin check table t`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` varchar(55) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY LIST (`a`)\n" +
		"(PARTITION `p0` VALUES IN (12,51,13),\n" +
		" PARTITION `p1` VALUES IN (23),\n" +
		" PARTITION `p2` VALUES IN (24,63),\n" +
		" PARTITION `p3` VALUES IN (45))"))
	tk.MustExec(`alter table t add primary key (a), add key (b), add key (c,b)`)

	// Note: MySQL cannot reorganize two non-consecutive list partitions :)
	// ERROR 1519 (HY000): When reorganizing a set of partitions they must be in consecutive order
	// https://bugs.mysql.com/bug.php?id=106011
	// https://bugs.mysql.com/bug.php?id=109939
	tk.MustExec(`alter table t reorganize partition p1, p3 into (partition pa values in (45,23,15))`)
	tk.MustExec(`admin check table t`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(11) NOT NULL,\n" +
		"  `b` varchar(55) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY LIST (`a`)\n" +
		"(PARTITION `p0` VALUES IN (12,51,13),\n" +
		" PARTITION `pa` VALUES IN (45,23,15),\n" +
		" PARTITION `p2` VALUES IN (24,63))"))
	tk.MustGetErrCode(`alter table t modify a varchar(20)`, errno.ErrUnsupportedDDLOperation)
}

type TestReorgDDLCallback struct {
	*callback.TestDDLCallback
	syncChan chan bool
}

func (tc *TestReorgDDLCallback) OnChanged(err error) error {
	err = tc.TestDDLCallback.OnChanged(err)
	<-tc.syncChan
	// We want to wait here
	<-tc.syncChan
	return err
}

func TestReorgPartitionConcurrent(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	schemaName := "ReorgPartConcurrent"
	tk.MustExec("create database " + schemaName)
	tk.MustExec("use " + schemaName)
	tk.MustExec(`create table t (a int unsigned PRIMARY KEY, b varchar(255), c int, key (b), key (c,b))` +
		` partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into t values (1,"1",1), (10,"10",10),(23,"23",32),(34,"34",43),(45,"45",54),(56,"56",65)`)
	dom := domain.GetDomain(tk.Session())
	originHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originHook)
	syncOnChanged := make(chan bool)
	defer close(syncOnChanged)
	hook := &TestReorgDDLCallback{TestDDLCallback: &callback.TestDDLCallback{Do: dom}, syncChan: syncOnChanged}
	dom.DDL().SetHook(hook)

	wait := make(chan bool)
	defer close(wait)

	currState := model.StateNone
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionReorganizePartition &&
			(job.SchemaState == model.StateDeleteOnly ||
				job.SchemaState == model.StateWriteOnly ||
				job.SchemaState == model.StateWriteReorganization ||
				job.SchemaState == model.StateDeleteReorganization) &&
			currState != job.SchemaState {
			currState = job.SchemaState
			<-wait
			<-wait
		}
	}
	alterErr := make(chan error, 1)
	go backgroundExec(store, schemaName, "alter table t reorganize partition p1 into (partition p1a values less than (15), partition p1b values less than (20))", alterErr)

	wait <- true
	// StateDeleteOnly
	deleteOnlyInfoSchema := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	wait <- true

	// StateWriteOnly
	wait <- true
	tk.MustExec(`insert into t values (11, "11", 11),(12,"12",21)`)
	tk.MustExec(`admin check table t`)
	writeOnlyInfoSchema := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	require.Equal(t, int64(1), writeOnlyInfoSchema.SchemaMetaVersion()-deleteOnlyInfoSchema.SchemaMetaVersion())
	deleteOnlyTbl, err := deleteOnlyInfoSchema.TableByName(model.NewCIStr(schemaName), model.NewCIStr("t"))
	require.NoError(t, err)
	writeOnlyTbl, err := writeOnlyInfoSchema.TableByName(model.NewCIStr(schemaName), model.NewCIStr("t"))
	require.NoError(t, err)
	writeOnlyParts := writeOnlyTbl.Meta().Partition
	writeOnlyTbl.Meta().Partition = deleteOnlyTbl.Meta().Partition
	// If not DeleteOnly is working, then this would show up when reorg is done
	tk.MustExec(`delete from t where a = 11`)
	tk.MustExec(`update t set b = "12b", c = 12 where a = 12`)
	tk.MustExec(`admin check table t`)
	writeOnlyTbl.Meta().Partition = writeOnlyParts
	tk.MustExec(`admin check table t`)
	wait <- true

	// StateWriteReorganization
	wait <- true
	tk.MustExec(`insert into t values (14, "14", 14),(15, "15",15)`)
	writeReorgInfoSchema := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (10),\n" +
		" PARTITION `p1` VALUES LESS THAN (20),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
	wait <- true

	// StateDeleteReorganization
	wait <- true
	tk.MustQuery(`select * from t where c between 10 and 22`).Sort().Check(testkit.Rows(""+
		"10 10 10",
		"12 12b 12",
		"14 14 14",
		"15 15 15"))
	deleteReorgInfoSchema := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	require.Equal(t, int64(1), deleteReorgInfoSchema.SchemaMetaVersion()-writeReorgInfoSchema.SchemaMetaVersion())
	tk.MustExec(`insert into t values (16, "16", 16)`)
	oldTbl, err := writeReorgInfoSchema.TableByName(model.NewCIStr(schemaName), model.NewCIStr("t"))
	require.NoError(t, err)
	partDef := oldTbl.Meta().Partition.Definitions[1]
	require.Equal(t, "p1", partDef.Name.O)
	rows := getNumRowsFromPartitionDefs(t, tk, oldTbl, oldTbl.Meta().Partition.Definitions[1:2])
	require.Equal(t, 5, rows)
	currTbl, err := deleteReorgInfoSchema.TableByName(model.NewCIStr(schemaName), model.NewCIStr("t"))
	require.NoError(t, err)
	currPart := currTbl.Meta().Partition
	currTbl.Meta().Partition = oldTbl.Meta().Partition
	tk.MustQuery(`select * from t where b = "16"`).Sort().Check(testkit.Rows("16 16 16"))
	tk.MustExec(`admin check table t`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (10),\n" +
		" PARTITION `p1` VALUES LESS THAN (20),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
	tk.MustQuery(`select * from t partition (p1)`).Sort().Check(testkit.Rows(""+
		"10 10 10",
		"12 12b 12",
		"14 14 14",
		"15 15 15",
		"16 16 16"))
	currTbl.Meta().Partition = currPart
	wait <- true
	syncOnChanged <- true
	// This reads the new schema (Schema update completed)
	tk.MustQuery(`select * from t where c between 10 and 22`).Sort().Check(testkit.Rows(""+
		"10 10 10",
		"12 12b 12",
		"14 14 14",
		"15 15 15",
		"16 16 16"))
	tk.MustExec(`admin check table t`)
	newInfoSchema := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	require.Equal(t, int64(1), newInfoSchema.SchemaMetaVersion()-deleteReorgInfoSchema.SchemaMetaVersion())
	oldTbl, err = deleteReorgInfoSchema.TableByName(model.NewCIStr(schemaName), model.NewCIStr("t"))
	require.NoError(t, err)
	partDef = oldTbl.Meta().Partition.Definitions[1]
	require.Equal(t, "p1a", partDef.Name.O)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (10),\n" +
		" PARTITION `p1a` VALUES LESS THAN (15),\n" +
		" PARTITION `p1b` VALUES LESS THAN (20),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
	newTbl, err := deleteReorgInfoSchema.TableByName(model.NewCIStr(schemaName), model.NewCIStr("t"))
	require.NoError(t, err)
	newPart := newTbl.Meta().Partition
	newTbl.Meta().Partition = oldTbl.Meta().Partition
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (10),\n" +
		" PARTITION `p1a` VALUES LESS THAN (15),\n" +
		" PARTITION `p1b` VALUES LESS THAN (20),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
	tk.MustExec(`admin check table t`)
	newTbl.Meta().Partition = newPart
	syncOnChanged <- true
	require.NoError(t, <-alterErr)
}

func TestReorgPartitionFailConcurrent(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	schemaName := "ReorgPartFailConcurrent"
	tk.MustExec("create database " + schemaName)
	tk.MustExec("use " + schemaName)
	tk.MustExec(`create table t (a int unsigned PRIMARY KEY, b varchar(255), c int, key (b), key (c,b))` +
		` partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into t values (1,"1",1), (12,"12",21),(23,"23",32),(34,"34",43),(45,"45",54),(56,"56",65)`)
	dom := domain.GetDomain(tk.Session())
	originHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originHook)
	hook := &callback.TestDDLCallback{Do: dom}
	dom.DDL().SetHook(hook)

	wait := make(chan bool)
	defer close(wait)

	// Test insert of duplicate key during copy phase
	injected := false
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionReorganizePartition && job.SchemaState == model.StateWriteReorganization && !injected {
			injected = true
			<-wait
			<-wait
		}
	}
	alterErr := make(chan error, 1)
	go backgroundExec(store, schemaName, "alter table t reorganize partition p1 into (partition p1a values less than (15), partition p1b values less than (20))", alterErr)
	wait <- true
	tk.MustExec(`insert into t values (14, "14", 14),(15, "15",15)`)
	tk.MustGetErrCode(`insert into t values (11, "11", 11),(12,"duplicate PK 💥", 13)`, errno.ErrDupEntry)
	tk.MustExec(`admin check table t`)
	wait <- true
	require.NoError(t, <-alterErr)
	tk.MustQuery(`select * from t where c between 10 and 22`).Sort().Check(testkit.Rows(""+
		"12 12 21",
		"14 14 14",
		"15 15 15"))
	tk.MustExec(`admin check table t`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (10),\n" +
		" PARTITION `p1a` VALUES LESS THAN (15),\n" +
		" PARTITION `p1b` VALUES LESS THAN (20),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))

	// Test reorg of duplicate key
	prevState := model.StateNone
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionReorganizePartition &&
			job.SchemaState == model.StateWriteReorganization &&
			job.SnapshotVer == 0 &&
			prevState != job.SchemaState {
			prevState = job.SchemaState
			<-wait
			<-wait
		}
		if job.Type == model.ActionReorganizePartition &&
			job.SchemaState == model.StateDeleteReorganization &&
			prevState != job.SchemaState {
			prevState = job.SchemaState
			<-wait
			<-wait
		}
	}
	go backgroundExec(store, schemaName, "alter table t reorganize partition p1a,p1b into (partition p1a values less than (14), partition p1b values less than (17), partition p1c values less than (20))", alterErr)
	wait <- true
	infoSchema := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	tbl, err := infoSchema.TableByName(model.NewCIStr(schemaName), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, 0, getNumRowsFromPartitionDefs(t, tk, tbl, tbl.Meta().Partition.AddingDefinitions))
	tk.MustExec(`delete from t where a = 14`)
	tk.MustExec(`insert into t values (13, "13", 31),(14,"14b",14),(16, "16",16)`)
	tk.MustExec(`admin check table t`)
	wait <- true
	wait <- true
	tbl, err = infoSchema.TableByName(model.NewCIStr(schemaName), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, 5, getNumRowsFromPartitionDefs(t, tk, tbl, tbl.Meta().Partition.AddingDefinitions))
	tk.MustExec(`delete from t where a = 15`)
	tk.MustExec(`insert into t values (11, "11", 11),(15,"15b",15),(17, "17",17)`)
	tk.MustExec(`admin check table t`)
	wait <- true
	require.NoError(t, <-alterErr)

	tk.MustExec(`admin check table t`)
	tk.MustQuery(`select * from t where a between 10 and 22`).Sort().Check(testkit.Rows(""+
		"11 11 11",
		"12 12 21",
		"13 13 31",
		"14 14b 14",
		"15 15b 15",
		"16 16 16",
		"17 17 17"))
	tk.MustQuery(`select * from t where c between 10 and 22`).Sort().Check(testkit.Rows(""+
		"11 11 11",
		"12 12 21",
		"14 14b 14",
		"15 15b 15",
		"16 16 16",
		"17 17 17"))
	tk.MustQuery(`select * from t where b between "10" and "22"`).Sort().Check(testkit.Rows(""+
		"11 11 11",
		"12 12 21",
		"13 13 31",
		"14 14b 14",
		"15 15b 15",
		"16 16 16",
		"17 17 17"))
}

func getNumRowsFromPartitionDefs(t *testing.T, tk *testkit.TestKit, tbl table.Table, defs []model.PartitionDefinition) int {
	ctx := tk.Session()
	pt := tbl.GetPartitionedTable()
	require.NotNil(t, pt)
	cnt := 0
	for _, def := range defs {
		data := getAllDataForPhysicalTable(t, ctx, pt.GetPartition(def.ID))
		require.True(t, len(data.keys) == len(data.vals))
		require.True(t, len(data.keys) == len(data.tp))
		for _, s := range data.tp {
			if s == "Record" {
				cnt++
			}
		}
	}
	return cnt
}

func TestReorgPartitionFailInject(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	schemaName := "ReorgPartFailInjectConcurrent"
	tk.MustExec("create database " + schemaName)
	tk.MustExec("use " + schemaName)
	tk.MustExec(`create table t (a int unsigned PRIMARY KEY, b varchar(255), c int, key (b), key (c,b))` +
		` partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into t values (1,"1",1), (12,"12",21),(23,"23",32),(34,"34",43),(45,"45",54),(56,"56",65)`)

	dom := domain.GetDomain(tk.Session())
	originHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originHook)
	hook := &callback.TestDDLCallback{Do: dom}
	dom.DDL().SetHook(hook)

	wait := make(chan bool)
	defer close(wait)

	injected := false
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionReorganizePartition && job.SchemaState == model.StateWriteReorganization && !injected {
			injected = true
			<-wait
			<-wait
		}
	}
	alterErr := make(chan error, 1)
	go backgroundExec(store, schemaName, "alter table t reorganize partition p1 into (partition p1a values less than (15), partition p1b values less than (20))", alterErr)
	wait <- true
	tk.MustExec(`insert into t values (14, "14", 14),(15, "15",15)`)
	tk.MustGetErrCode(`insert into t values (11, "11", 11),(12,"duplicate PK 💥", 13)`, errno.ErrDupEntry)
	tk.MustExec(`admin check table t`)
	wait <- true
	require.NoError(t, <-alterErr)
	tk.MustExec(`admin check table t`)
	tk.MustQuery(`select * from t where c between 10 and 22`).Sort().Check(testkit.Rows(""+
		"12 12 21",
		"14 14 14",
		"15 15 15"))
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (10),\n" +
		" PARTITION `p1a` VALUES LESS THAN (15),\n" +
		" PARTITION `p1b` VALUES LESS THAN (20),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))
}

func TestReorgPartitionRollback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	schemaName := "ReorgPartRollback"
	tk.MustExec("create database " + schemaName)
	tk.MustExec("use " + schemaName)
	tk.MustExec(`create table t (a int unsigned PRIMARY KEY, b varchar(255), c int, key (b), key (c,b))` +
		` partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into t values (1,"1",1), (12,"12",21),(23,"23",32),(34,"34",43),(45,"45",54),(56,"56",65)`)
	// TODO: Check that there are no additional placement rules,
	// bundles, or ranges with non-completed tableIDs
	// (partitions used during reorg, but was dropped)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockUpdateVersionAndTableInfoErr", `return(true)`))
	tk.MustExecToErr("alter table t reorganize partition p1 into (partition p1a values less than (15), partition p1b values less than (20))")
	tk.MustExec(`admin check table t`)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockUpdateVersionAndTableInfoErr"))
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr(schemaName), model.NewCIStr("t"))
	require.NoError(t, err)
	noNewTablesAfter(t, tk, ctx, tbl)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/reorgPartitionAfterDataCopy", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/ddl/reorgPartitionAfterDataCopy")
		require.NoError(t, err)
	}()
	tk.MustExecToErr("alter table t reorganize partition p1 into (partition p1a values less than (15), partition p1b values less than (20))")
	tk.MustExec(`admin check table t`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows("" +
		"t CREATE TABLE `t` (\n" +
		"  `a` int(10) unsigned NOT NULL,\n" +
		"  `b` varchar(255) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`),\n" +
		"  KEY `c` (`c`,`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (10),\n" +
		" PARTITION `p1` VALUES LESS THAN (20),\n" +
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))"))

	tbl, err = is.TableByName(model.NewCIStr(schemaName), model.NewCIStr("t"))
	require.NoError(t, err)
	noNewTablesAfter(t, tk, ctx, tbl)
}

func TestReorgPartitionData(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	schemaName := "ReorgPartData"
	tk.MustExec("create database " + schemaName)
	tk.MustExec("use " + schemaName)
	tk.MustExec(`SET @@session.sql_mode = default`)
	tk.MustExec(`create table t (a int PRIMARY KEY AUTO_INCREMENT, b varchar(255), c int, d datetime, key (b), key (c,b)) partition by range (a) (partition p1 values less than (0), partition p1M values less than (1000000))`)
	tk.MustContainErrMsg(`insert into t values (0, "Zero value!", 0, '2022-02-30')`, "[table:1292]Incorrect datetime value: '2022-02-30' for column 'd' at row 1")
	tk.MustExec(`SET @@session.sql_mode = 'ALLOW_INVALID_DATES,NO_AUTO_VALUE_ON_ZERO'`)
	tk.MustExec(`insert into t values (0, "Zero value!", 0, '2022-02-30')`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustQuery(`select * from t`).Sort().Check(testkit.Rows("0 Zero value! 0 2022-02-30 00:00:00"))
	tk.MustExec(`SET @@session.sql_mode = default`)
	tk.MustExec(`alter table t reorganize partition p1M into (partition p0 values less than (1), partition p2M values less than (2000000))`)
	tk.MustQuery(`select * from t`).Sort().Check(testkit.Rows("0 Zero value! 0 2022-02-30 00:00:00"))
	tk.MustExec(`admin check table t`)
}
