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

package partition

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/gcworker"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror"
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
func noNewTablesAfter(t *testing.T, tk *testkit.TestKit, ctx sessionctx.Context, tbl table.Table, msg string) {
	waitForGC := tk.MustQuery(`select start_key, end_key, "queue" from mysql.gc_delete_range union all select start_key, end_key, "done" from mysql.gc_delete_range_done`).Rows()
	require.NoError(t, sessiontxn.NewTxn(context.Background(), ctx))
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	defer func() {
		err := txn.Rollback()
		require.NoError(t, err)
	}()
	// Get max tableID (if partitioned)
	tblID := tbl.Meta().ID
	logutil.DDLLogger().Info("noNewTablesAfter", zap.Int64("Table ID", tblID))
	if pt := tbl.GetPartitionedTable(); pt != nil {
		defs := pt.Meta().Partition.Definitions
		{
			for i := range defs {
				logutil.DDLLogger().Info("noNewTablesAfter", zap.Int64("Part ID", defs[i].ID))
				tblID = max(tblID, defs[i].ID)
			}
		}
	}
	prefix := tablecodec.EncodeTablePrefix(tblID + 1)
	it, err := txn.Iter(prefix, nil)
	require.NoError(t, err)
	for _, rowGC := range waitForGC {
		logutil.DDLLogger().Info("GC",
			zap.String("start", fmt.Sprintf("%v", rowGC[0])),
			zap.String("end", fmt.Sprintf("%v", rowGC[1])),
			zap.String("status", fmt.Sprintf("%s", rowGC[2])))
	}
ROW:
	for it.Valid() {
		foundTblID := tablecodec.DecodeTableID(it.Key())
		// There are internal table ids starting from MaxInt48 -1 and allocating decreasing ids
		// Allow 0xFF of them, See JobTableID, ReorgTableID, HistoryTableID, MDLTableID
		if it.Key()[0] == 't' && foundTblID >= 0xFFFFFFFFFF00 {
			break
		}
		for _, rowGC := range waitForGC {
			// OK if queued for range delete / GC
			startHex := fmt.Sprintf("%v", rowGC[0])
			endHex := fmt.Sprintf("%v", rowGC[1])
			end, err := hex.DecodeString(endHex)
			require.NoError(t, err)
			keyHex := hex.EncodeToString(it.Key())
			if startHex <= keyHex && keyHex < endHex {
				it.Close()
				it, err = txn.Iter(end, nil)
				require.NoError(t, err)
				continue ROW
			}
			if keyHex < "748000f" {
				logutil.DDLLogger().Error("not found in GC",
					zap.String("key", keyHex),
					zap.String("start", startHex),
					zap.String("end", endHex))
			}
		}
		if it.Key()[0] == 't' {
			is := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
			tbl, found := is.TableByID(context.Background(), foundTblID)
			tblmsg := " Table ID no longer maps to a table"
			if found {
				tblmsg = fmt.Sprintf(" Table name: %s", tbl.Meta().Name.O)
			}
			decodedKey := expression.DecodeKeyFromString(ctx.GetExprCtx().GetEvalCtx().TypeCtx(), is, it.Key().String())
			require.False(t, true, "Found table data after highest physical Table ID %d < %d (%s)\n%s\n"+msg+tblmsg, tblID, foundTblID, it.Key(), decodedKey)
		}
		break
	}
}

func getAllDataForTableID(t *testing.T, ctx sessionctx.Context, tableID int64) allTableData {
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
	prefix := tablecodec.EncodeTablePrefix(tableID)
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
			require.Equal(t, tableID, tblID)
			vals, _ := tablecodec.DecodeValuesBytesToStrings(it.Value())
			logutil.DDLLogger().Info("Record",
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

func TestReorgPartitionFailures(t *testing.T) {
	create := `create table t (a int unsigned PRIMARY KEY, b varchar(255), c int, key (b), key (c,b))` +
		` partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition p2 values less than (30),` +
		` partition pMax values less than (MAXVALUE))`
	alter := "alter table t reorganize partition p1,p2 into (partition p1 values less than (17), partition p1b values less than (24), partition p2 values less than (30))"
	beforeDML := []string{
		`insert into t values (1,"1",1),(2,"2",2),(12,"12",21),(13,"13",13),(17,"17",17),(18,"18",18),(23,"23",32),(34,"34",43),(45,"45",54),(56,"56",65)`,
		`update t set a = 11, b = "11", c = 11 where a = 17`,
		`update t set b = "21", c = 12 where c = 12`,
		`delete from t where a = 13`,
		`delete from t where b = "56"`,
	}
	beforeResult := testkit.Rows(
		"1 1 1", "11 11 11", "12 12 21", "18 18 18", "2 2 2", "23 23 32", "34 34 43", "45 45 54",
	)
	afterDML := []string{
		`insert into t values (5,"5",5),(13,"13",13)`,
		`update t set a = 17, b = "17", c = 17 where a = 11`,
		`update t set b = "12", c = 21 where c = 12`,
		`delete from t where a = 34`,
		`delete from t where b = "56"`,
	}
	afterResult := testkit.Rows(
		"1 1 1", "12 12 21", "13 13 13", "17 17 17", "18 18 18", "2 2 2", "23 23 32", "45 45 54", "5 5 5",
	)
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, afterDML, afterResult)
}

func TestRemovePartitionFailures(t *testing.T) {
	create := `create table t (a int unsigned primary key nonclustered, b int not null, c varchar(255)) partition by range(a) (
                        partition p0 values less than (100),
                        partition p1 values less than (200))`
	alter := `alter table t remove partitioning`
	beforeDML := []string{
		`insert into t values (1,1,1),(2,2,2),(3,3,3),(101,101,101),(102,102,102),(103,103,103)`,
		`update t set a = 11, b = "11", c = 11 where a = 1`,
		`update t set b = "12", c = 12 where b = 2`,
		`delete from t where a = 102`,
		`delete from t where b = 103`,
	}
	beforeResult := testkit.Rows("101 101 101", "11 11 11", "2 12 12", "3 3 3")
	afterDML := []string{
		`insert into t values (4,4,4),(5,5,5),(104,104,104)`,
		`update t set a = 1, b = 1, c = 1 where a = 11`,
		`update t set b = 2, c = 2 where c = 12`,
		`update t set a = 9, b = 9 where a = 104`,
		`delete from t where a = 5`,
		`delete from t where b = 102`,
	}
	afterResult := testkit.Rows("1 1 1", "101 101 101", "2 2 2", "3 3 3", "4 4 4", "9 9 104")
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, afterDML, afterResult)
}

func TestPartitionByFailures(t *testing.T) {
	create := `create table t (a int unsigned primary key nonclustered, b int not null, c varchar(255)) partition by range(a) (
                        partition p0 values less than (100),
                        partition p1 values less than (200))`
	alter := "alter table t partition by range (b) (partition pNoneC values less than (150), partition p2 values less than (300)) update indexes (`primary` global)"
	beforeDML := []string{
		`insert into t values (1,1,1),(2,2,2),(3,3,3),(101,101,101),(102,102,102),(103,103,103)`,
		`update t set a = 11, b = "11", c = 11 where a = 1`,
		`update t set b = "12", c = 12 where b = 2`,
		`delete from t where a = 102`,
		`delete from t where b = 103`,
	}
	beforeResult := testkit.Rows("101 101 101", "11 11 11", "2 12 12", "3 3 3")
	afterDML := []string{
		`insert into t values (4,4,4),(5,5,5),(104,104,104)`,
		`update t set a = 1, b = 1, c = 1 where a = 11`,
		`update t set b = 2, c = 2 where c = 12`,
		`update t set a = 9, b = 9 where a = 104`,
		`delete from t where a = 5`,
		`delete from t where b = 102`,
	}
	afterResult := testkit.Rows("1 1 1", "101 101 101", "2 2 2", "3 3 3", "4 4 4", "9 9 104")
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, afterDML, afterResult)
}

func TestReorganizePartitionListFailures(t *testing.T) {
	create := `create table t (a int unsigned primary key nonclustered global, b int not null, c varchar(255), unique index (c) global) partition by list(b) (
                        partition p0 values in (1,2,3),
                        partition p1 values in (4,5,6),
                        partition p2 values in (7,8,9))`
	alter := `alter table t reorganize partition p0,p2 into (partition pNone1 values in (1,9), partition pNone2 values in (2,8), partition pNone3 values in (3,7))`
	beforeDML := []string{
		`insert into t values (1,1,1),(2,2,2),(4,4,4),(8,8,8),(9,9,9),(6,6,6)`,
		`update t set a = 7, b = 7, c = 7 where a = 1`,
		`update t set b = 3, c = 3 where c = 4`,
		`delete from t where a = 8`,
		`delete from t where b = 2`,
	}
	beforeResult := testkit.Rows("4 3 3", "6 6 6", "7 7 7", "9 9 9")
	afterDML := []string{
		`insert into t values (1,1,1),(5,5,5),(8,8,8)`,
		`update t set a = 2, b = 2, c = 2 where a = 1`,
		`update t set a = 1, b = 1, c = 1 where c = 6`,
		`update t set a = 6, b = 6 where a = 9`,
		`delete from t where a = 5`,
		`delete from t where b = 3`,
	}
	afterResult := testkit.Rows("1 1 1", "2 2 2", "6 6 9", "7 7 7", "8 8 8")
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, afterDML, afterResult)
}

func TestPartitionByListFailures(t *testing.T) {
	create := `create table t (a int unsigned primary key nonclustered global, b int not null, c varchar(255), unique index (b), unique index (c) global) partition by list(b) (
                        partition p0 values in (1,2,3,4,5,6),
                        partition p1 values in (11,10,9,8,7))`
	alter := `alter table t partition by list columns (c) (partition pNone1 values in (1,11,3,5,7,9), partition pNone2 values in (2,4,8,10,6)) update indexes (b global, c local)`
	beforeDML := []string{
		`insert into t values (1,1,1),(2,2,2),(4,4,4),(8,8,8),(9,9,9),(6,6,6)`,
		`update t set a = 7, b = 7, c = 7 where a = 1`,
		`update t set b = 3, c = 3 where c = "4"`,
		`delete from t where a = 8`,
		`delete from t where b = 2`,
	}
	beforeResult := testkit.Rows("4 3 3", "6 6 6", "7 7 7", "9 9 9")
	afterDML := []string{
		`insert into t values (1,1,1),(5,5,5),(8,8,8)`,
		`update t set a = 2, b = 2, c = 2 where a = 1`,
		`update t set a = 1, b = 1, c = 1 where c = "6"`,
		`update t set a = 6, b = 6 where a = 9`,
		`delete from t where a = 5`,
		`delete from t where b = 3`,
	}
	afterResult := testkit.Rows("1 1 1", "2 2 2", "6 6 9", "7 7 7", "8 8 8")
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, afterDML, afterResult)
}

func TestAddHashPartitionFailures(t *testing.T) {
	create := `create table t (a int unsigned primary key nonclustered global, b int not null, c varchar(255), unique index (c) global) partition by hash(b) partitions 3`
	alter := `alter table t add partition partitions 2`
	beforeDML := []string{
		`insert into t values (1,1,1),(2,2,2),(4,4,4),(8,8,8),(9,9,9),(6,6,6)`,
		`update t set a = 7, b = 7, c = 7 where a = 1`,
		`update t set b = 3, c = 3 where c = "4"`,
		`delete from t where a = 8`,
		`delete from t where b = 2`,
	}
	beforeResult := testkit.Rows("4 3 3", "6 6 6", "7 7 7", "9 9 9")
	afterDML := []string{
		`insert into t values (1,1,1),(5,5,5),(8,8,8)`,
		`update t set a = 2, b = 2, c = 2 where a = 1`,
		`update t set a = 1, b = 1, c = 1 where c = "6"`,
		`update t set a = 6, b = 6 where a = 9`,
		`delete from t where a = 5`,
		`delete from t where b = 3`,
	}
	afterResult := testkit.Rows("1 1 1", "2 2 2", "6 6 9", "7 7 7", "8 8 8")
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, afterDML, afterResult)
}

func TestCoalesceKeyPartitionFailures(t *testing.T) {
	create := `create table t (a int unsigned primary key nonclustered global, b int not null, c varchar(255), unique index (b) global, unique index (c)) partition by key(c) partitions 5`
	alter := `alter table t coalesce partition 2`
	beforeDML := []string{
		`insert into t values (1,1,1),(2,2,2),(4,4,4),(8,8,8),(9,9,9),(6,6,6)`,
		`update t set a = 7, b = 7, c = 7 where a = 1`,
		`update t set b = 3, c = 3 where c = "4"`,
		`delete from t where a = 8`,
		`delete from t where b = 2`,
	}
	beforeResult := testkit.Rows("4 3 3", "6 6 6", "7 7 7", "9 9 9")
	afterDML := []string{
		`insert into t values (1,1,1),(5,5,5),(8,8,8)`,
		`update t set a = 2, b = 2, c = 2 where a = 1`,
		`update t set a = 1, b = 1, c = 1 where c = "6"`,
		`update t set a = 6, b = 6 where a = 9`,
		`delete from t where a = 5`,
		`delete from t where b = 3`,
	}
	afterResult := testkit.Rows("1 1 1", "2 2 2", "6 6 9", "7 7 7", "8 8 8")
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, afterDML, afterResult)
}

func TestPartitionByNonPartitionedTable(t *testing.T) {
	create := `create table t (a int)`
	alter := `alter table t partition by range (a) (partition p0 values less than (20))`
	beforeResult := testkit.Rows()
	afterResult := testkit.Rows()
	testReorganizePartitionFailures(t, create, alter, nil, beforeResult, nil, afterResult)
}

func testReorganizePartitionFailures(t *testing.T, createSQL, alterSQL string, beforeDML []string, beforeResult [][]any, afterDML []string, afterResult [][]any, skipTests ...string) {
	// Skip GC emulator, we trigger it manually to also clean up PlacementBundles
	util.EmulatorGCDisable()
	store := testkit.CreateMockStore(t)
	gcWorker, err := gcworker.NewMockGCWorker(store)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Fail means we simply inject an error, and set the error count very high to see what happens
	//   we do expect to do best effort rollback here as well!
	// Cancel means we set job.State = JobStateCancelled, as in no need to do more
	// Rollback means we do full rollback before returning error.
	tests := []struct {
		name            string
		count           int
		rollForwardFrom int
	}{
		{
			"Cancel",
			1,
			-1,
		},
		{
			"Fail",
			5,
			4,
		},
		{
			"Rollback",
			4,
			-1,
		},
	}
	oldWaitTimeWhenErrorOccurred := ddl.WaitTimeWhenErrorOccurred
	defer func() {
		ddl.WaitTimeWhenErrorOccurred = oldWaitTimeWhenErrorOccurred
	}()
	ddl.WaitTimeWhenErrorOccurred = 0
	for _, test := range tests {
	SUBTEST:
		for i := 1; i <= test.count; i++ {
			suffix := test.name + strconv.Itoa(i)
			for _, skip := range skipTests {
				if suffix == skip {
					continue SUBTEST
				}
			}
			suffixComment := ` /* ` + suffix + ` */`
			tk.MustExec(createSQL + suffixComment)
			for _, sql := range beforeDML {
				tk.MustExec(sql + suffixComment)
			}
			tk.MustQuery(`select * from t ` + suffixComment).Sort().Check(beforeResult)
			tOrg := external.GetTableByName(t, tk, "test", "t")
			var idxID int64
			if len(tOrg.Meta().Indices) > 0 {
				idxID = tOrg.Meta().Indices[0].ID
			}
			oldCreate := tk.MustQuery(`show create table t` + suffixComment).Rows()
			// Run GC to clean changes in beforeDML
			require.Nil(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))
			oldBundles, err := infosync.GetAllRuleBundles(context.TODO())
			require.NoError(t, err)
			name := "github.com/pingcap/tidb/pkg/ddl/reorgPart" + suffix
			term := "return(true)"
			if test.rollForwardFrom > 0 && test.rollForwardFrom <= i {
				term = "10*" + term
			}
			testfailpoint.Enable(t, name, term)
			err = tk.ExecToErr(alterSQL + suffixComment)
			tt := external.GetTableByName(t, tk, "test", "t")
			partition := tt.Meta().Partition
			rollback := false
			if test.rollForwardFrom > 0 && test.rollForwardFrom <= i {
				require.NoError(t, err)
			} else {
				rollback = true
				require.Error(t, err, "failpoint reorgPart"+suffix)
				// TODO: gracefully handle failures during WriteReorg also for nonclustered tables
				// with unique indexes.
				// Currently it can also do:
				// 	Error "[kv:1062]Duplicate entry '7' for key 't.c'" does not contain "Injected error by reorgPartFail2"
				//require.ErrorContains(t, err, "Injected error by reorgPart"+suffix)
				tk.MustQuery(`show create table t` + suffixComment).Check(oldCreate)
				if partition == nil {
					require.Nil(t, tOrg.Meta().Partition, suffix)
				} else {
					require.Equal(t, len(tOrg.Meta().Partition.Definitions), len(partition.Definitions), suffix)
					require.Equal(t, 0, len(partition.AddingDefinitions), suffix)
					require.Equal(t, 0, len(partition.DroppingDefinitions), suffix)
				}
				noNewTablesAfter(t, tk, tk.Session(), tOrg, suffix)
			}
			testfailpoint.Disable(t, name)
			require.Equal(t, len(tOrg.Meta().Indices), len(tt.Meta().Indices), suffix)
			if rollback && idxID != 0 {
				require.Equal(t, idxID, tt.Meta().Indices[0].ID, suffix)
			}
			require.Nil(t, gcWorker.DeleteRanges(context.TODO(), math.MaxInt64))
			noNewTablesAfter(t, tk, tk.Session(), tt, suffix)
			tk.MustExec(`admin check table t` + suffixComment)
			for _, sql := range afterDML {
				tk.MustExec(sql + suffixComment)
			}
			tk.MustQuery(`select * from t` + suffixComment).Sort().Check(afterResult)
			newBundles, err := infosync.GetAllRuleBundles(context.TODO())
			require.NoError(t, err)
			if rollback {
				for i := range newBundles {
					found := false
					for j := range oldBundles {
						if newBundles[i].ID == oldBundles[j].ID {
							require.Equal(t, oldBundles[j].String(), newBundles[i].String(), suffix)
							found = true
							break
						}
					}
					require.True(t, found, "%s: New bundle not cleaned up '%s':\n%s", suffix, newBundles[i].ID, newBundles[i].String())
				}
				require.Equal(t, len(oldBundles), len(newBundles), suffix)
			}
			tk.MustQuery(`select * from t` + suffixComment).Sort().Check(afterResult)
			tk.MustExec(`drop table t` + suffixComment)
			// TODO: Check TiFlash replicas
			// TODO: Check Label rules
			// TODO: Check autoIDs
		}
	}
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
	syncOnChanged := make(chan bool)
	defer close(syncOnChanged)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterReorganizePartition", func() {
		<-syncOnChanged
		// We want to wait here
		<-syncOnChanged
	})

	wait := make(chan bool)
	defer close(wait)

	currState := model.StateNone
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
		if job.Type == model.ActionReorganizePartition &&
			(job.SchemaState == model.StateDeleteOnly ||
				job.SchemaState == model.StateWriteOnly ||
				job.SchemaState == model.StateWriteReorganization ||
				job.SchemaState == model.StateDeleteReorganization ||
				job.SchemaState == model.StatePublic) &&
			currState != job.SchemaState {
			currState = job.SchemaState
			<-wait
			<-wait
		}
	})
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
	deleteOnlyTbl, err := deleteOnlyInfoSchema.TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	writeOnlyTbl, err := writeOnlyInfoSchema.TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr("t"))
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
	oldTbl, err := writeReorgInfoSchema.TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	partDef := oldTbl.Meta().Partition.Definitions[1]
	require.Equal(t, "p1", partDef.Name.O)
	rows := getNumRowsFromPartitionDefs(t, tk, oldTbl, oldTbl.Meta().Partition.Definitions[1:2])
	require.Equal(t, 5, rows)
	currTbl, err := deleteReorgInfoSchema.TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr("t"))
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
	wait <- true

	// StatePublic
	wait <- true
	tk.MustQuery(`select * from t where c between 10 and 22`).Sort().Check(testkit.Rows(""+
		"10 10 10",
		"12 12b 12",
		"14 14 14",
		"15 15 15",
		"16 16 16"))
	publicInfoSchema := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	require.Equal(t, int64(1), publicInfoSchema.SchemaMetaVersion()-deleteReorgInfoSchema.SchemaMetaVersion())
	tk.MustExec(`insert into t values (17, "17", 17)`)
	oldTbl, err = deleteReorgInfoSchema.TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	partDef = oldTbl.Meta().Partition.Definitions[1]
	require.Equal(t, "p1a", partDef.Name.O)
	rows = getNumRowsFromPartitionDefs(t, tk, oldTbl, oldTbl.Meta().Partition.Definitions[1:2])
	require.Equal(t, 3, rows)
	tk.MustQuery(`select * from t partition (p1a)`).Sort().Check(testkit.Rows("10 10 10", "12 12b 12", "14 14 14"))
	currTbl, err = publicInfoSchema.TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	currPart = currTbl.Meta().Partition
	currTbl.Meta().Partition = oldTbl.Meta().Partition
	tk.MustQuery(`select * from t where b = "17"`).Sort().Check(testkit.Rows("17 17 17"))
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
	currTbl.Meta().Partition = currPart
	wait <- true
	syncOnChanged <- true
	// This reads the new schema (Schema update completed)
	tk.MustQuery(`select * from t where c between 10 and 22`).Sort().Check(testkit.Rows(""+
		"10 10 10",
		"12 12b 12",
		"14 14 14",
		"15 15 15",
		"16 16 16",
		"17 17 17"))
	tk.MustExec(`admin check table t`)
	newInfoSchema := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	require.Equal(t, int64(1), newInfoSchema.SchemaMetaVersion()-publicInfoSchema.SchemaMetaVersion())
	oldTbl, err = publicInfoSchema.TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr("t"))
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
	newTbl, err := newInfoSchema.TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr("t"))
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

	wait := make(chan bool)
	defer close(wait)

	// Test insert of duplicate key during copy phase
	injected := false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
		if job.Type == model.ActionReorganizePartition && job.SchemaState == model.StateWriteReorganization && !injected {
			injected = true
			<-wait
			<-wait
		}
	})
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
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
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
	})
	go backgroundExec(store, schemaName, "alter table t reorganize partition p1a,p1b into (partition p1a values less than (14), partition p1b values less than (17), partition p1c values less than (20))", alterErr)
	wait <- true
	infoSchema := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	tbl, err := infoSchema.TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, 0, getNumRowsFromPartitionDefs(t, tk, tbl, tbl.Meta().Partition.AddingDefinitions))
	tk.MustExec(`delete from t where a = 14`)
	tk.MustExec(`insert into t values (13, "13", 31),(14,"14b",14),(16, "16",16)`)
	tk.MustExec(`admin check table t`)
	wait <- true
	wait <- true
	tbl, err = infoSchema.TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr("t"))
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
		data := getAllDataForTableID(t, ctx, def.ID)
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

	wait := make(chan bool)
	defer close(wait)

	injected := false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
		if job.Type == model.ActionReorganizePartition && job.SchemaState == model.StateWriteReorganization && !injected {
			injected = true
			<-wait
			<-wait
		}
	})
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
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockUpdateVersionAndTableInfoErr", `return(1)`)
	tk.MustExecToErr("alter table t reorganize partition p1 into (partition p1a values less than (15), partition p1b values less than (20))")
	tk.MustExec(`admin check table t`)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/mockUpdateVersionAndTableInfoErr")
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	noNewTablesAfter(t, tk, ctx, tbl, "Reorganize rollback")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/reorgPartitionAfterDataCopy", `return(true)`)
	defer func() {
		testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/reorgPartitionAfterDataCopy")
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

	tbl, err = is.TableByName(context.Background(), pmodel.NewCIStr(schemaName), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	noNewTablesAfter(t, tk, ctx, tbl, "Reorganize rollback")
}

func TestPartitionByColumnChecks(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	cols := "(i int, f float, c char(20), b bit(2), b32 bit(32), b64 bit(64), d date, dt datetime, dt6 datetime(6), ts timestamp, ts6 timestamp(6), j json)"
	vals := `(1, 2.2, "A and c", b'10', b'10001000100010001000100010001000', b'1000100010001000100010001000100010001000100010001000100010001000', '2024-09-24', '2024-09-24 13:01:02', '2024-09-24 13:01:02.123456', '2024-09-24 13:01:02', '2024-09-24 13:01:02.123456', '{"key1": "value1", "key2": "value2"}')`
	tk.MustExec(`create table t ` + cols)
	testCases := []struct {
		partClause string
		err        error
	}{
		{"key (c) partitions 2", nil},
		{"key (j) partitions 2", dbterror.ErrNotAllowedTypeInPartition},
		{"list (c) (partition pDef default)", dbterror.ErrNotAllowedTypeInPartition},
		{"list (b) (partition pDef default)", nil},
		{"list (f) (partition pDef default)", dbterror.ErrNotAllowedTypeInPartition},
		{"list (j) (partition pDef default)", dbterror.ErrNotAllowedTypeInPartition},
		{"list columns (b) (partition pDef default)", dbterror.ErrNotAllowedTypeInPartition},
		{"list columns (f) (partition pDef default)", dbterror.ErrNotAllowedTypeInPartition},
		{"list columns (ts) (partition pDef default)", dbterror.ErrNotAllowedTypeInPartition},
		{"list columns (j) (partition pDef default)", dbterror.ErrNotAllowedTypeInPartition},
		{"hash (year(ts)) partitions 2", dbterror.ErrWrongExprInPartitionFunc},
		{"hash (ts) partitions 2", dbterror.ErrNotAllowedTypeInPartition},
		{"hash (ts6) partitions 2", dbterror.ErrNotAllowedTypeInPartition},
		{"hash (d) partitions 2", dbterror.ErrNotAllowedTypeInPartition},
		{"hash (f) partitions 2", dbterror.ErrNotAllowedTypeInPartition},
		{"range (c) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range (f) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range (d) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range (dt) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range (dt6) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range (ts) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range (ts6) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range (j) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range columns (b) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range columns (b64) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range columns (c) (partition pMax values less than (maxvalue))", nil},
		{"range columns (f) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range columns (d) (partition pMax values less than (maxvalue))", nil},
		{"range columns (dt) (partition pMax values less than (maxvalue))", nil},
		{"range columns (dt6) (partition pMax values less than (maxvalue))", nil},
		{"range columns (ts) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range columns (ts6) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
		{"range columns (j) (partition pMax values less than (maxvalue))", dbterror.ErrNotAllowedTypeInPartition},
	}
	for _, testCase := range testCases {
		err := tk.ExecToErr(`create table tt ` + cols + ` partition by ` + testCase.partClause)
		require.ErrorIs(t, err, testCase.err, testCase.partClause)
		if testCase.err == nil {
			tk.MustExec(`drop table tt`)
		}
		err = tk.ExecToErr(`alter table t partition by ` + testCase.partClause)
		require.ErrorIs(t, err, testCase.err)
	}

	// Not documented or tested!!
	// KEY - Allows more types than documented, should be OK!
	tk.MustExec(`create table kb ` + cols + ` partition by key(b) partitions 2`)
	tk.MustExec(`create table kf ` + cols + ` partition by key(f) partitions 2`)
	tk.MustExec(`create table kts ` + cols + ` partition by key(ts) partitions 2`)
	tk.MustExec(`create table hb ` + cols + ` partition by hash(b) partitions 2`)
	tk.MustExec(`insert into hb values ` + vals)
	tk.MustQuery(`select count(*) from hb where b = b'10'`).Check(testkit.Rows("1"))
	tk.MustExec(`alter table hb partition by hash(b) partitions 3`)
	tk.MustExec(`insert into hb values ` + vals)
	tk.MustQuery(`select count(*) from hb where b = b'10'`).Check(testkit.Rows("2"))
	tk.MustExec(`create table hb32 ` + cols + ` partition by hash(b32) partitions 2`)
	tk.MustExec(`insert into hb32 values ` + vals)
	tk.MustExec(`alter table hb32 partition by hash(b32) partitions 3`)
	tk.MustExec(`insert into hb32 values ` + vals)
	tk.MustExec(`create table rb ` + cols + ` partition by range (b) (partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into rb values ` + vals)
	tk.MustExec(`alter table rb partition by range(b) (partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into rb values ` + vals)
	tk.MustExec(`create table rb32 ` + cols + ` partition by range (b32) (partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into rb32 values ` + vals)
	tk.MustExec(`alter table rb32 partition by range(b32) (partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into rb32 values ` + vals)
	tk.MustExec(`create table rb64 ` + cols + ` partition by range (b64) (partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into rb64 values ` + vals)
	tk.MustExec(`alter table rb64 partition by range(b64) (partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into rb64 values ` + vals)
}

func TestPartitionIssue56634(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/updateVersionAndTableInfoErrInStateDeleteReorganization", `4*return(1)`)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	// Changed, since StatePublic can no longer rollback!
	tk.MustExec("alter table t partition by range(a) (partition p1 values less than (20))")
}

func TestReorgPartitionFailuresPlacementPolicy(t *testing.T) {
	create := `create table t (a int unsigned PRIMARY KEY, b varchar(255), c int, key (b), key (c,b))` +
		` partition by range (a) ` +
		`(partition p0 values less than (10),` +
		` partition p1 values less than (20),` +
		` partition p2 values less than (30),` +
		` partition pMax values less than (MAXVALUE))`
	beforeDML := []string{
		`create or replace placement policy pp1 followers=1`,
		`create or replace placement policy pp2 followers=2`,
		`create or replace placement policy pp3 followers=3`,
		`alter table t placement policy ='pp1'`,
		`alter table t partition p1 placement policy ='pp2'`,
		`alter table t partition p2 placement policy ='pp3'`,
	}
	beforeResult := testkit.Rows()
	alter := "alter table t reorganize partition p1,p2 into (partition p1 values less than (17), partition p1b values less than (24) placement policy 'pp1', partition p2 values less than (30))"
	afterResult := testkit.Rows()
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, nil, afterResult)
}

func TestRemovePartitionFailuresPlacementPolicy(t *testing.T) {
	create := `create table t (a int unsigned primary key nonclustered, b int not null, c varchar(255)) partition by range(a) (
                        partition p0 values less than (50),
                        partition p1 values less than (100),
                        partition p2 values less than (200))`
	alter := `alter table t remove partitioning`
	beforeDML := []string{
		`create or replace placement policy pp1 followers=1`,
		`create or replace placement policy pp2 followers=2`,
		`create or replace placement policy pp3 followers=2`,
		`alter table t placement policy ='pp3'`,
		`alter table t partition p1 placement policy ='pp1'`,
		`alter table t partition p2 placement policy ='pp2'`,
		`insert into t values (1,1,1),(2,2,2),(3,3,3),(101,101,101),(102,102,102),(103,103,103)`,
		`update t set a = 11, b = "11", c = 11 where a = 1`,
		`update t set b = "12", c = 12 where b = 2`,
		`delete from t where a = 102`,
		`delete from t where b = 103`,
	}
	beforeResult := testkit.Rows("101 101 101", "11 11 11", "2 12 12", "3 3 3")
	afterDML := []string{
		`insert into t values (4,4,4),(5,5,5),(104,104,104)`,
		`update t set a = 1, b = 1, c = 1 where a = 11`,
		`update t set b = 2, c = 2 where c = 12`,
		`update t set a = 9, b = 9 where a = 104`,
		`delete from t where a = 5`,
		`delete from t where b = 102`,
	}
	afterResult := testkit.Rows("1 1 1", "101 101 101", "2 2 2", "3 3 3", "4 4 4", "9 9 104")
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, afterDML, afterResult)
}

func TestPartitionByFailuresPlacementPolicy(t *testing.T) {
	create := `create table t (a int unsigned primary key nonclustered, b int not null, c varchar(255)) partition by range(a) (
                        partition p0 values less than (100),
                        partition p1 values less than (200))`
	beforeDML := []string{
		`create or replace placement policy pp1 followers=1`,
		`create or replace placement policy pp2 followers=2`,
		`create or replace placement policy pp3 followers=3`,
		`alter table t placement policy ='pp1'`,
		`alter table t partition p0 placement policy ='pp2'`,
		`insert into t values (1,1,1),(2,2,2),(3,3,3),(101,101,101),(102,102,102),(103,103,103)`,
		`update t set a = 11, b = "11", c = 11 where a = 1`,
		`update t set b = "12", c = 12 where b = 2`,
		`delete from t where a = 102`,
		`delete from t where b = 103`,
	}
	beforeResult := testkit.Rows("101 101 101", "11 11 11", "2 12 12", "3 3 3")
	alter := "alter table t partition by range (b) (partition pNoneC values less than (150) placement policy 'pp3', partition p2 values less than (300)) update indexes (`primary` global)"
	afterDML := []string{
		`insert into t values (4,4,4),(5,5,5),(104,104,104)`,
		`update t set a = 1, b = 1, c = 1 where a = 11`,
		`update t set b = 2, c = 2 where c = 12`,
		`update t set a = 9, b = 9 where a = 104`,
		`delete from t where a = 5`,
		`delete from t where b = 102`,
	}
	afterResult := testkit.Rows("1 1 1", "101 101 101", "2 2 2", "3 3 3", "4 4 4", "9 9 104")
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, afterDML, afterResult)
}

func TestPartitionNonPartitionedFailuresPlacementPolicy(t *testing.T) {
	create := `create table t (a int unsigned primary key nonclustered, b int not null, c varchar(255))`
	beforeDML := []string{
		`create or replace placement policy pp1 followers=1`,
		`create or replace placement policy pp2 followers=2`,
		`alter table t placement policy ='pp1'`,
		`insert into t values (1,1,1),(2,2,2),(3,3,3),(101,101,101),(102,102,102),(103,103,103)`,
		`update t set a = 11, b = "11", c = 11 where a = 1`,
		`update t set b = "12", c = 12 where b = 2`,
		`delete from t where a = 102`,
		`delete from t where b = 103`,
	}
	beforeResult := testkit.Rows("101 101 101", "11 11 11", "2 12 12", "3 3 3")
	alter := "alter table t partition by range (b) (partition pNoneC values less than (150), partition p2 values less than (300) placement policy 'pp1') update indexes (`primary` global)"
	afterDML := []string{
		`insert into t values (4,4,4),(5,5,5),(104,104,104)`,
		`update t set a = 1, b = 1, c = 1 where a = 11`,
		`update t set b = 2, c = 2 where c = 12`,
		`update t set a = 9, b = 9 where a = 104`,
		`delete from t where a = 5`,
		`delete from t where b = 102`,
	}
	afterResult := testkit.Rows("1 1 1", "101 101 101", "2 2 2", "3 3 3", "4 4 4", "9 9 104")
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, afterDML, afterResult)
}

func TestReorganizePartitionFailuresAddPlacementPolicy(t *testing.T) {
	create := `create table t (a int unsigned primary key nonclustered, b int not null, c varchar(255)) partition by range(a) (
                        partition p0 values less than (50),
                        partition p1 values less than (100),
                        partition p2 values less than (200))`
	beforeDML := []string{
		`create or replace placement policy pp1 followers=1`,
		`insert into t values (4,4,4),(5,5,5),(104,104,104)`,
	}
	beforeResult := testkit.Rows("104 104 104", "4 4 4", "5 5 5")
	alter := `alter table t reorganize partition p2 into (partition p2 values less than (200), partition pMax values less than (maxvalue) placement policy pp1)`
	afterResult := beforeResult
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, nil, afterResult)
}

func TestPartitionByFailuresAddPlacementPolicyGlobalIndex(t *testing.T) {
	create := `create table t (a int unsigned primary key nonclustered global, b int not null, c varchar(255), unique key (c) global) partition by range(a) (
                        partition p0 values less than (50),
                        partition p1 values less than (100),
                        partition p2 values less than (200))`
	beforeDML := []string{
		`create or replace placement policy pp1 followers=1`,
		`create or replace placement policy pp2 followers=2`,
		`alter table t placement policy pp1`,
		`alter table t partition p2 placement policy pp2`,
		`insert into t values (4,4,4),(50,50,50),(111,111,111),(155,155,155)`,
	}
	beforeResult := testkit.Rows("111 111 111", "155 155 155", "4 4 4", "50 50 50")
	alter := "alter table t partition by range (a) (partition p1 values less than (150), partition pMax values less than (maxvalue) placement policy pp1) update indexes (`primary` local, `c` global)"
	afterResult := beforeResult
	testReorganizePartitionFailures(t, create, alter, beforeDML, beforeResult, nil, afterResult)
}
