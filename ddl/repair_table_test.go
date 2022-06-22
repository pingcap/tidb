// Copyright 2022 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/util/domainutil"
	"github.com/stretchr/testify/require"
)

const repairTableLease = 600 * time.Millisecond

func TestRepairTable(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/infoschema/repairFetchCreateTable", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/infoschema/repairFetchCreateTable"))
	}()

	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, repairTableLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test repair table when TiDB is not in repair mode.
	tk.MustExec("CREATE TABLE t (a int primary key nonclustered, b varchar(10));")
	tk.MustGetErrMsg("admin repair table t CREATE TABLE t (a float primary key, b varchar(5));", "[ddl:8215]Failed to repair table: TiDB is not in REPAIR MODE")

	// Test repair table when the repaired list is empty.
	domainutil.RepairInfo.SetRepairMode(true)
	tk.MustGetErrMsg("admin repair table t CREATE TABLE t (a float primary key, b varchar(5));", "[ddl:8215]Failed to repair table: repair list is empty")

	// Test repair table when it's database isn't in repairInfo.
	domainutil.RepairInfo.SetRepairTableList([]string{"test.other_table"})
	tk.MustGetErrMsg("admin repair table t CREATE TABLE t (a float primary key, b varchar(5));", "[ddl:8215]Failed to repair table: database test is not in repair")

	// Test repair table when the table isn't in repairInfo.
	tk.MustExec("CREATE TABLE other_table (a int, b varchar(1), key using hash(b));")
	tk.MustGetErrMsg("admin repair table t CREATE TABLE t (a float primary key, b varchar(5));", "[ddl:8215]Failed to repair table: table t is not in repair")

	// Test user can't access to the repaired table.
	tk.MustGetErrMsg("select * from other_table", "[schema:1146]Table 'test.other_table' doesn't exist")

	// Test create statement use the same name with what is in repaired.
	tk.MustGetErrMsg("CREATE TABLE other_table (a int);", "[ddl:1103]Incorrect table name 'other_table'%!(EXTRA string=this table is in repair)")

	// Test column lost in repair table.
	tk.MustGetErrMsg("admin repair table other_table CREATE TABLE other_table (a int, c char(1));", "[ddl:8215]Failed to repair table: Column c has lost")

	// Test column type should be the same.
	tk.MustGetErrMsg("admin repair table other_table CREATE TABLE other_table (a bigint, b varchar(1), key using hash(b));", "[ddl:8215]Failed to repair table: Column a type should be the same")

	// Test index lost in repair table.
	tk.MustGetErrMsg("admin repair table other_table CREATE TABLE other_table (a int unique);", "[ddl:8215]Failed to repair table: Index a has lost")

	// Test index type should be the same.
	tk.MustGetErrMsg("admin repair table other_table CREATE TABLE other_table (a int, b varchar(2) unique)", "[ddl:8215]Failed to repair table: Index b type should be the same")

	// Test sub create statement in repair statement with the same name.
	tk.MustExec("admin repair table other_table CREATE TABLE other_table (a int);")

	// Test whether repair table name is case-sensitive.
	domainutil.RepairInfo.SetRepairMode(true)
	domainutil.RepairInfo.SetRepairTableList([]string{"test.other_table2"})
	tk.MustExec("CREATE TABLE otHer_tAblE2 (a int, b varchar(1));")
	tk.MustExec("admin repair table otHer_tAblE2 CREATE TABLE otHeR_tAbLe (a int, b varchar(2));")
	repairTable := external.GetTableByName(t, tk, "test", "otHeR_tAbLe") //nolint:typecheck
	require.Equal(t, "otHeR_tAbLe", repairTable.Meta().Name.O)

	// Test memory and system database is not for repair.
	domainutil.RepairInfo.SetRepairMode(true)
	domainutil.RepairInfo.SetRepairTableList([]string{"test.xxx"})
	tk.MustGetErrMsg("admin repair table performance_schema.xxx CREATE TABLE yyy (a int);", "[ddl:8215]Failed to repair table: memory or system database is not for repair")

	// Test the repair detail.
	turnRepairModeAndInit(true)
	defer turnRepairModeAndInit(false)
	// Domain reload the tableInfo and add it into repairInfo.
	tk.MustExec("CREATE TABLE origin (a int primary key nonclustered auto_increment, b varchar(10), c int);")
	// Repaired tableInfo has been filtered by `domain.InfoSchema()`, so get it in repairInfo.
	originTableInfo, _ := domainutil.RepairInfo.GetRepairedTableInfoByTableName("test", "origin")

	hook := &ddl.TestDDLCallback{Do: dom}
	var repairErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type != model.ActionRepairTable {
			return
		}
		if job.TableID != originTableInfo.ID {
			repairErr = errors.New("table id should be the same")
			return
		}
		if job.SchemaState != model.StateNone {
			repairErr = errors.New("repair job state should be the none")
			return
		}
		// Test whether it's readable, when repaired table is still stateNone.
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		_, repairErr = tk.Exec("select * from origin")
		// Repaired tableInfo has been filtered by `domain.InfoSchema()`, here will get an error cause user can't get access to it.
		if repairErr != nil && terror.ErrorEqual(repairErr, infoschema.ErrTableNotExists) {
			repairErr = nil
		}
	}
	originalHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originalHook)
	dom.DDL().SetHook(hook)

	// Exec the repair statement to override the tableInfo.
	tk.MustExec("admin repair table origin CREATE TABLE origin (a int primary key nonclustered auto_increment, b varchar(5), c int);")
	require.NoError(t, repairErr)

	// Check the repaired tableInfo is exactly the same with old one in tableID, indexID, colID.
	// testGetTableByName will extract the Table from `domain.InfoSchema()` directly.
	repairTable = external.GetTableByName(t, tk, "test", "origin") //nolint:typecheck
	require.Equal(t, originTableInfo.ID, repairTable.Meta().ID)
	require.Equal(t, 3, len(repairTable.Meta().Columns))
	require.Equal(t, originTableInfo.Columns[0].ID, repairTable.Meta().Columns[0].ID)
	require.Equal(t, originTableInfo.Columns[1].ID, repairTable.Meta().Columns[1].ID)
	require.Equal(t, originTableInfo.Columns[2].ID, repairTable.Meta().Columns[2].ID)
	require.Equal(t, 1, len(repairTable.Meta().Indices))
	require.Equal(t, originTableInfo.Columns[0].ID, repairTable.Meta().Indices[0].ID)
	require.Equal(t, originTableInfo.AutoIncID, repairTable.Meta().AutoIncID)

	require.Equal(t, mysql.TypeLong, repairTable.Meta().Columns[0].GetType())
	require.Equal(t, mysql.TypeVarchar, repairTable.Meta().Columns[1].GetType())
	require.Equal(t, 5, repairTable.Meta().Columns[1].GetFlen())
	require.Equal(t, mysql.TypeLong, repairTable.Meta().Columns[2].GetType())

	// Exec the show create table statement to make sure new tableInfo has been set.
	result := tk.MustQuery("show create table origin")
	require.Equal(t, "CREATE TABLE `origin` (\n  `a` int(11) NOT NULL AUTO_INCREMENT,\n  `b` varchar(5) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", result.Rows()[0][1])
}

func turnRepairModeAndInit(on bool) {
	list := make([]string, 0)
	if on {
		list = append(list, "test.origin")
	}
	domainutil.RepairInfo.SetRepairMode(on)
	domainutil.RepairInfo.SetRepairTableList(list)
}

func TestRepairTableWithPartition(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/infoschema/repairFetchCreateTable", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/infoschema/repairFetchCreateTable"))
	}()
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, repairTableLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists origin")

	turnRepairModeAndInit(true)
	defer turnRepairModeAndInit(false)
	// Domain reload the tableInfo and add it into repairInfo.
	tk.MustExec("create table origin (a int not null) partition by RANGE(a) (" +
		"partition p10 values less than (10)," +
		"partition p30 values less than (30)," +
		"partition p50 values less than (50)," +
		"partition p70 values less than (70)," +
		"partition p90 values less than (90));")
	// Test for some old partition has lost.
	tk.MustGetErrMsg("admin repair table origin create table origin (a int not null) partition by RANGE(a) ("+
		"partition p10 values less than (10),"+
		"partition p30 values less than (30),"+
		"partition p50 values less than (50),"+
		"partition p90 values less than (90),"+
		"partition p100 values less than (100));", "[ddl:8215]Failed to repair table: Partition p100 has lost")

	// Test for some partition changed the condition.
	tk.MustGetErrMsg("admin repair table origin create table origin (a int not null) partition by RANGE(a) ("+
		"partition p10 values less than (10),"+
		"partition p20 values less than (25),"+
		"partition p50 values less than (50),"+
		"partition p90 values less than (90));", "[ddl:8215]Failed to repair table: Partition p20 has lost")

	// Test for some partition changed the partition name.
	tk.MustGetErrMsg("admin repair table origin create table origin (a int not null) partition by RANGE(a) ("+
		"partition p10 values less than (10),"+
		"partition p30 values less than (30),"+
		"partition pNew values less than (50),"+
		"partition p90 values less than (90));", "[ddl:8215]Failed to repair table: Partition pnew has lost")

	originTableInfo, _ := domainutil.RepairInfo.GetRepairedTableInfoByTableName("test", "origin")
	tk.MustExec("admin repair table origin create table origin_rename (a int not null) partition by RANGE(a) (" +
		"partition p10 values less than (10)," +
		"partition p30 values less than (30)," +
		"partition p50 values less than (50)," +
		"partition p90 values less than (90));")
	repairTable := external.GetTableByName(t, tk, "test", "origin_rename") //nolint:typecheck
	require.Equal(t, originTableInfo.ID, repairTable.Meta().ID)
	require.Equal(t, 1, len(repairTable.Meta().Columns))
	require.Equal(t, originTableInfo.Columns[0].ID, repairTable.Meta().Columns[0].ID)
	require.Equal(t, 4, len(repairTable.Meta().Partition.Definitions))
	require.Equal(t, originTableInfo.Partition.Definitions[0].ID, repairTable.Meta().Partition.Definitions[0].ID)
	require.Equal(t, originTableInfo.Partition.Definitions[1].ID, repairTable.Meta().Partition.Definitions[1].ID)
	require.Equal(t, originTableInfo.Partition.Definitions[2].ID, repairTable.Meta().Partition.Definitions[2].ID)
	require.Equal(t, originTableInfo.Partition.Definitions[4].ID, repairTable.Meta().Partition.Definitions[3].ID)

	// Test hash partition.
	tk.MustExec("drop table if exists origin")
	domainutil.RepairInfo.SetRepairMode(true)
	domainutil.RepairInfo.SetRepairTableList([]string{"test.origin"})
	tk.MustExec("create table origin (a varchar(1), b int not null, c int, key idx(c)) partition by hash(b) partitions 30")

	// Test partition num in repair should be exactly same with old one, other wise will cause partition semantic problem.
	tk.MustGetErrMsg("admin repair table origin create table origin (a varchar(2), b int not null, c int, key idx(c)) partition by hash(b) partitions 20", "[ddl:8215]Failed to repair table: Hash partition num should be the same")

	originTableInfo, _ = domainutil.RepairInfo.GetRepairedTableInfoByTableName("test", "origin")
	tk.MustExec("admin repair table origin create table origin (a varchar(3), b int not null, c int, key idx(c)) partition by hash(b) partitions 30")
	repairTable = external.GetTableByName(t, tk, "test", "origin") //nolint:typecheck
	require.Equal(t, originTableInfo.ID, repairTable.Meta().ID)
	require.Equal(t, 30, len(repairTable.Meta().Partition.Definitions))
	require.Equal(t, originTableInfo.Partition.Definitions[0].ID, repairTable.Meta().Partition.Definitions[0].ID)
	require.Equal(t, originTableInfo.Partition.Definitions[1].ID, repairTable.Meta().Partition.Definitions[1].ID)
	require.Equal(t, originTableInfo.Partition.Definitions[29].ID, repairTable.Meta().Partition.Definitions[29].ID)
}
