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

package infoschemav2test

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestSpecialSchemas(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")

	tk.MustExec("set @@global.tidb_schema_cache_size = 1073741824;")
	tk.MustQuery("select @@global.tidb_schema_cache_size;").Check(testkit.Rows("1073741824"))
	tk.MustExec("create table t (id int);")
	is := domain.GetDomain(tk.Session()).InfoSchema()
	isV2, _ := infoschema.IsV2(is)
	require.True(t, isV2)

	tk.MustQuery("show databases;").Check(testkit.Rows(
		"INFORMATION_SCHEMA", "METRICS_SCHEMA", "PERFORMANCE_SCHEMA", "mysql", "sys", "test"))
	tk.MustExec("use information_schema;")
	tk.MustQuery("show tables;").MultiCheckContain([]string{
		"ANALYZE_STATUS",
		"ATTRIBUTES",
		"CHARACTER_SETS",
		"COLLATIONS",
		"COLUMNS",
		"COLUMN_PRIVILEGES",
		"COLUMN_STATISTICS",
		"VIEWS"})
	tk.MustQuery("show create table tables;").MultiCheckContain([]string{
		`TABLE_CATALOG`,
		`TABLE_SCHEMA`,
		`TABLE_NAME`,
		`TABLE_TYPE`,
	})

	tk.ExecToErr("drop database information_schema;")
	tk.ExecToErr("drop table views;")

	tk.MustExec("use metrics_schema;")
	tk.MustQuery("show tables;").CheckContain("uptime")
	tk.MustQuery("show create table uptime;").CheckContain("time")

	tk.MustExec("set @@global.tidb_schema_cache_size = default;")
}

func checkPIDNotExist(t *testing.T, dom *domain.Domain, pid int64) {
	is := dom.InfoSchema()
	ptbl, dbInfo, pdef := is.FindTableByPartitionID(pid)
	require.Nil(t, ptbl)
	require.Nil(t, dbInfo)
	require.Nil(t, pdef)
}

func getPIDForP3(t *testing.T, dom *domain.Domain) (int64, table.Table) {
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("pt"))
	require.NoError(t, err)
	pi := tbl.Meta().GetPartitionInfo()
	pid := pi.GetPartitionIDByName("p3")
	ptbl, _, _ := is.FindTableByPartitionID(pid)
	require.Equal(t, ptbl.Meta().ID, tbl.Meta().ID)
	return pid, tbl
}

func TestFindTableByPartitionID(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table pt (id int) partition by range (id) (
partition p0 values less than (10),
partition p1 values less than (20),
partition p2 values less than (30),
partition p3 values less than (40))`)

	pid, tbl := getPIDForP3(t, dom)
	is := dom.InfoSchema()
	tbl1, dbInfo, pdef := is.FindTableByPartitionID(pid)
	require.Equal(t, tbl1.Meta().ID, tbl.Meta().ID)
	require.Equal(t, dbInfo.Name.L, "test")
	require.Equal(t, pdef.ID, pid)

	// Test FindTableByPartitionID after dropping a unrelated partition.
	tk.MustExec("alter table pt drop partition p2")
	is = dom.InfoSchema()
	tbl2, dbInfo, pdef := is.FindTableByPartitionID(pid)
	require.Equal(t, tbl2.Meta().ID, tbl.Meta().ID)
	require.Equal(t, dbInfo.Name.L, "test")
	require.Equal(t, pdef.ID, pid)

	// Test FindTableByPartitionID after dropping that partition.
	tk.MustExec("alter table pt drop partition p3")
	checkPIDNotExist(t, dom, pid)

	// Test FindTableByPartitionID after adding back the partition.
	tk.MustExec("alter table pt add partition (partition p3 values less than (35))")
	checkPIDNotExist(t, dom, pid)
	pid, _ = getPIDForP3(t, dom)

	// Test FindTableByPartitionID after truncate partition.
	tk.MustExec("alter table pt truncate partition p3")
	checkPIDNotExist(t, dom, pid)
	pid, _ = getPIDForP3(t, dom)

	// Test FindTableByPartitionID after reorganize partition.
	tk.MustExec(`alter table pt reorganize partition p1,p3 INTO (
PARTITION p3 VALUES LESS THAN (1970),
PARTITION p5 VALUES LESS THAN (1980))`)
	checkPIDNotExist(t, dom, pid)
	_, _ = getPIDForP3(t, dom)

	// Test FindTableByPartitionID after exchange partition.
	tk.MustExec("create table nt (id int)")
	is = dom.InfoSchema()
	ntbl, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("nt"))
	require.NoError(t, err)

	tk.MustExec("alter table pt exchange partition p3 with table nt")
	is = dom.InfoSchema()
	ptbl, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("pt"))
	require.NoError(t, err)
	pi := ptbl.Meta().GetPartitionInfo()
	pid = pi.GetPartitionIDByName("p3")
	require.Equal(t, pid, ntbl.Meta().ID)
}

func TestListTablesWithSpecialAttribute(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`)
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	for _, v := range []int{1024000, 0} {
		tk.MustExec("set @@global.tidb_schema_cache_size = ?", v)

		tk.MustExec("create database test_db1")
		tk.MustExec("use test_db1")
		tk.MustExec("create table t_ttl (created_at datetime) ttl = created_at + INTERVAL 1 YEAR ttl_enable = 'ON'")
		checkResult(t, tk, "test_db1 t_ttl")

		tk.MustExec("alter table t_ttl remove ttl")
		checkResult(t, tk)

		tk.MustExec("drop table t_ttl")
		checkResult(t, tk)

		tk.MustExec("create table t_ttl (created_at1 datetime) ttl = created_at1 + INTERVAL 1 YEAR ttl_enable = 'ON'")
		checkResult(t, tk, "test_db1 t_ttl")

		tk.MustExec("create database test_db2")
		tk.MustExec("use test_db2")
		checkResult(t, tk, "test_db1 t_ttl")

		tk.MustExec("create table t_ttl (created_at datetime) ttl = created_at + INTERVAL 1 YEAR ttl_enable = 'ON'")
		checkResult(t, tk, "test_db1 t_ttl", "test_db2 t_ttl")

		tk.MustExec("create table t_tiflash (id int)")
		checkResult(t, tk, "test_db1 t_ttl", "test_db2 t_ttl")

		tk.MustExec("alter table t_tiflash set tiflash replica 1")
		checkResult(t, tk, "test_db1 t_ttl", "test_db2 t_tiflash", "test_db2 t_ttl")

		tk.MustExec("alter table t_tiflash set tiflash replica 0")
		checkResult(t, tk, "test_db1 t_ttl", "test_db2 t_ttl")

		tk.MustExec("drop table t_tiflash")
		checkResult(t, tk, "test_db1 t_ttl", "test_db2 t_ttl")

		tk.MustExec("create table t_tiflash (id int)")
		tk.MustExec("alter table t_tiflash set tiflash replica 1")
		checkResult(t, tk, "test_db1 t_ttl", "test_db2 t_tiflash", "test_db2 t_ttl")

		tk.MustExec("drop database test_db1")
		checkResult(t, tk, "test_db2 t_tiflash", "test_db2 t_ttl")

		tk.MustExec("create or replace placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1\"")
		tk.MustExec("create table t_placement (a int) placement policy=\"x\"")
		checkResult(t, tk, "test_db2 t_placement", "test_db2 t_tiflash", "test_db2 t_ttl")

		tk.MustExec("create table pt_placement (id int) placement policy x partition by HASH(id) PARTITIONS 4")
		checkResult(t, tk, "test_db2 pt_placement", "test_db2 t_placement", "test_db2 t_tiflash", "test_db2 t_ttl")

		tk.MustExec("drop database test_db2")
		checkResult(t, tk)
	}
}

func checkResult(t *testing.T, tk *testkit.TestKit, result ...string) {
	is := domain.GetDomain(tk.Session()).InfoSchema()
	ch := is.ListTablesWithSpecialAttribute(infoschema.AllSpecialAttribute)
	var rows []string
	for _, v := range ch {
		for _, tblInfo := range v.TableInfos {
			rows = append(rows, v.DBName.L+" "+tblInfo.Name.L)
		}
	}
	slices.SortFunc(rows, strings.Compare)
	require.Equal(t, rows, result)
}

func TestTiDBSchemaCacheSizeVariable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	dom.Reload() // need this to trigger infoschema rebuild to reset capacity
	is := dom.InfoSchema()
	ok, raw := infoschema.IsV2(is)
	if ok {
		val := variable.SchemaCacheSize.Load()
		tk.MustQuery("select @@global.tidb_schema_cache_size").CheckContain(strconv.FormatUint(val, 10))

		// On start, the capacity might not be set correctly because infoschema have not load global variable yet.
		// cap := raw.Data.CacheCapacity()
		// require.Equal(t, cap, uint64(val))
	}

	tk.MustExec("set @@global.tidb_schema_cache_size = 1024 * 1024 * 1024")
	tk.MustQuery("select @@global.tidb_schema_cache_size").CheckContain("1073741824")
	require.Equal(t, variable.SchemaCacheSize.Load(), uint64(1073741824))
	tk.MustExec("create table trigger_reload (id int)") // need to trigger infoschema rebuild to reset capacity
	is = dom.InfoSchema()
	ok, raw = infoschema.IsV2(is)
	require.True(t, ok)
	require.Equal(t, raw.Data.CacheCapacity(), uint64(1073741824))
}

func TestUnrelatedDDLTriggerReload(t *testing.T) {
	// TODO: pass context to loadTableInfo to avoid the global failpoint when it's ready
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_schema_cache_size = 512 * 1024 * 1024")

	tk.MustExec("create table t1 (id int)")
	tk.MustQuery("select * from t1").Check(testkit.Rows())
	// Mock t1 schema cache been evicted.
	is := dom.InfoSchema()
	ok, v2 := infoschema.IsV2(is)
	require.True(t, ok)
	v2.EvictTable(model.NewCIStr("test"), model.NewCIStr("t1"))

	tk.MustExec("create table t2 (id int)")

	// DDL on t2 should not cause reload or cache miss on t1
	// before test, check failpoint works
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadTableInfoError", `return(true)`)
	tk.MustExecToErr("select * from t1")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadTableInfoError")

	// Refill the cache, and do DDL
	tk.MustQuery("select * from t1").Check(testkit.Rows())
	tk.MustExec("create table t3 (id int)")

	// Ensure failpoint works, and now verify that the code never call loadTableInfo()
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadTableInfoError", `return(true)`)
	tk.MustQuery("select * from t1").Check(testkit.Rows())
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadTableInfoError")
}

func TestTrace(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_schema_cache_size = 1024 * 1024 * 1024")
	tk.MustExec("create table t_trace(id int key auto_increment)")
	is := dom.InfoSchema()
	ok, raw := infoschema.IsV2(is)
	require.True(t, ok)

	// Evict the table cache and check the trace information can catch this calling.
	raw.EvictTable(model.NewCIStr("test"), model.NewCIStr("t_trace"))
	tk.MustQuery("trace select * from information_schema.tables where table_schema='test' and table_name='t_trace'").CheckContain("infoschema.loadTableInfo")
}

func TestCachedTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_schema_cache_size = 1024 * 1024 * 1024")
	tk.MustExec("create table t_cache (id int key auto_increment)")
	tk.MustExec("insert into t_cache values (1)")
	tk.MustExec("alter table t_cache cache")
	is := dom.InfoSchema()
	ok, raw := infoschema.IsV2(is)
	require.True(t, ok)

	// Cover a case that after cached table evict and load, table.Table goes wrong.
	raw.EvictTable(model.NewCIStr("test"), model.NewCIStr("t_cache"))
	tk.MustExec("insert into t_cache values (2)") // no panic here
	tk.MustQuery("select * from t_cache").Check(testkit.Rows("1", "2"))
}

func BenchmarkTableByName(t *testing.B) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_schema_cache_size = 512 * 1024 * 1024")
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("create table t%d (id int)", i))
	}
	is := dom.InfoSchema()
	db := model.NewCIStr("test")
	tbl := model.NewCIStr("t123")
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		_, err := is.TableByName(context.Background(), db, tbl)
		require.NoError(t, err)
	}
	t.StopTimer()
}

func TestFullLoadAndSnapshot(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_schema_cache_size = 512 * 1024 * 1024")

	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	timeSafe := time.Now().Add(-48 * 60 * 60 * time.Second).Format("20060102-15:04:05 -0700 MST")
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk.MustExec(fmt.Sprintf(safePointSQL, timeSafe))

	tk.MustExec("use test")
	tk.MustExec("create global temporary table tmp (id int) on commit delete rows")

	tk.MustExec("create database db1")
	tk.MustExec("create database db2")
	tk.MustExec("use db1")
	tk.MustExec("create table t (id int)")

	timestamp := time.Now().Format(time.RFC3339Nano)
	time.Sleep(100 * time.Millisecond)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockTryLoadDiffError", `return("renametable")`)
	tk.MustExec("rename table db1.t to db2.t")

	tk.MustQuery("select * from db2.t").Check(testkit.Rows())
	tk.MustExecToErr("select * from db1.t")
	tk.MustExec("use db2")
	tk.MustQuery("show tables").Check(testkit.Rows("t"))
	tk.MustExec("use db1")
	tk.MustQuery("show tables").Check(testkit.Rows())

	// Cover a bug that after full load using infoschema v2, the temporary table is gone.
	// Check global temporary table not dispear after full load.
	require.True(t, dom.InfoSchema().HasTemporaryTable())
	tk.MustExec("begin")
	tk.MustExec("insert into test.tmp values (1)")
	tk.MustExec("commit")

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockTryLoadDiffError", `return("dropdatabase")`)
	tk.MustExec("drop database db1")
	tk.MustExecToErr("use db1")
	tk.MustQuery("select table_schema from information_schema.tables where table_schema = 'db2'").Check(testkit.Rows("db2"))
	tk.MustQuery("select * from information_schema.tables where table_schema = 'db1'").Check(testkit.Rows())

	// Set snapthost and read old schema.
	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot= '%s'", timestamp))
	tk.MustQuery("select * from db1.t").Check(testkit.Rows())
	tk.MustExecToErr("select * from db2.t")
	tk.MustExec("use db2")
	tk.MustQuery("show tables").Check(testkit.Rows())
	tk.MustExec("use db1")
	tk.MustQuery("show tables").Check(testkit.Rows("t"))
}

func TestIssue54926(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_schema_cache_size = 0")
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	time1 := time.Now()
	time1TS := oracle.GoTimeToTS(time1)
	schemaVer1 := tk.Session().GetInfoSchema().SchemaMetaVersion()
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key);")
	tk.MustExec(`drop table if exists t`)
	time.Sleep(50 * time.Millisecond)
	time2 := time.Now()
	time2TS := oracle.GoTimeToTS(time2)
	schemaVer2 := tk.Session().GetInfoSchema().SchemaMetaVersion()

	tk2.MustExec("create table test.t (id int primary key)")
	tk.MustExec("set @@global.tidb_schema_cache_size = 1073741824")
	dom.Reload()

	// test set txn as of will flush/mutex tidb_snapshot
	tk.MustExec(fmt.Sprintf(`set @@tidb_snapshot="%s"`, time1.Format("2006-1-2 15:04:05.000")))
	require.Equal(t, time1TS, tk.Session().GetSessionVars().SnapshotTS)
	require.NotNil(t, tk.Session().GetSessionVars().SnapshotInfoschema)
	require.Equal(t, schemaVer1, tk.Session().GetInfoSchema().SchemaMetaVersion())
	tk.MustExec(fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time2.Format("2006-1-2 15:04:05.000")))
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().SnapshotTS)
	require.NotNil(t, tk.Session().GetSessionVars().SnapshotInfoschema)
	require.Equal(t, time2TS, tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
	require.Equal(t, schemaVer2, tk.Session().GetInfoSchema().SchemaMetaVersion())
}

func TestSchemaSimpleTableInfos(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("set @@global.tidb_schema_cache_size = '512MB'")

	tk.MustExec("create database aaa")
	tk.MustExec("create database zzz")
	tk.MustExec("drop database zzz")

	tk.MustExec("create database simple")
	tk.MustExec("use simple")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create table t2 (id int)")

	time1 := time.Now()
	time.Sleep(50 * time.Millisecond)

	tk.MustExec("rename table simple.t2 to aaa.t2")
	tk.MustExec("drop database aaa")

	is := tk.Session().GetInfoSchema()
	// Cover special schema
	tblInfos, err := is.SchemaSimpleTableInfos(context.Background(), model.NewCIStr("INFORMATION_SCHEMA"))
	require.NoError(t, err)
	res := make([]string, 0, len(tblInfos))
	for _, tbl := range tblInfos {
		res = append(res, tbl.Name.L)
	}
	sort.Strings(res)
	tk.MustQuery("select lower(table_name) from information_schema.tables where table_schema = 'information_schema'").
		Sort().Check(testkit.Rows(res...))

	// Cover normal schema
	tblInfos, err = is.SchemaSimpleTableInfos(context.Background(), model.NewCIStr("simple"))
	require.NoError(t, err)
	require.Len(t, tblInfos, 1)
	require.Equal(t, tblInfos[0].Name.L, "t1")

	// Cover snapshot infoschema
	tk.MustExec(fmt.Sprintf(`set @@tidb_snapshot="%s"`, time1.Format("2006-1-2 15:04:05.000")))
	is = tk.Session().GetInfoSchema()
	tblInfos, err = is.SchemaSimpleTableInfos(context.Background(), model.NewCIStr("simple"))
	require.NoError(t, err)
	require.Len(t, tblInfos, 2)
	require.Equal(t, tblInfos[0].Name.L, "t2")
	require.Equal(t, tblInfos[1].Name.L, "t1")
}

func TestSnapshotInfoschemaReader(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	tk.MustExec("create database issue55827")
	tk.MustExec("use issue55827")

	time1 := time.Now()
	timeStr := time1.Format("2006-1-2 15:04:05.000")

	tk.MustExec("create table t (id int primary key);")
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.TABLES where table_schema = 'issue55827'").Check(testkit.Rows("1"))
	tk.MustQuery("select count(tidb_table_id) from INFORMATION_SCHEMA.TABLES where table_schema = 'issue55827'").Check(testkit.Rows("1"))

	// For issue 55827
	sql := fmt.Sprintf("select count(*) from INFORMATION_SCHEMA.TABLES as of timestamp '%s' where table_schema = 'issue55827'", timeStr)
	tk.MustQuery(sql).Check(testkit.Rows("0"))
	sql = fmt.Sprintf("select * from INFORMATION_SCHEMA.TABLES as of timestamp '%s' where table_schema = 'issue55827'", timeStr)
	tk.MustQuery(sql).Check(testkit.Rows())
}
