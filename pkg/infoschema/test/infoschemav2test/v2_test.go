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
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestSpecialSchemas(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")

	tk.MustExec("set @@global.tidb_schema_cache_size = 1024;")
	tk.MustQuery("select @@global.tidb_schema_cache_size;").Check(testkit.Rows("1024"))
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
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("pt"))
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
	ntbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("nt"))
	require.NoError(t, err)

	tk.MustExec("alter table pt exchange partition p3 with table nt")
	is = dom.InfoSchema()
	ptbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("pt"))
	require.NoError(t, err)
	pi := ptbl.Meta().GetPartitionInfo()
	pid = pi.GetPartitionIDByName("p3")
	require.Equal(t, pid, ntbl.Meta().ID)
}

func TestListTablesWithSpecialAttribute(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount")
		require.NoError(t, err)
	}()
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
			rows = append(rows, v.DBName+" "+tblInfo.Name.L)
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
		tk.MustQuery("select @@global.tidb_schema_cache_size").CheckContain(strconv.FormatInt(val, 10))

		// On start, the capacity might not be set correctly because infoschema have not load global variable yet.
		// cap := raw.Data.CacheCapacity()
		// require.Equal(t, cap, uint64(val))
	}

	tk.MustExec("set @@global.tidb_schema_cache_size = 32 * 1024 * 1024")
	tk.MustQuery("select @@global.tidb_schema_cache_size").CheckContain("33554432")
	require.Equal(t, variable.SchemaCacheSize.Load(), int64(33554432))
	tk.MustExec("create table trigger_reload (id int)") // need to trigger infoschema rebuild to reset capacity
	is = dom.InfoSchema()
	ok, raw = infoschema.IsV2(is)
	require.True(t, ok)
	require.Equal(t, raw.Data.CacheCapacity(), uint64(33554432))
}
