package ddl_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCreateTableGroupMeta(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)

	tk.MustExec("use test")
	tk.MustExec("create database if not exists test2")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("create tablegroup tg2 tables t1, t2")
	tgInfo, ok := domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg2"))
	require.True(t, ok)
	require.True(t, tgInfo.ID > 0)
	require.NotNil(t, tgInfo)
	require.Equal(t, "tg2", tgInfo.Name.String())
	require.Equal(t, 2, len(tgInfo.Tables))
	require.Equal(t, model.TableName{DB: "test", Table: "t1"}, tgInfo.Tables[0])
	require.Equal(t, model.TableName{DB: "test", Table: "t2"}, tgInfo.Tables[1])
	require.Equal(t, 1, len(tgInfo.AffinityGroups))
	require.Equal(t, 2, len(tgInfo.AffinityGroups[0].Tables))
	require.Equal(t, model.TableName{DB: "test", Table: "t1"}, tgInfo.AffinityGroups[0].Tables[0].TableName)
	require.Equal(t, model.TableName{DB: "test", Table: "t2"}, tgInfo.AffinityGroups[0].Tables[1].TableName)
	require.True(t, tgInfo.AffinityGroups[0].Tables[0].PhysicalTableID > 0)
	require.True(t, tgInfo.AffinityGroups[0].Tables[1].PhysicalTableID > 0)

	// Test for show create tablegroup
	err := tk.QueryToErr("show create tablegroup tg0")
	require.NotNil(t, err)
	require.Equal(t, "[schema:8281]Unknown tablegroup 'tg0'", err.Error())
	tk.MustQuery("show create tablegroup tg2").Check(testkit.RowsWithSep("|", "tg2|CREATE TABLEGROUP `tg2` TABLES `test`.`t1`, `test`.`t2`"))

	// Test for alter tablegroup
	tk.MustExec("alter tablegroup tg2 drop tables t1")
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg2"))
	require.True(t, ok)
	require.True(t, tgInfo.ID > 0)
	require.NotNil(t, tgInfo)
	require.Equal(t, "tg2", tgInfo.Name.String())
	require.Equal(t, 1, len(tgInfo.Tables))
	require.Equal(t, model.TableName{DB: "test", Table: "t2"}, tgInfo.Tables[0])
	require.Equal(t, 1, len(tgInfo.AffinityGroups))
	require.Equal(t, 1, len(tgInfo.AffinityGroups[0].Tables))
	require.Equal(t, model.TableName{DB: "test", Table: "t2"}, tgInfo.Tables[0])
	require.Equal(t, 1, len(tgInfo.AffinityGroups))
	require.Equal(t, 1, len(tgInfo.AffinityGroups[0].Tables))
	require.Equal(t, model.TableName{DB: "test", Table: "t2"}, tgInfo.AffinityGroups[0].Tables[0].TableName)
	require.True(t, tgInfo.AffinityGroups[0].Tables[0].PhysicalTableID > 0)

	tk.MustQuery("show create tablegroup tg2").Check(testkit.RowsWithSep("|", "tg2|CREATE TABLEGROUP `tg2` TABLES `test`.`t2`"))
	tk.MustGetErrMsg("alter tablegroup tg2 drop tables t1", "[schema:1146]Table 'test.t1' doesn't exist")
	tk.MustGetErrMsg("alter tablegroup tg2 drop tables t2", "drop all tables from tablegroup is not allowed, try drop tablegroup instead")
	tk.MustGetErrMsg("alter tablegroup tg2 add tables t3", "[schema:1146]Table 'test.t3' doesn't exist")
	tk.MustGetErrMsg("alter tablegroup tg2 add tables t2", "[schema:1050]Table 'test.t2' already exists")
	tk.MustExec("create table test2.t4 (a int)")
	tk.MustExec("create table t5 (a int)")
	tk.MustExec("alter tablegroup tg2 add tables test2.t4, t5")
	tk.MustQuery("show create tablegroup tg2").Check(testkit.RowsWithSep("|", "tg2|CREATE TABLEGROUP `tg2` TABLES `test`.`t2`, `test2`.`t4`, `test`.`t5`"))

	tk.MustExec("create table t3 (a int)")
	tk.MustExec("create tablegroup tg tables t3")
	tk.MustQuery("show create tablegroup tg").Check(testkit.RowsWithSep("|", "tg|CREATE TABLEGROUP `tg` TABLES `test`.`t3`"))
	tk.MustQuery("show tablegroups").Check(testkit.Rows("tg", "tg2"))
	//
	tk.MustQuery("select * from information_schema.tablegroups").Check(testkit.Rows(
		"test t3 <nil> tg _tidb_tg_tg",
		"test t2 <nil> tg2 _tidb_tg_tg2",
		"test2 t4 <nil> tg2 _tidb_tg_tg2",
		"test t5 <nil> tg2 _tidb_tg_tg2"))
	tk.MustQuery("select * from information_schema.tablegroup_status").Check(testkit.Rows(
		"_tidb_tg_tg tg <nil> <nil> <nil> <nil> <nil> <nil> <nil>",
		"_tidb_tg_tg2 tg2 <nil> <nil> <nil> <nil> <nil> <nil> <nil>"))

	// test for truncate table
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg"))
	require.True(t, ok)
	require.True(t, tgInfo.ID > 0)
	require.NotNil(t, tgInfo)
	oldAffinityGroups := tgInfo.AffinityGroups
	tk.MustExec("truncate table t3")
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg"))
	require.True(t, ok)
	require.True(t, tgInfo.ID > 0)
	require.NotNil(t, tgInfo)
	newAffinityGroups := tgInfo.AffinityGroups
	require.NotEqual(t, oldAffinityGroups, newAffinityGroups)
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByTableName(model.TableName{DB: "test", Table: "t3"})
	require.True(t, ok)
	require.True(t, tgInfo.ID > 0)
	require.NotNil(t, tgInfo)

	tk.MustExec("drop table t3")
	tk.MustQuery("show create tablegroup tg").Check(testkit.RowsWithSep("|", "tg|CREATE TABLEGROUP `tg`")) // should no table exists.
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg"))
	require.True(t, ok)
	require.True(t, tgInfo.ID > 0)
	require.NotNil(t, tgInfo)
	require.Equal(t, "tg", tgInfo.Name.String())
	require.Equal(t, 0, len(tgInfo.Tables))
	require.Equal(t, 1, len(tgInfo.AffinityGroups))
	require.Equal(t, 0, len(tgInfo.AffinityGroups[0].Tables))

	tk.MustExec("drop tablegroup tg")
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg"))
	require.False(t, ok)
	require.Nil(t, tgInfo)

	tk.MustExec("drop tablegroup tg2")
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg2"))
	require.False(t, ok)
	require.Nil(t, tgInfo)

	// Test for partition tablegroup
	tk.MustExec("drop table if exists t1,t2,t3")
	tk.MustExec("drop tablegroup if exists tg")
	tk.MustExec("create table t1 (a int key) partition by hash(a) partitions 5")
	tk.MustExec("create table t2 (a int key) partition by hash(a) partitions 5")
	tk.MustExec("create table t3 (a int key) partition by hash(a) partitions 5")
	tk.MustExec("create tablegroup tg tables t1, t2, t3")
	tk.MustQuery("show create tablegroup tg").Check(testkit.RowsWithSep("|", "tg|CREATE TABLEGROUP `tg` TABLES `test`.`t1`, `test`.`t2`, `test`.`t3`"))
	tk.MustQuery("select * from information_schema.tablegroups where tablegroup='tg'").Check(testkit.Rows(
		"test t1 p0 tg _tidb_ptg_tg_p0",
		"test t2 p0 tg _tidb_ptg_tg_p0",
		"test t3 p0 tg _tidb_ptg_tg_p0",
		"test t1 p1 tg _tidb_ptg_tg_p1",
		"test t2 p1 tg _tidb_ptg_tg_p1",
		"test t3 p1 tg _tidb_ptg_tg_p1",
		"test t1 p2 tg _tidb_ptg_tg_p2",
		"test t2 p2 tg _tidb_ptg_tg_p2",
		"test t3 p2 tg _tidb_ptg_tg_p2",
		"test t1 p3 tg _tidb_ptg_tg_p3",
		"test t2 p3 tg _tidb_ptg_tg_p3",
		"test t3 p3 tg _tidb_ptg_tg_p3",
		"test t1 p4 tg _tidb_ptg_tg_p4",
		"test t2 p4 tg _tidb_ptg_tg_p4",
		"test t3 p4 tg _tidb_ptg_tg_p4",
	))
	tk.MustQuery("select * from information_schema.tablegroup_status where tablegroup='tg'").Check(testkit.Rows(
		"_tidb_ptg_tg_p0 tg <nil> <nil> <nil> <nil> <nil> <nil> <nil>",
		"_tidb_ptg_tg_p1 tg <nil> <nil> <nil> <nil> <nil> <nil> <nil>",
		"_tidb_ptg_tg_p2 tg <nil> <nil> <nil> <nil> <nil> <nil> <nil>",
		"_tidb_ptg_tg_p3 tg <nil> <nil> <nil> <nil> <nil> <nil> <nil>",
		"_tidb_ptg_tg_p4 tg <nil> <nil> <nil> <nil> <nil> <nil> <nil>",
	))
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg"))
	require.True(t, ok)
	require.True(t, tgInfo.ID > 0)
	require.NotNil(t, tgInfo)
	require.Equal(t, "tg", tgInfo.Name.String())
	require.Equal(t, 3, len(tgInfo.Tables))
	require.Equal(t, model.TableName{DB: "test", Table: "t1"}, tgInfo.Tables[0])
	require.Equal(t, model.TableName{DB: "test", Table: "t2"}, tgInfo.Tables[1])
	require.Equal(t, model.TableName{DB: "test", Table: "t3"}, tgInfo.Tables[2])
	require.Equal(t, 5, len(tgInfo.AffinityGroups))
	for i := 0; i < 5; i++ {
		require.Equal(t, fmt.Sprintf("_tidb_ptg_%v_p%v", tgInfo.Name.L, i), tgInfo.AffinityGroups[i].Name)
		require.Equal(t, 3, len(tgInfo.AffinityGroups[i].Tables))
		require.Equal(t, model.TableName{DB: "test", Table: "t1"}, tgInfo.AffinityGroups[i].Tables[0].TableName)
		require.Equal(t, model.TableName{DB: "test", Table: "t2"}, tgInfo.AffinityGroups[i].Tables[1].TableName)
		require.Equal(t, model.TableName{DB: "test", Table: "t3"}, tgInfo.AffinityGroups[i].Tables[2].TableName)
		partition := fmt.Sprintf("p%v", i)
		require.Equal(t, partition, tgInfo.AffinityGroups[i].Tables[0].PartitionName)
		require.Equal(t, partition, tgInfo.AffinityGroups[i].Tables[1].PartitionName)
		require.Equal(t, partition, tgInfo.AffinityGroups[i].Tables[2].PartitionName)
	}

	tk.MustExec("alter tablegroup tg drop tables t2")
	tk.MustQuery("show create tablegroup tg").Check(testkit.RowsWithSep("|", "tg|CREATE TABLEGROUP `tg` TABLES `test`.`t1`, `test`.`t3`"))
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg"))
	require.True(t, ok)
	require.True(t, tgInfo.ID > 0)
	require.NotNil(t, tgInfo)
	require.Equal(t, 2, len(tgInfo.Tables))
	require.Equal(t, model.TableName{DB: "test", Table: "t1"}, tgInfo.Tables[0])
	require.Equal(t, model.TableName{DB: "test", Table: "t3"}, tgInfo.Tables[1])
	require.Equal(t, 5, len(tgInfo.AffinityGroups))
	for i := 0; i < 5; i++ {
		require.Equal(t, fmt.Sprintf("_tidb_ptg_%v_p%v", tgInfo.Name.L, i), tgInfo.AffinityGroups[i].Name)
		require.Equal(t, 2, len(tgInfo.AffinityGroups[i].Tables))
		require.Equal(t, model.TableName{DB: "test", Table: "t1"}, tgInfo.AffinityGroups[i].Tables[0].TableName)
		require.Equal(t, model.TableName{DB: "test", Table: "t3"}, tgInfo.AffinityGroups[i].Tables[1].TableName)
		partition := fmt.Sprintf("p%v", i)
		require.Equal(t, partition, tgInfo.AffinityGroups[i].Tables[0].PartitionName)
		require.Equal(t, partition, tgInfo.AffinityGroups[i].Tables[1].PartitionName)
	}
	tk.MustQuery("select * from information_schema.tablegroups where tablegroup='tg'").Check(testkit.Rows(
		"test t1 p0 tg _tidb_ptg_tg_p0",
		"test t3 p0 tg _tidb_ptg_tg_p0",
		"test t1 p1 tg _tidb_ptg_tg_p1",
		"test t3 p1 tg _tidb_ptg_tg_p1",
		"test t1 p2 tg _tidb_ptg_tg_p2",
		"test t3 p2 tg _tidb_ptg_tg_p2",
		"test t1 p3 tg _tidb_ptg_tg_p3",
		"test t3 p3 tg _tidb_ptg_tg_p3",
		"test t1 p4 tg _tidb_ptg_tg_p4",
		"test t3 p4 tg _tidb_ptg_tg_p4",
	))
	tk.MustExec("alter tablegroup tg add tables t2")
	tk.MustQuery("select * from information_schema.tablegroups where tablegroup='tg'").Check(testkit.Rows(
		"test t1 p0 tg _tidb_ptg_tg_p0",
		"test t3 p0 tg _tidb_ptg_tg_p0",
		"test t2 p0 tg _tidb_ptg_tg_p0",
		"test t1 p1 tg _tidb_ptg_tg_p1",
		"test t3 p1 tg _tidb_ptg_tg_p1",
		"test t2 p1 tg _tidb_ptg_tg_p1",
		"test t1 p2 tg _tidb_ptg_tg_p2",
		"test t3 p2 tg _tidb_ptg_tg_p2",
		"test t2 p2 tg _tidb_ptg_tg_p2",
		"test t1 p3 tg _tidb_ptg_tg_p3",
		"test t3 p3 tg _tidb_ptg_tg_p3",
		"test t2 p3 tg _tidb_ptg_tg_p3",
		"test t1 p4 tg _tidb_ptg_tg_p4",
		"test t3 p4 tg _tidb_ptg_tg_p4",
		"test t2 p4 tg _tidb_ptg_tg_p4",
	))

	// test for truncate table
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg"))
	require.True(t, ok)
	require.True(t, tgInfo.ID > 0)
	require.NotNil(t, tgInfo)
	oldAffinityGroups = tgInfo.AffinityGroups
	require.Equal(t, 5, len(oldAffinityGroups))
	tk.MustExec("truncate table t3")
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg"))
	require.True(t, ok)
	require.True(t, tgInfo.ID > 0)
	require.NotNil(t, tgInfo)
	newAffinityGroups = tgInfo.AffinityGroups
	require.Equal(t, 5, len(newAffinityGroups))
	require.NotEqual(t, oldAffinityGroups, newAffinityGroups)
	for i := 0; i < len(oldAffinityGroups); i++ {
		require.NotEqual(t, oldAffinityGroups[i], newAffinityGroups[i])
	}

	// test for truncate partition
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg"))
	require.True(t, ok)
	require.True(t, tgInfo.ID > 0)
	require.NotNil(t, tgInfo)
	oldAffinityGroups = tgInfo.AffinityGroups
	require.Equal(t, 5, len(oldAffinityGroups))
	tk.MustExec(`alter table t3 truncate partition p1, p4`)
	tgInfo, ok = domain.GetDomain(tk.Session()).InfoSchema().TableGroupByName(pmodel.NewCIStr("tg"))
	require.True(t, ok)
	require.True(t, tgInfo.ID > 0)
	require.NotNil(t, tgInfo)
	newAffinityGroups = tgInfo.AffinityGroups
	require.Equal(t, 5, len(newAffinityGroups))
	require.NotEqual(t, oldAffinityGroups, newAffinityGroups)
	for i := 0; i < len(oldAffinityGroups); i++ {
		if i == 1 || i == 4 {
			require.NotEqual(t, oldAffinityGroups[i], newAffinityGroups[i])
		} else {
			require.Equal(t, oldAffinityGroups[i], newAffinityGroups[i])
		}
	}

	tk.MustExec("drop database test")
	tk.MustQuery("select * from information_schema.tablegroups where tablegroup='tg'").Check(testkit.Rows(
		"<nil> <nil> <nil> tg _tidb_ptg_tg_p0",
		"<nil> <nil> <nil> tg _tidb_ptg_tg_p1",
		"<nil> <nil> <nil> tg _tidb_ptg_tg_p2",
		"<nil> <nil> <nil> tg _tidb_ptg_tg_p3",
		"<nil> <nil> <nil> tg _tidb_ptg_tg_p4",
	))
}

func TestCreateTableGroupError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	testcases := []struct {
		sql string
		err string
	}{
		{"use test", ""},
		{"create table t1 (a int)", ""},
		{"create tablegroup tg tables t1, t1", "[schema:8284]Duplicate table name 'test.t1'"},
		{"create tablegroup tg tables t1, test2.t2", "[schema:1146]Table 'test2.t2' doesn't exist"},
		{"create table t2 (a int)", ""},
		{"create tablegroup tg tables t1, t2", ""},
		{"create tablegroup tg tables t1", "[schema:8280]Tablegroup 'tg' already exists"},
		{"create tablegroup tg1 tables t2", "[schema:8282]Tablegroup not supported, reason: table t2 aleady in tablegroup tg"},
		{"create table t3 (a int key) partition by hash(a) partitions 3", ""},
		{"create table t4 (a int key) partition by hash(a) partitions 2", ""},
		{"create tablegroup tg3 tables t3, t4", "[schema:8283]Tablegroup partitions count not match"},
		{"create tablegroup tg3 tables t3", ""},
		{"alter tablegroup tg3 add tables t4", "[schema:8283]Tablegroup partitions count not match"},
		{"alter tablegroup tg3 add tables t3", "[schema:1050]Table 'test.t3' already exists"},
		{"create table t5 (a int key) partition by hash(a) partitions 3", ""},
		{"alter tablegroup tg3 add tables t5, t5", "[schema:8284]Duplicate table name 'test.t5'"},
	}
	for _, ca := range testcases {
		if ca.err == "" {
			tk.MustExec(ca.sql)
		} else {
			err := tk.ExecToErr(ca.sql)
			require.EqualError(t, err, ca.err, ca.sql)
		}
	}
}
