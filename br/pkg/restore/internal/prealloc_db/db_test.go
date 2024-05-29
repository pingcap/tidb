// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package preallocdb_test

import (
	"context"
	"math"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	preallocdb "github.com/pingcap/tidb/br/pkg/restore/internal/prealloc_db"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestoreAutoIncID(t *testing.T) {
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)
	tk.MustExec("use test")
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("drop table if exists `\"t\"`;")
	// Test SQL Mode
	tk.MustExec("create table `\"t\"` (" +
		"a int not null," +
		"time timestamp not null default '0000-00-00 00:00:00');",
	)
	tk.MustExec("insert into `\"t\"` values (10, '0000-00-00 00:00:00');")
	// Query the current AutoIncID
	autoIncID, err := strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	require.NoErrorf(t, err, "Error query auto inc id: %s", err)
	// Get schemas of db and table
	info, err := s.Mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoErrorf(t, err, "Error get snapshot info schema: %s", err)
	dbInfo, exists := info.SchemaByName(model.NewCIStr("test"))
	require.Truef(t, exists, "Error get db info")
	tableInfo, err := info.TableByName(model.NewCIStr("test"), model.NewCIStr("\"t\""))
	require.NoErrorf(t, err, "Error get table info: %s", err)
	table := metautil.Table{
		Info: tableInfo.Meta(),
		DB:   dbInfo,
	}
	// Get the next AutoIncID
	idAlloc := autoid.NewAllocator(s.Mock.Domain, dbInfo.ID, table.Info.ID, false, autoid.RowIDAllocType)
	globalAutoID, err := idAlloc.NextGlobalAutoID()
	require.NoErrorf(t, err, "Error allocate next auto id")
	require.Equal(t, uint64(globalAutoID), autoIncID)
	// Alter AutoIncID to the next AutoIncID + 100
	table.Info.AutoIncID = globalAutoID + 100
	db, _, err := preallocdb.NewDB(gluetidb.New(), s.Mock.Storage, "STRICT")
	require.NoErrorf(t, err, "Error create DB")
	tk.MustExec("drop database if exists test;")
	// Test empty collate value
	table.DB.Charset = "utf8mb4"
	table.DB.Collate = ""
	err = db.CreateDatabase(context.Background(), table.DB, false, nil)
	require.NoErrorf(t, err, "Error create empty collate db: %s %s", err, s.Mock.DSN)
	tk.MustExec("drop database if exists test;")
	// Test empty charset value
	table.DB.Charset = ""
	table.DB.Collate = "utf8mb4_bin"
	err = db.CreateDatabase(context.Background(), table.DB, false, nil)
	require.NoErrorf(t, err, "Error create empty charset db: %s %s", err, s.Mock.DSN)
	uniqueMap := make(map[restore.UniqueTableName]bool)
	err = db.CreateTable(context.Background(), &table, uniqueMap, false, nil)
	require.NoErrorf(t, err, "Error create table: %s %s", err, s.Mock.DSN)

	tk.MustExec("use test")
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	require.NoErrorf(t, err, "Error query auto inc id: %s", err)
	// Check if AutoIncID is altered successfully.
	require.Equal(t, uint64(globalAutoID+100), autoIncID)

	// try again, failed due to table exists.
	table.Info.AutoIncID = globalAutoID + 200
	err = db.CreateTable(context.Background(), &table, uniqueMap, false, nil)
	require.NoError(t, err)
	// Check if AutoIncID is not altered.
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	require.NoErrorf(t, err, "Error query auto inc id: %s", err)
	require.Equal(t, uint64(globalAutoID+100), autoIncID)

	// try again, success because we use alter sql in unique map.
	table.Info.AutoIncID = globalAutoID + 300
	uniqueMap[restore.UniqueTableName{DB: "test", Table: "\"t\""}] = true
	err = db.CreateTable(context.Background(), &table, uniqueMap, false, nil)
	require.NoError(t, err)
	// Check if AutoIncID is altered to globalAutoID + 300.
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	require.NoErrorf(t, err, "Error query auto inc id: %s", err)
	require.Equal(t, uint64(globalAutoID+300), autoIncID)
}

func TestCreateTablesInDb(t *testing.T) {
	s := utiltest.CreateRestoreSchemaSuite(t)
	info, err := s.Mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoErrorf(t, err, "Error get snapshot info schema: %s", err)

	dbSchema, isExist := info.SchemaByName(model.NewCIStr("test"))
	require.True(t, isExist)

	tables := make([]*metautil.Table, 4)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.SetCharset("binary")
	ddlJobMap := make(map[restore.UniqueTableName]bool)
	for i := len(tables) - 1; i >= 0; i-- {
		tables[i] = &metautil.Table{
			DB: dbSchema,
			Info: &model.TableInfo{
				ID:   int64(i),
				Name: model.NewCIStr("test" + strconv.Itoa(i)),
				Columns: []*model.ColumnInfo{{
					ID:        1,
					Name:      model.NewCIStr("id"),
					FieldType: *intField,
					State:     model.StatePublic,
				}},
				Charset: "utf8mb4",
				Collate: "utf8mb4_bin",
			},
		}
		ddlJobMap[restore.UniqueTableName{DB: dbSchema.Name.String(), Table: tables[i].Info.Name.String()}] = false
	}
	db, _, err := preallocdb.NewDB(gluetidb.New(), s.Mock.Storage, "STRICT")
	require.NoError(t, err)

	err = db.CreateTables(context.Background(), tables, ddlJobMap, false, nil)
	require.NoError(t, err)
}

func TestDB_ExecDDL(t *testing.T) {
	s := utiltest.CreateRestoreSchemaSuite(t)

	ctx := context.Background()
	ddlJobs := []*model.Job{
		{
			Type:       model.ActionAddIndex,
			Query:      "CREATE DATABASE IF NOT EXISTS test_db;",
			BinlogInfo: &model.HistoryInfo{},
		},
		{
			Type:       model.ActionAddIndex,
			Query:      "",
			BinlogInfo: &model.HistoryInfo{},
		},
	}

	db, _, err := preallocdb.NewDB(gluetidb.New(), s.Mock.Storage, "STRICT")
	require.NoError(t, err)

	for _, ddlJob := range ddlJobs {
		err = db.ExecDDL(ctx, ddlJob)
		assert.NoError(t, err)
	}
}

func TestCreateTableConsistent(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)
	tk.MustExec("use test")
	tk.MustExec("set @@sql_mode=''")

	db, supportPolicy, err := preallocdb.NewDB(gluetidb.New(), s.Mock.Storage, "STRICT")
	require.NoError(t, err)
	require.True(t, supportPolicy)
	defer db.Close()

	getTableInfo := func(name string) (*model.DBInfo, *model.TableInfo) {
		info, err := s.Mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
		require.NoError(t, err)
		dbInfo, exists := info.SchemaByName(model.NewCIStr("test"))
		require.True(t, exists)
		tableInfo, err := info.TableByName(model.NewCIStr("test"), model.NewCIStr(name))
		require.NoError(t, err)
		return dbInfo, tableInfo.Meta()
	}
	tk.MustExec("create sequence test.s increment by 1 minvalue = 10;")
	dbInfo, seqInfo := getTableInfo("s")
	tk.MustExec("drop sequence test.s;")

	newSeqInfo := seqInfo.Clone()
	newSeqInfo.ID += 100
	newTables := []*metautil.Table{
		{
			DB:   dbInfo.Clone(),
			Info: newSeqInfo,
		},
	}
	err = db.CreateTables(ctx, newTables, nil, false, nil)
	require.NoError(t, err)
	r11 := tk.MustQuery("select nextval(s)").Rows()
	r12 := tk.MustQuery("show create table test.s").Rows()

	tk.MustExec("drop sequence test.s;")

	newSeqInfo = seqInfo.Clone()
	newSeqInfo.ID += 100
	newTable := &metautil.Table{DB: dbInfo.Clone(), Info: newSeqInfo}
	err = db.CreateTable(ctx, newTable, nil, false, nil)
	require.NoError(t, err)
	r21 := tk.MustQuery("select nextval(s)").Rows()
	r22 := tk.MustQuery("show create table test.s").Rows()

	require.Equal(t, r11, r21)
	require.Equal(t, r12, r22)

	tk.MustExec("drop sequence test.s;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("create view v as select * from t;")

	_, tblInfo := getTableInfo("t")
	_, viewInfo := getTableInfo("v")
	tk.MustExec("drop table t;")
	tk.MustExec("drop view v;")

	newTblInfo := tblInfo.Clone()
	newTblInfo.ID += 100
	newViewInfo := viewInfo.Clone()
	newViewInfo.ID += 100
	newTables = []*metautil.Table{
		{
			DB:   dbInfo.Clone(),
			Info: newTblInfo,
		},
		{
			DB:   dbInfo.Clone(),
			Info: newViewInfo,
		},
	}
	err = db.CreateTables(ctx, newTables, nil, false, nil)
	require.NoError(t, err)
	r11 = tk.MustQuery("show create table t;").Rows()
	r12 = tk.MustQuery("show create view v;").Rows()

	tk.MustExec("drop table t;")
	tk.MustExec("drop view v;")

	newTblInfo = tblInfo.Clone()
	newTblInfo.ID += 200
	newTable = &metautil.Table{DB: dbInfo.Clone(), Info: newTblInfo}
	err = db.CreateTable(ctx, newTable, nil, false, nil)
	require.NoError(t, err)
	newViewInfo = viewInfo.Clone()
	newViewInfo.ID += 200
	newTable = &metautil.Table{DB: dbInfo.Clone(), Info: newViewInfo}
	err = db.CreateTable(ctx, newTable, nil, false, nil)
	require.NoError(t, err)

	r21 = tk.MustQuery("show create table t;").Rows()
	r22 = tk.MustQuery("show create view v;").Rows()

	require.Equal(t, r11, r21)
	require.Equal(t, r12, r22)
}
