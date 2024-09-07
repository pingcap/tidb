// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package preallocdb_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"

	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	preallocdb "github.com/pingcap/tidb/br/pkg/restore/internal/prealloc_db"
	prealloctableid "github.com/pingcap/tidb/br/pkg/restore/internal/prealloc_table_id"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
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
	dbInfo, exists := info.SchemaByName(pmodel.NewCIStr("test"))
	require.Truef(t, exists, "Error get db info")
	tableInfo, err := info.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("\"t\""))
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

var createTableSQLs []string = []string{
	"create table `test`.`t1` (id int);",
	"create table `test`.`t2` (id int, created_at TIMESTAMP) TTL = `created_at` + INTERVAL 3 MONTH;",
	"create sequence `test`.`t3` start 3 increment 2 minvalue 2 maxvalue 10 cache 3;",
	"create sequence `test`.`t4` start 3 increment 2 minvalue 2 maxvalue 10 cache 3 cycle;",
	"create view `test`.`t5` as select * from `test`.`t1`;",
	"create table `test`.`t6` (id int, store_id INT NOT NULL) PARTITION BY RANGE (store_id) (" +
		"PARTITION p0 VALUES LESS THAN (6)," +
		"PARTITION p1 VALUES LESS THAN (11)," +
		"PARTITION p2 VALUES LESS THAN MAXVALUE);",
	"create sequence `test`.`t7` start 3 increment -2 minvalue 2 maxvalue 10 cache 3 cycle;",
}

var insertTableSQLs []string = []string{
	//"SELECT NEXTVAL(`test`.`t3`);",
}

var checkTableSQLs = []func(t *testing.T, tk *testkit.TestKit, prefix string) error{
	func(t *testing.T, tk *testkit.TestKit, prefix string) error { // test.t1
		sql := fmt.Sprintf("show create table `test`.`%s1`;", prefix)
		tk.MustQuery(sql)
		return nil
	},
	func(t *testing.T, tk *testkit.TestKit, prefix string) error { // test.t2
		sql := fmt.Sprintf("show create table `test`.`%s2`;", prefix)
		rows := tk.MustQuery(sql).Rows()
		require.Contains(t, rows[0][1], "TTL_ENABLE='OFF'")
		return nil
	},
	func(t *testing.T, tk *testkit.TestKit, prefix string) error { // test.t3
		sql := fmt.Sprintf("SELECT NEXTVAL(`test`.`%s3`);", prefix)
		rows := tk.MustQuery(sql).Rows()
		require.Contains(t, rows[0][0], "3")
		return nil
	},
	func(t *testing.T, tk *testkit.TestKit, prefix string) error { // test.t4
		sql := fmt.Sprintf("SELECT NEXTVAL(`test`.`%s4`);", prefix)
		rows := tk.MustQuery(sql).Rows()
		require.Contains(t, rows[0][0], "4")
		return nil
	},
	func(t *testing.T, tk *testkit.TestKit, prefix string) error { // test.t5
		sql := fmt.Sprintf("show create table `test`.`%s5`;", prefix)
		tk.MustQuery(sql)
		return nil
	},
	func(t *testing.T, tk *testkit.TestKit, prefix string) error { // test.t6
		sql := fmt.Sprintf("show create table `test`.`%s6`;", prefix)
		tk.MustQuery(sql)
		return nil
	},
	func(t *testing.T, tk *testkit.TestKit, prefix string) error { // test.t7
		sql := fmt.Sprintf("SELECT NEXTVAL(`test`.`%s7`);", prefix)
		rows := tk.MustQuery(sql).Rows()
		require.Contains(t, rows[0][0], "0")
		return nil
	},
}

func prepareAllocTables(
	ctx context.Context,
	t *testing.T,
	db *preallocdb.DB,
	dom *domain.Domain,
) (tableInfos []*metautil.Table) {
	for _, sql := range createTableSQLs {
		err := db.Session().Execute(ctx, sql)
		require.NoError(t, err)
	}
	for _, sql := range insertTableSQLs {
		err := db.Session().Execute(ctx, sql)
		require.NoError(t, err)
	}

	info, err := dom.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbInfo, exists := info.SchemaByName(pmodel.NewCIStr("test"))
	require.True(t, exists)
	tableInfos = make([]*metautil.Table, 0, 4)
	for i := 1; i <= len(createTableSQLs); i += 1 {
		tableName := fmt.Sprintf("t%d", i)
		tableInfo, err := info.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr(tableName))
		require.NoError(t, err)
		tableInfos = append(tableInfos, &metautil.Table{
			DB:   dbInfo.Clone(),
			Info: tableInfo.Meta().Clone(),
		})
	}

	return tableInfos
}

func cloneTableInfos(
	ctx context.Context,
	t *testing.T,
	db *preallocdb.DB,
	dom *domain.Domain,
	prefix string,
	originTableInfos []*metautil.Table,
) (tableInfos []*metautil.Table) {
	// register preallocated ids
	var ids *prealloctableid.PreallocIDs
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	tableInfos = make([]*metautil.Table, 0, len(originTableInfos))
	err := kv.RunInNewTxn(ctx, dom.Store(), true, func(_ context.Context, txn kv.Transaction) error {
		allocater := meta.NewMeta(txn)
		id, e := allocater.GetGlobalID()
		if e != nil {
			return e
		}

		// resign the table id
		for i := int64(0); i < int64(len(createTableSQLs)); i += 1 {
			newTableInfo := originTableInfos[i].Info.Clone()
			newTableInfo.ID = id + i + 1
			newTableInfo.Name = pmodel.NewCIStr(fmt.Sprintf("%s%d", prefix, i+1))
			tableInfos = append(tableInfos, &metautil.Table{
				DB:   originTableInfos[i].DB.Clone(),
				Info: newTableInfo,
			})
		}

		ids = prealloctableid.New(tableInfos)
		return ids.Alloc(allocater)
	})
	require.NoError(t, err)
	db.RegisterPreallocatedIDs(ids)
	return tableInfos
}

func fakePolicyInfo(ident byte) *model.PolicyInfo {
	id := int64(ident)
	uid := uint64(ident)
	str := string(ident)
	cistr := pmodel.NewCIStr(str)
	return &model.PolicyInfo{
		PlacementSettings: &model.PlacementSettings{
			Followers: uid,
		},
		ID:    id,
		Name:  cistr,
		State: model.StatePublic,
	}
}

func TestPolicyMode(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)
	tk.MustExec("use test")
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("drop table if exists `t`;")
	// Test SQL Mode
	db, supportPolicy, err := preallocdb.NewDB(gluetidb.New(), s.Mock.Storage, "STRICT")
	require.NoError(t, err)
	require.True(t, supportPolicy)
	defer db.Close()

	// Prepare the tables
	oriTableInfos := prepareAllocTables(ctx, t, db, s.Mock.Domain)
	tableInfos := cloneTableInfos(ctx, t, db, s.Mock.Domain, "tt", oriTableInfos)

	// Prepare policy map
	policyMap := &sync.Map{}
	fakepolicy1 := fakePolicyInfo(1)
	fakepolicy2 := fakePolicyInfo(2)
	policyMap.Store(fakepolicy1.Name.L, fakepolicy1)
	policyMap.Store(fakepolicy2.Name.L, fakepolicy2)

	tableInfos[0].Info.PlacementPolicyRef = &model.PolicyRefInfo{
		ID:   fakepolicy1.ID,
		Name: fakepolicy1.Name,
	}
	tableInfos[5].Info.Partition.Definitions[0].PlacementPolicyRef = &model.PolicyRefInfo{
		ID:   fakepolicy2.ID,
		Name: fakepolicy2.Name,
	}
	err = db.CreateTables(ctx, tableInfos, nil, true, policyMap)
	require.NoError(t, err)
	for _, checkFn := range checkTableSQLs {
		checkFn(t, tk, "tt")
	}

	// clone again to test db.CreateTable
	tableInfos = cloneTableInfos(ctx, t, db, s.Mock.Domain, "ttt", oriTableInfos)

	// Prepare policy map
	policyMap = &sync.Map{}
	fakepolicy1 = fakePolicyInfo(1)
	fakepolicy2 = fakePolicyInfo(2)
	policyMap.Store(fakepolicy1.Name.L, fakepolicy1)
	policyMap.Store(fakepolicy2.Name.L, fakepolicy2)
	tableInfos[0].Info.PlacementPolicyRef = &model.PolicyRefInfo{
		ID:   fakepolicy1.ID,
		Name: fakepolicy1.Name,
	}
	tableInfos[5].Info.Partition.Definitions[0].PlacementPolicyRef = &model.PolicyRefInfo{
		ID:   fakepolicy2.ID,
		Name: fakepolicy2.Name,
	}
	for i := 0; i < len(createTableSQLs); i += 1 {
		err = db.CreateTable(ctx, tableInfos[i], nil, true, policyMap)
		require.NoError(t, err)
	}

	// test db.CreateDatabase
	// Prepare policy map
	policyMap = &sync.Map{}
	fakepolicy1 = fakePolicyInfo(1)
	policyMap.Store(fakepolicy1.Name.L, fakepolicy1)
	err = db.CreateDatabase(ctx, &model.DBInfo{
		ID:      20000,
		Name:    pmodel.NewCIStr("test_db"),
		Charset: "utf8mb4",
		Collate: "utf8mb4_bin",
		State:   model.StatePublic,
		PlacementPolicyRef: &model.PolicyRefInfo{
			ID:   fakepolicy1.ID,
			Name: fakepolicy1.Name,
		},
	}, true, policyMap)
	require.NoError(t, err)
}

func TestUpdateMetaVersion(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)
	tk.MustExec("use test")
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("drop table if exists `t`;")

	// Test SQL Mode
	db, supportPolicy, err := preallocdb.NewDB(gluetidb.New(), s.Mock.Storage, "STRICT")
	require.NoError(t, err)
	require.True(t, supportPolicy)
	defer db.Close()

	db.Session().Execute(ctx, "create table test.t (id int);")
	db.Session().Execute(ctx, "analyze table test.t;")
	db.Session().Execute(ctx, "insert into test.t values (1),(2),(3);")
	info, err := s.Mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	tableInfo, err := info.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	restoreTS := uint64(0)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	err = kv.RunInNewTxn(ctx, s.Mock.Domain.Store(), true, func(_ context.Context, txn kv.Transaction) error {
		restoreTS = txn.StartTS()
		return nil
	})
	require.NoError(t, err)
	tableID := tableInfo.Meta().ID
	err = db.UpdateStatsMeta(ctx, tableID, restoreTS, 3)
	require.NoError(t, err)

	rows := tk.MustQuery("select version, table_id, modify_count, count, snapshot from mysql.stats_meta;").Rows()
	require.Equal(t, fmt.Sprintf("%d", restoreTS), rows[0][0])
	require.Equal(t, fmt.Sprintf("%d", tableID), rows[0][1])
	require.Equal(t, "0", rows[0][2])
	require.Equal(t, "3", rows[0][3])
	require.Equal(t, fmt.Sprintf("%d", restoreTS), rows[0][4])
}

func TestCreateTablesInDb(t *testing.T) {
	s := utiltest.CreateRestoreSchemaSuite(t)
	info, err := s.Mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoErrorf(t, err, "Error get snapshot info schema: %s", err)

	dbSchema, isExist := info.SchemaByName(pmodel.NewCIStr("test"))
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
				Name: pmodel.NewCIStr("test" + strconv.Itoa(i)),
				Columns: []*model.ColumnInfo{{
					ID:        1,
					Name:      pmodel.NewCIStr("id"),
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

func TestDDLJobMap(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)
	tk.MustExec("use test")
	tk.MustExec("set @@sql_mode=''")

	db, supportPolicy, err := preallocdb.NewDB(gluetidb.New(), s.Mock.Storage, "STRICT")
	require.NoError(t, err)
	require.True(t, supportPolicy)
	defer db.Close()

	db.Session().Execute(ctx, "CREATE TABLE test.t1 (a BIGINT PRIMARY KEY AUTO_RANDOM, b VARCHAR(255));")
	db.Session().Execute(ctx, "CREATE TABLE test.t2 (a BIGINT AUTO_RANDOM, b VARCHAR(255), PRIMARY KEY (`a`, `b`));")
	db.Session().Execute(ctx, "CREATE TABLE test.t3 (a BIGINT PRIMARY KEY AUTO_INCREMENT, b VARCHAR(255));")
	db.Session().Execute(ctx, "CREATE TABLE test.t4 (a BIGINT, b VARCHAR(255));")
	db.Session().Execute(ctx, "CREATE TABLE test.t5 (a BIGINT PRIMARY KEY, b VARCHAR(255));")

	info, err := s.Mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbInfo, exists := info.SchemaByName(pmodel.NewCIStr("test"))
	require.True(t, exists)
	tableInfo1, err := info.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo2, err := info.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t2"))
	require.NoError(t, err)
	tableInfo3, err := info.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t3"))
	require.NoError(t, err)
	tableInfo4, err := info.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t4"))
	require.NoError(t, err)
	tableInfo5, err := info.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t5"))
	require.NoError(t, err)

	toBeCorrectedTables := map[restore.UniqueTableName]bool{
		{DB: "test", Table: "t1"}: true,
		{DB: "test", Table: "t2"}: true,
		{DB: "test", Table: "t3"}: true,
		{DB: "test", Table: "t4"}: true,
		{DB: "test", Table: "t5"}: true,
	}

	err = db.CreateTablePostRestore(ctx, &metautil.Table{DB: dbInfo.Clone(), Info: tableInfo1.Meta().Clone()}, toBeCorrectedTables)
	require.NoError(t, err)
	err = db.CreateTablePostRestore(ctx, &metautil.Table{DB: dbInfo.Clone(), Info: tableInfo2.Meta().Clone()}, toBeCorrectedTables)
	require.NoError(t, err)
	err = db.CreateTablePostRestore(ctx, &metautil.Table{DB: dbInfo.Clone(), Info: tableInfo3.Meta().Clone()}, toBeCorrectedTables)
	require.NoError(t, err)
	err = db.CreateTablePostRestore(ctx, &metautil.Table{DB: dbInfo.Clone(), Info: tableInfo4.Meta().Clone()}, toBeCorrectedTables)
	require.NoError(t, err)
	err = db.CreateTablePostRestore(ctx, &metautil.Table{DB: dbInfo.Clone(), Info: tableInfo5.Meta().Clone()}, toBeCorrectedTables)
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

func TestDB_ExecDDL2(t *testing.T) {
	s := utiltest.CreateRestoreSchemaSuite(t)

	ctx := context.Background()
	fieldType := types.NewFieldType(8)
	fieldType.SetFlen(20)
	fieldType.SetCharset("binary")
	fieldType.SetCollate("binary")
	ddlJobs := []*model.Job{
		{
			Type:  model.ActionCreateSchema,
			Query: "CREATE DATABASE IF NOT EXISTS test_db;",
			BinlogInfo: &model.HistoryInfo{
				DBInfo: &model.DBInfo{
					ID:      20000,
					Name:    pmodel.NewCIStr("test_db"),
					Charset: "utf8mb4",
					Collate: "utf8mb4_bin",
					State:   model.StatePublic,
				},
			},
		},
		{
			SchemaName: "test_db",
			Type:       model.ActionCreateTable,
			Query:      "CREATE TABLE test_db.t1 (id BIGINT);",
			BinlogInfo: &model.HistoryInfo{
				TableInfo: &model.TableInfo{
					ID:      20000,
					Name:    pmodel.NewCIStr("t1"),
					Charset: "utf8mb4",
					Collate: "utf8mb4_bin",
					Columns: []*model.ColumnInfo{
						{
							ID:        1,
							Name:      pmodel.NewCIStr("id"),
							FieldType: *fieldType,
							State:     model.StatePublic,
							Version:   2,
						},
					},
				},
			},
		},
		{
			SchemaName: "test_db",
			Type:       model.ActionAddIndex,
			Query:      "ALTER TABLE test_db.t1 ADD INDEX i1(id);",
			BinlogInfo: &model.HistoryInfo{
				TableInfo: &model.TableInfo{},
			},
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
		dbInfo, exists := info.SchemaByName(pmodel.NewCIStr("test"))
		require.True(t, exists)
		tableInfo, err := info.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr(name))
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
