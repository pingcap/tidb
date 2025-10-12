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

package diff

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tidb/util/importer"
	"github.com/stretchr/testify/require"
)

func TestGenerateSQLs(t *testing.T) {
	createTableSQL := "CREATE TABLE `diff_test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), `id_gen` int(11) GENERATED ALWAYS AS ((`id` + 1)) VIRTUAL, primary key(`id`, `name`))"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	rowsData := map[string]*dbutil.ColumnData{
		"id":          {Data: []byte("1"), IsNull: false},
		"name":        {Data: []byte("xxx"), IsNull: false},
		"birthday":    {Data: []byte("2018-01-01 00:00:00"), IsNull: false},
		"update_time": {Data: []byte("10:10:10"), IsNull: false},
		"money":       {Data: []byte("11.1111"), IsNull: false},
		"id_gen":      {Data: []byte("2"), IsNull: false}, // generated column should not be contained in fix sql
	}

	replaceSQL := generateDML("replace", rowsData, tableInfo, "diff_test")
	deleteSQL := generateDML("delete", rowsData, tableInfo, "diff_test")
	require.Equal(t, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,'xxx','2018-01-01 00:00:00','10:10:10',11.1111);", replaceSQL)
	require.Equal(t, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `name` = 'xxx' AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;", deleteSQL)

	// test the unique key
	createTableSQL2 := "CREATE TABLE `diff_test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), unique key(`id`, `name`))"
	tableInfo2, err := dbutil.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)
	replaceSQL = generateDML("replace", rowsData, tableInfo2, "diff_test")
	deleteSQL = generateDML("delete", rowsData, tableInfo2, "diff_test")
	require.Equal(t, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,'xxx','2018-01-01 00:00:00','10:10:10',11.1111);", replaceSQL)
	require.Equal(t, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `name` = 'xxx' AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;", deleteSQL)

	// test value is nil
	rowsData["name"] = &dbutil.ColumnData{Data: []byte(""), IsNull: true}
	replaceSQL = generateDML("replace", rowsData, tableInfo, "diff_test")
	deleteSQL = generateDML("delete", rowsData, tableInfo, "diff_test")
	require.Equal(t, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,NULL,'2018-01-01 00:00:00','10:10:10',11.1111);", replaceSQL)
	require.Equal(t, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `name` is NULL AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;", deleteSQL)

	rowsData["id"] = &dbutil.ColumnData{Data: []byte(""), IsNull: true}
	replaceSQL = generateDML("replace", rowsData, tableInfo, "diff_test")
	deleteSQL = generateDML("delete", rowsData, tableInfo, "diff_test")
	require.Equal(t, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (NULL,NULL,'2018-01-01 00:00:00','10:10:10',11.1111);", replaceSQL)
	require.Equal(t, "DELETE FROM `diff_test`.`atest` WHERE `id` is NULL AND `name` is NULL AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;", deleteSQL)

	// test value with "'"
	rowsData["name"] = &dbutil.ColumnData{Data: []byte("a'a"), IsNull: false}
	replaceSQL = generateDML("replace", rowsData, tableInfo, "diff_test")
	deleteSQL = generateDML("delete", rowsData, tableInfo, "diff_test")
	require.Equal(t, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (NULL,'a\\'a','2018-01-01 00:00:00','10:10:10',11.1111);", replaceSQL)
	require.Equal(t, "DELETE FROM `diff_test`.`atest` WHERE `id` is NULL AND `name` = 'a\\'a' AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;", deleteSQL)
}

func TestDiff(t *testing.T) {
	t.Skip("remove it after migrate CI from tidb-tools")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	conn, err := createConn()
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "DROP DATABASE IF EXISTS `diff_test`")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "CREATE DATABASE `diff_test`")
	require.NoError(t, err)

	testStructEqual(ctx, conn, t)
	testCases := []struct {
		sourceTables  []string
		targetTable   string
		hasEmptyTable bool
	}{
		{
			[]string{"testa"},
			"testb",
			false,
		},
		{
			[]string{"testc", "testd"},
			"teste",
			false,
		},
		{
			[]string{"testf", "testg", "testh"},
			"testi",
			false,
		},
		{
			[]string{"testj", "testk"},
			"testl",
			true,
		},
	}
	for _, testCase := range testCases {
		testDataEqual(ctx, conn, "diff_test", testCase.sourceTables, testCase.targetTable, testCase.hasEmptyTable, t)
	}
}

func testStructEqual(ctx context.Context, conn *sql.DB, t *testing.T) {
	testCases := []struct {
		createSourceTable string
		createTargetTable string
		dropSourceTable   string
		dropTargetTable   string
		structEqual       bool
	}{
		{
			"CREATE TABLE `diff_test`.`testa`(`id` int, `name` varchar(24))",
			"CREATE TABLE `diff_test`.`testb`(`id` int, `name` varchar(24), unique key (`id`))",
			"DROP TABLE `diff_test`.`testa`",
			"DROP TABLE `diff_test`.`testb`",
			false,
		}, {
			"CREATE TABLE `diff_test`.`testa`(`id` int, `name` varchar(24))",
			"CREATE TABLE `diff_test`.`testb`(`id` int, `name2` varchar(24))",
			"DROP TABLE `diff_test`.`testa`",
			"DROP TABLE `diff_test`.`testb`",
			false,
		}, {
			"CREATE TABLE `diff_test`.`testa`(`id` int, `name` varchar(24))",
			"CREATE TABLE `diff_test`.`testb`(`id` int)",
			"DROP TABLE `diff_test`.`testa`",
			"DROP TABLE `diff_test`.`testb`",
			false,
		}, {
			"CREATE TABLE `diff_test`.`testa`(`id` int, `name` varchar(24))",
			"CREATE TABLE `diff_test`.`testb`(`id` int, `name` varchar(24))",
			"DROP TABLE `diff_test`.`testa`",
			"DROP TABLE `diff_test`.`testb`",
			true,
		}, {
			"CREATE TABLE `diff_test`.`testa`(`id` int, `name` varchar(24))",
			"CREATE TABLE `diff_test`.`testb`(`id` varchar(24), name varchar(24))",
			"DROP TABLE `diff_test`.`testa`",
			"DROP TABLE `diff_test`.`testb`",
			false,
		}, {
			"CREATE TABLE `diff_test`.`test``a`(`id` int, `name` varchar(24))",
			"CREATE TABLE `diff_test`.`test``b`(`id` int, `name` varchar(24), unique key (`id`))",
			"DROP TABLE `diff_test`.`test``a`",
			"DROP TABLE `diff_test`.`test``b`",
			false,
		}, {
			"CREATE TABLE `diff_test`.`test``a`(`id` int, `na``me` varchar(24))",
			"CREATE TABLE `diff_test`.`test``b`(`id` int, `na``me` varchar(24))",
			"DROP TABLE `diff_test`.`test``a`",
			"DROP TABLE `diff_test`.`test``b`",
			true,
		},
	}

	for _, testCase := range testCases {
		_, err := conn.ExecContext(ctx, testCase.createSourceTable)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, testCase.createTargetTable)
		require.NoError(t, err)

		sourceInfo, err := dbutil.GetTableInfoBySQL(testCase.createSourceTable, parser.New())
		require.NoError(t, err)

		targetInfo, err := dbutil.GetTableInfoBySQL(testCase.createTargetTable, parser.New())
		require.NoError(t, err)

		tableDiff := createTableDiff(conn, "diff_test", []string{sourceInfo.Name.O}, targetInfo.Name.O)
		structEqual, _, err := tableDiff.Equal(context.Background(), func(sql string) error {
			fmt.Println(sql)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, testCase.structEqual, structEqual)

		_, err = conn.ExecContext(ctx, testCase.dropSourceTable)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, testCase.dropTargetTable)
		require.NoError(t, err)
	}
}

func testDataEqual(ctx context.Context, conn *sql.DB, schema string, sourceTables []string, targetTable string, hasEmptyTable bool, t *testing.T) {
	defer func() {
		for _, sourceTable := range sourceTables {
			_, _ = conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", dbutil.TableName(schema, sourceTable)))
		}
		_, _ = conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", dbutil.TableName(schema, targetTable)))
	}()

	err := generateData(ctx, conn, dbutil.GetDBConfigFromEnv(schema), sourceTables, targetTable, hasEmptyTable)
	require.NoError(t, err)

	// compare data, should be equal
	fixSqls := make([]string, 0, 10)
	writeSqls := func(sql string) error {
		fixSqls = append(fixSqls, sql)
		return nil
	}

	tableDiff := createTableDiff(conn, schema, sourceTables, targetTable)
	structEqual, dataEqual, err := tableDiff.Equal(context.Background(), writeSqls)
	require.NoError(t, err)
	require.Equal(t, true, structEqual)
	require.Equal(t, true, dataEqual)

	// update data and then compare data, dataEqual should be false
	err = updateData(ctx, conn, targetTable)
	require.NoError(t, err)

	structEqual, dataEqual, err = tableDiff.Equal(context.Background(), writeSqls)
	require.NoError(t, err)
	require.Equal(t, true, structEqual)
	require.Equal(t, false, dataEqual)

	// use fixSqls to fix data, and then compare data
	for _, sql := range fixSqls {
		_, err = conn.ExecContext(ctx, sql)
		require.NoError(t, err)
	}
	structEqual, dataEqual, err = tableDiff.Equal(ctx, writeSqls)
	require.NoError(t, err)
	require.Equal(t, true, structEqual)
	require.Equal(t, true, dataEqual)

	// cancel `Equal`, dataEqual will be false, and will not panic
	ctx1, cancel1 := context.WithCancel(ctx)
	cancelEqualFunc = cancel1
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/util/diff/CancelCheckChunkDataEqual", `return(2)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/util/diff/CancelCheckChunkDataEqual"))
	}()
	structEqual, dataEqual, err = tableDiff.Equal(ctx1, writeSqls)
	require.NoError(t, err)
	require.Equal(t, true, structEqual)
	require.Equal(t, false, dataEqual)
}

func createTableDiff(conn *sql.DB, schema string, sourceTableNames []string, targetTableName string) *TableDiff {
	sourceTables := make([]*TableInstance, len(sourceTableNames))
	for _, table := range sourceTableNames {
		sourceTableInstance := &TableInstance{
			Conn:   conn,
			Schema: schema,
			Table:  table,
		}

		sourceTables = append(sourceTables, sourceTableInstance)
	}

	targetTableInstance := &TableInstance{
		Conn:   conn,
		Schema: schema,
		Table:  targetTableName,
	}

	return &TableDiff{
		CpDB:         conn,
		SourceTables: sourceTables,
		TargetTable:  targetTableInstance,
	}
}

func createConn() (*sql.DB, error) {
	return dbutil.OpenDB(dbutil.GetDBConfigFromEnv(""), nil)
}

func generateData(ctx context.Context, db *sql.DB, dbCfg dbutil.DBConfig, sourceTables []string, targetTable string, hasEmptyTable bool) error {
	createTableSQL := fmt.Sprintf("CREATE TABLE %s (\n"+
		"`a``b` date NOT NULL,\n"+
		"b datetime DEFAULT NULL,\n"+
		"c time DEFAULT NULL,\n"+
		"d varchar(10) COLLATE latin1_bin DEFAULT NULL,\n"+
		"e int(10) DEFAULT NULL,\n"+
		"h year(4) DEFAULT NULL,\n"+
		"`table` varchar(10),\n"+
		"PRIMARY KEY (`a``b`))", dbutil.TableName("diff_test", targetTable))

	cfg := &importer.Config{
		TableSQL:    createTableSQL,
		WorkerCount: 5,
		JobCount:    10000,
		Batch:       100,
		DBCfg:       dbCfg,
	}

	// generate data for target table
	importer.DoProcess(cfg)

	// generate data for source tables
	for _, sourceTable := range sourceTables {
		_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s LIKE %s", dbutil.TableName("diff_test", sourceTable), dbutil.TableName("diff_test", targetTable)))
		if err != nil {
			return err
		}
	}

	randomValueNum := int64(len(sourceTables) - 1)
	if hasEmptyTable {
		randomValueNum--
	}

	values, err := dbutil.GetRandomValues(context.Background(), db, "diff_test", targetTable, "e", int(randomValueNum), "TRUE", nil, "")
	if err != nil {
		return err
	}

	conditions := make([]string, 0, 3)
	if randomValueNum == 0 {
		conditions = append(conditions, "true")
	} else {
		conditions = append(conditions, fmt.Sprintf("e < %s", values[0]))
		for i := 0; i < len(values)-1; i++ {
			conditions = append(conditions, fmt.Sprintf("e >= %s AND e < %s", values[i], values[i+1]))
		}
		conditions = append(conditions, fmt.Sprintf("e >= %s", values[len(values)-1]))
	}

	// if hasEmptyTable is true, the last source table will be empty.
	for j, condition := range conditions {
		_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (`a``b`, `b`, `c`, `d`, e, `h`, `table`) SELECT `a``b`, `b`, `c`, `d`, e, `h`, `table` FROM %s WHERE %s", dbutil.TableName("diff_test", sourceTables[j]), dbutil.TableName("diff_test", targetTable), condition))
		if err != nil {
			return err
		}
	}

	return nil
}

func updateData(ctx context.Context, db *sql.DB, table string) error {
	values, err := dbutil.GetRandomValues(context.Background(), db, "diff_test", table, "e", 3, "TRUE", nil, "")
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("UPDATE `diff_test`.`%s` SET e = e+1 WHERE e = %v", table, values[0]))
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("DELETE FROM `diff_test`.`%s` where e = %v", table, values[1]))
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("REPLACE INTO `diff_test`.`%s` VALUES('1992-09-27','2018-09-03 16:26:27','14:45:33','i',2048790075,2008, \"abc\")", table))
	if err != nil {
		return err
	}

	return nil
}

func TestConfigHash(t *testing.T) {
	tbDiff := &TableDiff{
		Range:     "a > 1",
		ChunkSize: 1000,
	}
	tbDiff.setConfigHash()
	hash1 := tbDiff.configHash

	tbDiff.CheckThreadCount = 10
	tbDiff.setConfigHash()
	hash2 := tbDiff.configHash
	require.Equal(t, hash2, hash1)

	tbDiff.Range = "b < 10"
	tbDiff.setConfigHash()
	hash3 := tbDiff.configHash
	require.Equal(t, false, hash1 == hash3)
}
