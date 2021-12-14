// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	tcontext "github.com/pingcap/tidb/dumpling/context"
)

func TestPrepareDumpingDatabases(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	tctx := tcontext.Background().WithLogger(appLogger)
	conn, err := db.Conn(tctx)
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"Database"}).
		AddRow("db1").
		AddRow("db2").
		AddRow("db3").
		AddRow("db5")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	conf := defaultConfigForTest(t)
	conf.Databases = []string{"db1", "db2", "db3"}
	result, err := prepareDumpingDatabases(tctx, conf, conn)
	require.NoError(t, err)
	require.Equal(t, []string{"db1", "db2", "db3"}, result)

	conf.Databases = nil
	rows = sqlmock.NewRows([]string{"Database"}).
		AddRow("db1").
		AddRow("db2")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	result, err = prepareDumpingDatabases(tctx, conf, conn)
	require.NoError(t, err)
	require.Equal(t, []string{"db1", "db2"}, result)

	mock.ExpectQuery("SHOW DATABASES").WillReturnError(fmt.Errorf("err"))
	_, err = prepareDumpingDatabases(tctx, conf, conn)
	require.Error(t, err)

	rows = sqlmock.NewRows([]string{"Database"}).
		AddRow("db1").
		AddRow("db2").
		AddRow("db3").
		AddRow("db5")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	conf.Databases = []string{"db1", "db2", "db4", "db6"}
	_, err = prepareDumpingDatabases(tctx, conf, conn)
	require.EqualError(t, err, `Unknown databases [db4,db6]`)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListAllTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	tctx := tcontext.Background().WithLogger(appLogger)

	// Test list all tables and skipping views.
	data := NewDatabaseTables().
		AppendTables("db1", []string{"t1", "t2"}, []uint64{1, 2}).
		AppendTables("db2", []string{"t3", "t4", "t5"}, []uint64{3, 4, 5}).
		AppendViews("db3", "t6", "t7", "t8")

	dbNames := make([]databaseName, 0, len(data))
	rows := sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "AVG_ROW_LENGTH"})
	for dbName, tableInfos := range data {
		dbNames = append(dbNames, dbName)

		for _, tbInfo := range tableInfos {
			if tbInfo.Type == TableTypeView {
				continue
			}
			rows.AddRow(dbName, tbInfo.Name, tbInfo.Type.String(), tbInfo.AvgRowLength)
		}
	}
	query := "SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,AVG_ROW_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
	mock.ExpectQuery(query).WillReturnRows(rows)

	tables, err := ListAllDatabasesTables(tctx, conn, dbNames, listTableByInfoSchema, TableTypeBase)
	require.NoError(t, err)

	for d, table := range tables {
		expectedTbs, ok := data[d]
		require.True(t, ok)
		for i := 0; i < len(table); i++ {
			require.Truef(t, table[i].Equals(expectedTbs[i]), "%v mismatches expected: %v", table[i], expectedTbs[i])
		}
	}

	// Test list all tables and not skipping views.
	data = NewDatabaseTables().
		AppendTables("db", []string{"t1"}, []uint64{1}).
		AppendViews("db", "t2")
	query = "SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,AVG_ROW_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW'"
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "AVG_ROW_LENGTH"}).
		AddRow("db", "t1", TableTypeBaseStr, 1).AddRow("db", "t2", TableTypeViewStr, nil))
	tables, err = ListAllDatabasesTables(tctx, conn, []string{"db"}, listTableByInfoSchema, TableTypeBase, TableTypeView)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Len(t, tables["db"], 2)

	for i := 0; i < len(tables["db"]); i++ {
		require.Truef(t, tables["db"][i].Equals(data["db"][i]), "%v mismatches expected: %v", tables["db"][i], data["db"][i])
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListAllTablesByTableStatus(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	tctx := tcontext.Background().WithLogger(appLogger)

	// Test list all tables and skipping views.
	data := NewDatabaseTables().
		AppendTables("db1", []string{"t1", "t2"}, []uint64{1, 2}).
		AppendTables("db2", []string{"t3", "t4", "t5"}, []uint64{3, 4, 5}).
		AppendViews("db3", "t6", "t7", "t8")

	query := "SHOW TABLE STATUS FROM `%s`"
	showTableStatusColumnNames := []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length", "Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment", "Create_time", "Update_time", "Check_time", "Collation", "Checksum", "Create_options", "Comment"}
	dbNames := make([]databaseName, 0, len(data))
	for dbName, tableInfos := range data {
		dbNames = append(dbNames, dbName)
		rows := sqlmock.NewRows(showTableStatusColumnNames)

		for _, tbInfo := range tableInfos {
			if tbInfo.Type == TableTypeBase {
				rows.AddRow(tbInfo.Name, "InnoDB", 10, "Dynamic", 0, 0, 16384, 0, 0, 0, nil, "2021-07-08 03:04:07", nil, nil, "latin1_swedish_ci", nil, "", "")
			} else {
				rows.AddRow(tbInfo.Name, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, TableTypeView.String())
			}
		}
		mock.ExpectQuery(fmt.Sprintf(query, dbName)).WillReturnRows(rows)
	}

	tables, err := ListAllDatabasesTables(tctx, conn, dbNames, listTableByShowTableStatus, TableTypeBase)
	require.NoError(t, err)

	for d, table := range tables {
		expectedTbs, ok := data[d]
		require.True(t, ok)

		for i := 0; i < len(table); i++ {
			require.Truef(t, table[i].Equals(expectedTbs[i]), "%v mismatches expected: %v", table[i], expectedTbs[i])
		}
	}

	// Test list all tables and not skipping views.
	data = NewDatabaseTables().
		AppendTables("db", []string{"t1"}, []uint64{1}).
		AppendViews("db", "t2")
	mock.ExpectQuery(fmt.Sprintf(query, "db")).WillReturnRows(sqlmock.NewRows(showTableStatusColumnNames).
		AddRow("t1", "InnoDB", 10, "Dynamic", 0, 1, 16384, 0, 0, 0, nil, "2021-07-08 03:04:07", nil, nil, "latin1_swedish_ci", nil, "", "").
		AddRow("t2", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, TableTypeView.String()))
	tables, err = ListAllDatabasesTables(tctx, conn, []string{"db"}, listTableByShowTableStatus, TableTypeBase, TableTypeView)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Len(t, tables["db"], 2)

	for i := 0; i < len(tables["db"]); i++ {
		require.Truef(t, tables["db"][i].Equals(data["db"][i]), "%v mismatches expected: %v", tables["db"][i], data["db"][i])
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListAllTablesByShowFullTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	tctx := tcontext.Background().WithLogger(appLogger)

	// Test list all tables and skipping views.
	data := NewDatabaseTables().
		AppendTables("db1", []string{"t1", "t2"}, []uint64{1, 2}).
		AppendTables("db2", []string{"t3", "t4", "t5"}, []uint64{3, 4, 5}).
		AppendViews("db3", "t6", "t7", "t8")

	query := "SHOW FULL TABLES FROM `%s` WHERE TABLE_TYPE='BASE TABLE'"
	dbNames := make([]databaseName, 0, len(data))
	for dbName, tableInfos := range data {
		dbNames = append(dbNames, dbName)
		columnNames := []string{strings.ToUpper(fmt.Sprintf("Tables_in_%s", dbName)), "TABLE_TYPE"}
		rows := sqlmock.NewRows(columnNames)
		for _, tbInfo := range tableInfos {
			if tbInfo.Type == TableTypeBase {
				rows.AddRow(tbInfo.Name, TableTypeBase.String())
			} else {
				rows.AddRow(tbInfo.Name, TableTypeView.String())
			}
		}
		mock.ExpectQuery(fmt.Sprintf(query, dbName)).WillReturnRows(rows)
	}

	tables, err := ListAllDatabasesTables(tctx, conn, dbNames, listTableByShowFullTables, TableTypeBase)
	require.NoError(t, err)

	for d, table := range tables {
		expectedTbs, ok := data[d]
		require.True(t, ok)

		for i := 0; i < len(table); i++ {
			require.Truef(t, table[i].Equals(expectedTbs[i]), "%v mismatches expected: %v", table[i], expectedTbs[i])
		}
	}

	// Test list all tables and not skipping views.
	query = "SHOW FULL TABLES FROM `%s` WHERE TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW'"
	data = NewDatabaseTables().
		AppendTables("db", []string{"t1"}, []uint64{1}).
		AppendViews("db", "t2")
	for dbName, tableInfos := range data {
		columnNames := []string{strings.ToUpper(fmt.Sprintf("Tables_in_%s", dbName)), "TABLE_TYPE"}
		rows := sqlmock.NewRows(columnNames)
		for _, tbInfo := range tableInfos {
			if tbInfo.Type == TableTypeBase {
				rows.AddRow(tbInfo.Name, TableTypeBase.String())
			} else {
				rows.AddRow(tbInfo.Name, TableTypeView.String())
			}
		}
		mock.ExpectQuery(fmt.Sprintf(query, dbName)).WillReturnRows(rows)
	}
	tables, err = ListAllDatabasesTables(tctx, conn, []string{"db"}, listTableByShowFullTables, TableTypeBase, TableTypeView)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Len(t, tables["db"], 2)

	for i := 0; i < len(tables["db"]); i++ {
		require.Truef(t, tables["db"][i].Equals(data["db"][i]), "%v mismatches expected: %v", tables["db"][i], data["db"][i])
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConfigValidation(t *testing.T) {
	conf := defaultConfigForTest(t)
	conf.Where = "id < 5"
	conf.SQL = "select * from t where id > 3"
	require.EqualError(t, validateSpecifiedSQL(conf), "can't specify both --sql and --where at the same time. Please try to combine them into --sql")

	conf.Where = ""
	require.NoError(t, validateSpecifiedSQL(conf))

	conf.FileType = FileFormatSQLTextString
	err := adjustFileFormat(conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "please unset --filetype or set it to 'csv'")

	conf.FileType = FileFormatCSVString
	require.NoError(t, adjustFileFormat(conf))

	conf.FileType = ""
	require.NoError(t, adjustFileFormat(conf))
	require.Equal(t, FileFormatCSVString, conf.FileType)

	conf.SQL = ""
	conf.FileType = FileFormatSQLTextString
	require.NoError(t, adjustFileFormat(conf))

	conf.FileType = ""
	require.NoError(t, adjustFileFormat(conf))
	require.Equal(t, FileFormatSQLTextString, conf.FileType)

	conf.FileType = "rand_str"
	require.EqualError(t, adjustFileFormat(conf), "unknown config.FileType 'rand_str'")
}

func TestValidateResolveAutoConsistency(t *testing.T) {
	conf1 := defaultConfigForTest(t)
	d := &Dumper{conf: conf1}
	conf := d.conf

	testCases := []struct {
		confConsistency string
		confSnapshot    string
		err             bool
	}{
		{consistencyTypeAuto, "", true},
		{consistencyTypeAuto, "123", false},
		{consistencyTypeFlush, "", true},
		{consistencyTypeFlush, "456", false},
		{consistencyTypeLock, "", true},
		{consistencyTypeLock, "789", false},
		{consistencyTypeSnapshot, "", true},
		{consistencyTypeSnapshot, "456", true},
		{consistencyTypeNone, "", true},
		{consistencyTypeNone, "123", false},
	}
	for _, testCase := range testCases {
		conf.Consistency = testCase.confConsistency
		conf.Snapshot = testCase.confSnapshot
		if testCase.err == true {
			require.NoError(t, validateResolveAutoConsistency(d))
		} else {
			require.EqualError(t, validateResolveAutoConsistency(d), fmt.Sprintf("can't specify --snapshot when --consistency isn't snapshot, resolved consistency: %s", conf.Consistency))
		}
	}
}
