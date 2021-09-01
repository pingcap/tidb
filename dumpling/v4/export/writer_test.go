// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql/driver"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	tcontext "github.com/pingcap/dumpling/v4/context"
	"github.com/stretchr/testify/require"
)

func TestWriteDatabaseMeta(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir

	writer, clean := createTestWriter(config, t)
	defer clean()

	err := writer.WriteDatabaseMeta("test", "CREATE DATABASE `test`")
	require.NoError(t, err)

	p := path.Join(dir, "test-schema-create.sql")
	_, err = os.Stat(p)
	require.NoError(t, err)

	bytes, err := ioutil.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, "/*!40101 SET NAMES binary*/;\nCREATE DATABASE `test`;\n", string(bytes))
}

func TestWriteTableMeta(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	config := defaultConfigForTest(t)
	config.OutputDirPath = dir

	writer, clean := createTestWriter(config, t)
	defer clean()

	err := writer.WriteTableMeta("test", "t", "CREATE TABLE t (a INT)")
	require.NoError(t, err)
	p := path.Join(dir, "test.t-schema.sql")
	_, err = os.Stat(p)
	require.NoError(t, err)
	bytes, err := ioutil.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, "/*!40101 SET NAMES binary*/;\nCREATE TABLE t (a INT);\n", string(bytes))
}

func TestWriteViewMeta(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir

	writer, clean := createTestWriter(config, t)
	defer clean()

	specCmt := "/*!40101 SET NAMES binary*/;\n"
	createTableSQL := "CREATE TABLE `v`(\n`a` int\n)ENGINE=MyISAM;\n"
	createViewSQL := "DROP TABLE IF EXISTS `v`;\nDROP VIEW IF EXISTS `v`;\nSET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;\nSET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;\nSET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;\nSET character_set_client = utf8;\nSET character_set_results = utf8;\nSET collation_connection = utf8_general_ci;\nCREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` (`a`) AS SELECT `t`.`a` AS `a` FROM `test`.`t`;\nSET character_set_client = @PREV_CHARACTER_SET_CLIENT;\nSET character_set_results = @PREV_CHARACTER_SET_RESULTS;\nSET collation_connection = @PREV_COLLATION_CONNECTION;\n"
	err := writer.WriteViewMeta("test", "v", createTableSQL, createViewSQL)
	require.NoError(t, err)

	p := path.Join(dir, "test.v-schema.sql")
	_, err = os.Stat(p)
	require.NoError(t, err)
	bytes, err := ioutil.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, specCmt+createTableSQL, string(bytes))

	p = path.Join(dir, "test.v-schema-view.sql")
	_, err = os.Stat(p)
	require.NoError(t, err)
	bytes, err = ioutil.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, specCmt+createViewSQL, string(bytes))
}

func TestWriteTableData(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir

	writer, clean := createTestWriter(config, t)
	defer clean()

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	specCmts := []string{
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	tableIR := newMockTableIR("test", "employee", data, specCmts, colTypes)
	err := writer.WriteTableData(tableIR, tableIR, 0)
	require.NoError(t, err)

	p := path.Join(dir, "test.employee.000000000.sql")
	_, err = os.Stat(p)
	require.NoError(t, err)
	bytes, err := ioutil.ReadFile(p)
	require.NoError(t, err)

	expected := "/*!40101 SET NAMES binary*/;\n" +
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"INSERT INTO `employee` VALUES\n" +
		"(1,'male','bob@mail.com','020-1234',NULL),\n" +
		"(2,'female','sarah@mail.com','020-1253','healthy'),\n" +
		"(3,'male','john@mail.com','020-1256','healthy'),\n" +
		"(4,'female','sarah@mail.com','020-1235','healthy');\n"
	require.Equal(t, expected, string(bytes))
}

func TestWriteTableDataWithFileSize(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir
	config.FileSize = 50
	specCmts := []string{
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	config.FileSize += uint64(len(specCmts[0]) + 1)
	config.FileSize += uint64(len(specCmts[1]) + 1)
	config.FileSize += uint64(len("INSERT INTO `employees` VALUES\n"))

	writer, clean := createTestWriter(config, t)
	defer clean()

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	tableIR := newMockTableIR("test", "employee", data, specCmts, colTypes)
	err := writer.WriteTableData(tableIR, tableIR, 0)
	require.NoError(t, err)

	cases := map[string]string{
		"test.employee.000000000.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(1,'male','bob@mail.com','020-1234',NULL),\n" +
			"(2,'female','sarah@mail.com','020-1253','healthy');\n",
		"test.employee.000000001.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(3,'male','john@mail.com','020-1256','healthy'),\n" +
			"(4,'female','sarah@mail.com','020-1235','healthy');\n",
	}

	for p, expected := range cases {
		p = path.Join(dir, p)
		_, err := os.Stat(p)
		require.NoError(t, err)
		bytes, err := ioutil.ReadFile(p)
		require.NoError(t, err)
		require.Equal(t, expected, string(bytes))
	}
}

func TestWriteTableDataWithFileSizeAndRows(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir
	config.FileSize = 50
	config.Rows = 4
	specCmts := []string{
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	config.FileSize += uint64(len(specCmts[0]) + 1)
	config.FileSize += uint64(len(specCmts[1]) + 1)
	config.FileSize += uint64(len("INSERT INTO `employees` VALUES\n"))

	writer, clean := createTestWriter(config, t)
	defer clean()

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	tableIR := newMockTableIR("test", "employee", data, specCmts, colTypes)
	err := writer.WriteTableData(tableIR, tableIR, 0)
	require.NoError(t, err)

	cases := map[string]string{
		"test.employee.0000000000000.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(1,'male','bob@mail.com','020-1234',NULL),\n" +
			"(2,'female','sarah@mail.com','020-1253','healthy');\n",
		"test.employee.0000000000001.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(3,'male','john@mail.com','020-1256','healthy'),\n" +
			"(4,'female','sarah@mail.com','020-1235','healthy');\n",
	}

	for p, expected := range cases {
		p = path.Join(dir, p)
		_, err = os.Stat(p)
		require.NoError(t, err)
		bytes, err := ioutil.ReadFile(p)
		require.NoError(t, err)
		require.Equal(t, expected, string(bytes))
	}
}

func TestWriteTableDataWithStatementSize(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir
	config.StatementSize = 50
	config.StatementSize += uint64(len("INSERT INTO `employee` VALUES\n"))
	var err error
	config.OutputFileTemplate, err = ParseOutputFileTemplate("specified-name")
	require.NoError(t, err)

	writer, clean := createTestWriter(config, t)
	defer clean()

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	specCmts := []string{
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	tableIR := newMockTableIR("te%/st", "employee", data, specCmts, colTypes)
	err = writer.WriteTableData(tableIR, tableIR, 0)
	require.NoError(t, err)

	// only with statement size
	cases := map[string]string{
		"specified-name.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(1,'male','bob@mail.com','020-1234',NULL),\n" +
			"(2,'female','sarah@mail.com','020-1253','healthy');\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(3,'male','john@mail.com','020-1256','healthy'),\n" +
			"(4,'female','sarah@mail.com','020-1235','healthy');\n",
	}

	for p, expected := range cases {
		p = path.Join(config.OutputDirPath, p)
		_, err = os.Stat(p)
		require.NoError(t, err)
		bytes, err1 := ioutil.ReadFile(p)
		require.NoError(t, err1)
		require.Equal(t, expected, string(bytes))
	}

	// with file size and statement size
	config.FileSize = 204
	config.StatementSize = 95
	config.FileSize += uint64(len(specCmts[0]) + 1)
	config.FileSize += uint64(len(specCmts[1]) + 1)
	config.StatementSize += uint64(len("INSERT INTO `employee` VALUES\n"))
	// test specifying filename format
	config.OutputFileTemplate, err = ParseOutputFileTemplate("{{.Index}}-{{.Table}}-{{fn .DB}}")
	require.NoError(t, err)
	err = os.RemoveAll(config.OutputDirPath)
	require.NoError(t, err)
	config.OutputDirPath, err = ioutil.TempDir("", "dumpling")

	writer, clean = createTestWriter(config, t)
	defer clean()

	cases = map[string]string{
		"000000000-employee-te%25%2Fst.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(1,'male','bob@mail.com','020-1234',NULL),\n" +
			"(2,'female','sarah@mail.com','020-1253','healthy');\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(3,'male','john@mail.com','020-1256','healthy');\n",
		"000000001-employee-te%25%2Fst.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(4,'female','sarah@mail.com','020-1235','healthy');\n",
	}

	tableIR = newMockTableIR("te%/st", "employee", data, specCmts, colTypes)
	require.NoError(t, writer.WriteTableData(tableIR, tableIR, 0))
	require.NoError(t, err)
	for p, expected := range cases {
		p = path.Join(config.OutputDirPath, p)
		_, err = os.Stat(p)
		require.NoError(t, err)
		bytes, err := ioutil.ReadFile(p)
		require.NoError(t, err)
		require.Equal(t, expected, string(bytes))
	}
}

var mu sync.Mutex

func createTestWriter(conf *Config, t *testing.T) (w *Writer, clean func()) {
	mu.Lock()
	extStore, err := conf.createExternalStorage(context.Background())
	mu.Unlock()

	require.NoError(t, err)
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	w = NewWriter(tcontext.Background(), 0, conf, conn, extStore)
	clean = func() {
		require.NoError(t, db.Close())
	}

	return
}
