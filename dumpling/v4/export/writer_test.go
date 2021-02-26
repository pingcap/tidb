// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql/driver"
	"io/ioutil"
	"os"
	"path"

	tcontext "github.com/pingcap/dumpling/v4/context"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/br/pkg/storage"
	. "github.com/pingcap/check"
)

var _ = Suite(&testWriterSuite{})

type testWriterSuite struct{}

func (s *testWriterSuite) newWriter(conf *Config, c *C) *Writer {
	b, err := storage.ParseBackend(conf.OutputDirPath, &conf.BackendOptions)
	c.Assert(err, IsNil)
	extStore, err := storage.Create(context.Background(), b, false)
	c.Assert(err, IsNil)
	db, _, err := sqlmock.New()
	c.Assert(err, IsNil)
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	return NewWriter(tcontext.Background(), 0, conf, conn, extStore)
}

func (s *testWriterSuite) TestWriteDatabaseMeta(c *C) {
	dir := c.MkDir()

	config := DefaultConfig()
	config.OutputDirPath = dir

	writer := s.newWriter(config, c)
	err := writer.WriteDatabaseMeta("test", "CREATE DATABASE `test`")
	c.Assert(err, IsNil)
	p := path.Join(dir, "test-schema-create.sql")
	_, err = os.Stat(p)
	c.Assert(err, IsNil)
	bytes, err := ioutil.ReadFile(p)
	c.Assert(err, IsNil)
	c.Assert(string(bytes), Equals, "/*!40101 SET NAMES binary*/;\nCREATE DATABASE `test`;\n")
}

func (s *testWriterSuite) TestWriteTableMeta(c *C) {
	dir := c.MkDir()

	config := DefaultConfig()
	config.OutputDirPath = dir

	writer := s.newWriter(config, c)
	err := writer.WriteTableMeta("test", "t", "CREATE TABLE t (a INT)")
	c.Assert(err, IsNil)
	p := path.Join(dir, "test.t-schema.sql")
	_, err = os.Stat(p)
	c.Assert(err, IsNil)
	bytes, err := ioutil.ReadFile(p)
	c.Assert(err, IsNil)
	c.Assert(string(bytes), Equals, "/*!40101 SET NAMES binary*/;\nCREATE TABLE t (a INT);\n")
}

func (s *testWriterSuite) TestWriteViewMeta(c *C) {
	dir := c.MkDir()

	config := DefaultConfig()
	config.OutputDirPath = dir

	writer := s.newWriter(config, c)
	specCmt := "/*!40101 SET NAMES binary*/;\n"
	createTableSQL := "CREATE TABLE `v`(\n`a` int\n)ENGINE=MyISAM;\n"
	createViewSQL := "DROP TABLE IF EXISTS `v`;\nDROP VIEW IF EXISTS `v`;\nSET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;\nSET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;\nSET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;\nSET character_set_client = utf8;\nSET character_set_results = utf8;\nSET collation_connection = utf8_general_ci;\nCREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` (`a`) AS SELECT `t`.`a` AS `a` FROM `test`.`t`;\nSET character_set_client = @PREV_CHARACTER_SET_CLIENT;\nSET character_set_results = @PREV_CHARACTER_SET_RESULTS;\nSET collation_connection = @PREV_COLLATION_CONNECTION;\n"
	err := writer.WriteViewMeta("test", "v", createTableSQL, createViewSQL)
	c.Assert(err, IsNil)

	p := path.Join(dir, "test.v-schema.sql")
	_, err = os.Stat(p)
	c.Assert(err, IsNil)
	bytes, err := ioutil.ReadFile(p)
	c.Assert(err, IsNil)
	c.Assert(string(bytes), Equals, specCmt+createTableSQL)

	p = path.Join(dir, "test.v-schema-view.sql")
	_, err = os.Stat(p)
	c.Assert(err, IsNil)
	bytes, err = ioutil.ReadFile(p)
	c.Assert(err, IsNil)
	c.Assert(string(bytes), Equals, specCmt+createViewSQL)
}

func (s *testWriterSuite) TestWriteTableData(c *C) {
	dir := c.MkDir()

	config := DefaultConfig()
	config.OutputDirPath = dir

	writer := s.newWriter(config, c)

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
	c.Assert(err, IsNil)

	p := path.Join(dir, "test.employee.000000000.sql")
	_, err = os.Stat(p)
	c.Assert(err, IsNil)
	bytes, err := ioutil.ReadFile(p)
	c.Assert(err, IsNil)

	expected := "/*!40101 SET NAMES binary*/;\n" +
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"INSERT INTO `employee` VALUES\n" +
		"(1,'male','bob@mail.com','020-1234',NULL),\n" +
		"(2,'female','sarah@mail.com','020-1253','healthy'),\n" +
		"(3,'male','john@mail.com','020-1256','healthy'),\n" +
		"(4,'female','sarah@mail.com','020-1235','healthy');\n"
	c.Assert(string(bytes), Equals, expected)
}

func (s *testWriterSuite) TestWriteTableDataWithFileSize(c *C) {
	dir := c.MkDir()

	config := DefaultConfig()
	config.OutputDirPath = dir
	config.FileSize = 50
	specCmts := []string{
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	config.FileSize += uint64(len(specCmts[0]) + 1)
	config.FileSize += uint64(len(specCmts[1]) + 1)
	config.FileSize += uint64(len("INSERT INTO `employees` VALUES\n"))

	writer := s.newWriter(config, c)

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	tableIR := newMockTableIR("test", "employee", data, specCmts, colTypes)
	err := writer.WriteTableData(tableIR, tableIR, 0)
	c.Assert(err, IsNil)

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
		c.Assert(err, IsNil)
		bytes, err := ioutil.ReadFile(p)
		c.Assert(err, IsNil)
		c.Assert(string(bytes), Equals, expected)
	}
}

func (s *testWriterSuite) TestWriteTableDataWithFileSizeAndRows(c *C) {
	dir := c.MkDir()

	config := DefaultConfig()
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

	writer := s.newWriter(config, c)

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	tableIR := newMockTableIR("test", "employee", data, specCmts, colTypes)
	err := writer.WriteTableData(tableIR, tableIR, 0)
	c.Assert(err, IsNil)

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
		c.Assert(err, IsNil)
		bytes, err := ioutil.ReadFile(p)
		c.Assert(err, IsNil)
		c.Assert(string(bytes), Equals, expected)
	}
}

func (s *testWriterSuite) TestWriteTableDataWithStatementSize(c *C) {
	dir := c.MkDir()

	config := DefaultConfig()
	config.OutputDirPath = dir
	config.StatementSize = 50
	config.StatementSize += uint64(len("INSERT INTO `employee` VALUES\n"))
	var err error
	config.OutputFileTemplate, err = ParseOutputFileTemplate("specified-name")
	c.Assert(err, IsNil)

	writer := s.newWriter(config, c)

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
	c.Assert(err, IsNil)

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
		c.Assert(err, IsNil)
		bytes, err1 := ioutil.ReadFile(p)
		c.Assert(err1, IsNil)
		c.Assert(string(bytes), Equals, expected)
	}

	// with file size and statement size
	config.FileSize = 204
	config.StatementSize = 95
	config.FileSize += uint64(len(specCmts[0]) + 1)
	config.FileSize += uint64(len(specCmts[1]) + 1)
	config.StatementSize += uint64(len("INSERT INTO `employee` VALUES\n"))
	// test specifying filename format
	config.OutputFileTemplate, err = ParseOutputFileTemplate("{{.Index}}-{{.Table}}-{{fn .DB}}")
	c.Assert(err, IsNil)
	err = os.RemoveAll(config.OutputDirPath)
	c.Assert(err, IsNil)
	config.OutputDirPath, err = ioutil.TempDir("", "dumpling")
	writer = s.newWriter(config, c)

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
	c.Assert(writer.WriteTableData(tableIR, tableIR, 0), IsNil)
	c.Assert(err, IsNil)
	for p, expected := range cases {
		p = path.Join(config.OutputDirPath, p)
		_, err = os.Stat(p)
		c.Assert(err, IsNil)
		bytes, err := ioutil.ReadFile(p)
		c.Assert(err, IsNil)
		c.Assert(string(bytes), Equals, expected)
	}
}
