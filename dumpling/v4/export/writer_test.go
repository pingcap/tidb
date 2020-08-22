package export

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	. "github.com/pingcap/check"
)

var _ = Suite(&testWriterSuite{})

type testWriterSuite struct{}

func (s *testDumpSuite) TestWriteDatabaseMeta(c *C) {
	dir, err := ioutil.TempDir("", "dumpling")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	config := DefaultConfig()
	config.OutputDirPath = dir
	ctx := context.Background()

	writer, err := NewSimpleWriter(config)
	c.Assert(err, IsNil)
	err = writer.WriteDatabaseMeta(ctx, "test", "CREATE DATABASE `test`")
	c.Assert(err, IsNil)
	p := path.Join(dir, "test-schema-create.sql")
	_, err = os.Stat(p)
	c.Assert(err, IsNil)
	bytes, err := ioutil.ReadFile(p)
	c.Assert(err, IsNil)
	c.Assert(string(bytes), Equals, "CREATE DATABASE `test`;\n")
}

func (s *testDumpSuite) TestWriteTableMeta(c *C) {
	dir, err := ioutil.TempDir("", "dumpling")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	config := DefaultConfig()
	config.OutputDirPath = dir
	ctx := context.Background()

	writer, err := NewSimpleWriter(config)
	c.Assert(err, IsNil)
	err = writer.WriteTableMeta(ctx, "test", "t", "CREATE TABLE t (a INT)")
	c.Assert(err, IsNil)
	p := path.Join(dir, "test.t-schema.sql")
	_, err = os.Stat(p)
	c.Assert(err, IsNil)
	bytes, err := ioutil.ReadFile(p)
	c.Assert(err, IsNil)
	c.Assert(string(bytes), Equals, "CREATE TABLE t (a INT);\n")
}

func (s *testDumpSuite) TestWriteTableData(c *C) {
	dir, err := ioutil.TempDir("", "dumpling")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	config := DefaultConfig()
	config.OutputDirPath = dir
	ctx := context.Background()

	simpleWriter, err := NewSimpleWriter(config)
	c.Assert(err, IsNil)
	writer := SQLWriter{SimpleWriter: simpleWriter}

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
	err = writer.WriteTableData(ctx, tableIR)
	c.Assert(err, IsNil)

	p := path.Join(dir, "test.employee.0.sql")
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

func (s *testDumpSuite) TestWriteTableDataWithFileSize(c *C) {
	dir, err := ioutil.TempDir("", "dumpling")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	config := DefaultConfig()
	config.OutputDirPath = dir
	config.FileSize = 50
	ctx := context.Background()

	simpleWriter, err := NewSimpleWriter(config)
	c.Assert(err, IsNil)
	writer := SQLWriter{SimpleWriter: simpleWriter}

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
	err = writer.WriteTableData(ctx, tableIR)
	c.Assert(err, IsNil)

	cases := map[string]string{
		"test.employee.0.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(1,'male','bob@mail.com','020-1234',NULL),\n" +
			"(2,'female','sarah@mail.com','020-1253','healthy');\n",
		"test.employee.1.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(3,'male','john@mail.com','020-1256','healthy'),\n" +
			"(4,'female','sarah@mail.com','020-1235','healthy');\n",
	}

	for p, expected := range cases {
		p := path.Join(dir, p)
		_, err = os.Stat(p)
		c.Assert(err, IsNil)
		bytes, err := ioutil.ReadFile(p)
		c.Assert(err, IsNil)
		c.Assert(string(bytes), Equals, expected)
	}
}

func (s *testDumpSuite) TestWriteTableDataWithStatementSize(c *C) {
	dir, err := ioutil.TempDir("", "dumpling")
	c.Assert(err, IsNil)

	config := DefaultConfig()
	config.OutputDirPath = dir
	config.StatementSize = 50
	config.OutputFileTemplate, err = ParseOutputFileTemplate("specified-name")
	c.Assert(err, IsNil)
	ctx := context.Background()
	defer os.RemoveAll(config.OutputDirPath)

	simpleWriter, err := NewSimpleWriter(config)
	c.Assert(err, IsNil)
	writer := SQLWriter{SimpleWriter: simpleWriter}

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
	err = writer.WriteTableData(ctx, tableIR)
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
		p := path.Join(config.OutputDirPath, p)
		_, err = os.Stat(p)
		c.Assert(err, IsNil)
		bytes, err := ioutil.ReadFile(p)
		c.Assert(err, IsNil)
		c.Assert(string(bytes), Equals, expected)
	}

	// with file size and statement size
	config.FileSize = 90
	config.StatementSize = 30
	// test specifying filename format
	config.OutputFileTemplate, err = ParseOutputFileTemplate("{{.Index}}-{{.Table}}-{{fn .DB}}")
	c.Assert(err, IsNil)
	os.RemoveAll(config.OutputDirPath)
	config.OutputDirPath, err = ioutil.TempDir("", "dumpling")
	fmt.Println(config.OutputDirPath)
	c.Assert(err, IsNil)

	cases = map[string]string{
		"0-employee-te%25%2Fst.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(1,'male','bob@mail.com','020-1234',NULL),\n" +
			"(2,'female','sarah@mail.com','020-1253','healthy');\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(3,'male','john@mail.com','020-1256','healthy');\n",
		"1-employee-te%25%2Fst.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(4,'female','sarah@mail.com','020-1235','healthy');\n",
	}

	c.Assert(writer.WriteTableData(ctx, tableIR), IsNil)
	c.Assert(err, IsNil)
	for p, expected := range cases {
		p := path.Join(config.OutputDirPath, p)
		_, err = os.Stat(p)
		c.Assert(err, IsNil)
		bytes, err := ioutil.ReadFile(p)
		c.Assert(err, IsNil)
		c.Assert(string(bytes), Equals, expected)
	}
}
