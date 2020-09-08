package export

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"database/sql/driver"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct {
	mockCfg *Config
}

func (s *testUtilSuite) SetUpSuite(c *C) {
	s.mockCfg = &Config{
		FileSize: UnspecifiedSize,
	}
}

func (s *testUtilSuite) TestWriteMeta(c *C) {
	createTableStmt := "CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	specCmts := []string{"/*!40103 SET TIME_ZONE='+00:00' */;"}
	meta := newMockMetaIR("t1", createTableStmt, specCmts)
	strCollector := &mockStringCollector{}

	err := WriteMeta(meta, strCollector)
	c.Assert(err, IsNil)
	expected := "/*!40103 SET TIME_ZONE='+00:00' */;\n" +
		"CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
	c.Assert(strCollector.buf, Equals, expected)
}

func (s *testUtilSuite) TestWriteInsert(c *C) {
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
	bf := &bytes.Buffer{}

	err := WriteInsert(context.Background(), tableIR, bf, UnspecifiedSize, UnspecifiedSize)
	c.Assert(err, IsNil)
	expected := "/*!40101 SET NAMES binary*/;\n" +
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"INSERT INTO `employee` VALUES\n" +
		"(1,'male','bob@mail.com','020-1234',NULL),\n" +
		"(2,'female','sarah@mail.com','020-1253','healthy'),\n" +
		"(3,'male','john@mail.com','020-1256','healthy'),\n" +
		"(4,'female','sarah@mail.com','020-1235','healthy');\n"
	c.Assert(bf.String(), Equals, expected)
}

func (s *testUtilSuite) TestWriteInsertReturnsError(c *C) {
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
	// row errors at last line
	rowErr := errors.New("mock row error")
	tableIR := newMockTableIR("test", "employee", data, specCmts, colTypes)
	tableIR.rowErr = rowErr
	bf := &bytes.Buffer{}

	err := WriteInsert(context.Background(), tableIR, bf, UnspecifiedSize, UnspecifiedSize)
	c.Assert(err, Equals, rowErr)
	expected := "/*!40101 SET NAMES binary*/;\n" +
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"INSERT INTO `employee` VALUES\n" +
		"(1,'male','bob@mail.com','020-1234',NULL),\n" +
		"(2,'female','sarah@mail.com','020-1253','healthy'),\n" +
		"(3,'male','john@mail.com','020-1256','healthy');\n"
	c.Assert(bf.String(), Equals, expected)
}

func (s *testUtilSuite) TestWriteInsertInCsv(c *C) {
	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	tableIR := newMockTableIR("test", "employee", data, nil, colTypes)
	bf := &bytes.Buffer{}

	// test nullValue
	opt := &csvOption{separator: []byte(","), delimiter: doubleQuotationMark, nullValue: "\\N"}
	err := WriteInsertInCsv(context.Background(), tableIR, bf, true, opt, UnspecifiedSize)
	c.Assert(err, IsNil)
	expected := "1,\"male\",\"bob@mail.com\",\"020-1234\",\\N\n" +
		"2,\"female\",\"sarah@mail.com\",\"020-1253\",\"healthy\"\n" +
		"3,\"male\",\"john@mail.com\",\"020-1256\",\"healthy\"\n" +
		"4,\"female\",\"sarah@mail.com\",\"020-1235\",\"healthy\"\n"
	c.Assert(bf.String(), Equals, expected)

	// test delimiter
	bf.Reset()
	opt.delimiter = quotationMark
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	err = WriteInsertInCsv(context.Background(), tableIR, bf, true, opt, UnspecifiedSize)
	c.Assert(err, IsNil)
	expected = "1,'male','bob@mail.com','020-1234',\\N\n" +
		"2,'female','sarah@mail.com','020-1253','healthy'\n" +
		"3,'male','john@mail.com','020-1256','healthy'\n" +
		"4,'female','sarah@mail.com','020-1235','healthy'\n"
	c.Assert(bf.String(), Equals, expected)

	// test separator
	bf.Reset()
	opt.separator = []byte(";")
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	err = WriteInsertInCsv(context.Background(), tableIR, bf, true, opt, UnspecifiedSize)
	c.Assert(err, IsNil)
	expected = "1;'male';'bob@mail.com';'020-1234';\\N\n" +
		"2;'female';'sarah@mail.com';'020-1253';'healthy'\n" +
		"3;'male';'john@mail.com';'020-1256';'healthy'\n" +
		"4;'female';'sarah@mail.com';'020-1235';'healthy'\n"
	c.Assert(bf.String(), Equals, expected)

	// test delimiter that included in values
	bf.Reset()
	opt.separator = []byte("&;,?")
	opt.delimiter = []byte("ma")
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	tableIR.colNames = []string{"id", "gender", "email", "phone_number", "status"}
	err = WriteInsertInCsv(context.Background(), tableIR, bf, false, opt, UnspecifiedSize)
	c.Assert(err, IsNil)
	expected = "maidma&;,?magenderma&;,?maemamailma&;,?maphone_numberma&;,?mastatusma\n" +
		"1&;,?mamamalema&;,?mabob@mamail.comma&;,?ma020-1234ma&;,?\\N\n" +
		"2&;,?mafemamalema&;,?masarah@mamail.comma&;,?ma020-1253ma&;,?mahealthyma\n" +
		"3&;,?mamamalema&;,?majohn@mamail.comma&;,?ma020-1256ma&;,?mahealthyma\n" +
		"4&;,?mafemamalema&;,?masarah@mamail.comma&;,?ma020-1235ma&;,?mahealthyma\n"
	c.Assert(bf.String(), Equals, expected)
}

func (s *testUtilSuite) TestSQLDataTypes(c *C) {
	data := [][]driver.Value{
		{"CHAR", "char1", `'char1'`},
		{"INT", 12345, `12345`},
		{"BINARY", 1234, "x'31323334'"},
	}

	for _, datum := range data {
		sqlType, origin, result := datum[0].(string), datum[1], datum[2].(string)

		tableData := [][]driver.Value{{origin}}
		colType := []string{sqlType}
		tableIR := newMockTableIR("test", "t", tableData, nil, colType)
		bf := &bytes.Buffer{}

		err := WriteInsert(context.Background(), tableIR, bf, UnspecifiedSize, UnspecifiedSize)
		c.Assert(err, IsNil)
		lines := strings.Split(bf.String(), "\n")
		c.Assert(len(lines), Equals, 3)
		c.Assert(lines[1], Equals, fmt.Sprintf("(%s);", result))
	}
}

func (s *testUtilSuite) TestWrite(c *C) {
	mocksw := &mockStringWriter{}
	src := []string{"test", "loooooooooooooooooooong", "poison"}
	exp := []string{"test", "loooooooooooooooooooong", "poison_error"}

	for i, s := range src {
		err := write(mocksw, s)
		if err != nil {
			c.Assert(err.Error(), Equals, exp[i])
		} else {
			c.Assert(s, Equals, mocksw.buf)
			c.Assert(mocksw.buf, Equals, exp[i])
		}
	}
	err := write(mocksw, "test")
	c.Assert(err, IsNil)
}
