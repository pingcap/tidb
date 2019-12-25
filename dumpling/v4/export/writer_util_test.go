package export

import (
	. "github.com/pingcap/check"
	"testing"
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
		Logger:   &DummyLogger{},
		FileSize: UnspecifiedSize,
	}
}

func (s *testUtilSuite) TestWriteMeta(c *C) {
	createTableStmt := "CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	specCmts := []string{"/*!40103 SET TIME_ZONE='+00:00' */;"}
	meta := newMockMetaIR("t1", createTableStmt, specCmts)
	strCollector := &mockStringCollector{}

	err := WriteMeta(meta, strCollector, s.mockCfg)
	c.Assert(err, IsNil)
	expected := "/*!40103 SET TIME_ZONE='+00:00' */;\n" +
		"CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
	c.Assert(strCollector.buf, Equals, expected)
}

func (s *testUtilSuite) TestWriteInsert(c *C) {
	data := [][]string{
		{"1", "male", "bob@mail.com", "020-1234", ""},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	specCmts := []string{
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	tableIR := newMockTableDataIR("test", "employee", data, specCmts, colTypes)
	strCollector := &mockStringCollector{}

	err := WriteInsert(tableIR, strCollector, s.mockCfg)
	c.Assert(err, IsNil)
	expected := "/*!40101 SET NAMES binary*/;\n" +
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"INSERT INTO `employee` VALUES \n" +
		"(1, 'male', 'bob@mail.com', '020-1234', NULL),\n" +
		"(2, 'female', 'sarah@mail.com', '020-1253', 'healthy'),\n" +
		"(3, 'male', 'john@mail.com', '020-1256', 'healthy'),\n" +
		"(4, 'female', 'sarah@mail.com', '020-1235', 'healthy');\n"
	c.Assert(strCollector.buf, Equals, expected)
}

func (s *testUtilSuite) TestWrite(c *C) {
	mocksw := &mockStringWriter{}
	src := []string{"test", "loooooooooooooooooooong", "poison"}
	exp := []string{"test", "loooooooooooooooooooong", "poison_error"}

	for i, s := range src {
		err := write(mocksw, s, nil)
		if err != nil {
			c.Assert(err.Error(), Equals, exp[i])
		} else {
			c.Assert(s, Equals, mocksw.buf)
			c.Assert(mocksw.buf, Equals, exp[i])
		}
	}
	err := write(mocksw, "test", nil)
	c.Assert(err, IsNil)
}

func (s *testUtilSuite) TestConvert(c *C) {
	srcColTypes := []string{"INT", "CHAR", "BIGINT", "VARCHAR", "SET"}
	src := makeNullString([]string{"255", "", "25535", "computer_science", "male"})
	exp := []string{"255", "NULL", "25535", "'computer_science'", "'male'"}
	c.Assert(convert(src, srcColTypes), DeepEquals, exp)
}
