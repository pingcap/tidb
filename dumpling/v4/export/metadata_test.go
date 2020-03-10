package export

import (
	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"path"
)

var _ = Suite(&testMetaDataSuite{})

type testMetaDataSuite struct{}

func (s *testMetaDataSuite) TestMysqlMetaData(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	logFile := "ON.000001"
	pos := "7502"
	gtidSet := "6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29"
	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos, "", "", gtidSet)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)

	testFilePath := "/test"
	m := newGlobalMetadata(testFilePath)
	c.Assert(m.getGlobalMetaData(db, ServerTypeMySQL), IsNil)
	c.Assert(m.filePath, Equals, path.Join(testFilePath, metadataPath))

	c.Assert(m.logFile, Equals, logFile)
	c.Assert(m.pos, Equals, pos)
	c.Assert(m.gtidSet, Equals, gtidSet)

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testMetaDataSuite) TestMariaDBMetaData(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	logFile := "mariadb-bin.000016"
	pos := "475"
	gtidSet := "0-1-2"
	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
		AddRow(logFile, pos, "", "")
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"@@global.gtid_binlog_pos"}).
		AddRow(gtidSet)
	mock.ExpectQuery("SELECT @@global.gtid_binlog_pos").WillReturnRows(rows)
	testFilePath := "/test"
	m := newGlobalMetadata(testFilePath)
	c.Assert(m.getGlobalMetaData(db, ServerTypeMariaDB), IsNil)
	c.Assert(m.filePath, Equals, path.Join(testFilePath, metadataPath))

	c.Assert(m.logFile, Equals, logFile)
	c.Assert(m.pos, Equals, pos)
	c.Assert(m.gtidSet, Equals, gtidSet)

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}