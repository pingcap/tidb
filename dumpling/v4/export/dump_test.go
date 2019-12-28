package export

import (
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
)

var _ = Suite(&testDumpSuite{})

type testDumpSuite struct{}

func (s *testDumpSuite) TestDetectServerInfo(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	mkVer := makeVersion
	data := [][]interface{}{
		{1, "8.0.18", ServerTypeMySQL, mkVer(8, 0, 18, "")},
		{2, "10.4.10-MariaDB-1:10.4.10+maria~bionic", ServerTypeMariaDB, mkVer(10, 4, 10, "MariaDB-1")},
		{3, "5.7.25-TiDB-v4.0.0-alpha-1263-g635f2e1af", ServerTypeTiDB, mkVer(4, 0, 0, "alpha-1263-g635f2e1af")},
		{4, "5.7.25-TiDB-v3.0.7-58-g6adce2367", ServerTypeTiDB, mkVer(3, 0, 7, "58-g6adce2367")},
		{5, "invalid version", ServerTypeUnknown, (*semver.Version)(nil)},
	}
	dec := func(d []interface{}) (tag int, verStr string, tp ServerType, v *semver.Version) {
		return d[0].(int), d[1].(string), ServerType(d[2].(int)), d[3].(*semver.Version)
	}

	for _, datum := range data {
		tag, r, serverTp, expectVer := dec(datum)
		cmt := Commentf("test case number: %d", tag)

		rows := sqlmock.NewRows([]string{"version"}).AddRow(r)
		mock.ExpectQuery("SELECT version()").WillReturnRows(rows)

		info, err := detectServerInfo(db)
		c.Assert(err, IsNil, cmt)
		c.Assert(info.ServerType, Equals, serverTp, cmt)
		c.Assert(info.ServerVersion == nil, Equals, expectVer == nil, cmt)
		if info.ServerVersion == nil {
			c.Assert(expectVer, IsNil, cmt)
		} else {
			c.Assert(info.ServerVersion.Equal(*expectVer), IsTrue)
		}
		c.Assert(mock.ExpectationsWereMet(), IsNil, cmt)
	}
}

func (s *testDumpSuite) TestBuildSelectAllQuery(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	mockConf := DefaultConfig()
	mockConf.SortByPk = true

	// Test when the server is TiDB.
	mockConf.ServerInfo.ServerType = ServerTypeTiDB

	// _tidb_rowid is available.
	mock.ExpectExec("SELECT _tidb_rowid from test.t").
		WillReturnResult(sqlmock.NewResult(0, 0))
	q, err := buildSelectAllQuery(mockConf, db, "test", "t")
	c.Assert(q, Equals, "SELECT * FROM test.t ORDER BY _tidb_rowid")

	// _tidb_rowid is unavailable, or PKIsHandle.
	mock.ExpectExec("SELECT _tidb_rowid from test.t").
		WillReturnError(errors.New(`1054, "Unknown column '_tidb_rowid' in 'field list'"`))
	q, err = buildSelectAllQuery(mockConf, db, "test", "t")
	c.Assert(q, Equals, "SELECT * FROM test.t")
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// Test other server.
	otherServers := []ServerType{ServerTypeUnknown, ServerTypeMySQL, ServerTypeMariaDB}

	// Test table with primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		cmt := Commentf("server type: %s", serverTp)
		mock.ExpectPrepare("SELECT column_name FROM information_schema.columns").
			ExpectQuery().WithArgs("test", "t").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id"))

		q, err := buildSelectAllQuery(mockConf, db, "test", "t")
		c.Assert(err, IsNil, cmt)
		c.Assert(q, Equals, "SELECT * FROM test.t ORDER BY id", cmt)
		err = mock.ExpectationsWereMet()
		c.Assert(err, IsNil, cmt)
		c.Assert(mock.ExpectationsWereMet(), IsNil, cmt)
	}

	// Test table without primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		cmt := Commentf("server type: %s", serverTp)
		mock.ExpectPrepare("SELECT column_name FROM information_schema.columns").
			ExpectQuery().WithArgs("test", "t").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}))

		q, err := buildSelectAllQuery(mockConf, db, "test", "t")
		c.Assert(err, IsNil, cmt)
		c.Assert(q, Equals, "SELECT * FROM test.t", cmt)
		err = mock.ExpectationsWereMet()
		c.Assert(err, IsNil, cmt)
		c.Assert(mock.ExpectationsWereMet(), IsNil)
	}

	// Test when config.SortByPk is disabled.
	mockConf.SortByPk = false
	for tp := ServerTypeUnknown; tp < ServerTypeAll; tp += 1 {
		mockConf.ServerInfo.ServerType = ServerType(tp)
		cmt := Commentf("current server type: ", tp)

		q, err := buildSelectAllQuery(mockConf, db, "test", "t")
		c.Assert(err, IsNil, cmt)
		c.Assert(q, Equals, "SELECT * FROM test.t", cmt)
		c.Assert(mock.ExpectationsWereMet(), IsNil, cmt)
	}
}

func makeVersion(major, minor, patch int64, preRelease string) *semver.Version {
	return &semver.Version{
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		PreRelease: semver.PreRelease(preRelease),
		Metadata:   "",
	}
}
