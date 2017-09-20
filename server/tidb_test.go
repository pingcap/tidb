// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
// +build !race

package server

import (
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/config"
	tmysql "github.com/pingcap/tidb/mysql"
)

type TidbTestSuite struct {
	tidbdrv *TiDBDriver
	server  *Server
}

var _ = Suite(new(TidbTestSuite))

func (ts *TidbTestSuite) SetUpSuite(c *C) {
	log.SetLevelByString("error")
	store, err := tidb.NewStore("memory:///tmp/tidb")
	c.Assert(err, IsNil)
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(store)
	cfg := &config.Config{
		Addr:         ":4001",
		LogLevel:     "debug",
		StatusAddr:   ":10090",
		ReportStatus: true,
		TCPKeepAlive: true,
	}

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go ts.server.Run()
	waitUntilServerOnline(cfg.StatusAddr)

	// Run this test here because parallel would affect the result of it.
	runTestStmtCount(c)
	defaultLoadDataBatchCnt = 3
}

func (ts *TidbTestSuite) TearDownSuite(c *C) {
	if ts.server != nil {
		ts.server.Close()
	}
}

func (ts *TidbTestSuite) TestRegression(c *C) {
	if regression {
		c.Parallel()
		runTestRegression(c, nil, "Regression")
	}
}

func (ts *TidbTestSuite) TestUint64(c *C) {
	runTestPrepareResultFieldType(c)
}

func (ts *TidbTestSuite) TestSpecialType(c *C) {
	c.Parallel()
	runTestSpecialType(c)
}

func (ts *TidbTestSuite) TestPreparedString(c *C) {
	c.Parallel()
	runTestPreparedString(c)
}

func (ts *TidbTestSuite) TestLoadData(c *C) {
	c.Parallel()
	runTestLoadData(c)
}

func (ts *TidbTestSuite) TestConcurrentUpdate(c *C) {
	c.Parallel()
	runTestConcurrentUpdate(c)
}

func (ts *TidbTestSuite) TestErrorCode(c *C) {
	c.Parallel()
	runTestErrorCode(c)
}

func (ts *TidbTestSuite) TestAuth(c *C) {
	c.Parallel()
	runTestAuth(c)
}

func (ts *TidbTestSuite) TestIssues(c *C) {
	c.Parallel()
	runTestIssue3662(c)
	runTestIssue3680(c)
	runTestIssue3682(c)
}

func (ts *TidbTestSuite) TestDBNameEscape(c *C) {
	c.Parallel()
	runTestDBNameEscape(c)
}

func (ts *TidbTestSuite) TestResultFieldTableIsNull(c *C) {
	c.Parallel()
	runTestResultFieldTableIsNull(c)
}

func (ts *TidbTestSuite) TestStatusAPI(c *C) {
	c.Parallel()
	runTestStatusAPI(c)
}

func (ts *TidbTestSuite) TestMultiStatements(c *C) {
	c.Parallel()
	runTestMultiStatements(c)
}

func (ts *TidbTestSuite) TestSocket(c *C) {
	cfg := &config.Config{
		LogLevel:   "debug",
		StatusAddr: ":10091",
		Socket:     "/tmp/tidbtest.sock",
	}

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()

	runTestRegression(c, func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = "/tmp/tidbtest.sock"
		config.DBName = "test"
		config.Strict = true
	}, "SocketRegression")
}

func (ts *TidbTestSuite) TestShowCreateTableFlen(c *C) {
	// issue #4540
	ctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test")
	c.Assert(err, IsNil)
	_, err = ctx.Execute("use test;")
	c.Assert(err, IsNil)

	testSQL := "CREATE TABLE `t1` (" +
		"`a` char(36) NOT NULL," +
		"`b` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`c` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`d` varchar(50) DEFAULT ''," +
		"`e` char(36) NOT NULL DEFAULT ''," +
		"`f` char(36) NOT NULL DEFAULT ''," +
		"`g` char(1) NOT NULL DEFAULT 'N'," +
		"`h` varchar(100) NOT NULL," +
		"`i` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`j` varchar(10) DEFAULT ''," +
		"`k` varchar(10) DEFAULT ''," +
		"`l` varchar(20) DEFAULT ''," +
		"`m` varchar(20) DEFAULT ''," +
		"`n` varchar(30) DEFAULT ''," +
		"`o` varchar(100) DEFAULT ''," +
		"`p` varchar(50) DEFAULT ''," +
		"`q` varchar(50) DEFAULT ''," +
		"`r` varchar(100) DEFAULT ''," +
		"`s` varchar(20) DEFAULT ''," +
		"`t` varchar(50) DEFAULT ''," +
		"`u` varchar(100) DEFAULT ''," +
		"`v` varchar(50) DEFAULT ''," +
		"`w` varchar(300) NOT NULL," +
		"`x` varchar(250) DEFAULT ''," +
		"PRIMARY KEY (`a`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
	_, err = ctx.Execute(testSQL)
	c.Assert(err, IsNil)
	rs, err := ctx.Execute("show create table t1")
	row, err := rs[0].Next()
	c.Assert(err, IsNil)
	cols, err := rs[0].Columns()
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 2)
	c.Assert(int(cols[0].ColumnLength), Equals, tmysql.MaxTableNameLength*tmysql.MaxBytesOfCharacter)
	c.Assert(int(cols[1].ColumnLength), Equals, len(row[1].GetString())*tmysql.MaxBytesOfCharacter)
}
