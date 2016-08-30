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

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
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
	ts.tidbdrv = NewTiDBDriver(store)
	cfg := &Config{
		Addr:         ":4001",
		LogLevel:     "debug",
		StatusAddr:   ":10090",
		ReportStatus: true,
	}
	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go ts.server.Run()
	time.Sleep(time.Millisecond * 100)
}

func (ts *TidbTestSuite) TearDownSuite(c *C) {
	if ts.server != nil {
		ts.server.Close()
	}
}

func (ts *TidbTestSuite) TestRegression(c *C) {
	if regression {
		runTestRegression(c)
	}
}

func (ts *TidbTestSuite) TestUint64(c *C) {
	runTestPrepareResultFieldType(c)
}

func (ts *TidbTestSuite) TestSpecialType(c *C) {
	runTestSpecialType(c)
}

func (ts *TidbTestSuite) TestPreparedString(c *C) {
	runTestPreparedString(c)
}

func (ts *TidbTestSuite) TestLoadData(c *C) {
	runTestLoadData(c)
}

func (ts *TidbTestSuite) TestConcurrentUpdate(c *C) {
	runTestConcurrentUpdate(c)
}

func (ts *TidbTestSuite) TestErrorCode(c *C) {
	runTestErrorCode(c)
}

func (ts *TidbTestSuite) TestAuth(c *C) {
	runTestAuth(c)
}

func (ts *TidbTestSuite) TestIssues(c *C) {
	runTestIssues(c)
}

func (ts *TidbTestSuite) TestResultFieldTableIsNull(c *C) {
	runTestResultFieldTableIsNull(c)
}

func (ts *TidbTestSuite) TestStatusAPI(c *C) {
	runTestStatusAPI(c)
}

func (ts *TidbTestSuite) TestMultiPacket(c *C) {
	runTestMultiPacket(c)
}

func (ts *TidbTestSuite) TestMultiStatements(c *C) {
	runTestMultiStatements(c)
}

func (ts *TidbTestSuite) TestSocket(c *C) {
	cfg := &Config{
		LogLevel:   "debug",
		StatusAddr: ":10091",
		Socket:     "/tmp/tidbtest.sock",
	}
	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	tcpDsn := dsn
	dsn = "root@unix(/tmp/tidbtest.sock)/test?strict=true"
	runTestRegression(c)
	dsn = tcpDsn
	server.Close()
}
