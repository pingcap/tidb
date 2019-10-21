// Copyright 2019 PingCAP, Inc.
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

package expensivequery

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testSuite struct{}

func (s *testSuite) SetUpSuite(c *C)    {}
func (s *testSuite) TearDownSuite(c *C) {}
func (s *testSuite) SetUpTest(c *C)     { testleak.BeforeTest() }
func (s *testSuite) TearDownTest(c *C)  { testleak.AfterTest(c)() }

var _ = Suite(&testSuite{})

func (s *testSuite) TestLogFormat(c *C) {
	mem := new(memory.Tracker)
	mem.Consume(1<<30 + 1<<29 + 1<<28 + 1<<27)
	info := &util.ProcessInfo{
		ID:            233,
		User:          "PingCAP",
		Host:          "127.0.0.1",
		DB:            "Database",
		Info:          "select * from table where a > 1",
		CurTxnStartTS: 23333,
		StatsInfo: func(interface{}) map[string]uint64 {
			return nil
		},
		StmtCtx: &stmtctx.StatementContext{
			MemTracker: mem,
		},
	}
	costTime := time.Second * 233
	logFields := genLogFields(costTime, info)
	c.Assert(len(logFields), Equals, 7)
	c.Assert(logFields[0].Key, Equals, "cost_time")
	c.Assert(logFields[0].String, Equals, "233s")
	c.Assert(logFields[1].Key, Equals, "conn_id")
	c.Assert(logFields[1].Integer, Equals, int64(233))
	c.Assert(logFields[2].Key, Equals, "user")
	c.Assert(logFields[2].String, Equals, "PingCAP")
	c.Assert(logFields[3].Key, Equals, "database")
	c.Assert(logFields[3].String, Equals, "Database")
	c.Assert(logFields[4].Key, Equals, "txn_start_ts")
	c.Assert(logFields[4].Integer, Equals, int64(23333))
	c.Assert(logFields[5].Key, Equals, "mem_max")
	c.Assert(logFields[5].String, Equals, "2013265920 Bytes (1.875 GB)")
	c.Assert(logFields[6].Key, Equals, "sql")
	c.Assert(logFields[6].String, Equals, "select * from table where a > 1")
}
