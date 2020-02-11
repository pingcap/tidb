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

package main

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
)

var isCoverageServer = "0"

// TestRunMain is a dummy test case, which contains only the main function of tidb-server,
// and it is used to generate coverage_server.
func TestRunMain(t *testing.T) {
	if isCoverageServer == "1" {
		main()
	}
}

var _ = Suite(&testMainSuite{})

type testMainSuite struct{}

func TestT(t *testing.T) {
	TestingT(t)
}

func (t *testMainSuite) TestSafeMode(c *C) {
	cfg = config.GetGlobalConfig()
	setSafeModeConfig()
	c.Assert(cfg.TokenLimit == 1, IsTrue)
	c.Assert(cfg.EnableStreaming, IsFalse)
	c.Assert(cfg.EnableBatchDML, IsFalse)
	c.Assert(cfg.AlterPrimaryKey, IsFalse)
	c.Assert(cfg.EnableTableLock, IsFalse)
	c.Assert(cfg.TxnLocalLatches.Enabled, IsFalse)
	c.Assert(cfg.Performance.RunAutoAnalyze, IsFalse)
	c.Assert(cfg.Performance.CrossJoin, IsFalse)
	c.Assert(cfg.Performance.FeedbackProbability == 0, IsTrue)
	c.Assert(cfg.Performance.ForcePriority == "HIGH_PRIORITY", IsTrue)
	c.Assert(cfg.Performance.TxnTotalSizeLimit == config.DefTxnTotalSizeLimit, IsTrue)
	c.Assert(cfg.PreparedPlanCache.Enabled, IsFalse)
	c.Assert(cfg.TiKVClient.GrpcConnectionCount == 1, IsTrue)
	c.Assert(cfg.TiKVClient.StoreLimit == 1, IsTrue)
}
