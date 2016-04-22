// Copyright 2016 PingCAP, Inc.
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

package perfschema

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

type testStatementSuit struct {
}

var _ = Suite(&testStatementSuit{})

func (p *testStatementSuit) TestUninitPS(c *C) {
	defer testleak.AfterTest(c)()
	// Run init()
	ps := &perfSchema{}
	// ps is uninitialized, all mTables are missing.
	// This may happen at the bootstrap stage.
	// So we must make sure the following actions are safe.
	err := ps.updateEventsStmtsCurrent(0, []types.Datum{})
	c.Assert(err, IsNil)
	err = ps.appendEventsStmtsHistory([]types.Datum{})
	c.Assert(err, IsNil)
}
