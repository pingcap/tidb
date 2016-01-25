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

package plans_test

import (
	"database/sql"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
)

type testPerfSchemaSuit struct {
	vars map[string]interface{}
}

var _ = Suite(&testPerfSchemaSuit{
	vars: make(map[string]interface{}),
})

func (p *testPerfSchemaSuit) TestPerfSchema(c *C) {
	testDB, err := sql.Open(tidb.DriverName, tidb.EngineGoLevelDBMemory+"/test/test")
	c.Assert(err, IsNil)
	cnt := mustQuery(c, testDB, "select * from performance_schema.setup_actors")
	c.Assert(cnt, Equals, 1)
	cnt = mustQuery(c, testDB, "select * from performance_schema.setup_objects")
	c.Assert(cnt, Equals, 12)
	// Note: so far, there has no instrumentation point yet
	cnt = mustQuery(c, testDB, "select * from performance_schema.setup_instruments")
	c.Assert(cnt, Equals, 0)
	cnt = mustQuery(c, testDB, "select * from performance_schema.setup_consumers")
	c.Assert(cnt, Equals, 12)
	cnt = mustQuery(c, testDB, "select * from performance_schema.setup_timers")
	c.Assert(cnt, Equals, 3)
	cnt = mustQuery(c, testDB, "select * from performance_schema.events_statements_current")
	c.Assert(cnt, Equals, 0)
	cnt = mustQuery(c, testDB, "select * from performance_schema.events_statements_history")
	c.Assert(cnt, Equals, 0)
	cnt = mustQuery(c, testDB, "select * from performance_schema.events_statements_history_long")
	c.Assert(cnt, Equals, 0)
	cnt = mustQuery(c, testDB, "select * from performance_schema.prepared_statements_instances")
	c.Assert(cnt, Equals, 0)
	cnt = mustQuery(c, testDB, "select * from performance_schema.events_transactions_current")
	c.Assert(cnt, Equals, 0)
	cnt = mustQuery(c, testDB, "select * from performance_schema.events_transactions_history")
	c.Assert(cnt, Equals, 0)
	cnt = mustQuery(c, testDB, "select * from performance_schema.events_transactions_history_long")
	c.Assert(cnt, Equals, 0)
	cnt = mustQuery(c, testDB, "select * from performance_schema.events_stages_current")
	c.Assert(cnt, Equals, 0)
	cnt = mustQuery(c, testDB, "select * from performance_schema.events_stages_history")
	c.Assert(cnt, Equals, 0)
	cnt = mustQuery(c, testDB, "select * from performance_schema.events_stages_history_long")
	c.Assert(cnt, Equals, 0)
}
