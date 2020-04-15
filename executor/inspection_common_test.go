// Copyright 2020 PingCAP, Inc.
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

package executor_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/executor"
	"github.com/pingcap/tidb/v4/session"
	"github.com/pingcap/tidb/v4/util/testkit"
)

func (s *inspectionSummarySuite) TestInspectionRules(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	inspectionCount := len(executor.InspectionRules)
	summaryCount := len(executor.InspectionSummaryRules)
	var cases = []struct {
		sql       string
		ruleCount int
	}{
		{
			sql:       "select * from information_schema.inspection_rules",
			ruleCount: inspectionCount + summaryCount,
		},
		{
			sql:       "select * from information_schema.inspection_rules where type='inspection'",
			ruleCount: inspectionCount,
		},
		{
			sql:       "select * from information_schema.inspection_rules where type='summary'",
			ruleCount: summaryCount,
		},
		{
			sql:       "select * from information_schema.inspection_rules where type='inspection' and type='summary'",
			ruleCount: 0,
		},
	}

	for _, ca := range cases {
		rs, err := tk.Exec(ca.sql)
		c.Assert(err, IsNil)
		rules, err := session.ResultSetToStringSlice(context.Background(), tk.Se, rs)
		c.Assert(err, IsNil)
		c.Assert(len(rules), Equals, ca.ruleCount)
	}
}
