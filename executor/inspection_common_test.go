package executor_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/testkit"
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
