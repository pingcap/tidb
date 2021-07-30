// Copyright 2021 PingCAP, Inc.
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

package label

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
)

var _ = Suite(&testRuleSuite{})

type testRuleSuite struct{}

func (t *testRuleSuite) TestApplyAttributesSpec(c *C) {
	spec := &ast.AttributesSpec{Attributes: "attr1,attr2"}
	rule := NewRule()
	rule.ApplyAttributesSpec(spec)
	c.Assert(rule.Labels, HasLen, 2)
	c.Assert(rule.Labels[0].Key, Equals, "attr1")
	c.Assert(rule.Labels[1].Key, Equals, "attr2")
}

func (t *testRuleSuite) TestResetID(c *C) {
	rule := NewRule()
	rule.ResetTable(1, "db1", "t1")
	c.Assert(rule.ID, Equals, "schema/db1/t1")
	c.Assert(rule.RuleType, Equals, ruleType)
	c.Assert(rule.Labels, HasLen, 2)
	c.Assert(rule.Labels[0].Value, Equals, "db1")
	c.Assert(rule.Labels[1].Value, Equals, "t1")
	r := rule.Rule.(map[string]string)
	c.Assert(r["start_key"], Equals, "7480000000000000ff015f720000000000fa")
	c.Assert(r["end_key"], Equals, "7480000000000000ff025f720000000000fa")

	r1 := rule.Clone()
	c.Assert(rule, DeepEquals, r1)
}
