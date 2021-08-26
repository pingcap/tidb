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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

func (t *testRuleSuite) TestDefaultOrEmpty(c *C) {
	spec := &ast.AttributesSpec{Attributes: ""}
	rule := NewRule()
	rule.ApplyAttributesSpec(spec)
	rule.Reset(1, "db", "t")
	c.Assert(rule.Labels, HasLen, 0)
	spec = &ast.AttributesSpec{Default: true}
	rule = NewRule()
	rule.ApplyAttributesSpec(spec)
	rule.Reset(1, "db", "t")
	c.Assert(rule.Labels, HasLen, 0)
}

func (t *testRuleSuite) TestReset(c *C) {
	spec := &ast.AttributesSpec{Attributes: "attr"}
	rule := NewRule()
	rule.ApplyAttributesSpec(spec)
	rule.Reset(1, "db1", "t1")
	c.Assert(rule.ID, Equals, "schema/db1/t1")
	c.Assert(rule.RuleType, Equals, ruleType)
	c.Assert(rule.Labels, HasLen, 3)
	c.Assert(rule.Labels[0].Value, Equals, "true")
	c.Assert(rule.Labels[1].Value, Equals, "db1")
	c.Assert(rule.Labels[2].Value, Equals, "t1")
	r := rule.Rule.(map[string]string)
	c.Assert(r["start_key"], Equals, "7480000000000000ff015f720000000000fa")
	c.Assert(r["end_key"], Equals, "7480000000000000ff025f720000000000fa")

	r1 := rule.Clone()
	c.Assert(rule, DeepEquals, r1)

	r2 := rule.Reset(2, "db2", "t2", "p2")
	c.Assert(r2.ID, Equals, "schema/db2/t2/p2")
	c.Assert(r2.Labels, HasLen, 4)
	c.Assert(rule.Labels[0].Value, Equals, "true")
	c.Assert(rule.Labels[1].Value, Equals, "db2")
	c.Assert(rule.Labels[2].Value, Equals, "t2")
	c.Assert(rule.Labels[3].Value, Equals, "p2")
	r = r2.Rule.(map[string]string)
	c.Assert(r["start_key"], Equals, "7480000000000000ff025f720000000000fa")
	c.Assert(r["end_key"], Equals, "7480000000000000ff035f720000000000fa")
}
