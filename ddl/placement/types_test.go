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

package placement

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBundleSuite{})
var _ = Suite(&testRuleSuite{})

type testBundleSuite struct{}

func (t *testBundleSuite) TestEmpty(c *C) {
	bundle := &Bundle{ID: GroupID(1)}
	c.Assert(bundle.IsEmpty(), IsTrue)

	bundle = &Bundle{ID: GroupID(1), Index: 1}
	c.Assert(bundle.IsEmpty(), IsFalse)

	bundle = &Bundle{ID: GroupID(1), Override: true}
	c.Assert(bundle.IsEmpty(), IsFalse)

	bundle = &Bundle{ID: GroupID(1), Rules: []*Rule{{ID: "434"}}}
	c.Assert(bundle.IsEmpty(), IsFalse)

	bundle = &Bundle{ID: GroupID(1), Index: 1, Override: true}
	c.Assert(bundle.IsEmpty(), IsFalse)
}

func (t *testBundleSuite) TestClone(c *C) {
	bundle := &Bundle{ID: GroupID(1), Rules: []*Rule{{ID: "434"}}}

	newBundle := bundle.Clone()
	newBundle.ID = GroupID(2)
	newBundle.Rules[0] = &Rule{ID: "121"}

	c.Assert(bundle, DeepEquals, &Bundle{ID: GroupID(1), Rules: []*Rule{{ID: "434"}}})
	c.Assert(newBundle, DeepEquals, &Bundle{ID: GroupID(2), Rules: []*Rule{{ID: "121"}}})
}

type testRuleSuite struct{}

func (t *testRuleSuite) TestClone(c *C) {
	rule := &Rule{ID: "434"}
	newRule := rule.Clone()
	newRule.ID = "121"

	c.Assert(rule, DeepEquals, &Rule{ID: "434"})
	c.Assert(newRule, DeepEquals, &Rule{ID: "121"})
}
