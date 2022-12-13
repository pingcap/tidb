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

package texttree_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/texttree"
)

type texttreeSuite struct{}

var _ = Suite(&texttreeSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

func (s *texttreeSuite) TestPrettyIdentifier(c *C) {
	c.Assert(texttree.PrettyIdentifier("test", "", false), Equals, "test")
	c.Assert(texttree.PrettyIdentifier("test", "  │  ", false), Equals, "  ├ ─test")
	c.Assert(texttree.PrettyIdentifier("test", "\t\t│\t\t", false), Equals, "\t\t├\t─test")
	c.Assert(texttree.PrettyIdentifier("test", "  │  ", true), Equals, "  └ ─test")
	c.Assert(texttree.PrettyIdentifier("test", "\t\t│\t\t", true), Equals, "\t\t└\t─test")
}

func (s *texttreeSuite) TestIndent4Child(c *C) {
	c.Assert(texttree.Indent4Child("    ", false), Equals, "    │ ")
	c.Assert(texttree.Indent4Child("    ", true), Equals, "    │ ")
	c.Assert(texttree.Indent4Child("   │ ", true), Equals, "     │ ")
}
