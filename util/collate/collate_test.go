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

package collate

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBinCollatorSuite{})

type testBinCollatorSuite struct {
}

func (s *testBinCollatorSuite) TestBinCollator(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Left   string
		Right  string
		Expect int
	}{
		{"a", "b", -1},
		{"a", "A", 1},
		{"abc", "abc", 0},
		{"abc", "ab", 1},
	}

	for i, t := range table {
		comment := Commentf("%d %v %v", i, t.Left, t.Right)
		c.Assert(GetCollator("binary").Compare(t.Left, t.Right), Equals, t.Expect, comment)
	}

	// TODO: test Key function, move the test in TestBytesCodec here.
}
