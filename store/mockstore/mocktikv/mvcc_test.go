// Copyright 2018-present, PingCAP, Inc.
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

package mocktikv

import . "github.com/pingcap/check"

var _ = Suite(testMvccSuite{})

type testMvccSuite struct {
}

func (s testMvccSuite) TestRegionContains(c *C) {
	c.Check(regionContains([]byte{}, []byte{}, []byte{}), IsTrue)
	c.Check(regionContains([]byte{}, []byte{}, []byte{1}), IsTrue)
	c.Check(regionContains([]byte{1, 1, 1}, []byte{}, []byte{1, 1, 0}), IsFalse)
	c.Check(regionContains([]byte{1, 1, 1}, []byte{}, []byte{1, 1, 1}), IsTrue)
	c.Check(regionContains([]byte{}, []byte{2, 2, 2}, []byte{2, 2, 1}), IsTrue)
	c.Check(regionContains([]byte{}, []byte{2, 2, 2}, []byte{2, 2, 2}), IsFalse)
	c.Check(regionContains([]byte{1, 1, 1}, []byte{2, 2, 2}, []byte{1, 1, 0}), IsFalse)
	c.Check(regionContains([]byte{1, 1, 1}, []byte{2, 2, 2}, []byte{1, 1, 1}), IsTrue)
	c.Check(regionContains([]byte{1, 1, 1}, []byte{2, 2, 2}, []byte{2, 2, 1}), IsTrue)
	c.Check(regionContains([]byte{1, 1, 1}, []byte{2, 2, 2}, []byte{2, 2, 2}), IsFalse)
}
