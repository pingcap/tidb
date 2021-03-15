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

package executor

import . "github.com/pingcap/check"

type testBRIESuite struct{}

var _ = Suite(&testBRIESuite{})

func (s *testBRIESuite) TestGlueGetVersion(c *C) {
	g := tidbGlueSession{}
	version := g.GetVersion()
	c.Assert(version, Matches, `(.|\n)*Release Version(.|\n)*`)
	c.Assert(version, Matches, `(.|\n)*Git Commit Hash(.|\n)*`)
	c.Assert(version, Matches, `(.|\n)*GoVersion(.|\n)*`)
}
