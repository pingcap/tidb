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

package set

import (
	"fmt"
	"sync"

	"github.com/pingcap/check"
)

var _ = check.Suite(&syncSetTestSuite{})

type syncSetTestSuite struct{}

func (s *syncSetTestSuite) TestSyncSet(c *check.C) {
	set := NewSyncSet()
	vals := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	for i := range vals {
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
	}
	c.Assert(set.Count(), check.Equals, len(vals))

	c.Assert(set.Count(), check.Equals, len(vals))
	for i := range vals {
		c.Assert(set.Exist(vals[i]), check.IsTrue)
	}

	c.Assert(set.Exist("11"), check.IsFalse)

	set = NewSyncSet("1", "2", "3", "4", "5", "6")
	for i := 1; i < 7; i++ {
		c.Assert(set.Exist(fmt.Sprintf("%d", i)), check.IsTrue)
	}
	c.Assert(set.Exist("7"), check.IsFalse)

	wg := sync.WaitGroup{}
	for cnt := 0; cnt < 10; cnt++ {
		wg.Add(1)
		go func() {
			for i := range vals {
				set.InsertIfNotExist(vals[i])
			}
			wg.Done()
		}()
	}

	wg.Wait()
	c.Assert(set.Count(), check.Equals, len(vals))
}
