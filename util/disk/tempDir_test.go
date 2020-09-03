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

package disk

import (
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"os"
	"sync"
	"testing"
)

func TestT(t *testing.T) {
	check.TestingT(t)
}

var _ = check.SerialSuites(&testDiskSerialSuite{})

type testDiskSerialSuite struct {
}

func (s *testDiskSerialSuite) TestRemoveDir(c *check.C) {
	err := CheckAndInitTempDir()
	c.Assert(err, check.IsNil)
	c.Assert(checkTempDirExist(), check.Equals, true)
	c.Assert(os.RemoveAll(config.GetGlobalConfig().TempStoragePath), check.IsNil)
	c.Assert(checkTempDirExist(), check.Equals, false)
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(c *check.C) {
			err := CheckAndInitTempDir()
			c.Assert(err, check.IsNil)
			wg.Done()
		}(c)
	}
	wg.Wait()
	err = CheckAndInitTempDir()
	c.Assert(err, check.IsNil)
	c.Assert(checkTempDirExist(), check.Equals, true)
}
