// Copyright 2018 PingCAP, Inc.
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

package tikv

import (
	"flag"
	"os"
	"sync"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/logutil"
)

var (
	withTiKVGlobalLock sync.RWMutex
	WithTiKV           = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")
)

// OneByOneSuite is a suite, When with-tikv flag is true, there is only one storage, so the test suite have to run one by one.
type OneByOneSuite struct{}

func (s *OneByOneSuite) SetUpSuite(c *C) {
	if *WithTiKV {
		withTiKVGlobalLock.Lock()
	} else {
		withTiKVGlobalLock.RLock()
	}
}

func (s *OneByOneSuite) TearDownSuite(c *C) {
	if *WithTiKV {
		withTiKVGlobalLock.Unlock()
	} else {
		withTiKVGlobalLock.RUnlock()
	}
}

type testTiKVSuite struct {
	OneByOneSuite
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	TestingT(t)
}

var _ = Suite(&testTiKVSuite{})

func (s *testTiKVSuite) TestBasicFunc(c *C) {
	if IsMockCommitErrorEnable() {
		defer MockCommitErrorEnable()
	} else {
		defer MockCommitErrorDisable()
	}

	MockCommitErrorEnable()
	c.Assert(IsMockCommitErrorEnable(), IsTrue)
	MockCommitErrorDisable()
	c.Assert(IsMockCommitErrorEnable(), IsFalse)
}
