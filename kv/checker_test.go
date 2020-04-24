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

package kv_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
)

type checkerSuite struct{}

var _ = Suite(&checkerSuite{})

func (s checkerSuite) TestIsRequestTypeSupported(c *C) {
	checker := kv.RequestTypeSupportedChecker{}.IsRequestTypeSupported
	c.Assert(checker(kv.ReqTypeSelect, kv.ReqSubTypeGroupBy), IsTrue)
	c.Assert(checker(kv.ReqTypeDAG, kv.ReqSubTypeSignature), IsTrue)
	c.Assert(checker(kv.ReqTypeDAG, kv.ReqSubTypeDesc), IsTrue)
	c.Assert(checker(kv.ReqTypeDAG, kv.ReqSubTypeSignature), IsTrue)
	c.Assert(checker(kv.ReqTypeDAG, kv.ReqSubTypeAnalyzeIdx), IsFalse)
	c.Assert(checker(kv.ReqTypeAnalyze, 0), IsTrue)
	c.Assert(checker(kv.ReqTypeChecksum, 0), IsFalse)
}
