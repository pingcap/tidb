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

package retry

import (
	"context"
	"errors"

	. "github.com/pingcap/check"
)

type testBackoffSuite struct {
}

var _ = Suite(&testBackoffSuite{})

func (s *testBackoffSuite) TestBackoffWithMax(c *C) {
	b := NewBackofferWithVars(context.TODO(), 2000, nil)
	err := b.BackoffWithMaxSleepTxnLockFast(30, errors.New("test"))
	c.Assert(err, IsNil)
	c.Assert(b.totalSleep, Equals, 30)
}
