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

package tikv

import (
	"context"
	"errors"
	. "github.com/pingcap/check"
)

type testBackoffSuite struct {
	OneByOneSuite
	store *tikvStore
}

var _ = Suite(&testBackoffSuite{})

func (s *testBackoffSuite) SetUpTest(c *C) {
	s.store = NewTestStore(c).(*tikvStore)
}

func (s *testBackoffSuite) TearDownTest(c *C) {
	s.store.Close()
}

func (s *testBackoffSuite) TestBackoffWithMax(c *C) {
	b := NewBackoffer(context.TODO(), 2000)
	err := b.BackoffWithMaxSleep(boTxnLockFast, 30, errors.New("test"))
	c.Assert(err, IsNil)
	c.Assert(b.totalSleep, Equals, 30)
}
