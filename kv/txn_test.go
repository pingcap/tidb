// Copyright 2015 PingCAP, Inc.
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

package kv

import (
	"errors"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/util/testleak"
)

var _ = Suite(&testTxnSuite{})

type testTxnSuite struct {
}

func (s *testTxnSuite) SetUpTest(c *C) {
}

func (s *testTxnSuite) TearDownTest(c *C) {
}

func (s *testTxnSuite) TestBackOff(c *C) {
	defer testleak.AfterTest(c)()
	mustBackOff(c, 1, 2)
	mustBackOff(c, 2, 4)
	mustBackOff(c, 3, 8)
	mustBackOff(c, 100000, 100)
}

func mustBackOff(c *C, cnt uint, sleep int) {
	c.Assert(BackOff(cnt), LessEqual, sleep*int(time.Millisecond))
}

func (s *testTxnSuite) TestRetryExceedCountError(c *C) {
	defer testleak.AfterTest(c)()
	defer func(cnt uint) {
		maxRetryCnt = cnt
	}(maxRetryCnt)

	maxRetryCnt = 5
	err := RunInNewTxn(&mockStorage{}, true, func(txn Transaction) error {
		return nil
	})
	c.Assert(err, NotNil)

	err = RunInNewTxn(&mockStorage{}, true, func(txn Transaction) error {
		return ErrTxnRetryable
	})
	c.Assert(err, NotNil)

	err = RunInNewTxn(&mockStorage{}, true, func(txn Transaction) error {
		return errors.New("do not retry")
	})
	c.Assert(err, NotNil)

	var cfg InjectionConfig
	err1 := errors.New("foo")
	cfg.SetGetError(err1)
	cfg.SetCommitError(err1)
	storage := NewInjectedStore(NewMockStorage(), &cfg)
	err = RunInNewTxn(storage, true, func(txn Transaction) error {
		return nil
	})
	c.Assert(err, NotNil)
}

func (s *testTxnSuite) TestBasicFunc(c *C) {
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
