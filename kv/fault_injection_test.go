// Copyright 2017 PingCAP, Inc.
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
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

type testFaultInjectionSuite struct{}

var _ = Suite(testFaultInjectionSuite{})

func (s testFaultInjectionSuite) TestFaultInjectionBasic(c *C) {
	var cfg InjectionConfig
	err1 := errors.New("foo")
	cfg.SetGetError(err1)
	cfg.SetCommitError(err1)

	storage := NewInjectedStore(newMockStorage(), &cfg)
	txn, err := storage.Begin()
	c.Assert(err, IsNil)
	_, err = storage.BeginWithOption(TransactionOption{}.SetTxnScope(oracle.GlobalTxnScope).SetStartTs(0))
	c.Assert(err, IsNil)
	ver := Version{Ver: 1}
	snap := storage.GetSnapshot(ver)
	b, err := txn.Get(context.TODO(), []byte{'a'})
	c.Assert(err.Error(), Equals, err1.Error())
	c.Assert(b, IsNil)
	b, err = snap.Get(context.TODO(), []byte{'a'})
	c.Assert(err.Error(), Equals, err1.Error())
	c.Assert(b, IsNil)

	bs, err := snap.BatchGet(context.Background(), nil)
	c.Assert(err.Error(), Equals, err1.Error())
	c.Assert(bs, IsNil)

	bs, err = txn.BatchGet(context.Background(), nil)
	c.Assert(err.Error(), Equals, err1.Error())
	c.Assert(bs, IsNil)

	err = txn.Commit(context.Background())
	c.Assert(err.Error(), Equals, err1.Error())

	cfg.SetGetError(nil)
	cfg.SetCommitError(nil)

	storage = NewInjectedStore(newMockStorage(), &cfg)
	txn, err = storage.Begin()
	c.Assert(err, IsNil)
	snap = storage.GetSnapshot(ver)

	b, err = txn.Get(context.TODO(), []byte{'a'})
	c.Assert(err, IsNil)
	c.Assert(b, IsNil)

	bs, err = txn.BatchGet(context.Background(), nil)
	c.Assert(err, IsNil)
	c.Assert(bs, IsNil)

	b, err = snap.Get(context.TODO(), []byte{'a'})
	c.Assert(terror.ErrorEqual(ErrNotExist, err), IsTrue)
	c.Assert(b, IsNil)

	bs, err = snap.BatchGet(context.Background(), []Key{[]byte("a")})
	c.Assert(err, IsNil)
	c.Assert(len(bs), Equals, 0)

	err = txn.Commit(context.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ErrTxnRetryable), IsTrue)
}
