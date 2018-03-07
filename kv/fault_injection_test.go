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

package kv_test

import (
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
)

type testFaultInjectionSuite struct{}

var _ = Suite(testFaultInjectionSuite{})

func (s testFaultInjectionSuite) TestFaultInjectionBasic(c *C) {
	var cfg kv.InjectionConfig
	err := errors.New("foo")
	cfg.SetGetError(err)

	storage := kv.NewInjectedStore(kv.NewMockStorage(), &cfg)
	txn, err := storage.Begin()
	c.Assert(err, IsNil)
	_, err = storage.BeginWithStartTS(0)
	c.Assert(err, IsNil)
	ver := kv.Version{Ver: 1}
	snap, err := storage.GetSnapshot(ver)
	c.Assert(err, IsNil)
	b, err := txn.Get([]byte{'a'})
	c.Assert(err.Error(), Equals, errors.New("foo").Error())
	c.Assert(b, IsNil)
	b, err = snap.Get([]byte{'a'})
	c.Assert(err.Error(), Equals, errors.New("foo").Error())
	c.Assert(b, IsNil)
}
