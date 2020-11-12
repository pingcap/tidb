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

package mockstore

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testSuite struct{}

func (s testSuite) SetUpSuite(c *C) {}

var _ = Suite(testSuite{})

func (s testSuite) TestConfig(c *C) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches = config.TxnLocalLatches{
			Enabled:  true,
			Capacity: 10240,
		}
	})

	type LatchEnableChecker interface {
		IsLatchEnabled() bool
	}

	var driver MockTiKVDriver
	store, err := driver.Open("mocktikv://")
	c.Assert(err, IsNil)
	c.Assert(store.(LatchEnableChecker).IsLatchEnabled(), IsTrue)
	store.Close()

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches = config.TxnLocalLatches{
			Enabled:  false,
			Capacity: 10240,
		}
	})
	store, err = driver.Open("mocktikv://")
	c.Assert(err, IsNil)
	c.Assert(store.(LatchEnableChecker).IsLatchEnabled(), IsFalse)
	store.Close()

	store, err = driver.Open(":")
	c.Assert(err, NotNil)
	if store != nil {
		store.Close()
	}

	store, err = driver.Open("faketikv://")
	c.Assert(err, NotNil)
	if store != nil {
		store.Close()
	}
}
