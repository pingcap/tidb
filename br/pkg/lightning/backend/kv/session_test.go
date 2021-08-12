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

package kv

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
)

type kvSuite struct{}

var _ = Suite(&kvSuite{})

func TestKV(t *testing.T) {
	TestingT(t)
}

func (s *kvSuite) TestSession(c *C) {
	session := newSession(&SessionOptions{SQLMode: mysql.ModeNone, Timestamp: 1234567890})
	_, err := session.Txn(true)
	c.Assert(err, IsNil)
}
