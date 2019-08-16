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

package variable

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSysVarSuite{})

type testSysVarSuite struct {
}

func (*testSysVarSuite) TestSysVar(c *C) {
	f := GetSysVar("autocommit")
	c.Assert(f, NotNil)

	f = GetSysVar("wrong-var-name")
	c.Assert(f, IsNil)

	f = GetSysVar("explicit_defaults_for_timestamp")
	c.Assert(f, NotNil)
	c.Assert(f.Value, Equals, "1")

	f = GetSysVar("port")
	c.Assert(f, NotNil)
	c.Assert(f.Value, Equals, "4000")

	f = GetSysVar("tidb_low_resolution_tso")
	c.Assert(f.Value, Equals, "0")

	f = GetSysVar("tidb_replica_read")
	c.Assert(f.Value, Equals, "leader")
}

func (*testSysVarSuite) TestTxnMode(c *C) {
	seVar := NewSessionVars()
	c.Assert(seVar, NotNil)
	c.Assert(seVar.TxnMode, Equals, "")
	err := seVar.setTxnMode("pessimistic")
	c.Assert(err, IsNil)
	err = seVar.setTxnMode("optimistic")
	c.Assert(err, IsNil)
	err = seVar.setTxnMode("")
	c.Assert(err, IsNil)
	err = seVar.setTxnMode("something else")
	c.Assert(err, NotNil)
}
