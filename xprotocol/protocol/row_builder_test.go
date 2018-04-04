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

package protocol

import (
	"errors"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/arena"
)

type testProtocolTestSuite struct{}

var _ = Suite(&testProtocolTestSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (ts *testProtocolTestSuite) TestDump(c *C) {
	c.Parallel()
	b := dumpIntBinary(1)
	c.Assert(b, DeepEquals, []byte{0x2})
	b = dumpIntBinary(-1)
	c.Assert(b, DeepEquals, []byte{0x1})
	b = dumpUintBinary(1)
	c.Assert(b, DeepEquals, []byte{0x1})
	data := []byte("a")
	alloc := arena.NewAllocator(2)
	b = dumpStringBinary(data, alloc)
	c.Assert(b, DeepEquals, []byte("a\000"))
}

func (ts *testProtocolTestSuite) TestStrToXDecimal(c *C) {
	c.Parallel()
	ins := []string{"", "1.1.1", "1", "+1.1", "-1.1", "1.1", "-.1", ".1", "1.11", "a"}
	ierr := errors.New("invalid decimal")
	expect := []struct {
		b []byte
		e error
	}{
		{nil, nil},
		{nil, ierr},
		{[]byte{0x0, 0x1c}, nil},
		{[]byte{0x1, 0x11, 0xc0}, nil},
		{[]byte{0x1, 0x11, 0xd0}, nil},
		{[]byte{0x1, 0x11, 0xc0}, nil},
		{nil, ierr},
		{nil, ierr},
		{[]byte{0x2, 0x11, 0x1c}, nil},
		{nil, ierr},
	}
	for i, v := range ins {
		out, err := strToXDecimal(v)
		c.Assert(terror.ErrorEqual(expect[i].e, err), IsTrue, Commentf("%s : %d", v, i))
		c.Assert(expect[i].b, DeepEquals, out, Commentf("%s : %d", v, i))
	}
}
