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

package errors2

import (
	"errors"
	"testing"

	jerrors "github.com/juju/errors"
	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testErrors2Suite{})

type testErrors2Suite struct {
}

func (s *testErrors2Suite) TestErrorEuqal(c *C) {
	e1 := errors.New("test error")
	c.Assert(e1, NotNil)

	e2 := jerrors.Trace(e1)
	c.Assert(e2, NotNil)

	e3 := jerrors.Trace(e2)
	c.Assert(e3, NotNil)

	c.Assert(jerrors.Cause(e2), Equals, e1)
	c.Assert(jerrors.Cause(e3), Equals, e1)
	c.Assert(jerrors.Cause(e2), Equals, jerrors.Cause(e3))

	e4 := jerrors.New("test error")
	c.Assert(jerrors.Cause(e4), Not(Equals), e1)

	e5 := jerrors.Errorf("test error")
	c.Assert(jerrors.Cause(e5), Not(Equals), e1)

	c.Assert(ErrorEqual(e1, e2), IsTrue)
	c.Assert(ErrorEqual(e1, e3), IsTrue)
	c.Assert(ErrorEqual(e1, e4), IsTrue)
	c.Assert(ErrorEqual(e1, e5), IsTrue)

	var e6 error

	c.Assert(ErrorEqual(nil, nil), IsTrue)
	c.Assert(ErrorNotEqual(e1, e6), IsTrue)
}
