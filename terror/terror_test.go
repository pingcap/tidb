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

package terror

import (
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTErrorSuite{})

type testTErrorSuite struct {
}

func (s *testTErrorSuite) TestTError(c *C) {
	c.Assert(Parser.String(), Not(Equals), "")
	c.Assert(Optimizer.String(), Not(Equals), "")
	c.Assert(KV.String(), Not(Equals), "")
	c.Assert(Server.String(), Not(Equals), "")

	parserErr := Parser.New(ErrCode(1), "error 1")
	c.Assert(parserErr.Error(), Not(Equals), "")
	c.Assert(Parser.EqualClass(parserErr), IsTrue)
	c.Assert(Parser.NotEqualClass(parserErr), IsFalse)

	c.Assert(Parser.EqualCode(parserErr, ErrCode(1)), IsTrue)
	c.Assert(Parser.NotEqualCode(parserErr, ErrCode(1)), IsFalse)

	c.Assert(Parser.EqualCode(parserErr, ErrCode(2)), IsFalse)
	c.Assert(Optimizer.EqualClass(parserErr), IsFalse)

	optimizerErr := Optimizer.New(ErrCode(2), "abc %s", "def")
	c.Assert(Optimizer.EqualCode(optimizerErr, ErrCode(2)), IsTrue)

	c.Assert(Optimizer.EqualCode(errors.New("abc"), ErrCode(3)), IsFalse)
	c.Assert(Optimizer.EqualCode(nil, ErrCode(3)), IsFalse)
	c.Assert(Optimizer.EqualClass(errors.New("abc")), IsFalse)
	c.Assert(Optimizer.EqualClass(nil), IsFalse)
}
