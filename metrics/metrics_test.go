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

package metrics

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (s *testSuite) TestMetrics(c *C) {
	// Make sure it doesn't panic.
	PanicCounter.WithLabelValues(LabelDomain).Inc()
}

func (s *testSuite) TestRegisterMetrics(c *C) {
	// Make sure it doesn't panic.
	RegisterMetrics()
}

func (s *testSuite) TestRetLabel(c *C) {
	c.Assert(RetLabel(nil), Equals, opSucc)
	c.Assert(RetLabel(errors.New("test error")), Equals, opFailed)
}

func (s *testSuite) TestExecuteErrorToLabel(c *C) {
	c.Assert(ExecuteErrorToLabel(errors.New("test")), Equals, `unknown`)
	c.Assert(ExecuteErrorToLabel(terror.ErrResultUndetermined), Equals, `global:2`)
}
