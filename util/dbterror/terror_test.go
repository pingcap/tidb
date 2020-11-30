// Copyright 2020 PingCAP, Inc.
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

package dbterror

import (
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testkSuite{})

type testkSuite struct{}

func (s *testkSuite) TestErrorRedact(c *C) {
	original := errors.RedactLogEnabled.Load()
	errors.RedactLogEnabled.Store(true)
	defer func() { errors.RedactLogEnabled.Store(original) }()

	class := ErrClass{}
	err := class.NewStd(errno.ErrDupEntry).GenWithStackByArgs("sensitive_data", "no_sensitive")
	c.Assert(strings.Contains(err.Error(), "?"), IsTrue)
	c.Assert(strings.Contains(err.Error(), "no_sensitive"), IsTrue)
	c.Assert(strings.Contains(err.Error(), "sensitive_data"), IsFalse)
}
