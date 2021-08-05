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

package common_test

import (
	"errors"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
)

func TestCommon(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&onceErrorSuite{})

type onceErrorSuite struct{}

func (s *onceErrorSuite) TestOnceError(c *C) {
	var err common.OnceError

	c.Assert(err.Get(), IsNil)

	err.Set(nil)
	c.Assert(err.Get(), IsNil)

	e := errors.New("1")
	err.Set(e)
	c.Assert(err.Get(), Equals, e)

	e2 := errors.New("2")
	err.Set(e2)
	c.Assert(err.Get(), Equals, e) // e, not e2.

	err.Set(nil)
	c.Assert(err.Get(), Equals, e)

	ch := make(chan struct{})
	go func() {
		err.Set(nil)
		ch <- struct{}{}
	}()
	<-ch
	c.Assert(err.Get(), Equals, e)
}
