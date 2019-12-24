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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
)

var _ = Suite(&diagnosticsSuite{})

type diagnosticsSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *diagnosticsSuite) SetUpSuite(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
}

func (s *diagnosticsSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *diagnosticsSuite) TestInspectionResult(c *C) {

}
