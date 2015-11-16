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
	. "github.com/pingcap/check"
)

var _ = Suite(&testStatusVarSuite{})

type testStatusVarSuite struct {
	ms *MockStatist
}

func (s *testStatusVarSuite) SetUpSuite(c *C) {
	s.ms = &MockStatist{}
	RegisterStatist(s.ms)
}

// MockStatist represents mocked statist.
type MockStatist struct{}

const (
	TestStatusSessionScope    = "test_status_session_scope"
	TestStatusGlobalScope     = "test_status_global_scope"
	TestStatusBothScopes      = "test_status_both_scope"
	TestStatusValSessionScope = "test_status_val_session_scope"
	TestStatusValGlobalScope  = "test_status_val_global_scope"
	TestStatusValBothScope    = "test_status_val_both_scope"
)

var defaultStatusScopes map[string]ScopeFlag = map[string]ScopeFlag{
	TestStatusSessionScope: ScopeSession,
	TestStatusGlobalScope:  ScopeGlobal,
	TestStatusBothScopes:   ScopeGlobal | ScopeSession,
}

func (ms *MockStatist) GetDefaultStatusScopes() map[string]ScopeFlag {
	return defaultStatusScopes
}

func (ms *MockStatist) Stat() (map[string]*StatusVal, error) {
	m := make(map[string]*StatusVal, len(defaultStatusScopes))
	m[TestStatusSessionScope] = FillStatusVal(TestStatusSessionScope, TestStatusValSessionScope)
	m[TestStatusGlobalScope] = FillStatusVal(TestStatusGlobalScope, TestStatusValGlobalScope)
	m[TestStatusBothScopes] = FillStatusVal(TestStatusBothScopes, TestStatusValBothScope)

	return m, nil
}
func (s *testStatusVarSuite) TestStatusVar(c *C) {
	scopes := s.ms.GetDefaultStatusScopes()
	c.Assert(scopes, NotNil)

	vars, err := GetStatusVars()
	c.Assert(err, IsNil)
	for status, val := range vars {
		scope, ok := scopes[status]
		c.Assert(ok, IsTrue)
		c.Assert(scope, Equals, val.Scope)

		v := GetStatusVar(status)
		c.Assert(v, Equals, val)
	}

	v := GetStatusVar("wrong-var-name")
	c.Assert(v, IsNil)
}
