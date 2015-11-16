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
	ms *mockStatist
}

func (s *testStatusVarSuite) SetUpSuite(c *C) {
	s.ms = &mockStatist{}
	RegisterStatist(s.ms)
}

// mockStatist represents mocked statist.
type mockStatist struct{}

const (
	testStatus    = "test_status"
	testStatusVal = "test_status_val"
)

var defaultStatusScopes = map[string]ScopeFlag{
	testStatus: ScopeGlobal | ScopeSession,
}

func (ms *mockStatist) GetDefaultStatusScopes() map[string]ScopeFlag {
	return defaultStatusScopes
}

func (ms *mockStatist) Stat() (map[string]*StatusVal, error) {
	m := make(map[string]*StatusVal, len(defaultStatusScopes))
	m[testStatus] = FillStatusVal(testStatus, testStatusVal)

	return m, nil
}

func (s *testStatusVarSuite) testStatusVar(c *C) {
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
