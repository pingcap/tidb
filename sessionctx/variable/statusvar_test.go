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
	ms *mockStatistics
}

func (s *testStatusVarSuite) SetUpSuite(c *C) {
	s.ms = &mockStatistics{}
	RegisterStatistics(s.ms)
}

// mockStatistics represents mocked statistics.
type mockStatistics struct{}

const (
	testStatus        = "test_status"
	testSessionStatus = "test_session_status"
	testStatusVal     = "test_status_val"
)

var specificStatusScopes = map[string]ScopeFlag{
	testSessionStatus: ScopeSession,
}

func (ms *mockStatistics) GetScope(status string) ScopeFlag {
	scope, ok := specificStatusScopes[status]
	if !ok {
		return DefaultScopeFlag
	}

	return scope
}

func (ms *mockStatistics) Stats() (map[string]interface{}, error) {
	m := make(map[string]interface{}, len(specificStatusScopes))
	m[testStatus] = testStatusVal

	return m, nil
}

func (s *testStatusVarSuite) TestStatusVar(c *C) {
	scope := s.ms.GetScope(testStatus)
	c.Assert(scope, Equals, DefaultScopeFlag)
	scope = s.ms.GetScope(testSessionStatus)
	c.Assert(scope, Equals, ScopeSession)

	vars, err := GetStatusVars()
	c.Assert(err, IsNil)
	v := &StatusVal{Scope: DefaultScopeFlag, Value: testStatusVal}
	c.Assert(v, DeepEquals, vars[testStatus])
}
