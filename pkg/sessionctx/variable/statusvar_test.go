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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
		return DefaultStatusVarScopeFlag
	}

	return scope
}

func (ms *mockStatistics) Stats(_ *SessionVars) (map[string]interface{}, error) {
	m := make(map[string]interface{}, len(specificStatusScopes))
	m[testStatus] = testStatusVal

	return m, nil
}

func TestStatusVar(t *testing.T) {
	ms := &mockStatistics{}
	RegisterStatistics(ms)

	scope := ms.GetScope(testStatus)
	require.Equal(t, DefaultStatusVarScopeFlag, scope)
	scope = ms.GetScope(testSessionStatus)
	require.Equal(t, ScopeSession, scope)

	vars, err := GetStatusVars(nil)
	require.NoError(t, err)
	v := &StatusVal{Scope: DefaultStatusVarScopeFlag, Value: testStatusVal}
	require.EqualValues(t, vars[testStatus], v)
}
