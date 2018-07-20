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

package variable

type mockGlobalAccessor struct {
	vars map[string]string
}

// NewMockGlobalAccessor implements GlobalVarAccessor interface.
func NewMockGlobalAccessor() *mockGlobalAccessor {
	m := &mockGlobalAccessor{
		vars: make(map[string]string),
	}
	for name, val := range SysVars {
		m.vars[name] = val.Value
	}
	return m
}

// GetGlobalSysVar implements GlobalVarAccessor.GetGlobalSysVar interface.
func (m *mockGlobalAccessor) GetGlobalSysVar(name string) (string, error) {
	return m.vars[name], nil
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (m *mockGlobalAccessor) SetGlobalSysVar(name string, value string) error {
	m.vars[name] = value
	return nil
}

// GetAllSysVars implements GlobalVarAccessor.GetAllSysVars interface.
func (m *mockGlobalAccessor) GetAllSysVars() (map[string]string, error) {
	return m.vars, nil
}
