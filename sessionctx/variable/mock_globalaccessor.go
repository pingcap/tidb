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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

// MockGlobalAccessor implements GlobalVarAccessor interface. it's used in tests
type MockGlobalAccessor struct {
	SessionVars *SessionVars // can be overwritten if needed for correctness.
	vals        map[string]string
	testSuite   bool
}

// NewMockGlobalAccessor implements GlobalVarAccessor interface.
func NewMockGlobalAccessor() *MockGlobalAccessor {
	return new(MockGlobalAccessor)
}

// NewMockGlobalAccessor4Tests creates a new MockGlobalAccessor for use in the testsuite.
// It behaves like the real GlobalVarAccessor and has a list of sessionvars.
// Because we use the real GlobalVarAccessor outside of tests,
// this is unsafe to use by default (performance regression).
func NewMockGlobalAccessor4Tests() *MockGlobalAccessor {
	tmp := new(MockGlobalAccessor)
	tmp.vals = make(map[string]string)
	tmp.testSuite = true
	// There's technically a test bug here where the sessionVars won't match
	// the session vars in the test which this MockGlobalAccessor is assigned to.
	// But if the test requires accurate sessionVars, it can do the following:
	//
	// vars := NewSessionVars()
	// mock := NewMockGlobalAccessor()
	// mock.SessionVars = vars
	// vars.GlobalVarsAccessor = mock

	tmp.SessionVars = NewSessionVars()

	// Set all sysvars to the default value
	for k, sv := range GetSysVars() {
		tmp.vals[k] = sv.Value
	}
	return tmp
}

// GetGlobalSysVar implements GlobalVarAccessor.GetGlobalSysVar interface.
func (m *MockGlobalAccessor) GetGlobalSysVar(name string) (string, error) {
	if !m.testSuite {
		v, ok := sysVars[name]
		if ok {
			return v.Value, nil
		}
		return "", nil
	}
	v, ok := m.vals[name]
	if ok {
		return v, nil
	}
	return "", ErrUnknownSystemVar.GenWithStackByArgs(name)
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (m *MockGlobalAccessor) SetGlobalSysVar(name string, value string) (err error) {
	sv := GetSysVar(name)
	if sv == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if value, err = sv.Validate(m.SessionVars, value, ScopeGlobal); err != nil {
		return err
	}
	if err = sv.SetGlobalFromHook(m.SessionVars, value, false); err != nil {
		return err
	}
	m.vals[name] = value
	return nil
}

// SetGlobalSysVarOnly implements GlobalVarAccessor.SetGlobalSysVarOnly interface.
func (m *MockGlobalAccessor) SetGlobalSysVarOnly(name string, value string) error {
	sv := GetSysVar(name)
	if sv == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	m.vals[name] = value
	return nil
}

// GetTiDBTableValue implements GlobalVarAccessor.GetTiDBTableValue interface.
func (m *MockGlobalAccessor) GetTiDBTableValue(name string) (string, error) {
	// add for test tidb_gc_max_wait_time validation
	if name == "tikv_gc_life_time" {
		sv := GetSysVar(TiDBGCLifetime)
		if sv == nil {
			panic("Get SysVar Failed")
		}
		return sv.Value, nil
	}
	panic("not supported")
}

// SetTiDBTableValue implements GlobalVarAccessor.SetTiDBTableValue interface.
func (m *MockGlobalAccessor) SetTiDBTableValue(name, value, comment string) error {
	panic("not supported")
}
