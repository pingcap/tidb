// Copyright 2025 PingCAP, Inc.
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

package testcase

import (
	"encoding/json"
	"io"
	"os"
	"sync"
)

// Case represents a test case.
type Case struct {
	SQL  string `json:"sql"`
	Args []any  `json:"args"`

	Pass    bool   `json:"pass"`
	Known   bool   `json:"known"`
	Comment string `json:"comment"`
}

// Manager manages the test cases.
type Manager struct {
	mu sync.Mutex

	path string

	cases map[string][]*Case
}

// Open creates a new Manager on a given path.
func Open(path string) (*Manager, error) {
	m := &Manager{
		path:  path,
		cases: make(map[string][]*Case),
	}

	err := m.loadFromFile()
	if err != nil {
		return nil, err
	}
	return m, nil
}

// loadFromFile loads the test cases from the file.
func (m *Manager) loadFromFile() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, err := os.Open(m.path)
	if err != nil {
		return err
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	err = json.Unmarshal(bytes, &m.cases)
	if err != nil {
		return err
	}

	return nil
}

// Save saves the test cases to the file.
func (m *Manager) Save() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// `MarshalIdent` to make it more readable
	bytes, err := json.MarshalIndent(m.cases, "", "  ")
	if err != nil {
		return err
	}

	err = os.WriteFile(m.path, bytes, 0644)
	if err != nil {
		return err
	}

	return nil
}

// AppendCase appends a test case to the manager.
func (m *Manager) AppendCase(group string, c Case) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.cases[group]; !ok {
		m.cases[group] = make([]*Case, 0)
	}

	m.cases[group] = append(m.cases[group], &c)
}

// ExistCases returns the test cases in a group.
func (m *Manager) ExistCases(group string) []*Case {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.cases[group]
}

// AllGroups returns all the groups.
func (m *Manager) AllGroups() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]string, 0, len(m.cases))
	for g := range m.cases {
		result = append(result, g)
	}
	return result
}
