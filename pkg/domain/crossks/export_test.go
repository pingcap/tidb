// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crossks

// Get returns the session manager for the specified keyspace and whether it exists.
func (m *Manager) Get(ks string) (*SessionManager, bool) {
	return m.get(ks)
}

// CloseKS closes the session manager for the specified keyspace in tests.
func (m *Manager) CloseKS(targetKS string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.runtimes[targetKS]; ok {
		entry.sessMgr.close()
		delete(m.runtimes, targetKS)
	}
}
