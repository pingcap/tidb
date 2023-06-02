// Copyright 2023 PingCAP, Inc.
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

package infosync

import (
	"sync"

	"github.com/pingcap/errors"
)

// MockGlobalServerInfoManagerEntry is a mock global ServerInfoManager entry.
var MockGlobalServerInfoManagerEntry = &MockGlobalServerInfoManager{}

// Manager serverInfos in Distributed unit tests
type MockGlobalServerInfoManager struct {
	infos []*ServerInfo
	mu    sync.Mutex
}

func (m *MockGlobalServerInfoManager) Add(id string, serverIDGetter func() uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.infos = append(m.infos, getServerInfo(id, serverIDGetter))
}

func (m *MockGlobalServerInfoManager) Delete(idx int) error {
	if idx >= len(m.infos) || idx < 0 {
		return errors.New("server idx out of bound")
	}
	m.infos = append(m.infos[:idx], m.infos[idx+1:]...)
	return nil
}

func (m *MockGlobalServerInfoManager) GetAllServerInfo() map[string]*ServerInfo {
	allInfo := make(map[string]*ServerInfo)
	for _, info := range m.infos {
		allInfo[info.ID] = getServerInfo(info.ID, info.ServerIDGetter)
	}
	return allInfo
}
