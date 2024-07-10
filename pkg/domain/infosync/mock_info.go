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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/binloginfo"
	"github.com/pingcap/tidb/pkg/util/versioninfo"
)

// MockGlobalServerInfoManagerEntry is a mock global ServerInfoManager entry.
var MockGlobalServerInfoManagerEntry = &MockGlobalServerInfoManager{
	mockServerPort: 4000,
}

// MockGlobalServerInfoManager manages serverInfos in Distributed unit tests.
type MockGlobalServerInfoManager struct {
	infos          []*ServerInfo
	mu             sync.Mutex
	mockServerPort uint // used to mock ServerInfo, then every mock server will have different port
}

// Add one mock ServerInfo.
func (m *MockGlobalServerInfoManager) Add(id string, serverIDGetter func() uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.infos = append(m.infos, m.getServerInfo(id, serverIDGetter))
}

// Delete one mock ServerInfo by idx.
func (m *MockGlobalServerInfoManager) Delete(idx int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if idx >= len(m.infos) || idx < 0 {
		return errors.New("server idx out of bound")
	}
	m.infos = append(m.infos[:idx], m.infos[idx+1:]...)
	return nil
}

// DeleteByExecID delete ServerInfo by execID.
func (m *MockGlobalServerInfoManager) DeleteByExecID(execID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < len(m.infos); i++ {
		name := fmt.Sprintf("%s:%d", m.infos[i].IP, m.infos[i].Port)
		if name == execID {
			m.infos = append(m.infos[:i], m.infos[i+1:]...)
			break
		}
	}
}

// GetAllServerInfo return all serverInfo in a map.
func (m *MockGlobalServerInfoManager) GetAllServerInfo() map[string]*ServerInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	allInfo := make(map[string]*ServerInfo)
	for _, info := range m.infos {
		allInfo[info.ID] = info
	}
	return allInfo
}

// getServerInfo gets self tidb server information.
func (m *MockGlobalServerInfoManager) getServerInfo(id string, serverIDGetter func() uint64) *ServerInfo {
	cfg := config.GetGlobalConfig()

	// TODO: each mock server can have different config
	info := &ServerInfo{
		ID:             id,
		IP:             cfg.AdvertiseAddress,
		Port:           m.mockServerPort,
		StatusPort:     cfg.Status.StatusPort,
		Lease:          cfg.Lease,
		BinlogStatus:   binloginfo.GetStatus().String(),
		StartTimestamp: time.Now().Unix(),
		Labels:         cfg.Labels,
		ServerIDGetter: serverIDGetter,
	}

	m.mockServerPort++

	info.Version = mysql.ServerVersion
	info.GitHash = versioninfo.TiDBGitHash
	return info
}

// Close reset MockGlobalServerInfoManager.
func (m *MockGlobalServerInfoManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mockServerPort = 4000
	m.infos = m.infos[:0]
}
