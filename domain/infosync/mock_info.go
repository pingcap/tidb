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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/util/versioninfo"
)

// MockGlobalServerInfoManagerEntry is a mock global ServerInfoManager entry.
var MockGlobalServerInfoManagerEntry = &MockGlobalServerInfoManager{
	inited: false,
}

// MockGlobalServerInfoManager manages serverInfos in Distributed unit tests
type MockGlobalServerInfoManager struct {
	infos  []*ServerInfo
	mu     sync.Mutex
	inited bool
}

// used to mock ServerInfo, then every mock server will have different port
var mockServerPort uint = 4000

// Inited check if MockGlobalServerInfoManager inited for Distributed unit tests
func (m *MockGlobalServerInfoManager) Inited() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inited
}

// Add one mock ServerInfo
func (m *MockGlobalServerInfoManager) Add(id string, serverIDGetter func() uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inited = true
	m.infos = append(m.infos, m.getServerInfo(id, serverIDGetter))
}

// Delete one mock ServerInfo by idx
func (m *MockGlobalServerInfoManager) Delete(idx int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if idx >= len(m.infos) || idx < 0 {
		return errors.New("server idx out of bound")
	}
	m.infos = append(m.infos[:idx], m.infos[idx+1:]...)
	return nil
}

// GetAllServerInfo return all serverInfo in a map
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
		Port:           mockServerPort,
		StatusPort:     cfg.Status.StatusPort,
		Lease:          cfg.Lease,
		BinlogStatus:   binloginfo.GetStatus().String(),
		StartTimestamp: time.Now().Unix(),
		Labels:         cfg.Labels,
		ServerIDGetter: serverIDGetter,
	}

	mockServerPort++

	info.Version = mysql.ServerVersion
	info.GitHash = versioninfo.TiDBGitHash
	return info
}
