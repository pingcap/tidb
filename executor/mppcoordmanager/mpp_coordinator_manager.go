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

package mppcoordmanager

import (
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// InstanceMPPCoordinatorManager is a local instance mpp coordinator manager
var InstanceMPPCoordinatorManager = newMPPCoordinatorManger()

// CoordinatorUniqueID identifies a unique coordinator
type CoordinatorUniqueID struct {
	MPPQueryID kv.MPPQueryID
	GatherId   uint64
}

// MPPCoordinatorManager manages all mpp coordinator instances
type MPPCoordinatorManager struct {
	mu             sync.Mutex
	serverOn       bool
	serverAddr     string
	coordinatorMap map[CoordinatorUniqueID]kv.MppCoordinator
}

// InitServerAddr init grpcServer address
func (m *MPPCoordinatorManager) InitServerAddr(serverOn bool, serverAddr string) {
	m.serverOn = serverOn
	if serverOn {
		m.serverAddr = serverAddr
	}
}

func (m *MPPCoordinatorManager) GetServerAddr() (serverOn bool, serverAddr string) {
	return m.serverOn, m.serverAddr
}

// Register is to register mpp coordinator
// TODO: add metric to track coordinator counter
func (m *MPPCoordinatorManager) Register(coordID CoordinatorUniqueID, mppCoord kv.MppCoordinator) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.coordinatorMap[coordID]
	if exists {
		return errors.Errorf("Already added mpp coordinator: %d %d %d %d", coordID.MPPQueryID.QueryTs, coordID.MPPQueryID.LocalQueryID, coordID.MPPQueryID.ServerID, coordID.GatherId)
	}
	m.coordinatorMap[coordID] = mppCoord
	logutil.BgLogger().Info("Register track mpp coordinator instances", zap.Int("CoordCounter", len(m.coordinatorMap)))
	return nil
}

// Unregister is to unregister mpp coordinator
func (m *MPPCoordinatorManager) Unregister(coordID CoordinatorUniqueID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.coordinatorMap, coordID)
	logutil.BgLogger().Info("Unregister track mpp coordinator instances", zap.Int("CoordCounter", len(m.coordinatorMap)))
	return nil
}

// ReportStatus reports mpp task execution status to specific coordinator
func (m *MPPCoordinatorManager) ReportStatus(request *mpp.ReportTaskStatusRequest) *mpp.ReportTaskStatusResponse {
	mppQueryID := kv.MPPQueryID{
		QueryTs:      request.Meta.QueryTs,
		LocalQueryID: request.Meta.LocalQueryId,
		ServerID:     request.Meta.ServerId,
	}
	coordID := CoordinatorUniqueID{
		MPPQueryID: mppQueryID,
		GatherId:   request.Meta.GatherId,
	}
	m.mu.Lock()
	coord, exists := m.coordinatorMap[coordID]
	m.mu.Unlock()
	resp := new(mpp.ReportTaskStatusResponse)
	if !exists {
		resp.Error = &mpp.Error{MppVersion: request.Meta.MppVersion, Msg: fmt.Sprintf("MppTask not exists, taskID: %d", request.Meta.TaskId)}
		return resp
	}
	err := coord.ReportStatus(kv.ReportStatusRequest{Request: request})
	if err != nil {
		resp.Error = &mpp.Error{MppVersion: request.Meta.MppVersion, Msg: err.Error()}
		return resp
	}
	return resp
}

// newMPPCoordinatorManger is to create a new mpp coordinator manager
func newMPPCoordinatorManger() *MPPCoordinatorManager {
	return &MPPCoordinatorManager{coordinatorMap: make(map[CoordinatorUniqueID]kv.MppCoordinator)}
}
