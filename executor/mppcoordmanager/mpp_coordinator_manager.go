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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
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
	coordinatorMap map[CoordinatorUniqueID]*kv.MppCoordinator
}

// Register is to register mpp coordinator
func (m *MPPCoordinatorManager) Register(coordID CoordinatorUniqueID, mppCoord *kv.MppCoordinator) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.coordinatorMap[coordID]
	if exists {
		return errors.Errorf("Already added mpp coordinator: %d %d %d %d", coordID.MPPQueryID.QueryTs, coordID.MPPQueryID.LocalQueryID, coordID.MPPQueryID.ServerID, coordID.GatherId)
	}
	m.coordinatorMap[coordID] = mppCoord
	return nil
}

// Unregister is to unregister mpp coordinator
func (m *MPPCoordinatorManager) Unregister(coordID CoordinatorUniqueID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.coordinatorMap, coordID)
	return nil
}

func (m *MPPCoordinatorManager) ReportStatus(CoordinatorUniqueID) error {
	return nil
}

// newMPPCoordinatorManger is to create a new mpp coordinator manager
func newMPPCoordinatorManger() *MPPCoordinatorManager {
	return &MPPCoordinatorManager{}
}
