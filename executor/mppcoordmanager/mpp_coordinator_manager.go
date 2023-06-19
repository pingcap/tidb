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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/executor/metrics"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// InstanceMPPCoordinatorManager is a local instance mpp coordinator manager
var InstanceMPPCoordinatorManager = newMPPCoordinatorManger()

const (
	detectFrequency = 5 * time.Minute
)

// CoordinatorUniqueID identifies a unique coordinator
type CoordinatorUniqueID struct {
	MPPQueryID kv.MPPQueryID
	GatherID   uint64
}

// MPPCoordinatorManager manages all mpp coordinator instances
type MPPCoordinatorManager struct {
	mu             sync.Mutex
	serverOn       bool
	serverAddr     string // empty if server is off
	coordinatorMap map[CoordinatorUniqueID]kv.MppCoordinator
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	maxLifeTime    uint64 // in Nano
}

// Run use a loop to detect and remove out of time Coordinators
func (m *MPPCoordinatorManager) Run() {
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.wg.Add(1)
	m.maxLifeTime = uint64(copr.TiFlashReadTimeoutUltraLong.Nanoseconds() + detectFrequency.Nanoseconds())
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(detectFrequency)
		defer ticker.Stop()
		for {
			select {
			case <-m.ctx.Done():
				return
			case <-ticker.C:
				m.detectAndDelete()
			}
		}
	}()
}

func (m *MPPCoordinatorManager) detectAndDelete() {
	var outOfTimeIDs []CoordinatorUniqueID
	m.mu.Lock()
	for id := range m.coordinatorMap {
		if uint64(time.Now().UnixNano()) >= id.MPPQueryID.QueryTs+m.maxLifeTime {
			outOfTimeIDs = append(outOfTimeIDs, id)
			delete(m.coordinatorMap, id)
		}
	}
	m.mu.Unlock()

	for _, deletedID := range outOfTimeIDs {
		metrics.MppCoordinatorCounterOverTimeCounter.Inc()
		logutil.BgLogger().Error("Delete MppCoordinator due to OutOfTime",
			zap.Uint64("QueryID", deletedID.MPPQueryID.LocalQueryID),
			zap.Uint64("QueryTs", deletedID.MPPQueryID.QueryTs))
	}
}

// Stop stop background goroutine
func (m *MPPCoordinatorManager) Stop() {
	m.cancel()
	m.wg.Wait()
}

// InitServerAddr init grpcServer address
func (m *MPPCoordinatorManager) InitServerAddr(serverOn bool, serverAddr string) {
	m.serverOn = serverOn
	if serverOn {
		m.serverAddr = serverAddr
	}
}

// GetServerAddr returns grpcServer address, empty serverAddr if server not on
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
		return errors.Errorf("Already added mpp coordinator: %d %d %d %d", coordID.MPPQueryID.QueryTs, coordID.MPPQueryID.LocalQueryID, coordID.MPPQueryID.ServerID, coordID.GatherID)
	}
	m.coordinatorMap[coordID] = mppCoord
	metrics.MppCoordinatorCounterTotalCounter.Inc()
	return nil
}

// Unregister is to unregister mpp coordinator
func (m *MPPCoordinatorManager) Unregister(coordID CoordinatorUniqueID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.coordinatorMap[coordID]
	delete(m.coordinatorMap, coordID)
	if exists {
		metrics.MppCoordinatorCounterTotalCounter.Desc()
	}
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
		GatherID:   request.Meta.GatherId,
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

// newMPPCoordinatorManger is to create a new mpp coordinator manager, only used to create global InstanceMPPCoordinatorManager
func newMPPCoordinatorManger() *MPPCoordinatorManager {
	return &MPPCoordinatorManager{coordinatorMap: make(map[CoordinatorUniqueID]kv.MppCoordinator)}
}
