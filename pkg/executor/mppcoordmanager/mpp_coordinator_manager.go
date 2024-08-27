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
	"github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/util/logutil"
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
				m.detectAndDelete(uint64(time.Now().UnixNano()))
			}
		}
	}()
}

func (m *MPPCoordinatorManager) detectAndDelete(nowTs uint64) {
	var outOfTimeIDs []CoordinatorUniqueID
	m.mu.Lock()
	for id, coord := range m.coordinatorMap {
		// Mpp queries may run forever if it continuously sends data to coordinator, thus we need check IsClosed here
		if nowTs > id.MPPQueryID.QueryTs+m.maxLifeTime && coord.IsClosed() {
			outOfTimeIDs = append(outOfTimeIDs, id)
			delete(m.coordinatorMap, id)
		}
	}
	m.mu.Unlock()

	for _, deletedID := range outOfTimeIDs {
		metrics.MppCoordinatorStatsOverTimeNumber.Inc()
		logutil.BgLogger().Error("Delete MppCoordinator due to OutOfTime",
			zap.Uint64("QueryID", deletedID.MPPQueryID.LocalQueryID),
			zap.Uint64("QueryTs", deletedID.MPPQueryID.QueryTs))
	}
}

// Stop stops background goroutine
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
func (m *MPPCoordinatorManager) Register(coordID CoordinatorUniqueID, mppCoord kv.MppCoordinator) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.coordinatorMap[coordID]
	if exists {
		return errors.Errorf("Mpp coordinator already registered: %d %d %d %d", coordID.MPPQueryID.QueryTs, coordID.MPPQueryID.LocalQueryID, coordID.MPPQueryID.ServerID, coordID.GatherID)
	}
	m.coordinatorMap[coordID] = mppCoord
	metrics.MppCoordinatorStatsTotalRegisteredNumber.Inc()
	metrics.MppCoordinatorStatsActiveNumber.Inc()
	return nil
}

// Unregister is to unregister mpp coordinator
func (m *MPPCoordinatorManager) Unregister(coordID CoordinatorUniqueID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.coordinatorMap[coordID]
	delete(m.coordinatorMap, coordID)
	if exists {
		metrics.MppCoordinatorStatsActiveNumber.Dec()
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

	// Following logic is not lock protected, thus shouldn't change any state outside coordinator itself
	resp := new(mpp.ReportTaskStatusResponse)
	if !exists {
		// It is expected that coordinator has chance to be unregistered before ReportStatus, no log needed here
		resp.Error = &mpp.Error{MppVersion: request.Meta.MppVersion, Msg: "MppCoordinator not exists"}
		return resp
	}
	err := coord.ReportStatus(kv.ReportStatusRequest{Request: request})
	if err != nil {
		resp.Error = &mpp.Error{MppVersion: request.Meta.MppVersion, Msg: err.Error()}
		logutil.BgLogger().Warn(fmt.Sprintf("Mpp coordinator handles ReportMPPTaskStatus met error: %s", err.Error()),
			zap.Uint64("QueryID", coordID.MPPQueryID.LocalQueryID),
			zap.Uint64("QueryTs", coordID.MPPQueryID.QueryTs),
			zap.Int64("TaskID", request.Meta.TaskId))
		return resp
	}
	return resp
}

// newMPPCoordinatorManger is to create a new mpp coordinator manager, only used to create global InstanceMPPCoordinatorManager
func newMPPCoordinatorManger() *MPPCoordinatorManager {
	return &MPPCoordinatorManager{coordinatorMap: make(map[CoordinatorUniqueID]kv.MppCoordinator)}
}
