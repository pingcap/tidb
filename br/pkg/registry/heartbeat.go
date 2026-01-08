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

package registry

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	UpdateHeartbeatSQLTemplate = `
		UPDATE %s.%s
		SET last_heartbeat_time = FROM_UNIXTIME(%%?)
		WHERE id = %%?`

	// defaultHeartbeatIntervalSeconds is the default interval in seconds between heartbeat updates
	defaultHeartbeatIntervalSeconds = 60
)

// UpdateHeartbeat updates the last_heartbeat_time timestamp for a task
func (r *Registry) UpdateHeartbeat(ctx context.Context, restoreID uint64) error {
	currentTime := time.Now().UTC().Unix()
	updateSQL := fmt.Sprintf(UpdateHeartbeatSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)

	if err := r.heartbeatSession.ExecuteInternal(ctx, updateSQL, currentTime, restoreID); err != nil {
		return errors.Annotatef(err, "failed to update heartbeat for task %d", restoreID)
	}

	log.Debug("updated task heartbeat",
		zap.Uint64("restore_id", restoreID),
		zap.Int64("timestamp", currentTime))

	return nil
}

// HeartbeatManager handles periodic heartbeat updates for a restore task
// it only updates the restore task but will not remove any stalled tasks, the purpose of this logic is to provide
// some insights to user of the task status
type HeartbeatManager struct {
	registry  *Registry
	restoreID uint64
	interval  time.Duration
	stopCh    chan struct{}
	doneCh    chan struct{}
}

// NewHeartbeatManager creates a new heartbeat manager for the given restore task
func NewHeartbeatManager(registry *Registry, restoreID uint64) *HeartbeatManager {
	return &HeartbeatManager{
		registry:  registry,
		restoreID: restoreID,
		interval:  time.Duration(defaultHeartbeatIntervalSeconds) * time.Second,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// Start begins the heartbeat background process
func (m *HeartbeatManager) Start(ctx context.Context) {
	go func() {
		defer close(m.doneCh)

		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()

		// send an initial heartbeat
		if err := m.registry.UpdateHeartbeat(ctx, m.restoreID); err != nil {
			log.Warn("failed to send initial heartbeat",
				zap.Uint64("restore_id", m.restoreID),
				zap.Error(err))
		}

		for {
			select {
			case <-ticker.C:
				if err := m.registry.UpdateHeartbeat(ctx, m.restoreID); err != nil {
					log.Warn("failed to update heartbeat",
						zap.Uint64("restore_id", m.restoreID),
						zap.Error(err))
				}
			case <-m.stopCh:
				return
			case <-ctx.Done():
				log.Warn("heartbeat manager context done",
					zap.Uint64("restore_id", m.restoreID),
					zap.Error(ctx.Err()))
				return
			}
		}
	}()
}

// Stop ends the heartbeat background process
func (m *HeartbeatManager) Stop() {
	close(m.stopCh)
	<-m.doneCh // Wait for goroutine to exit
	log.Info("stopped heartbeat manager")
}
