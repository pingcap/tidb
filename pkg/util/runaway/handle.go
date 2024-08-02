// Copyright 2024 PingCAP, Inc.
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

package runaway

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Handle is the handler for expensive query.
type Handle struct {
	exitCh chan struct{}
	sm     atomic.Value
}

// NewRunawayHandle builds a new expensive query handler.
func NewRunawayHandle(exitCh chan struct{}) *Handle {
	return &Handle{exitCh: exitCh}
}

// SetSessionManager sets the SessionManager which is used to fetching the info
// of all active sessions.
func (eqh *Handle) SetSessionManager(sm util.SessionManager) *Handle {
	eqh.sm.Store(sm)
	return eqh
}

// Run starts a runaway checker goroutine at the start time of the server.
func (eqh *Handle) Run() {
	// use 100ms as tickInterval temporarily, may use given interval or use defined variable later
	tickInterval := time.Millisecond * time.Duration(100)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	sm := eqh.sm.Load().(util.SessionManager)
	for {
		select {
		case <-ticker.C:
			processInfo := sm.ShowProcessList()
			for _, info := range processInfo {
				if len(info.Info) == 0 {
					continue
				}
				if info.RunawayChecker != nil {
					if info.Plan == nil {
						continue
					}
					id := info.Plan.(base.Plan).ID()

					var ru *execdetails.RURuntimeStats
					_, groupRss := info.RuntimeStatsColl.GetRootStats(id).MergeStats()
					for _, rss := range groupRss {
						if rss.Tp() == execdetails.TpRURuntimeStats {
							ru = rss.(*execdetails.RURuntimeStats)
							logutil.BgLogger().Info("CHECK!!! ru", zap.Any("ru", ru))
							break
						}
					}

					processKeys := info.RuntimeStatsColl.GetCopStats(id).GetProcessKeys()
					now := time.Now()
					if info.RunawayChecker.CheckKillAction(now, ru.RUDetails, processKeys) {
						logutil.BgLogger().Warn("runaway query exceed limit", zap.Duration("costTime", now.Sub(info.Time)), zap.String("groupName", info.ResourceGroupName),
							zap.String("rule", info.RunawayChecker.Rule()), zap.String("processInfo", info.String()))
						sm.Kill(info.ID, true, false)
					}
				}
			}
		case <-eqh.exitCh:
			return
		}
	}
}
