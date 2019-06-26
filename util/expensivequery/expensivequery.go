// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expensivequery

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/util"
)

// Handle is the handler for expensive query.
type Handle struct {
	mu     sync.RWMutex
	exitCh chan struct{}
	sm     util.SessionManager
}

// NewExpensiveQueryHandle builds a new expensive query handler.
func NewExpensiveQueryHandle(exitCh chan struct{}) *Handle {
	return &Handle{exitCh: exitCh}
}

// SetSessionManager sets the SessionManager which is used to fetching the info
// of all active sessions.
func (eqh *Handle) SetSessionManager(sm util.SessionManager) *Handle {
	eqh.sm = sm
	return eqh
}

// Run starts a expensive query checker goroutine at the start time of the server.
func (eqh *Handle) Run() {
	// use 100ms as tickInterval temply, may use given interval or use defined variable later
	tickInterval := time.Millisecond * time.Duration(100)
	ticker := time.NewTicker(tickInterval)
	for {
		select {
		case <-ticker.C:
			processInfo := eqh.sm.ShowProcessList()
			for _, info := range processInfo {
				if len(info.Info) == 0 {
					continue
				}
				costTime := time.Since(info.Time)
				if info.MaxExecutionTime > 0 && costTime > time.Duration(info.MaxExecutionTime)*time.Millisecond {
					eqh.sm.Kill(info.ID, true)
				}
			}
		case <-eqh.exitCh:
			return
		}
	}
}

// Close closes the handle and release the background goroutine.
func (eqh *Handle) Close() {
	close(eqh.exitCh)
}
