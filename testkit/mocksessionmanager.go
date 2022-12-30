// Copyright 2022 PingCAP, Inc.
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

package testkit

import (
	"crypto/tls"
	"sync"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/util"
)

// MockSessionManager is a mocked session manager which is used for test.
type MockSessionManager struct {
	PS      []*util.ProcessInfo
	PSMu    sync.RWMutex
	SerID   uint64
	TxnInfo []*txninfo.TxnInfo
	Dom     *domain.Domain
	conn    map[uint64]session.Session
	mu      sync.Mutex
}

// ShowTxnList is to show txn list.
func (msm *MockSessionManager) ShowTxnList() []*txninfo.TxnInfo {
	msm.mu.Lock()
	defer msm.mu.Unlock()
	if len(msm.TxnInfo) > 0 {
		return msm.TxnInfo
	}
	rs := make([]*txninfo.TxnInfo, 0, len(msm.conn))
	for _, se := range msm.conn {
		info := se.TxnInfo()
		if info != nil {
			rs = append(rs, info)
		}
	}
	return rs
}

// ShowProcessList implements the SessionManager.ShowProcessList interface.
func (msm *MockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo {
	msm.PSMu.RLock()
	defer msm.PSMu.RUnlock()
	ret := make(map[uint64]*util.ProcessInfo)
	if len(msm.PS) > 0 {
		for _, item := range msm.PS {
			ret[item.ID] = item
		}
		return ret
	}
	msm.mu.Lock()
	for connID, pi := range msm.conn {
		ret[connID] = pi.ShowProcess()
	}
	msm.mu.Unlock()
	if msm.Dom != nil {
		for connID, pi := range msm.Dom.SysProcTracker().GetSysProcessList() {
			ret[connID] = pi
		}
	}
	return ret
}

// GetProcessInfo implements the SessionManager.GetProcessInfo interface.
func (msm *MockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	msm.PSMu.RLock()
	defer msm.PSMu.RUnlock()
	for _, item := range msm.PS {
		if item.ID == id {
			return item, true
		}
	}
	msm.mu.Lock()
	defer msm.mu.Unlock()
	if sess := msm.conn[id]; sess != nil {
		return sess.ShowProcess(), true
	}
	if msm.Dom != nil {
		if pinfo, ok := msm.Dom.SysProcTracker().GetSysProcessList()[id]; ok {
			return pinfo, true
		}
	}
	return &util.ProcessInfo{}, false
}

// Kill implements the SessionManager.Kill interface.
func (*MockSessionManager) Kill(uint64, bool) {
}

// KillAllConnections implements the SessionManager.KillAllConections interface.
func (*MockSessionManager) KillAllConnections() {
}

// UpdateTLSConfig implements the SessionManager.UpdateTLSConfig interface.
func (*MockSessionManager) UpdateTLSConfig(*tls.Config) {
}

// ServerID get server id.
func (msm *MockSessionManager) ServerID() uint64 {
	return msm.SerID
}

// StoreInternalSession is to store internal session.
func (*MockSessionManager) StoreInternalSession(interface{}) {}

// DeleteInternalSession is to delete the internal session pointer from the map in the SessionManager
func (*MockSessionManager) DeleteInternalSession(interface{}) {}

// GetInternalSessionStartTSList is to get all startTS of every transactions running in the current internal sessions
func (*MockSessionManager) GetInternalSessionStartTSList() []uint64 {
	return nil
}

// KillNonFlashbackClusterConn implement SessionManager interface.
func (msm *MockSessionManager) KillNonFlashbackClusterConn() {
	for _, se := range msm.conn {
		processInfo := se.ShowProcess()
		ddl, ok := processInfo.StmtCtx.GetPlan().(*core.DDL)
		if !ok {
			msm.Kill(se.GetSessionVars().ConnectionID, false)
			continue
		}
		_, ok = ddl.Statement.(*ast.FlashBackToTimestampStmt)
		if !ok {
			msm.Kill(se.GetSessionVars().ConnectionID, false)
			continue
		}
	}
}

// CheckOldRunningTxn implement SessionManager interface.
func (msm *MockSessionManager) CheckOldRunningTxn(job2ver map[int64]int64, job2ids map[int64]string) {
	msm.mu.Lock()
	for _, se := range msm.conn {
		session.RemoveLockDDLJobs(se, job2ver, job2ids)
	}
	msm.mu.Unlock()
}

// GetMinStartTS implements SessionManager interface.
func (msm *MockSessionManager) GetMinStartTS(lowerBound uint64) (ts uint64) {
	msm.PSMu.RLock()
	defer msm.PSMu.RUnlock()
	if len(msm.PS) > 0 {
		for _, pi := range msm.PS {
			if thisTS := pi.GetMinStartTS(lowerBound); thisTS > lowerBound && (thisTS < ts || ts == 0) {
				ts = thisTS
			}
		}
		return
	}
	msm.mu.Lock()
	defer msm.mu.Unlock()
	for _, s := range msm.conn {
		if thisTS := s.ShowProcess().GetMinStartTS(lowerBound); thisTS > lowerBound && (thisTS < ts || ts == 0) {
			ts = thisTS
		}
	}
	return
}
