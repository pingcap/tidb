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

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
)

// MockSessionManager is a mocked session manager which is used for test.
type MockSessionManager struct {
	PS       []*util.ProcessInfo
	PSMu     sync.RWMutex
	SerID    uint64
	TxnInfo  []*txninfo.TxnInfo
	Dom      *domain.Domain
	Conn     map[uint64]sessiontypes.Session
	mu       sync.Mutex
	ConAttrs map[uint64]map[string]string

	internalSessions map[any]struct{}
}

// ShowTxnList is to show txn list.
func (msm *MockSessionManager) ShowTxnList() []*txninfo.TxnInfo {
	msm.mu.Lock()
	defer msm.mu.Unlock()
	if len(msm.TxnInfo) > 0 {
		return msm.TxnInfo
	}
	rs := make([]*txninfo.TxnInfo, 0, len(msm.Conn))
	for _, se := range msm.Conn {
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
	for connID, pi := range msm.Conn {
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
	if sess := msm.Conn[id]; sess != nil {
		return sess.ShowProcess(), true
	}
	if msm.Dom != nil {
		if pinfo, ok := msm.Dom.SysProcTracker().GetSysProcessList()[id]; ok {
			return pinfo, true
		}
	}
	return &util.ProcessInfo{}, false
}

// GetConAttrs returns the connection attributes of all connections
func (msm *MockSessionManager) GetConAttrs(user *auth.UserIdentity) map[uint64]map[string]string {
	return msm.ConAttrs
}

// Kill implements the SessionManager.Kill interface.
func (*MockSessionManager) Kill(uint64, bool, bool) {
}

// KillAllConnections implements the SessionManager.KillAllConnections interface.
func (*MockSessionManager) KillAllConnections() {
}

// UpdateTLSConfig implements the SessionManager.UpdateTLSConfig interface.
func (*MockSessionManager) UpdateTLSConfig(*tls.Config) {
}

// ServerID get server id.
func (msm *MockSessionManager) ServerID() uint64 {
	return msm.SerID
}

// GetAutoAnalyzeProcID implement SessionManager interface.
func (msm *MockSessionManager) GetAutoAnalyzeProcID() uint64 {
	return uint64(1)
}

// StoreInternalSession is to store internal session.
func (msm *MockSessionManager) StoreInternalSession(s any) {
	msm.mu.Lock()
	if msm.internalSessions == nil {
		msm.internalSessions = make(map[any]struct{})
	}
	msm.internalSessions[s] = struct{}{}
	msm.mu.Unlock()
}

// DeleteInternalSession is to delete the internal session pointer from the map in the SessionManager
func (msm *MockSessionManager) DeleteInternalSession(s any) {
	msm.mu.Lock()
	delete(msm.internalSessions, s)
	msm.mu.Unlock()
}

// GetInternalSessionStartTSList is to get all startTS of every transaction running in the current internal sessions
func (msm *MockSessionManager) GetInternalSessionStartTSList() []uint64 {
	msm.mu.Lock()
	defer msm.mu.Unlock()
	ret := make([]uint64, 0, len(msm.internalSessions))
	for internalSess := range msm.internalSessions {
		se := internalSess.(sessionctx.Context)
		sessVars := se.GetSessionVars()
		sessVars.TxnCtxMu.Lock()
		startTS := sessVars.TxnCtx.StartTS
		sessVars.TxnCtxMu.Unlock()
		ret = append(ret, startTS)
	}
	return ret
}

// KillNonFlashbackClusterConn implement SessionManager interface.
func (msm *MockSessionManager) KillNonFlashbackClusterConn() {
	for _, se := range msm.Conn {
		processInfo := se.ShowProcess()
		ddl, ok := processInfo.StmtCtx.GetPlan().(*core.DDL)
		if !ok {
			msm.Kill(se.GetSessionVars().ConnectionID, false, false)
			continue
		}
		_, ok = ddl.Statement.(*ast.FlashBackToTimestampStmt)
		if !ok {
			msm.Kill(se.GetSessionVars().ConnectionID, false, false)
			continue
		}
	}
}

// CheckOldRunningTxn is to get all startTS of every transactions running in the current internal sessions
func (msm *MockSessionManager) CheckOldRunningTxn(job2ver map[int64]int64, job2ids map[int64]string) {
	msm.mu.Lock()
	for _, se := range msm.Conn {
		session.RemoveLockDDLJobs(se, job2ver, job2ids, false)
	}
	msm.mu.Unlock()
}
