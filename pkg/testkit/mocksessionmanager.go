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
	"maps"
	"sync"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer/mdldef"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// MockSessionManager is a mocked session manager which is used for test.
type MockSessionManager struct {
	PS       []*sessmgr.ProcessInfo
	PSMu     sync.RWMutex
	SerID    uint64
	TxnInfo  []*txninfo.TxnInfo
	Dom      *domain.Domain
	Conn     map[uint64]sessionapi.Session
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
		if info != nil && info.ProcessInfo != nil {
			rs = append(rs, info)
		}
	}
	return rs
}

// ShowProcessList implements the Manager.ShowProcessList interface.
func (msm *MockSessionManager) ShowProcessList() map[uint64]*sessmgr.ProcessInfo {
	msm.PSMu.RLock()
	defer msm.PSMu.RUnlock()
	ret := make(map[uint64]*sessmgr.ProcessInfo)
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
		maps.Copy(ret, msm.Dom.SysProcTracker().GetSysProcessList())
	}
	return ret
}

// GetProcessInfo implements the Manager.GetProcessInfo interface.
func (msm *MockSessionManager) GetProcessInfo(id uint64) (*sessmgr.ProcessInfo, bool) {
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
	return &sessmgr.ProcessInfo{}, false
}

// GetConAttrs returns the connection attributes of all connections
func (msm *MockSessionManager) GetConAttrs(user *auth.UserIdentity) map[uint64]map[string]string {
	return msm.ConAttrs
}

// Kill implements the Manager.Kill interface.
func (*MockSessionManager) Kill(uint64, bool, bool, bool) {
}

// KillAllConnections implements the Manager.KillAllConnections interface.
func (*MockSessionManager) KillAllConnections() {
}

// UpdateTLSConfig implements the Manager.UpdateTLSConfig interface.
func (*MockSessionManager) UpdateTLSConfig(*tls.Config) {
}

// ServerID get server id.
func (msm *MockSessionManager) ServerID() uint64 {
	return msm.SerID
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

// ContainsInternalSession checks if the internal session pointer is in the map in the Manager
func (msm *MockSessionManager) ContainsInternalSession(se any) bool {
	msm.mu.Lock()
	defer msm.mu.Unlock()
	if msm.internalSessions == nil {
		return false
	}
	_, ok := msm.internalSessions[se]
	return ok
}

// InternalSessionCount implements the Manager.InternalSessionCount interface.
func (msm *MockSessionManager) InternalSessionCount() int {
	msm.mu.Lock()
	defer msm.mu.Unlock()
	return len(msm.internalSessions)
}

// DeleteInternalSession is to delete the internal session pointer from the map in the Manager
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
		// Ref the implementation of `GetInternalSessionStartTSList` on the real session manager. The `TxnInfo` is more
		// accurate, because if a session is pending, the `StartTS` in `sessVars.TxnCtx` will not be updated. For example,
		// if there is not DDL for a long time, the minimal internal session start ts will not have any progress.
		if se, ok := internalSess.(interface{ TxnInfo() *txninfo.TxnInfo }); ok {
			txn := se.TxnInfo()
			if txn != nil {
				ret = append(ret, txn.StartTS)
			}
			continue
		}
	}
	return ret
}

// KillNonFlashbackClusterConn implement Manager interface.
func (msm *MockSessionManager) KillNonFlashbackClusterConn() {
	for _, se := range msm.Conn {
		processInfo := se.ShowProcess()
		ddl, ok := processInfo.StmtCtx.GetPlan().(*core.DDL)
		if !ok {
			msm.Kill(se.GetSessionVars().ConnectionID, false, false, false)
			continue
		}
		_, ok = ddl.Statement.(*ast.FlashBackToTimestampStmt)
		if !ok {
			msm.Kill(se.GetSessionVars().ConnectionID, false, false, false)
			continue
		}
	}
}

// CheckOldRunningTxn is to get all startTS of every transactions running in the current internal sessions
func (msm *MockSessionManager) CheckOldRunningTxn(jobs map[int64]*mdldef.JobMDL) {
	msm.mu.Lock()
	for _, se := range msm.Conn {
		variable.RemoveLockDDLJobs(se.GetSessionVars(), jobs, false)
	}
	msm.mu.Unlock()
}

// GetStatusVars is getting the per-session status variables
func (msm *MockSessionManager) GetStatusVars() map[uint64]map[string]string {
	return map[uint64]map[string]string{}
}
