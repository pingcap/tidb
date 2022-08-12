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

	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/util"
)

// MockSessionManager is a mocked session manager which is used for test.
type MockSessionManager struct {
	PS []*util.ProcessInfo
}

// ShowTxnList is to show txn list.
func (msm *MockSessionManager) ShowTxnList() []*txninfo.TxnInfo {
	return nil
}

// ShowProcessList implements the SessionManager.ShowProcessList interface.
func (msm *MockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo {
	ret := make(map[uint64]*util.ProcessInfo)
	for _, item := range msm.PS {
		ret[item.ID] = item
	}
	return ret
}

// GetProcessInfo implements the SessionManager.GetProcessInfo interface.
func (msm *MockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	for _, item := range msm.PS {
		if item.ID == id {
			return item, true
		}
	}
	return &util.ProcessInfo{}, false
}

// Kill implements the SessionManager.Kill interface.
func (msm *MockSessionManager) Kill(cid uint64, query bool) {
}

// KillAllConnections implements the SessionManager.KillAllConections interface.
func (msm *MockSessionManager) KillAllConnections() {
}

// UpdateTLSConfig implements the SessionManager.UpdateTLSConfig interface.
func (msm *MockSessionManager) UpdateTLSConfig(cfg *tls.Config) {
}

// ServerID get server id.
func (msm *MockSessionManager) ServerID() uint64 {
	return 1
}

// StoreInternalSession is to store internal session.
func (msm *MockSessionManager) StoreInternalSession(se interface{}) {}

// DeleteInternalSession is to delete the internal session pointer from the map in the SessionManager
func (msm *MockSessionManager) DeleteInternalSession(se interface{}) {}

// GetInternalSessionStartTSList is to get all startTS of every transactions running in the current internal sessions
func (msm *MockSessionManager) GetInternalSessionStartTSList() []uint64 {
	return nil
}
