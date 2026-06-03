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

package session

import (
	"crypto/tls"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

type advisoryLockTestSessionManager struct {
	mu               sync.Mutex
	internalSessions map[any]struct{}
}

func (m *advisoryLockTestSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo { return nil }
func (m *advisoryLockTestSessionManager) ShowTxnList() []*txninfo.TxnInfo               { return nil }
func (m *advisoryLockTestSessionManager) GetProcessInfo(uint64) (*util.ProcessInfo, bool) {
	return nil, false
}
func (m *advisoryLockTestSessionManager) Kill(uint64, bool, bool, bool) {}
func (m *advisoryLockTestSessionManager) KillAllConnections()           {}
func (m *advisoryLockTestSessionManager) UpdateTLSConfig(*tls.Config)   {}
func (m *advisoryLockTestSessionManager) ServerID() uint64              { return 0 }
func (m *advisoryLockTestSessionManager) StoreInternalSession(se any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.internalSessions == nil {
		m.internalSessions = make(map[any]struct{})
	}
	m.internalSessions[se] = struct{}{}
}
func (m *advisoryLockTestSessionManager) DeleteInternalSession(se any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.internalSessions, se)
}
func (m *advisoryLockTestSessionManager) ContainsInternalSession(se any) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.internalSessions[se]
	return ok
}
func (m *advisoryLockTestSessionManager) GetInternalSessionStartTSList() []uint64 { return nil }
func (m *advisoryLockTestSessionManager) CheckOldRunningTxn(map[int64]int64, map[int64]string) {
}
func (m *advisoryLockTestSessionManager) KillNonFlashbackClusterConn() {}
func (m *advisoryLockTestSessionManager) GetConAttrs(*auth.UserIdentity) map[uint64]map[string]string {
	return nil
}

func TestGetStartMode(t *testing.T) {
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion))
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion+1))
	require.Equal(t, ddl.Upgrade, getStartMode(currentBootstrapVersion-1))
	require.Equal(t, ddl.Bootstrap, getStartMode(0))
}

func TestAdvisoryLockWithoutProcessInfo(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se, err := createSession(store)
	require.NoError(t, err)

	require.Nil(t, se.ShowProcess())
	lockName := fmt.Sprintf("%s-lock", t.Name())

	require.NoError(t, se.GetAdvisoryLock(lockName, 1))
	require.NotZero(t, se.advisoryLockOwnerID.Load())
	require.Equal(t, se.advisoryLockOwnerID.Load(), se.IsUsedAdvisoryLock(lockName))
	require.True(t, se.ReleaseAdvisoryLock(lockName))
	require.Zero(t, se.IsUsedAdvisoryLock(lockName))

	se.Close()
	require.Zero(t, se.advisoryLockOwnerID.Load())
}

func TestAdvisoryLockUsesProcessInfoOwner(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se, err := createSession(store)
	require.NoError(t, err)
	defer se.Close()

	const connID uint64 = 12345
	lockName := fmt.Sprintf("%s-processinfo-lock", t.Name())
	se.SetConnectionID(connID)
	se.SetProcessInfo("select 1", time.Now(), mysql.ComQuery, 0)

	require.NoError(t, se.GetAdvisoryLock(lockName, 1))
	require.Equal(t, connID, se.IsUsedAdvisoryLock(lockName))
	require.Equal(t, connID, se.advisoryLockOwnerID.Load())
	require.True(t, se.ReleaseAdvisoryLock(lockName))
}

func TestAdvisoryLockConflictDoesNotLeakInternalSession(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se1, err := createSession(store)
	require.NoError(t, err)
	defer se1.Close()

	se2, err := createSession(store)
	require.NoError(t, err)
	defer se2.Close()

	manager := &advisoryLockTestSessionManager{}
	dom.InfoSyncer().SetSessionManager(manager)

	var (
		mu              sync.Mutex
		createdSessions []*session
	)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/session/afterCreateAdvisoryLockSession", func(se *session) {
		mu.Lock()
		defer mu.Unlock()
		createdSessions = append(createdSessions, se)
	})

	lockName := fmt.Sprintf("%s-conflict-lock", t.Name())
	require.NoError(t, se1.GetAdvisoryLock(lockName, 1))
	mu.Lock()
	require.Len(t, createdSessions, 1)
	successSession := createdSessions[0]
	mu.Unlock()
	require.True(t, infosync.ContainsInternalSessionForTest(successSession))

	err = se2.GetAdvisoryLock(lockName, 1)
	require.Error(t, err)
	mu.Lock()
	require.Len(t, createdSessions, 2)
	failedSession := createdSessions[1]
	mu.Unlock()
	require.False(t, infosync.ContainsInternalSessionForTest(failedSession))
	require.True(t, infosync.ContainsInternalSessionForTest(successSession))

	require.True(t, se1.ReleaseAdvisoryLock(lockName))
	require.False(t, infosync.ContainsInternalSessionForTest(successSession))
}
