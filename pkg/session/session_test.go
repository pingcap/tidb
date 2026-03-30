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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

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
