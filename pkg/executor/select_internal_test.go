// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type lockCtxTxnManager struct {
	sessiontxn.TxnManager
	forUpdateTS uint64
}

func (m lockCtxTxnManager) GetStmtForUpdateTS() (uint64, error) { return m.forUpdateTS, nil }

func TestNewLockCtxPropagatesSharedLockUpgrade(t *testing.T) {
	originalGetTxnManager := sessiontxn.GetTxnManager
	sessiontxn.GetTxnManager = func(sessionctx.Context) sessiontxn.TxnManager {
		return lockCtxTxnManager{forUpdateTS: 9527}
	}
	t.Cleanup(func() {
		sessiontxn.GetTxnManager = originalGetTxnManager
	})

	sctx := mock.NewContext()
	sctx.GetSessionVars().EnableSharedLockUpgrade = true

	lockCtx, err := newLockCtx(sctx, 123, 1, true)
	require.NoError(t, err)
	require.True(t, lockCtx.InShareMode)
	require.True(t, lockCtx.AllowSharedLockUpgrade)
	require.Equal(t, uint64(9527), lockCtx.ForUpdateTS)
}
