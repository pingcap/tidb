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

package executor

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type stubTxnManager struct {
	forUpdateTS uint64
}

func (m stubTxnManager) AdviseWarmup() error { return nil }

func (m stubTxnManager) AdviseOptimizeWithPlan(any) error { return nil }

func (m stubTxnManager) GetTxnInfoSchema() infoschema.InfoSchema { return nil }

func (m stubTxnManager) GetTxnScope() string { return "" }

func (m stubTxnManager) GetReadReplicaScope() string { return "" }

func (m stubTxnManager) GetStmtReadTS() (uint64, error) { return 0, nil }

func (m stubTxnManager) GetStmtForUpdateTS() (uint64, error) { return m.forUpdateTS, nil }

func (m stubTxnManager) GetContextProvider() sessiontxn.TxnContextProvider { return nil }

func (m stubTxnManager) GetSnapshotWithStmtReadTS() (kv.Snapshot, error) { return nil, nil }

func (m stubTxnManager) GetSnapshotWithStmtForUpdateTS() (kv.Snapshot, error) { return nil, nil }

func (m stubTxnManager) EnterNewTxn(context.Context, *sessiontxn.EnterNewTxnRequest) error {
	return nil
}

func (m stubTxnManager) OnTxnEnd() {}

func (m stubTxnManager) OnStmtStart(context.Context, ast.StmtNode) error { return nil }

func (m stubTxnManager) OnPessimisticStmtStart(context.Context) error { return nil }

func (m stubTxnManager) OnPessimisticStmtEnd(context.Context, bool) error { return nil }

func (m stubTxnManager) OnStmtErrorForNextAction(context.Context, sessiontxn.StmtErrorHandlePoint, error) (sessiontxn.StmtErrorAction, error) {
	return sessiontxn.StmtActionNoIdea, nil
}

func (m stubTxnManager) OnStmtRetry(context.Context) error { return nil }

func (m stubTxnManager) OnStmtCommit(context.Context) error { return nil }

func (m stubTxnManager) OnStmtRollback(context.Context, bool) error { return nil }

func (m stubTxnManager) OnStmtEnd() {}

func (m stubTxnManager) OnLocalTemporaryTableCreated() {}

func (m stubTxnManager) ActivateTxn() (kv.Transaction, error) { return nil, nil }

func (m stubTxnManager) GetCurrentStmt() ast.StmtNode { return nil }

func (m stubTxnManager) SetOptionsBeforeCommit(kv.Transaction, func(uint64) bool) error { return nil }

func BenchmarkResetContextOfStmt(b *testing.B) {
	stmt := &ast.SelectStmt{}
	ctx := mock.NewContext()
	ctx.BindDomainAndSchValidator(&domain.Domain{}, nil)
	for i := 0; i < b.N; i++ {
		ResetContextOfStmt(ctx, stmt)
	}
}

func TestImportIntoShouldHaveSameFlagsAsInsert(t *testing.T) {
	insertStmt := &ast.InsertStmt{}
	importStmt := &ast.ImportIntoStmt{}
	insertCtx := mock.NewContext()
	importCtx := mock.NewContext()
	insertCtx.BindDomainAndSchValidator(&domain.Domain{}, nil)
	importCtx.BindDomainAndSchValidator(&domain.Domain{}, nil)
	for _, modeStr := range []string{
		"",
		"IGNORE_SPACE",
		"STRICT_TRANS_TABLES",
		"STRICT_ALL_TABLES",
		"ALLOW_INVALID_DATES",
		"NO_ZERO_IN_DATE",
		"NO_ZERO_DATE",
		"NO_ZERO_IN_DATE,STRICT_ALL_TABLES",
		"NO_ZERO_DATE,STRICT_ALL_TABLES",
		"NO_ZERO_IN_DATE,NO_ZERO_DATE,STRICT_ALL_TABLES",
	} {
		t.Run(fmt.Sprintf("mode %s", modeStr), func(t *testing.T) {
			mode, err := mysql.GetSQLMode(modeStr)
			require.NoError(t, err)
			insertCtx.GetSessionVars().SQLMode = mode
			require.NoError(t, ResetContextOfStmt(insertCtx, insertStmt))
			importCtx.GetSessionVars().SQLMode = mode
			require.NoError(t, ResetContextOfStmt(importCtx, importStmt))

			insertTypeCtx := insertCtx.GetSessionVars().StmtCtx.TypeCtx()
			importTypeCtx := importCtx.GetSessionVars().StmtCtx.TypeCtx()
			require.EqualValues(t, insertTypeCtx.Flags(), importTypeCtx.Flags())
		})
	}

	t.Run("shared lock upgrade gate propagates to lock ctx", func(t *testing.T) {
		originalGetTxnManager := sessiontxn.GetTxnManager
		sessiontxn.GetTxnManager = func(sctx sessionctx.Context) sessiontxn.TxnManager {
			return stubTxnManager{forUpdateTS: 9527}
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
	})
}
