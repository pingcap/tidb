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

package isolation

import (
	"math"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// OptimisticTxnContextProvider provides txn context for optimistic transaction
type OptimisticTxnContextProvider struct {
	baseTxnContextProvider
}

// NewOptimisticTxnContextProvider returns a new OptimisticTxnContextProvider
func NewOptimisticTxnContextProvider(sctx sessionctx.Context, causalConsistencyOnly bool) *OptimisticTxnContextProvider {
	provider := &OptimisticTxnContextProvider{
		baseTxnContextProvider: baseTxnContextProvider{
			sctx:                  sctx,
			causalConsistencyOnly: causalConsistencyOnly,
			onTxnActive: func(_ kv.Transaction, tp sessiontxn.EnterNewTxnType) {
				sessVars := sctx.GetSessionVars()
				sessVars.TxnCtx.CouldRetry = isOptimisticTxnRetryable(sessVars, tp)
			},
		},
	}

	provider.getStmtReadTSFunc = provider.getTxnStartTS
	provider.getStmtForUpdateTSFunc = provider.getTxnStartTS
	return provider
}

// isOptimisticTxnRetryable (if returns true) means the transaction could retry.
// We only consider retry in this optimistic mode.
// If the session is already in transaction, enable retry or internal SQL could retry.
// If not, the transaction could always retry, because it should be auto committed transaction.
// Anyway the retry limit is 0, the transaction could not retry.
func isOptimisticTxnRetryable(sessVars *variable.SessionVars, tp sessiontxn.EnterNewTxnType) bool {
	if tp == sessiontxn.EnterNewTxnDefault {
		return false
	}

	// If retry limit is 0, the transaction could not retry.
	if sessVars.RetryLimit == 0 {
		return false
	}

	// When `@@tidb_snapshot` is set, it is a ready-only statement and will not cause the errors that should retry a transaction in optimistic mode.
	if sessVars.SnapshotTS != 0 {
		return false
	}

	// If the session is not InTxn, it is an auto-committed transaction.
	// The auto-committed transaction could always retry.
	if !sessVars.InTxn() {
		return true
	}

	// The internal transaction could always retry.
	if sessVars.InRestrictedSQL {
		return true
	}

	// If the retry is enabled, the transaction could retry.
	if !sessVars.DisableTxnAutoRetry {
		return true
	}

	return false
}

// AdviseOptimizeWithPlan providers optimization according to the plan
// It will use MaxTS as the startTS in autocommit txn for some plans.
func (p *OptimisticTxnContextProvider) AdviseOptimizeWithPlan(plan interface{}) (err error) {
	if p.isTidbSnapshotEnabled() || p.isBeginStmtWithStaleRead() {
		return nil
	}

	realPlan, ok := plan.(plannercore.Plan)
	if !ok {
		return nil
	}

	if execute, ok := plan.(*plannercore.Execute); ok {
		realPlan = execute.Plan
	}

	ok, err = plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(p.sctx, realPlan)
	if err != nil {
		return err
	}

	if ok {
		sessVars := p.sctx.GetSessionVars()
		logutil.BgLogger().Debug("init txnStartTS with MaxUint64",
			zap.Uint64("conn", sessVars.ConnectionID),
			zap.String("text", sessVars.StmtCtx.OriginalSQL),
		)
		if err = p.prepareTxnWithTS(math.MaxUint64); err != nil {
			return err
		}

		if sessVars.StmtCtx.Priority == mysql.NoPriority {
			sessVars.StmtCtx.Priority = kv.PriorityHigh
		}
	}
	return nil
}
