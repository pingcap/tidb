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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var emptyOptimisticTxnContextProvider = OptimisticTxnContextProvider{}

// OptimisticTxnContextProvider provides txn context for optimistic transaction
type OptimisticTxnContextProvider struct {
	baseTxnContextProvider
	optimizeWithMaxTS bool
}

// ResetForNewTxn resets OptimisticTxnContextProvider to an initial state for a new txn
func (p *OptimisticTxnContextProvider) ResetForNewTxn(sctx sessionctx.Context, causalConsistencyOnly bool) {
	*p = emptyOptimisticTxnContextProvider
	p.sctx = sctx
	p.causalConsistencyOnly = causalConsistencyOnly
	p.onTxnActiveFunc = p.onTxnActive
	p.getStmtReadTSFunc = p.getTxnStartTS
	p.getStmtForUpdateTSFunc = p.getTxnStartTS
}

func (p *OptimisticTxnContextProvider) onTxnActive(_ kv.Transaction, tp sessiontxn.EnterNewTxnType) {
	sessVars := p.sctx.GetSessionVars()
	sessVars.TxnCtx.CouldRetry = isOptimisticTxnRetryable(sessVars, tp)
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

// GetStmtReadTS returns the read timestamp used by select statement (not for select ... for update)
func (p *OptimisticTxnContextProvider) GetStmtReadTS() (uint64, error) {
	// If `math.MaxUint64` is used for point get optimization, it is not necessary to activate the txn.
	// Just return `math.MaxUint64` to save the performance.
	if p.optimizeWithMaxTS {
		return math.MaxUint64, nil
	}
	return p.baseTxnContextProvider.GetStmtReadTS()
}

// GetStmtForUpdateTS returns the read timestamp used by select statement (not for select ... for update)
func (p *OptimisticTxnContextProvider) GetStmtForUpdateTS() (uint64, error) {
	if p.optimizeWithMaxTS {
		return math.MaxUint64, nil
	}
	return p.baseTxnContextProvider.GetStmtForUpdateTS()
}

// AdviseOptimizeWithPlan providers optimization according to the plan
// It will use MaxTS as the startTS in autocommit txn for some plans.
func (p *OptimisticTxnContextProvider) AdviseOptimizeWithPlan(plan any) (err error) {
	if p.optimizeWithMaxTS || p.isTidbSnapshotEnabled() || p.isBeginStmtWithStaleRead() {
		return nil
	}

	if p.txn != nil {
		// `p.txn != nil` means the txn has already been activated, we should not optimize the startTS because the startTS
		// has already been used.
		return nil
	}

	realPlan, ok := plan.(base.Plan)
	if !ok {
		return nil
	}

	if execute, ok := plan.(*plannercore.Execute); ok {
		realPlan = execute.Plan
	}

	ok = plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(p.sctx.GetSessionVars(), realPlan)

	if ok {
		sessVars := p.sctx.GetSessionVars()
		logutil.BgLogger().Debug("init txnStartTS with MaxUint64",
			zap.Uint64("conn", sessVars.ConnectionID),
			zap.String("text", sessVars.StmtCtx.OriginalSQL),
		)

		if err = p.forcePrepareConstStartTS(math.MaxUint64); err != nil {
			logutil.BgLogger().Error("failed init txnStartTS with MaxUint64",
				zap.Error(err),
				zap.Uint64("conn", sessVars.ConnectionID),
				zap.String("text", sessVars.StmtCtx.OriginalSQL),
			)
			return err
		}

		p.optimizeWithMaxTS = true
		if sessVars.StmtCtx.Priority == mysql.NoPriority {
			sessVars.StmtCtx.Priority = kv.PriorityHigh
		}
	}
	return nil
}
