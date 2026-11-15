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

package sessiontxn

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/tikv/client-go/v2/oracle"
)

type spfreshTimestampContext interface {
	GetStore() kv.Storage
	GetSessionVars() *variable.SessionVars
}

type spfreshReadVersion struct {
	mu     sync.Mutex
	taskID uint64
	scope  string
	future oracle.Future
}

func spfreshVersionFuture(ctx context.Context, sctx spfreshTimestampContext, consume bool) oracle.Future {
	vars := sctx.GetSessionVars()
	sc := vars.StmtCtx
	value, _ := sc.GetOrEvaluateStmtCache(stmtctx.StmtSPFreshReadVersionCacheKey, func() (any, error) {
		return &spfreshReadVersion{}, nil
	})
	state := value.(*spfreshReadVersion)
	state.mu.Lock()
	defer state.mu.Unlock()
	scope := vars.TxnCtx.TxnScope
	// Statement reset clears this cache; retries have a new TaskID. Neither may
	// reuse a timestamp allocated for an earlier execution attempt.
	if state.future == nil || state.taskID != sc.TaskID || state.scope != scope {
		state.taskID, state.scope = sc.TaskID, scope
		state.future = sctx.GetStore().GetOracle().GetTimestampAsync(ctx, &oracle.Option{TxnScope: scope})
	}
	future := state.future
	if consume {
		state.future = nil
	}
	return future
}

// PrefetchSPFreshReadVersion starts this execution's actual fresh TSO request.
// It never uses a transaction timestamp, tidb_snapshot or a low-resolution TSO.
// Repeated planning advice shares only the pending future, never a consumed TS.
func PrefetchSPFreshReadVersion(ctx context.Context, sctx spfreshTimestampContext) {
	sc := sctx.GetSessionVars().StmtCtx
	if sc.InExplainStmt && !sc.InExplainAnalyzeStmt {
		return
	}
	spfreshVersionFuture(ctx, sctx, false)
}

// SPFreshReadVersion consumes the execution's prefetched TSO exactly once.
// Direct executor entry points obtain a new future on demand. A transient
// prefetch failure uses CurrentVersion's existing bounded PD retry path.
func SPFreshReadVersion(ctx context.Context, sctx spfreshTimestampContext) (kv.Version, error) {
	if err := ctx.Err(); err != nil {
		return kv.Version{}, err
	}
	ts, err := spfreshVersionFuture(ctx, sctx, true).Wait()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return kv.Version{}, ctxErr
	}
	if err != nil {
		return sctx.GetStore().CurrentVersion(sctx.GetSessionVars().TxnCtx.TxnScope)
	}
	return kv.NewVersion(ts), nil
}
