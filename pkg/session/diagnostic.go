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

package session

import (
	"context"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/statistics/handle/syncload"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// bootstrapSessionImplDiagnostic opens an already bootstrapped store for queries.
// Keep this allowlist separate from normal bootstrap so new bootstrap steps do
// not implicitly run in diagnostic mode. In particular, this path must not
// create/upgrade system tables, persist bootstrap versions, or run startup hooks.
func bootstrapSessionImplDiagnostic(ctx context.Context, store kv.Storage) (_ *domain.Domain, err error) {
	// step1: load metadata from an already bootstrapped store.
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBootstrap)
	ver, err := domain.LoadDiagnosticMetadata(ctx, store)
	if err != nil {
		return nil, err
	}
	logutil.BgLogger().Info("initialize diagnostic session without bootstrap or upgrade", zap.Int64("version", ver))

	// step2: initialize the system time zone and collation mode.
	if err = initGlobalVarFromSystemDB(ctx, store); err != nil {
		return nil, err
	}

	// step3: get the Domain that owns this store's query runtime.
	dom, err := domap.Get(store)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			dom.Close()
		}
	}()

	// step4: prepare restricted sessions for the individual services.
	sessions, err := createDiagnosticSessions(store, dom)
	if err != nil {
		return nil, err
	}

	// step5: start the background services needed by diagnostic queries.
	if err = dom.StartDiagnostic(); err != nil {
		return nil, err
	}

	// step6: initialize the privilege cache, global variable cache, and binding cache.
	cfg := config.GetGlobalConfig()
	if !cfg.Security.SkipGrantTable {
		if err = dom.LoadPrivilegeLoop(sessions[diagnosticPrivilegeSession]); err != nil {
			return nil, err
		}
	}
	if err = dom.LoadSysVarCacheLoop(sessions[diagnosticSysvarSession]); err != nil {
		return nil, err
	}
	if err = dom.LoadBindingLoop(); err != nil {
		return nil, err
	}

	// step7: load expression pushdown and optimizer rule restrictions.
	if err = executor.LoadExprPushdownBlacklist(sessions[diagnosticQuerySession]); err != nil {
		return nil, err
	}
	if err = executor.LoadOptRuleBlacklist(ctx, sessions[diagnosticQuerySession]); err != nil {
		return nil, err
	}

	// step8: create the stats handle and start asynchronous statistics readers.
	concurrency := cfg.Performance.StatsLoadConcurrency
	if concurrency == 0 {
		concurrency = syncload.GetSyncLoadConcurrencyByCPU()
	}
	if err = dom.LoadStatsDiagnostic(ctx, max(concurrency, 0)); err != nil {
		return nil, err
	}
	return dom, nil
}

const (
	diagnosticQuerySession = iota
	diagnosticPrivilegeSession
	diagnosticSysvarSession
	diagnosticSessionCount
)

// createDiagnosticSessions registers Domain cleanup before creating restricted
// sessions. The caller must close the Domain on any subsequent startup error,
// including a failure to create one of these sessions.
func createDiagnosticSessions(store kv.Storage, dom *domain.Domain) ([]*session, error) {
	sessions := make([]*session, diagnosticSessionCount)
	dom.SetOnClose(func() {
		for _, s := range sessions {
			if s != nil {
				s.Close()
			}
		}
		// The binding maintenance worker normally owns this cleanup.
		if h := dom.BindingHandle(); h != nil {
			h.Close()
		}
		domap.Delete(store)
	})
	for i := range sessions {
		s, err := createSessionWithOpt(store, dom, dom.GetSchemaValidator(), dom.InfoCache(), nil)
		if err != nil {
			return nil, err
		}
		sessions[i] = s
		s.GetSessionVars().InRestrictedSQL = true
	}
	return sessions, nil
}
