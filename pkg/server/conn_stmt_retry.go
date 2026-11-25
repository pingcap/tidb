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

package server

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
)

// stmtRetrySafety describes whether an attempt can be repeated without replaying
// client-visible output or other protocol side effects, such as a file transfer.
type stmtRetrySafety bool

const (
	stmtRetryUnsafe stmtRetrySafety = false
	stmtRetrySafe   stmtRetrySafety = true
)

// runStmtWithRetry runs a statement and at most one transaction retry or TiFlash
// fallback. The attempt owns protocol-specific warning handling: tiFlashErr is
// non-nil only for fallback, and contains the original error to report as a warning.
func (cc *clientConn) runStmtWithRetry(ctx context.Context, attempt func(tiFlashErr error) (stmtRetrySafety, error)) error {
	safety, err := attempt(nil)
	if err == nil {
		return nil
	}

	// Classification is required even when protocol side effects prohibit retry.
	action, txnErr := sessiontxn.GetTxnManager(&cc.ctx).OnStmtErrorForNextAction(ctx, sessiontxn.StmtErrAfterQuery, err)
	if txnErr != nil {
		return txnErr
	}
	if safety != stmtRetrySafe {
		return err
	}

	vars := cc.ctx.GetSessionVars()
	if action == sessiontxn.StmtActionRetryReady {
		retryInfo := vars.RetryInfo
		wasRetrying := retryInfo.Retrying
		retryInfo.Retrying = true
		defer func() { retryInfo.Retrying = wasRetrying }()
		_, err = attempt(nil)
		return err
	}

	if _, allowed := vars.AllowFallbackToTiKV[kv.TiFlash]; !allowed || !errors.ErrorEqual(err, storeerr.ErrTiFlashServerTimeout) {
		return err
	}
	engines := vars.IsolationReadEngines
	_, hadTiFlash := engines[kv.TiFlash]
	delete(engines, kv.TiFlash)
	defer func() {
		if hadTiFlash {
			engines[kv.TiFlash] = struct{}{}
		} else {
			delete(engines, kv.TiFlash)
		}
	}()
	_, err = attempt(err)
	return err
}
