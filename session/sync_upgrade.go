// Copyright 2023 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/syncer"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// isContextDone checks if context is done.
func isContextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}

// SyncUpgradeState syncs upgrade state to etcd.
func SyncUpgradeState(s sessionctx.Context, timeout time.Duration) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	dom := domain.GetDomain(s)
	err := dom.DDL().StateSyncer().UpdateGlobalState(ctx, syncer.NewStateInfo(syncer.StateUpgrading))
	logger := logutil.BgLogger().With(zap.String("category", "upgrading"))
	if err != nil {
		logger.Error("update global state failed", zap.String("state", syncer.StateUpgrading), zap.Error(err))
		return err
	}

	interval := 200 * time.Millisecond
	for i := 0; ; i++ {
		if isContextDone(ctx) {
			logger.Error("get owner op failed", zap.Duration("timeout", timeout), zap.Error(err))
			return ctx.Err()
		}

		var op owner.OpType
		childCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		op, err = owner.GetOwnerOpValue(childCtx, dom.EtcdClient(), ddl.DDLOwnerKey, "upgrade bootstrap")
		cancel()
		if err == nil && op.String() == owner.OpGetUpgradingState.String() {
			break
		}
		if i%10 == 0 {
			logger.Warn("get owner op failed", zap.Stringer("state", op), zap.Error(err))
		}
		time.Sleep(interval)
	}

	logger.Info("update global state to upgrading", zap.String("state", syncer.StateUpgrading))
	return nil
}

// SyncNormalRunning syncs normal state to etcd.
func SyncNormalRunning(s sessionctx.Context) error {
	failpoint.Inject("mockResumeAllJobsFailed", func(val failpoint.Value) {
		if val.(bool) {
			dom := domain.GetDomain(s)
			//nolint: errcheck
			dom.DDL().StateSyncer().UpdateGlobalState(context.Background(), syncer.NewStateInfo(syncer.StateNormalRunning))
			failpoint.Return(nil)
		}
	})

	logger := logutil.BgLogger().With(zap.String("category", "upgrading"))
	jobErrs, err := ddl.ResumeAllJobsBySystem(s)
	if err != nil {
		logger.Warn("resume all paused jobs failed", zap.Error(err))
	}
	for _, e := range jobErrs {
		logger.Warn("resume the job failed", zap.Error(e))
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFunc()
	dom := domain.GetDomain(s)
	err = dom.DDL().StateSyncer().UpdateGlobalState(ctx, syncer.NewStateInfo(syncer.StateNormalRunning))
	if err != nil {
		logger.Error("update global state to normal failed", zap.Error(err))
		return err
	}
	logger.Info("update global state to normal running finished")
	return nil
}

// IsUpgradingClusterState checks whether the global state is upgrading.
func IsUpgradingClusterState(s sessionctx.Context) (bool, error) {
	dom := domain.GetDomain(s)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFunc()
	stateInfo, err := dom.DDL().StateSyncer().GetGlobalState(ctx)
	if err != nil {
		return false, err
	}

	return stateInfo.State == syncer.StateUpgrading, nil
}

func checkOrSyncUpgrade(s Session, ver int64) {
	if ver < SupportUpgradeHTTPOpVer {
		terror.MustNil(SyncUpgradeState(s, time.Duration(internalSQLTimeout)*time.Second))
		return
	}
	isUpgradingClusterStateWithRetry(s, ver, currentBootstrapVersion, time.Duration(internalSQLTimeout)*time.Second)
}

func isUpgradingClusterStateWithRetry(s sessionctx.Context, oldVer, newVer int64, timeout time.Duration) {
	now := time.Now()
	interval := 200 * time.Millisecond
	logger := logutil.BgLogger().With(zap.String("category", "upgrading"))
	for i := 0; ; i++ {
		isUpgrading, err := IsUpgradingClusterState(s)
		if err == nil {
			if isUpgrading {
				break
			}
			logger.Fatal("global state isn't upgrading, please send a request to start the upgrade first", zap.Error(err))
		}

		if time.Since(now) >= timeout {
			logger.Fatal("get global state failed", zap.Error(err))
		}
		if i%10 == 0 {
			logger.Warn("get global state failed", zap.Error(err))
		}
		time.Sleep(interval)
	}
	logger.Info("global state is upgrading", zap.Int64("old version", oldVer), zap.Int64("latest version", newVer))
}
