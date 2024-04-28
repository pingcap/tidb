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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/syncer"
	dist_store "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/owner"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
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
		if err == nil && op.IsSyncedUpgradingState() {
			break
		}
		if i%10 == 0 {
			logger.Warn("get owner op failed", zap.Stringer("op", op), zap.Error(err))
		}
		time.Sleep(interval)
	}

	logger.Info("update global state to upgrading", zap.String("state", syncer.StateUpgrading))
	return nil
}

// SyncNormalRunning syncs normal state to etcd.
func SyncNormalRunning(s sessionctx.Context) error {
	bgCtx := context.Background()
	failpoint.Inject("mockResumeAllJobsFailed", func(val failpoint.Value) {
		if val.(bool) {
			dom := domain.GetDomain(s)
			//nolint: errcheck
			dom.DDL().StateSyncer().UpdateGlobalState(bgCtx, syncer.NewStateInfo(syncer.StateNormalRunning))
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

	if mgr, _ := dist_store.GetTaskManager(); mgr != nil {
		ctx := kv.WithInternalSourceType(bgCtx, kv.InternalDistTask)
		err := mgr.AdjustTaskOverflowConcurrency(ctx, s)
		if err != nil {
			log.Warn("cannot adjust task overflow concurrency", zap.Error(err))
		}
	}

	ctx, cancelFunc := context.WithTimeout(bgCtx, 3*time.Second)
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

func printClusterState(s sessiontypes.Session, ver int64) {
	// After SupportUpgradeHTTPOpVer version, the upgrade by paused user DDL can be notified through the HTTP API.
	// We check the global state see if we are upgrading by paused the user DDL.
	if ver >= SupportUpgradeHTTPOpVer {
		isUpgradingClusterStateWithRetry(s, ver, currentBootstrapVersion, time.Duration(internalSQLTimeout)*time.Second)
	}
}

func isUpgradingClusterStateWithRetry(s sessionctx.Context, oldVer, newVer int64, timeout time.Duration) {
	now := time.Now()
	interval := 200 * time.Millisecond
	logger := logutil.BgLogger().With(zap.String("category", "upgrading"))
	for i := 0; ; i++ {
		isUpgrading, err := IsUpgradingClusterState(s)
		if err == nil {
			logger.Info("get global state", zap.Int64("old version", oldVer), zap.Int64("latest version", newVer), zap.Bool("is upgrading state", isUpgrading))
			return
		}

		if time.Since(now) >= timeout {
			logger.Error("get global state failed", zap.Int64("old version", oldVer), zap.Int64("latest version", newVer), zap.Error(err))
			return
		}
		if i%25 == 0 {
			logger.Warn("get global state failed", zap.Int64("old version", oldVer), zap.Int64("latest version", newVer), zap.Error(err))
		}
		time.Sleep(interval)
	}
}
