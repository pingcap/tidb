// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// upgrade function  will do some upgrade works, when the system is bootstrapped by low version TiDB server
// For example, add new system variables into mysql.global_variables table.
func upgrade(s sessionapi.Session) {
	// Do upgrade works then update bootstrap version.
	isNull, err := InitMDLVariableForUpgrade(s.GetStore())
	if err != nil {
		logutil.BgLogger().Fatal("init metadata lock failed during upgrade", zap.Error(err))
	}

	var ver int64
	ver, err = getBootstrapVersion(s)
	terror.MustNil(err)
	if ver >= currentBootstrapVersion {
		// It is already bootstrapped/upgraded by a higher version TiDB server.
		return
	}

	printClusterState(s, ver)

	// when upgrade from v6.4.0 or earlier, enables metadata lock automatically,
	// but during upgrade we disable it.
	if isNull {
		upgradeToVer99Before(s)
	}

	// It is only used in test.
	upgradeFns := addMockBootstrapVersionForTest(s)
	for _, verFn := range upgradeFns {
		if ver < verFn.version {
			verFn.fn(s, ver)
			logutil.BgLogger().Info("upgrade to bootstrap version.",
				zap.Int64("old-start-version", ver),
				zap.Int64("in-progress-version", verFn.version),
				zap.Int64("target-version", currentBootstrapVersion))
		}
	}
	if isNull {
		upgradeToVer99After(s)
	}

	updateBootstrapVer(s)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	_, err = s.ExecuteInternal(ctx, "COMMIT")

	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("update bootstrap ver failed",
			zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if TiDB is already upgraded.
		v, err1 := getBootstrapVersion(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("upgrade failed", zap.Error(err1))
		}
		if v >= currentBootstrapVersion {
			// It is already bootstrapped/upgraded by a higher version TiDB server.
			return
		}
		logutil.BgLogger().Fatal("[upgrade] upgrade failed",
			zap.Int64("from", ver),
			zap.Int64("to", currentBootstrapVersion),
			zap.Error(err))
	}
}

// InitMDLVariableForUpgrade initializes the metadata lock variable.
func InitMDLVariableForUpgrade(store kv.Storage) (bool, error) {
	isNull := false
	enable := false
	var err error
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		enable, isNull, err = t.GetMetadataLock()
		if err != nil {
			return err
		}
		return nil
	})
	if isNull || !enable {
		vardef.SetEnableMDL(false)
	} else {
		vardef.SetEnableMDL(true)
	}
	return isNull, err
}

func printClusterState(s sessionapi.Session, ver int64) {
	// After SupportUpgradeHTTPOpVer version, the upgrade by paused user DDL can be notified through the HTTP API.
	// We check the global state see if we are upgrading by paused the user DDL.
	if ver >= SupportUpgradeHTTPOpVer {
		isUpgradingClusterStateWithRetry(s, ver, currentBootstrapVersion, time.Duration(internalSQLTimeout)*time.Second)
	}
}
