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

// upgradeWithMDLMigration runs upgrade work with the legacy MDL migration selected
// before starting the temporary upgrade domain.
func upgradeWithMDLMigration(s sessionapi.Session, migration legacyMDLMigration) {
	// Do upgrade works then update bootstrap version.
	var ver int64
	ver, err := getBootstrapVersion(s)
	terror.MustNil(err)
	if ver >= currentBootstrapVersion {
		// It is already bootstrapped/upgraded by a higher version TiDB server.
		return
	}

	printClusterState(s, ver)

	// when upgrade from v6.4.0 or earlier, enables metadata lock automatically,
	// but during upgrade we disable it.
	if migration == requireLegacyMDLMigration {
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
	if migration == requireLegacyMDLMigration {
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

type mdlInitPolicy int

const (
	// mdlInitForceEnable always writes ON to the metadata key, even if it is
	// absent or explicitly OFF, and then applies the committed value to the
	// process-wide MDL setting. Fresh-cluster bootstrap uses this before its
	// DDL scheduler starts, so bootstrap DDLs use MDL from their first dispatch.
	mdlInitForceEnable mdlInitPolicy = iota

	// mdlInitEnableOnNull loads the persisted ON/OFF value, writing ON only if
	// the metadata key is absent. An explicit OFF is preserved. Normal startup
	// uses this before starting DDL, including the final normal domain after
	// upgrade and the case where another TiDB has already finished upgrading.
	// Reloading here applies any MDL value persisted by the upgrade migration.
	mdlInitEnableOnNull

	// mdlInitKeepDisabledOnNull reads the persisted ON/OFF value without writing
	// metadata. An absent key is interpreted as OFF, left absent, and reported
	// to the caller. Upgrade uses this before its temporary domain starts:
	// TiDB v6.4.0-or-earlier clusters stay non-MDL for upgrade DDLs. The legacy
	// version-99 migration then persists ON, which normal startup reloads before
	// starting the final scheduler. Existing explicit ON/OFF values are honored.
	// BR also uses this read-only policy so creating a session or domain does
	// not change the target cluster's MDL setting.
	mdlInitKeepDisabledOnNull
)

// InitMDLVariableForBR loads MDL without changing persisted metadata. BR runs
// against an already bootstrapped cluster and must not enable MDL on its behalf.
func InitMDLVariableForBR(store kv.Storage) error {
	_, err := initMDLVariable(store, mdlInitKeepDisabledOnNull)
	return err
}

// initMDLVariable applies the policy-specific change in one transaction and
// updates process-wide runtime state only after that transaction succeeds.
func initMDLVariable(store kv.Storage, policy mdlInitPolicy) (isNull bool, err error) {
	enable := false
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		if policy == mdlInitForceEnable {
			enable = true
			return t.SetMetadataLock(true)
		}
		enable, isNull, err = t.GetMetadataLock()
		if err != nil {
			return err
		}
		switch policy {
		case mdlInitEnableOnNull:
			if isNull {
				enable = true
				logutil.BgLogger().Info("metadata lock is null, set to enabled")
				return t.SetMetadataLock(true)
			}
		case mdlInitKeepDisabledOnNull:
			// A missing key is read as disabled and remains absent.
		default:
			// All callers select a private constant, so this cannot occur.
			logutil.BgLogger().Fatal("invalid metadata lock initialization policy", zap.Int("policy", int(policy)))
		}
		return nil
	})
	if err != nil {
		return isNull, err
	}
	vardef.SetEnableMDL(enable)
	return isNull, nil
}

func printClusterState(s sessionapi.Session, ver int64) {
	// After SupportUpgradeHTTPOpVer version, the upgrade by paused user DDL can be notified through the HTTP API.
	// We check the global state see if we are upgrading by paused the user DDL.
	if ver >= SupportUpgradeHTTPOpVer {
		isUpgradingClusterStateWithRetry(s, ver, currentBootstrapVersion, time.Duration(internalSQLTimeout)*time.Second)
	}
}
