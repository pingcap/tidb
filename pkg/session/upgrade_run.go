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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

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
	variable.EnableMDL.Store(enable)
	return isNull, nil
}
