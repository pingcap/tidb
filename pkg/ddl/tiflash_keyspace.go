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

package ddl

import (
	"context"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

// checkColumnarStorageEnabled checks whether columnar storage is enabled for the current keyspace.
// It gates `SET TIFLASH REPLICA n (n>0)` so that replicas are not silently created on clusters without columnar
// storage.
// * The gate applies only to columnar architectures; classic TiFlash clusters keep the existing behavior.
// * The enabled flag is stored in the PD keyspace meta config, the kernel only reads it.
//
// Behavior:
//   - flag is "false": returns ErrTiFlashColumnarStorageNotEnabled;
//   - flag is "true" or missing: allowed (missing defaults to allowed for backward compatibility);
//   - keyspace meta cannot be loaded from PD: fail-closed, returns ErrTiFlashColumnarStorageCheckFailed.
func checkColumnarStorageEnabled(store kv.Storage) error {
	if !config.GetGlobalConfig().CSE.IsColumnarStoreEnabled() {
		return nil
	}
	if store == nil || store.GetKeyspace() == "" {
		// Self-hosted / null keyspace does not participate in the gate.
		return nil
	}
	storeWithPD, ok := store.(kv.StorageWithPD)
	if !ok || storeWithPD.GetPDClient() == nil {
		return nil
	}
	failpoint.Inject("mockColumnarStorageEnabledResult", func(val failpoint.Value) {
		if v, ok := val.(string); ok {
			switch v {
			case "not-enabled":
				failpoint.Return(dbterror.ErrTiFlashColumnarStorageNotEnabled.GenWithStackByArgs(store.GetKeyspace()))
			case "check-failed":
				failpoint.Return(dbterror.ErrTiFlashColumnarStorageCheckFailed.GenWithStackByArgs(store.GetKeyspace()))
			}
		}
	})
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	meta, err := storeWithPD.GetPDClient().LoadKeyspace(ctx, store.GetKeyspace())
	if err != nil {
		// PD is the ground truth of keyspace info. When the enabled status cannot be
		// verified, reject the DDL instead of allowing the optimizer to route queries
		// to a non-existent tiflash compute node later.
		logutil.DDLLogger().Error("failed to load keyspace meta for columnar storage check",
			zap.String("keyspace", store.GetKeyspace()), zap.Error(err))
		return dbterror.ErrTiFlashColumnarStorageCheckFailed.GenWithStackByArgs(store.GetKeyspace())
	}
	if meta == nil || meta.Config == nil {
		return nil
	}
	if val, ok := meta.Config[keyspace.KeyspaceConfigColumnarStorageEnabled]; ok {
		if strings.EqualFold(val, "false") {
			return dbterror.ErrTiFlashColumnarStorageNotEnabled.GenWithStackByArgs(store.GetKeyspace())
		}
		return nil
	}
	// Flag missing: default to enabled for backward compatibility. Log a warning so gaps are discoverable.
	logutil.DDLLogger().Warn("columnar storage enabled flag is missing in keyspace meta, default to enabled",
		zap.String("keyspace", store.GetKeyspace()))
	return nil
}
