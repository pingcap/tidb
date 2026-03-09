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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/util/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// cleanupStaleDDLOwnerKeys tries to remove stale DDL owner campaign keys left by a previous
// TiDB process that has already been replaced by a newer process on the same IP:port.
//
// Why we need it:
//   - The DDL owner election key is stored with an etcd lease (default TTL is 60s).
//   - If the previous TiDB exits abnormally, or it fails to revoke the lease, the key can
//     remain until TTL expiry and block the new DDL owner election, making the first DDL
//     after restart slow.
//
// This is a best-effort cleanup:
//   - Only keys belonging to stale instances on the same IP:port (same address but different
//     server-info ID) will be deleted.
func cleanupStaleDDLOwnerKeys(ctx context.Context, etcdCli *clientv3.Client, selfID string) {
	if etcdCli == nil || selfID == "" {
		return
	}

	listCtx, cancel := context.WithTimeout(ctx, etcd.KeyOpDefaultTimeout)
	defer cancel()

	serverInfos, err := infosync.GetAllServerInfo(listCtx)
	if err != nil {
		logutil.DDLLogger().Debug("skip DDL owner key cleanup, get server infos failed", zap.Error(err))
		return
	}
	selfInfo, ok := serverInfos[selfID]
	if !ok {
		// This might happen in unit tests or during early bootstrap.
		logutil.DDLLogger().Debug("skip DDL owner key cleanup, self server info not found", zap.String("selfID", selfID))
		return
	}

	staleIDs := make(map[string]struct{})

	// Multiple server-info IDs can exist for the same IP:port after an ungraceful restart.
	// Only clean keys from other IDs on our own address ("cleanup self keys" only).
	for _, info := range serverInfos {
		if info.IP != selfInfo.IP || info.Port != selfInfo.Port {
			continue
		}
		if info.ID == selfID {
			continue
		}
		staleIDs[info.ID] = struct{}{}
	}

	if len(staleIDs) == 0 {
		return
	}

	prefix := DDLOwnerKey + "/"
	getResp, err := etcdCli.Get(listCtx, prefix, clientv3.WithPrefix())
	if err != nil {
		logutil.DDLLogger().Debug("skip DDL owner key cleanup, list owner keys failed", zap.Error(err))
		return
	}

	// For tests: simulate a slow discovery/list path so the listCtx can expire.
	failpoint.Inject("ddlCleanupStaleDDLOwnerKeysDelayBeforeDelete", func() {})

	deleted := 0
	for _, kv := range getResp.Kvs {
		ownerIDBytes, _ := owner.SplitOwnerValues(kv.Value)
		ownerID := string(ownerIDBytes)
		if ownerID == selfID {
			continue
		}
		_, isStaleID := staleIDs[ownerID]
		if !isStaleID {
			continue
		}

		// Use a short timeout for each delete, and continue on errors.
		delCtx, delCancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := etcdCli.Delete(delCtx, string(kv.Key))
		delCancel()
		if err != nil {
			logutil.DDLLogger().Info("delete stale DDL owner key failed",
				zap.String("key", string(kv.Key)),
				zap.String("ownerID", ownerID),
				zap.Error(err))
			continue
		}
		deleted++
		logutil.DDLLogger().Info("delete stale DDL owner key",
			zap.String("key", string(kv.Key)),
			zap.String("ownerID", ownerID))
	}

	if deleted > 0 {
		logutil.DDLLogger().Info("DDL owner key cleanup finished", zap.Int("deleted", deleted))
	}
}
