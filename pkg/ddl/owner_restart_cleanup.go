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
//   - If the ownerID is found in `/tidb/server/info`, only keys belonging to stale instances on
//     the same IP:port (same address but different server-info ID) will be deleted.
//   - If the ownerID is NOT found in `/tidb/server/info`, the key will be treated as stale and
//     deleted to avoid being blocked by the lease TTL.
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
	if len(serverInfos) == 0 {
		// Be conservative: if server-info list is unexpectedly empty, don't treat all owner keys as "unknown".
		logutil.DDLLogger().Debug("skip DDL owner key cleanup, empty server infos", zap.String("selfID", selfID))
		return
	}

	staleIDs := make(map[string]struct{})

	if selfInfo, ok := serverInfos[selfID]; ok {
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
	} else {
		// This might happen in unit tests or during early bootstrap. We can still try to
		// clean "unknown" owner keys based on the server-info list.
		logutil.DDLLogger().Debug("self server info not found, only clean unknown owner keys",
			zap.String("selfID", selfID))
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

		_, hasServerInfo := serverInfos[ownerID]
		_, isStaleID := staleIDs[ownerID]
		if !isStaleID && hasServerInfo {
			// Not a stale key from our own IP:port, and not an unknown ID.
			continue
		}
		unknownOwnerID := !hasServerInfo

		// Use a short timeout for each delete, and continue on errors.
		delCtx, delCancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := etcdCli.Delete(delCtx, string(kv.Key))
		delCancel()
		if err != nil {
			logutil.DDLLogger().Info("delete stale DDL owner key failed",
				zap.String("key", string(kv.Key)),
				zap.String("ownerID", ownerID),
				zap.Bool("unknownOwnerID", unknownOwnerID),
				zap.Error(err))
			continue
		}
		deleted++
		logutil.DDLLogger().Info("delete stale DDL owner key",
			zap.String("key", string(kv.Key)),
			zap.String("ownerID", ownerID),
			zap.Bool("unknownOwnerID", unknownOwnerID))
	}

	if deleted > 0 {
		logutil.DDLLogger().Info("DDL owner key cleanup finished", zap.Int("deleted", deleted))
	}
}
