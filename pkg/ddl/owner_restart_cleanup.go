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
	"net"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
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
//   - In multi-node cluster, only keys belonging to stale instances (same IP:port but older
//     server-info ID) will be deleted.
//   - In single-node cluster, keys that are not owned by the current instance are deleted,
//     even if the stale instance has already removed its server info.
func cleanupStaleDDLOwnerKeys(ctx context.Context, etcdCli *clientv3.Client, selfID string) {
	if etcdCli == nil || selfID == "" {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, etcd.KeyOpDefaultTimeout)
	defer cancel()

	serverInfos, err := infosync.GetAllServerInfo(ctx)
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

	selfExecID := net.JoinHostPort(selfInfo.IP, strconv.FormatUint(uint64(selfInfo.Port), 10))
	bestByExecID := make(map[string]*serverinfo.ServerInfo, len(serverInfos))
	staleIDs := make(map[string]struct{})

	// Find stale server-info IDs: multiple IDs can exist for the same IP:port after
	// an ungraceful restart.
	for _, info := range serverInfos {
		execID := net.JoinHostPort(info.IP, strconv.FormatUint(uint64(info.Port), 10))
		prev, ok := bestByExecID[execID]
		if !ok {
			bestByExecID[execID] = info
			continue
		}
		// StartTimestamp is seconds-level (time.Now().Unix()). When it ties, we use ID
		// string order as a deterministic tie-breaker for this best-effort cleanup.
		if info.StartTimestamp > prev.StartTimestamp ||
			(info.StartTimestamp == prev.StartTimestamp && info.ID > prev.ID) {
			staleIDs[prev.ID] = struct{}{}
			bestByExecID[execID] = info
			continue
		}
		staleIDs[info.ID] = struct{}{}
	}
	delete(staleIDs, selfID)

	// Extra safeguard: if the cluster currently only has one IP:port (ours) and the best
	// server-info entry is also ours, then any other DDL owner key must be stale.
	singleNodeAndSelfIsBest := len(bestByExecID) == 1 &&
		bestByExecID[selfExecID] != nil &&
		bestByExecID[selfExecID].ID == selfID

	if len(staleIDs) == 0 && !singleNodeAndSelfIsBest {
		return
	}

	prefix := DDLOwnerKey + "/"
	getResp, err := etcdCli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		logutil.DDLLogger().Debug("skip DDL owner key cleanup, list owner keys failed", zap.Error(err))
		return
	}

	deleted := 0
	for _, kv := range getResp.Kvs {
		ownerIDBytes, _ := owner.SplitOwnerValues(kv.Value)
		ownerID := string(ownerIDBytes)
		if ownerID == selfID {
			continue
		}
		_, isStaleID := staleIDs[ownerID]
		if !isStaleID && !singleNodeAndSelfIsBest {
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
			zap.String("ownerID", ownerID),
			zap.Bool("singleNodeCleanup", singleNodeAndSelfIsBest))
	}

	if deleted > 0 {
		logutil.DDLLogger().Info("DDL owner key cleanup finished", zap.Int("deleted", deleted))
	}
}
