// Copyright 2015 PingCAP, Inc.
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

package domain

import (
	"context"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/daemon"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	opt "github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
)

func (do *Domain) initLogBackup(ctx context.Context, pdClient pd.Client) error {
	cfg := config.GetGlobalConfig()
	if pdClient == nil || do.etcdClient == nil {
		log.Warn("pd / etcd client not provided, won't begin Advancer.")
		return nil
	}
	tikvStore, ok := do.Store().(tikv.Storage)
	if !ok {
		log.Warn("non tikv store, stop begin Advancer.")
		return nil
	}
	env, err := streamhelper.TiDBEnv(tikvStore, pdClient, do.etcdClient, cfg)
	if err != nil {
		return err
	}
	adv := streamhelper.NewTiDBCheckpointAdvancer(env)
	do.brOwnerMgr = streamhelper.OwnerManagerForLogBackup(ctx, do.etcdClient)
	do.logBackupAdvancer = daemon.New(adv, do.brOwnerMgr, adv.Config().TickTimeout())
	loop, err := do.logBackupAdvancer.Begin(ctx)
	if err != nil {
		return err
	}
	do.wg.Run(loop, "logBackupAdvancer")
	return nil
}

// when tidb_replica_read = 'closest-adaptive', check tidb and tikv's zone label matches.
// if not match, disable replica_read to avoid uneven read traffic distribution.
func (do *Domain) closestReplicaReadCheckLoop(ctx context.Context, pdClient pd.Client) {
	defer util.Recover(metrics.LabelDomain, "closestReplicaReadCheckLoop", nil, false)

	// trigger check once instantly.
	if err := do.checkReplicaRead(ctx, pdClient); err != nil {
		logutil.BgLogger().Warn("refresh replicaRead flag failed", zap.Error(err))
	}

	ticker := time.NewTicker(time.Minute)
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("closestReplicaReadCheckLoop exited.")
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := do.checkReplicaRead(ctx, pdClient); err != nil {
				logutil.BgLogger().Warn("refresh replicaRead flag failed", zap.Error(err))
			}
		}
	}
}

// Periodically check and update the replica-read status when `tidb_replica_read` is set to "closest-adaptive"
// We disable "closest-adaptive" in following conditions to ensure the read traffic is evenly distributed across
// all AZs:
// - There are no TiKV servers in the AZ of this tidb instance
// - The AZ if this tidb contains more tidb than other AZ and this tidb's id is the bigger one.
func (do *Domain) checkReplicaRead(ctx context.Context, pdClient pd.Client) error {
	do.sysVarCache.RLock()
	replicaRead := do.sysVarCache.global[vardef.TiDBReplicaRead]
	do.sysVarCache.RUnlock()

	if !strings.EqualFold(replicaRead, "closest-adaptive") {
		logutil.BgLogger().Debug("closest replica read is not enabled, skip check!", zap.String("mode", replicaRead))
		return nil
	}

	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return err
	}
	zone := ""
	for k, v := range serverInfo.Labels {
		if k == placement.DCLabelKey && v != "" {
			zone = v
			break
		}
	}
	if zone == "" {
		logutil.BgLogger().Debug("server contains no 'zone' label, disable closest replica read", zap.Any("labels", serverInfo.Labels))
		variable.SetEnableAdaptiveReplicaRead(false)
		return nil
	}

	stores, err := pdClient.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		return err
	}

	storeZones := make(map[string]int)
	for _, s := range stores {
		// skip tumbstone stores or tiflash
		if s.NodeState == metapb.NodeState_Removing || s.NodeState == metapb.NodeState_Removed || engine.IsTiFlash(s) {
			continue
		}
		for _, label := range s.Labels {
			if label.Key == placement.DCLabelKey && label.Value != "" {
				storeZones[label.Value] = 0
				break
			}
		}
	}

	// no stores in this AZ
	if _, ok := storeZones[zone]; !ok {
		variable.SetEnableAdaptiveReplicaRead(false)
		return nil
	}

	servers, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		return err
	}
	svrIDsInThisZone := make([]string, 0)
	for _, s := range servers {
		if v, ok := s.Labels[placement.DCLabelKey]; ok && v != "" {
			if _, ok := storeZones[v]; ok {
				storeZones[v]++
				if v == zone {
					svrIDsInThisZone = append(svrIDsInThisZone, s.ID)
				}
			}
		}
	}
	enabledCount := math.MaxInt
	for _, count := range storeZones {
		if count < enabledCount {
			enabledCount = count
		}
	}
	// sort tidb in the same AZ by ID and disable the tidb with bigger ID
	// because ID is unchangeable, so this is a simple and stable algorithm to select
	// some instances across all tidb servers.
	if enabledCount < len(svrIDsInThisZone) {
		sort.Slice(svrIDsInThisZone, func(i, j int) bool {
			return strings.Compare(svrIDsInThisZone[i], svrIDsInThisZone[j]) < 0
		})
	}
	enabled := true
	if slices.Contains(svrIDsInThisZone[enabledCount:], serverInfo.ID) {
		enabled = false
	}

	if variable.SetEnableAdaptiveReplicaRead(enabled) {
		logutil.BgLogger().Info("tidb server adaptive closest replica read is changed", zap.Bool("enable", enabled))
	}
	return nil
}
