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

package domain

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/zap"
)

const (
	runawayWatchSyncInterval         = time.Second
	runawayLoopLogErrorIntervalCount = 1800
)

func (do *Domain) initResourceGroupsController(ctx context.Context, pdClient pd.Client, uniqueID uint64) error {
	if pdClient == nil {
		logutil.BgLogger().Warn("cannot setup up resource controller, not using tikv storage")
		// return nil as unistore doesn't support it
		return nil
	}

	control, err := rmclient.NewResourceGroupController(ctx, uniqueID, pdClient, nil, rmclient.WithMaxWaitDuration(runaway.MaxWaitDuration))
	if err != nil {
		return err
	}
	control.Start(ctx)
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return err
	}
	serverAddr := net.JoinHostPort(serverInfo.IP, strconv.Itoa(int(serverInfo.Port)))
	do.runawayManager = runaway.NewRunawayManager(control, serverAddr,
		do.sysSessionPool, do.exit, do.infoCache, do.ddl)
	do.resourceGroupsController = control
	tikv.SetResourceControlInterceptor(control)
	return nil
}

func (do *Domain) runawayStartLoop() {
	defer util.Recover(metrics.LabelDomain, "runawayStartLoop", nil, false)
	runawayWatchSyncTicker := time.NewTicker(runawayWatchSyncInterval)
	count := 0
	var err error
	logutil.BgLogger().Info("try to start runaway manager loop")
	for {
		select {
		case <-do.exit:
			return
		case <-runawayWatchSyncTicker.C:
			// Due to the watch and watch done tables is created later than runaway queries table
			err = do.runawayManager.UpdateNewAndDoneWatch()
			if err == nil {
				logutil.BgLogger().Info("preparations for the runaway manager are finished and start runaway manager loop")
				do.wg.Run(do.runawayManager.RunawayRecordFlushLoop, "runawayRecordFlushLoop")
				do.wg.Run(do.runawayManager.RunawayWatchSyncLoop, "runawayWatchSyncLoop")
				do.runawayManager.MarkSyncerInitialized()
				return
			}
		}
		if count %= runawayLoopLogErrorIntervalCount; count == 0 {
			logutil.BgLogger().Warn(
				"failed to start runaway manager loop, please check whether the bootstrap or update is finished",
				zap.Error(err))
		}
		count++
	}
}
