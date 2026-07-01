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
	"os"
	"strconv"
	"strings"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

const (
	defaultDegradedRUFillRate      = 2_000_000
	defaultDegradedRUBurstLimit    = 50_000_000_000
	defaultDegradedModeWaitTimeout = 3 * time.Second / 2
	vipWaitRetryInterval           = 100 * time.Millisecond
	vipWaitRetryTimes              = 20
)

func newDefaultDegradedRUSettings() *rmpb.GroupRequestUnitSettings {
	return &rmpb.GroupRequestUnitSettings{
		RU: &rmpb.TokenBucket{
			Settings: &rmpb.TokenLimitSettings{
				FillRate:   defaultDegradedRUFillRate,
				BurstLimit: defaultDegradedRUBurstLimit,
			},
		},
	}
}

func newResourceGroupsControllerOptions() []rmclient.ResourceControlCreateOption {
	opts := []rmclient.ResourceControlCreateOption{
		rmclient.WithMaxWaitDuration(runaway.MaxWaitDuration),
		rmclient.WithDegradedModeWaitDuration(defaultDegradedModeWaitTimeout),
		rmclient.WithDegradedRUSettings(newDefaultDegradedRUSettings()),
	}
	if strings.Contains(os.Getenv("NAMESPACE"), "vip") {
		opts = append(opts,
			rmclient.WithWaitRetryInterval(vipWaitRetryInterval),
			rmclient.WithWaitRetryTimes(vipWaitRetryTimes),
		)
	}
	return opts
}

func (do *Domain) initResourceGroupsController(ctx context.Context, pdClient pd.Client, uniqueID uint64) error {
	if pdClient == nil {
		logutil.BgLogger().Warn("cannot setup up resource controller, not using tikv storage")
		// return nil as unistore doesn't support it
		return nil
	}

	keyspaceID := constants.NullKeyspaceID
	if codec := do.Store().GetCodec(); codec != nil {
		keyspaceID = uint32(codec.GetKeyspaceID())
	}
	control, err := rmclient.NewResourceGroupController(ctx, uniqueID, pdClient, nil, keyspaceID, newResourceGroupsControllerOptions()...)
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
	do.SetResourceGroupsController(control)
	tikv.SetResourceControlInterceptor(control)
	return nil
}
