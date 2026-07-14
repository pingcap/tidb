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

package domain

import (
	"strings"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

const (
	defaultDegradedRUFillRate      = 2_000_000
	defaultDegradedRUBurstLimit    = 50_000_000_000
	defaultDegradedModeWaitTimeout = 3 * time.Second / 2
	tokenWaitRetryInterval         = 100 * time.Millisecond
	tokenWaitRetryTimes            = 20
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
	}
	if deploymode.IsStarter() {
		opts = append(opts,
			// Keep degraded-group synthesis inside the controller so degraded
			// resource groups are not inserted into the controller cache.
			rmclient.WithDegradedRUSettings(newDefaultDegradedRUSettings()),
			rmclient.WithDegradedModeWaitDuration(defaultDegradedModeWaitTimeout),
		)
	}
	if strings.Contains(config.GetGlobalConfig().StarterParams.PodNamespace, "vip") {
		opts = append(opts,
			rmclient.WithWaitRetryInterval(tokenWaitRetryInterval),
			rmclient.WithWaitRetryTimes(tokenWaitRetryTimes),
		)
	}
	return opts
}
