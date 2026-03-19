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

package initstats

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	server2 "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestInitStatsSessionBlockGC(t *testing.T) {
	origConfig := config.GetGlobalConfig()
	defer func() {
		config.StoreGlobalConfig(origConfig)
	}()
	newConfig := *origConfig
	for _, lite := range []bool{false, true} {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/beforeInitStats", "pause"))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/beforeInitStatsLite", "pause"))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/syssession/ForceBlockGCInTest", "return(true)"))
		newConfig.Performance.LiteInitStats = lite
		config.StoreGlobalConfig(&newConfig)

		store, err := mockstore.NewMockStore()
		require.NoError(t, err)
		dom, err := session.BootstrapSession(store)
		require.NoError(t, err)

		infoSyncer := dom.InfoSyncer()
		sv := server2.CreateMockServer(t, store)
		sv.SetDomain(dom)
		infoSyncer.SetSessionManager(sv)
		time.Sleep(time.Second)
		require.Eventually(t, func() bool {
			now := time.Now()
			startTSList := sv.GetInternalSessionStartTSList()
			for _, startTS := range startTSList {
				if startTS != 0 {
					startTime := oracle.GetTimeFromTS(startTS)
					if now.Sub(startTime) > time.Second {
						return true
					}
				}
			}
			return false
		}, 10*time.Second, 10*time.Millisecond, "min_start_ts is not blocked over 1s")
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/beforeInitStats"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/beforeInitStatsLite"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/syssession/ForceBlockGCInTest"))
		dom.Close()
		require.NoError(t, store.Close())
	}
}

func TestInitStatsSessionBlockGCCanBeCanceled(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/syssession/ForceBlockGCInTest", "return(true)"))

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	infoSyncer := dom.InfoSyncer()
	infoSyncer.SetSessionManager(nil)
	h := dom.StatsHandle()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Second)
		cancel()
	}()
	require.ErrorIs(t, h.InitStats(ctx, dom.InfoSchema()), context.Canceled)
	require.ErrorIs(t, h.InitStatsLite(ctx), context.Canceled)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/syssession/ForceBlockGCInTest"))
	dom.Close()
	require.NoError(t, store.Close())
}
