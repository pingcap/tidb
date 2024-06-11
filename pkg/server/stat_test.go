// Copyright 2021 PingCAP, Inc.
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

package server

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestUptime(t *testing.T) {
	var err error

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/infosync/mockServerInfo", "return(true)"))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/domain/infosync/mockServerInfo")
		require.NoError(t, err)
	}()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	dom, err := session.BootstrapSession(store)
	defer func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), dom.GetEtcdClient(), dom.GetPDClient(), dom.GetPDHTTPClient(), keyspace.CodecV1, true)
	require.NoError(t, err)

	tidbdrv := NewTiDBDriver(store)
	cfg := util.NewTestConfig()
	cfg.Socket = ""
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)

	stats, err := server.Stats(nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, stats[upTime].(int64), int64(time.Since(time.Unix(1282967700, 0)).Seconds()))
}

func TestInitStatsSessionBlockGC(t *testing.T) {
	origConfig := config.GetGlobalConfig()
	defer func() {
		config.StoreGlobalConfig(origConfig)
	}()
	newConfig := *origConfig
	for _, lite := range []bool{false, true} {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/beforeInitStats", "pause"))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/beforeInitStatsLite", "pause"))
		newConfig.Performance.LiteInitStats = lite
		config.StoreGlobalConfig(&newConfig)

		store, err := mockstore.NewMockStore()
		require.NoError(t, err)
		dom, err := session.BootstrapSession(store)
		require.NoError(t, err)

		infoSyncer := dom.InfoSyncer()
		sv := CreateMockServer(t, store)
		sv.SetDomain(dom)
		infoSyncer.SetSessionManager(sv)
		time.Sleep(time.Second)
		require.Eventually(t, func() bool {
			now := time.Now()
			startTSList := sv.GetInternalSessionStartTSList()
			for _, startTs := range startTSList {
				if startTs != 0 {
					startTime := oracle.GetTimeFromTS(startTs)
					// test pass if the min_start_ts is blocked over 1s.
					if now.Sub(startTime) > time.Second {
						return true
					}
				}
			}
			return false
		}, 10*time.Second, 10*time.Millisecond, "min_start_ts is not blocked over 1s")
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/beforeInitStats"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/beforeInitStatsLite"))
		dom.Close()
		require.NoError(t, store.Close())
	}
}
