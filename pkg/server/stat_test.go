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
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestUptime(t *testing.T) {
	var err error

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/serverinfo/mockServerInfo", "return(true)"))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/domain/serverinfo/mockServerInfo")
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

	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), dom.GetEtcdClient(), dom.GetPDClient(), dom.GetPDHTTPClient(), keyspace.CodecV1, true, dom.InfoCache())
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
	origStatsLease := vardef.GetStatsLease()
	defer vardef.SetStatsLease(origStatsLease)
	vardef.SetStatsLease(3 * time.Second)
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
		<-dom.StatsHandle().InitStatsDone

		// Preset pooled internal sessions with PESSIMISTIC TxnMode (OwnerResetState does
		// not reset TxnMode on Put), then verify InitStats / InitStatsLite still enter an
		// optimistic transaction, skip MDL on mysql.stats_*, and report StartTS for GC.
		sPool := dom.StatsHandle().Pool.SPool()
		presetSessions := make([]*syssession.Session, 0, 16)
		for range 16 {
			se, getErr := sPool.Get()
			require.NoError(t, getErr)
			require.NoError(t, se.WithSessionContext(func(sctx sessionctx.Context) error {
				sctx.GetSessionVars().TxnMode = "PESSIMISTIC"
				return nil
			}))
			presetSessions = append(presetSessions, se)
		}
		for _, se := range presetSessions {
			sPool.Put(se)
		}

		pauseFP := "github.com/pingcap/tidb/pkg/statistics/handle/mockBucketsLoadMemoryLimit"
		if lite {
			pauseFP = "github.com/pingcap/tidb/pkg/statistics/handle/afterInitStatsMetaLite"
		}
		require.NoError(t, failpoint.Enable(pauseFP, "pause"))
		pauseFPDisabled := false
		defer func() {
			if !pauseFPDisabled {
				_ = failpoint.Disable(pauseFP)
			}
			_ = failpoint.Disable("github.com/pingcap/tidb/pkg/session/syssession/ForceBlockGCInTest")
		}()
		initDone := make(chan error, 1)
		go func() {
			if lite {
				initDone <- dom.StatsHandle().InitStatsLite(context.Background(), dom.InfoSchema())
			} else {
				initDone <- dom.StatsHandle().InitStats(context.Background(), dom.InfoSchema())
			}
		}()

		var activeStartTS uint64
		require.Eventually(t, func() bool {
			sv.sessionMapMutex.Lock()
			internalSessions := make([]any, 0, len(sv.internalSessions))
			for se := range sv.internalSessions {
				internalSessions = append(internalSessions, se)
			}
			sv.sessionMapMutex.Unlock()

			for _, se := range internalSessions {
				sctx, ok := se.(interface{ GetSessionVars() *variable.SessionVars })
				if !ok {
					continue
				}
				vars := sctx.GetSessionVars()
				vars.TxnCtxMu.Lock()
				startTS := uint64(0)
				isPessimistic := false
				if vars.TxnCtx != nil {
					startTS = vars.TxnCtx.StartTS
					isPessimistic = vars.TxnCtx.IsPessimistic
				}
				vars.TxnCtxMu.Unlock()
				if startTS == 0 || !vars.HasReadOnlySkippedMDL() {
					continue
				}
				require.Equal(t, "PESSIMISTIC", vars.TxnMode)
				require.False(t, isPessimistic)
				mdlCount := 0
				vars.GetRelatedTableForMDL().Range(func(_, _ any) bool {
					mdlCount++
					return true
				})
				require.Zero(t, mdlCount)
				activeStartTS = startTS
				return true
			}
			return false
		}, 10*time.Second, 10*time.Millisecond)
		require.Contains(t, sv.GetInternalSessionStartTSList(), activeStartTS)

		require.NoError(t, failpoint.Disable(pauseFP))
		pauseFPDisabled = true
		require.NoError(t, <-initDone)
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
	// This prevents the session from being created because we do not set a session manager.
	infoSyncer.SetSessionManager(nil)
	h := dom.StatsHandle()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(1 * time.Second)
		cancel()
	}()
	require.ErrorIs(t, h.InitStats(ctx, dom.InfoSchema()), context.Canceled)
	require.ErrorIs(t, h.InitStatsLite(ctx, dom.InfoSchema()), context.Canceled)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/syssession/ForceBlockGCInTest"))
	dom.Close()
	require.NoError(t, store.Close())
}
