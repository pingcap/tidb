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
	"encoding/json"
	"math/rand"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)
func (do *Domain) decodePrivilegeEvent(resp clientv3.WatchResponse) PrivilegeEvent {
	var msg PrivilegeEvent
	isNewVersionEvents := false
	for _, event := range resp.Events {
		if event.Kv != nil {
			val := event.Kv.Value
			if len(val) > 0 {
				var tmp PrivilegeEvent
				err := json.Unmarshal(val, &tmp)
				if err != nil {
					logutil.BgLogger().Warn("decodePrivilegeEvent unmarshal fail", zap.Error(err))
					break
				}
				isNewVersionEvents = true
				if do.ServerID() != 0 && tmp.ServerID == do.ServerID() {
					// Skip the events from this TiDB-Server
					continue
				}
				if tmp.All {
					msg.All = true
					break
				}
				// duplicated users in list is ok.
				msg.UserList = append(msg.UserList, tmp.UserList...)
			}
		}
	}

	// In case old version triggers the event, the event value is empty,
	// Then we fall back to the old way: reload all the users.
	if len(msg.UserList) == 0 && !isNewVersionEvents {
		msg.All = true
	}
	return msg
}

func (do *Domain) batchReadMoreData(ch clientv3.WatchChan, event PrivilegeEvent) PrivilegeEvent {
	timer := time.NewTimer(5 * time.Millisecond)
	defer timer.Stop()
	const maxBatchSize = 128
	for range maxBatchSize {
		select {
		case resp, ok := <-ch:
			if !ok {
				return event
			}
			tmp := do.decodePrivilegeEvent(resp)
			if tmp.All {
				event.All = true
			} else {
				if !event.All {
					event.UserList = append(event.UserList, tmp.UserList...)
				}
			}
			succ := timer.Reset(5 * time.Millisecond)
			if !succ {
				return event
			}
		case <-timer.C:
			return event
		}
	}
	return event
}

// LoadPrivilegeLoop create a goroutine loads privilege tables in a loop, it
// should be called only once in BootstrapSession.
func (do *Domain) LoadPrivilegeLoop(sctx sessionctx.Context) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	sctx.GetSessionVars().InRestrictedSQL = true
	_, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, "set @@autocommit = 1")
	if err != nil {
		return err
	}
	do.privHandle = privileges.NewHandle(do.SysSessionPool(), sctx.GetSessionVars().GlobalVarsAccessor)

	var watchCh clientv3.WatchChan
	duration := 5 * time.Minute
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(do.ctx, privilegeKey)
		duration = 10 * time.Minute
	}

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("loadPrivilegeInLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "loadPrivilegeInLoop", nil, false)

		var count int
		for {
			var event PrivilegeEvent
			select {
			case <-do.exit:
				return
			case resp, ok := <-watchCh:
				if ok {
					count = 0
					event = do.decodePrivilegeEvent(resp)
					event = do.batchReadMoreData(watchCh, event)
				} else {
					if do.ctx.Err() == nil {
						logutil.BgLogger().Warn("load privilege loop watch channel closed")
						watchCh = do.etcdClient.Watch(do.ctx, privilegeKey)
						count++
						if count > 10 {
							time.Sleep(time.Duration(count) * time.Second)
						}
						continue
					}
				}
			case <-time.After(duration):
				event.All = true
				event = do.batchReadMoreData(watchCh, event)
			}

			// All events are from this TiDB-Server, skip them
			if !event.All && len(event.UserList) == 0 {
				continue
			}

			err := privReloadEvent(do.privHandle, &event)
			metrics.LoadPrivilegeCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				logutil.BgLogger().Error("load privilege failed", zap.Error(err))
			}
		}
	}, "loadPrivilegeInLoop")
	return nil
}

func privReloadEvent(h *privileges.Handle, event *PrivilegeEvent) (err error) {
	switch {
	case !vardef.AccelerateUserCreationUpdate.Load():
		err = h.UpdateAll()
	case event.All:
		err = h.UpdateAllActive()
	default:
		err = h.Update(event.UserList)
	}
	return
}

// LoadSysVarCacheLoop create a goroutine loads sysvar cache in a loop,
// it should be called only once in BootstrapSession.
func (do *Domain) LoadSysVarCacheLoop(ctx sessionctx.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	err := do.rebuildSysVarCache(ctx)
	if err != nil {
		return err
	}
	var watchCh clientv3.WatchChan
	duration := 30 * time.Second
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(context.Background(), sysVarCacheKey)
	}

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("LoadSysVarCacheLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "LoadSysVarCacheLoop", nil, false)

		var count int
		for {
			ok := true
			select {
			case <-do.exit:
				return
			case _, ok = <-watchCh:
			case <-time.After(duration):
			}

			failpoint.Inject("skipLoadSysVarCacheLoop", func(val failpoint.Value) {
				// In some pkg integration test, there are many testSuite, and each testSuite has separate storage and
				// `LoadSysVarCacheLoop` background goroutine. Then each testSuite `RebuildSysVarCache` from it's
				// own storage.
				// Each testSuit will also call `checkEnableServerGlobalVar` to update some local variables.
				// That's the problem, each testSuit use different storage to update some same local variables.
				// So just skip `RebuildSysVarCache` in some integration testing.
				if val.(bool) {
					failpoint.Continue()
				}
			})

			if !ok {
				logutil.BgLogger().Warn("LoadSysVarCacheLoop loop watch channel closed")
				watchCh = do.etcdClient.Watch(context.Background(), sysVarCacheKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}
			count = 0
			logutil.BgLogger().Debug("Rebuilding sysvar cache from etcd watch event.")
			err := do.rebuildSysVarCache(ctx)
			metrics.LoadSysVarCacheCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				logutil.BgLogger().Warn("LoadSysVarCacheLoop failed", zap.Error(err))
			}
		}
	}, "LoadSysVarCacheLoop")
	return nil
}

// WatchTiFlashComputeNodeChange create a routine to watch if the topology of tiflash_compute node is changed.
// TODO: tiflashComputeNodeKey is not put to etcd yet(finish this when AutoScaler is done)
//
//	store cache will only be invalidated every n seconds.
func (do *Domain) WatchTiFlashComputeNodeChange() error {
	var watchCh clientv3.WatchChan
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(context.Background(), tiflashComputeNodeKey)
	}
	duration := 10 * time.Second
	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("WatchTiFlashComputeNodeChange exit")
		}()
		defer util.Recover(metrics.LabelDomain, "WatchTiFlashComputeNodeChange", nil, false)

		var count int
		var logCount int
		for {
			ok := true
			var watched bool
			select {
			case <-do.exit:
				return
			case _, ok = <-watchCh:
				watched = true
			case <-time.After(duration):
			}
			if !ok {
				logutil.BgLogger().Error("WatchTiFlashComputeNodeChange watch channel closed")
				watchCh = do.etcdClient.Watch(context.Background(), tiflashComputeNodeKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}
			count = 0
			switch s := do.store.(type) {
			case tikv.Storage:
				logCount++
				s.GetRegionCache().InvalidateTiFlashComputeStores()
				if logCount == 6 {
					// Print log every 6*duration seconds.
					logutil.BgLogger().Debug("tiflash_compute store cache invalied, will update next query", zap.Bool("watched", watched))
					logCount = 0
				}
			default:
				logutil.BgLogger().Debug("No need to watch tiflash_compute store cache for non-tikv store")
				return
			}
		}
	}, "WatchTiFlashComputeNodeChange")
	return nil
}

// PrivilegeHandle returns the MySQLPrivilege.
func (do *Domain) PrivilegeHandle() *privileges.Handle {
	return do.privHandle
}

// BindingHandle returns domain's bindHandle.
func (do *Domain) BindingHandle() bindinfo.BindingHandle {
	v := do.bindHandle.Load()
	if v == nil {
		return nil
	}
	return v.(bindinfo.BindingHandle)
}

// InitBindingHandle create a goroutine loads BindInfo in a loop, it should
// be called only once in BootstrapSession.
func (do *Domain) InitBindingHandle() error {
	do.bindHandle.Store(bindinfo.NewBindingHandle(do.sysSessionPool))
	err := do.BindingHandle().LoadFromStorageToCache(true, false)
	if err != nil || bindinfo.Lease == 0 {
		return err
	}

	owner := do.NewOwnerManager(bindinfo.Prompt, bindinfo.OwnerKey)
	err = owner.CampaignOwner()
	if err != nil {
		logutil.BgLogger().Warn("campaign owner failed", zap.Error(err))
		return err
	}
	do.globalBindHandleWorkerLoop(owner)
	return nil
}

func (do *Domain) globalBindHandleWorkerLoop(owner owner.Manager) {
	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("globalBindHandleWorkerLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "globalBindHandleWorkerLoop", nil, false)

		bindWorkerTicker := time.NewTicker(bindinfo.Lease)
		gcBindTicker := time.NewTicker(100 * bindinfo.Lease)
		writeBindingUsageTicker := time.NewTicker(100 * bindinfo.Lease)
		defer func() {
			bindWorkerTicker.Stop()
			gcBindTicker.Stop()
			writeBindingUsageTicker.Stop()
		}()
		for {
			select {
			case <-do.exit:
				do.BindingHandle().Close()
				owner.Close()
				return
			case <-bindWorkerTicker.C:
				bindHandle := do.BindingHandle()
				err := bindHandle.LoadFromStorageToCache(false, false)
				if err != nil {
					logutil.BgLogger().Error("update bindinfo failed", zap.Error(err))
				}
			case <-gcBindTicker.C:
				if !owner.IsOwner() {
					continue
				}
				err := do.BindingHandle().GCBinding()
				if err != nil {
					logutil.BgLogger().Error("GC bind record failed", zap.Error(err))
				}
			case <-writeBindingUsageTicker.C:
				bindHandle := do.BindingHandle()
				err := bindHandle.UpdateBindingUsageInfoToStorage()
				if err != nil {
					logutil.BgLogger().Warn("BindingHandle.UpdateBindingUsageInfoToStorage", zap.Error(err))
				}
				// randomize the next write interval to avoid thundering herd problem
				// if there are many tidb servers. The next write interval is [3h, 6h].
				writeBindingUsageTicker.Reset(
					randomDuration(
						3*60*60, // 3h
						6*60*60, // 6h
					),
				)
			}
		}
	}, "globalBindHandleWorkerLoop")
}

func randomDuration(minSeconds, maxSeconds int) time.Duration {
	randomIntervalSeconds := rand.Intn(maxSeconds-minSeconds+1) + minSeconds
	newDuration := time.Duration(randomIntervalSeconds) * time.Second
	return newDuration
}
