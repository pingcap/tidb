// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runaway

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/zap"
)

const (
	// ManualSource shows the item added manually.
	ManualSource = "manual"

	// MaxWaitDuration is the max duration to wait for acquiring token buckets.
	MaxWaitDuration           = time.Second * 30
	maxWatchListCap           = 10000
	maxWatchRecordChannelSize = 1024

	runawayRecordFlushInterval   = time.Second
	runawayRecordGCInterval      = time.Hour * 24
	runawayRecordExpiredDuration = time.Hour * 24 * 7

	runawayRecordGCBatchSize         = 100
	runawayRecordGCSelectBatchSize   = runawayRecordGCBatchSize * 5
	runawayLoopLogErrorIntervalCount = 1800
)

// Manager is used to detect and record runaway queries.
type Manager struct {
	logOnce sync.Once
	exit    chan struct{}
	// queryLock is used to avoid repeated additions. Since we will add new items to the system table,
	// in order to avoid repeated additions, we need a lock to ensure that
	// action "judging whether there is this record in the current watch list and adding records" have atomicity.
	queryLock sync.Mutex
	watchList *ttlcache.Cache[string, *QuarantineRecord]
	// activeGroup is used to manage the active runaway watches of resource group
	ActiveGroup map[string]int64
	ActiveLock  sync.RWMutex
	MetricsMap  generic.SyncMap[string, prometheus.Counter]

	ResourceGroupCtl   *rmclient.ResourceGroupsController
	serverID           string
	runawayQueriesChan chan *Record
	quarantineChan     chan *QuarantineRecord
	// staleQuarantineRecord is used to clean outdated record. There are three scenarios:
	// 1. Record is expired in watch list.
	// 2. The record that will be added is itself out of date.
	// 	  Like that tidb cluster is paused, and record is expired when restarting.
	// 3. Duplicate added records.
	// It replaces clean up loop.
	staleQuarantineRecord chan *QuarantineRecord
	evictionCancel        func()
	insertionCancel       func()

	syncerInitialized atomic.Bool

	// domain related fields
	infoCache *infoschema.InfoCache
	ddl       ddl.DDL

	// syncer is used to sync runaway watch records.
	runawaySyncer  *syncer
	sysSessionPool util.SessionPool
}

// NewRunawayManager creates a new Manager.
func NewRunawayManager(resourceGroupCtl *rmclient.ResourceGroupsController, serverAddr string,
	pool util.SessionPool, exit chan struct{}, infoCache *infoschema.InfoCache, ddl ddl.DDL) *Manager {
	watchList := ttlcache.New[string, *QuarantineRecord](
		ttlcache.WithTTL[string, *QuarantineRecord](ttlcache.NoTTL),
		ttlcache.WithCapacity[string, *QuarantineRecord](maxWatchListCap),
		ttlcache.WithDisableTouchOnHit[string, *QuarantineRecord](),
	)
	go watchList.Start()
	staleQuarantineChan := make(chan *QuarantineRecord, maxWatchRecordChannelSize)
	m := &Manager{
		syncerInitialized:     atomic.Bool{},
		ResourceGroupCtl:      resourceGroupCtl,
		watchList:             watchList,
		serverID:              serverAddr,
		runawayQueriesChan:    make(chan *Record, maxWatchRecordChannelSize),
		quarantineChan:        make(chan *QuarantineRecord, maxWatchRecordChannelSize),
		staleQuarantineRecord: staleQuarantineChan,
		ActiveGroup:           make(map[string]int64),
		MetricsMap:            generic.NewSyncMap[string, prometheus.Counter](8),
		sysSessionPool:        pool,
		exit:                  exit,
		infoCache:             infoCache,
		ddl:                   ddl,
	}
	m.insertionCancel = watchList.OnInsertion(func(_ context.Context, i *ttlcache.Item[string, *QuarantineRecord]) {
		m.ActiveLock.Lock()
		m.ActiveGroup[i.Value().ResourceGroupName]++
		m.ActiveLock.Unlock()
	})
	m.evictionCancel = watchList.OnEviction(func(_ context.Context, _ ttlcache.EvictionReason, i *ttlcache.Item[string, *QuarantineRecord]) {
		m.ActiveLock.Lock()
		m.ActiveGroup[i.Value().ResourceGroupName]--
		m.ActiveLock.Unlock()
		if i.Value().ID == 0 {
			return
		}
		staleQuarantineChan <- i.Value()
	})
	m.runawaySyncer = newSyncer(pool)

	return m
}

// RunawayRecordFlushLoop is used to flush runaway records.
func (rm *Manager) RunawayRecordFlushLoop() {
	defer util.Recover(metrics.LabelDomain, "runawayRecordFlushLoop", nil, false)

	// this times used to batch flushing records, with 1s duration,
	// we can guarantee a watch record can be seen by the user within 1s.
	runawayRecordFlushTimer := time.NewTimer(runawayRecordFlushInterval)
	runawayRecordGCTicker := time.NewTicker(runawayRecordGCInterval)
	failpoint.Inject("FastRunawayGC", func() {
		runawayRecordFlushTimer.Stop()
		runawayRecordGCTicker.Stop()
		runawayRecordFlushTimer = time.NewTimer(time.Millisecond * 50)
		runawayRecordGCTicker = time.NewTicker(time.Millisecond * 200)
	})

	fired := false
	recordCh := rm.runawayRecordChan()
	quarantineRecordCh := rm.quarantineRecordChan()
	staleQuarantineRecordCh := rm.staleQuarantineRecordChan()
	flushThreshold := flushThreshold()
	records := make([]*Record, 0, flushThreshold)

	flushRunawayRecords := func() {
		if len(records) == 0 {
			return
		}
		sql, params := genRunawayQueriesStmt(records)
		if _, err := ExecRCRestrictedSQL(rm.sysSessionPool, sql, params); err != nil {
			logutil.BgLogger().Error("flush runaway records failed", zap.Error(err), zap.Int("count", len(records)))
		}
		records = records[:0]
	}

	for {
		select {
		case <-rm.exit:
			return
		case <-runawayRecordFlushTimer.C:
			flushRunawayRecords()
			fired = true
		case r := <-recordCh:
			records = append(records, r)
			failpoint.Inject("FastRunawayGC", func() {
				flushRunawayRecords()
			})
			if len(records) >= flushThreshold {
				flushRunawayRecords()
			} else if fired {
				fired = false
				// meet a new record, reset the timer.
				runawayRecordFlushTimer.Reset(runawayRecordFlushInterval)
			}
		case <-runawayRecordGCTicker.C:
			go rm.deleteExpiredRows(runawayRecordExpiredDuration)
		case r := <-quarantineRecordCh:
			go func() {
				_, err := rm.AddRunawayWatch(r)
				if err != nil {
					logutil.BgLogger().Error("add runaway watch", zap.Error(err))
				}
			}()
		case r := <-staleQuarantineRecordCh:
			go func() {
				for i := 0; i < 3; i++ {
					err := handleRemoveStaleRunawayWatch(rm.sysSessionPool, r)
					if err == nil {
						break
					}
					logutil.BgLogger().Error("remove stale runaway watch", zap.Error(err))
					time.Sleep(time.Second)
				}
			}()
		}
	}
}

// RunawayWatchSyncLoop is used to sync runaway watch records.
func (rm *Manager) RunawayWatchSyncLoop() {
	defer util.Recover(metrics.LabelDomain, "runawayWatchSyncLoop", nil, false)
	runawayWatchSyncTicker := time.NewTicker(watchSyncInterval)
	count := 0
	for {
		select {
		case <-rm.exit:
			return
		case <-runawayWatchSyncTicker.C:
			err := rm.UpdateNewAndDoneWatch()
			if err != nil {
				if count %= runawayLoopLogErrorIntervalCount; count == 0 {
					logutil.BgLogger().Warn("get runaway watch record failed", zap.Error(err))
				}
				count++
			}
		}
	}
}

func (rm *Manager) markQuarantine(resourceGroupName, convict string, watchType rmpb.RunawayWatchType, action rmpb.RunawayAction, ttl time.Duration, now *time.Time) {
	var endTime time.Time
	if ttl > 0 {
		endTime = now.UTC().Add(ttl)
	}
	record := &QuarantineRecord{
		ResourceGroupName: resourceGroupName,
		StartTime:         now.UTC(),
		EndTime:           endTime,
		Watch:             watchType,
		WatchText:         convict,
		Source:            rm.serverID,
		Action:            action,
	}
	// Add record without ID into watch list in this TiDB right now.
	rm.addWatchList(record, ttl, false)
	if !rm.syncerInitialized.Load() {
		rm.logOnce.Do(func() {
			logutil.BgLogger().Warn("runaway syncer is not initialized, so can't records about runaway")
		})
		return
	}
	select {
	case rm.quarantineChan <- record:
	default:
		// TODO: add warning for discard flush records
	}
}

func (rm *Manager) addWatchList(record *QuarantineRecord, ttl time.Duration, force bool) {
	key := record.getRecordKey()
	// This is a pre-check, because we generally believe that in most cases, we will not add a watch list to a key repeatedly.
	item := rm.getWatchFromWatchList(key)
	if force {
		rm.queryLock.Lock()
		defer rm.queryLock.Unlock()
		if item != nil {
			// check the ID because of the earlier scan.
			if item.ID == record.ID {
				return
			}
			rm.watchList.Delete(key)
		}
		rm.watchList.Set(key, record, ttl)
	} else {
		if item == nil {
			rm.queryLock.Lock()
			// When watchList get record, it will check whether the record is stale, so add new record if returns nil.
			if rm.watchList.Get(key) == nil {
				rm.watchList.Set(key, record, ttl)
			} else {
				rm.staleQuarantineRecord <- record
			}
			rm.queryLock.Unlock()
		} else if item.ID == 0 {
			// to replace the record without ID.
			rm.queryLock.Lock()
			defer rm.queryLock.Unlock()
			rm.watchList.Set(key, record, ttl)
		} else if item.ID != record.ID {
			// check the ID because of the earlier scan.
			rm.staleQuarantineRecord <- record
		}
	}
}

// GetWatchList is used to get all watch items.
func (rm *Manager) GetWatchList() []*QuarantineRecord {
	items := rm.watchList.Items()
	ret := make([]*QuarantineRecord, 0, len(items))
	for _, item := range items {
		ret = append(ret, item.Value())
	}
	return ret
}

func (rm *Manager) getWatchFromWatchList(key string) *QuarantineRecord {
	item := rm.watchList.Get(key)
	if item != nil {
		return item.Value()
	}
	return nil
}

func (rm *Manager) markRunaway(resourceGroupName, originalSQL, planDigest, action, matchType string, now *time.Time) {
	source := rm.serverID
	if !rm.syncerInitialized.Load() {
		rm.logOnce.Do(func() {
			logutil.BgLogger().Warn("runaway syncer is not initialized, so can't records about runaway")
		})
		return
	}
	select {
	case rm.runawayQueriesChan <- &Record{
		ResourceGroupName: resourceGroupName,
		Time:              *now,
		Match:             matchType,
		Action:            action,
		SQLText:           originalSQL,
		PlanDigest:        planDigest,
		Source:            source,
	}:
	default:
		// TODO: add warning for discard flush records
	}
}

// runawayRecordChan returns the channel of Record
func (rm *Manager) runawayRecordChan() <-chan *Record {
	return rm.runawayQueriesChan
}

// quarantineRecordChan returns the channel of QuarantineRecord
func (rm *Manager) quarantineRecordChan() <-chan *QuarantineRecord {
	return rm.quarantineChan
}

// staleQuarantineRecordChan returns the channel of staleQuarantineRecord
func (rm *Manager) staleQuarantineRecordChan() <-chan *QuarantineRecord {
	return rm.staleQuarantineRecord
}

// examineWatchList check whether the query is in watch list.
func (rm *Manager) examineWatchList(resourceGroupName string, convict string) (bool, rmpb.RunawayAction) {
	item := rm.getWatchFromWatchList(resourceGroupName + "/" + convict)
	if item == nil {
		return false, 0
	}
	return true, item.Action
}

// Stop stops the watchList which is a ttlCache.
func (rm *Manager) Stop() {
	if rm == nil {
		return
	}
	if rm.watchList != nil {
		rm.watchList.Stop()
	}
}

// UpdateNewAndDoneWatch is used to update new and done watch items.
func (rm *Manager) UpdateNewAndDoneWatch() error {
	rm.runawaySyncer.mu.Lock()
	defer rm.runawaySyncer.mu.Unlock()
	records, err := rm.runawaySyncer.getNewWatchRecords()
	if err != nil {
		return err
	}
	for _, r := range records {
		rm.AddWatch(r)
	}
	doneRecords, err := rm.runawaySyncer.getNewWatchDoneRecords()
	if err != nil {
		return err
	}
	for _, r := range doneRecords {
		rm.removeWatch(r)
	}
	return nil
}

// AddWatch is used to add watch items from system table.
func (rm *Manager) AddWatch(record *QuarantineRecord) {
	ttl := time.Until(record.EndTime)
	if record.EndTime.Equal(NullTime) {
		ttl = 0
	} else if ttl <= 0 {
		rm.staleQuarantineRecord <- record
		return
	}

	force := false
	// The manual record replaces the old record.
	force = record.Source == ManualSource
	rm.addWatchList(record, ttl, force)
}

// removeWatch is used to remove watch item, and this action is triggered by reading done watch system table.
func (rm *Manager) removeWatch(record *QuarantineRecord) {
	// we should check whether the cached record is not the same as the removing record.
	rm.queryLock.Lock()
	defer rm.queryLock.Unlock()
	item := rm.getWatchFromWatchList(record.getRecordKey())
	if item == nil {
		return
	}
	if item.ID == record.ID {
		rm.watchList.Delete(record.getRecordKey())
	}
}

// FlushThreshold specifies the threshold for the number of records in trigger flush
func flushThreshold() int {
	return maxWatchRecordChannelSize / 2
}

// MarkSyncerInitialized is used to mark the syncer is initialized.
func (rm *Manager) MarkSyncerInitialized() {
	rm.syncerInitialized.Store(true)
}

// IsSyncerInitialized is only used for test.
func (rm *Manager) IsSyncerInitialized() bool {
	return rm.syncerInitialized.Load()
}
