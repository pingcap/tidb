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
	MaxWaitDuration = time.Second * 30
	maxWatchListCap = 10000

	runawayRecordFlushInterval   = 30 * time.Second
	runawayRecordGCInterval      = time.Hour * 24
	runawayRecordExpiredDuration = time.Hour * 24 * 7

	runawayRecordGCBatchSize       = 100
	runawayRecordGCSelectBatchSize = runawayRecordGCBatchSize * 5

	watchRecordFlushInterval = time.Second
)

var sampleLogger = logutil.SampleLoggerFactory(time.Minute, 1, zap.String(logutil.LogFieldCategory, "runaway"))

// Manager is used to detect and record runaway queries.
type Manager struct {
	exit chan struct{}
	// queryLock is used to avoid repeated additions. Since we will add new items to the system table,
	// in order to avoid repeated additions, we need a lock to ensure that
	// action "judging whether there is this record in the current watch list and adding records" have atomicity.
	queryLock sync.Mutex
	watchList *ttlcache.Cache[string, *QuarantineRecord]
	// activeGroup is used to manage the active runaway watches of resource group.
	// It uses sync.Map + atomic.Int64 for lock-free reads on the per-query hot path.
	activeGroup sync.Map // map[string]*atomic.Int64
	MetricsMap  generic.SyncMap[string, prometheus.Counter]

	ResourceGroupCtl *rmclient.ResourceGroupsController
	serverID         string

	runawayRecordFlusher    *batchFlusher[recordKey, *Record]
	quarantineRecordFlusher *batchFlusher[string, *QuarantineRecord]
	staleQuarantineFlusher  *batchFlusher[int64, *QuarantineRecord]

	evictionCancel  func()
	insertionCancel func()

	// domain related fields
	infoCache *infoschema.InfoCache
	ddl       ddl.DDL

	// syncer is used to sync runaway watch records.
	runawaySyncer  *syncer
	sysSessionPool util.SessionPool
}

// NewRunawayManager creates a new Manager.
func NewRunawayManager(
	resourceGroupCtl *rmclient.ResourceGroupsController,
	serverAddr string,
	pool util.SessionPool,
	exit chan struct{},
	infoCache *infoschema.InfoCache,
	ddl ddl.DDL,
) *Manager {
	watchList := ttlcache.New(
		ttlcache.WithTTL[string, *QuarantineRecord](ttlcache.NoTTL),
		ttlcache.WithCapacity[string, *QuarantineRecord](maxWatchListCap),
		ttlcache.WithDisableTouchOnHit[string, *QuarantineRecord](),
	)
	go watchList.Start()

	runawayFlushInterval := runawayRecordFlushInterval
	watchFlushInterval := watchRecordFlushInterval
	failpoint.Inject("FastRunawayGC", func() {
		runawayFlushInterval = time.Millisecond * 50
		watchFlushInterval = time.Millisecond * 50
	})

	m := &Manager{
		ResourceGroupCtl: resourceGroupCtl,
		watchList:        watchList,
		serverID:         serverAddr,
		MetricsMap:       generic.NewSyncMap[string, prometheus.Counter](8),
		sysSessionPool:   pool,
		exit:             exit,
		infoCache:        infoCache,
		ddl:              ddl,
	}

	m.runawayRecordFlusher = newBatchFlusher(
		"runaway-record",
		runawayFlushInterval,
		func(buf map[recordKey]*Record, k recordKey, v *Record) {
			if existing, ok := buf[k]; ok {
				existing.Repeats++
			} else {
				buf[k] = v
			}
		},
		genRunawayQueriesStmt,
		pool,
	)

	m.quarantineRecordFlusher = newBatchFlusher(
		"quarantine-record",
		watchFlushInterval,
		func(buf map[string]*QuarantineRecord, k string, v *QuarantineRecord) {
			if _, ok := buf[k]; !ok {
				buf[k] = v
			}
		},
		genBatchInsertWatchStmt,
		pool,
	)

	m.staleQuarantineFlusher = newBatchFlusher(
		"stale-quarantine-record",
		watchFlushInterval,
		func(buf map[int64]*QuarantineRecord, k int64, v *QuarantineRecord) {
			buf[k] = v
		},
		genBatchDeleteWatchByIDStmt,
		pool,
	)

	m.insertionCancel = watchList.OnInsertion(func(_ context.Context, i *ttlcache.Item[string, *QuarantineRecord]) {
		name := i.Value().ResourceGroupName
		counter, _ := m.loadOrStoreActiveCounter(name)
		counter.Add(1)
	})
	m.evictionCancel = watchList.OnEviction(func(_ context.Context, _ ttlcache.EvictionReason, i *ttlcache.Item[string, *QuarantineRecord]) {
		name := i.Value().ResourceGroupName
		counter, _ := m.loadOrStoreActiveCounter(name)
		counter.Add(-1)
		if i.Value().ID == 0 {
			return
		}
		m.staleQuarantineFlusher.add(i.Value().ID, i.Value())
	})
	m.runawaySyncer = newSyncer(pool, infoCache)

	return m
}

// RunawayRecordFlushLoop is used to flush runaway records.
func (rm *Manager) RunawayRecordFlushLoop() {
	defer util.Recover(metrics.LabelDomain, "runawayRecordFlushLoop", nil, false)

	go rm.runawayRecordFlusher.run()
	go rm.quarantineRecordFlusher.run()
	go rm.staleQuarantineFlusher.run()

	gcInterval := runawayRecordGCInterval
	failpoint.Inject("FastRunawayGC", func() {
		gcInterval = time.Millisecond * 200
	})
	runawayRecordGCTicker := time.NewTicker(gcInterval)

	for {
		select {
		case <-rm.exit:
			rm.runawayRecordFlusher.stop()
			rm.quarantineRecordFlusher.stop()
			rm.staleQuarantineFlusher.stop()
			logutil.BgLogger().Info("runaway record flush loop exit")
			return
		case <-runawayRecordGCTicker.C:
			go rm.deleteExpiredRows(runawayRecordExpiredDuration)
		}
	}
}

// RunawayWatchSyncLoop is used to sync runaway watch records.
func (rm *Manager) RunawayWatchSyncLoop() {
	defer util.Recover(metrics.LabelDomain, "runawayWatchSyncLoop", nil, false)
	runawayWatchSyncTicker := time.NewTicker(watchSyncInterval)
	for {
		select {
		case <-rm.exit:
			logutil.BgLogger().Info("runaway watch sync loop exit")
			return
		case <-runawayWatchSyncTicker.C:
			err := rm.UpdateNewAndDoneWatch()
			if err != nil {
				sampleLogger().Warn("get runaway watch record failed", zap.Error(err))
			}
		}
	}
}

func (rm *Manager) markQuarantine(
	resourceGroupName, convict string,
	watchType rmpb.RunawayWatchType, action rmpb.RunawayAction, switchGroupName string,
	ttl time.Duration, now *time.Time, exceedCause string,
) {
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
		SwitchGroupName:   switchGroupName,
		ExceedCause:       exceedCause,
	}
	// Add record without ID into watch list in this TiDB right now.
	rm.addWatchList(record, ttl, false)
	rm.quarantineRecordFlusher.add(record.getRecordKey(), record)
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
				rm.staleQuarantineFlusher.add(record.ID, record)
			}
			rm.queryLock.Unlock()
		} else if item.ID == 0 {
			// to replace the record without ID.
			rm.queryLock.Lock()
			defer rm.queryLock.Unlock()
			rm.watchList.Set(key, record, ttl)
		} else if item.ID != record.ID {
			// check the ID because of the earlier scan.
			rm.staleQuarantineFlusher.add(record.ID, record)
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

func (rm *Manager) markRunaway(checker *Checker, action, matchType string, now *time.Time, exceedCause string) {
	r := &Record{
		ResourceGroupName: checker.resourceGroupName,
		StartTime:         *now,
		Match:             matchType,
		Action:            action,
		SampleText:        checker.originalSQL,
		SQLDigest:         checker.sqlDigest,
		PlanDigest:        checker.planDigest,
		Source:            rm.serverID,
		ExceedCause:       exceedCause,
		Repeats:           1,
	}
	key := recordKey{
		ResourceGroupName: r.ResourceGroupName,
		SQLDigest:         r.SQLDigest,
		PlanDigest:        r.PlanDigest,
		Match:             r.Match,
	}
	rm.runawayRecordFlusher.add(key, r)
}

// examineWatchList check whether the query is in watch list.
func (rm *Manager) examineWatchList(resourceGroupName string, convict string) (bool, rmpb.RunawayAction, string, string) {
	item := rm.getWatchFromWatchList(resourceGroupName + "/" + convict)
	if item == nil {
		return false, 0, "", ""
	}
	return true, item.Action, item.getSwitchGroupName(), item.GetExceedCause()
}

// loadOrStoreActiveCounter returns the counter for the given resource group name,
// creating a new one if it doesn't exist.
func (rm *Manager) loadOrStoreActiveCounter(name string) (*atomic.Int64, bool) {
	if counter, ok := rm.activeGroup.Load(name); ok {
		return counter.(*atomic.Int64), true
	}
	counter, loaded := rm.activeGroup.LoadOrStore(name, &atomic.Int64{})
	return counter.(*atomic.Int64), loaded
}

// getActiveWatchCount returns the active watch count for the given resource group name.
func (rm *Manager) getActiveWatchCount(name string) int64 {
	counter, ok := rm.activeGroup.Load(name)
	if !ok {
		return 0
	}
	return counter.(*atomic.Int64).Load()
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
	if !rm.runawaySyncer.checkWatchTableExist() {
		return nil
	}
	for {
		records, err := rm.runawaySyncer.getNewWatchRecords()
		if err != nil {
			return err
		}
		for _, r := range records {
			rm.AddWatch(r)
		}
		if len(records) < watchSyncBatchLimit {
			break
		}
	}
	if !rm.runawaySyncer.checkWatchDoneTableExist() {
		return nil
	}
	for {
		doneRecords, err := rm.runawaySyncer.getNewWatchDoneRecords()
		if err != nil {
			return err
		}
		for _, r := range doneRecords {
			rm.removeWatch(r)
		}
		if len(doneRecords) < watchSyncBatchLimit {
			break
		}
	}
	return nil
}

// AddWatch is used to add watch items from system table.
func (rm *Manager) AddWatch(record *QuarantineRecord) {
	ttl := time.Until(record.EndTime)
	if record.EndTime.Equal(NullTime) {
		ttl = 0
	} else if ttl <= 0 {
		rm.staleQuarantineFlusher.add(record.ID, record)
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
