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
	m := &Manager{
		ResourceGroupCtl:      resourceGroupCtl,
		watchList:             watchList,
		serverID:              serverAddr,
		runawayQueriesChan:    make(chan *Record, maxWatchRecordChannelSize),
		quarantineChan:        make(chan *QuarantineRecord, maxWatchRecordChannelSize),
		staleQuarantineRecord: make(chan *QuarantineRecord, maxWatchRecordChannelSize),
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
		m.staleQuarantineRecord <- i.Value()
	})
	m.runawaySyncer = newSyncer(pool, infoCache)

	return m
}

// RunawayRecordFlushLoop is used to flush runaway records.
func (rm *Manager) RunawayRecordFlushLoop() {
	defer util.Recover(metrics.LabelDomain, "runawayRecordFlushLoop", nil, false)

	runawayFlushInterval := runawayRecordFlushInterval
	watchFlushInterval := watchRecordFlushInterval
	gcInterval := runawayRecordGCInterval
	batchSize := flushThreshold()
	failpoint.Inject("FastRunawayGC", func() {
		runawayFlushInterval = time.Millisecond * 50
		watchFlushInterval = time.Millisecond * 50
		gcInterval = time.Millisecond * 200
	})

	runawayRecordFlusher := newBatchFlusher(
		"runaway-record",
		runawayFlushInterval,
		batchSize,
		func(m map[recordKey]*Record, k recordKey, v *Record) {
			if existing, ok := m[k]; ok {
				existing.Repeats++
			} else {
				m[k] = v
			}
		},
		genRunawayQueriesStmt,
		rm.sysSessionPool,
	)

	quarantineRecordFlusher := newBatchFlusher(
		"quarantine-record",
		watchFlushInterval,
		batchSize,
		func(m map[string]*QuarantineRecord, k string, v *QuarantineRecord) {
			if _, ok := m[k]; !ok {
				m[k] = v
			}
		},
		genBatchInsertWatchStmt,
		rm.sysSessionPool,
	)

	staleQuarantineFlusher := newBatchFlusher(
		"stale-quarantine-record",
		watchFlushInterval,
		batchSize,
		func(m map[int64]*QuarantineRecord, k int64, v *QuarantineRecord) {
			m[k] = v
		},
		genBatchDeleteWatchByIDStmt,
		rm.sysSessionPool,
	)

	runawayRecordGCTicker := time.NewTicker(gcInterval)
	recordCh := rm.runawayRecordChan()
	quarantineRecordCh := rm.quarantineRecordChan()
	staleQuarantineRecordCh := rm.staleQuarantineRecordChan()

	for {
		select {
		case <-rm.exit: // flush all remaining records before the loop exits
			logutil.BgLogger().Info("flushing remaining runaway records before the loop exits")
			runawayRecordFlusher.flush()
			logutil.BgLogger().Info("flushing remaining quarantine records before the loop exits")
			quarantineRecordFlusher.flush()
			logutil.BgLogger().Info("flushing remaining stale quarantine records before the loop exits")
			staleQuarantineFlusher.flush()
			logutil.BgLogger().Info("runaway record flush loop exit")
			return
		case <-runawayRecordFlusher.timerChan(): // flush runaway records periodically
			runawayRecordFlusher.flush()
		case r := <-recordCh: // add runaway records to the flusher
			key := recordKey{
				ResourceGroupName: r.ResourceGroupName,
				SQLDigest:         r.SQLDigest,
				PlanDigest:        r.PlanDigest,
				Match:             r.Match,
			}
			runawayRecordFlusher.add(key, r)
		case <-runawayRecordGCTicker.C: // delete expired runaway records periodically
			go rm.deleteExpiredRows(runawayRecordExpiredDuration)
		case <-quarantineRecordFlusher.timerChan(): // flush quarantine records periodically
			quarantineRecordFlusher.flush()
		case r := <-quarantineRecordCh: // add quarantine records to the flusher
			quarantineRecordFlusher.add(r.getRecordKey(), r)
		case <-staleQuarantineFlusher.timerChan(): // flush stale quarantine records periodically
			staleQuarantineFlusher.flush()
		case r := <-staleQuarantineRecordCh: // add stale quarantine records to the flusher
			if r.ID == 0 {
				continue
			}
			staleQuarantineFlusher.add(r.ID, r)
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

func (rm *Manager) markRunaway(checker *Checker, action, matchType string, now *time.Time, exceedCause string) {
	source := rm.serverID
	select {
	case rm.runawayQueriesChan <- &Record{
		ResourceGroupName: checker.resourceGroupName,
		StartTime:         *now,
		Match:             matchType,
		Action:            action,
		SampleText:        checker.originalSQL,
		SQLDigest:         checker.sqlDigest,
		PlanDigest:        checker.planDigest,
		Source:            source,
		ExceedCause:       exceedCause,
		// default value for Repeats
		Repeats: 1,
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
func (rm *Manager) examineWatchList(resourceGroupName string, convict string) (bool, rmpb.RunawayAction, string, string) {
	item := rm.getWatchFromWatchList(resourceGroupName + "/" + convict)
	if item == nil {
		return false, 0, "", ""
	}
	return true, item.Action, item.getSwitchGroupName(), item.GetExceedCause()
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
	records, err := rm.runawaySyncer.getNewWatchRecords()
	if err != nil {
		return err
	}
	for _, r := range records {
		rm.AddWatch(r)
	}
	if !rm.runawaySyncer.checkWatchDoneTableExist() {
		return nil
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
