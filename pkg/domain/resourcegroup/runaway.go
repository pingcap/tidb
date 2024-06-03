// Copyright 2023 PingCAP, Inc.
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

package resourcegroup

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/zap"
)

const (
	// DefaultResourceGroupName is the default resource group name.
	DefaultResourceGroupName = "default"
	// ManualSource shows the item added manually.
	ManualSource = "manual"
	// RunawayWatchTableName is the name of system table which save runaway watch items.
	RunawayWatchTableName = "mysql.tidb_runaway_watch"
	// RunawayWatchDoneTableName is the name of system table which save done runaway watch items.
	RunawayWatchDoneTableName = "mysql.tidb_runaway_watch_done"

	// MaxWaitDuration is the max duration to wait for acquiring token buckets.
	MaxWaitDuration           = time.Second * 30
	maxWatchListCap           = 10000
	maxWatchRecordChannelSize = 1024
)

// NullTime is a zero time.Time.
var NullTime time.Time

// RunawayMatchType is used to indicate whether query was interrupted by runaway identification or quarantine watch.
type RunawayMatchType uint

const (
	// RunawayMatchTypeWatch shows quarantine watch.
	RunawayMatchTypeWatch RunawayMatchType = iota
	// RunawayMatchTypeIdentify shows identification.
	RunawayMatchTypeIdentify
)

func (t RunawayMatchType) String() string {
	switch t {
	case RunawayMatchTypeWatch:
		return "watch"
	case RunawayMatchTypeIdentify:
		return "identify"
	default:
		panic("unknown type")
	}
}

// RunawayRecord is used to save records which will be insert into mysql.tidb_runaway_queries.
type RunawayRecord struct {
	ResourceGroupName string
	Time              time.Time
	Match             string
	Action            string
	SQLText           string
	PlanDigest        string
	Source            string
}

// GenRunawayQueriesStmt generates statement with given RunawayRecords.
func GenRunawayQueriesStmt(records []*RunawayRecord) (string, []any) {
	var builder strings.Builder
	params := make([]any, 0, len(records)*7)
	builder.WriteString("insert into mysql.tidb_runaway_queries VALUES ")
	for count, r := range records {
		if count > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString("(%?, %?, %?, %?, %?, %?, %?)")
		params = append(params, r.ResourceGroupName)
		params = append(params, r.Time)
		params = append(params, r.Match)
		params = append(params, r.Action)
		params = append(params, r.SQLText)
		params = append(params, r.PlanDigest)
		params = append(params, r.Source)
	}
	return builder.String(), params
}

// QuarantineRecord is used to save records which will be insert into mysql.tidb_runaway_watch.
type QuarantineRecord struct {
	ID                int64
	ResourceGroupName string
	StartTime         time.Time
	EndTime           time.Time
	Watch             rmpb.RunawayWatchType
	WatchText         string
	Source            string
	Action            rmpb.RunawayAction
}

// GetRecordKey is used to get the key in ttl cache.
func (r *QuarantineRecord) GetRecordKey() string {
	return r.ResourceGroupName + "/" + r.WatchText
}

func writeInsert(builder *strings.Builder, tableName string) {
	builder.WriteString("insert into ")
	builder.WriteString(tableName)
	builder.WriteString(" VALUES ")
}

// GenInsertionStmt is used to generate insertion sql.
func (r *QuarantineRecord) GenInsertionStmt() (string, []any) {
	var builder strings.Builder
	params := make([]any, 0, 6)
	writeInsert(&builder, RunawayWatchTableName)
	builder.WriteString("(null, %?, %?, %?, %?, %?, %?, %?)")
	params = append(params, r.ResourceGroupName)
	params = append(params, r.StartTime)
	if r.EndTime.Equal(NullTime) {
		params = append(params, nil)
	} else {
		params = append(params, r.EndTime)
	}
	params = append(params, r.Watch)
	params = append(params, r.WatchText)
	params = append(params, r.Source)
	params = append(params, r.Action)
	return builder.String(), params
}

// GenInsertionDoneStmt is used to generate insertion sql for runaway watch done record.
func (r *QuarantineRecord) GenInsertionDoneStmt() (string, []any) {
	var builder strings.Builder
	params := make([]any, 0, 9)
	writeInsert(&builder, RunawayWatchDoneTableName)
	builder.WriteString("(null, %?, %?, %?, %?, %?, %?, %?, %?, %?)")
	params = append(params, r.ID)
	params = append(params, r.ResourceGroupName)
	params = append(params, r.StartTime)
	if r.EndTime.Equal(NullTime) {
		params = append(params, nil)
	} else {
		params = append(params, r.EndTime)
	}
	params = append(params, r.Watch)
	params = append(params, r.WatchText)
	params = append(params, r.Source)
	params = append(params, r.Action)
	params = append(params, time.Now().UTC())
	return builder.String(), params
}

// GenDeletionStmt is used to generate deletion sql.
func (r *QuarantineRecord) GenDeletionStmt() (string, []any) {
	var builder strings.Builder
	params := make([]any, 0, 1)
	builder.WriteString("delete from ")
	builder.WriteString(RunawayWatchTableName)
	builder.WriteString(" where id = %?")
	params = append(params, r.ID)
	return builder.String(), params
}

// RunawayManager is used to detect and record runaway queries.
type RunawayManager struct {
	syncerInitialized atomic.Bool
	logOnce           sync.Once

	// queryLock is used to avoid repeated additions. Since we will add new items to the system table,
	// in order to avoid repeated additions, we need a lock to ensure that
	// action "judging whether there is this record in the current watch list and adding records" have atomicity.
	queryLock sync.Mutex
	watchList *ttlcache.Cache[string, *QuarantineRecord]
	// activeGroup is used to manage the active runaway watches of resource group
	activeGroup map[string]int64
	activeLock  sync.RWMutex
	metricsMap  generic.SyncMap[string, prometheus.Counter]

	resourceGroupCtl   *rmclient.ResourceGroupsController
	serverID           string
	runawayQueriesChan chan *RunawayRecord
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
}

// NewRunawayManager creates a new RunawayManager.
func NewRunawayManager(resourceGroupCtl *rmclient.ResourceGroupsController, serverAddr string) *RunawayManager {
	watchList := ttlcache.New[string, *QuarantineRecord](
		ttlcache.WithTTL[string, *QuarantineRecord](ttlcache.NoTTL),
		ttlcache.WithCapacity[string, *QuarantineRecord](maxWatchListCap),
		ttlcache.WithDisableTouchOnHit[string, *QuarantineRecord](),
	)
	go watchList.Start()
	staleQuarantineChan := make(chan *QuarantineRecord, maxWatchRecordChannelSize)
	m := &RunawayManager{
		syncerInitialized:     atomic.Bool{},
		resourceGroupCtl:      resourceGroupCtl,
		watchList:             watchList,
		serverID:              serverAddr,
		runawayQueriesChan:    make(chan *RunawayRecord, maxWatchRecordChannelSize),
		quarantineChan:        make(chan *QuarantineRecord, maxWatchRecordChannelSize),
		staleQuarantineRecord: staleQuarantineChan,
		activeGroup:           make(map[string]int64),
		metricsMap:            generic.NewSyncMap[string, prometheus.Counter](8),
	}
	m.insertionCancel = watchList.OnInsertion(func(_ context.Context, i *ttlcache.Item[string, *QuarantineRecord]) {
		m.activeLock.Lock()
		m.activeGroup[i.Value().ResourceGroupName]++
		m.activeLock.Unlock()
	})
	m.evictionCancel = watchList.OnEviction(func(_ context.Context, _ ttlcache.EvictionReason, i *ttlcache.Item[string, *QuarantineRecord]) {
		m.activeLock.Lock()
		m.activeGroup[i.Value().ResourceGroupName]--
		m.activeLock.Unlock()
		if i.Value().ID == 0 {
			return
		}
		staleQuarantineChan <- i.Value()
	})
	return m
}

// MarkSyncerInitialized is used to mark the syncer is initialized.
func (rm *RunawayManager) MarkSyncerInitialized() {
	rm.syncerInitialized.Store(true)
}

// DeriveChecker derives a RunawayChecker from the given resource group
func (rm *RunawayManager) DeriveChecker(resourceGroupName, originalSQL, sqlDigest, planDigest string) *RunawayChecker {
	group, err := rm.resourceGroupCtl.GetResourceGroup(resourceGroupName)
	if err != nil || group == nil {
		logutil.BgLogger().Warn("cannot setup up runaway checker", zap.Error(err))
		return nil
	}
	rm.activeLock.RLock()
	defer rm.activeLock.RUnlock()
	if group.RunawaySettings == nil && rm.activeGroup[resourceGroupName] == 0 {
		return nil
	}
	counter, ok := rm.metricsMap.Load(resourceGroupName)
	if !ok {
		counter = metrics.RunawayCheckerCounter.WithLabelValues(resourceGroupName, "hit", "")
		rm.metricsMap.Store(resourceGroupName, counter)
	}
	counter.Inc()
	return newRunawayChecker(rm, resourceGroupName, group.RunawaySettings, originalSQL, sqlDigest, planDigest)
}

func (rm *RunawayManager) markQuarantine(resourceGroupName, convict string, watchType rmpb.RunawayWatchType, action rmpb.RunawayAction, ttl time.Duration, now *time.Time) {
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

// IsSyncerInitialized is only used for test.
func (rm *RunawayManager) IsSyncerInitialized() bool {
	return rm.syncerInitialized.Load()
}

func (rm *RunawayManager) addWatchList(record *QuarantineRecord, ttl time.Duration, force bool) {
	key := record.GetRecordKey()
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
			// When watchlist get record, it will check whether the record is stale, so add new record if returns nil.
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
			// check the ID because of the eariler scan.
			rm.staleQuarantineRecord <- record
		}
	}
}

// GetWatchByKey is used to get a watch item by given key.
func (rm *RunawayManager) GetWatchByKey(key string) *QuarantineRecord {
	return rm.getWatchFromWatchList(key)
}

// GetWatchList is used to get all watch items.
func (rm *RunawayManager) GetWatchList() []*QuarantineRecord {
	items := rm.watchList.Items()
	ret := make([]*QuarantineRecord, 0, len(items))
	for _, item := range items {
		ret = append(ret, item.Value())
	}
	return ret
}

// AddWatch is used to add watch items from system table.
func (rm *RunawayManager) AddWatch(record *QuarantineRecord) {
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

// RemoveWatch is used to remove watch item, and this action is triggered by reading done watch system table.
func (rm *RunawayManager) RemoveWatch(record *QuarantineRecord) {
	// we should check whether the cached record is not the same as the removing record.
	rm.queryLock.Lock()
	defer rm.queryLock.Unlock()
	item := rm.getWatchFromWatchList(record.GetRecordKey())
	if item == nil {
		return
	}
	if item.ID == record.ID {
		rm.watchList.Delete(record.GetRecordKey())
	}
}
func (rm *RunawayManager) getWatchFromWatchList(key string) *QuarantineRecord {
	item := rm.watchList.Get(key)
	if item != nil {
		return item.Value()
	}
	return nil
}

func (rm *RunawayManager) markRunaway(resourceGroupName, originalSQL, planDigest string, action string, matchType RunawayMatchType, now *time.Time) {
	source := rm.serverID
	if !rm.syncerInitialized.Load() {
		rm.logOnce.Do(func() {
			logutil.BgLogger().Warn("runaway syncer is not initialized, so can't records about runaway")
		})
		return
	}
	select {
	case rm.runawayQueriesChan <- &RunawayRecord{
		ResourceGroupName: resourceGroupName,
		Time:              *now,
		Match:             matchType.String(),
		Action:            action,
		SQLText:           originalSQL,
		PlanDigest:        planDigest,
		Source:            source,
	}:
	default:
		// TODO: add warning for discard flush records
	}
}

// FlushThreshold specifies the threshold for the number of records in trigger flush
func (*RunawayManager) FlushThreshold() int {
	return maxWatchRecordChannelSize / 2
}

// RunawayRecordChan returns the channel of RunawayRecord
func (rm *RunawayManager) RunawayRecordChan() <-chan *RunawayRecord {
	return rm.runawayQueriesChan
}

// QuarantineRecordChan returns the channel of QuarantineRecord
func (rm *RunawayManager) QuarantineRecordChan() <-chan *QuarantineRecord {
	return rm.quarantineChan
}

// StaleQuarantineRecordChan returns the channel of staleQuarantineRecord
func (rm *RunawayManager) StaleQuarantineRecordChan() <-chan *QuarantineRecord {
	return rm.staleQuarantineRecord
}

// examineWatchList check whether the query is in watch list.
func (rm *RunawayManager) examineWatchList(resourceGroupName string, convict string) (bool, rmpb.RunawayAction) {
	item := rm.getWatchFromWatchList(resourceGroupName + "/" + convict)
	if item == nil {
		return false, 0
	}
	return true, item.Action
}

// Stop stops the watchList which is a ttlcache.
func (rm *RunawayManager) Stop() {
	if rm.watchList != nil {
		rm.watchList.Stop()
	}
}

// RunawayChecker is used to check if the query is runaway.
type RunawayChecker struct {
	manager           *RunawayManager
	resourceGroupName string
	originalSQL       string
	sqlDigest         string
	planDigest        string

	deadline time.Time
	setting  *rmpb.RunawaySettings

	markedByRule  atomic.Bool
	markedByWatch bool
	watchAction   rmpb.RunawayAction
}

func newRunawayChecker(manager *RunawayManager, resourceGroupName string, setting *rmpb.RunawaySettings, originalSQL, sqlDigest, planDigest string) *RunawayChecker {
	c := &RunawayChecker{
		manager:           manager,
		resourceGroupName: resourceGroupName,
		originalSQL:       originalSQL,
		sqlDigest:         sqlDigest,
		planDigest:        planDigest,
		setting:           setting,
		markedByRule:      atomic.Bool{},
		markedByWatch:     false,
	}
	if setting != nil {
		c.deadline = time.Now().Add(time.Duration(setting.Rule.ExecElapsedTimeMs) * time.Millisecond)
	}
	return c
}

// BeforeExecutor checks whether query is in watch list before executing and after compiling.
func (r *RunawayChecker) BeforeExecutor() error {
	if r == nil {
		return nil
	}
	for _, convict := range r.getConvictIdentifiers() {
		watched, action := r.manager.examineWatchList(r.resourceGroupName, convict)
		if watched {
			if action == rmpb.RunawayAction_NoneAction && r.setting != nil {
				action = r.setting.Action
			}
			r.markedByWatch = true
			now := time.Now()
			r.watchAction = action
			r.markRunaway(RunawayMatchTypeWatch, action, &now)
			// If no match action, it will do nothing.
			switch action {
			case rmpb.RunawayAction_Kill:
				return exeerrors.ErrResourceGroupQueryRunawayQuarantine
			case rmpb.RunawayAction_CoolDown:
				// This action should be done in BeforeCopRequest.
				return nil
			case rmpb.RunawayAction_DryRun:
				return nil
			default:
			}
		}
	}
	return nil
}

// BeforeCopRequest checks runaway and modifies the request if necessary before sending coprocessor request.
func (r *RunawayChecker) BeforeCopRequest(req *tikvrpc.Request) error {
	if r.setting == nil && !r.markedByWatch {
		return nil
	}
	marked := r.markedByRule.Load()
	if !marked {
		// note: now we don't check whether query is in watch list again.
		if r.markedByWatch {
			if r.watchAction == rmpb.RunawayAction_CoolDown {
				req.ResourceControlContext.OverridePriority = 1 // set priority to lowest
			}
		}

		now := time.Now()
		until := r.deadline.Sub(now)
		if until > 0 {
			if r.setting.Action == rmpb.RunawayAction_Kill {
				// if the execution time is close to the threshold, set a timeout
				if until < tikv.ReadTimeoutMedium {
					req.Context.MaxExecutionDurationMs = uint64(until.Milliseconds())
				}
			}
			return nil
		}
		// execution time exceeds the threshold, mark the query as runaway
		if r.markedByRule.CompareAndSwap(false, true) {
			r.markRunaway(RunawayMatchTypeIdentify, r.setting.Action, &now)
			if !r.markedByWatch {
				r.markQuarantine(&now)
			}
		}
	}
	switch r.setting.Action {
	case rmpb.RunawayAction_Kill:
		return exeerrors.ErrResourceGroupQueryRunawayInterrupted
	case rmpb.RunawayAction_CoolDown:
		req.ResourceControlContext.OverridePriority = 1 // set priority to lowest
		return nil
	case rmpb.RunawayAction_DryRun:
		return nil
	default:
		return nil
	}
}

// CheckCopRespError checks TiKV error after receiving coprocessor response.
func (r *RunawayChecker) CheckCopRespError(err error) error {
	if err == nil || r.setting == nil || r.setting.Action != rmpb.RunawayAction_Kill {
		return err
	}
	if strings.HasPrefix(err.Error(), "Coprocessor task terminated due to exceeding the deadline") {
		if !r.markedByRule.Load() {
			now := time.Now()
			if r.deadline.Before(now) && r.markedByRule.CompareAndSwap(false, true) {
				r.markRunaway(RunawayMatchTypeIdentify, r.setting.Action, &now)
				if !r.markedByWatch {
					r.markQuarantine(&now)
				}
				return exeerrors.ErrResourceGroupQueryRunawayInterrupted
			}
		}
		// Due to concurrency, check again.
		if r.markedByRule.Load() {
			return exeerrors.ErrResourceGroupQueryRunawayInterrupted
		}
	}
	return err
}

func (r *RunawayChecker) markQuarantine(now *time.Time) {
	if r.setting.Watch == nil {
		return
	}
	ttl := time.Duration(r.setting.Watch.LastingDurationMs) * time.Millisecond

	r.manager.markQuarantine(r.resourceGroupName, r.getSettingConvictIdentifier(), r.setting.Watch.Type, r.setting.Action, ttl, now)
}

func (r *RunawayChecker) markRunaway(matchType RunawayMatchType, action rmpb.RunawayAction, now *time.Time) {
	actionStr := strings.ToLower(rmpb.RunawayAction_name[int32(action)])
	metrics.RunawayCheckerCounter.WithLabelValues(r.resourceGroupName, matchType.String(), actionStr).Inc()
	r.manager.markRunaway(r.resourceGroupName, r.originalSQL, r.planDigest, actionStr, matchType, now)
}

func (r *RunawayChecker) getSettingConvictIdentifier() string {
	if r.setting.Watch == nil {
		return ""
	}
	switch r.setting.Watch.Type {
	case rmpb.RunawayWatchType_Plan:
		return r.planDigest
	case rmpb.RunawayWatchType_Similar:
		return r.sqlDigest
	case rmpb.RunawayWatchType_Exact:
		return r.originalSQL
	default:
		return ""
	}
}

func (r *RunawayChecker) getConvictIdentifiers() []string {
	return []string{r.originalSQL, r.sqlDigest, r.planDigest}
}
