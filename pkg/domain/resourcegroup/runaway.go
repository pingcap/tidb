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
	"fmt"
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
	// startTime and endTime are in UTC.
	StartTime time.Time
	EndTime   time.Time
	Watch     rmpb.RunawayWatchType
	WatchText string
	Source    string
	Action    rmpb.RunawayAction
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
func (rm *RunawayManager) DeriveChecker(resourceGroupName, originalSQL, sqlDigest, planDigest string, startTime time.Time) *RunawayChecker {
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
	return newRunawayChecker(rm, resourceGroupName, group.RunawaySettings, originalSQL, sqlDigest, planDigest, startTime)
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

func (rm *RunawayManager) markRunaway(resourceGroupName, originalSQL, planDigest, action, matchType string, now *time.Time) {
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
	if rm == nil {
		return
	}
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
	// From the group runaway settings, which will be applied when a query lacks a specified watch rule.
	settings *rmpb.RunawaySettings

	// markedByRule is set to true when the query matches the group runaway settings.
	markedByRule atomic.Bool
	// markedByWatch is set to true when the query matches the specified watch rules.
	markedByWatch bool
	watchAction   rmpb.RunawayAction
}

func newRunawayChecker(
	manager *RunawayManager,
	resourceGroupName string, settings *rmpb.RunawaySettings,
	originalSQL, sqlDigest, planDigest string, startTime time.Time,
) *RunawayChecker {
	c := &RunawayChecker{
		manager:           manager,
		resourceGroupName: resourceGroupName,
		originalSQL:       originalSQL,
		sqlDigest:         sqlDigest,
		planDigest:        planDigest,
		settings:          settings,
	}
	if settings != nil {
		c.deadline = startTime.Add(time.Duration(settings.Rule.ExecElapsedTimeMs) * time.Millisecond)
	}
	return c
}

// BeforeExecutor checks whether query is in watch list before executing and after compiling.
func (r *RunawayChecker) BeforeExecutor() error {
	if r == nil {
		return nil
	}
	// Check if the query matches any specified watch rules.
	for _, convict := range r.getConvictIdentifiers() {
		watched, action := r.manager.examineWatchList(r.resourceGroupName, convict)
		if !watched {
			continue
		}
		// Use the group runaway settings if none are provided.
		if action == rmpb.RunawayAction_NoneAction && r.settings != nil {
			action = r.settings.Action
		}
		// Mark it if this is the first time being watched.
		r.markRunawayByWatch(action)
		// Take action if needed.
		switch action {
		case rmpb.RunawayAction_Kill:
			// Return an error to interrupt the query.
			return exeerrors.ErrResourceGroupQueryRunawayQuarantine
		case rmpb.RunawayAction_CoolDown:
			// This action will be handled in `BeforeCopRequest`.
			return nil
		case rmpb.RunawayAction_DryRun:
			// Noop.
			return nil
		default:
			// Continue to examine other convicts.
		}
	}
	return nil
}

// CheckAction is used to check current action of the query.
// It's safe to call this method concurrently.
func (r *RunawayChecker) CheckAction() rmpb.RunawayAction {
	if r == nil {
		return rmpb.RunawayAction_NoneAction
	}
	if r.markedByWatch {
		return r.watchAction
	}
	if r.markedByRule.Load() {
		return r.settings.Action
	}
	return rmpb.RunawayAction_NoneAction
}

// CheckRuleKillAction checks whether the query should be killed according to the group settings.
func (r *RunawayChecker) CheckRuleKillAction() bool {
	// If the group settings are not available and it's not marked by watch, skip this part.
	if r.settings == nil && !r.markedByWatch {
		return false
	}
	// If the group settings are available and it's not marked by rule, check the execution time.
	if r.settings != nil && !r.markedByRule.Load() {
		now := time.Now()
		until := r.deadline.Sub(now)
		if until > 0 {
			return false
		}
		r.markRunawayByIdentify(r.settings.Action, &now)
		return r.settings.Action == rmpb.RunawayAction_Kill
	}
	return false
}

// Rule returns the rule of the runaway checker.
func (r *RunawayChecker) Rule() string {
	var execElapsedTime time.Duration
	if r.settings != nil {
		execElapsedTime = time.Duration(r.settings.Rule.ExecElapsedTimeMs) * time.Millisecond
	}
	return fmt.Sprintf("execElapsedTime:%s", execElapsedTime)
}

// BeforeCopRequest checks runaway and modifies the request if necessary before sending coprocessor request.
func (r *RunawayChecker) BeforeCopRequest(req *tikvrpc.Request) error {
	// If the group settings are not available and it's not marked by watch, skip this part.
	if r.settings == nil && !r.markedByWatch {
		return nil
	}
	// If it's marked by watch and the action is cooldown, override the priority,
	if r.markedByWatch && r.watchAction == rmpb.RunawayAction_CoolDown {
		req.ResourceControlContext.OverridePriority = 1 // set priority to lowest
	}
	// If group settings are available and the query is not marked by a rule,
	// verify if it matches any rules in the settings.
	if r.settings != nil && !r.markedByRule.Load() {
		now := time.Now()
		until := r.deadline.Sub(now)
		if until > 0 {
			if r.settings.Action == rmpb.RunawayAction_Kill {
				// if the execution time is close to the threshold, set a timeout
				if until < tikv.ReadTimeoutMedium {
					req.Context.MaxExecutionDurationMs = uint64(until.Milliseconds())
				}
			}
			return nil
		}
		// execution time exceeds the threshold, mark the query as runaway
		r.markRunawayByIdentify(r.settings.Action, &now)
		// Take action if needed.
		switch r.settings.Action {
		case rmpb.RunawayAction_Kill:
			return exeerrors.ErrResourceGroupQueryRunawayInterrupted
		case rmpb.RunawayAction_CoolDown:
			req.ResourceControlContext.OverridePriority = 1 // set priority to lowest
			return nil
		default:
			return nil
		}
	}
	return nil
}

// CheckCopRespError checks TiKV error after receiving coprocessor response.
func (r *RunawayChecker) CheckCopRespError(err error) error {
	if err == nil || r.settings == nil || r.settings.Action != rmpb.RunawayAction_Kill {
		return err
	}
	if strings.HasPrefix(err.Error(), "Coprocessor task terminated due to exceeding the deadline") {
		if !r.markedByRule.Load() {
			now := time.Now()
			if r.deadline.Before(now) && r.markRunawayByIdentify(r.settings.Action, &now) {
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
	if r.settings == nil || r.settings.Watch == nil {
		return
	}
	ttl := time.Duration(r.settings.Watch.LastingDurationMs) * time.Millisecond

	r.manager.markQuarantine(r.resourceGroupName, r.getSettingConvictIdentifier(), r.settings.Watch.Type, r.settings.Action, ttl, now)
}

func (r *RunawayChecker) markRunawayByIdentify(action rmpb.RunawayAction, now *time.Time) bool {
	swapped := r.markedByRule.CompareAndSwap(false, true)
	if swapped {
		r.markRunaway("identify", action, now)
		if !r.markedByWatch {
			r.markQuarantine(now)
		}
	}
	return swapped
}

func (r *RunawayChecker) markRunawayByWatch(action rmpb.RunawayAction) {
	r.markedByWatch = true
	r.watchAction = action
	now := time.Now()
	r.markRunaway("watch", action, &now)
}

func (r *RunawayChecker) markRunaway(matchType string, action rmpb.RunawayAction, now *time.Time) {
	actionStr := strings.ToLower(action.String())
	metrics.RunawayCheckerCounter.WithLabelValues(r.resourceGroupName, matchType, actionStr).Inc()
	r.manager.markRunaway(r.resourceGroupName, r.originalSQL, r.planDigest, actionStr, matchType, now)
}

func (r *RunawayChecker) getSettingConvictIdentifier() string {
	if r == nil || r.settings == nil || r.settings.Watch == nil {
		return ""
	}
	switch r.settings.Watch.Type {
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
