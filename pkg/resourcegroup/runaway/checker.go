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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// Checker is used to check if the query is runaway.
type Checker struct {
	manager           *Manager
	resourceGroupName string
	originalSQL       string
	sqlDigest         string
	planDigest        string

	// threshold for resource usage
	deadline               time.Time
	ruThreshold            int64
	processedKeysThreshold int64
	// From the group runaway settings, which will be applied when a query lacks a specified watch rule.
	settings *rmpb.RunawaySettings

	// watchAction is the specified watch action for the runaway query.
	// If it's not given, the action defined in `settings` will be used.
	watchAction rmpb.RunawayAction

	// mutable fields below
	// using total processed_keys to accumulate all coprocessor tasks.
	totalProcessedKeys int64
	// markedByIdentifyInRunawaySettings is set to true when the query matches the group runaway settings.
	markedByIdentifyInRunawaySettings atomic.Bool
	// markedByQueryWatchRule is set to true when the query matches the specified watch rules.
	markedByQueryWatchRule bool
}

// NewChecker creates a new RunawayChecker.
func NewChecker(
	manager *Manager,
	resourceGroupName string, settings *rmpb.RunawaySettings,
	originalSQL, sqlDigest, planDigest string, startTime time.Time,
) *Checker {
	c := &Checker{
		manager:                           manager,
		resourceGroupName:                 resourceGroupName,
		originalSQL:                       originalSQL,
		sqlDigest:                         sqlDigest,
		planDigest:                        planDigest,
		settings:                          settings,
		markedByIdentifyInRunawaySettings: atomic.Bool{},
		markedByQueryWatchRule:            false,
	}
	if settings != nil {
		// avoid setting deadline if the threshold is 0
		if settings.Rule.ExecElapsedTimeMs != 0 {
			c.deadline = startTime.Add(time.Duration(settings.Rule.ExecElapsedTimeMs) * time.Millisecond)
		}
		c.ruThreshold = settings.Rule.RequestUnit
		c.processedKeysThreshold = settings.Rule.ProcessedKeys
	}
	return c
}

// DeriveChecker derives a RunawayChecker from the given resource group
func (rm *Manager) DeriveChecker(resourceGroupName, originalSQL, sqlDigest, planDigest string, startTime time.Time) *Checker {
	group, err := rm.ResourceGroupCtl.GetResourceGroup(resourceGroupName)
	if err != nil || group == nil {
		logutil.BgLogger().Warn("cannot setup up runaway checker", zap.Error(err))
		return nil
	}
	// Only check the normal statement.
	if len(planDigest) == 0 {
		return nil
	}
	rm.ActiveLock.RLock()
	defer rm.ActiveLock.RUnlock()
	if group.RunawaySettings == nil && rm.ActiveGroup[resourceGroupName] == 0 {
		return nil
	}
	counter, ok := rm.MetricsMap.Load(resourceGroupName)
	if !ok {
		counter = metrics.RunawayCheckerCounter.WithLabelValues(resourceGroupName, "hit", "")
		rm.MetricsMap.Store(resourceGroupName, counter)
	}
	counter.Inc()
	return NewChecker(rm, resourceGroupName, group.RunawaySettings, originalSQL, sqlDigest, planDigest, startTime)
}

func (r *Checker) isMarkedByIdentifyInRunawaySettings() bool {
	if r == nil {
		return false
	}
	return r.markedByIdentifyInRunawaySettings.Load()
}

// BeforeExecutor checks whether query is in watch list before executing and after compiling.
func (r *Checker) BeforeExecutor() (string, error) {
	if r == nil {
		return "", nil
	}
	var (
		watched         bool
		action          rmpb.RunawayAction
		switchGroupName string
		exceedCause     string
	)
	// Check if the query matches any specified watch rules.
	for _, convict := range r.getConvictIdentifiers() {
		watched, action, switchGroupName, exceedCause = r.manager.examineWatchList(r.resourceGroupName, convict)
		if !watched {
			continue
		}
		// Use the group runaway settings if none are provided.
		if action == rmpb.RunawayAction_NoneAction && r.settings != nil {
			action = r.settings.Action
			switchGroupName = r.settings.SwitchGroupName
		}
		// Mark it if this is the first time being watched.
		r.markRunawayByQueryWatchRule(action, switchGroupName, exceedCause)
		// Take action if needed.
		switch action {
		case rmpb.RunawayAction_Kill:
			// Return an error to interrupt the query.
			return "", exeerrors.ErrResourceGroupQueryRunawayQuarantine
		case rmpb.RunawayAction_CoolDown:
			// This action will be handled in `BeforeCopRequest`.
			return "", nil
		case rmpb.RunawayAction_DryRun:
			// Noop.
			return "", nil
		case rmpb.RunawayAction_SwitchGroup:
			// Return the switch group name to switch the resource group before executing.
			return r.checkSwitchGroupName(switchGroupName), nil
		default:
			// Continue to examine other convicts.
		}
	}
	return "", nil
}

func (r *Checker) checkSwitchGroupName(groupName string) string {
	if len(groupName) == 0 {
		return ""
	}
	group, err := r.manager.ResourceGroupCtl.GetResourceGroup(groupName)
	if err != nil || group == nil {
		logutil.BgLogger().Debug("invalid switch resource group", zap.String("switch-group-name", groupName), zap.Error(err))
		return ""
	}
	return groupName
}

// BeforeCopRequest checks runaway and modifies the request if necessary before sending coprocessor request.
func (r *Checker) BeforeCopRequest(req *tikvrpc.Request) error {
	if r == nil {
		return nil
	}
	// If it's marked by watch and the action is cooldown, override the priority.
	// Other watch actions should already been handled in `BeforeExecutor` before.
	if r.markedByQueryWatchRule && r.watchAction == rmpb.RunawayAction_CoolDown {
		req.ResourceControlContext.OverridePriority = 1 // set priority to lowest
	}
	// If group settings are available, verify if it matches any rules in the settings.
	if r.settings != nil {
		now := time.Now()
		// Check and ensure the deadline exists.
		exceedCause := r.exceedsThresholds(now, nil, 0)
		// Only set timeout when the query has not been marked as runaway yet.
		if !r.isMarkedByIdentifyInRunawaySettings() && len(exceedCause) == 0 {
			if r.settings.Action == rmpb.RunawayAction_Kill {
				until := r.deadline.Sub(now)
				// if the execution time is close to the threshold, set a timeout
				if !r.deadline.IsZero() && until < tikv.ReadTimeoutMedium {
					req.Context.MaxExecutionDurationMs = uint64(until.Milliseconds())
				}
			}
			return nil
		}
		// Try to mark the query as runaway, it's safe to call this method concurrently.
		// So it's possible that the query has already been marked as runaway in `CheckThresholds`.
		r.markRunawayByIdentifyInRunawaySettings(&now, exceedCause)
		// Take action if needed.
		switch r.settings.Action {
		case rmpb.RunawayAction_Kill:
			return exeerrors.ErrResourceGroupQueryRunawayInterrupted.FastGenByArgs(exceedCause)
		case rmpb.RunawayAction_CoolDown:
			req.ResourceControlContext.OverridePriority = 1 // set priority to lowest
			return nil
		case rmpb.RunawayAction_SwitchGroup:
			if switchGroupName := r.checkSwitchGroupName(r.settings.SwitchGroupName); len(switchGroupName) != 0 {
				req.ResourceControlContext.ResourceGroupName = switchGroupName
			}
			return nil
		default:
			return nil
		}
	}
	return nil
}

// CheckAction is used to check current action of the query.
// It's safe to call this method concurrently.
func (r *Checker) CheckAction() rmpb.RunawayAction {
	if r == nil {
		return rmpb.RunawayAction_NoneAction
	}
	if r.markedByQueryWatchRule {
		return r.watchAction
	}
	if r.isMarkedByIdentifyInRunawaySettings() {
		return r.settings.Action
	}
	return rmpb.RunawayAction_NoneAction
}

// CheckRuleKillAction checks whether the query should be killed according to the group settings.
func (r *Checker) CheckRuleKillAction() (string, bool) {
	// If the group settings are not available, and it's not marked by watch, skip this part.
	if r == nil || r.settings == nil && !r.markedByQueryWatchRule {
		return "", false
	}
	// If the group settings are available, and it's not marked by rule, check the execution time.
	if r.settings != nil && !r.isMarkedByIdentifyInRunawaySettings() {
		now := time.Now()
		exceedCause := r.exceedsThresholds(now, nil, 0)
		if exceedCause == "" {
			return "", false
		}
		r.markRunawayByIdentifyInRunawaySettings(&now, exceedCause)
		return exceedCause, r.settings.Action == rmpb.RunawayAction_Kill
	}
	return "", false
}

func (r *Checker) markQuarantine(now *time.Time, exceedCause string) {
	if r.settings == nil || r.settings.Watch == nil {
		return
	}
	// If the latest group settings have been changed, do not mark quarantine.
	group, err := r.manager.ResourceGroupCtl.GetResourceGroup(r.resourceGroupName)
	if err != nil || group == nil || !proto.Equal(r.settings, group.RunawaySettings) {
		return
	}
	ttl := time.Duration(r.settings.Watch.LastingDurationMs) * time.Millisecond

	r.manager.markQuarantine(r.resourceGroupName, r.getSettingConvictIdentifier(), r.settings.Watch.Type,
		r.settings.Action, r.settings.SwitchGroupName, ttl, now, exceedCause)
}

func (r *Checker) markRunawayByIdentifyInRunawaySettings(now *time.Time, exceedCause string) {
	swapped := r.markedByIdentifyInRunawaySettings.CompareAndSwap(false, true)
	if swapped {
		r.markRunaway("identify", r.settings.Action, r.settings.SwitchGroupName, now, exceedCause)
		if !r.markedByQueryWatchRule {
			r.markQuarantine(now, exceedCause)
		}
	}
}

func (r *Checker) markRunawayByQueryWatchRule(action rmpb.RunawayAction, switchGroupName, exceedCause string) {
	r.markedByQueryWatchRule = true
	r.watchAction = action
	now := time.Now()
	r.markRunaway("watch", action, switchGroupName, &now, exceedCause)
}

func (r *Checker) markRunaway(matchType string, action rmpb.RunawayAction,
	switchGroupName string, now *time.Time, exceedCause string) {
	var actionStr string
	switch action {
	case rmpb.RunawayAction_NoneAction, rmpb.RunawayAction_DryRun, rmpb.RunawayAction_CoolDown, rmpb.RunawayAction_Kill:
		actionStr = action.String()
	case rmpb.RunawayAction_SwitchGroup:
		actionStr = fmt.Sprintf("%s(%s)", action.String(), switchGroupName)
	}
	actionStr = strings.ToLower(actionStr)
	metrics.RunawayCheckerCounter.WithLabelValues(r.resourceGroupName, matchType, actionStr).Inc()
	r.manager.markRunaway(r, actionStr, matchType, now, exceedCause)
}

func (r *Checker) getSettingConvictIdentifier() string {
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

func (r *Checker) getConvictIdentifiers() []string {
	return []string{r.originalSQL, r.sqlDigest, r.planDigest}
}

// CheckThresholds checks error after receiving coprocessor response.
func (r *Checker) CheckThresholds(ruDetail *util.RUDetails, processKeys int64, err error) error {
	if r == nil {
		return err
	}
	failpoint.Inject("checkThresholds", func(v failpoint.Value) {
		// the pass value format is `Time is int` or `processKeys is bool` to reduce redundant failpoint code.
		switch val := v.(type) {
		case int:
			//nolint:durationcheck
			time.Sleep(time.Millisecond * time.Duration(val))
			if val > 50 {
				err = errors.Errorf("Coprocessor task terminated due to exceeding the deadline")
			}
		case bool:
			// default processKeys is 100
			processKeys = int64(100)
		}
	})
	if r.settings == nil {
		return err
	}

	checkTime, now := NullTime, time.Now()
	// decide whether to check the time.
	if err != nil && strings.HasPrefix(err.Error(), "Coprocessor task terminated due to exceeding the deadline") {
		checkTime = now
	}
	// add the processed keys to the total processed keys.
	atomic.AddInt64(&r.totalProcessedKeys, processKeys)
	totalProcessedKeys := atomic.LoadInt64(&r.totalProcessedKeys)
	exceedCause := r.exceedsThresholds(checkTime, ruDetail, totalProcessedKeys)
	// No need to mark as runaway if the query is not exceeded any threshold.
	if len(exceedCause) == 0 {
		return err
	}
	r.markRunawayByIdentifyInRunawaySettings(&now, exceedCause)
	// Other actions will be handled in `BeforeCopRequest` since they need to modify the request.
	if r.settings.Action == rmpb.RunawayAction_Kill {
		return exeerrors.ErrResourceGroupQueryRunawayInterrupted.FastGenByArgs(exceedCause)
	}
	return err
}

// exceedCause is used to indicate whether query was interrupted by
type exceedCause struct {
	cause          uint
	actualValue    any
	thresholdValue any
}

const (
	exceedCauseTime = iota
	exceedCauseRU
	exceedCauseProcessKeys
)

func (t exceedCause) String() string {
	switch t.cause {
	case exceedCauseTime:
		return fmt.Sprintf("ElapsedTime = %s(%s)", t.actualValue, t.thresholdValue)
	case exceedCauseRU:
		return fmt.Sprintf("RequestUnit = %s(%d)", t.actualValue, t.thresholdValue)
	case exceedCauseProcessKeys:
		return fmt.Sprintf("ProcessedKeys = %d(%d)", t.actualValue, t.thresholdValue)
	default:
		panic("unknown type")
	}
}

func (r *Checker) exceedsThresholds(now time.Time, ru *util.RUDetails, processedKeys int64) string {
	until := r.deadline.Sub(now)
	if !r.deadline.IsZero() && until <= 0 {
		return exceedCause{
			cause:          exceedCauseTime,
			actualValue:    now.Format(time.RFC3339),
			thresholdValue: r.deadline.Format(time.RFC3339),
		}.String()
	}

	if ru != nil && r.ruThreshold != 0 && (int64(ru.WRU()+ru.RRU()) >= r.ruThreshold) {
		return exceedCause{
			cause:          exceedCauseRU,
			actualValue:    ru.String(),
			thresholdValue: r.ruThreshold,
		}.String()
	}

	if processedKeys != 0 && r.processedKeysThreshold != 0 && processedKeys >= r.processedKeysThreshold {
		return exceedCause{
			cause:          exceedCauseProcessKeys,
			actualValue:    processedKeys,
			thresholdValue: r.processedKeysThreshold,
		}.String()
	}

	return ""
}

// ResetTotalProcessedKeys resets the current total processed keys.
func (r *Checker) ResetTotalProcessedKeys() {
	if r == nil {
		return
	}
	atomic.StoreInt64(&r.totalProcessedKeys, 0)
}
