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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

// Checker is used to check if the query is runaway.
type Checker struct {
	manager           *Manager
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
	// watchAction is the specified watch action for the runaway query.
	// If it's not given, the action defined in `settings` will be used.
	watchAction rmpb.RunawayAction
}

// NewChecker creates a new RunawayChecker.
func NewChecker(
	manager *Manager,
	resourceGroupName string, settings *rmpb.RunawaySettings,
	originalSQL, sqlDigest, planDigest string, startTime time.Time,
) *Checker {
	c := &Checker{
		manager:           manager,
		resourceGroupName: resourceGroupName,
		originalSQL:       originalSQL,
		sqlDigest:         sqlDigest,
		planDigest:        planDigest,
		settings:          settings,
		markedByRule:      atomic.Bool{},
		markedByWatch:     false,
	}
	if settings != nil {
		c.deadline = startTime.Add(time.Duration(settings.Rule.ExecElapsedTimeMs) * time.Millisecond)
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

// BeforeExecutor checks whether query is in watch list before executing and after compiling.
func (r *Checker) BeforeExecutor() (string, error) {
	if r == nil {
		return "", nil
	}
	var (
		watched         bool
		action          rmpb.RunawayAction
		switchGroupName string
	)
	// Check if the query matches any specified watch rules.
	for _, convict := range r.getConvictIdentifiers() {
		watched, action, switchGroupName = r.manager.examineWatchList(r.resourceGroupName, convict)
		if !watched {
			continue
		}
		// Use the group runaway settings if none are provided.
		if action == rmpb.RunawayAction_NoneAction && r.settings != nil {
			action = r.settings.Action
			switchGroupName = r.settings.SwitchGroupName
		}
		// Mark it if this is the first time being watched.
		r.markRunawayByWatch(action, switchGroupName)
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
			return switchGroupName, nil
		default:
			// Continue to examine other convicts.
		}
	}
	return "", nil
}

// BeforeCopRequest checks runaway and modifies the request if necessary before sending coprocessor request.
func (r *Checker) BeforeCopRequest(req *tikvrpc.Request) error {
	if r == nil {
		return nil
	}
	// If the group settings are not available, and it's not marked by watch, skip this part.
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
		r.markRunawayByIdentify(&now)
		// Take action if needed.
		switch r.settings.Action {
		case rmpb.RunawayAction_Kill:
			return exeerrors.ErrResourceGroupQueryRunawayInterrupted
		case rmpb.RunawayAction_CoolDown:
			req.ResourceControlContext.OverridePriority = 1 // set priority to lowest
			return nil
		case rmpb.RunawayAction_SwitchGroup:
			req.ResourceControlContext.ResourceGroupName = r.settings.SwitchGroupName
			return nil
		default:
			return nil
		}
	}
	return nil
}

// CheckCopRespError checks TiKV error after receiving coprocessor response.
func (r *Checker) CheckCopRespError(err error) error {
	if r == nil {
		return err
	}
	failpoint.Inject("sleepCoprAfterReq", func(v failpoint.Value) {
		//nolint:durationcheck
		value := v.(int)
		time.Sleep(time.Millisecond * time.Duration(value))
		if value > 50 {
			err = errors.Errorf("Coprocessor task terminated due to exceeding the deadline")
		}
	})
	if err == nil || r.settings == nil || r.settings.Action != rmpb.RunawayAction_Kill {
		return err
	}
	if strings.HasPrefix(err.Error(), "Coprocessor task terminated due to exceeding the deadline") {
		if !r.markedByRule.Load() {
			now := time.Now()
			if r.deadline.Before(now) && r.markRunawayByIdentify(&now) {
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

// CheckAction is used to check current action of the query.
// It's safe to call this method concurrently.
func (r *Checker) CheckAction() rmpb.RunawayAction {
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
func (r *Checker) CheckRuleKillAction() bool {
	// If the group settings are not available, and it's not marked by watch, skip this part.
	if r == nil || r.settings == nil && !r.markedByWatch {
		return false
	}
	// If the group settings are available, and it's not marked by rule, check the execution time.
	if r.settings != nil && !r.markedByRule.Load() {
		now := time.Now()
		until := r.deadline.Sub(now)
		if until > 0 {
			return false
		}
		r.markRunawayByIdentify(&now)
		return r.settings.Action == rmpb.RunawayAction_Kill
	}
	return false
}

// Rule returns the rule of the runaway checker.
func (r *Checker) Rule() string {
	var execElapsedTime time.Duration
	if r.settings != nil {
		execElapsedTime = time.Duration(r.settings.Rule.ExecElapsedTimeMs) * time.Millisecond
	}
	return fmt.Sprintf("execElapsedTime:%s", execElapsedTime)
}

func (r *Checker) markQuarantine(now *time.Time) {
	if r.settings == nil || r.settings.Watch == nil {
		return
	}
	ttl := time.Duration(r.settings.Watch.LastingDurationMs) * time.Millisecond

	r.manager.markQuarantine(
		r.resourceGroupName, r.getSettingConvictIdentifier(),
		r.settings.Watch.Type, r.settings.Action, r.settings.SwitchGroupName,
		ttl, now,
	)
}

func (r *Checker) markRunawayByIdentify(now *time.Time) bool {
	swapped := r.markedByRule.CompareAndSwap(false, true)
	if swapped {
		r.markRunaway("identify", r.settings.Action, r.settings.SwitchGroupName, now)
		if !r.markedByWatch {
			r.markQuarantine(now)
		}
	}
	return swapped
}

func (r *Checker) markRunawayByWatch(action rmpb.RunawayAction, switchGroupName string) {
	r.markedByWatch = true
	r.watchAction = action
	now := time.Now()
	r.markRunaway("watch", action, switchGroupName, &now)
}

func (r *Checker) markRunaway(matchType string, action rmpb.RunawayAction, switchGroupName string, now *time.Time) {
	var actionStr string
	switch action {
	case rmpb.RunawayAction_NoneAction, rmpb.RunawayAction_DryRun, rmpb.RunawayAction_CoolDown, rmpb.RunawayAction_Kill:
		actionStr = action.String()
	case rmpb.RunawayAction_SwitchGroup:
		actionStr = fmt.Sprintf("%s(%s)", action.String(), switchGroupName)
	}
	actionStr = strings.ToLower(actionStr)
	metrics.RunawayCheckerCounter.WithLabelValues(r.resourceGroupName, matchType, actionStr).Inc()
	r.manager.markRunaway(r, actionStr, matchType, now)
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
