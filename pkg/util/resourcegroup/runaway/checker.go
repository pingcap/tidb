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
	setting  *rmpb.RunawaySettings

	markedByRule  atomic.Bool
	markedByWatch bool
	watchAction   rmpb.RunawayAction
}

// NewChecker creates a new RunawayChecker.
func NewChecker(manager *Manager, resourceGroupName string, setting *rmpb.RunawaySettings,
	originalSQL, sqlDigest, planDigest string, startTime time.Time) *Checker {
	c := &Checker{
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
		c.deadline = startTime.Add(time.Duration(setting.Rule.ExecElapsedTimeMs) * time.Millisecond)
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
func (r *Checker) BeforeExecutor() error {
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
			r.markRunaway(MatchTypeWatch, action, &now)
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
func (r *Checker) BeforeCopRequest(req *tikvrpc.Request) error {
	if r == nil {
		return nil
	}
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
			r.markRunaway(MatchTypeIdentify, r.setting.Action, &now)
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
	if err == nil || r.setting == nil || r.setting.Action != rmpb.RunawayAction_Kill {
		return err
	}
	if strings.HasPrefix(err.Error(), "Coprocessor task terminated due to exceeding the deadline") {
		if !r.markedByRule.Load() {
			now := time.Now()
			if r.deadline.Before(now) && r.markedByRule.CompareAndSwap(false, true) {
				r.markRunaway(MatchTypeIdentify, r.setting.Action, &now)
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

func (r *Checker) markQuarantine(now *time.Time) {
	if r.setting.Watch == nil {
		return
	}
	ttl := time.Duration(r.setting.Watch.LastingDurationMs) * time.Millisecond

	r.manager.markQuarantine(r.resourceGroupName, r.getSettingConvictIdentifier(), r.setting.Watch.Type, r.setting.Action, ttl, now)
}

func (r *Checker) markRunaway(matchType MatchType, action rmpb.RunawayAction, now *time.Time) {
	actionStr := strings.ToLower(rmpb.RunawayAction_name[int32(action)])
	metrics.RunawayCheckerCounter.WithLabelValues(r.resourceGroupName, matchType.String(), actionStr).Inc()
	r.manager.markRunaway(r.resourceGroupName, r.originalSQL, r.planDigest, actionStr, matchType, now)
}

func (r *Checker) getSettingConvictIdentifier() string {
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

func (r *Checker) getConvictIdentifiers() []string {
	return []string{r.originalSQL, r.sqlDigest, r.planDigest}
}
