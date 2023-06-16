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
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/zap"
)

const (
	// DefaultResourceGroupName is the default resource group name.
	DefaultResourceGroupName = "default"
	// MaxWaitDuration is the max duration to wait for acquiring token buckets.
	MaxWaitDuration = time.Second * 30
)

// RunawayManager is used to detect and record runaway queries.
type RunawayManager struct {
	resourceGroupCtl *rmclient.ResourceGroupsController
	watchList        *ttlcache.Cache[string, struct{}]
}

// NewRunawayManager creates a new RunawayManager.
func NewRunawayManager(resourceGroupCtl *rmclient.ResourceGroupsController) *RunawayManager {
	watchList := ttlcache.New[string, struct{}]()
	go watchList.Start()
	return &RunawayManager{
		resourceGroupCtl: resourceGroupCtl,
		watchList:        watchList,
	}
}

// DeriveChecker derives a RunawayChecker from the given resource group
func (rm *RunawayManager) DeriveChecker(resourceGroupName string, originalSQL string, planDigest string) *RunawayChecker {
	group, err := rm.resourceGroupCtl.GetResourceGroup(resourceGroupName)
	if err != nil || group == nil {
		logutil.BgLogger().Warn("cannot setup up runaway checker", zap.Error(err))
		return nil
	}
	if group.RunawaySettings == nil {
		return nil
	}

	return newRunawayChecker(rm, resourceGroupName, group.RunawaySettings, originalSQL, planDigest)
}

// MarkRunaway marks the query as runaway.
func (rm *RunawayManager) MarkRunaway(resourceGroupName string, convict string, ttl time.Duration) {
	rm.watchList.Set(resourceGroupName+"/"+convict, struct{}{}, ttl)
}

// ExamineWatchList check whether the query is in watch list.
func (rm *RunawayManager) ExamineWatchList(resourceGroupName string, convict string) bool {
	return rm.watchList.Get(resourceGroupName+"/"+convict) != nil
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
	planDigest        string

	deadline time.Time
	setting  *rmpb.RunawaySettings

	marked atomic.Bool
}

func newRunawayChecker(manager *RunawayManager, resourceGroupName string, setting *rmpb.RunawaySettings, originalSQL string, planDigest string) *RunawayChecker {
	return &RunawayChecker{
		manager:           manager,
		resourceGroupName: resourceGroupName,
		originalSQL:       originalSQL,
		planDigest:        planDigest,
		deadline:          time.Now().Add(time.Duration(setting.Rule.ExecElapsedTimeMs) * time.Millisecond),
		setting:           setting,
		marked:            atomic.Bool{},
	}
}

// BeforeExecutor checks whether query is in watch list before executing and after compiling.
func (r *RunawayChecker) BeforeExecutor() error {
	if r == nil {
		return nil
	}
	result := r.manager.ExamineWatchList(r.resourceGroupName, r.getConvictIdentifier())
	if result {
		r.marked.Store(result)
		switch r.setting.Action {
		case rmpb.RunawayAction_Kill:
			return exeerrors.ErrResourceGroupQueryRunawayQuarantine
		case rmpb.RunawayAction_CoolDown:
			return nil
		case rmpb.RunawayAction_DryRun:
			return nil
		default:
		}
	}
	return nil
}

// BeforeCopRequest checks runaway and modifies the request if necessary before sending coprocessor request.
func (r *RunawayChecker) BeforeCopRequest(req *tikvrpc.Request) error {
	marked := r.marked.Load()
	if !marked {
		until := time.Until(r.deadline)
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
		if r.marked.CompareAndSwap(false, true) {
			r.markRunaway()
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

// AfterCopRequest checks runaway after receiving coprocessor response.
func (r *RunawayChecker) AfterCopRequest() {
	// Do not perform action here as it may be the last cop request and just let it finish. If it's not the last cop request, action would be performed in `BeforeCopRequest` when handling the next cop request.
	// Here only marks the query as runaway
	if !r.marked.Load() && r.deadline.Before(time.Now()) {
		if r.marked.CompareAndSwap(false, true) {
			r.markRunaway()
		}
	}
}

func (r *RunawayChecker) markRunaway() {
	if r.setting.Watch == nil {
		return
	}
	r.manager.MarkRunaway(r.resourceGroupName, r.getConvictIdentifier(), time.Duration(r.setting.Watch.LastingDurationMs)*time.Millisecond)
}

func (r *RunawayChecker) getConvictIdentifier() string {
	if r.setting.Watch == nil {
		return ""
	}
	switch r.setting.Watch.Type {
	case rmpb.RunawayWatchType_Similar:
		return r.planDigest
	case rmpb.RunawayWatchType_Exact:
		return r.originalSQL
	default:
		return ""
	}
}
