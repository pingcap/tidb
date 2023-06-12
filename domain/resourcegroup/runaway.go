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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

type RunawayManager struct {
	resourceGroupCtl *rmclient.ResourceGroupsController
	watchList        *ttlcache.Cache[string, struct{}]
}

func NewRunawayManager(resourceGroupCtl *rmclient.ResourceGroupsController) *RunawayManager {
	watchList := ttlcache.New[string, struct{}]()
	go watchList.Start()
	return &RunawayManager{
		resourceGroupCtl: resourceGroupCtl,
		watchList:        watchList,
	}
}

func (rm *RunawayManager) DeriveChecker(resourceGroupName string, originalSql string, planDigest string) *RunawayChecker {
	meta, err := rm.resourceGroupCtl.GetResourceGroup(resourceGroupName)
	if err != nil || meta == nil || meta.RunawaySettings == nil {
		return nil
	}

	return newRunawayChecker(rm, resourceGroupName, meta.RunawaySettings, originalSql, planDigest)
}

func (rm *RunawayManager) MarkRunaway(resourceGroupName string, convict string, ttl time.Duration) {
	rm.watchList.Set(resourceGroupName+"/"+convict, struct{}{}, ttl)
}

func (rm *RunawayManager) ExamineWatchList(resourceGroupName string, convict string) bool {
	return rm.watchList.Get(resourceGroupName+"/"+convict) != nil
}

func (rm *RunawayManager) Stop() {
	rm.watchList.Stop()
}

type RunawayChecker struct {
	manager           *RunawayManager
	resourceGroupName string
	convict           string
	setting           *rmpb.RunawaySettings
	deadline          time.Time
	marked            atomic.Bool
}

func newRunawayChecker(manager *RunawayManager, resourceGroupName string, setting *rmpb.RunawaySettings, originalSql string, planDigest string) *RunawayChecker {
	convict := getConvictName(setting, originalSql, planDigest)
	return &RunawayChecker{
		manager:           manager,
		resourceGroupName: resourceGroupName,
		convict:           convict,
		deadline:          time.Now().Add(time.Duration(setting.Rule.ExecElapsedTimeMs) * time.Millisecond),
		setting:           setting,
		marked:            atomic.Bool{},
	}
}

// BeforeExecutor checks whether query is in watch list before executing and after compiling.
func (r *RunawayChecker) BeforeExecutor() error {
	result := r.manager.ExamineWatchList(r.resourceGroupName, r.convict)
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
	if r == nil {
		return nil
	}
	marked := r.marked.Load()
	fmt.Println("############", time.Now(), r.deadline)
	if !marked {
		until := time.Until(r.deadline)
		// execution time exceeds the threshold, mark the query as runaway
		if until <= 0 {
			r.marked.Store(true)
			r.markRunaway()
			fmt.Println("r.marked.Store(true)")
		} else {
			if r.setting.Action == rmpb.RunawayAction_Kill {
				// if the execution time is close to the threshold, set a timeout
				if until < tikv.ReadTimeoutMedium {
					req.Context.MaxExecutionDurationMs = uint64(until.Milliseconds())
				}
			}
			return nil
		}
	}
	switch r.setting.Action {
	case rmpb.RunawayAction_Kill:
		return exeerrors.ErrResourceGroupQueryRunaway
	case rmpb.RunawayAction_CoolDown:
		req.ResourceControlContext.OverridePriority = 1 // set priority to lowest
		return nil
	case rmpb.RunawayAction_DryRun:
		return nil
	default:
		return nil
	}
}

func (r *RunawayChecker) AfterCopRequest() {
	if r == nil {
		return
	}
	// Do not perform action here as it may be the last cop request and just let it finish. If it's not the last cop request, action would be performed in `BeforeCopRequest` when handling the next cop request.
	// Here only marks the query as runaway
	if !r.marked.Load() && r.deadline.Before(time.Now()) {
		r.marked.Store(true)
		r.markRunaway()
	}
}

func (r *RunawayChecker) markRunaway() {
	if r.setting.Watch == nil {
		return
	}
	r.manager.MarkRunaway(r.resourceGroupName, r.convict, time.Duration(r.setting.Watch.LastingDurationMs)*time.Millisecond)
}

func getConvictName(settings *rmpb.RunawaySettings, originalSql string, planDigest string) string {
	if settings.Watch == nil {
		return ""
	}
	switch settings.Watch.Type {
	case rmpb.RunawayWatchType_Similar:
		return planDigest
	case rmpb.RunawayWatchType_Exact:
		return originalSql
	default:
		return ""
	}
}

type DoAction func(rmpb.RunawayAction) error

type RunawayActionWorker interface {
	Type() rmpb.RunawayAction
	Action() func() error
}
