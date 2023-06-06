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
	setting, err := rm.resourceGroupCtl.RunawaySettings(resourceGroupName)
	if err != nil || setting == nil {
		return nil
	}
	return newRunawayChecker(rm, resourceGroupName, setting, originalSql, planDigest)
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
	setttings         *rmpb.RunawaySettings
	deadline          time.Time
	marked            atomic.Bool
}

func newRunawayChecker(manager *RunawayManager, resourceGroupName string, setting *rmpb.RunawaySettings, originalSql string, planDigest string) *RunawayChecker {
	marked := atomic.Bool{}
	convict := getConvictName(setting, originalSql, planDigest)
	marked.Store(manager.ExamineWatchList(resourceGroupName, convict))
	return &RunawayChecker{
		manager:           manager,
		resourceGroupName: resourceGroupName,
		convict:           convict,
		deadline:          time.Now().Add(time.Duration(setting.Rule.ExecElapsedTimeMs) * time.Millisecond),
		setttings:         setting,
		marked:            marked,
	}
}

func (r *RunawayChecker) BeforeCopRequest(candidateActions ...RunawayActionWorker) error {
	if r == nil {
		return nil
	}
	marked := r.marked.Load()
	if !marked {
		// execution time exceeds the threshold, mark the query as runaway
		if r.deadline.Before(time.Now()) {
			r.marked.Store(true)
			r.markRunaway()
		} else {
			return nil
		}
	}
	for _, ac := range candidateActions {
		if ac.Type() == r.setttings.Action {
			return ac.Action()()
		}
	}
	return nil
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
	if r.setttings.Watch == nil {
		return
	}
	if !r.marked.Load() && r.deadline.Before(time.Now()) {
		r.marked.Store(true)
		r.manager.MarkRunaway(r.resourceGroupName, r.convict, time.Duration(r.setttings.Watch.LastingDurationMs)*time.Millisecond)
	}
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
