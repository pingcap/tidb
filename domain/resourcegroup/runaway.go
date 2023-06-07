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

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/tikv/client-go/v2/tikvrpc"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

type RunawayManager struct {
	resourceGroupCtl *rmclient.ResourceGroupsController
	// TODO: add watch records
}

func NewRunawayManager(resourceGroupCtl *rmclient.ResourceGroupsController) *RunawayManager {
	return &RunawayManager{
		resourceGroupCtl: resourceGroupCtl,
	}
}

func (rm *RunawayManager) DeriveChecker(resourceGroupName string, originalSql string, planDigest string) *RunawayChecker {
	setting, err := rm.resourceGroupCtl.RunawaySettings(resourceGroupName)
	if err != nil || setting == nil {
		return nil
	}
	return newRunawayChecker(rm, setting, originalSql, planDigest)
}

func (rm *RunawayManager) MarkRunaway(originalSql string, planDigest string) {
	// TODO: insert into watch records
}

type RunawayChecker struct {
	manager     *RunawayManager
	originalSql string
	planDigest  string

	deadline time.Time
	action   rmpb.RunawayAction

	marked atomic.Bool
}

func newRunawayChecker(manager *RunawayManager, setting *rmpb.RunawaySettings, originalSql string, planDigest string) *RunawayChecker {
	return &RunawayChecker{
		manager:     manager,
		originalSql: originalSql,
		planDigest:  planDigest,
		deadline:    time.Now().Add(time.Duration(setting.Rule.ExecElapsedTimeMs) * time.Millisecond),
		action:      setting.Action,
		marked:      atomic.Bool{},
	}
}

func (r *RunawayChecker) BeforeCopRequest(req *tikvrpc.Request) error {
	marked := r.marked.Load()
	if !marked {
		// execution time exceeds the threshold, mark the query as runaway
		if r.deadline.Before(time.Now()) {
			r.marked.Store(true)
			r.manager.MarkRunaway(r.originalSql, r.planDigest)
		} else {
			return nil
		}
	}
	switch r.action {
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
	// Do not perform action here as it may be the last cop request and just let it finish. If it's not the last cop request, action would be performed in `BeforeCopRequest` when handling the next cop request.
	// Here only marks the query as runaway
	if !r.marked.Load() && r.deadline.Before(time.Now()) {
		r.marked.Store(true)
		r.manager.MarkRunaway(r.originalSql, r.planDigest)
	}
}
