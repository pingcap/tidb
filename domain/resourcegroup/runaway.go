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
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

// DefaultResourceGroupName is the default resource group name.
const DefaultResourceGroupName = "default"

// RunawayManager is used to detect and record runaway queries.
type RunawayManager struct {
	resourceGroupCtl *rmclient.ResourceGroupsController
	// TODO: add watch records
}

// NewRunawayManager creates a new RunawayManager.
func NewRunawayManager(resourceGroupCtl *rmclient.ResourceGroupsController) *RunawayManager {
	return &RunawayManager{
		resourceGroupCtl: resourceGroupCtl,
	}
}

// DeriveChecker derives a RunawayChecker from the given resource group
func (rm *RunawayManager) DeriveChecker(resourceGroupName string, originalSQL string, planDigest string) *RunawayChecker {
	group, err := rm.resourceGroupCtl.GetResourceGroup(resourceGroupName)
	if err != nil || group == nil || group.RunawaySettings == nil {
		return nil
	}
	return newRunawayChecker(rm, group.RunawaySettings, originalSQL, planDigest)
}

// MarkRunaway marks the query as runaway.
func (rm *RunawayManager) MarkRunaway(originalSQL string, planDigest string) {
	// TODO: insert into watch records
}

// RunawayChecker is used to check if the query is runaway.
type RunawayChecker struct {
	manager     *RunawayManager
	originalSQL string
	planDigest  string

	deadline time.Time
	action   rmpb.RunawayAction

	marked atomic.Bool
}

func newRunawayChecker(manager *RunawayManager, setting *rmpb.RunawaySettings, originalSQL string, planDigest string) *RunawayChecker {
	return &RunawayChecker{
		manager:     manager,
		originalSQL: originalSQL,
		planDigest:  planDigest,
		deadline:    time.Now().Add(time.Duration(setting.Rule.ExecElapsedTimeMs) * time.Millisecond),
		action:      setting.Action,
		marked:      atomic.Bool{},
	}
}

// BeforeCopRequest checks runaway and modifies the request if necessary before sending coprocessor request.
func (r *RunawayChecker) BeforeCopRequest(req *tikvrpc.Request) error {
	marked := r.marked.Load()
	if !marked {
		until := time.Until(r.deadline)
		// execution time exceeds the threshold, mark the query as runaway
		if until <= 0 {
			if r.marked.CompareAndSwap(false, true) {
				r.manager.MarkRunaway(r.originalSQL, r.planDigest)
			}
		} else {
			if r.action == rmpb.RunawayAction_Kill {
				// if the execution time is close to the threshold, set a timeout
				if until < tikv.ReadTimeoutMedium {
					req.Context.MaxExecutionDurationMs = uint64(until.Milliseconds())
				}
			}
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

// AfterCopRequest checks runaway after receiving coprocessor response.
func (r *RunawayChecker) AfterCopRequest() {
	// Do not perform action here as it may be the last cop request and just let it finish. If it's not the last cop request, action would be performed in `BeforeCopRequest` when handling the next cop request.
	// Here only marks the query as runaway
	if !r.marked.Load() && r.deadline.Before(time.Now()) {
		if r.marked.CompareAndSwap(false, true) {
			r.manager.MarkRunaway(r.originalSQL, r.planDigest)
		}
	}
}
