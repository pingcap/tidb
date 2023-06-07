// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type RunawayWorker struct {
	actionType rmpb.RunawayAction
	action     func() error
}

func (w *RunawayWorker) Action() func() error {
	return w.action
}

func (w *RunawayWorker) Type() rmpb.RunawayAction {
	return w.actionType
}

var (
	RunawayActionKillWorker *RunawayWorker = &RunawayWorker{
		actionType: rmpb.RunawayAction_Kill,
		action: func() error {
			return derr.ErrResourceGroupQueryRunaway
		},
	}
	RunawayActionWatchKillWorker *RunawayWorker = &RunawayWorker{
		actionType: rmpb.RunawayAction_Kill,
		action: func() error {
			return derr.ErrResourceGroupQueryRunawayQuarantine
		},
	}
	// Todo: add function for dryrun.
)

func createCooldownWorker(req *tikvrpc.Request) *RunawayWorker {
	return &RunawayWorker{
		actionType: rmpb.RunawayAction_CoolDown,
		action: func() error {
			req.ResourceControlContext.OverridePriority = 1 // set priority to lowest
			return nil
		},
	}
}
