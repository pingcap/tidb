// Copyright 2024 PingCAP, Inc.
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

package join

import (
	"github.com/pingcap/tidb/pkg/util/memory"
)

// This variable should be const, but we need to modify it for test
var spillChunkSize = 1024

const spillInfo = "memory exceeds quota, spill to disk now."

const (
	notSpilled = iota
	needSpill
	inSpilling
)

type hashJoinSpillAction struct {
	memory.BaseOOMAction
	spillHelper *hashJoinSpillHelper
}

func newHashJoinSpillAction(spillHelper *hashJoinSpillHelper) *hashJoinSpillAction {
	return &hashJoinSpillAction{
		spillHelper: spillHelper,
	}
}

// GetPriority get the priority of the Action.
func (*hashJoinSpillAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

func (h *hashJoinSpillAction) Action(t *memory.Tracker) {
	if h.actionImpl(t) {
		return
	}

	if t.CheckExceed() && (!hasEnoughDataToSpill(h.spillHelper.hashJoinExec.memTracker, t) || !h.spillHelper.canSpill()) {
		h.triggerFallBackAction(t)
	}
}

func (h *hashJoinSpillAction) triggerFallBackAction(t *memory.Tracker) {
	if fallback := h.GetFallback(); fallback != nil {
		fallback.Action(t)
	}
}

// Return true if it successfully sets spill flag
func (h *hashJoinSpillAction) actionImpl(t *memory.Tracker) bool {
	h.spillHelper.cond.L.Lock()
	defer h.spillHelper.cond.L.Unlock()

	for h.spillHelper.isInSpillingNoLock() {
		h.spillHelper.cond.Wait()
	}

	if t.CheckExceed() && h.spillHelper.isNotSpilledNoLock() && hasEnoughDataToSpill(h.spillHelper.hashJoinExec.memTracker, t) && h.spillHelper.canSpill() {
		h.spillHelper.setNeedSpillNoLock()

		// Because all executors could keep running before spill flag is set to `inSpilling`, memory
		// consumption will be modified before we print the spill log. It's necessary to record the
		// memory consumption when spill is triggered.
		h.spillHelper.bytesConsumed.Store(t.BytesConsumed())
		h.spillHelper.bytesLimit.Store(t.GetBytesLimit())
		return true
	}

	return false
}

func hasEnoughDataToSpill(hashJoinTracker *memory.Tracker, passedInTracker *memory.Tracker) bool {
	return hashJoinTracker.BytesConsumed() >= passedInTracker.GetBytesLimit()/20
}
