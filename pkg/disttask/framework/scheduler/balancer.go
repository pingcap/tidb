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

package scheduler

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	// balanceCheckInterval is the interval to check whether we need to balance the subtasks.
	balanceCheckInterval = 3 * checkTaskFinishedInterval
)

// balancer is used to balance subtasks on managed nodes
// it handles 2 cases:
//   - managed node scale in/out.
//   - nodes might run subtasks in different speed, cause the subtasks are not balanced.
//
// we will try balance in task order, subtasks will be scheduled to the node with
// enough slots to run them, if there is no such node, we will skip balance for
// the task and try next one.
type balancer struct {
	Param

	// a helper temporary map to record the used slots of each node during balance
	// to avoid passing too many parameters.
	currUsedSlots map[string]int
}

func newBalancer(param Param) *balancer {
	return &balancer{
		Param:         param,
		currUsedSlots: make(map[string]int),
	}
}

func (b *balancer) balanceLoop(ctx context.Context, sm *Manager) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(balanceCheckInterval):
		}
		b.balance(ctx, sm)
	}
}

func (b *balancer) balance(ctx context.Context, sm *Manager) {
	// we will use currUsedSlots to calculate adjusted eligible nodes during balance,
	// it's initial value depends on the managed nodes, to have a consistent view,
	// DO NOT call getManagedNodes twice during 1 balance.
	managedNodes := b.nodeMgr.getManagedNodes()
	b.currUsedSlots = make(map[string]int, len(managedNodes))
	for _, n := range managedNodes {
		b.currUsedSlots[n] = 0
	}

	schedulers := sm.getSchedulers()
	for _, sch := range schedulers {
		if err := b.balanceSubtasks(ctx, sch, managedNodes); err != nil {
			logutil.Logger(ctx).Warn("failed to balance subtasks",
				zap.Int64("task-id", sch.GetTask().ID), log.ShortError(err))
			return
		}
	}
}

func (b *balancer) balanceSubtasks(ctx context.Context, sch Scheduler, managedNodes []string) error {
	task := sch.GetTask()
	eligibleNodes, err := getEligibleNodes(ctx, sch, managedNodes)
	if err != nil {
		return err
	}
	if len(eligibleNodes) == 0 {
		return errors.New("no eligible nodes to balance subtasks")
	}
	return b.doBalanceSubtasks(ctx, task.ID, eligibleNodes)
}

func (b *balancer) doBalanceSubtasks(ctx context.Context, taskID int64, eligibleNodes []string) (err error) {
	subtasks, err := b.taskMgr.GetActiveSubtasks(ctx, taskID)
	if err != nil {
		return err
	}
	if len(subtasks) == 0 {
		return nil
	}

	// balance subtasks only to nodes with enough slots, from the view of all
	// managed nodes, subtasks of task might not be balanced.
	adjustedNodes := filterNodesWithEnoughSlots(b.currUsedSlots, b.slotMgr.getCapacity(),
		eligibleNodes, subtasks[0].Concurrency)
	if len(adjustedNodes) == 0 {
		// no node has enough slots to run the subtasks, skip balance and skip
		// update used slots.
		return nil
	}
	adjustedNodeMap := make(map[string]struct{}, len(adjustedNodes))
	for _, n := range adjustedNodes {
		adjustedNodeMap[n] = struct{}{}
	}

	defer func() {
		if err == nil {
			b.updateUsedNodes(subtasks)
		}
	}()

	averageSubtaskCnt := (len(subtasks) + len(adjustedNodes) - 1) / len(adjustedNodes)
	executorSubtasks := make(map[string][]*proto.Subtask, len(adjustedNodes))
	executorPendingCnts := make(map[string]int, len(adjustedNodes))
	for _, node := range adjustedNodes {
		executorSubtasks[node] = make([]*proto.Subtask, 0, averageSubtaskCnt)
	}
	for _, subtask := range subtasks {
		// put running subtask in the front of slice.
		// if subtask fail-over, it's possible that there are multiple running
		// subtasks for one task executor.
		if subtask.State == proto.TaskStateRunning {
			executorSubtasks[subtask.ExecID] = append([]*proto.Subtask{subtask}, executorSubtasks[subtask.ExecID]...)
		} else {
			executorSubtasks[subtask.ExecID] = append(executorSubtasks[subtask.ExecID], subtask)
			executorPendingCnts[subtask.ExecID]++
		}
	}

	subtasksNeedSchedule := make([]*proto.Subtask, 0)
	for k, v := range executorSubtasks {
		if _, ok := adjustedNodeMap[k]; !ok {
			// dead node
			subtasksNeedSchedule = append(subtasksNeedSchedule, v...)
			delete(executorSubtasks, k)
			continue
		}
		if len(v) > averageSubtaskCnt {
			// running subtasks are never balanced.
			cnt := min(executorPendingCnts[k], len(v)-averageSubtaskCnt)
			subtasksNeedSchedule = append(subtasksNeedSchedule, v[len(v)-cnt:]...)
			executorSubtasks[k] = v[:len(v)-cnt]
		}
	}
	if len(subtasksNeedSchedule) == 0 {
		return nil
	}

	fillIdx := 0
	for _, node := range adjustedNodes {
		sts := executorSubtasks[node]
		for i := len(sts); i < averageSubtaskCnt && fillIdx < len(subtasksNeedSchedule); i++ {
			subtasksNeedSchedule[fillIdx].ExecID = node
			fillIdx++
		}
	}

	if err = b.taskMgr.UpdateSubtasksExecIDs(ctx, subtasksNeedSchedule); err != nil {
		return err
	}
	logutil.BgLogger().Info("balance subtasks", zap.Stringers("balanced-cnt", subtasksNeedSchedule))
	return nil
}

func (b *balancer) updateUsedNodes(subtasks []*proto.Subtask) {
	used := make(map[string]int, len(b.currUsedSlots))
	for _, st := range subtasks {
		if _, ok := used[st.ExecID]; !ok {
			used[st.ExecID] = st.Concurrency
		}
	}

	for node, slots := range used {
		b.currUsedSlots[node] += slots
	}
}
