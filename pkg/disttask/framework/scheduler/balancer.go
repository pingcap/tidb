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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	llog "github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

var (
	// balanceCheckInterval is the interval to check whether we need to balance the subtasks.
	balanceCheckInterval = 3 * CheckTaskFinishedInterval
)

// balancer is used to balance subtasks on managed nodes
// it handles 2 cases:
//   - managed node scale in/out.
//   - nodes might run subtasks in different speed, the amount of data processed by subtasks varies, cause the subtasks are not balanced.
//
// we will try balance in task order, subtasks will be scheduled to the node with
// enough slots to run them, if there is no such node, we will skip balance for
// the task and try next one.
type balancer struct {
	Param
	logger *zap.Logger
	// a helper temporary map to record the used slots of each node during balance
	// to avoid passing it around.
	currUsedSlots map[string]int
}

func newBalancer(param Param) *balancer {
	logger := log.L()
	if intest.InTest {
		logger = log.L().With(zap.String("server-id", param.serverID))
	}
	return &balancer{
		Param:         param,
		logger:        logger,
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
	managedNodes := b.nodeMgr.getNodes()
	b.currUsedSlots = make(map[string]int, len(managedNodes))
	for _, n := range managedNodes {
		b.currUsedSlots[n.ID] = 0
	}

	schedulers := sm.getSchedulers()
	for _, sch := range schedulers {
		nodeIDs := filterByScope(managedNodes, sch.GetTask().TargetScope)
		if err := b.balanceSubtasks(ctx, sch, nodeIDs); err != nil {
			b.logger.Warn("failed to balance subtasks",
				zap.Int64("task-id", sch.GetTask().ID), llog.ShortError(err))
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

	averageSubtaskCnt := len(subtasks) / len(adjustedNodes)
	averageSubtaskRemainder := len(subtasks) - averageSubtaskCnt*len(adjustedNodes)
	executorSubtasks := make(map[string][]*proto.SubtaskBase, len(adjustedNodes))
	executorPendingCnts := make(map[string]int, len(adjustedNodes))
	for _, node := range adjustedNodes {
		executorSubtasks[node] = make([]*proto.SubtaskBase, 0, averageSubtaskCnt+1)
	}
	for _, subtask := range subtasks {
		// put running subtask in the front of slice.
		// if subtask fail-over, it's possible that there are multiple running
		// subtasks for one task executor.
		if subtask.State == proto.SubtaskStateRunning {
			executorSubtasks[subtask.ExecID] = append([]*proto.SubtaskBase{subtask}, executorSubtasks[subtask.ExecID]...)
		} else {
			executorSubtasks[subtask.ExecID] = append(executorSubtasks[subtask.ExecID], subtask)
			executorPendingCnts[subtask.ExecID]++
		}
	}

	subtasksNeedSchedule := make([]*proto.SubtaskBase, 0)
	remainder := averageSubtaskRemainder
	executorWithOneMoreSubtask := make(map[string]struct{}, remainder)
	for node, sts := range executorSubtasks {
		if _, ok := adjustedNodeMap[node]; !ok {
			b.logger.Info("dead node or not have enough slots, schedule subtasks away",
				zap.Int64("task-id", taskID),
				zap.String("node", node),
				zap.Int("slot-capacity", b.slotMgr.getCapacity()),
				zap.Int("used-slots", b.currUsedSlots[node]))
			// dead node or not have enough slots
			subtasksNeedSchedule = append(subtasksNeedSchedule, sts...)
			delete(executorSubtasks, node)
			continue
		}
		if remainder > 0 {
			// first remainder nodes will get 1 more subtask.
			if len(sts) >= averageSubtaskCnt+1 {
				needScheduleCnt := len(sts) - (averageSubtaskCnt + 1)
				// running subtasks are never balanced.
				needScheduleCnt = min(executorPendingCnts[node], needScheduleCnt)
				subtasksNeedSchedule = append(subtasksNeedSchedule, sts[len(sts)-needScheduleCnt:]...)
				executorSubtasks[node] = sts[:len(sts)-needScheduleCnt]

				executorWithOneMoreSubtask[node] = struct{}{}
				remainder--
			}
		} else if len(sts) > averageSubtaskCnt {
			// running subtasks are never balanced.
			cnt := min(executorPendingCnts[node], len(sts)-averageSubtaskCnt)
			subtasksNeedSchedule = append(subtasksNeedSchedule, sts[len(sts)-cnt:]...)
			executorSubtasks[node] = sts[:len(sts)-cnt]
		}
	}
	if len(subtasksNeedSchedule) == 0 {
		return nil
	}

	for i := 0; i < len(adjustedNodes) && remainder > 0; i++ {
		if _, ok := executorWithOneMoreSubtask[adjustedNodes[i]]; !ok {
			executorWithOneMoreSubtask[adjustedNodes[i]] = struct{}{}
			remainder--
		}
	}

	fillIdx := 0
	for _, node := range adjustedNodes {
		sts := executorSubtasks[node]
		targetSubtaskCnt := averageSubtaskCnt
		if _, ok := executorWithOneMoreSubtask[node]; ok {
			targetSubtaskCnt = averageSubtaskCnt + 1
		}
		for i := len(sts); i < targetSubtaskCnt && fillIdx < len(subtasksNeedSchedule); i++ {
			subtasksNeedSchedule[fillIdx].ExecID = node
			fillIdx++
		}
	}

	if err = b.taskMgr.UpdateSubtasksExecIDs(ctx, subtasksNeedSchedule); err != nil {
		return err
	}
	b.logger.Info("balance subtasks", zap.Stringers("subtasks", subtasksNeedSchedule))
	return nil
}

func (b *balancer) updateUsedNodes(subtasks []*proto.SubtaskBase) {
	used := make(map[string]int, len(b.currUsedSlots))
	// see slotManager.alloc in task executor.
	for _, st := range subtasks {
		if _, ok := used[st.ExecID]; !ok {
			used[st.ExecID] = st.Concurrency
		}
	}

	for node, slots := range used {
		b.currUsedSlots[node] += slots
	}
}
