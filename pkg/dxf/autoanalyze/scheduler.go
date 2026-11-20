// Copyright 2026 PingCAP, Inc.
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

package autoanalyze

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"go.uber.org/zap"
)

// placements holds the node of each task whose subtask the used slots in the storage may not
// count yet. Without it, tasks placed at about the same time would all take the same node.
type placements struct {
	mu    sync.Mutex
	nodes map[int64]string
}

var placed = &placements{nodes: make(map[int64]string)}

// place returns nodes ordered for the task, fewest used slots first, and keeps the first as
// its place. The place of another task counts as one used slot, which is what a task asks for.
func (p *placements) place(taskID int64, nodes []string, usedSlots map[string]int) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.nodes, taskID)
	used := make(map[string]int, len(nodes))
	for _, node := range nodes {
		used[node] = usedSlots[node]
	}
	for _, node := range p.nodes {
		used[node]++
	}
	ordered := orderByUsedSlots(nodes, used, taskID)
	p.nodes[taskID] = ordered[0]
	return ordered
}

func (p *placements) release(taskID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.nodes, taskID)
}

type schedulerImpl struct {
	*scheduler.BaseScheduler
	// closed makes GetEligibleInstances keep no place for the task after
	// Close: nothing would release it.
	closed atomic.Bool
}

var _ scheduler.Scheduler = (*schedulerImpl)(nil)

func newScheduler(ctx context.Context, task *proto.Task, param scheduler.Param) *schedulerImpl {
	return &schedulerImpl{BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param)}
}

func (s *schedulerImpl) Init() error {
	s.BaseScheduler.Extension = s
	return s.BaseScheduler.Init()
}

// OnTick releases the place of the task. It is called once the subtask exists,
// and from then on the used slots in the storage count it.
func (*schedulerImpl) OnTick(_ context.Context, task *proto.Task) {
	placed.release(task.ID)
}

// OnNextSubtasksBatch generates the only subtask, which carries the task meta as is.
func (*schedulerImpl) OnNextSubtasksBatch(_ context.Context, _ storage.TaskHandle,
	task *proto.Task, _ []string, _ proto.Step) ([][]byte, error) {
	return [][]byte{task.Meta}, nil
}

// OnDone releases the place of the task: a subtask that ends within a tick
// never reaches OnTick.
func (*schedulerImpl) OnDone(_ context.Context, _ storage.TaskHandle, task *proto.Task) error {
	placed.release(task.ID)
	return nil
}

// Close releases the place of the task when the scheduler stops before the
// task is done, e.g. when this node is no longer the DXF owner.
func (s *schedulerImpl) Close() {
	s.closed.Store(true)
	placed.release(s.GetTask().ID)
	s.BaseScheduler.Close()
}

// GetEligibleInstances returns the nodes of the target scope, fewest used slots first: the
// framework runs a task of max node count 1 on the first node. Nil falls back to host order.
func (s *schedulerImpl) GetEligibleInstances(ctx context.Context, task *proto.Task) ([]string, error) {
	nodes := s.GetNodesInScope()
	if len(nodes) == 0 {
		return nil, nil
	}
	usedSlots, err := s.GetTaskMgr().GetUsedSlotsOnNodes(ctx)
	if err != nil {
		s.GetLogger().Warn("get used slots on nodes failed, place auto analyze task by host order", zap.Error(err))
		return nil, nil
	}
	if task.Step != proto.StepInit {
		// The subtask exists and usedSlots counts it. Only the balancer asks
		// for such a task, to move the subtask away from a lost node.
		return orderByUsedSlots(nodes, usedSlots, task.ID), nil
	}
	ordered := placed.place(task.ID, nodes, usedSlots)
	// The balancer may also ask after Close. Close sets closed before it
	// releases, so one of the two releases comes after this place.
	if s.closed.Load() {
		placed.release(task.ID)
	}
	return ordered, nil
}

// orderByUsedSlots returns nodes, which is not empty, sorted by used slots, ascending. Nodes
// with equal used slots rotate by the task ID, so that tasks do not all prefer the first one.
func orderByUsedSlots(nodes []string, usedSlots map[string]int, taskID int64) []string {
	n := len(nodes)
	shift := int(taskID % int64(n))
	positions := make([]int, n)
	for i := range positions {
		positions[i] = i
	}
	slices.SortFunc(positions, func(a, b int) int {
		return cmp.Or(
			cmp.Compare(usedSlots[nodes[a]], usedSlots[nodes[b]]),
			cmp.Compare((a-shift+n)%n, (b-shift+n)%n),
		)
	})
	ordered := make([]string, 0, n)
	for _, i := range positions {
		ordered = append(ordered, nodes[i])
	}
	return ordered
}

// IsRetryableErr returns false: the priority queue of the stats owner analyzes
// the table again later if the task fails.
func (*schedulerImpl) IsRetryableErr(error) bool {
	return false
}

func (*schedulerImpl) GetNextStep(task *proto.TaskBase) proto.Step {
	if task.Step == proto.StepInit {
		return proto.AutoAnalyzeStepRun
	}
	return proto.StepDone
}

func (*schedulerImpl) OnPrepare(context.Context, storage.TaskHandle, *proto.Task) error {
	return nil
}

// ModifyMeta implements scheduler.Extension. The task meta is never modified.
func (*schedulerImpl) ModifyMeta(oldMeta []byte, _ []proto.Modification) ([]byte, error) {
	return oldMeta, nil
}
