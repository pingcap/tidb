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

package testutil

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

type tidbNode struct {
	id     string
	owner  bool
	exeMgr *taskexecutor.Manager
	schMgr *scheduler.Manager
}

// TestDXFContext is the context for testing DXF.
type TestDXFContext struct {
	T           testing.TB
	Store       kv.Storage
	Ctx         context.Context
	TaskMgr     *storage.TaskManager
	MockCtrl    *gomock.Controller
	TestContext *TestContext
	Rand        *rand.Rand

	idAllocator atomic.Int32
	// in real case, when node scale in/out, the node might use the same IP or host name
	// such as using K8S, so we use this to simulate this case.
	nodeIDPool chan string
	wg         tidbutil.WaitGroupWrapper
	mu         struct {
		sync.RWMutex
		// to test network partition, we allow multiple owners
		ownerIndices map[string]int
		nodeIndices  map[string]int
		nodes        []*tidbNode
	}
}

// NewDXFContextWithRandomNodes creates a new TestDXFContext with random number
// of nodes in range [minCnt, maxCnt].
func NewDXFContextWithRandomNodes(t testing.TB, minCnt, maxCnt int) *TestDXFContext {
	c := newTestDXFContext(t)
	nodeNum := c.Rand.Intn(maxCnt-minCnt+1) + minCnt
	t.Logf("dxf context with random node num: %d", nodeNum)
	c.init(nodeNum, 16, true)
	return c
}

// NewTestDXFContext creates a new TestDXFContext.
func NewTestDXFContext(t testing.TB, nodeNum int, cpuCount int, reduceCheckInterval bool) *TestDXFContext {
	c := newTestDXFContext(t)
	c.init(nodeNum, cpuCount, reduceCheckInterval)
	return c
}

func newTestDXFContext(t testing.TB) *TestDXFContext {
	seed := time.Now().UnixNano()
	t.Log("dxf context seed:", seed)
	c := &TestDXFContext{
		T: t,
		TestContext: &TestContext{
			subtasksHasRun: make(map[string]map[int64]struct{}),
		},
		Rand:       rand.New(rand.NewSource(seed)),
		nodeIDPool: make(chan string, 100),
	}
	c.mu.ownerIndices = make(map[string]int)
	c.mu.nodeIndices = make(map[string]int)
	return c
}

func (c *TestDXFContext) init(nodeNum, cpuCount int, reduceCheckInterval bool) {
	if reduceCheckInterval {
		// make test faster
		ReduceCheckInterval(c.T)
	}
	// all nodes are isometric
	term := fmt.Sprintf("return(%d)", cpuCount)
	testfailpoint.Enable(c.T, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", term)
	testfailpoint.Enable(c.T, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")
	testfailpoint.Enable(c.T, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTaskExecutorNodes", "return()")
	store := testkit.CreateMockStore(c.T)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return testkit.NewSession(c.T, store), nil
	}, 10, 10, time.Second)
	c.T.Cleanup(func() {
		pool.Close()
	})
	taskManager := storage.NewTaskManager(pool)
	storage.SetTaskManager(taskManager)
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	ctrl := gomock.NewController(c.T)
	c.Store = store
	c.Ctx = ctx
	c.TaskMgr = taskManager
	c.MockCtrl = ctrl

	for i := 0; i < nodeNum; i++ {
		c.ScaleOutBy(c.getNodeID(), false)
	}
	c.electIfNeeded()

	c.T.Cleanup(func() {
		ctrl.Finish()
		c.close()
	})
}

func (c *TestDXFContext) getNodeID() string {
	select {
	case id := <-c.nodeIDPool:
		return id
	default:
		return fmt.Sprintf(":%d", 4000-1+c.idAllocator.Add(1))
	}
}

func (c *TestDXFContext) recycleNodeID(id string) {
	select {
	case c.nodeIDPool <- id:
	default:
	}
}

// ScaleOut scales out a tidb node, and elect owner if required.
func (c *TestDXFContext) ScaleOut(nodeNum int) {
	for i := 0; i < nodeNum; i++ {
		c.ScaleOutBy(c.getNodeID(), false)
	}
	c.electIfNeeded()
}

// ScaleOutBy scales out a tidb node by id, and set it as owner if required.
func (c *TestDXFContext) ScaleOutBy(id string, owner bool) {
	c.T.Logf("scale out node of id = %s, owner = %t", id, owner)
	exeMgr, err := taskexecutor.NewManager(c.Ctx, id, c.TaskMgr)
	require.NoError(c.T, err)
	require.NoError(c.T, exeMgr.InitMeta())
	require.NoError(c.T, exeMgr.Start())
	var schMgr *scheduler.Manager
	if owner {
		schMgr = scheduler.NewManager(c.Ctx, c.TaskMgr, id)
		schMgr.Start()
	}
	node := &tidbNode{
		id:     id,
		owner:  owner,
		exeMgr: exeMgr,
		schMgr: schMgr,
	}

	c.mu.Lock()
	c.mu.nodes = append(c.mu.nodes, node)
	c.mu.nodeIndices[id] = len(c.mu.nodes) - 1
	if owner {
		c.mu.ownerIndices[id] = len(c.mu.nodes) - 1
	}
	c.mu.Unlock()

	c.updateLiveExecIDs()
}

func (c *TestDXFContext) updateLiveExecIDs() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	execIDs := make([]string, 0, len(c.mu.nodes)+1)
	for _, n := range c.mu.nodes {
		execIDs = append(execIDs, n.id)
	}
	scheduler.MockServerInfo.Store(&execIDs)
}

// ScaleIn scales in some last added tidb nodes, elect new owner if required.
func (c *TestDXFContext) ScaleIn(nodeNum int) {
	for i := 0; i < nodeNum; i++ {
		c.mu.RLock()
		if len(c.mu.nodes) == 0 {
			c.mu.RUnlock()
			return
		}
		node := c.mu.nodes[len(c.mu.nodes)-1]
		c.mu.RUnlock()

		c.ScaleInBy(node.id)
	}
}

// ScaleInBy scales in a tidb node by id, elect new owner if required.
func (c *TestDXFContext) ScaleInBy(id string) {
	c.mu.Lock()
	idx, ok := c.mu.nodeIndices[id]
	if !ok {
		c.mu.Unlock()
		c.T.Logf("scale in failed, cannot find node %s", id)
		return
	}
	node := c.mu.nodes[idx]
	c.mu.nodes = append(c.mu.nodes[:idx], c.mu.nodes[idx+1:]...)
	c.mu.nodeIndices = make(map[string]int, len(c.mu.nodes))
	c.mu.ownerIndices = make(map[string]int, len(c.mu.nodes))
	for i, n := range c.mu.nodes {
		c.mu.nodeIndices[n.id] = i
		if n.owner {
			c.mu.ownerIndices[n.id] = i
		}
	}
	c.recycleNodeID(id)
	c.mu.Unlock()

	c.updateLiveExecIDs()

	c.T.Logf("scale in node of id = %s, owner = %t", node.id, node.owner)
	node.exeMgr.Stop()
	if node.owner {
		node.schMgr.Stop()
	}

	c.electIfNeeded()
}

// AsyncChangeOwner resigns all current owners and changes the owner of the cluster to random node asynchronously.
func (c *TestDXFContext) AsyncChangeOwner() {
	c.wg.RunWithLog(c.ChangeOwner)
}

// ChangeOwner resigns all current owners and changes the owner of the cluster to random node.
func (c *TestDXFContext) ChangeOwner() {
	c.T.Logf("changing owner")
	c.mu.Lock()
	if len(c.mu.nodes) == 0 {
		c.mu.Unlock()
		c.T.Logf("there no node, cannot change owner")
		return
	}
	for _, idx := range c.mu.ownerIndices {
		c.mu.nodes[idx].schMgr.Stop()
		c.mu.nodes[idx].schMgr = nil
		c.mu.nodes[idx].owner = false
	}
	c.mu.ownerIndices = make(map[string]int)
	c.mu.Unlock()

	c.electIfNeeded()
}

// AsyncShutdown shutdown node asynchronously.
func (c *TestDXFContext) AsyncShutdown(id string) {
	c.T.Logf("shuting down node of id %s", id)
	// as this code is run inside a fail-point, so we cancel them first, then
	// wait them asynchronously.
	node := c.getNode(id)
	if node == nil {
		return
	}
	node.exeMgr.Cancel()
	if node.owner {
		node.schMgr.Cancel()
	}
	c.wg.RunWithLog(func() {
		c.ScaleInBy(id)
	})
}

// GetRandNodeIDs returns `limit` random node ids.
// if limit > len(nodes), return all nodes.
func (c *TestDXFContext) GetRandNodeIDs(limit int) map[string]struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.mu.nodes) == 0 {
		return nil
	}
	cloneSlice := make([]*tidbNode, len(c.mu.nodes))
	copy(cloneSlice, c.mu.nodes)
	rand.Shuffle(len(cloneSlice), func(i, j int) {
		cloneSlice[i], cloneSlice[j] = cloneSlice[j], cloneSlice[i]
	})

	if limit > len(c.mu.nodes) {
		limit = len(c.mu.nodes)
	}
	ids := make(map[string]struct{}, limit)
	for i := 0; i < limit; i++ {
		ids[cloneSlice[i].id] = struct{}{}
	}
	return ids
}

// GetNodeIDByIdx returns nodeID by idx in nodes.
func (c *TestDXFContext) GetNodeIDByIdx(idx int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.nodes[idx].id
}

// NodeCount returns the number of nodes.
func (c *TestDXFContext) NodeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.mu.nodes)
}

func (c *TestDXFContext) getNode(id string) *tidbNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	idx, ok := c.mu.nodeIndices[id]
	if !ok {
		c.T.Logf("cannot find node of id %s", id)
		return nil
	}
	clone := *c.mu.nodes[idx]
	return &clone
}

func (c *TestDXFContext) electIfNeeded() {
	c.mu.Lock()
	if len(c.mu.nodes) == 0 || len(c.mu.ownerIndices) > 0 {
		c.mu.Unlock()
		return
	}

	newOwnerIdx := int(rand.Int31n(int32(len(c.mu.nodes))))
	ownerNode := c.mu.nodes[newOwnerIdx]
	c.mu.ownerIndices[ownerNode.id] = newOwnerIdx
	ownerNode.schMgr = scheduler.NewManager(c.Ctx, c.TaskMgr, ownerNode.id)
	ownerNode.schMgr.Start()
	ownerNode.owner = true
	c.mu.Unlock()

	c.T.Logf("new owner elected, id = %s, newOwnerIdx = %d", ownerNode.id, newOwnerIdx)
}

// WaitAsyncOperations waits all async operations to finish.
func (c *TestDXFContext) WaitAsyncOperations() {
	c.wg.Wait()
}

func (c *TestDXFContext) close() {
	c.wg.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, node := range c.mu.nodes {
		node.exeMgr.Stop()
		if node.owner {
			node.schMgr.Stop()
		}
	}
	c.mu.nodes = nil
	c.mu.ownerIndices = nil
	c.mu.nodeIndices = nil
}

// TestContext defines shared variables for disttask tests.
type TestContext struct {
	sync.RWMutex
	// taskID/step -> subtask map.
	subtasksHasRun map[string]map[int64]struct{}
	// for plan err handling tests.
	CallTime int
}

// CollectSubtask collects subtask info
func (c *TestContext) CollectSubtask(subtask *proto.Subtask) {
	key := getTaskStepKey(subtask.TaskID, subtask.Step)
	c.Lock()
	defer c.Unlock()
	m, ok := c.subtasksHasRun[key]
	if !ok {
		m = make(map[int64]struct{})
		c.subtasksHasRun[key] = m
	}
	m[subtask.ID] = struct{}{}
}

// CollectedSubtaskCnt returns the collected subtask count.
func (c *TestContext) CollectedSubtaskCnt(taskID int64, step proto.Step) int {
	key := getTaskStepKey(taskID, step)
	c.RLock()
	defer c.RUnlock()
	return len(c.subtasksHasRun[key])
}

// getTaskStepKey returns the key of a task step.
func getTaskStepKey(id int64, step proto.Step) string {
	return fmt.Sprintf("%d/%d", id, step)
}

// ReduceCheckInterval reduces the check interval for test.
func ReduceCheckInterval(t testing.TB) {
	schedulerMgrCheckIntervalBak := scheduler.CheckTaskRunningInterval
	schedulerCheckIntervalBak := scheduler.CheckTaskFinishedInterval
	taskCheckIntervalBak := taskexecutor.TaskCheckInterval
	checkIntervalBak := taskexecutor.SubtaskCheckInterval
	maxIntervalBak := taskexecutor.MaxSubtaskCheckInterval
	t.Cleanup(func() {
		scheduler.CheckTaskRunningInterval = schedulerMgrCheckIntervalBak
		scheduler.CheckTaskFinishedInterval = schedulerCheckIntervalBak
		taskexecutor.TaskCheckInterval = taskCheckIntervalBak
		taskexecutor.SubtaskCheckInterval = checkIntervalBak
		taskexecutor.MaxSubtaskCheckInterval = maxIntervalBak
	})
	scheduler.CheckTaskRunningInterval, scheduler.CheckTaskFinishedInterval = 100*time.Millisecond, 100*time.Millisecond
	taskexecutor.TaskCheckInterval, taskexecutor.MaxSubtaskCheckInterval, taskexecutor.SubtaskCheckInterval =
		10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond
}
