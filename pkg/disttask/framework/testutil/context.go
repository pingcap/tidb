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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
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
	T           *testing.T
	Store       kv.Storage
	Ctx         context.Context
	TaskMgr     *storage.TaskManager
	MockCtrl    *gomock.Controller
	TestContext *TestContext

	idAllocator atomic.Int32
	// in real case, when node scale in/out, the node might use the same IP or host name
	// such as using K8S, so we use this to simulate this case.
	nodeIDPool chan string
	rand       *rand.Rand
	wg         tidbutil.WaitGroupWrapper
	mu         struct {
		sync.RWMutex
		// to test network partition, we allow multiple owners
		ownerIndices map[string]int
		nodeIndices  map[string]int
		nodes        []*tidbNode
	}
}

// NewTestDXFContext creates a new TestDXFContext.
func NewTestDXFContext(t *testing.T, nodeNum int) *TestDXFContext {
	// all nodes are isometric with 16 CPUs
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTaskExecutorNodes", "return()"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTaskExecutorNodes"))
	})
	store := testkit.CreateMockStore(t)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return testkit.NewSession(t, store), nil
	}, 10, 10, time.Second)
	t.Cleanup(func() {
		pool.Close()
	})
	taskManager := storage.NewTaskManager(pool)
	storage.SetTaskManager(taskManager)
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)

	seed := time.Now().UnixNano()
	t.Log("dxf context seed:", seed)
	ctrl := gomock.NewController(t)
	c := &TestDXFContext{
		T:        t,
		Store:    store,
		Ctx:      ctx,
		TaskMgr:  taskManager,
		MockCtrl: ctrl,
		TestContext: &TestContext{
			subtasksHasRun: make(map[string]map[int64]struct{}),
		},
		nodeIDPool: make(chan string, 100),
		rand:       rand.New(rand.NewSource(seed)),
	}
	c.mu.ownerIndices = make(map[string]int)
	c.mu.nodeIndices = make(map[string]int, nodeNum)
	c.init(nodeNum)

	t.Cleanup(func() {
		ctrl.Finish()
		c.close()
	})

	return c
}

// init initializes the context with nodeNum tidb nodes.
// The last node is the owner.
func (c *TestDXFContext) init(nodeNum int) {
	for i := 0; i < nodeNum; i++ {
		c.ScaleOutBy(c.getNodeID(), i == nodeNum-1)
	}
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
	c.updateLiveExecIDs(id)
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
	defer c.mu.Unlock()
	c.mu.nodes = append(c.mu.nodes, node)
	c.mu.nodeIndices[id] = len(c.mu.nodes) - 1
	if owner {
		c.mu.ownerIndices[id] = len(c.mu.nodes) - 1
	}
}

func (c *TestDXFContext) updateLiveExecIDs(newID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	execIDs := make([]string, 0, len(c.mu.nodes)+1)
	for _, n := range c.mu.nodes {
		execIDs = append(execIDs, n.id)
	}
	if len(newID) > 0 {
		execIDs = append(execIDs, newID)
	}
	scheduler.MockServerInfo.Store(&execIDs)
}

// ScaleIn scales in some last added tidb nodes, elect new owner if required.
func (c *TestDXFContext) ScaleIn(nodeNum int) {
	for i := 0; i < nodeNum; i++ {
		c.mu.Lock()
		if len(c.mu.nodes) == 0 {
			c.mu.Unlock()
			return
		}
		node := c.mu.nodes[len(c.mu.nodes)-1]
		c.mu.Unlock()

		c.ScaleInBy(node.id)
	}
}

// ScaleInBy scales in a tidb node by id, elect new owner if required.
func (c *TestDXFContext) ScaleInBy(id string) {
	c.mu.Lock()
	idx, ok := c.mu.nodeIndices[id]
	if !ok {
		c.mu.Unlock()
		return
	}
	node := c.mu.nodes[idx]
	c.mu.nodes = append(c.mu.nodes[:idx], c.mu.nodes[idx+1:]...)
	delete(c.mu.nodeIndices, id)
	if node.owner {
		delete(c.mu.ownerIndices, id)
	}
	c.recycleNodeID(id)
	c.mu.Unlock()

	c.updateLiveExecIDs("")

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
	c.mu.Lock()
	if len(c.mu.nodes) == 0 {
		c.mu.Unlock()
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

// InitTestContext inits test context for disttask tests.
func InitTestContext(t *testing.T, nodeNum int) (context.Context, *gomock.Controller, *TestContext, *testkit.DistExecutionContext) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu"))
	})

	executionContext := testkit.NewDistExecutionContext(t, nodeNum)
	testCtx := &TestContext{
		subtasksHasRun: make(map[string]map[int64]struct{}),
	}
	return ctx, ctrl, testCtx, executionContext
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
