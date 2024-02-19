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
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	llog "github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

var (
	// liveNodesCheckInterval is the tick interval of fetching all server infos from etcs.
	nodesCheckInterval = 2 * CheckTaskFinishedInterval
)

// NodeManager maintains live TiDB nodes in the cluster, and maintains the nodes
// managed by the framework.
type NodeManager struct {
	logger *zap.Logger
	// prevLiveNodes is used to record the live nodes in last checking.
	prevLiveNodes map[string]struct{}
	// managedNodes is the cached nodes managed by the framework.
	// see TaskManager.GetManagedNodes for more details.
	managedNodes atomic.Pointer[[]string]
}

func newNodeManager(serverID string) *NodeManager {
	logger := log.L()
	if intest.InTest {
		logger = log.L().With(zap.String("server-id", serverID))
	}
	nm := &NodeManager{
		logger:        logger,
		prevLiveNodes: make(map[string]struct{}),
	}
	managedNodes := make([]string, 0, 10)
	nm.managedNodes.Store(&managedNodes)
	return nm
}

func (nm *NodeManager) maintainLiveNodesLoop(ctx context.Context, taskMgr TaskManager) {
	ticker := time.NewTicker(nodesCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nm.maintainLiveNodes(ctx, taskMgr)
		}
	}
}

// maintainLiveNodes manages live node info in dist_framework_meta table
// see recoverMetaLoop in task executor for when node is inserted into dist_framework_meta.
func (nm *NodeManager) maintainLiveNodes(ctx context.Context, taskMgr TaskManager) {
	// Safe to discard errors since this function can be called at regular intervals.
	liveExecIDs, err := GetLiveExecIDs(ctx)
	if err != nil {
		nm.logger.Warn("generate task executor nodes met error", llog.ShortError(err))
		return
	}
	nodeChanged := len(liveExecIDs) != len(nm.prevLiveNodes)
	currLiveNodes := make(map[string]struct{}, len(liveExecIDs))
	for _, execID := range liveExecIDs {
		if _, ok := nm.prevLiveNodes[execID]; !ok {
			nodeChanged = true
		}
		currLiveNodes[execID] = struct{}{}
	}
	if !nodeChanged {
		return
	}

	oldNodes, err := taskMgr.GetAllNodes(ctx)
	if err != nil {
		nm.logger.Warn("get all nodes met error", llog.ShortError(err))
		return
	}

	deadNodes := make([]string, 0)
	for _, node := range oldNodes {
		if _, ok := currLiveNodes[node.ID]; !ok {
			deadNodes = append(deadNodes, node.ID)
		}
	}
	if len(deadNodes) == 0 {
		nm.prevLiveNodes = currLiveNodes
		return
	}
	nm.logger.Info("delete dead nodes from dist_framework_meta",
		zap.Strings("dead-nodes", deadNodes))
	err = taskMgr.DeleteDeadNodes(ctx, deadNodes)
	if err != nil {
		nm.logger.Warn("delete dead nodes from dist_framework_meta failed", llog.ShortError(err))
		return
	}
	nm.prevLiveNodes = currLiveNodes
}

func (nm *NodeManager) refreshManagedNodesLoop(ctx context.Context, taskMgr TaskManager, slotMgr *SlotManager) {
	ticker := time.NewTicker(nodesCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nm.refreshManagedNodes(ctx, taskMgr, slotMgr)
		}
	}
}

// TestRefreshedChan is used to sync the test.
var TestRefreshedChan = make(chan struct{})

// refreshManagedNodes maintains the nodes managed by the framework.
func (nm *NodeManager) refreshManagedNodes(ctx context.Context, taskMgr TaskManager, slotMgr *SlotManager) {
	newNodes, err := taskMgr.GetManagedNodes(ctx)
	if err != nil {
		nm.logger.Warn("get managed nodes met error", llog.ShortError(err))
		return
	}
	nodeIDs := make([]string, 0, len(newNodes))
	var cpuCount int
	for _, node := range newNodes {
		nodeIDs = append(nodeIDs, node.ID)
		if node.CPUCount > 0 {
			cpuCount = node.CPUCount
		}
	}
	slotMgr.updateCapacity(cpuCount)
	nm.managedNodes.Store(&nodeIDs)

	failpoint.Inject("syncRefresh", func() {
		TestRefreshedChan <- struct{}{}
	})
}

// GetManagedNodes returns the nodes managed by the framework.
// return a copy of the managed nodes.
func (nm *NodeManager) getManagedNodes() []string {
	nodes := *nm.managedNodes.Load()
	res := make([]string, len(nodes))
	copy(res, nodes)
	return res
}
