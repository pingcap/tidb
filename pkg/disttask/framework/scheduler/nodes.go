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

	"github.com/pingcap/tidb/br/pkg/lightning/log"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	// liveNodesCheckInterval is the tick interval of fetching all server infos from etcs.
	nodesCheckInterval = 2 * checkTaskFinishedInterval
)

// NodeManager maintains live TiDB nodes in the cluster, and maintains the nodes
// managed by the framework.
type NodeManager struct {
	// prevLiveNodes is used to record the live nodes in last checking.
	prevLiveNodes map[string]struct{}
	// managedNodes is the cached nodes managed by the framework.
	// see TaskManager.GetManagedNodes for more details.
	managedNodes atomic.Pointer[[]string]
}

func newNodeManager() *NodeManager {
	nm := &NodeManager{
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
	serverInfos, err := GenerateTaskExecutorNodes(ctx)
	if err != nil {
		logutil.BgLogger().Warn("generate task executor nodes met error", log.ShortError(err))
		return
	}
	nodeChanged := len(serverInfos) != len(nm.prevLiveNodes)
	currLiveNodes := make(map[string]struct{}, len(serverInfos))
	for _, info := range serverInfos {
		execID := disttaskutil.GenerateExecID(info)
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
		logutil.BgLogger().Warn("get all nodes met error", log.ShortError(err))
		return
	}

	deadNodes := make([]string, 0)
	for _, nodeID := range oldNodes {
		if _, ok := currLiveNodes[nodeID]; !ok {
			deadNodes = append(deadNodes, nodeID)
		}
	}
	if len(deadNodes) == 0 {
		nm.prevLiveNodes = currLiveNodes
		return
	}
	logutil.BgLogger().Info("delete dead nodes from dist_framework_meta",
		zap.Int("dead-nodes", len(deadNodes)))
	err = taskMgr.DeleteDeadNodes(ctx, deadNodes)
	if err != nil {
		logutil.BgLogger().Warn("delete dead nodes from dist_framework_meta failed", log.ShortError(err))
		return
	}
	nm.prevLiveNodes = currLiveNodes
}

func (nm *NodeManager) refreshManagedNodesLoop(ctx context.Context, taskMgr TaskManager) {
	ticker := time.NewTicker(nodesCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nm.refreshManagedNodes(ctx, taskMgr)
		}
	}
}

// refreshManagedNodes maintains the nodes managed by the framework.
func (nm *NodeManager) refreshManagedNodes(ctx context.Context, taskMgr TaskManager) {
	newNodes, err := taskMgr.GetManagedNodes(ctx)
	if err != nil {
		logutil.BgLogger().Warn("get managed nodes met error", log.ShortError(err))
		return
	}
	if newNodes == nil {
		newNodes = []string{}
	}
	nm.managedNodes.Store(&newNodes)
}

// GetManagedNodes returns the nodes managed by the framework.
// The returned map is read-only, don't write to it.
func (nm *NodeManager) getManagedNodes() []string {
	return *nm.managedNodes.Load()
}
