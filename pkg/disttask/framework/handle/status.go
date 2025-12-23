// Copyright 2025 PingCAP, Inc.
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

package handle

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/schstatus"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/tidbvar"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/cpu"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
)

// GetScheduleStatus returns the schedule status.
func GetScheduleStatus(ctx context.Context) (*schstatus.Status, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	manager, err := storage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	tasks, err := manager.GetTaskBasesInStates(ctx, proto.TaskStateRunning, proto.TaskStateModifying)
	if err != nil {
		return nil, errors.Trace(err)
	}
	nodeCount, nodeCPU, err := GetNodesInfo(ctx, manager)
	if err != nil {
		return nil, errors.Trace(err)
	}
	busyNodes, err := GetBusyNodes(ctx, manager)
	if err != nil {
		return nil, errors.Trace(err)
	}
	flags, err := GetScheduleFlags(ctx, manager)
	if err != nil {
		return nil, errors.Trace(err)
	}
	requiredNodes := CalculateRequiredNodes(tasks, nodeCPU)
	status := &schstatus.Status{
		Version:   schstatus.Version1,
		TaskQueue: schstatus.TaskQueue{ScheduledCount: len(tasks)},
		TiDBWorker: schstatus.NodeGroup{
			CPUCount:      nodeCPU,
			RequiredCount: requiredNodes,
			CurrentCount:  nodeCount,
			BusyNodes:     busyNodes,
		},
		TiKVWorker: schstatus.NodeGroup{
			// currently, we expect same mount of TiKV worker as TiDB worker.
			RequiredCount: requiredNodes,
		},
		Flags: flags,
	}
	return status, nil
}

// GetNodesInfo retrieves the number of managed nodes and their CPU count.
// exported for test.
func GetNodesInfo(ctx context.Context, manager *storage.TaskManager) (nodeCount int, cpuCount int, err error) {
	nodes, err := manager.GetAllNodes(ctx)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if len(nodes) == 0 {
		// shouldn't happen normally as every node will register itself to the meta
		// table.
		logutil.BgLogger().Warn("no managed nodes found, use local node CPU count instead")
		cpuCount = cpu.GetCPUCount()
	} else {
		cpuCount = nodes[0].CPUCount
	}
	return len(nodes), cpuCount, nil
}

// GetBusyNodes get nodes that are currently running subtasks, or is the DXF owner
// as we don't want owner node floating around.
// exported for test.
func GetBusyNodes(ctx context.Context, manager *storage.TaskManager) ([]schstatus.Node, error) {
	var ownerID string
	if err := manager.WithNewSession(func(se sessionctx.Context) error {
		var err2 error
		ownerID, err2 = se.GetSQLServer().GetDDLOwnerMgr().GetOwnerID(ctx)
		return err2
	}); err != nil {
		return nil, errors.Trace(err)
	}
	serverInfo, err := infosync.GetServerInfoByID(ctx, ownerID)
	if err != nil {
		return nil, err
	}
	ownerExecID := disttaskutil.GenerateExecID(serverInfo)
	busyNodes, err := manager.GetBusyNodes(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var found bool
	for i := range busyNodes {
		if busyNodes[i].ID == ownerExecID {
			busyNodes[i].IsOwner = true
			found = true
			break
		}
	}
	if !found {
		busyNodes = append(busyNodes, schstatus.Node{
			ID:      ownerExecID,
			IsOwner: true,
		})
	}
	return busyNodes, nil
}

// CalculateRequiredNodes simulates how scheduler and balancer schedules tasks,
// and calculates the required node count to run the tasks.
// 'tasks' must be ordered by its rank, see TaskBase for more info about task rank.
func CalculateRequiredNodes(tasks []*proto.TaskBase, cpuCount int) int {
	availResources := make([]int, 0, len(tasks))
	// for each task, at most MaxNodeCount subtasks can be run in parallel, and
	// on each node, each task can have at most 1 subtask running. we will try to
	// run the subtask on existing nodes, if not enough resources, we will create
	// new nodes.
	for _, t := range tasks {
		needed := t.MaxNodeCount
		for i, avail := range availResources {
			if needed <= 0 {
				break
			}
			if avail >= t.RequiredSlots {
				availResources[i] -= t.RequiredSlots
				needed--
			}
		}
		for range needed {
			// we have restricted the concurrency of each task to be less than
			// node CPU count at submit time, so t.Concurrency > cpuCount should
			// not happen.
			availResources = append(availResources, cpuCount-t.RequiredSlots)
		}
	}
	// make sure 1 node exist for DXF owner and works as a reserved node, to make
	// small tasks more responsive.
	return max(len(availResources), 1)
}

// GetScheduleFlags returns the schedule flags, such as pause-scale-in flag.
// exported for test.
func GetScheduleFlags(ctx context.Context, manager *storage.TaskManager) (map[schstatus.Flag]schstatus.TTLFlag, error) {
	flags := make(map[schstatus.Flag]schstatus.TTLFlag)
	pauseScaleIn, err := getPauseScaleInFlag(ctx, manager)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if pauseScaleIn.Enabled {
		flags[schstatus.PauseScaleInFlag] = *pauseScaleIn
	}
	return flags, nil
}

func getPauseScaleInFlag(ctx context.Context, manager *storage.TaskManager) (*schstatus.TTLFlag, error) {
	flag := &schstatus.TTLFlag{}
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err := manager.WithNewSession(func(se sessionctx.Context) error {
		rs, err2 := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `SELECT VARIABLE_VALUE from mysql.tidb WHERE VARIABLE_NAME = %?`,
			tidbvar.DXFSchedulePauseScaleIn)
		if err2 != nil {
			return err2
		}
		if len(rs) == 0 {
			// not set before, use default
			return nil
		}
		value := rs[0].GetString(0)
		if err2 = json.Unmarshal([]byte(value), flag); err2 != nil {
			return errors.Trace(err2)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// pause-scale-in flag is not cleaned up even if it is expired, reset it.
	if flag.Enabled {
		if flag.ExpireTime.Before(time.Now()) {
			flag = &schstatus.TTLFlag{}
		}
	}
	return flag, nil
}
