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

package schstatus

import (
	"encoding/json"
	"fmt"
	"time"
)

// Version represents the version of the scheduler status.
type Version int

const (
	// Version1 is the first version of the scheduler status.
	Version1 Version = iota + 1
)

// TaskQueue represents the status of a task queue in the scheduler.
type TaskQueue struct {
	// PendingCount is the number of tasks that are scheduled, it only includes
	// the number of tasks in running/modifying state.
	// cancelling , pausing and resuming tasks are also scheduled, but since they
	// mostly run in a short time, they will be excluded.
	ScheduledCount int `json:"scheduled_count,omitempty"`
}

// Node represents the status of a node.
type Node struct {
	// ID is the unique identifier of the node, it's the same as the exec_id
	// field of mysql.tidb_background_subtask table.
	ID string `json:"id,omitempty"`
	// IsOwner indicates whether the node is the owner of DXF.
	IsOwner bool `json:"is_owner,omitempty"`
}

// NodeGroup represents the resource status of TiDB or TiKV worker node group.
type NodeGroup struct {
	// CPUCount is the number of CPUs available for the node.
	CPUCount int `json:"cpu_count,omitempty"`
	// RequiredCount is the required number of nodes to run the scheduled tasks.
	// physical cluster controller should try to scale in/out nodes to match
	// this number.
	RequiredCount int `json:"required_count,omitempty"`
	// CurrentCount is the current number of nodes available, either idle or busy.
	CurrentCount int `json:"current_count,omitempty"`
	// BusyNodes is the list of busy nodes, which are currently running subtasks.
	// Nodes in this list shouldn't be selected as the target node to be scaled in,
	// to avoid the subtask being rerun and enlarge its execution time.
	BusyNodes []Node `json:"busy_nodes,omitempty"`
}

// Flag represents a flag in the scheduler.
type Flag string

const (
	// PauseScaleInFlag is the flag to pause the scale-in action of the workers.
	PauseScaleInFlag Flag = "pause_scale_in"
)

// TTLInfo represents the TTL info of a flag or resource tune factors in the
// scheduler.
type TTLInfo struct {
	TTL        time.Duration `json:"ttl,omitempty"`
	ExpireTime time.Time     `json:"expire_time,omitempty"`
}

// TTLFlag represents a flag with TTL in the scheduler.
type TTLFlag struct {
	Enabled bool `json:"enabled,omitempty"`
	TTLInfo
}

// String implements fmt.Stringer interface for TTLFlag.
func (a *TTLFlag) String() string {
	bytes, _ := json.Marshal(a)
	return string(bytes)
}

// Status represents the status of the scheduler.
type Status struct {
	Version    Version   `json:"version,omitempty"`
	TaskQueue  TaskQueue `json:"task_queue,omitempty"`
	TiDBWorker NodeGroup `json:"tidb_worker,omitempty"`
	TiKVWorker NodeGroup `json:"tikv_worker,omitempty"`
	// Flags is a map of flags, we only have one type of flag right now.
	// PauseScaleInFlag is the flag to notify the cluster controller to pause the
	// scale-in action of the workers. as the schedule and scale-in/out operations
	// are run asynchronously, when there are multiple tasks running in parallel,
	// it's possible that some subtask keeps being scheduled to run on a worker
	// that is being scaled in, which will cause the subtask repeatedly being
	// balanced to other workers, i.e., scale-in/schedule conflict issue.
	// although this issue will disappear when there is no node to be scaled in,
	// it might make the task take a long time to finish, so if we meet this issue
	// we can use this flag to workaround.
	Flags map[Flag]TTLFlag `json:"flags,omitempty"`
}

// String implements fmt.Stringer interface for Status.
func (s *Status) String() string {
	bak := *s
	if len(bak.TiDBWorker.BusyNodes) > 5 {
		bak.TiDBWorker.BusyNodes = bak.TiDBWorker.BusyNodes[:5]
		bak.TiDBWorker.BusyNodes = append(bak.TiDBWorker.BusyNodes,
			Node{ID: fmt.Sprintf("... too many nodes, total %d busy nodes ...", len(s.TiDBWorker.BusyNodes))})
	}
	bytes, _ := json.Marshal(&bak)
	return string(bytes)
}
