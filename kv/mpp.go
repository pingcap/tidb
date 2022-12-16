// Copyright 2020 PingCAP, Inc.
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

package kv

import (
	"context"
	atomicutil "go.uber.org/atomic"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/mpp"
)

const (
	// MppVersionV0 supports TiFlash version [~, v6.5.0]
	// Used when cluster version <= v6.5.0
	MppVersionV0 int64 = 0

	// MppVersionV1 supports TiFlash version [v6.6.0, ~]
	// Features: data compression in exchange operator;
	MppVersionV1             int64  = 1000
	MppVersionV1StoreVersion string = "6.6.0"

	// MppVersionV2 int64 = MppVersionV1 * 2
	// MppVersionV3 int64 = MppVersionV1 * 3

	// CurMppVersion means the latest version used in MPP tasks
	CurMppVersion int64 = MppVersionV1
)

// var ClusterMinMppVersion = atomicutil.NewInt64(CurMppVersion)

// MPPTaskMeta means the meta info such as location of a mpp task.
type MPPTaskMeta interface {
	// GetAddress indicates which node this task should execute on.
	GetAddress() string
}

// MPPTask means the minimum execution unit of a mpp computation job.
type MPPTask struct {
	Meta       MPPTaskMeta // on which store this task will execute
	ID         int64       // mppTaskID
	StartTs    uint64      //
	TableID    int64       // physical table id
	MppVersion int64       // mpp version

	PartitionTableIDs []int64
}

// ToPB generates the pb structure.
func (t *MPPTask) ToPB() *mpp.TaskMeta {
	meta := &mpp.TaskMeta{
		StartTs:    t.StartTs,
		TaskId:     t.ID,
		MppVersion: t.MppVersion,
	}
	if t.ID != -1 {
		meta.Address = t.Meta.GetAddress()
	}
	return meta
}

// MppTaskStates denotes the state of mpp tasks
type MppTaskStates uint8

const (
	// MppTaskReady means the task is ready
	MppTaskReady MppTaskStates = iota
	// MppTaskRunning means the task is running
	MppTaskRunning
	// MppTaskCancelled means the task is cancelled
	MppTaskCancelled
	// MppTaskDone means the task is done
	MppTaskDone
)

// MPPDispatchRequest stands for a dispatching task.
type MPPDispatchRequest struct {
	Data    []byte      // data encodes the dag coprocessor request.
	Meta    MPPTaskMeta // mpp store is the location of tiflash store.
	IsRoot  bool        // root task returns data to tidb directly.
	Timeout uint64      // If task is assigned but doesn't receive a connect request during timeout, the task should be destroyed.
	// SchemaVer is for any schema-ful storage (like tiflash) to validate schema correctness if necessary.
	SchemaVar          int64
	StartTs            uint64
	ID                 int64 // identify a single task
	State              MppTaskStates
	MppVersion         int64                   // mpp version
	ExchangeSenderMeta *mpp.ExchangeSenderMeta // exchange sender info, compress method
}

// MPPClient accepts and processes mpp requests.
type MPPClient interface {
	// ConstructMPPTasks schedules task for a plan fragment.
	// TODO:: This interface will be refined after we support more executors.
	ConstructMPPTasks(context.Context, *MPPBuildTasksRequest, *sync.Map, time.Duration) ([]MPPTaskMeta, error)
	// DispatchMPPTasks dispatches ALL mpp requests at once, and returns an iterator that transfers the data.
	DispatchMPPTasks(ctx context.Context, vars interface{}, reqs []*MPPDispatchRequest, needTriggerFallback bool, startTs uint64) Response
	//
	GetClusterMinMppVersion() *atomicutil.Int64
}

// MPPBuildTasksRequest request the stores allocation for a mpp plan fragment.
// However, the request doesn't contain the particular plan, because only key ranges take effect on the location assignment.
type MPPBuildTasksRequest struct {
	KeyRanges []KeyRange
	StartTS   uint64

	PartitionIDAndRanges []PartitionIDAndRanges
}

type ExchangeCompressMethod int

const (
	NONE ExchangeCompressMethod = iota
	LZ4
	ZSTD
	UNSPECIFIED = 255
)

func (t ExchangeCompressMethod) Name() string {
	if t == NONE {
		return "none"
	} else if t == LZ4 {
		return "lz4"
	} else if t == ZSTD {
		return "zstd"
	}
	return "unspecified"
}

func (t ExchangeCompressMethod) ToMppCompressMethod() mpp.CompressMethod {
	if t == NONE {
		return mpp.CompressMethod_NONE
	} else if t == LZ4 {
		return mpp.CompressMethod_LZ4
	} else if t == ZSTD {
		return mpp.CompressMethod_ZSTD
	}
	return mpp.CompressMethod_LZ4
}
