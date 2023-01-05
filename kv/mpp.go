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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/mpp"
)

type MppVersion int64

const (
	// MppVersionV0 supports TiFlash version [~, ~]
	MppVersionV0 MppVersion = iota

	// MppVersionV1 supports TiFlash version [v6.6.x, ~]
	MppVersionV1

	// MppVersionV2
	// MppVersionV3

	mppVersionMax

	// NewestMppVersion means the latest version used in MPP tasks
	NewestMppVersion MppVersion = mppVersionMax - 1

	// MppVersionUnspecified means the illegal version
	MppVersionUnspecified MppVersion = -1
)

func (v MppVersion) ToInt64() int64 {
	return int64(v)
}

// GetTiDBMppVersion returns the mpp-version can be used in mpp plan
func GetTiDBMppVersion() MppVersion {
	return NewestMppVersion
}

var mppVersionFeatures = map[MppVersion]string{
	MppVersionV0: "none",
	MppVersionV1: "exchange data compression",
}

// GetMppVersionFeatures return the features for mpp-version
func GetMppVersionFeatures(mppVersion MppVersion) string {
	val, ok := mppVersionFeatures[mppVersion]
	if ok {
		return val
	}
	return ""
}

// FmtMppVersion return
func FmtMppVersion(v MppVersion) string {
	var version string
	if v == MppVersionUnspecified {
		v = GetTiDBMppVersion()
		version = fmt.Sprintf("unspecified(use %d)", v)
	} else {
		version = fmt.Sprintf("%d", v)
	}

	return fmt.Sprintf("`%s` features `%s`", version, GetMppVersionFeatures(v))
}

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
	MppVersion MppVersion  // mpp version

	PartitionTableIDs []int64
}

// ToPB generates the pb structure.
func (t *MPPTask) ToPB() *mpp.TaskMeta {
	meta := &mpp.TaskMeta{
		StartTs:    t.StartTs,
		TaskId:     t.ID,
		MppVersion: t.MppVersion.ToInt64(),
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
	MppVersion         MppVersion              // mpp version
	ExchangeSenderMeta *mpp.ExchangeSenderMeta // exchange sender info, compress method
}

// MPPClient accepts and processes mpp requests.
type MPPClient interface {
	// ConstructMPPTasks schedules task for a plan fragment.
	// TODO:: This interface will be refined after we support more executors.
	ConstructMPPTasks(context.Context, *MPPBuildTasksRequest, *sync.Map, time.Duration) ([]MPPTaskMeta, error)
	// DispatchMPPTasks dispatches ALL mpp requests at once, and returns an iterator that transfers the data.
	DispatchMPPTasks(ctx context.Context, vars interface{}, reqs []*MPPDispatchRequest, needTriggerFallback bool, startTs uint64, mppVersion MppVersion) Response
}

// MPPBuildTasksRequest request the stores allocation for a mpp plan fragment.
// However, the request doesn't contain the particular plan, because only key ranges take effect on the location assignment.
type MPPBuildTasksRequest struct {
	KeyRanges []KeyRange
	StartTS   uint64

	PartitionIDAndRanges []PartitionIDAndRanges
}

// ExchangeCompressMethod means the compress method used in exchange operator
type ExchangeCompressMethod int

const (
	// ExchangeCompressMethodNONE indicates no compression
	ExchangeCompressMethodNONE ExchangeCompressMethod = iota
	// ExchangeCompressMethodFast indicates fast compression/decompression speed, compression ratio is lower than HC mode
	ExchangeCompressMethodFast
	// ExchangeCompressMethodHC indicates high compression (HC) ratio mode
	ExchangeCompressMethodHC
	// ExchangeCompressMethodUnspecified means unspecified compress method, let TiDB choose one
	ExchangeCompressMethodUnspecified

	// DefaultExchangeCompressMethod means default compress method
	DefaultExchangeCompressMethod ExchangeCompressMethod = ExchangeCompressMethodUnspecified

	exchangeCompressMethodUnspecifiedName string = "UNSPECIFIED"
)

// Name returns the name of ExchangeCompressMethod
func (t ExchangeCompressMethod) Name() string {
	if t == ExchangeCompressMethodUnspecified {
		return exchangeCompressMethodUnspecifiedName
	}
	return t.ToMppCompressMethod().String()
}

// ToExchangeCompressMethod returns the ExchangeCompressMethod from name
func ToExchangeCompressMethod(name string) (ExchangeCompressMethod, bool) {
	if name == exchangeCompressMethodUnspecifiedName {
		return ExchangeCompressMethodUnspecified, true
	}
	value, ok := mpp.CompressMethod_value[name]
	if ok {
		return ExchangeCompressMethod(value), true
	}
	return DefaultExchangeCompressMethod, false
}

// ToMppCompressMethod returns mpp.CompressMethod from kv.ExchangeCompressMethod
func (t ExchangeCompressMethod) ToMppCompressMethod() mpp.CompressMethod {
	switch t {
	case ExchangeCompressMethodNONE:
		return mpp.CompressMethod_NONE
	case ExchangeCompressMethodFast:
		return mpp.CompressMethod_FAST
	case ExchangeCompressMethodHC:
		return mpp.CompressMethod_HIGH_COMPRESSION
	}

	// Use `FAST` as the defualt method
	return mpp.CompressMethod_FAST
}
