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
	"time"

	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tipb/go-tipb"
)

// MppVersion indicates the mpp-version used to build mpp plan
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

// ToInt64 transforms MppVersion to int64“
func (v MppVersion) ToInt64() int64 {
	return int64(v)
}

// GetTiDBMppVersion returns the mpp-version can be used in mpp plan
func GetTiDBMppVersion() MppVersion {
	return NewestMppVersion
}

var mppVersionFeatures = map[MppVersion]string{
	MppVersionV1: "exchange data compression",
}

// GetMppVersionFeatures returns the features for mpp-version
func GetMppVersionFeatures(mppVersion MppVersion) string {
	val, ok := mppVersionFeatures[mppVersion]
	if ok {
		return val
	}
	return "none"
}

// FmtMppVersion returns the description about mpp-version
func FmtMppVersion(v MppVersion) string {
	var version string
	if v == MppVersionUnspecified {
		v = GetTiDBMppVersion()
		version = fmt.Sprintf("unspecified(use %d)", v)
	} else {
		version = fmt.Sprintf("%d", v)
	}

	return fmt.Sprintf("%s, features `%s`", version, GetMppVersionFeatures(v))
}

// MPPTaskMeta means the meta info such as location of a mpp task.
type MPPTaskMeta interface {
	// GetAddress indicates which node this task should execute on.
	GetAddress() string
}

// MPPQueryID means the global unique id of a mpp query.
type MPPQueryID struct {
	QueryTs      uint64 // timestamp of query execution, used for TiFlash minTSO schedule
	LocalQueryID uint64 // unique mpp query id in local tidb memory.
	ServerID     uint64
}

// MPPTask means the minimum execution unit of a mpp computation job.
type MPPTask struct {
	Meta       MPPTaskMeta // on which store this task will execute
	ID         int64       // mppTaskID
	StartTs    uint64
	MppQueryID MPPQueryID
	TableID    int64      // physical table id
	MppVersion MppVersion // mpp version

	PartitionTableIDs []int64
}

// ToPB generates the pb structure.
func (t *MPPTask) ToPB() *mpp.TaskMeta {
	meta := &mpp.TaskMeta{
		StartTs:      t.StartTs,
		QueryTs:      t.MppQueryID.QueryTs,
		LocalQueryId: t.MppQueryID.LocalQueryID,
		ServerId:     t.MppQueryID.ServerID,
		TaskId:       t.ID,
		MppVersion:   t.MppVersion.ToInt64(),
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
	SchemaVar  int64
	StartTs    uint64
	MppQueryID MPPQueryID
	ID         int64 // identify a single task
	State      MppTaskStates
	MppVersion MppVersion // mpp version
}

// MPPClient accepts and processes mpp requests.
type MPPClient interface {
	// ConstructMPPTasks schedules task for a plan fragment.
	// TODO:: This interface will be refined after we support more executors.
	ConstructMPPTasks(context.Context, *MPPBuildTasksRequest, time.Duration) ([]MPPTaskMeta, error)
	// DispatchMPPTasks dispatches ALL mpp requests at once, and returns an iterator that transfers the data.
	DispatchMPPTasks(ctx context.Context, vars interface{}, reqs []*MPPDispatchRequest, needTriggerFallback bool, startTs uint64, mppQueryID MPPQueryID, mppVersion MppVersion) Response
}

// MPPBuildTasksRequest request the stores allocation for a mpp plan fragment.
// However, the request doesn't contain the particular plan, because only key ranges take effect on the location assignment.
type MPPBuildTasksRequest struct {
	KeyRanges []KeyRange
	StartTS   uint64

	PartitionIDAndRanges []PartitionIDAndRanges
}

// ExchangeCompressionMode means the compress method used in exchange operator
type ExchangeCompressionMode int

const (
	// ExchangeCompressionModeNONE indicates no compression
	ExchangeCompressionModeNONE ExchangeCompressionMode = iota
	// ExchangeCompressionModeFast indicates fast compression/decompression speed, compression ratio is lower than HC mode
	ExchangeCompressionModeFast
	// ExchangeCompressionModeHC indicates high compression (HC) ratio mode
	ExchangeCompressionModeHC
	// ExchangeCompressionModeUnspecified means unspecified compress method, let TiDB choose one
	ExchangeCompressionModeUnspecified

	// DefaultExchangeCompressionMode means default compress method
	DefaultExchangeCompressionMode ExchangeCompressionMode = ExchangeCompressionModeUnspecified

	exchangeCompressionModeUnspecifiedName string = "UNSPECIFIED"
)

// Name returns the name of ExchangeCompressionMode
func (t ExchangeCompressionMode) Name() string {
	if t == ExchangeCompressionModeUnspecified {
		return exchangeCompressionModeUnspecifiedName
	}
	return t.ToMppCompressionMode().String()
}

// ToExchangeCompressionMode returns the ExchangeCompressionMode from name
func ToExchangeCompressionMode(name string) (ExchangeCompressionMode, bool) {
	if name == exchangeCompressionModeUnspecifiedName {
		return ExchangeCompressionModeUnspecified, true
	}
	value, ok := tipb.CompressionMode_value[name]
	if ok {
		return ExchangeCompressionMode(value), true
	}
	return DefaultExchangeCompressionMode, false
}

// ToMppCompressionMode returns tipb.CompressionMode from kv.ExchangeCompressionMode
func (t ExchangeCompressionMode) ToMppCompressionMode() tipb.CompressionMode {
	switch t {
	case ExchangeCompressionModeNONE:
		return tipb.CompressionMode_NONE
	case ExchangeCompressionModeFast:
		return tipb.CompressionMode_FAST
	case ExchangeCompressionModeHC:
		return tipb.CompressionMode_HIGH_COMPRESSION
	}

	// Use `FAST` as the defualt method
	return tipb.CompressionMode_FAST
}
