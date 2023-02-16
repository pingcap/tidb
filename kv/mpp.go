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
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/util/memory"
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

	newestMppVersion MppVersion = mppVersionMax - 1

	// MppVersionUnspecified means the illegal or unspecified version, it only used in TiDB.
	MppVersionUnspecified MppVersion = -1

	// MppVersionUnspecifiedName denotes name of UNSPECIFIED mpp version
	MppVersionUnspecifiedName string = "UNSPECIFIED"
)

// ToInt64 transforms MppVersion to int64
func (v MppVersion) ToInt64() int64 {
	return int64(v)
}

// ToMppVersion transforms string to MppVersion
func ToMppVersion(name string) (MppVersion, bool) {
	name = strings.ToUpper(name)
	if name == MppVersionUnspecifiedName {
		return MppVersionUnspecified, true
	}
	v, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return MppVersionUnspecified, false
	}
	version := MppVersion(v)
	if version >= MppVersionUnspecified && version <= newestMppVersion {
		return version, true
	}
	return MppVersionUnspecified, false
}

// GetNewestMppVersion returns the mpp-version can be used in mpp plan
func GetNewestMppVersion() MppVersion {
	return newestMppVersion
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

	PartitionTableIDs                 []int64
	IsDisaggregatedTiFlashStaticPrune bool
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
}

// MPPClient accepts and processes mpp requests.
type MPPClient interface {
	// ConstructMPPTasks schedules task for a plan fragment.
	// TODO:: This interface will be refined after we support more executors.
	ConstructMPPTasks(context.Context, *MPPBuildTasksRequest, time.Duration) ([]MPPTaskMeta, error)
	// DispatchMPPTasks dispatches ALL mpp requests at once, and returns an iterator that transfers the data.
	DispatchMPPTasks(ctx context.Context, vars interface{}, reqs []*MPPDispatchRequest, needTriggerFallback bool, startTs uint64, mppQueryID MPPQueryID, mppVersion MppVersion, memTracker *memory.Tracker) Response
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
	// ExchangeCompressionModeUnspecified indicates unspecified compress method, let TiDB choose one
	ExchangeCompressionModeUnspecified

	// RecommendedExchangeCompressionMode indicates recommended compression mode
	RecommendedExchangeCompressionMode ExchangeCompressionMode = ExchangeCompressionModeFast

	exchangeCompressionModeUnspecifiedName string = "UNSPECIFIED"
)

// Name returns the name of ExchangeCompressionMode
func (t ExchangeCompressionMode) Name() string {
	if t == ExchangeCompressionModeUnspecified {
		return exchangeCompressionModeUnspecifiedName
	}
	return t.ToTipbCompressionMode().String()
}

// ToExchangeCompressionMode returns the ExchangeCompressionMode from name
func ToExchangeCompressionMode(name string) (ExchangeCompressionMode, bool) {
	name = strings.ToUpper(name)
	if name == exchangeCompressionModeUnspecifiedName {
		return ExchangeCompressionModeUnspecified, true
	}
	value, ok := tipb.CompressionMode_value[name]
	if ok {
		return ExchangeCompressionMode(value), true
	}
	return ExchangeCompressionModeNONE, false
}

// ToTipbCompressionMode returns tipb.CompressionMode from kv.ExchangeCompressionMode
func (t ExchangeCompressionMode) ToTipbCompressionMode() tipb.CompressionMode {
	switch t {
	case ExchangeCompressionModeNONE:
		return tipb.CompressionMode_NONE
	case ExchangeCompressionModeFast:
		return tipb.CompressionMode_FAST
	case ExchangeCompressionModeHC:
		return tipb.CompressionMode_HIGH_COMPRESSION
	}
	return tipb.CompressionMode_NONE
}
